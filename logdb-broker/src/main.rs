//! logdb-broker binary (cr-037).
//!
//! Loads [`BrokerConfig`] (YAML path in `LOGDB_BROKER_CONFIG`, or defaults),
//! installs tracing + an optional Prometheus exporter, recovers committed
//! offsets from the logdbd meta stream, and serves [`BrokerServiceImpl`] with
//! graceful shutdown on SIGTERM/Ctrl-C.

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;

use logdb_broker::config::BrokerConfig;
use logdb_broker::coordinator::CoordinatorRegistry;
use logdb_broker::forwarder::Forwarder;
use logdb_broker::persistence::Persistence;
use logdb_broker::service::BrokerServiceImpl;
use logdb_broker_proto::pb::broker_service_server::BrokerServiceServer;

fn main() {
    let config = load_config();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    if let Err(e) = rt.block_on(run(config)) {
        eprintln!("logdb-broker: fatal: {e}");
        std::process::exit(1);
    }
}

fn load_config() -> BrokerConfig {
    // Initialize tracing first so config-load errors are observable.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    match std::env::var("LOGDB_BROKER_CONFIG") {
        Ok(path) if !path.is_empty() => {
            let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| {
                panic!("failed to read LOGDB_BROKER_CONFIG={path}: {e}");
            });
            serde_yaml::from_str(&raw).unwrap_or_else(|e| {
                panic!("failed to parse broker config {path}: {e}");
            })
        }
        _ => {
            tracing::info!("LOGDB_BROKER_CONFIG unset — using defaults");
            BrokerConfig::default()
        }
    }
}

async fn run(config: BrokerConfig) -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = config.bind_addr.parse()?;
    let num_shards = config.num_shards;
    let logdbd_addr = config.logdbd_addr.clone();

    // Optional Prometheus /metrics endpoint.
    if let Some(metrics_addr) = &config.metrics_addr {
        match metrics_addr.parse::<SocketAddr>() {
            Ok(parsed) => {
                match metrics_exporter_prometheus::PrometheusBuilder::new()
                    .with_http_listener(parsed)
                    .install()
                {
                    Ok(_) => tracing::info!(metrics_addr = %metrics_addr, "Prometheus /metrics endpoint"),
                    Err(e) => tracing::warn!(error = %e, "failed to install Prometheus exporter; metrics disabled"),
                }
            }
            Err(e) => tracing::warn!(error = %e, "metrics_addr '{metrics_addr}' not a valid socket addr; metrics disabled"),
        }
    }

    // Connect to logdbd up front so a misconfigured address fails fast.
    let forwarder = Forwarder::connect(logdbd_addr.clone())
        .await
        .map_err(|e| format!("failed to connect to logdbd at {logdbd_addr}: {e}"))?;
    let persistence = Persistence::connect(logdbd_addr.clone())
        .await
        .map_err(|e| format!("failed to connect persistence to logdbd at {logdbd_addr}: {e}"))?;
    persistence.ensure_meta_stream().await?;

    let registry = Arc::new(CoordinatorRegistry::new(num_shards));
    // Recover committed offsets BEFORE serving so consumers resume correctly.
    let recovered = persistence.scan_offsets().await?;
    for rec in recovered {
        registry.commit_offset(&rec.ns, &rec.stream, &rec.group, rec.shard, rec.seq);
    }
    tracing::info!("recovered committed offsets from logdbd meta stream");

    let svc = BrokerServiceImpl::new(registry, Some(forwarder), Some(persistence));

    tracing::info!(
        bind = %addr,
        num_shards,
        logdbd = %logdbd_addr,
        "logdb-broker serving"
    );

    // Graceful shutdown: stop accepting on SIGTERM/Ctrl-C. Active Consume
    // streams are long-lived; offsets are already event-sourced per commit, so
    // a forced stop after the grace window is data-loss-free. The process exits
    // when run() returns.
    Server::builder()
        .add_service(BrokerServiceServer::new(svc))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("logdb-broker stopped");
    Ok(())
}

/// Resolves on SIGTERM (Unix) or Ctrl-C — whichever fires first.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "ctrl_c signal handler failed");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "SIGTERM handler failed");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("shutdown signal: Ctrl-C"),
        _ = terminate => tracing::info!("shutdown signal: SIGTERM"),
    }
}

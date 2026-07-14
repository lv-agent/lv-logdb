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

    // ── Embedded logdbd (single-process mode) ──────────────────────────────
    // When enabled the broker starts its own in-process logdbd and connects to
    // it — no external process needed.  Development / single-binary deploy.
    let _embedded_guard: Option<EmbeddedLogdbd> = if config.embedded {
        let g = start_embedded_logdbd(&config).await?;
        tracing::info!(addr = %g.addr, "embedded logdbd started");
        Some(g)
    } else {
        None
    };
    let logdbd_addr = if config.embedded {
        let g = _embedded_guard.as_ref().unwrap();
        format!("http://{}", g.addr)
    } else {
        config.logdbd_addr.clone()
    };

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

    // Auto-discover num_shards from logdbd (cr-037 F). Fall back to config
    // if the server is older or the query fails.
    let num_shards = match forwarder.query_num_shards().await {
        Ok(n) if n > 0 => {
            tracing::info!(discovered = n, config = config.num_shards, "using logdbd's num_shards");
            n
        }
        Ok(_) | Err(_) => {
            tracing::info!(
                num_shards = config.num_shards,
                "num_shards from config (logdbd pre-cr-037 or unreachable)"
            );
            config.num_shards
        }
    };
    let persistence = Persistence::connect(logdbd_addr.clone())
        .await
        .map_err(|e| format!("failed to connect persistence to logdbd at {logdbd_addr}: {e}"))?;
    persistence.ensure_meta_stream().await?;

    let registry = Arc::new(CoordinatorRegistry::new(num_shards));
    // Recover committed offsets BEFORE serving so consumers resume correctly.
    // `load_recovered_offsets` handles both offset snapshots (compaction) and
    // per-commit deltas, producing the final max-per-shard state.
    let recovered = persistence.load_recovered_offsets().await?;
    for rec in &recovered {
        registry.commit_offset(&rec.ns, &rec.stream, &rec.group, rec.shard, rec.seq);
    }
    tracing::info!("recovered committed offsets from logdbd meta stream");
    // Compact so the next startup scans fewer individual events.
    if let Err(e) = persistence.compact_offsets(&recovered).await {
        tracing::warn!(error = %e, "failed to compact offset meta stream");
    }

    // Leader election (cr-037 E): elect a single leader across broker instances
    // via logdbd meta stream. Only the leader serves coordination RPCs.
    let leader = std::sync::Arc::new(logdb_broker::leader::LeaderElection::new(
        config.broker_id.clone(),
        config.bind_addr.clone(),
        forwarder
            .channel()
            .clone(),
        None, // use default lease (10 s)
    ));
    leader.start();

    let svc = BrokerServiceImpl::new(
        registry,
        Some(forwarder),
        Some(persistence),
        Some(leader),
    );
    if config.session_timeout_ms > 0 {
        let svc_arc = std::sync::Arc::new(svc.clone());
        svc_arc.start_liveness_check(config.session_timeout_ms);
    }

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

// ── Embedded logdbd (single-process mode) ────────────────────────────────────

struct EmbeddedLogdbd {
    addr: SocketAddr,
}

/// Start an in-process logdbd.  Uses `config.data_dir` (default `./data`) for
/// the log directory.  Returns the bound address for the Forwarder/Persistence.
async fn start_embedded_logdbd(
    config: &BrokerConfig,
) -> Result<EmbeddedLogdbd, Box<dyn std::error::Error>> {
    use logdbd::catalog::Catalog;
    use logdbd::consumer::ConsumerTracker;
    use logdbd::service::LogDbServiceImpl;
    use logdbd::storage::Storage;
    use logdbd::subscribe::SubscribeHub;
    use logdbd_proto::pb::log_db_service_server::LogDbServiceServer;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    let dbg_addr: SocketAddr = config.logdbd_addr.parse()?;
    let data_dir = config
        .data_dir
        .clone()
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));
    std::fs::create_dir_all(&data_dir)?;

    let mut db_config = logdb::Config::default();
    db_config.data_dir = data_dir.clone();
    db_config.shards = config.num_shards as usize;
    db_config.ring_size = 65536;
    db_config.durability_mode = logdb::DurabilityMode::Sync;
    db_config.flush_timeout = std::time::Duration::from_secs(5);
    let db = logdb::LogDb::open(db_config)?;
    let num_shards = config.num_shards as usize;
    let storage = std::sync::Arc::new(Storage::new(db, num_shards));
    let catalog = std::sync::Arc::new(Catalog::open(&data_dir)?);
    let svc = LogDbServiceImpl::new(
        std::sync::Arc::clone(&storage),
        catalog,
        std::sync::Arc::new(ConsumerTracker::new(None)),
        std::sync::Arc::new(SubscribeHub::new()),
        "embedded-logdbd".into(),
        "primary".into(),
    );
    let listener = tokio::net::TcpListener::bind(dbg_addr).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        Server::builder()
            .add_service(LogDbServiceServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    Ok(EmbeddedLogdbd { addr })
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

//! logdbd — clustered log service on gRPC.
//!
//! A node runs as either `primary` (accepts writes, replicates to standbys)
//! or `standby` (read-only locally, receives records from the primary).
//!
//! # Configuration
//!
//! ```bash
//! logdbd --config /etc/logdbd/logdbd.yaml
//! ```
//!
//! Environment variables in the YAML file (`${VAR}`) are substituted at load
//! time. Additionally, specific `LOGDBD_*` env vars can override YAML values
//! for container-friendly deployment.

use std::net::SocketAddr;
use std::sync::Arc;

use logdb::LogDb;
use metrics_exporter_prometheus::PrometheusBuilder;
use tonic::transport::Server;
use tonic::transport::{Identity, ServerTlsConfig};

use logdbd::auth::AuthInterceptor;
use logdbd::catalog::Catalog;
use logdbd::config::Config;
use logdbd::consumer::ConsumerTracker;
use logdbd::node::{NodeIdentity, ProcessLock};
use logdbd::pb::log_db_service_server::LogDbServiceServer;
use logdbd::pb::replication_service_server::ReplicationServiceServer;
use logdbd::pb::snapshot_service_server::SnapshotServiceServer;
use logdbd::replication::{ReplicationServiceImpl, run_primary_sync};
use logdbd::service::LogDbServiceImpl;
use logdbd::snapshot::SnapshotServiceImpl;
use logdbd::storage::Storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse CLI: only flag is --config <path>
    let config_path = parse_args()?;

    // Load and validate configuration
    let config = Config::load(&config_path)?;

    // Node identity
    let node = NodeIdentity::from_config(&config.node);

    // Structured logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(config.observability.log_level.as_str())
            }),
        )
        .with_target(true)
        .try_init();

    // Process lock (primary only)
    let _lock = ProcessLock::acquire(&config.logdb.data_dir, &config.node.role)?;

    let listen: SocketAddr = config.server.bind.parse()?;

    // Security: refuse non-loopback without TLS+auth
    let tls_enabled = config.server.tls.mode != logdbd::config::TlsMode::Disabled;
    let auth_enabled = config.server.auth.token_file.is_some();
    if !listen.ip().is_loopback() && (!tls_enabled || !auth_enabled) {
        if std::env::var("LOGDBD_ALLOW_INSECURE").as_deref() != Ok("1") {
            return Err(format!(
                "refusing to start: non-loopback bind ({}) without TLS+auth. \
                 Configure server.tls and server.auth in the config file, or set \
                 LOGDBD_ALLOW_INSECURE=1 to override (NOT recommended for production).",
                listen
            )
            .into());
        }
    }

    // TLS
    let tls_config = load_server_tls(&config)?;

    // Auth token entries — RBAC with roles
    let mut token_entries: Vec<logdbd::auth::TokenEntry> = Vec::new();

    // Legacy single-token support (admin role)
    if let Some(ref p) = config.server.auth.token_file {
        let token = std::fs::read_to_string(p)
            .map_err(|e| format!("cannot read auth token_file {p}: {e}"))?;
        if token.trim().is_empty() {
            return Err(format!("auth token_file {p} is empty").into());
        }
        token_entries.push(logdbd::auth::TokenEntry {
            token: token.trim().to_string(),
            roles: vec![logdbd::auth::Role::Admin],
        });
    }
    if let Ok(t) = std::env::var("LOGDBD_AUTH_TOKEN") {
        if !t.is_empty() {
            token_entries.push(logdbd::auth::TokenEntry {
                token: t,
                roles: vec![logdbd::auth::Role::Admin],
            });
        }
    }

    // New multi-token RBAC
    for tc in &config.server.auth.tokens {
        let roles: Vec<logdbd::auth::Role> = tc
            .roles
            .iter()
            .filter_map(|r| logdbd::auth::Role::from_str(r))
            .collect();
        if roles.is_empty() {
            return Err(format!("token '{}' has no valid roles", tc.token).into());
        }
        token_entries.push(logdbd::auth::TokenEntry {
            token: tc.token.clone(),
            roles,
        });
    }

    let auth_enabled = !token_entries.is_empty();

    let _tls_ca: Option<Vec<u8>> = match config.server.tls.ca_file.as_ref() {
        Some(p) => {
            let ca = std::fs::read(p).map_err(|e| format!("cannot read TLS CA file {p}: {e}"))?;
            Some(ca)
        }
        None => None,
    };

    // Data directory
    let data_dir = config.logdb.data_dir.clone();
    std::fs::create_dir_all(&data_dir).map_err(|e| {
        format!(
            "cannot create data directory '{}': {}. \
             If running as non-root, set logdb.data_dir to a writable path, e.g.:\n  \
             logdb:\n    data_dir: ./data",
            data_dir.display(),
            e
        )
    })?;

    // Build logdb config from our Config
    let mut db_config = logdb::Config::default();
    db_config.data_dir = data_dir.clone();
    db_config.shards = config.logdb.shards;
    db_config.segment_size = config.logdb.segment_size;
    db_config.ring_size = config.logdb.ring_size;
    db_config.durability_mode = map_durability(config.logdb.durability_mode);
    db_config.flush_timeout = std::time::Duration::from_millis(config.logdb.flush_timeout_ms);
    db_config.hash_enabled = config.audit.hash_chain;
    db_config.compression_enabled = config.storage.compression.enabled;
    db_config.encryption_keys = config
        .storage
        .encryption
        .resolve_key_ring()
        .map_err(|e| format!("encryption config: {}", e))?;

    let num_shards = config.logdb.shards;
    let ckpt_path = data_dir.join("seq_map.ckpt");
    let db_config_fallback = db_config.clone();
    let db = LogDb::open(db_config)
        .map_err(|e| format!("logdb open: {}", e))?;

    tracing::info!(
        listen = %listen,
        node = %node.id,
        cluster_id = %node.cluster_id,
        role = %config.node.role,
        epoch = config.node.epoch,
        tls = tls_enabled,
        auth = auth_enabled,
        standbys = config.replication.standbys.len(),
        "logdbd starting"
    );

    // Catalog — namespace & stream name → ID mapping
    let catalog = Arc::new(Catalog::open(&data_dir).map_err(|e| format!("catalog open: {}", e))?);

    // Storage — try seq-map checkpoint for fast startup, fall back to full rebuild.
    let storage = match logdbd::storage::Storage::try_new_from_checkpoint(db, num_shards, &ckpt_path) {
        Ok(s) => {
            tracing::info!("storage loaded from seq-map checkpoint");
            s
        }
        Err(e) => {
            tracing::warn!(error = %e, "seq-map checkpoint load failed; rebuilding from full scan");
            let db = LogDb::open(db_config_fallback)
                .map_err(|e| format!("logdb reopen for full rebuild: {}", e))?;
            logdbd::storage::Storage::new(db, num_shards)
        }
    };
    let storage = Arc::new(storage);

    // Shared subscriber hub — SubscribePublisher pushes, Subscribe RPC reads
    let subscribe_hub = Arc::new(logdbd::subscribe::SubscribeHub::new());
    let consumer_tracker = Arc::new(ConsumerTracker::new(Some(data_dir.join("offsets"))));
    consumer_tracker.start_flush_loop(std::time::Duration::from_secs(5));

    // Subscribe publisher — chases the durable cursor and fans records out to
    // the hub. The publisher also tracks replicated tombstones on standbys
    // (cr-027 phase 5).
    // Tombstone + quota trackers — seeded once from the committed prefix.
    // Quota is then maintained incrementally.
    let committed = storage.committed_gid();
    let seed_records = storage.scan(0, committed).unwrap_or_else(|e| {
        tracing::warn!(error = %e, "seed scan failed; starting empty");
        Vec::new()
    });
    let tombstone_tracker = Arc::new(
        logdbd::tombstone::TombstoneTracker::rebuild_from_records(&seed_records),
    );
    let quota_tracker = Arc::new(logdbd::quota::QuotaTracker::seed_from_records(
        &seed_records,
        {
            let tt = Arc::clone(&tombstone_tracker);
            move |sid, seq| tt.is_live(sid, seq)
        },
    ));

    let subscribe_publisher = Arc::new(logdbd::publisher::SubscribePublisher::new(
        Arc::clone(&storage),
        Arc::clone(&subscribe_hub),
        Arc::clone(&tombstone_tracker),
    ));
    subscribe_publisher.clone().start();

    let hostname = node.id.clone();
    let role_str = node.role.to_string();
    let quotas = config.limits.quotas.clone();

    let log_svc = LogDbServiceImpl::with_quotas(
        Arc::clone(&storage),
        Arc::clone(&catalog),
        Arc::clone(&consumer_tracker),
        Arc::clone(&subscribe_hub),
        quotas,
        hostname,
        role_str,
        Arc::clone(&quota_tracker),
        Arc::clone(&tombstone_tracker),
    );
    let repl_svc = ReplicationServiceImpl::new(
        Arc::clone(&storage),
        config.node.cluster_id.clone(),
        config.node.epoch,
    );
    let snap_svc = SnapshotServiceImpl::new(data_dir);

    let (mut health_reporter, health_svc) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<LogDbServiceServer<LogDbServiceImpl>>()
        .await;

    // Prometheus /metrics endpoint
    let metrics_addr: Option<SocketAddr> =
        if config.observability.metrics && !config.observability.metrics_bind.is_empty() {
            config.observability.metrics_bind.parse().ok()
        } else {
            None
        };
    if let Some(addr) = metrics_addr {
        match PrometheusBuilder::new()
            .with_http_listener(addr)
            .install_recorder()
        {
            Ok(_) => tracing::info!(metrics_addr = %addr, "Prometheus /metrics endpoint"),
            Err(e) => {
                tracing::warn!(error = %e, "failed to install Prometheus exporter; metrics disabled")
            }
        }
    }

    // Background probe: refresh gauges every 5s
    {
        let probe_db = storage.db_arc();
        let mut hr = health_reporter.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            tick.tick().await;
            loop {
                tick.tick().await;
                probe_db.record_gauges();
                if probe_db.health_code().is_some() {
                    hr.set_not_serving::<LogDbServiceServer<LogDbServiceImpl>>()
                        .await;
                } else {
                    hr.set_serving::<LogDbServiceServer<LogDbServiceImpl>>()
                        .await;
                }
            }
        });
    }

    // Primary: spawn replication to standbys
    if node.is_primary() && !config.replication.standbys.is_empty() {
        tokio::spawn(run_primary_sync(
            Arc::clone(&storage),
            config.replication.clone(),
            config.node.cluster_id.clone(),
            config.node.epoch,
        ));
    }

    let interceptor = if auth_enabled {
        logdbd::auth::AnyInterceptor::Auth(AuthInterceptor::new(&token_entries))
    } else {
        logdbd::auth::AnyInterceptor::NoAuth(logdbd::auth::NoAuthInterceptor)
    };

    let mut builder = Server::builder();
    if let Some(tls) = tls_config {
        builder = builder.tls_config(tls)?;
    }

    let server = builder
        .add_service(LogDbServiceServer::with_interceptor(
            log_svc,
            interceptor.clone(),
        ))
        .add_service(ReplicationServiceServer::with_interceptor(
            repl_svc,
            interceptor.clone(),
        ))
        .add_service(SnapshotServiceServer::with_interceptor(
            snap_svc,
            interceptor,
        ))
        .add_service(health_svc);

    let db_for_drain = storage.db_arc();
    let pub_for_drain = Arc::clone(&subscribe_publisher);
    let offsets_for_drain = Arc::clone(&consumer_tracker);
    server
        .serve_with_shutdown(listen, async move {
            shutdown_signal().await;
            tracing::info!(
                "shutdown signal received; stopping subscribe publisher"
            );
            pub_for_drain.stop();
            if let Err(e) = offsets_for_drain.flush() {
                tracing::warn!(error = %e, "final consumer offset flush failed");
            }
            tracing::info!(
                "draining logdb (flush in-flight to durable, up to 30s)"
            );
            match db_for_drain.drain(std::time::Duration::from_secs(30)) {
                Ok(logdb::ShutdownReport::Clean) => tracing::info!(report = "clean", "drain complete"),
                Ok(r) => tracing::warn!(report = ?r, "drain complete — some data may be only in page cache"),
                Err(e) => tracing::error!(error = ?e, "drain failed — in-flight data may be lost"),
            }
        })
        .await?;

    Ok(())
}

/// Parse `--config <path>` from CLI args.
fn parse_args() -> Result<String, Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && (args[1] == "--help" || args[1] == "-h") {
        eprintln!("Usage: {} --config <path>", args[0]);
        eprintln!();
        eprintln!("Environment variables in the YAML file (${{VAR}}) are substituted.");
        eprintln!("LOGDBD_* env vars override YAML values for container deployments.");
        std::process::exit(0);
    }
    if args.len() == 3 && args[1] == "--config" {
        Ok(args[2].clone())
    } else {
        Err(format!("Usage: {} --config <path>", args[0]).into())
    }
}

/// Map our DurabilityMode to logdb's.
fn map_durability(mode: logdbd::config::DurabilityMode) -> logdb::DurabilityMode {
    match mode {
        logdbd::config::DurabilityMode::Sync => logdb::DurabilityMode::Sync,
        logdbd::config::DurabilityMode::Batch => logdb::DurabilityMode::Batch,
        logdbd::config::DurabilityMode::Async => logdb::DurabilityMode::Async,
    }
}

/// Load server TLS from config.
fn load_server_tls(config: &Config) -> Result<Option<ServerTlsConfig>, Box<dyn std::error::Error>> {
    if config.server.tls.mode == logdbd::config::TlsMode::Disabled {
        return Ok(None);
    }
    match (
        config.server.tls.cert_file.as_ref(),
        config.server.tls.key_file.as_ref(),
    ) {
        (Some(c), Some(k)) => {
            let cert = std::fs::read(c)?;
            let key = std::fs::read(k)?;
            let identity = Identity::from_pem(cert, key);
            Ok(Some(ServerTlsConfig::new().identity(identity)))
        }
        _ => Ok(None),
    }
}

/// Wait for SIGINT / SIGTERM.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
    }
}

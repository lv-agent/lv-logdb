//! logdbd — clustered log service on gRPC.
//!
//! A node runs as either `primary` (accepts writes, replicates to standbys)
//! or `standby` (read-only locally, receives records from the primary).
//!
//! # Environment
//!
//! - `LOGDBD_LISTEN`        — bind address (default `127.0.0.1:50051`).
//!                            Binding a non-loopback address without TLS+auth
//!                            is refused unless `LOGDBD_ALLOW_INSECURE=1`.
//! - `LOGDBD_DATA_DIR`      — logdb data directory (default `/var/lib/logdbd`)
//! - `LOGDBD_ROLE`          — `primary` | `standby` (default `primary`)
//! - `LOGDBD_STANDBYS`      — comma-separated standby addresses for the primary
//!                            to replicate to (primary-only)
//! - `LOGDBD_AUTH_TOKEN`    — if set, every RPC must carry
//!                            `authorization: Bearer <token>` (P0-3)
//! - `LOGDBD_TLS_CERT` / `LOGDBD_TLS_KEY` — PEM paths; if both set, TLS is
//!                            enabled on the server (P0-3)
//! - `LOGDBD_TLS_CA`        — PEM CA the primary trusts for standby TLS (P0-3)
//! - `LOGDBD_MAX_MSG_SIZE`  — max inbound RPC body size in bytes (default 4 MiB)
//! - `HOSTNAME`             — node identity reported via `Status`

use std::sync::Arc;

use logdb::Config;
use logdb::LogDb;
use tonic::transport::Server;
use tonic::transport::{Identity, ServerTlsConfig};

use logdbd::auth::AuthInterceptor;
use logdbd::pb::log_db_service_server::LogDbServiceServer;
use logdbd::pb::replication_service_server::ReplicationServiceServer;
use logdbd::replication::{run_primary_sync, ReplicationServiceImpl};
use logdbd::service::LogDbServiceImpl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Structured logging. Level via RUST_LOG (default info); e.g.
    // RUST_LOG=logdbd=debug,logdb=info to see per-RPC detail.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(true)
        .try_init();

    let listen: std::net::SocketAddr = std::env::var("LOGDBD_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:50051".into())
        .parse()?;
    let data_dir = std::env::var("LOGDBD_DATA_DIR").unwrap_or_else(|_| "/var/lib/logdbd".into());
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
    let role = std::env::var("LOGDBD_ROLE")
        .unwrap_or_else(|_| "primary".into())
        .trim()
        .to_ascii_lowercase();

    let standbys: Vec<String> = std::env::var("LOGDBD_STANDBYS")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    // Security (P0-3).
    let auth_token: Option<String> = std::env::var("LOGDBD_AUTH_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    let tls_config = load_server_tls()?;
    let tls_ca: Option<Vec<u8>> = std::env::var("LOGDBD_TLS_CA")
        .ok()
        .filter(|p| !p.is_empty())
        .and_then(|p| std::fs::read(p).ok());

    // Safety: refuse to expose an unauthenticated / plaintext service on a
    // non-loopback interface unless explicitly overridden.
    if !listen.ip().is_loopback() && (!tls_config.is_some() || auth_token.is_none()) {
        if std::env::var("LOGDBD_ALLOW_INSECURE").as_deref() != Ok("1") {
            return Err(format!(
                "refusing to start: non-loopback bind ({}) without TLS+auth. \
                 Configure LOGDBD_TLS_CERT/KEY and LOGDBD_AUTH_TOKEN, or set \
                 LOGDBD_ALLOW_INSECURE=1 to override (NOT recommended for production).",
                listen
            )
            .into());
        }
    }

    std::fs::create_dir_all(&data_dir)?;

    let mut db_config = Config::default();
    db_config.data_dir = data_dir.into();
    // Replication requires a single linear sequence space (offset-preserving).
    db_config.shards = 1;
    let db = Arc::new(LogDb::open(db_config).map_err(|e| format!("logdb open: {}", e))?);

    tracing::info!(
        listen = %listen,
        node = %hostname,
        role = %role,
        tls = tls_config.is_some(),
        auth = auth_token.is_some(),
        standbys = if standbys.is_empty() { "none".to_string() } else { standbys.join(",") },
        "logdbd v0.2.0 starting"
    );

    let log_svc = LogDbServiceImpl::new(Arc::clone(&db), hostname, role.clone());
    let repl_svc = ReplicationServiceImpl::new(Arc::clone(&db));

    let (mut health_reporter, health_svc) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<LogDbServiceServer<LogDbServiceImpl>>()
        .await;

    // Primary: spawn background replication to standbys (with token + TLS).
    if role == "primary" && !standbys.is_empty() {
        tokio::spawn(run_primary_sync(
            Arc::clone(&db),
            standbys,
            auth_token.clone(),
            tls_ca.clone(),
        ));
    }

    let interceptor = auth_token.as_ref().map(|t| AuthInterceptor::new(t));

    let mut builder = Server::builder();
    if let Some(tls) = tls_config {
        builder = builder.tls_config(tls)?;
    }

    let server = match interceptor {
        Some(i) => builder
            .add_service(LogDbServiceServer::with_interceptor(log_svc, i.clone()))
            .add_service(ReplicationServiceServer::with_interceptor(repl_svc, i))
            .add_service(health_svc),
        None => builder
            .add_service(LogDbServiceServer::new(log_svc))
            .add_service(ReplicationServiceServer::new(repl_svc))
            .add_service(health_svc),
    };

    let db_for_drain = Arc::clone(&db);
    server
        .serve_with_shutdown(listen, async move {
            // Wait for SIGINT (ctrl-c) or SIGTERM (container stop), then drain
            // logdb: flush all in-flight records to durable storage BEFORE the
            // gRPC server stops and the process exits. Without this, the LogDb
            // drop would abort the Committer and lose in-flight data (P1/L4).
            shutdown_signal().await;
            tracing::info!("shutdown signal received; draining logdb (flush in-flight to durable, up to 30s)");
            match db_for_drain.drain(std::time::Duration::from_secs(30)) {
                Ok(logdb::ShutdownReport::Clean) => tracing::info!(report = "clean", "drain complete"),
                Ok(r) => tracing::warn!(report = ?r, "drain complete — some data may be only in page cache"),
                Err(e) => tracing::error!(error = ?e, "drain failed — in-flight data may be lost"),
            }
        })
        .await?;

    Ok(())
}

/// Wait for a process shutdown signal (SIGINT on all platforms, SIGTERM on Unix).
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

/// Load server TLS config from `LOGDBD_TLS_CERT` / `LOGDBD_TLS_KEY` if both set.
fn load_server_tls() -> Result<Option<ServerTlsConfig>, Box<dyn std::error::Error>> {
    let cert_path = std::env::var("LOGDBD_TLS_CERT")
        .ok()
        .filter(|s| !s.is_empty());
    let key_path = std::env::var("LOGDBD_TLS_KEY")
        .ok()
        .filter(|s| !s.is_empty());
    match (cert_path, key_path) {
        (Some(c), Some(k)) => {
            let cert = std::fs::read(&c)?;
            let key = std::fs::read(&k)?;
            let identity = Identity::from_pem(cert, key);
            Ok(Some(ServerTlsConfig::new().identity(identity)))
        }
        _ => Ok(None),
    }
}

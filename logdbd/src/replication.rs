//! Primary-standby replication via gRPC.
//!
//! # Primary
//!
//! Reads durable records from Storage and pushes to all configured standbys.
//! In `sync` mode the push loop waits for standby acks; in `async` mode it
//! fires-and-forgets. Caches gRPC clients and reconnects on failure.
//!
//! # Standby
//!
//! Validates `cluster_id` and `epoch` on every request, preventing cross-cluster
//! or stale-primary corruption. Writes records at the primary's exact gid,
//! decodes headers to rebuild the Storage seq→gid mapping.

use std::sync::Arc;
use std::time::Duration;

use crate::config::{OnSyncTimeout, ReplicationConfig, SyncPolicy};
use crate::pb;
use crate::pb::replication_service_server::ReplicationService;
use crate::storage::Storage;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Response, Status};

// ── Standby handler ───────────────────────────────────────────────────────────

pub struct ReplicationServiceImpl {
    storage: Arc<Storage>,
    cluster_id: String,
    epoch: u64,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl ReplicationServiceImpl {
    pub fn new(storage: Arc<Storage>, cluster_id: String, epoch: u64) -> Self {
        Self { storage, cluster_id, epoch, lock: Arc::new(tokio::sync::Mutex::new(())) }
    }
}

#[tonic::async_trait]
impl ReplicationService for ReplicationServiceImpl {
    async fn sync(
        &self,
        req: Request<pb::ReplicationRequest>,
    ) -> Result<Response<pb::ReplicationResponse>, Status> {
        let r = req.get_ref();

        // Validate cluster_id and epoch (C3 fix)
        if !r.cluster_id.is_empty() && r.cluster_id != self.cluster_id {
            return Err(Status::failed_precondition(format!(
                "WRONG_CLUSTER: got '{}', expected '{}'",
                r.cluster_id, self.cluster_id
            )));
        }
        if r.epoch > 0 && r.epoch < self.epoch {
            return Err(Status::failed_precondition(format!(
                "STALE_EPOCH: got {}, local {}",
                r.epoch, self.epoch
            )));
        }

        if r.records.is_empty() {
            return Ok(Response::new(pb::ReplicationResponse { last_gid: 0 }));
        }

        let _guard = self.lock.lock().await;

        let mut last_gid: u64 = 0;
        for rec in &r.records {
            self.storage.replicate(rec.gid, rec.timestamp_ns, &rec.content)
                .map_err(|e| Status::internal(format!("replicate gid={}: {}", rec.gid, e)))?;
            last_gid = rec.gid;
        }

        Ok(Response::new(pb::ReplicationResponse { last_gid }))
    }
}

// ── Primary push loop ─────────────────────────────────────────────────────────

/// Push durable records to all standbys. Respects sync/async mode,
/// sync_policy, and sync_timeout from config (C1 fix).
pub async fn run_primary_sync(
    storage: Arc<Storage>,
    repl_config: ReplicationConfig,
    cluster_id: String,
    epoch: u64,
) {
    let mut clients: Vec<(
        String,
        pb::replication_service_client::ReplicationServiceClient<Channel>,
        Option<String>,
    )> = Vec::new();
    for s in &repl_config.standbys {
        let tls_ca = s.tls.ca_file.as_ref().and_then(|p| std::fs::read(p).ok());
        let token = s.auth_token_file.as_ref().and_then(|p| std::fs::read_to_string(p).ok())
            .map(|t| t.trim().to_string()).filter(|t| !t.is_empty());
        match connect_standby(&s.addr, tls_ca.as_deref()).await {
            Some(channel) => {
                clients.push((s.addr.clone(), pb::replication_service_client::ReplicationServiceClient::new(channel), token));
            }
            None => {
                tracing::warn!(addr = %s.addr, "initial standby connection failed");
            }
        }
    }

    let mut push_seq: u64 = 0;
    let mut reconnect_backoff = Duration::from_millis(500);
    let sync_required = repl_config.mode == crate::config::ReplicationMode::Sync;

    loop {
        tokio::time::sleep(Duration::from_millis(50)).await;

        let durable = storage.durable_gid();
        if durable <= push_seq {
            continue;
        }

        let records = match read_durable_batch(&storage, push_seq, durable) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "replication read error");
                continue;
            }
        };
        if records.is_empty() {
            continue;
        }

        let req = pb::ReplicationRequest {
            cluster_id: cluster_id.clone(),
            epoch,
            records: records.clone(),
        };

        let sync_timeout = Duration::from_millis(repl_config.sync_timeout_ms);

        // Push to each standby
        let mut acked_count: usize = 0;
        let mut last_acked: u64 = push_seq;
        let total = clients.len();
        if total == 0 {
            let last = records.last().unwrap().gid;
            push_seq = last;
            storage.advance_replicated(last);
            continue;
        }

        for (addr, client, token) in &mut clients {
            let mut grpc_req = tonic::Request::new(req.clone());
            if let Some(t) = token {
                if let Ok(val) = tonic::metadata::MetadataValue::try_from(format!("Bearer {}", t)) {
                    grpc_req.metadata_mut().insert("authorization", val);
                }
            }
            let result = if sync_required {
                tokio::time::timeout(sync_timeout, client.sync(grpc_req)).await
            } else {
                Ok(client.sync(grpc_req).await)
            };

            match result {
                Ok(Ok(resp)) => {
                    last_acked = last_acked.max(resp.into_inner().last_gid);
                    acked_count += 1;
                }
                Ok(Err(e)) => {
                    tracing::warn!(addr = %addr, error = %e, "push failed, reconnecting");
                    if let Some(ch) = connect_standby(addr, repl_config.standbys.iter()
                        .find(|s| &s.addr == addr)
                        .and_then(|s| s.tls.ca_file.as_ref())
                        .and_then(|p| std::fs::read(p).ok()).as_deref()
                    ).await {
                        *client = pb::replication_service_client::ReplicationServiceClient::new(ch);
                        // keep the existing token
                    }
                }
                Err(_timeout) => {
                    tracing::warn!(addr = %addr, timeout_ms = repl_config.sync_timeout_ms, "sync push timed out");
                }
            }
        }

        // Decide whether to advance push_seq based on sync_policy
        let required = match repl_config.sync_policy {
            SyncPolicy::All => total,
            SyncPolicy::Quorum => total / 2 + 1,
            SyncPolicy::N => (repl_config.required_acks as usize).min(total),
        };

        if !sync_required || acked_count >= required {
            if last_acked > push_seq {
                push_seq = last_acked;
                storage.advance_replicated(push_seq);
            }
            reconnect_backoff = Duration::from_millis(500);
        } else if repl_config.on_sync_timeout == OnSyncTimeout::Fail {
            // Don't advance push_seq; records will be retried
            tokio::time::sleep(reconnect_backoff).await;
            reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(30));
        } else if repl_config.on_sync_timeout == OnSyncTimeout::AsyncWarn {
            tracing::warn!(acked = acked_count, required, "sync degraded: advancing despite insufficient acks");
            if last_acked > push_seq {
                push_seq = last_acked;
                storage.advance_replicated(push_seq);
            }
        }
        // Block case: loop continues, retrying same batch
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn connect_standby(addr: &str, tls_ca: Option<&[u8]>) -> Option<Channel> {
    if let Some(ca) = tls_ca {
        let tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca.to_vec()));
        let uri: tonic::transport::Uri = match format!("https://{}", addr).parse() {
            Ok(u) => u,
            Err(e) => { tracing::warn!(addr = %addr, error = %e, "invalid URI"); return None; }
        };
        match Endpoint::from(uri).tls_config(tls) {
            Ok(ep) => Some(ep.connect_lazy()),
            Err(e) => { tracing::warn!(addr = %addr, error = %e, "TLS config"); None }
        }
    } else {
        let uri: tonic::transport::Uri = match format!("http://{}", addr).parse() {
            Ok(u) => u,
            Err(_) => return None,
        };
        Some(Channel::builder(uri).connect_lazy())
    }
}

fn read_durable_batch(
    storage: &Storage,
    from_gid: u64,
    to_gid: u64,
) -> Result<Vec<pb::ReplicationRecord>, String> {
    let iter = storage.db_arc().scan(from_gid, to_gid)
        .map_err(|e| format!("scan: {:?}", e))?;
    let mut records = Vec::new();
    for r in iter {
        let rec = r.map_err(|e| format!("scan iter: {:?}", e))?;
        records.push(pb::ReplicationRecord {
            gid: rec.id.sequence,
            timestamp_ns: rec.timestamp_ns,
            content: rec.content,
        });
    }
    Ok(records)
}

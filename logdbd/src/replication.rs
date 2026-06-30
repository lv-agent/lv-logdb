//! Primary-standby replication via gRPC.
//!
//! Primary: spawns a background task that reads durable records from logdb
//! and pushes them to standby nodes via ReplicationService::Sync RPC.
//!
//! Standby: implements ReplicationService, receives records from primary
//! and writes them to local logdb at the PRIMARY's exact sequence using
//! [`LogDb::replicate`](logdb::LogDb::replicate), preserving the global
//! offset space so consumers can fail over primary → standby.
//!
//! ## Consistency
//!
//! The primary advances its push cursor only once *every* configured standby
//! has acknowledged a batch. A transient standby failure therefore stalls
//! replication (no data loss) until the standby recovers; `replicate` is
//! idempotent, so replayed batches are safe.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Response, Status};

use crate::pb;
use crate::pb::replication_service_server::ReplicationService;
use logdb::LogDb;

pub struct ReplicationServiceImpl {
    db: Arc<LogDb>,
    /// Serializes concurrent Sync RPCs so replicate() sees a single writer.
    /// Required: replicate() advances producer_cursor under an in-order
    /// assumption that would be violated by interleaved batches.
    replicate_lock: Arc<Mutex<()>>,
}

impl ReplicationServiceImpl {
    pub fn new(db: Arc<LogDb>) -> Self {
        Self {
            db,
            replicate_lock: Arc::new(Mutex::new(())),
        }
    }
}

#[tonic::async_trait]
impl ReplicationService for ReplicationServiceImpl {
    async fn sync(
        &self,
        req: Request<pb::ReplicationRequest>,
    ) -> Result<Response<pb::ReplicationResponse>, Status> {
        let records = &req.get_ref().records;
        if records.is_empty() {
            return Ok(Response::new(pb::ReplicationResponse { last_sequence: 0 }));
        }

        // Serialize: only one batch applies records at a time.
        let _lock = self.replicate_lock.lock().await;

        let mut last_seq: u64 = 0;
        for rec in records {
            self.db
                .replicate(rec.sequence, rec.timestamp_ns, &rec.content)
                .map_err(|e| {
                    Status::internal(format!("replicate seq={} failed: {:?}", rec.sequence, e))
                })?;
            last_seq = rec.sequence;
        }
        Ok(Response::new(pb::ReplicationResponse {
            last_sequence: last_seq,
        }))
    }
}

/// Run primary-side replication: periodically push durable records to standbys.
///
/// The push cursor advances only when ALL standbys acknowledge a batch,
/// guaranteeing no record is skipped on any standby. On failure, the same
/// batch is retried next cycle (idempotent on the standby).
///
/// `auth_token` is sent as `authorization: Bearer <token>` so an authenticated
/// standby accepts the Sync RPC. `tls_ca` (PEM) enables TLS to standbys,
/// trusting that CA (P0-3).
pub async fn run_primary_sync(
    db: Arc<LogDb>,
    standby_addrs: Vec<String>,
    auth_token: Option<String>,
    tls_ca: Option<Vec<u8>>,
) {
    if standby_addrs.is_empty() {
        return;
    }

    // Pre-build the per-cycle auth header value once.
    let bearer = auth_token.as_ref().map(|t| format!("Bearer {}", t));

    // Build a persistent, reusable channel to each standby ONCE. tonic's Channel
    // multiplexes over a single HTTP/2 connection and auto-reconnects on
    // transient failure, so we avoid a fresh TCP+TLS handshake every 100ms.
    // `None` means the channel could not even be built (misconfigured address)
    // — that target is treated as perpetually failing so push_seq never advances
    // past it (no silent data loss for a misconfigured standby).
    let channels: Vec<Option<Channel>> = standby_addrs
        .iter()
        .map(|addr| build_channel(addr, tls_ca.as_deref()))
        .collect();

    let mut push_seq = db.durable_cursor();
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;

        let durable = db.durable_cursor();
        if durable <= push_seq {
            continue;
        }

        // Collect durable records not yet pushed.
        let iter = match db.scan(push_seq, durable) {
            Ok(i) => i,
            Err(_) => continue,
        };
        let records: Vec<pb::Record> = iter
            .filter_map(|r| r.ok())
            .map(|r| pb::Record {
                sequence: r.id.sequence,
                timestamp_ns: r.timestamp_ns,
                content: r.content,
            })
            .collect();
        if records.is_empty() {
            continue;
        }

        // Push to ALL standbys IN PARALLEL so one slow/unreachable standby
        // cannot stall replication to the others. Advance push_seq only when
        // every target acknowledged.
        let mut set = tokio::task::JoinSet::new();
        let mut all_ok = true;
        for ch in &channels {
            match ch {
                Some(channel) => {
                    let channel = channel.clone();
                    let records = records.clone();
                    let bearer = bearer.clone();
                    set.spawn(async move {
                        push_via_channel(channel, &records, bearer.as_deref()).await
                    });
                }
                None => all_ok = false, // misconfigured target → block
            }
        }
        while let Some(res) = set.join_next().await {
            if !res.unwrap_or(false) {
                all_ok = false;
            }
        }

        if all_ok {
            let advanced_to = records.last().unwrap().sequence + 1;
            tracing::debug!(
                from = push_seq,
                to = advanced_to,
                records = records.len(),
                "replicated to all standbys"
            );
            push_seq = advanced_to;
        } else {
            tracing::warn!(
                from = push_seq,
                records = records.len(),
                "replication: not all standbys acknowledged, will retry"
            );
        }
        // else: retry the same batch next cycle.
    }
}

/// Build a persistent channel to a standby. Uses `connect_lazy` so startup
/// doesn't block on a standby that's temporarily down; the first RPC triggers
/// the connection and tonic reconnects automatically on later failure.
fn build_channel(addr: &str, tls_ca: Option<&[u8]>) -> Option<Channel> {
    let scheme = if tls_ca.is_some() { "https" } else { "http" };
    let endpoint = Endpoint::from_shared(format!("{}://{}", scheme, addr)).ok()?;
    let endpoint = match tls_ca {
        Some(ca) => {
            // domain_name = host portion of the address (before the port).
            let domain = addr.split(':').next().unwrap_or("localhost").to_string();
            let tls = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(ca))
                .domain_name(domain);
            endpoint.tls_config(tls).ok()?
        }
        None => endpoint,
    };
    Some(endpoint.connect_lazy())
}

/// Push a batch over an existing (reused) channel. Returns true on success.
async fn push_via_channel(channel: Channel, records: &[pb::Record], bearer: Option<&str>) -> bool {
    let mut client = pb::replication_service_client::ReplicationServiceClient::new(channel);
    let mut req = Request::new(pb::ReplicationRequest {
        records: records.to_vec(),
    });
    if let Some(b) = bearer {
        if let Ok(v) = b.parse() {
            req.metadata_mut().insert("authorization", v);
        }
    }
    client.sync(req).await.is_ok()
}

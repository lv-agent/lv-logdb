//! Per-group leader election via logdbd meta stream (cr-037 E).
//!
//! Each `(namespace, stream, group)` elects its own leader by atomically
//! appending `leader_claim` events to the meta stream — logdbd's append
//! provides the linearizability (no external Raft/ZK). Different groups can
//! be led by different brokers (load distribution).
//!
//! The elected broker periodically re-posts its claim (heartbeat). Standbys
//! watch the meta stream; when a group's leader claim goes stale (lease
//! timeout), a standby appends a new claim with `epoch + 1` and takes over.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use logdbd_proto::pb::log_db_service_client::LogDbServiceClient;
use logdbd_proto::pb::{AppendRequest, ScanRequest};
use tonic::Status;

use crate::coordinator::GroupKey;
use crate::persistence::{META_NAMESPACE, META_STREAM};

const EVENT_LEADER_CLAIM: &str = "leader_claim";
const DEFAULT_LEASE_MS: u64 = 10_000;
const HEARTBEAT_DIVISOR: u64 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaderClaim {
    broker_id: String,
    address: String,
    epoch: u64,
    timestamp_ms: u64,
    /// Per-group: "ns/stream/group".  Absent ⇒ global (legacy / single-broker).
    #[serde(default)]
    group: Option<String>,
}

/// The broker whose claim currently holds for a group.
#[derive(Debug, Clone)]
pub struct LeaderInfo {
    pub broker_id: String,
    pub address: String,
    pub epoch: u64,
}

/// Per-group leader state.
#[derive(Debug, Clone)]
enum GroupState {
    /// We hold the lease for this group.
    Leader { epoch: u64 },
    /// Another broker is the leader.
    Standby { leader: LeaderInfo, last_claim_ms: u64 },
}

/// Per-group leader election.  Tracks leadership for every group seen in
/// the meta stream.  Call [`LeaderElection::start`] once after construction;
/// then query [`LeaderElection::require_leader`] per RPC.
pub struct LeaderElection {
    broker_id: String,
    address: String,
    channel: Channel,
    lease_ms: u64,
    state: RwLock<HashMap<GroupKey, GroupState>>,
    running: AtomicBool,
}

impl LeaderElection {
    pub fn new(broker_id: String, address: String, channel: Channel, lease_ms: Option<u64>) -> Self {
        Self {
            broker_id,
            address,
            channel,
            lease_ms: lease_ms.unwrap_or(DEFAULT_LEASE_MS),
            state: RwLock::new(HashMap::new()),
            running: AtomicBool::new(false),
        }
    }

    /// Stop the background election loop (simulates a broker crash in tests).
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Launch the background election loop.
    pub fn start(self: &Arc<Self>) {
        self.running.store(true, Ordering::Release);
        let this = Arc::clone(self);
        tokio::spawn(async move { this.run().await });
    }

    /// Check whether we are the leader for `group`.  Returns `Err` with the
    /// current leader's address if we are standby (or unknown).
    pub fn require_leader(&self, key: &GroupKey) -> Result<(), Status> {
        let s = self.state.read().unwrap();
        match s.get(key) {
            Some(GroupState::Leader { .. }) => Ok(()),
            Some(GroupState::Standby { leader, .. }) => Err(Status::new(
                tonic::Code::Unavailable,
                format!("not the leader; leader is at {}", leader.address),
            )),
            None => {
                // Unknown group — we haven't seen a claim for it yet.  Accept
                // the RPC (first broker to serve the group becomes leader on
                // the next scan cycle).  This handles the single-broker case
                // and the first request after startup.
                Ok(())
            }
        }
    }

    /// Whether we are the leader for `group`.
    pub fn is_leader(&self, key: &GroupKey) -> bool {
        let s = self.state.read().unwrap();
        matches!(s.get(key), Some(GroupState::Leader { .. }))
    }

    // ── internals ───────────────────────────────────────────────────────────

    async fn run(&self) {
        // Prime: scan once to discover existing claims.
        self.scan_and_reconcile().await;
        let interval = Duration::from_millis(self.lease_ms / HEARTBEAT_DIVISOR);
        loop {
            tokio::time::sleep(interval).await;
            if !self.running.load(Ordering::Acquire) {
                return;
            }
            self.scan_and_reconcile().await;
        }
    }

    async fn scan_and_reconcile(&self) {
        let claims = match self.scan_claims().await {
            Some(c) => c,
            None => return,
        };

        // Group claims by (ns, stream, group).
        let mut by_group: HashMap<GroupKey, Vec<LeaderClaim>> = HashMap::new();
        for c in &claims {
            if let Some(g) = &c.group {
                let parts: Vec<&str> = g.splitn(3, '/').collect();
                if parts.len() == 3 {
                    let key = GroupKey::new(parts[0], parts[1], parts[2]);
                    by_group.entry(key).or_default().push(c.clone());
                }
            }
        }

        let mut new_state: HashMap<GroupKey, GroupState> = HashMap::new();

        for (key, group_claims) in &by_group {
            // Find the latest claim (highest epoch, tie-break timestamp).
            let latest = group_claims
                .iter()
                .max_by_key(|c| (c.epoch, c.timestamp_ms))
                .unwrap();

            let age = now_ms().saturating_sub(latest.timestamp_ms);
            if latest.broker_id == self.broker_id {
                // We hold the latest claim.
                if age <= self.lease_ms {
                    new_state.insert(key.clone(), GroupState::Leader {
                        epoch: latest.epoch,
                    });
                    // Re-post as heartbeat.
                    let _ = self.append_claim(Some(key), latest.epoch).await;
                }
                // else: our claim is stale, will try to claim below.
            } else if age <= self.lease_ms {
                // Another broker's claim is fresh.
                new_state.insert(key.clone(), GroupState::Standby {
                    leader: LeaderInfo {
                        broker_id: latest.broker_id.clone(),
                        address: latest.address.clone(),
                        epoch: latest.epoch,
                    },
                    last_claim_ms: latest.timestamp_ms,
                });
            }
            // If the latest claim is stale (age > lease_ms), we leave this
            // group out of new_state and try to claim it below.
        }

        // Try to claim groups without a fresh leader.
        for key in by_group.keys() {
            if !new_state.contains_key(key) {
                let epoch = 1u64; // will be bumped if we saw an existing claim
                // Find the highest epoch seen for this group.
                if let Some(claims) = by_group.get(key) {
                    if let Some(max_epoch) = claims.iter().map(|c| c.epoch).max() {
                        let next = max_epoch.saturating_add(1);
                        if self.append_claim(Some(key), next).await.is_ok() {
                            new_state.insert(key.clone(), GroupState::Leader {
                                epoch: next,
                            });
                            tracing::info!(
                                broker_id = %self.broker_id, group = %format!("{}/{}/{}", key.namespace, key.stream, key.group),
                                epoch = next, "claimed leadership for group"
                            );
                            continue;
                        }
                    }
                }
                // Try epoch 1 as fallback.
                if self.append_claim(Some(key), 1).await.is_ok() {
                    new_state.insert(key.clone(), GroupState::Leader { epoch: 1 });
                    tracing::info!(
                        broker_id = %self.broker_id, group = %format!("{}/{}/{}", key.namespace, key.stream, key.group),
                        "claimed leadership for group (epoch 1)"
                    );
                }
            }
        }

        // Heartbeat for groups we're already the leader of.
        for (key, st) in &new_state {
            if let GroupState::Leader { epoch } = st {
                let _ = self.append_claim(Some(key), *epoch).await;
            }
        }

        *self.state.write().unwrap() = new_state;
    }

    async fn append_claim(&self, group: Option<&GroupKey>, epoch: u64) -> Result<(), Status> {
        let claim = LeaderClaim {
            broker_id: self.broker_id.clone(),
            address: self.address.clone(),
            epoch,
            timestamp_ms: now_ms(),
            group: group.map(|k| format!("{}/{}/{}", k.namespace, k.stream, k.group)),
        };
        let content = serde_json::to_vec(&claim)
            .map_err(|e| Status::internal(format!("encode leader claim: {e}")))?;
        let mut client = LogDbServiceClient::new(self.channel.clone());
        client
            .append(AppendRequest {
                namespace: META_NAMESPACE.into(),
                stream: META_STREAM.into(),
                event_type: EVENT_LEADER_CLAIM.into(),
                content_type: "application/json".into(),
                content,
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Scan the meta stream for all `leader_claim` events.
    async fn scan_claims(&self) -> Option<Vec<LeaderClaim>> {
        let mut client = LogDbServiceClient::new(self.channel.clone());
        let mut stream = client
            .scan(ScanRequest {
                namespace: META_NAMESPACE.into(),
                stream: META_STREAM.into(),
                from_seq: 0,
                to_seq: 0,
                limit: 0,
            })
            .await
            .ok()?
            .into_inner();
        let mut out = Vec::new();
        while let Ok(Some(resp)) = stream.message().await {
            for r in resp.records {
                if r.event_type == EVENT_LEADER_CLAIM {
                    if let Ok(c) = serde_json::from_slice::<LeaderClaim>(&r.content) {
                        out.push(c);
                    }
                }
            }
        }
        Some(out)
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

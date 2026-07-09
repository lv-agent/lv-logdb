//! gRPC `BrokerService` implementation.
//!
//! Membership/assignment handlers (Phase 2) delegate to a shared
//! [`CoordinatorRegistry`]. `Consume` (Phase 3) opens a per-consumer Tail to
//! logdbd via the [`Forwarder`] and streams records back — the symmetric data
//! path. Phase 5 adds `Heartbeat`/`CommitShardOffset` + rebalance signals.

use std::sync::Arc;

use logdb_broker_proto::pb::broker_service_server::BrokerService;
use logdb_broker_proto::pb::consume_response::Payload as ConsumePayload;
use logdb_broker_proto::pb::{
    Assignment, BatchProduceRequest, BatchProduceResponse, CommitShardOffsetRequest, CommitShardOffsetResponse,
    ConsumeRequest, ConsumeResponse, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, MemberInfo, ListMembersRequest,
    ListMembersResponse, ProduceRequest, ProduceResponse, RebalanceSignal,
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::coordinator::{CoordinatorRegistry, GroupKey};
use crate::forwarder::Forwarder;
use crate::persistence::{OffsetRecord, Persistence};
use crate::sessions::{SessionHandle, Sessions};

/// The broker gRPC service.
///
/// `forwarder` is `None` when no logdbd is configured (membership-only test
/// setups) — `Consume`/`Produce` then return `UNIMPLEMENTED`. `persistence`
/// is `None` when offset durability is disabled — commits stay in-memory only.
/// `leader` is `None` in single-broker mode (all RPCs served). When configured
/// (cr-037 E), only the leader serves coordination RPCs; standbys return
/// `UNAVAILABLE` with the leader address for redirect.
#[derive(Clone)]
pub struct BrokerServiceImpl {
    registry: Arc<CoordinatorRegistry>,
    forwarder: Option<Forwarder>,
    persistence: Option<Persistence>,
    sessions: Arc<Sessions>,
    leader: Option<Arc<crate::leader::LeaderElection>>,
}

impl BrokerServiceImpl {
    pub fn new(
        registry: Arc<CoordinatorRegistry>,
        forwarder: Option<Forwarder>,
        persistence: Option<Persistence>,
        leader: Option<Arc<crate::leader::LeaderElection>>,
    ) -> Self {
        Self {
            registry,
            forwarder,
            persistence,
            sessions: Arc::new(Sessions::new()),
            leader,
        }
    }

    /// Return `UNAVAILABLE` with the leader address if we are a standby
    /// for this group.  Single-broker mode (`leader` is `None`) always OK.
    fn require_leader(&self, key: &GroupKey) -> Result<(), Status> {
        match &self.leader {
            None => Ok(()),
            Some(e) => e.require_leader(key),
        }
    }
}

#[tonic::async_trait]
impl BrokerService for BrokerServiceImpl {
    async fn join_group(
        &self,
        req: Request<JoinGroupRequest>,
    ) -> Result<Response<JoinGroupResponse>, Status> {
        let key = GroupKey::new(&req.get_ref().namespace, &req.get_ref().stream, &req.get_ref().group);
        self.require_leader(&key)?;
        let r = req.into_inner();
        if r.consumer_id.is_empty() || r.group.is_empty() {
            return Err(Status::invalid_argument(
                "consumer_id and group are required",
            ));
        }
        let result = self
            .registry
            .join(&r.namespace, &r.stream, &r.group, &r.consumer_id);
        metrics::counter!("broker.joins").increment(1);
        tracing::info!(
            ns = %r.namespace,
            stream = %r.stream,
            group = %r.group,
            consumer = %r.consumer_id,
            generation = result.generation,
            "consumer joined"
        );
        // A join changes membership → stop-the-world rebalance of open streams.
        self.rebalance_group(&r.namespace, &r.stream, &r.group).await;
        Ok(Response::new(JoinGroupResponse {
            generation: result.generation,
            num_shards: self.registry.num_shards(),
            assigned_shards: result.assigned_shards,
            // Populated from persisted offsets in Phase 6.
            initial_offsets: Default::default(),
        }))
    }

    async fn leave_group(
        &self,
        req: Request<LeaveGroupRequest>,
    ) -> Result<Response<LeaveGroupResponse>, Status> {
        let key = GroupKey::new(&req.get_ref().namespace, &req.get_ref().stream, &req.get_ref().group);
        self.require_leader(&key)?;
        let r = req.into_inner();
        let ok = self
            .registry
            .leave(&r.namespace, &r.stream, &r.group, &r.consumer_id);
        if ok {
            metrics::counter!("broker.leaves").increment(1);
            // A leave changes membership → rebalance the remaining open streams.
            self.rebalance_group(&r.namespace, &r.stream, &r.group).await;
        }
        Ok(Response::new(LeaveGroupResponse { ok }))
    }

    async fn list_members(
        &self,
        req: Request<ListMembersRequest>,
    ) -> Result<Response<ListMembersResponse>, Status> {
        let r = req.into_inner();
        match self
            .registry
            .group_snapshot(&r.namespace, &r.stream, &r.group)
        {
            Some(snap) => Ok(Response::new(ListMembersResponse {
                generation: snap.generation,
                members: snap
                    .members
                    .into_iter()
                    .map(|(consumer_id, assigned_shards)| MemberInfo {
                        consumer_id,
                        assigned_shards,
                    })
                    .collect(),
            })),
            None => Err(Status::not_found("group not found")),
        }
    }

    type ConsumeStream = tokio_stream::wrappers::ReceiverStream<Result<ConsumeResponse, Status>>;

    async fn consume(
        &self,
        req: Request<ConsumeRequest>,
    ) -> Result<Response<Self::ConsumeStream>, Status> {
        let key = GroupKey::new(&req.get_ref().namespace, &req.get_ref().stream, &req.get_ref().group);
        self.require_leader(&key)?;
        let r = req.into_inner();
        if r.consumer_id.is_empty() || r.group.is_empty() {
            return Err(Status::invalid_argument(
                "consumer_id and group are required",
            ));
        }

        if self.forwarder.is_none() {
            return Err(Status::unimplemented(
                "data forwarding disabled (no logdbd configured)",
            ));
        }

        // Resolve the consumer's CURRENT assignment + verify its generation.
        let snap = self
            .registry
            .group_snapshot(&r.namespace, &r.stream, &r.group)
            .ok_or_else(|| {
                Status::failed_precondition("not a member of the group; JoinGroup first")
            })?;
        if snap.generation != r.generation {
            return Err(Status::failed_precondition(format!(
                "generation mismatch: client={} group={} (a rebalance happened; rejoin)",
                r.generation, snap.generation
            )));
        }
        let assigned: Vec<u32> = snap
            .members
            .iter()
            .find(|(id, _)| id == &r.consumer_id)
            .map(|(_, shards)| shards.clone())
            .ok_or_else(|| Status::not_found("consumer not in group"))?;
        if assigned.is_empty() {
            return Err(Status::failed_precondition(
                "consumer has no assigned shards (more consumers than shards)",
            ));
        }

        // Register an open session for this consumer and start its forward task.
        // The session is reused across rebalances (its forward task is swapped);
        // it self-removes when the consumer disconnects (forward's tx fails).
        let (tx, rx) = mpsc::channel(16);
        let session = SessionHandle::new(tx);
        let key = GroupKey::new(&r.namespace, &r.stream, &r.group);
        self.sessions
            .insert(key.clone(), r.consumer_id.clone(), session.clone());
        metrics::counter!("broker.consume_sessions").increment(1);
        self.spawn_forward(
            session,
            key,
            r.consumer_id,
            r.namespace,
            r.stream,
            assigned,
        );
        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn produce(
        &self,
        req: Request<ProduceRequest>,
    ) -> Result<Response<ProduceResponse>, Status> {
        let r = req.into_inner();
        let forwarder = self.forwarder.clone().ok_or_else(|| {
            Status::unimplemented("produce disabled (no logdbd configured)")
        })?;
        // Map the broker's independent Produce schema → logdbd.AppendRequest.
        let append_req = logdbd_proto::pb::AppendRequest {
            namespace: r.namespace,
            stream: r.stream,
            event_type: r.event_type,
            timestamp_ns: r.timestamp_ns,
            content_type: r.content_type,
            metadata: r.metadata,
            content: r.content,
            shard_key: r.shard_key,
        };
        let resp = forwarder.append(append_req).await?;
        Ok(Response::new(ProduceResponse {
            gid: resp.gid,
            seq: resp.seq,
        }))
    }

    async fn batch_produce(
        &self,
        req: Request<BatchProduceRequest>,
    ) -> Result<Response<BatchProduceResponse>, Status> {
        let r = req.into_inner();
        let forwarder = self.forwarder.clone().ok_or_else(|| {
            Status::unimplemented("batch produce disabled (no logdbd configured)")
        })?;
        let append_reqs: Vec<logdbd_proto::pb::AppendRequest> = r
            .requests
            .into_iter()
            .map(|p| logdbd_proto::pb::AppendRequest {
                namespace: p.namespace,
                stream: p.stream,
                event_type: p.event_type,
                timestamp_ns: p.timestamp_ns,
                content_type: p.content_type,
                metadata: p.metadata,
                content: p.content,
                shard_key: p.shard_key,
            })
            .collect();
        let resps = forwarder.append_batch(append_reqs).await?;
        let records: Vec<ProduceResponse> = resps
            .into_iter()
            .map(|a| ProduceResponse { gid: a.gid, seq: a.seq })
            .collect();
        Ok(Response::new(BatchProduceResponse { records }))
    }

    async fn commit_shard_offset(
        &self,
        req: Request<CommitShardOffsetRequest>,
    ) -> Result<Response<CommitShardOffsetResponse>, Status> {
        let key = GroupKey::new(&req.get_ref().namespace, &req.get_ref().stream, &req.get_ref().group);
        self.require_leader(&key)?;
        let r = req.into_inner();
        // Apply in-memory first (monotonic); only persist if it actually
        // advanced, so no-op/stale commits don't spam the meta stream. If the
        // persist fails the in-memory offset is slightly ahead of durable — on
        // restart that offset is lost and the shard re-consumes (at-least-once).
        let advanced = self
            .registry
            .commit_offset(&r.namespace, &r.stream, &r.group, r.shard_id, r.committed_seq);
        if advanced {
            metrics::counter!("broker.offsets_committed").increment(1);
            if let Some(pers) = &self.persistence {
                pers.append_offset(OffsetRecord {
                    ns: r.namespace,
                    stream: r.stream,
                    group: r.group,
                    shard: r.shard_id,
                    seq: r.committed_seq,
                })
                .await?;
            }
        }
        Ok(Response::new(CommitShardOffsetResponse { advanced }))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let key = GroupKey::new(&req.get_ref().namespace, &req.get_ref().stream, &req.get_ref().group);
        self.require_leader(&key)?;
        let r = req.into_inner();
        let key = GroupKey::new(&r.namespace, &r.stream, &r.group);
        let sessions = self.sessions.get_group(&key);
        let mut found = false;
        for (cid, h) in &sessions {
            if cid == &r.consumer_id {
                h.touch();
                found = true;
                break;
            }
        }
        if !found {
            return Err(Status::not_found("consumer session not found; call Consume first"));
        }
        let current_gen = self
            .registry
            .group_snapshot(&r.namespace, &r.stream, &r.group)
            .map(|s| s.generation)
            .unwrap_or(0);
        Ok(Response::new(HeartbeatResponse {
            generation: current_gen,
            rebalance: current_gen != r.generation,
        }))
    }
}

// ── rebalance helpers (Phase 5) — not trait methods ──────────────────────────
impl BrokerServiceImpl {
    /// Build shard → logdbd from_seq (last-committed + 1) for `shards`.
    fn shard_offsets_for(
        &self,
        namespace: &str,
        stream: &str,
        group: &str,
        shards: &[u32],
    ) -> std::collections::HashMap<u32, u64> {
        let mut m = std::collections::HashMap::new();
        for &shard in shards {
            let last = self.registry.shard_offset(namespace, stream, group, shard);
            m.insert(shard, last.saturating_add(1));
        }
        m
    }

    /// Spawn a consumer's forward task: Tails its shards → session channel. On
    /// exit (consumer disconnected — tx fails — OR aborted by a rebalance) it
    /// deregisters the session. An ABORTED task is cancelled mid-`await` so the
    /// deregister line does not run; the rebalance reuses the session with a
    /// fresh task, which deregisters on the eventual disconnect.
    fn spawn_forward(
        &self,
        session: Arc<SessionHandle>,
        key: GroupKey,
        consumer_id: String,
        namespace: String,
        stream: String,
        shards: Vec<u32>,
    ) {
        let Some(forwarder) = self.forwarder.clone() else {
            return; // no logdbd: nothing to forward (membership-only setup)
        };
        let shard_from_seq = self.shard_offsets_for(&namespace, &stream, &key.group, &shards);
        let tx = session.tx.clone();
        let sessions = Arc::clone(&self.sessions);
        let handle = tokio::spawn(async move {
            if let Err(e) = forwarder.forward(namespace, stream, shard_from_seq, tx).await {
                tracing::warn!(error = %e, "forward task ended with error");
            }
            sessions.remove(&key, &consumer_id);
        });
        session.set_forward(handle);
    }

    /// Stop-the-world rebalance for a group's open Consume streams. For each
    /// active session: push a RebalanceSignal, abort its forward task, push an
    /// Assignment (new shards), and spawn a fresh forward task resuming from
    /// committed offsets. At-least-once during the swap (a record in flight when
    /// the old task is aborted may be redelivered after resume).
    async fn rebalance_group(&self, namespace: &str, stream: &str, group: &str) {
        let key = GroupKey::new(namespace, stream, group);
        let sessions = self.sessions.get_group(&key);
        if sessions.is_empty() {
            return;
        }
        let Some(snap) = self.registry.group_snapshot(namespace, stream, group) else {
            return;
        };
        metrics::counter!("broker.rebalances").increment(1);

        // Sticky: only swap forward tasks for consumers whose shard set
        // actually changed (the rest keep running uninterrupted).
        let changed = self.registry.last_changed(namespace, stream, group);
        tracing::info!(
            ns = namespace,
            stream = stream,
            group = group,
            generation = snap.generation,
            sessions = sessions.len(),
            changed = changed.len(),
            "rebalancing open consume streams (sticky)"
        );

        for (consumer_id, session) in sessions {
            if !changed.contains(&consumer_id) {
                continue; // unchanged — forward task keeps running
            }
            // New shard assignment for this consumer (empty if it now has none).
            let new_shards = snap
                .members
                .iter()
                .find(|(id, _)| id == &consumer_id)
                .map(|(_, shards)| shards.clone())
                .unwrap_or_default();

            // 1. Pause signal. A send failure means the consumer is gone →
            //    lazy-remove the session and skip it.
            let signal = ConsumeResponse {
                payload: Some(ConsumePayload::Rebalance(RebalanceSignal {
                    generation: snap.generation,
                })),
            };
            if session.tx.send(Ok(signal)).await.is_err() {
                self.sessions.remove(&key, &consumer_id);
                continue;
            }

            // 2. Abort the old forward task (its records stop here).
            session.abort_forward();

            // 3. Resume signal with the new assignment.
            let resume = ConsumeResponse {
                payload: Some(ConsumePayload::Assignment(Assignment {
                    generation: snap.generation,
                    shards: new_shards.clone(),
                })),
            };
            let _ = session.tx.send(Ok(resume)).await;

            // 4. Spawn the new forward task (only if the consumer still has shards).
            if !new_shards.is_empty() {
                self.spawn_forward(
                    session,
                    key.clone(),
                    consumer_id,
                    namespace.into(),
                    stream.into(),
                    new_shards,
                );
            }
        }
    }

    /// Evict consumers whose heartbeat gap exceeds `timeout_ms`. For each
    /// stale consumer: abort its forward, remove from sessions + registry,
    /// then rebalance its group so remaining members pick up the freed shards.
    pub async fn evict_stale(&self, timeout_ms: u64) {
        let stale = self.sessions.stale_consumers(timeout_ms);
        for (key, consumer_id) in stale {
            self.sessions.remove(&key, &consumer_id);
            self.registry.leave(&key.namespace, &key.stream, &key.group, &consumer_id);
            tracing::warn!(
                ns = %key.namespace, stream = %key.stream, group = %key.group,
                consumer = %consumer_id, timeout_ms,
                "evicting stale consumer"
            );
            self.rebalance_group(&key.namespace, &key.stream, &key.group).await;
        }
    }

    /// Launch a background task that periodically evicts stale consumers.
    /// Call once after server startup. Pass `session_timeout_ms`: consumers
    /// missing heartbeats for longer than this are evicted.
    pub fn start_liveness_check(self: &std::sync::Arc<Self>, session_timeout_ms: u64) {
        let svc = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            let interval = std::time::Duration::from_millis(session_timeout_ms.max(1000) / 3);
            loop {
                tokio::time::sleep(interval).await;
                svc.evict_stale(session_timeout_ms).await;
            }
        });
    }
}

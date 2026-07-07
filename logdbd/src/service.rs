use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};

use crate::catalog::Catalog;
use crate::consumer::ConsumerTracker;
use crate::pb;
use crate::pb::log_db_service_server::LogDbService;
use crate::query::{
    self, AbsentMatch as QueryAbsentMatch, MetadataFilter as QueryMetadataFilter, Query,
    QueryResult as EngineQueryResult, ResultSet,
};
use crate::storage::Storage;
use crate::subscribe::SubscribeHub;

/// Convert protobuf map to BTreeMap for record encoding.
fn to_btree(hm: &HashMap<String, String>) -> BTreeMap<String, String> {
    hm.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Convert BTreeMap to HashMap for protobuf serialization.
fn to_hashmap(bm: &BTreeMap<String, String>) -> HashMap<String, String> {
    bm.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Map a proto `QueryRequest` into the engine's `Query`.
fn build_query(req: &pb::QueryRequest) -> Query {
    Query {
        event_types: req.event_types.clone(),
        from_seq: req.from_seq,
        to_seq: req.to_seq,
        metadata: req
            .metadata
            .iter()
            .map(|m| QueryMetadataFilter {
                key: m.key.clone(),
                value: m.value.clone(),
            })
            .collect(),
        // prost represents the proto3 enum field as `i32` (so unknown enum
        // values survive a round-trip). `TryFrom` recovers the typed variant;
        // RECORDS (0) and any future/unspecified value fall back to Records.
        result: match pb::QueryResult::try_from(req.result) {
            Ok(pb::QueryResult::Count) => EngineQueryResult::Count,
            Ok(pb::QueryResult::Exists) => EngineQueryResult::Exists,
            Ok(pb::QueryResult::CountDistinct) => EngineQueryResult::CountDistinct,
            Ok(pb::QueryResult::Min) => EngineQueryResult::Min,
            Ok(pb::QueryResult::Max) => EngineQueryResult::Max,
            Ok(pb::QueryResult::DistinctValues) => EngineQueryResult::DistinctValues,
            _ => EngineQueryResult::Records,
        },
        aggregate_field: if req.aggregate_field.is_empty() {
            None
        } else {
            Some(req.aggregate_field.clone())
        },
        absent: req.absent.as_ref().map(|a| QueryAbsentMatch {
            peer_event_types: a.peer_event_types.clone(),
            join_key: a.join_key.clone(),
        }),
        limit: req.limit.max(0) as usize,
        descending: req.descending,
    }
}

/// Map an engine `ResultSet` into the proto `QueryResponse` oneof.
fn result_to_response(rs: ResultSet) -> pb::QueryResponse {
    let result = match rs {
        ResultSet::Records(recs) => Some(pb::query_response::Result::Records(pb::RecordsResult {
            records: recs.iter().map(decoded_to_pb).collect(),
        })),
        ResultSet::Count(n) => Some(pb::query_response::Result::Count(n)),
        ResultSet::Exists(b) => Some(pb::query_response::Result::Exists(b)),
        ResultSet::CountDistinct(n) => Some(pb::query_response::Result::CountDistinct(n)),
        ResultSet::Min(n) => Some(pb::query_response::Result::Min(n)),
        ResultSet::Max(n) => Some(pb::query_response::Result::Max(n)),
        ResultSet::DistinctValues(vals) => Some(pb::query_response::Result::DistinctValues(
            pb::DistinctValuesResult { values: vals },
        )),
    };
    pb::QueryResponse { result }
}

/// Encode a `DecodedRecord` into the wire `Record`.
fn decoded_to_pb(r: &crate::record::DecodedRecord) -> pb::Record {
    pb::Record {
        namespace_id: r.namespace_id,
        stream_id: r.stream_id,
        seq: r.seq,
        event_type: r.event_type.clone(),
        timestamp_ns: r.timestamp_ns,
        content_type: r.content_type.clone(),
        metadata: to_hashmap(&r.metadata),
        content: r.user_content.clone(),
    }
}

/// Stream-replay helper for `subscribe`: read durable records from the segment,
/// keep only those matching `stream_id` + `seq > last_committed` + `event_types`,
/// and forward them on `tx`. Tombstoned records (seq ≤ the stream's cutoff) are
/// filtered out via `tombstones` (cr-027 phase 5).
///
/// TODO(cr-027 phase 5): this materializes the whole durable prefix into a Vec on
/// every Subscribe call. Stream + filter without collecting once phase 5 removes
/// the dual-scan transition cost.
async fn scan_stream_replay(
    storage: &Storage,
    durable: u64,
    stream_id: u64,
    last_committed: u64,
    event_types: &std::collections::HashSet<String>,
    tombstones: &crate::tombstone::TombstoneTracker,
    tx: &tokio::sync::mpsc::Sender<Result<pb::Record, Status>>,
) -> Result<(), crate::storage::StorageError> {
    for rec in storage.scan(0, durable)? {
        if rec.stream_id == stream_id
            && rec.seq > last_committed
            && event_types.contains(&rec.event_type)
            && tombstones.is_live(stream_id, rec.seq)
        {
            if tx.send(Ok(decoded_to_pb(&rec))).await.is_err() {
                return Ok(()); // client disconnected
            }
        }
    }
    Ok(())
}

pub struct LogDbServiceImpl {
    storage: Arc<Storage>,
    catalog: Arc<Catalog>,
    consumer_tracker: Arc<ConsumerTracker>,
    subscribe_hub: Arc<SubscribeHub>,
    quotas: Vec<crate::config::StreamQuota>,
    hostname: String,
    role: String,
    quota_tracker: Arc<crate::quota::QuotaTracker>,
    tombstone_tracker: Arc<crate::tombstone::TombstoneTracker>,
}

impl LogDbServiceImpl {
    pub fn new(
        storage: Arc<Storage>,
        catalog: Arc<Catalog>,
        consumer_tracker: Arc<ConsumerTracker>,
        subscribe_hub: Arc<SubscribeHub>,
        hostname: String,
        role: String,
    ) -> Self {
        Self {
            storage,
            catalog,
            consumer_tracker,
            subscribe_hub,
            quotas: Vec::new(),
            hostname,
            role,
            quota_tracker: Arc::new(crate::quota::QuotaTracker::new()),
            tombstone_tracker: Arc::new(crate::tombstone::TombstoneTracker::new()),
        }
    }

    /// Same as `new` but with stream quotas.
    #[allow(clippy::too_many_arguments)]
    pub fn with_quotas(
        storage: Arc<Storage>,
        catalog: Arc<Catalog>,
        consumer_tracker: Arc<ConsumerTracker>,
        subscribe_hub: Arc<SubscribeHub>,
        quotas: Vec<crate::config::StreamQuota>,
        hostname: String,
        role: String,
        quota_tracker: Arc<crate::quota::QuotaTracker>,
        tombstone_tracker: Arc<crate::tombstone::TombstoneTracker>,
    ) -> Self {
        Self {
            storage,
            catalog,
            consumer_tracker,
            subscribe_hub,
            quotas,
            hostname,
            role,
            quota_tracker,
            tombstone_tracker,
        }
    }

    /// Check stream quota — reject if appending `incoming_records` records
    /// totalling `incoming_bytes` of payload would exceed the limit. Reads the
    /// in-memory `QuotaTracker` (segment-seeded, append-maintained); no SQLite.
    ///
    /// The records bound `usage + incoming > max` is, for a single append
    /// (`incoming_records = 1`), exactly v0.6.0's `usage >= max`. The bytes
    /// bound is unchanged: `usage + incoming > max`.
    fn check_quota(
        &self,
        ns: &str,
        stream: &str,
        ns_id: u32,
        stream_id: u64,
        incoming_records: u64,
        incoming_bytes: usize,
        quotas: &[crate::config::StreamQuota],
    ) -> Result<(), Status> {
        for q in quotas {
            if q.namespace == ns && q.stream == stream {
                let usage = self.quota_tracker.usage(ns_id, stream_id);
                if let Some(max_records) = q.max_records {
                    if usage.records + incoming_records > max_records {
                        return Err(Status::resource_exhausted(format!(
                            "stream {}/{} record quota exceeded: {} + {} > {}",
                            ns, stream, usage.records, incoming_records, max_records
                        )));
                    }
                }
                if let Some(max_bytes) = q.max_bytes {
                    if usage.bytes + incoming_bytes as u64 > max_bytes {
                        return Err(Status::resource_exhausted(format!(
                            "stream {}/{} byte quota exceeded: {} + {} > {}",
                            ns, stream, usage.bytes, incoming_bytes, max_bytes
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn check_write(&self) -> Result<(), Status> {
        if self.role != "primary" {
            Err(Status::permission_denied("not primary"))
        } else {
            Ok(())
        }
    }

    /// Resolve namespace + stream → IDs.
    fn resolve(&self, ns: &str, stream: &str) -> Result<(u32, u64), Status> {
        self.catalog
            .resolve(ns, stream)
            .map_err(|e| Status::invalid_argument(format!("invalid namespace/stream: {}", e)))
    }
}

#[tonic::async_trait]
impl LogDbService for LogDbServiceImpl {
    // ── Write ──────────────────────────────────────────────────────────

    async fn append(
        &self,
        req: Request<pb::AppendRequest>,
    ) -> Result<Response<pb::AppendResponse>, Status> {
        crate::auth::require_role(&req, crate::auth::Role::Writer)?;
        self.check_write()?;
        let r = req.get_ref();

        let (ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;
        self.check_quota(
            &r.namespace,
            &r.stream,
            ns_id,
            stream_id,
            1,
            r.content.len(),
            &self.quotas,
        )?;

        let meta = to_btree(&r.metadata);
        let ts = if r.timestamp_ns > 0 { r.timestamp_ns } else { 0 };
        let ct = if r.content_type.is_empty() {
            "application/json"
        } else {
            &r.content_type
        };

        let result = self
            .storage
            .append(ns_id, stream_id, &r.event_type, ct, &meta, ts, &r.content)
            .map_err(|e| Status::internal(e.to_string()))?;

        // Incremental quota update — only on a successful append.
        self.quota_tracker.add(ns_id, stream_id, r.content.len());

        Ok(Response::new(pb::AppendResponse {
            namespace_id: ns_id,
            stream_id,
            seq: result.stream_seq,
            gid: result.gid,
        }))
    }

    async fn batch_append(
        &self,
        req: Request<pb::BatchAppendRequest>,
    ) -> Result<Response<pb::AppendBatchResponse>, Status> {
        self.check_write()?;
        let r = req.get_ref();

        // For now, batch is implemented as sequential appends within the same stream.
        // Future optimization: use logdb.append_batch() for true atomicity.
        if r.requests.is_empty() {
            return Ok(Response::new(pb::AppendBatchResponse {
                records: vec![],
                error: None,
            }));
        }

        // All requests must target the same namespace+stream for atomicity
        let first_ns = &r.requests[0].namespace;
        let first_stream = &r.requests[0].stream;
        for req in &r.requests[1..] {
            if req.namespace != *first_ns || req.stream != *first_stream {
                return Err(Status::invalid_argument(
                    "BatchAppend requires all requests in the same namespace+stream",
                ));
            }
        }

        let (ns_id, stream_id) = self.resolve(first_ns, first_stream)?;

        // Pre-flight the whole batch against the quota — atomic: either the
        // entire batch still fits within the limits, or none of it is written.
        let batch_records = r.requests.len() as u64;
        let batch_bytes: usize = r.requests.iter().map(|q| q.content.len()).sum();
        self.check_quota(
            first_ns,
            first_stream,
            ns_id,
            stream_id,
            batch_records,
            batch_bytes,
            &self.quotas,
        )?;

        let mut responses = Vec::with_capacity(r.requests.len());

        for req in &r.requests {
            let meta = to_btree(&req.metadata);
            let ts = if req.timestamp_ns > 0 {
                req.timestamp_ns
            } else {
                0
            };
            let ct = if req.content_type.is_empty() {
                "application/json"
            } else {
                &req.content_type
            };

            let result = self
                .storage
                .append(
                    ns_id,
                    stream_id,
                    &req.event_type,
                    ct,
                    &meta,
                    ts,
                    &req.content,
                )
                .map_err(|e| Status::internal(e.to_string()))?;

            // Incremental quota update — only on a successful append.
            self.quota_tracker.add(ns_id, stream_id, req.content.len());

            responses.push(pb::AppendResponse {
                namespace_id: ns_id,
                stream_id,
                seq: result.stream_seq,
                gid: result.gid,
            });
        }

        Ok(Response::new(pb::AppendBatchResponse {
            records: responses,
            error: None,
        }))
    }

    // ── Read ───────────────────────────────────────────────────────────

    async fn read(
        &self,
        req: Request<pb::ReadRequest>,
    ) -> Result<Response<pb::ReadResponse>, Status> {
        crate::auth::require_role(&req, crate::auth::Role::Reader)?;
        let r = req.get_ref();
        let (_ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;

        match self.storage.read(stream_id, r.seq) {
            Ok(Some(rec)) => {
                if !self.tombstone_tracker.is_live(stream_id, rec.seq) {
                    return Ok(Response::new(pb::ReadResponse {
                        record: None,
                        found: false,
                    }));
                }
                Ok(Response::new(pb::ReadResponse {
                    record: Some(pb::Record {
                        namespace_id: rec.namespace_id,
                        stream_id: rec.stream_id,
                        seq: rec.seq,
                        event_type: rec.event_type,
                        timestamp_ns: rec.timestamp_ns,
                        content_type: rec.content_type,
                        metadata: to_hashmap(&rec.metadata),
                        content: rec.user_content,
                    }),
                    found: true,
                }))
            }
            Ok(None) => Ok(Response::new(pb::ReadResponse {
                record: None,
                found: false,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    type ScanStream = tokio_stream::wrappers::ReceiverStream<Result<pb::ScanResponse, Status>>;

    async fn scan(
        &self,
        req: Request<pb::ScanRequest>,
    ) -> Result<Response<Self::ScanStream>, Status> {
        let r = req.get_ref();
        let (_ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;
        let from = r.from_seq;
        let limit = if r.limit == 0 {
            10000
        } else {
            r.limit as usize
        };

        let storage = Arc::clone(&self.storage);
        let tombstones = Arc::clone(&self.tombstone_tracker);
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        tokio::spawn(async move {
            // Scan all gids, filter by stream + per-stream seq range
            // FIXME: O(n) — Phase 6 will add per-stream indexing
            let all = match storage.scan(0, u64::MAX) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error = %e, "scan failed");
                    let _ = tx.send(Err(Status::internal(format!("scan: {}", e)))).await;
                    return;
                }
            };
            let stream_records: Vec<_> = all
                .into_iter()
                .filter(|r| {
                    r.stream_id == stream_id && r.seq >= from && tombstones.is_live(stream_id, r.seq)
                })
                .collect();

            let total_chunks = stream_records.len().div_ceil(limit);
            for (chunk_idx, chunk) in stream_records.chunks(limit).enumerate() {
                let records: Vec<pb::Record> = chunk
                    .iter()
                    .map(|r| pb::Record {
                        namespace_id: r.namespace_id,
                        stream_id: r.stream_id,
                        seq: r.seq,
                        event_type: r.event_type.clone(),
                        timestamp_ns: r.timestamp_ns,
                        content_type: r.content_type.clone(),
                        metadata: to_hashmap(&r.metadata),
                        content: r.user_content.clone(),
                    })
                    .collect();

                let last_seq = records.last().map(|r| r.seq).unwrap_or(0);
                let has_more = chunk_idx + 1 < total_chunks;
                if tx
                    .send(Ok(pb::ScanResponse {
                        records,
                        next_seq: last_seq + 1,
                        has_more,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    type TailStream = tokio_stream::wrappers::ReceiverStream<Result<pb::TailResponse, Status>>;

    async fn tail(
        &self,
        req: Request<pb::TailRequest>,
    ) -> Result<Response<Self::TailStream>, Status> {
        let r = req.get_ref();
        let (_ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;

        let storage = Arc::clone(&self.storage);
        let tracker = Arc::clone(&self.consumer_tracker);
        let tombstones = Arc::clone(&self.tombstone_tracker);
        let ns = r.namespace.clone();
        let stream = r.stream.clone();
        let group = r.consumer_group.clone();
        let cid = r.consumer_id.clone();

        // Auto-resume from committed offset for consumer groups
        let from_seq = if r.from_seq == 0 && !group.is_empty() && !cid.is_empty() {
            let committed = tracker.get(&ns, &stream, &group, &cid);
            if committed > 0 {
                tracing::info!(ns = %ns, stream = %stream, group = %group, consumer = %cid, seq = committed + 1, "consumer resuming from committed offset");
                committed + 1
            } else {
                0
            }
        } else {
            r.from_seq
        };

        let batch_size = if r.batch_size == 0 {
            100u32
        } else {
            r.batch_size
        };

        let (tx, rx) = tokio::sync::mpsc::channel(16);

        tokio::spawn(async move {
            let mut last_seq = from_seq;
            loop {
                let durable = storage.durable_gid();
                let all = match storage.scan(0, durable) {
                    Ok(v) => v,
                    Err(_) => {
                        let _ = tx.send(Err(Status::internal("scan error"))).await;
                        return;
                    }
                };
                let new_records: Vec<_> = all
                    .into_iter()
                    .filter(|r| {
                        r.stream_id == stream_id
                            && r.seq >= last_seq
                            && tombstones.is_live(stream_id, r.seq)
                    })
                    .take(batch_size as usize)
                    .collect();

                if new_records.is_empty() {
                    // Send heartbeat
                    if tx
                        .send(Ok(pb::TailResponse {
                            records: vec![],
                            durable_seq: durable,
                            heartbeat: true,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }

                let records: Vec<pb::Record> = new_records
                    .iter()
                    .map(|r| pb::Record {
                        namespace_id: r.namespace_id,
                        stream_id: r.stream_id,
                        seq: r.seq,
                        event_type: r.event_type.clone(),
                        timestamp_ns: r.timestamp_ns,
                        content_type: r.content_type.clone(),
                        metadata: to_hashmap(&r.metadata),
                        content: r.user_content.clone(),
                    })
                    .collect();

                last_seq = records.last().unwrap().seq + 1;

                if tx
                    .send(Ok(pb::TailResponse {
                        records,
                        durable_seq: durable,
                        heartbeat: false,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    // ── Watermark ───────────────────────────────────────────────────────

    async fn get_watermark(
        &self,
        req: Request<pb::GetWatermarkRequest>,
    ) -> Result<Response<pb::Watermark>, Status> {
        let r = req.get_ref();
        let ns = &r.namespace;
        let stream_opt = if r.stream.is_empty() {
            None
        } else {
            Some(r.stream.as_str())
        };

        let durable = self.storage.durable_gid();
        let replicated = self.storage.replicated_gid();
        Ok(Response::new(pb::Watermark {
            namespace: ns.clone(),
            stream: stream_opt.unwrap_or("").into(),
            oldest_seq: 0,
            durable_seq: durable,
            replicated_seq: replicated,
            node_id: self.hostname.clone(),
            role: self.role.clone(),
        }))
    }

    // ── Admin ───────────────────────────────────────────────────────────

    async fn checkpoint(
        &self,
        req: Request<pb::CheckpointRequest>,
    ) -> Result<Response<pb::CheckpointResponse>, Status> {
        self.check_write()?;
        self.storage.checkpoint(req.get_ref().sequence);
        Ok(Response::new(pb::CheckpointResponse {}))
    }

    async fn verify_chain(
        &self,
        req: Request<pb::VerifyChainRequest>,
    ) -> Result<Response<pb::VerifyChainResponse>, Status> {
        let r = req.get_ref();
        let (_ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;

        let from = r.from_seq;
        let to = if r.to_seq == 0 { u64::MAX } else { r.to_seq };

        let all = self
            .storage
            .scan(0, u64::MAX)
            .map_err(|e| Status::internal(format!("scan: {}", e)))?;

        let records: Vec<_> = all
            .iter()
            .filter(|r| {
                r.stream_id == stream_id && r.seq >= from && (to == u64::MAX || r.seq <= to)
            })
            .collect();

        if records.is_empty() {
            return Ok(Response::new(pb::VerifyChainResponse {
                ok: true,
                verified_from: 0,
                verified_to: 0,
                error_at_seq: 0,
                error_message: String::new(),
            }));
        }

        // Verify seq continuity (no gaps, no duplicates) and record decodes
        let first = records[0].seq;
        let last = records.last().unwrap().seq;
        let mut expected = first;

        for rec in &records {
            if rec.stream_id != stream_id {
                return Ok(Response::new(pb::VerifyChainResponse {
                    ok: false,
                    verified_from: first,
                    verified_to: last,
                    error_at_seq: rec.seq,
                    error_message: format!("stream_id mismatch at seq={}", rec.seq),
                }));
            }
            if rec.seq != expected {
                return Ok(Response::new(pb::VerifyChainResponse {
                    ok: false,
                    verified_from: first,
                    verified_to: last,
                    error_at_seq: expected,
                    error_message: format!(
                        "seq gap: expected {}, got {} ({} records verified before gap)",
                        expected,
                        rec.seq,
                        (expected - first)
                    ),
                }));
            }
            expected += 1;
        }

        // Note: content-level tamper detection is provided by logdb's per-shard
        // BLAKE3 hash chain, verified at the storage layer during recovery and
        // on every read. This RPC verifies stream-level seq continuity.

        Ok(Response::new(pb::VerifyChainResponse {
            ok: true,
            verified_from: first,
            verified_to: last,
            error_at_seq: 0,
            error_message: format!(
                "{} records verified (storage-level hash chain managed by logdb)",
                records.len()
            ),
        }))
    }

    async fn commit_offset(
        &self,
        req: Request<pb::CommitOffsetRequest>,
    ) -> Result<Response<pb::CommitOffsetResponse>, Status> {
        let r = req.get_ref();
        self.consumer_tracker.commit(
            &r.namespace,
            &r.stream,
            &r.consumer_group,
            &r.consumer_id,
            r.committed_seq,
        );
        Ok(Response::new(pb::CommitOffsetResponse {
            ok: true,
            message: String::new(),
        }))
    }

    async fn get_committed_offset(
        &self,
        req: Request<pb::GetCommittedOffsetRequest>,
    ) -> Result<Response<pb::GetCommittedOffsetResponse>, Status> {
        let r = req.get_ref();
        let seq =
            self.consumer_tracker
                .get(&r.namespace, &r.stream, &r.consumer_group, &r.consumer_id);
        Ok(Response::new(pb::GetCommittedOffsetResponse {
            committed_seq: seq,
        }))
    }

    async fn status(
        &self,
        _req: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let durable = self.storage.durable_gid();
        Ok(Response::new(pb::StatusResponse {
            durable_sequence: durable,
            checkpoint: 0,
            wal_bytes_used: 0,
            wal_bytes_total: 0,
            node_id: self.hostname.clone(),
            role: self.role.clone(),
        }))
    }

    async fn list_namespaces(
        &self,
        _req: Request<pb::ListNamespacesRequest>,
    ) -> Result<Response<pb::ListNamespacesResponse>, Status> {
        let namespaces: Vec<pb::NamespaceInfo> = self
            .catalog
            .list_namespaces()
            .into_iter()
            .map(|ns| pb::NamespaceInfo {
                name: ns.name,
                id: ns.id,
                stream_count: ns.stream_count,
            })
            .collect();
        Ok(Response::new(pb::ListNamespacesResponse { namespaces }))
    }

    async fn list_streams(
        &self,
        req: Request<pb::ListStreamsRequest>,
    ) -> Result<Response<pb::ListStreamsResponse>, Status> {
        let ns = &req.get_ref().namespace;
        let streams = self
            .catalog
            .list_streams(ns)
            .map_err(|e| Status::not_found(e.to_string()))?;
        let streams: Vec<pb::StreamInfo> = streams
            .into_iter()
            .map(|s| pb::StreamInfo {
                name: s.name,
                id: s.id,
                first_seq: s.first_seq,
                durable_seq: s.durable_seq,
                record_count: s.record_count,
            })
            .collect();
        Ok(Response::new(pb::ListStreamsResponse { streams }))
    }

    async fn query(
        &self,
        req: Request<pb::QueryRequest>,
    ) -> Result<Response<pb::QueryResponse>, Status> {
        crate::auth::require_role(&req, crate::auth::Role::Reader)?;
        let r = req.get_ref();

        // Resolve namespace + stream (validates existence, gives stream_id).
        let (_ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;

        // Read boundary: COMMITTED cursor. The segment is the source of truth,
        // so a record becomes queryable within a Committer cycle (~≤10ms) of
        // Append returning. See cr-027 读边界.
        let committed = self.storage.committed_gid();
        let all = self
            .storage
            .scan(0, committed)
            .map_err(|e| Status::internal(format!("scan: {}", e)))?;

        // The engine operates on one stream's records.
        let stream_records: Vec<crate::record::DecodedRecord> = all
            .into_iter()
            .filter(|rec| {
                rec.stream_id == stream_id && self.tombstone_tracker.is_live(stream_id, rec.seq)
            })
            .collect();

        let q = build_query(r);
        let rs = query::execute(&q, &stream_records);
        Ok(Response::new(result_to_response(rs)))
    }

    type SubscribeStream = tokio_stream::wrappers::ReceiverStream<Result<pb::Record, Status>>;

    async fn subscribe(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        crate::auth::require_role(&req, crate::auth::Role::Subscriber)?;
        let r = req.get_ref();
        let (_ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;

        let event_types: std::collections::HashSet<String> =
            r.event_types.iter().cloned().collect();

        if event_types.is_empty() {
            return Err(Status::invalid_argument("event_types must not be empty"));
        }

        let ns = r.namespace.clone();
        let stream = r.stream.clone();
        let group = r.consumer_group.clone();
        let consumer_id = r.consumer_id.clone();

        // Get last committed offset — where to resume from
        let last_committed = self
            .consumer_tracker
            .get(&ns, &stream, &group, &consumer_id);

        // Clone what we need for the spawned task
        let storage = Arc::clone(&self.storage);
        let hub = Arc::clone(&self.subscribe_hub);
        let tombstones = Arc::clone(&self.tombstone_tracker);
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            // Phase 1: replay missed records directly from the log segment at the
            // durable cursor — the segment is the source of truth (cr-027 phase 4).
            let durable = storage.durable_gid();
            if let Err(e) = scan_stream_replay(
                &storage,
                durable,
                stream_id,
                last_committed,
                &event_types,
                &tombstones,
                &tx,
            )
            .await
            {
                let _ = tx
                    .send(Err(Status::internal(format!("replay: {}", e))))
                    .await;
                return;
            }

            // Phase 2: subscribe to real-time hub (unchanged model; uses decoded_to_pb).
            // The publisher already filters tombstones before pushing to the hub, so
            // the LIVE-push loop here needs no tombstone filter (cr-027 phase 5).
            let mut handle = hub.subscribe(stream_id, event_types);
            loop {
                match handle.next_matching().await {
                    Ok(rec) => {
                        if tx.send(Ok(decoded_to_pb(&rec))).await.is_err() {
                            return; // client disconnected
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(
                            ns = ns,
                            stream = stream,
                            skipped = n,
                            "subscribe client lagging, records skipped"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn create_stream(
        &self,
        req: Request<pb::CreateStreamRequest>,
    ) -> Result<Response<pb::CreateStreamResponse>, Status> {
        crate::auth::require_role(&req, crate::auth::Role::Admin)?;
        let r = req.get_ref();
        let (ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;
        Ok(Response::new(pb::CreateStreamResponse {
            namespace_id: ns_id,
            stream_id,
            created: true,
        }))
    }

    async fn delete_stream(
        &self,
        req: Request<pb::DeleteStreamRequest>,
    ) -> Result<Response<pb::DeleteStreamResponse>, Status> {
        crate::auth::require_role(&req, crate::auth::Role::Admin)?;
        // Appending a tombstone is a write ⇒ primary only. (v0.6.0 allowed this
        // on standbys only because it mutated local SQLite, which never replicated.)
        self.check_write()?;
        let r = req.get_ref();
        let (ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;

        // Records that will be logically deleted (reported in the response).
        let deleted_count = self.quota_tracker.usage(ns_id, stream_id).records;

        // Append a tombstone record to the segment. Its seq becomes the cutoff:
        // every record in this stream with seq ≤ cutoff is logically deleted.
        // A normal record ⇒ replicates to standbys and survives restart.
        let result = self
            .storage
            .append(
                ns_id,
                stream_id,
                crate::tombstone::STREAM_DELETED_EVENT,
                "application/json",
                &std::collections::BTreeMap::new(),
                0,
                &[],
            )
            .map_err(|e| Status::internal(e.to_string()))?;

        // Sync-update so reads respect the delete immediately (no ~10ms
        // publisher lag). The publisher also updates it later (idempotent).
        self.tombstone_tracker.record(stream_id, result.stream_seq);
        self.quota_tracker.reset(ns_id, stream_id);

        Ok(Response::new(pb::DeleteStreamResponse {
            deleted: true,
            deleted_count,
        }))
    }
}

#[cfg(test)]
mod mapping_tests {
    use super::*;
    use crate::record::DecodedRecord;
    use std::collections::BTreeMap;

    fn rec(seq: u64, event_type: &str, meta: &[(&str, &str)]) -> DecodedRecord {
        let mut metadata = BTreeMap::new();
        for (k, v) in meta {
            metadata.insert((*k).to_string(), (*v).to_string());
        }
        DecodedRecord {
            namespace_id: 1,
            stream_id: 7,
            seq,
            event_type: event_type.to_string(),
            content_type: "application/json".to_string(),
            metadata,
            timestamp_ns: seq,
            user_content: format!("c-{}", seq).into_bytes(),
        }
    }

    #[test]
    fn build_query_maps_all_fields_and_defaults_result_to_records() {
        let req = pb::QueryRequest {
            namespace: "ns".into(),
            stream: "s".into(),
            event_types: vec!["a".into(), "b".into()],
            from_seq: Some(5),
            to_seq: Some(9),
            metadata: vec![pb::MetadataFilter {
                key: "turn_id".into(),
                value: "1".into(),
            }],
            result: pb::QueryResult::CountDistinct.into(),
            aggregate_field: "turn_id".into(),
            absent: Some(pb::AbsentMatch {
                peer_event_types: vec!["turn_completed".into()],
                join_key: "turn_id".into(),
            }),
            limit: 3,
            descending: true,
        };
        let q = build_query(&req);
        assert_eq!(q.event_types, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(q.from_seq, Some(5));
        assert_eq!(q.to_seq, Some(9));
        assert_eq!(q.metadata.len(), 1);
        assert_eq!(q.metadata[0].key, "turn_id");
        assert_eq!(q.result, EngineQueryResult::CountDistinct);
        assert_eq!(q.aggregate_field.as_deref(), Some("turn_id"));
        assert_eq!(q.absent.as_ref().unwrap().join_key, "turn_id");
        assert_eq!(q.limit, 3);
        assert!(q.descending);
    }

    #[test]
    fn build_query_unspecified_result_is_records_and_empty_aggregate_is_none() {
        let req = pb::QueryRequest::default();
        let q = build_query(&req);
        assert_eq!(q.result, EngineQueryResult::Records);
        assert!(q.aggregate_field.is_none());
        assert!(q.absent.is_none());
        assert_eq!(q.limit, 0);
    }

    #[test]
    fn build_query_negative_limit_clamped_to_zero() {
        let req = pb::QueryRequest {
            limit: -5,
            ..Default::default()
        };
        assert_eq!(build_query(&req).limit, 0);
    }

    #[test]
    fn result_to_response_count() {
        let resp = result_to_response(ResultSet::Count(42));
        assert!(matches!(
            resp.result,
            Some(pb::query_response::Result::Count(42))
        ));
    }

    #[test]
    fn result_to_response_records_carries_fields() {
        let resp = result_to_response(ResultSet::Records(vec![rec(3, "x", &[])]));
        match resp.result {
            Some(pb::query_response::Result::Records(rr)) => {
                assert_eq!(rr.records.len(), 1);
                assert_eq!(rr.records[0].seq, 3);
                assert_eq!(rr.records[0].stream_id, 7);
            }
            _ => panic!("expected Records"),
        }
    }

    #[test]
    fn result_to_response_distinct_values() {
        let resp = result_to_response(ResultSet::DistinctValues(vec!["9".into(), "10".into()]));
        match resp.result {
            Some(pb::query_response::Result::DistinctValues(dv)) => {
                assert_eq!(dv.values, vec!["9".to_string(), "10".to_string()])
            }
            _ => panic!("expected DistinctValues"),
        }
    }
}

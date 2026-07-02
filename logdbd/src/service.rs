use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::catalog::Catalog;
use crate::consumer::ConsumerTracker;
use crate::pb;
use crate::pb::log_db_service_server::LogDbService;
use crate::storage::Storage;

/// Convert protobuf map to BTreeMap for record encoding.
fn to_btree(hm: &HashMap<String, String>) -> BTreeMap<String, String> {
    hm.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Convert BTreeMap to HashMap for protobuf serialization.
fn to_hashmap(bm: &BTreeMap<String, String>) -> HashMap<String, String> {
    bm.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

pub struct LogDbServiceImpl {
    storage: Arc<Storage>,
    catalog: Arc<Catalog>,
    consumer_tracker: Arc<ConsumerTracker>,
    hostname: String,
    role: String,
}

impl LogDbServiceImpl {
    pub fn new(
        storage: Arc<Storage>,
        catalog: Arc<Catalog>,
        consumer_tracker: Arc<ConsumerTracker>,
        hostname: String,
        role: String,
    ) -> Self {
        Self { storage, catalog, consumer_tracker, hostname, role }
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
        self.catalog.resolve(ns, stream).map_err(|e| {
            Status::invalid_argument(format!("invalid namespace/stream: {}", e))
        })
    }
}

#[tonic::async_trait]
impl LogDbService for LogDbServiceImpl {
    // ── Write ──────────────────────────────────────────────────────────

    async fn append(
        &self,
        req: Request<pb::AppendRequest>,
    ) -> Result<Response<pb::AppendResponse>, Status> {
        self.check_write()?;
        let r = req.get_ref();

        let (ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;

        let meta = to_btree(&r.metadata);
        let ts = if r.timestamp_ns > 0 { r.timestamp_ns } else { 0 };
        let ct = if r.content_type.is_empty() { "application/json" } else { &r.content_type };

        let result = self.storage.append(
            ns_id, stream_id,
            &r.event_type, ct, &meta,
            ts, &r.content,
        ).map_err(|e| Status::internal(e.to_string()))?;

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
            return Ok(Response::new(pb::AppendBatchResponse { records: vec![], error: None }));
        }

        // All requests must target the same namespace+stream for atomicity
        let first_ns = &r.requests[0].namespace;
        let first_stream = &r.requests[0].stream;
        for req in &r.requests[1..] {
            if req.namespace != *first_ns || req.stream != *first_stream {
                return Err(Status::invalid_argument(
                    "BatchAppend requires all requests in the same namespace+stream"
                ));
            }
        }

        let (ns_id, stream_id) = self.resolve(first_ns, first_stream)?;
        let mut responses = Vec::with_capacity(r.requests.len());

        for req in &r.requests {
            let meta = to_btree(&req.metadata);
            let ts = if req.timestamp_ns > 0 { req.timestamp_ns } else { 0 };
            let ct = if req.content_type.is_empty() { "application/json" } else { &req.content_type };

            let result = self.storage.append(
                ns_id, stream_id, &req.event_type, ct, &meta, ts, &req.content,
            ).map_err(|e| Status::internal(e.to_string()))?;

            responses.push(pb::AppendResponse {
                namespace_id: ns_id,
                stream_id,
                seq: result.stream_seq,
                gid: result.gid,
            });
        }

        Ok(Response::new(pb::AppendBatchResponse { records: responses, error: None }))
    }

    // ── Read ───────────────────────────────────────────────────────────

    async fn read(
        &self,
        req: Request<pb::ReadRequest>,
    ) -> Result<Response<pb::ReadResponse>, Status> {
        let r = req.get_ref();
        let (_ns_id, stream_id) = self.resolve(&r.namespace, &r.stream)?;

        match self.storage.read(stream_id, r.seq) {
            Ok(Some(rec)) => Ok(Response::new(pb::ReadResponse {
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
            })),
            Ok(None) => Ok(Response::new(pb::ReadResponse { record: None, found: false })),
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
        let limit = if r.limit == 0 { 10000 } else { r.limit as usize };

        let storage = Arc::clone(&self.storage);
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
                .filter(|r| r.stream_id == stream_id && r.seq >= from)
                .collect();

            let total_chunks = stream_records.len().div_ceil(limit);
            for (chunk_idx, chunk) in stream_records.chunks(limit).enumerate() {
                let records: Vec<pb::Record> = chunk.iter().map(|r| pb::Record {
                    namespace_id: r.namespace_id,
                    stream_id: r.stream_id,
                    seq: r.seq,
                    event_type: r.event_type.clone(),
                    timestamp_ns: r.timestamp_ns,
                    content_type: r.content_type.clone(),
                    metadata: to_hashmap(&r.metadata),
                    content: r.user_content.clone(),
                }).collect();

                let last_seq = records.last().map(|r| r.seq).unwrap_or(0);
                let has_more = chunk_idx + 1 < total_chunks;
                if tx.send(Ok(pb::ScanResponse {
                    records,
                    next_seq: last_seq + 1,
                    has_more,
                })).await.is_err() {
                    return;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
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

        let batch_size = if r.batch_size == 0 { 100u32 } else { r.batch_size };

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
                    .filter(|r| r.stream_id == stream_id && r.seq >= last_seq)
                    .take(batch_size as usize)
                    .collect();

                if new_records.is_empty() {
                    // Send heartbeat
                    if tx.send(Ok(pb::TailResponse {
                        records: vec![],
                        durable_seq: durable,
                        heartbeat: true,
                    })).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }

                let records: Vec<pb::Record> = new_records.iter().map(|r| pb::Record {
                    namespace_id: r.namespace_id,
                    stream_id: r.stream_id,
                    seq: r.seq,
                    event_type: r.event_type.clone(),
                    timestamp_ns: r.timestamp_ns,
                    content_type: r.content_type.clone(),
                    metadata: to_hashmap(&r.metadata),
                    content: r.user_content.clone(),
                }).collect();

                last_seq = records.last().unwrap().seq + 1;

                if tx.send(Ok(pb::TailResponse {
                    records,
                    durable_seq: durable,
                    heartbeat: false,
                })).await.is_err() {
                    return;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }

    // ── Watermark ───────────────────────────────────────────────────────

    async fn get_watermark(
        &self,
        req: Request<pb::GetWatermarkRequest>,
    ) -> Result<Response<pb::Watermark>, Status> {
        let r = req.get_ref();
        let ns = &r.namespace;
        let stream_opt = if r.stream.is_empty() { None } else { Some(r.stream.as_str()) };

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

        let all = self.storage.scan(0, u64::MAX).map_err(|e| {
            Status::internal(format!("scan: {}", e))
        })?;

        let records: Vec<_> = all
            .iter()
            .filter(|r| r.stream_id == stream_id && r.seq >= from && (to == u64::MAX || r.seq <= to))
            .collect();

        if records.is_empty() {
            return Ok(Response::new(pb::VerifyChainResponse {
                ok: true, verified_from: 0, verified_to: 0,
                error_at_seq: 0, error_message: String::new(),
            }));
        }

        // Verify seq continuity (no gaps, no duplicates) and record decodes
        let first = records[0].seq;
        let last = records.last().unwrap().seq;
        let mut expected = first;

        for rec in &records {
            if rec.stream_id != stream_id {
                return Ok(Response::new(pb::VerifyChainResponse {
                    ok: false, verified_from: first, verified_to: last,
                    error_at_seq: rec.seq,
                    error_message: format!("stream_id mismatch at seq={}", rec.seq),
                }));
            }
            if rec.seq != expected {
                return Ok(Response::new(pb::VerifyChainResponse {
                    ok: false, verified_from: first, verified_to: last,
                    error_at_seq: expected,
                    error_message: format!(
                        "seq gap: expected {}, got {} ({} records verified before gap)",
                        expected, rec.seq, (expected - first)
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
            error_message: format!("{} records verified (storage-level hash chain managed by logdb)", records.len()),
        }))
    }

    async fn commit_offset(
        &self,
        req: Request<pb::CommitOffsetRequest>,
    ) -> Result<Response<pb::CommitOffsetResponse>, Status> {
        let r = req.get_ref();
        self.consumer_tracker.commit(
            &r.namespace, &r.stream, &r.consumer_group, &r.consumer_id, r.committed_seq,
        );
        Ok(Response::new(pb::CommitOffsetResponse { ok: true, message: String::new() }))
    }

    async fn get_committed_offset(
        &self,
        req: Request<pb::GetCommittedOffsetRequest>,
    ) -> Result<Response<pb::GetCommittedOffsetResponse>, Status> {
        let r = req.get_ref();
        let seq = self.consumer_tracker.get(
            &r.namespace, &r.stream, &r.consumer_group, &r.consumer_id,
        );
        Ok(Response::new(pb::GetCommittedOffsetResponse { committed_seq: seq }))
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
        let streams = self.catalog.list_streams(ns).map_err(|e| {
            Status::not_found(e.to_string())
        })?;
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
}

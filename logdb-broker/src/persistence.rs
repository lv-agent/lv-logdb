//! Durable coordination state — the broker's offsets are event-sourced into a
//! logdbd meta stream so they survive a broker restart (the broker is otherwise
//! in-memory). Only offsets are durable; membership is transient (consumers
//! rejoin after a restart).
//!
//! Meta stream: namespace `_broker`, stream `coord_state`. Each committed offset
//! is appended as one JSON record (`event_type = offset_committed`). On startup
//! the broker scans the stream and replays (taking the max seq per shard).

use logdbd_proto::pb::log_db_service_client::LogDbServiceClient;
use logdbd_proto::pb::{AppendRequest, CreateStreamRequest, ScanRequest};
use tonic::transport::Channel;
use tonic::Status;

pub const META_NAMESPACE: &str = "logdb_broker";
pub const META_STREAM: &str = "coord_state";

/// One durable offset-commit record (JSON on the wire).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OffsetRecord {
    pub ns: String,
    pub stream: String,
    pub group: String,
    pub shard: u32,
    pub seq: u64,
}

/// Appends + replays offset-commit events on the logdbd meta stream.
#[derive(Clone)]
pub struct Persistence {
    channel: Channel,
}

impl Persistence {
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let channel = tonic::transport::Endpoint::from_shared(addr)?
            .connect()
            .await?;
        Ok(Self { channel })
    }

    /// Ensure the `_broker/coord_state` stream exists. Idempotent — ignores
    /// "already exists". Call once on startup before append/scan.
    pub async fn ensure_meta_stream(&self) -> Result<(), Status> {
        let mut client = LogDbServiceClient::new(self.channel.clone());
        let _ = client
            .create_stream(CreateStreamRequest {
                namespace: META_NAMESPACE.into(),
                stream: META_STREAM.into(),
                max_records: 0,
                max_bytes: 0,
            })
            .await; // ignore AlreadyExists / namespace-present errors
        Ok(())
    }

    /// Durably append an offset commit.
    pub async fn append_offset(&self, rec: OffsetRecord) -> Result<(), Status> {
        let mut client = LogDbServiceClient::new(self.channel.clone());
        let content = serde_json::to_vec(&rec)
            .map_err(|e| Status::internal(format!("encode offset record: {e}")))?;
        client
            .append(AppendRequest {
                namespace: META_NAMESPACE.into(),
                stream: META_STREAM.into(),
                event_type: "offset_committed".into(),
                content_type: "application/json".into(),
                content,
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Replay every committed offset (in log order). The caller applies each,
    /// taking the max seq per (group, shard) to rebuild the in-memory state.
    /// (If snapshots were written, prefer [`load_recovered_offsets`] which
    /// handles both snapshots and deltas.)
    pub async fn scan_offsets(&self) -> Result<Vec<OffsetRecord>, Status> {
        let mut out = Vec::new();
        for (_seq, event_type, content) in self.scan_raw_meta().await? {
            if event_type == "offset_committed" {
                if let Ok(rec) = serde_json::from_slice::<OffsetRecord>(&content) {
                    out.push(rec);
                }
            }
        }
        Ok(out)
    }

    /// Compact: write all current offsets as a single snapshot event. On the
    /// next recovery, [`load_recovered_offsets`] will find this snapshot and
    /// skip replaying individual offset events before it — the snapshot acts
    /// as a checkpoint. Compact periodically (e.g. on startup) to keep
    /// recovery fast.
    pub async fn compact_offsets(&self, recs: &[OffsetRecord]) -> Result<(), Status> {
        let mut client = LogDbServiceClient::new(self.channel.clone());
        let content = serde_json::to_vec(recs)
            .map_err(|e| Status::internal(format!("encode snapshot: {e}")))?;
        client
            .append(AppendRequest {
                namespace: META_NAMESPACE.into(),
                stream: META_STREAM.into(),
                event_type: "offset_snapshot".into(),
                content_type: "application/json".into(),
                content,
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Recover the final offset state. Scans the meta stream once; when it
    /// sees an `offset_snapshot` event it **replaces** the current state with
    /// the snapshot; `offset_committed` events (deltas after the latest
    /// snapshot) are applied via max per (group, shard). The result is
    /// equivalent to replaying every single event, but snapshots compact the
    /// history.
    pub async fn load_recovered_offsets(&self) -> Result<Vec<OffsetRecord>, Status> {
        let mut state: std::collections::HashMap<
            (String, String, String, u32), u64,
        > = std::collections::HashMap::new();
        for (_seq, event_type, content) in self.scan_raw_meta().await? {
            match event_type.as_str() {
                "offset_snapshot" => {
                    if let Ok(snapshot) =
                        serde_json::from_slice::<Vec<OffsetRecord>>(&content)
                    {
                        state.clear();
                        for r in snapshot {
                            let k = (r.ns, r.stream, r.group, r.shard);
                            state
                                .entry(k)
                                .and_modify(|v| *v = (*v).max(r.seq))
                                .or_insert(r.seq);
                        }
                    }
                }
                "offset_committed" => {
                    if let Ok(r) = serde_json::from_slice::<OffsetRecord>(&content)
                    {
                        let k = (r.ns, r.stream, r.group, r.shard);
                        state
                            .entry(k)
                            .and_modify(|v| *v = (*v).max(r.seq))
                            .or_insert(r.seq);
                    }
                }
                _ => {}
            }
        }
        Ok(state
            .into_iter()
            .map(|((ns, stream, group, shard), seq)| OffsetRecord {
                ns,
                stream,
                group,
                shard,
                seq,
            })
            .collect())
    }

    /// Scan every record in the meta stream, returning `(per-stream seq,
    /// event_type, content)` tuples in log order.
    async fn scan_raw_meta(&self) -> Result<Vec<(u64, String, Vec<u8>)>, Status> {
        let mut client = LogDbServiceClient::new(self.channel.clone());
        let mut stream = client
            .scan(ScanRequest {
                namespace: META_NAMESPACE.into(),
                stream: META_STREAM.into(),
                from_seq: 0,
                to_seq: 0,
                limit: 0,
            })
            .await?
            .into_inner();
        let mut out = Vec::new();
        while let Some(resp) = stream.message().await? {
            for r in resp.records {
                out.push((r.seq, r.event_type, r.content));
            }
        }
        Ok(out)
    }
}

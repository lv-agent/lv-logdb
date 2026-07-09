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
    pub async fn scan_offsets(&self) -> Result<Vec<OffsetRecord>, Status> {
        let mut client = LogDbServiceClient::new(self.channel.clone());
        let mut stream = client
            .scan(ScanRequest {
                namespace: META_NAMESPACE.into(),
                stream: META_STREAM.into(),
                from_seq: 0,
                to_seq: 0, // 0 = durable tail
                limit: 0,  // 0 = unlimited
            })
            .await?
            .into_inner();
        let mut out = Vec::new();
        while let Some(resp) = stream.message().await? {
            for r in resp.records {
                match serde_json::from_slice::<OffsetRecord>(&r.content) {
                    Ok(rec) => out.push(rec),
                    Err(e) => {
                        tracing_compat_warn(&e);
                    }
                }
            }
        }
        Ok(out)
    }
}

fn tracing_compat_warn(e: &serde_json::Error) {
    tracing::warn!(error = %e, "skipping malformed meta-stream record");
}

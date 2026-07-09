//! Data forwarder: the broker Tails logdbd per consumer session and forwards
//! each record onto the consumer's `Consume` stream (the symmetric data path,
//! cr-037).
//!
//! [`forward_stream`] is the pure core — it maps a stream of logdbd
//! `TailResponse`s onto `ConsumeResponse` frames, skips heartbeats, and stops
//! when the consumer disconnects. It is unit-tested with a fake stream.
//! [`Forwarder`] wraps a logdbd connection and feeds a real Tail into
//! [`forward_stream`].

use std::collections::HashMap;

use logdb_broker_proto::pb::{
    consume_response::Payload as ConsumePayload, ConsumeResponse, Record as BrokerRecord,
};
use logdbd_proto::pb::{Record as LogdbdRecord, TailResponse};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;

/// Map a logdbd record onto the broker's standalone `Record` schema, stamping
/// the `shard_id` the broker routed this Tail by (the consumer needs it to
/// commit per-shard offsets).
fn into_broker_record(r: LogdbdRecord, shard_id: u32) -> BrokerRecord {
    BrokerRecord {
        namespace_id: r.namespace_id,
        stream_id: r.stream_id,
        seq: r.seq,
        event_type: r.event_type,
        timestamp_ns: r.timestamp_ns,
        content_type: r.content_type,
        metadata: r.metadata,
        content: r.content,
        shard_id,
    }
}

/// Forward one shard's logdbd Tail stream onto a consumer's Consume channel,
/// stamping `shard_id` on every record.
///
/// - Each record becomes one `ConsumeResponse{record}` (broker schema, stamped).
/// - Heartbeat responses (empty records) produce nothing.
/// - Stops cleanly when the consumer disconnects (send fails) or the Tail ends.
/// - A Tail error is forwarded to the consumer, then forwarding stops.
pub async fn forward_stream<S>(
    mut tail: S,
    shard_id: u32,
    tx: mpsc::Sender<Result<ConsumeResponse, Status>>,
) where
    S: Stream<Item = Result<TailResponse, Status>> + Unpin,
{
    while let Some(msg) = tail.next().await {
        match msg {
            Ok(resp) => {
                for record in resp.records {
                    metrics::counter!("broker.records_forwarded").increment(1);
                    let frame = ConsumeResponse {
                        payload: Some(ConsumePayload::Record(into_broker_record(
                            record,
                            shard_id,
                        ))),
                    };
                    if tx.send(Ok(frame)).await.is_err() {
                        return; // consumer disconnected
                    }
                }
            }
            Err(status) => {
                let _ = tx.send(Err(status)).await;
                return;
            }
        }
    }
}

// ── Forwarder (real logdbd connection) ──────────────────────────────────────

/// Holds a connection to logdbd; each [`Forwarder::forward`] call opens a fresh
/// Tail scoped to one consumer's assigned shards and pumps it through
/// [`forward_stream`].
#[derive(Clone)]
pub struct Forwarder {
    channel: tonic::transport::Channel,
}

impl Forwarder {
    /// Connect to logdbd at `addr` (e.g. "http://127.0.0.1:9090").
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let channel = tonic::transport::Endpoint::from_shared(addr)?.connect().await?;
        Ok(Self { channel })
    }

    /// Open one Tail per assigned shard and forward records to `tx`, stamping
    /// each record's `shard_id`. `shard_from_seq` maps shard → logdbd `from_seq`
    /// resume point (last-committed-seq + 1; 0/1 = from the start). One Tail per
    /// shard so each resumes at its own offset (a single Tail's `from_seq` is a
    /// per-stream seq and can't express per-shard offsets).
    ///
    /// Returns when every per-shard task ends (consumer disconnect → `tx`
    /// sends fail → each `forward_stream` returns).
    pub async fn forward(
        &self,
        namespace: String,
        stream: String,
        shard_from_seq: HashMap<u32, u64>,
        tx: mpsc::Sender<Result<ConsumeResponse, Status>>,
    ) -> Result<(), Status> {
        let mut handles = Vec::new();
        for (shard, from_seq) in shard_from_seq {
            let channel = self.channel.clone();
            let ns = namespace.clone();
            let st = stream.clone();
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                let mut client = logdbd_proto::pb::log_db_service_client::LogDbServiceClient::new(
                    channel,
                );
                let tail = match client
                    .tail(logdbd_proto::pb::TailRequest {
                        namespace: ns,
                        stream: st,
                        from_seq,
                        batch_size: 500,
                        consumer_group: String::new(),
                        consumer_id: String::new(),
                        shard_ids: vec![shard],
                    })
                    .await
                {
                    Ok(r) => r.into_inner(),
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                forward_stream(tail, shard, tx).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        Ok(())
    }

    /// Forward a produce to logdbd.Append. Returns the logdb global id + the
    /// per-stream sequence the record landed at. (Symmetric gateway: producers
    /// publish to the broker, which writes to logdbd as the storage backend.)
    pub async fn append(
        &self,
        req: logdbd_proto::pb::AppendRequest,
    ) -> Result<logdbd_proto::pb::AppendResponse, Status> {
        let mut client = logdbd_proto::pb::log_db_service_client::LogDbServiceClient::new(
            self.channel.clone(),
        );
        Ok(client.append(req).await?.into_inner())
    }

    /// Discover logdbd's shard count by querying its Status RPC. If the
    /// server doesn't report `num_shards` (pre-cr-037), returns 0.
    pub async fn query_num_shards(&self) -> Result<u32, Status> {
        let mut client = logdbd_proto::pb::log_db_service_client::LogDbServiceClient::new(
            self.channel.clone(),
        );
        let status = client
            .status(logdbd_proto::pb::StatusRequest {})
            .await?
            .into_inner();
        Ok(status.num_shards)
    }

    /// Forward a batch produce to logdbd.BatchAppend (one gRPC call for the
    /// whole batch — much faster than N individual Appends).
    pub async fn append_batch(
        &self,
        reqs: Vec<logdbd_proto::pb::AppendRequest>,
    ) -> Result<Vec<logdbd_proto::pb::AppendResponse>, Status> {
        let mut client = logdbd_proto::pb::log_db_service_client::LogDbServiceClient::new(
            self.channel.clone(),
        );
        let resp = client
            .batch_append(logdbd_proto::pb::BatchAppendRequest { requests: reqs })
            .await?
            .into_inner();
        Ok(resp.records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logdbd_proto::pb::Record;

    fn rec(seq: u64) -> Record {
        Record {
            namespace_id: 1,
            stream_id: 1,
            seq,
            event_type: "e".into(),
            timestamp_ns: 0,
            content_type: "text/plain".into(),
            metadata: Default::default(),
            content: format!("r-{seq}").into_bytes(),
        }
    }

    #[allow(clippy::result_large_err)] // test helper; Status Err is never used
    fn tail(records: Vec<Record>, heartbeat: bool) -> Result<TailResponse, Status> {
        Ok(TailResponse {
            records,
            durable_seq: 0,
            heartbeat,
        })
    }

    async fn drain_frames(
        rx: &mut mpsc::Receiver<Result<ConsumeResponse, Status>>,
    ) -> Vec<ConsumeResponse> {
        let mut out = Vec::new();
        while let Ok(Some(msg)) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            if let Ok(frame) = msg {
                out.push(frame);
            }
        }
        out
    }

    fn record_seq(frame: &ConsumeResponse) -> u64 {
        match &frame.payload {
            Some(ConsumePayload::Record(r)) => r.seq,
            _ => panic!("expected a record frame"),
        }
    }

    fn record_shard(frame: &ConsumeResponse) -> u32 {
        match &frame.payload {
            Some(ConsumePayload::Record(r)) => r.shard_id,
            _ => panic!("expected a record frame"),
        }
    }

    #[tokio::test]
    async fn forwards_records_skips_heartbeats_and_stamps_shard() {
        let (tx, mut rx) = mpsc::channel(16);
        let stream = tokio_stream::iter(vec![
            tail(vec![rec(1), rec(2)], false),
            tail(vec![], true), // heartbeat — nothing forwarded
            tail(vec![rec(3)], false),
        ]);
        forward_stream(stream, 7, tx).await;

        let frames = drain_frames(&mut rx).await;
        let seqs: Vec<u64> = frames.iter().map(record_seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
        for f in &frames {
            assert_eq!(record_shard(f), 7, "every record must carry the stamped shard_id");
        }
    }

    #[tokio::test]
    async fn empty_tail_produces_nothing() {
        let (tx, mut rx) = mpsc::channel(16);
        let stream = tokio_stream::iter(Vec::<Result<TailResponse, Status>>::new());
        forward_stream(stream, 0, tx).await;
        let frames = drain_frames(&mut rx).await;
        assert!(frames.is_empty());
    }

    #[tokio::test]
    async fn stops_when_consumer_disconnects() {
        // Buffer of 1; drop the receiver so sends fail after the buffer fills.
        let (tx, rx) = mpsc::channel::<Result<ConsumeResponse, Status>>(1);
        drop(rx);
        let stream = tokio_stream::iter(vec![
            tail(vec![rec(1), rec(2), rec(3), rec(4)], false),
        ]);
        // Must return promptly (not hang) once send fails.
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            forward_stream(stream, 0, tx),
        )
        .await
        .expect("forward_stream must not hang when the consumer is gone");
    }

    #[tokio::test]
    async fn forwards_tail_error_then_stops() {
        let (tx, mut rx) = mpsc::channel(16);
        let stream = tokio_stream::iter(vec![
            tail(vec![rec(1)], false),
            Err(Status::internal("logdbd boom")),
            tail(vec![rec(99)], false), // must NOT be forwarded after the error
        ]);
        forward_stream(stream, 0, tx).await;

        let mut got_err = false;
        let mut record_seqs = Vec::new();
        while let Ok(Some(msg)) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            match msg {
                Ok(frame) => record_seqs.push(record_seq(&frame)),
                Err(_) => got_err = true,
            }
        }
        assert_eq!(record_seqs, vec![1], "only the pre-error record forwards");
        assert!(got_err, "the Tail error must be forwarded to the consumer");
    }

}

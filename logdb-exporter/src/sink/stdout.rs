use crate::sink::Sink;
use base64::Engine;
use logdbd_proto::pb::Record;

pub struct StdoutSink;

impl Sink for StdoutSink {
    fn push(&mut self, records: &[Record]) -> Result<(), String> {
        for rec in records {
            let json = serde_json::json!({
                "seq": rec.seq,
                "namespace_id": rec.namespace_id,
                "stream_id": rec.stream_id,
                "event_type": rec.event_type,
                "timestamp_ns": rec.timestamp_ns,
                "content_type": rec.content_type,
                "content": base64::engine::general_purpose::STANDARD.encode(&rec.content),
            });
            println!("{}", json);
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "stdout"
    }
}

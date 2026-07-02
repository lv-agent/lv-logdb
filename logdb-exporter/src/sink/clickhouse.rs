//! ClickHouse sink — HTTP insert with deduplication.
//!
//! Uses insert_deduplication_token for block-level dedup and relies on
//! ReplacingMergeTree for long-tail dedup at the table level.

use base64::Engine;
use crate::config::ClickHouseConfig;
use crate::sink::Sink;
use logdbd_proto::pb::Record;
use std::time::Duration;

pub struct ClickHouseSink {
    url: String,
    table: String,
    batch_size: usize,
    flush_interval: Duration,
    buffer: Vec<Record>,
    last_flush: std::time::Instant,
    client: reqwest::blocking::Client,
}

impl ClickHouseSink {
    pub fn new(config: &ClickHouseConfig, _namespace: &str, _stream: &str) -> Self {
        let url = format!(
            "{}/?database={}&query=INSERT+INTO+{}+FORMAT+JSONEachRow",
            config.url.trim_end_matches('/'),
            config.database,
            config.table,
        );
        Self {
            url,
            table: format!("{}.{}", config.database, config.table),
            batch_size: config.batch_size,
            flush_interval: Duration::from_millis(config.flush_interval_ms),
            buffer: Vec::with_capacity(config.batch_size),
            last_flush: std::time::Instant::now(),
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
        }
    }

    fn flush(&mut self) -> Result<(), String> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Build dedup token from first and last seq in batch
        let first_seq = self.buffer.first().unwrap().seq;
        let last_seq = self.buffer.last().unwrap().seq;
        let dedup_token = format!("{}-{}", first_seq, last_seq);

        // Serialize records as JSONEachRow
        let mut body = String::new();
        for rec in &self.buffer {
            let json = serde_json::json!({
                "namespace_id": rec.namespace_id,
                "stream_id": rec.stream_id,
                "seq": rec.seq,
                "event_type": rec.event_type,
                "timestamp_ns": rec.timestamp_ns,
                "content_type": rec.content_type,
                "metadata": rec.metadata,
                "content": base64::engine::general_purpose::STANDARD.encode(&rec.content),
            });
            body.push_str(&json.to_string());
            body.push('\n');
        }

        let count = self.buffer.len();
        let resp = self.client
            .post(&self.url)
            .header("Content-Type", "application/json")
            .query(&[("insert_deduplication_token", dedup_token)])
            .body(body)
            .send()
            .map_err(|e| format!("ClickHouse HTTP error: {}", e))?;

        let status = resp.status();
        if !status.is_success() {
            let err_body = resp.text().unwrap_or_default();
            return Err(format!("ClickHouse returned {}: {}", status, err_body));
        }

        tracing::info!(count, table = %self.table, "flushed to ClickHouse");
        self.buffer.clear();
        self.last_flush = std::time::Instant::now();
        Ok(())
    }
}

impl Sink for ClickHouseSink {
    fn push(&mut self, records: &[Record]) -> Result<(), String> {
        self.buffer.extend_from_slice(records);

        if self.buffer.len() >= self.batch_size || self.last_flush.elapsed() >= self.flush_interval {
            self.flush()?;
        }
        Ok(())
    }

    fn name(&self) -> &str { "clickhouse" }
}

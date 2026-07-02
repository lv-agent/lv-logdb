//! Event-type subscription hub — per-stream broadcast channels.
//!
//! The Indexer publishes each committed record to the hub after writing
//! to the SQLite cache.  gRPC `Subscribe` handlers read from the hub.
//! Non-blocking sends — records are durable in segment regardless.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use tokio::sync::broadcast;

use crate::record::DecodedRecord;

/// Capacity of each per-stream broadcast channel.
const CHANNEL_CAPACITY: usize = 256;

/// Hub that fans out committed records to subscribers by stream.
///
/// Each stream gets a lazily-created broadcast channel.  The Indexer
/// publishes every committed record via `publish`; the Subscribe gRPC
/// handler calls `subscribe` to receive matching records.
pub struct SubscribeHub {
    senders: RwLock<HashMap<u64, broadcast::Sender<Arc<DecodedRecord>>>>,
}

impl SubscribeHub {
    pub fn new() -> Self {
        Self {
            senders: RwLock::new(HashMap::new()),
        }
    }

    /// Publish a record to the broadcast channel for `stream_id`.
    /// Non-blocking — if no subscribers exist or the channel is full,
    /// the record is silently dropped (it stays in the segment).
    pub fn publish(&self, stream_id: u64, record: &DecodedRecord) {
        let map = self.senders.read().unwrap_or_else(|e| e.into_inner());
        if let Some(sender) = map.get(&stream_id) {
            let _ = sender.send(Arc::new(record.clone()));
        }
    }

    /// Subscribe to a stream's broadcast channel.
    ///
    /// Returns a handle that yields only records whose `event_type`
    /// matches the given set.
    pub fn subscribe(&self, stream_id: u64, event_types: HashSet<String>) -> SubscribeHandle {
        let mut map = self.senders.write().unwrap_or_else(|e| e.into_inner());
        let sender = map
            .entry(stream_id)
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
                tx
            });
        let receiver = sender.subscribe();
        SubscribeHandle {
            receiver,
            event_types,
        }
    }
}

/// Handle returned from [`SubscribeHub::subscribe`].
pub struct SubscribeHandle {
    receiver: broadcast::Receiver<Arc<DecodedRecord>>,
    event_types: HashSet<String>,
}

impl SubscribeHandle {
    /// Wait for the next record matching the subscribed event_types.
    pub async fn next_matching(
        &mut self,
    ) -> Result<Arc<DecodedRecord>, broadcast::error::RecvError> {
        loop {
            let rec = self.receiver.recv().await?;
            if self.event_types.contains(&rec.event_type) {
                return Ok(rec);
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_record(stream_id: u64, seq: u64, event_type: &str) -> DecodedRecord {
        DecodedRecord {
            namespace_id: 1,
            stream_id,
            seq,
            event_type: event_type.into(),
            content_type: "text/plain".into(),
            metadata: BTreeMap::new(),
            timestamp_ns: seq * 1000,
            user_content: format!("r-{}", seq).into_bytes(),
        }
    }

    #[tokio::test]
    async fn publish_and_receive() {
        let hub = SubscribeHub::new();
        let ets: HashSet<String> = ["tool.call".into(), "llm.call".into()].into();

        // Subscribe first to create the channel, then publish
        let mut handle = hub.subscribe(42, ets);
        hub.publish(42, &make_record(42, 1, "tool.call"));
        let rec = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.next_matching(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(rec.event_type, "tool.call");
        assert_eq!(rec.seq, 1);
    }

    #[tokio::test]
    async fn filter_excludes_non_matching() {
        let hub = SubscribeHub::new();
        let ets: HashSet<String> = ["tool.call".into()].into();

        let mut handle = hub.subscribe(1, ets);
        // Publish non-matching, then matching
        hub.publish(1, &make_record(1, 1, "user.input"));
        hub.publish(1, &make_record(1, 2, "tool.call"));
        let rec = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.next_matching(),
        )
        .await
        .unwrap()
        .unwrap();
        // Should skip "user.input" and get "tool.call"
        assert_eq!(rec.event_type, "tool.call");
        assert_eq!(rec.seq, 2);
    }

    #[tokio::test]
    async fn multi_subscriber_same_stream() {
        let hub = SubscribeHub::new();
        let tool_ets: HashSet<String> = ["tool.call".into()].into();
        let llm_ets: HashSet<String> = ["llm.call".into()].into();

        let mut tool_handle = hub.subscribe(1, tool_ets);
        let mut llm_handle = hub.subscribe(1, llm_ets);

        hub.publish(1, &make_record(1, 1, "tool.call"));
        hub.publish(1, &make_record(1, 2, "llm.call"));

        let tool_rec = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tool_handle.next_matching(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(tool_rec.event_type, "tool.call");

        let llm_rec = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            llm_handle.next_matching(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(llm_rec.event_type, "llm.call");
    }

    #[tokio::test]
    async fn stream_isolation() {
        let hub = SubscribeHub::new();
        let ets: HashSet<String> = ["tool.call".into()].into();

        let mut handle_s1 = hub.subscribe(1, ets.clone());
        let mut handle_s2 = hub.subscribe(2, ets);

        // Publish to both streams
        hub.publish(1, &make_record(1, 1, "tool.call"));
        hub.publish(2, &make_record(2, 1, "tool.call"));

        let r1 = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle_s1.next_matching(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(r1.stream_id, 1);

        let r2 = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle_s2.next_matching(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(r2.stream_id, 2);
    }

    #[tokio::test]
    async fn publish_without_subscribers_no_panic() {
        let hub = SubscribeHub::new();
        // No subscribers exist — publish must not panic
        hub.publish(1, &make_record(1, 1, "test"));
    }
}

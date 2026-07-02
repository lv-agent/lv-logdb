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

//! Consumer group — server-side offset tracking for shared stream consumption.
//!
//! Multiple consumers in the same group can independently process records
//! from a stream. Each consumer commits its progress via `CommitOffset`,
//! and resumes from the last committed seq on reconnect.
//!
//! # Model
//!
//! - `consumer_group` — logical group name (e.g., "audit-processors")
//! - `consumer_id` — unique consumer instance within the group
//! - Committed offsets are per (namespace, stream, consumer_group, consumer_id)
//!
//! # Persistence
//!
//! Offsets are stored in memory only. On restart, consumers reconnect and resume
//! from their last committed point. For durable offset storage, offsets should
//! be committed to the consumer's own persistence layer.

use std::collections::HashMap;
use std::sync::RwLock;

/// Key for consumer offset lookup.
type ConsumerKey = (String, String, String, String); // (ns, stream, group, id)

pub struct ConsumerTracker {
    offsets: RwLock<HashMap<ConsumerKey, u64>>,
}

impl ConsumerTracker {
    pub fn new() -> Self {
        Self {
            offsets: RwLock::new(HashMap::new()),
        }
    }

    /// Commit an offset for a consumer.
    pub fn commit(
        &self,
        namespace: &str,
        stream: &str,
        consumer_group: &str,
        consumer_id: &str,
        seq: u64,
    ) {
        let key = (
            namespace.to_string(),
            stream.to_string(),
            consumer_group.to_string(),
            consumer_id.to_string(),
        );
        let mut map = self
            .offsets
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.insert(key, seq);
    }

    /// Get the last committed offset for a consumer. Returns 0 if none.
    pub fn get(
        &self,
        namespace: &str,
        stream: &str,
        consumer_group: &str,
        consumer_id: &str,
    ) -> u64 {
        let key = (
            namespace.to_string(),
            stream.to_string(),
            consumer_group.to_string(),
            consumer_id.to_string(),
        );
        let map = self
            .offsets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.get(&key).copied().unwrap_or(0)
    }

    /// List all committed offsets for a consumer group in a stream.
    pub fn list_group(
        &self,
        namespace: &str,
        stream: &str,
        consumer_group: &str,
    ) -> Vec<(String, u64)> {
        let map = self
            .offsets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prefix = (
            namespace.to_string(),
            stream.to_string(),
            consumer_group.to_string(),
        );
        map.iter()
            .filter(|((ns, s, g, _), _)| *ns == prefix.0 && *s == prefix.1 && *g == prefix.2)
            .map(|((_, _, _, id), seq)| (id.clone(), *seq))
            .collect()
    }
}

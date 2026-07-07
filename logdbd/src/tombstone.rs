//! Per-stream tombstone tracking for logical stream deletion.
//!
//! `delete_stream` appends a tombstone record (event_type =
//! [`STREAM_DELETED_EVENT`]) to the target stream. A record is logically
//! deleted iff its per-stream `seq` is ≤ the stream's max tombstone seq.
//! The tombstone is a normal segment record, so it replicates to standbys and
//! survives restart — unlike v0.6.0's local SQLite `deleted` column.
//! Future appends (`seq > cutoff`) remain live, so a deleted stream can be
//! re-created.
//!
//! Updated from three idempotent (max-wins) sources: startup rebuild, the
//! durable-cursor publisher (so standbys learn of replicated tombstones), and
//! `delete_stream` itself (synchronous, to avoid publisher lag).

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use crate::record::DecodedRecord;

/// Reserved event type marking a stream as deleted up to the record's seq.
pub const STREAM_DELETED_EVENT: &str = "logdb.stream_deleted";

/// Tracks the max deleted seq per stream (`stream_id → cutoff`; 0 = not deleted).
pub struct TombstoneTracker {
    cutoffs: Mutex<HashMap<u64, u64>>,
}

impl TombstoneTracker {
    pub fn new() -> Self {
        Self {
            cutoffs: Mutex::new(HashMap::new()),
        }
    }

    /// Record a tombstone at `seq` for `stream_id`. Idempotent — keeps the max.
    pub fn record(&self, stream_id: u64, seq: u64) {
        let mut m = self.cutoffs.lock().unwrap_or_else(PoisonError::into_inner);
        m.entry(stream_id)
            .and_modify(|e| *e = (*e).max(seq))
            .or_insert(seq);
    }

    /// Rebuild from a full segment scan (order irrelevant; we keep the per-stream max).
    pub fn rebuild_from_records(records: &[DecodedRecord]) -> Self {
        let t = Self::new();
        for r in records {
            if r.event_type == STREAM_DELETED_EVENT {
                t.record(r.stream_id, r.seq);
            }
        }
        t
    }

    /// A record is live iff its seq exceeds the stream's tombstone cutoff.
    /// seq starts at 1; absent cutoff ⇒ 0 ⇒ always live. The tombstone record
    /// itself has seq == cutoff ⇒ not live ⇒ filtered by the read paths.
    pub fn is_live(&self, stream_id: u64, seq: u64) -> bool {
        let m = self.cutoffs.lock().unwrap_or_else(PoisonError::into_inner);
        seq > m.get(&stream_id).copied().unwrap_or(0)
    }

    /// Current cutoff for a stream (0 = not deleted).
    pub fn cutoff(&self, stream_id: u64) -> u64 {
        self.cutoffs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&stream_id)
            .copied()
            .unwrap_or(0)
    }
}

impl Default for TombstoneTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::DecodedRecord;
    use std::collections::BTreeMap;

    fn rec(stream_id: u64, seq: u64, event_type: &str) -> DecodedRecord {
        DecodedRecord {
            namespace_id: 1, stream_id, seq,
            event_type: event_type.into(),
            content_type: "application/json".into(),
            metadata: BTreeMap::new(),
            timestamp_ns: seq,
            user_content: format!("c-{}", seq).into_bytes(),
        }
    }

    #[test]
    fn no_tombstone_means_all_live() {
        let t = TombstoneTracker::new();
        assert!(t.is_live(7, 1));
        assert!(t.is_live(7, 999));
        assert_eq!(t.cutoff(7), 0);
    }

    #[test]
    fn record_sets_cutoff_and_filters() {
        let t = TombstoneTracker::new();
        t.record(7, 100);
        assert!(!t.is_live(7, 100), "tombstone seq itself is not live");
        assert!(!t.is_live(7, 50),  "records at/below cutoff are deleted");
        assert!( t.is_live(7, 101), "records above cutoff stay live");
        assert_eq!(t.cutoff(7), 100);
    }

    #[test]
    fn record_is_idempotent_max() {
        let t = TombstoneTracker::new();
        t.record(7, 50);
        t.record(7, 200);
        t.record(7, 50);
        assert_eq!(t.cutoff(7), 200);
    }

    #[test]
    fn streams_are_independent() {
        let t = TombstoneTracker::new();
        t.record(7, 10);
        assert!(!t.is_live(7, 10));
        assert!( t.is_live(9, 10), "other stream unaffected");
    }

    #[test]
    fn rebuild_from_records_collects_tombstones() {
        let recs = vec![
            rec(7, 1, "user.input"),
            rec(7, 2, "tool.call"),
            rec(7, 3, STREAM_DELETED_EVENT),
            rec(7, 4, "user.input"),
            rec(9, 1, "tool.call"),
        ];
        let t = TombstoneTracker::rebuild_from_records(&recs);
        assert_eq!(t.cutoff(7), 3);
        assert!(!t.is_live(7, 2));
        assert!( t.is_live(7, 4));
        assert_eq!(t.cutoff(9), 0);
    }

    #[test]
    fn record_zero_leaves_stream_live() {
        // seq starts at 1 in practice; a defensive record(_, 0) must not
        // make any real (seq ≥ 1) record appear deleted.
        let t = TombstoneTracker::new();
        t.record(7, 0);
        assert!(t.is_live(7, 1));
        assert!(t.is_live(7, 999));
    }

    #[test]
    fn rebuild_empty_slice_means_all_live() {
        let t = TombstoneTracker::rebuild_from_records(&[]);
        assert!(t.is_live(7, 1));
        assert_eq!(t.cutoff(7), 0);
    }
}

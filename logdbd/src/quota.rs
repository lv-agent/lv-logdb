//! In-memory per-stream usage counts for quota enforcement.
//!
//! Seeded once at startup from a committed-prefix segment scan (respecting
//! tombstones), then maintained incrementally: [`QuotaTracker::add`] on every
//! successful append, [`QuotaTracker::reset`] on `delete_stream`. O(1) on the
//! append hot path — avoids a full segment scan per append.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use crate::record::DecodedRecord;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub records: u64,
    pub bytes: u64,
}

/// Per-stream usage, keyed by `(namespace_id, stream_id)`.
pub struct QuotaTracker {
    usage: Mutex<HashMap<(u32, u64), Usage>>,
}

impl QuotaTracker {
    pub fn new() -> Self {
        Self {
            usage: Mutex::new(HashMap::new()),
        }
    }

    /// Seed from a full segment scan. `is_live` excludes tombstoned records;
    /// tombstone records themselves are always skipped.
    pub fn seed_from_records<F>(records: &[DecodedRecord], mut is_live: F) -> Self
    where
        F: FnMut(u64, u64) -> bool,
    {
        let t = Self::new();
        {
            let mut m = t.usage.lock().unwrap_or_else(PoisonError::into_inner);
            for r in records {
                if r.event_type == crate::tombstone::STREAM_DELETED_EVENT {
                    continue;
                }
                if !is_live(r.stream_id, r.seq) {
                    continue;
                }
                let u = m.entry((r.namespace_id, r.stream_id)).or_default();
                u.records += 1;
                u.bytes += r.user_content.len() as u64;
            }
        }
        t
    }

    pub fn add(&self, namespace_id: u32, stream_id: u64, bytes: usize) {
        let mut m = self.usage.lock().unwrap_or_else(PoisonError::into_inner);
        let u = m.entry((namespace_id, stream_id)).or_default();
        u.records += 1;
        u.bytes += bytes as u64;
    }

    pub fn reset(&self, namespace_id: u32, stream_id: u64) {
        self.usage
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((namespace_id, stream_id), Usage::default());
    }

    pub fn usage(&self, namespace_id: u32, stream_id: u64) -> Usage {
        self.usage
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(namespace_id, stream_id))
            .copied()
            .unwrap_or_default()
    }
}

impl Default for QuotaTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::DecodedRecord;
    use std::collections::BTreeMap;

    fn rec(ns: u32, stream: u64, seq: u64, bytes: usize, et: &str) -> DecodedRecord {
        DecodedRecord {
            namespace_id: ns, stream_id: stream, seq,
            event_type: et.into(),
            content_type: "application/json".into(),
            metadata: BTreeMap::new(),
            timestamp_ns: seq,
            user_content: vec![0u8; bytes],
        }
    }

    #[test]
    fn empty_usage_for_unknown_stream() {
        let t = QuotaTracker::new();
        let u = t.usage(1, 7);
        assert_eq!(u.records, 0);
        assert_eq!(u.bytes, 0);
    }

    #[test]
    fn add_accumulates() {
        let t = QuotaTracker::new();
        t.add(1, 7, 10);
        t.add(1, 7, 5);
        let u = t.usage(1, 7);
        assert_eq!(u.records, 2);
        assert_eq!(u.bytes, 15);
    }

    #[test]
    fn reset_zeros_a_stream() {
        let t = QuotaTracker::new();
        t.add(1, 7, 10);
        t.reset(1, 7);
        let u = t.usage(1, 7);
        assert_eq!(u.records, 0);
        assert_eq!(u.bytes, 0);
    }

    #[test]
    fn streams_are_independent() {
        let t = QuotaTracker::new();
        t.add(1, 7, 10);
        t.add(1, 9, 20);
        assert_eq!(t.usage(1, 7).records, 1);
        assert_eq!(t.usage(1, 9).bytes, 20);
    }

    #[test]
    fn seed_counts_live_records_excluding_tombstones() {
        let recs = vec![
            rec(1, 7, 1, 4, "user.input"),
            rec(1, 7, 2, 8, "tool.call"),
            rec(1, 7, 3, 0, crate::tombstone::STREAM_DELETED_EVENT), // tombstone, not counted
            rec(1, 7, 4, 6, "user.input"), // seq 4 > cutoff 3 ⇒ live
            rec(1, 9, 1, 5, "tool.call"),
        ];
        // is_live predicate mirrors a TombstoneTracker with cutoff(7)=3.
        let t = QuotaTracker::seed_from_records(&recs, |sid, seq| !(sid == 7 && seq <= 3));
        let u7 = t.usage(1, 7);
        assert_eq!(u7.records, 1, "only seq-4 record of stream 7 is live");
        assert_eq!(u7.bytes, 6);
        let u9 = t.usage(1, 9);
        assert_eq!(u9.records, 1);
        assert_eq!(u9.bytes, 5);
    }

    #[test]
    fn seed_with_always_live_counts_all_non_tombstone() {
        let recs = vec![
            rec(1, 7, 1, 4, "user.input"),
            rec(1, 7, 2, 0, crate::tombstone::STREAM_DELETED_EVENT),
        ];
        let t = QuotaTracker::seed_from_records(&recs, |_, _| true);
        let u = t.usage(1, 7);
        assert_eq!(u.records, 1, "tombstone record itself is never counted");
        assert_eq!(u.bytes, 4);
    }

    #[test]
    fn add_for_new_stream_starts_from_zero() {
        let t = QuotaTracker::new();
        t.add(2, 5, 7);
        let u = t.usage(2, 5);
        assert_eq!(u.records, 1);
        assert_eq!(u.bytes, 7);
    }
}

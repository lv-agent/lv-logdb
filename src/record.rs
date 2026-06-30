//! Record types: the logical identifier, in-memory read view, and on-disk record.
//!
//! # RecordId
//!
//! `RecordId` is the logical position of a record in the log. It follows Kafka's
//! partition-offset semantics: a `(partition_id, sequence)` tuple, NOT a
//! single compressed u64 with encoded physical topology.
//!
//! - `partition_id` identifies a logical partition (0 for single-partition v1.0)
//! - `sequence` is a partition-local monotonically increasing u64
//!
//! For the common single-partition case, `RecordId` implements `Into<u64>`
//! (returns the sequence directly) and `Display` shows just the sequence.
//!
//! Shard IDs and node IDs are NOT encoded in RecordId — they are internal
//! implementation details that may change during rebalancing.

use std::fmt;

/// Logical position of a record in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordId {
    /// Logical partition identifier. 0 for single-partition v1.0.
    pub partition_id: u32,
    /// Monotonically increasing sequence number within the partition.
    /// With sharded rings, sequences from different shards are interleaved:
    /// `global_seq = local_seq * num_shards + shard_id`.
    pub sequence: u64,
}

impl RecordId {
    /// Create a new RecordId.
    #[inline]
    pub fn new(partition_id: u32, sequence: u64) -> Self {
        Self {
            partition_id,
            sequence,
        }
    }

    /// The default record ID for single-partition usage (partition 0, sequence 0).
    pub const ZERO: Self = Self {
        partition_id: 0,
        sequence: 0,
    };
}

impl fmt::Display for RecordId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.partition_id == 0 {
            write!(f, "{}", self.sequence)
        } else {
            write!(f, "{}/{}", self.partition_id, self.sequence)
        }
    }
}

impl From<RecordId> for u64 {
    #[inline]
    fn from(id: RecordId) -> u64 {
        id.sequence
    }
}

impl From<u64> for RecordId {
    #[inline]
    fn from(sequence: u64) -> Self {
        Self {
            partition_id: 0,
            sequence,
        }
    }
}

/// A borrowed, zero-copy view of a record stored in a ring slot.
#[derive(Debug)]
pub struct ReadView<'a> {
    /// Global record identifier.
    pub record_id: u64,
    /// Timestamp in nanoseconds (CLOCK_REALTIME_COARSE).
    pub timestamp_ns: u64,
    /// Record content.
    pub content: &'a [u8],
    /// SHA-256 hash chain value (all zeros if hash is disabled).
    pub hash_n: &'a [u8; 32],
}

/// A fully owned record, typically read back from a segment file.
#[derive(Debug, Clone)]
pub struct Record {
    /// Logical record identifier.
    pub id: RecordId,
    /// Timestamp in nanoseconds.
    pub timestamp_ns: u64,
    /// Record content (owned).
    pub content: Vec<u8>,
    /// SHA-256 hash chain value.
    pub hash_n: [u8; 32],
}

impl Record {
    /// Create a new owned record.
    pub fn new(id: RecordId, timestamp_ns: u64, content: Vec<u8>, hash_n: [u8; 32]) -> Self {
        Self {
            id,
            timestamp_ns,
            content,
            hash_n,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_id_display_single_partition() {
        let id = RecordId::new(0, 42);
        assert_eq!(format!("{}", id), "42");
    }

    #[test]
    fn record_id_display_multi_partition() {
        let id = RecordId::new(3, 42);
        assert_eq!(format!("{}", id), "3/42");
    }

    #[test]
    fn record_id_into_u64() {
        let id = RecordId::new(0, 99);
        let seq: u64 = id.into();
        assert_eq!(seq, 99);
    }

    #[test]
    fn record_id_from_u64() {
        let id: RecordId = 99u64.into();
        assert_eq!(id.partition_id, 0);
        assert_eq!(id.sequence, 99);
    }

    #[test]
    fn record_id_ordering() {
        let a = RecordId::new(0, 10);
        let b = RecordId::new(0, 20);
        let c = RecordId::new(1, 5);
        assert!(a < b);
        // partition_id takes precedence in Ord
        assert!(a < c);
    }
}

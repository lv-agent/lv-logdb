//! Sharding — multi-ring support for high-core-count scalability.
//!
//! # Design (§7, §11)
//!
//! When `shards > 1`, logdb creates N independent ring buffers. Each producer
//! thread selects a shard via thread-ID hashing (thread-affine), reducing CAS
//! contention on the producer cursor.
//!
//! # RecordId Encoding
//!
//! Global record_ids are bit-encoded:
//!
//! ```text
//! record_id (u64):
//!   [63..SHARD_BITS] = per-shard sequence number (local_seq)
//!   [SHARD_BITS-1..0] = shard_id
//! ```
//!
//! When `shards == 1`, SHARD_BITS = 0 (no encoding overhead).
//! When `shards > 1`, SHARD_BITS = ceil(log2(shards)).
//!
//! This encoding gives a deterministic total order: records from different
//! shards with the same local_seq are ordered by shard_id. The Committer
//! processes shards round-robin, preserving approximate insertion order.

use std::cell::RefCell;
use std::sync::Arc;

use crate::config::QueueFullPolicy;
use crate::error::AppendError;
use crate::ring::Ring;

// ── RecordId encoding ──────────────────────────────────────────────────────

/// Compute the number of bits needed to represent `shards - 1`.
pub fn shard_bits(num_shards: usize) -> u32 {
    if num_shards <= 1 {
        return 0;
    }
    let max_shard_id = (num_shards - 1) as u64;
    64 - max_shard_id.leading_zeros()
}

/// Encode a (shard_id, local_seq) pair into a global record_id.
#[inline]
pub fn encode_record_id(shard_id: usize, local_seq: u64, shard_bits: u32) -> u64 {
    if shard_bits == 0 {
        local_seq
    } else {
        (local_seq << shard_bits) | (shard_id as u64)
    }
}

/// Decode a global record_id into (shard_id, local_seq).
#[inline]
pub fn decode_record_id(global_id: u64, shard_bits: u32) -> (usize, u64) {
    if shard_bits == 0 {
        (0, global_id)
    } else {
        let mask = (1u64 << shard_bits) - 1;
        ((global_id & mask) as usize, global_id >> shard_bits)
    }
}

// ── ShardMap ───────────────────────────────────────────────────────────────

/// Manages N independent ring buffers for multi-shard operation.
///
/// Each shard has its own `Ring` with independent cursors. The `ShardMap`
/// handles shard selection (thread-affine by default) and provides access
/// to individual rings for the Committer.
pub struct ShardMap {
    /// The ring buffers, one per shard.
    rings: Vec<Arc<Ring>>,
    /// Number of shard bits in the record_id encoding.
    shard_bits: u32,
}

impl ShardMap {
    /// Create a new ShardMap with `num_shards` rings, all resuming from the same
    /// `initial_seq` (fresh create, or single-shard recovery). Equivalent to
    /// [`new_with_initial`](Self::new_with_initial) with a uniform sequence.
    ///
    /// Each ring gets `ring_size / num_shards` slots so the total slot count
    /// across all shards equals `ring_size`. `initial_seq` is the per-shard
    /// starting LOCAL sequence (0 for fresh, last_local+1 for recovery).
    pub fn new(num_shards: usize, ring_size: usize, hash_enabled: bool, initial_seq: u64) -> Self {
        Self::new_with_initial(
            num_shards,
            ring_size,
            hash_enabled,
            &vec![initial_seq; num_shards],
        )
    }

    /// Create a ShardMap where each shard's ring resumes from its OWN
    /// `initial_seqs[s]` (per-shard recovery: each shard may have recovered a
    /// different last-local sequence). `initial_seqs.len()` must equal
    /// `num_shards`; each value is the LOCAL sequence at which that shard's ring
    /// resumes (0 for a fresh/empty shard).
    pub fn new_with_initial(
        num_shards: usize,
        ring_size: usize,
        hash_enabled: bool,
        initial_seqs: &[u64],
    ) -> Self {
        assert!(num_shards >= 1, "num_shards must be >= 1");
        assert_eq!(
            initial_seqs.len(),
            num_shards,
            "initial_seqs length must match num_shards"
        );
        let per_shard_slots: usize = (ring_size / num_shards).next_power_of_two().max(16);

        let sb = shard_bits(num_shards);
        let rings: Vec<Arc<Ring>> = (0..num_shards)
            .map(|s| Arc::new(Ring::new(per_shard_slots, hash_enabled, initial_seqs[s])))
            .collect();

        Self {
            rings,
            shard_bits: sb,
        }
    }

    /// Create a ShardMap from an existing single ring (for backward compat).
    pub fn from_single_ring(ring: Arc<Ring>) -> Self {
        Self {
            rings: vec![ring],
            shard_bits: 0,
        }
    }

    /// Get the number of shards.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.rings.len()
    }

    /// Get the number of shard bits used in record_id encoding.
    #[inline]
    pub fn shard_bits(&self) -> u32 {
        self.shard_bits
    }

    /// Get a reference to a specific shard's ring.
    #[inline]
    pub fn ring(&self, shard_id: usize) -> &Arc<Ring> {
        &self.rings[shard_id]
    }

    /// Get references to all rings (for Committer polling).
    #[inline]
    pub fn all_rings(&self) -> &[Arc<Ring>] {
        &self.rings
    }

    /// Select a shard for the current thread.
    ///
    /// Uses thread-ID hashing for thread affinity. Falls back to a
    /// round-robin counter if thread ID is unavailable.
    pub fn select_shard(&self) -> usize {
        THREAD_SHARD_KEY.with(|key| {
            let mut key = key.borrow_mut();
            if *key == usize::MAX {
                // Lazy init: hash the thread ID
                *key = thread_shard_index(self.num_shards());
            }
            *key
        })
    }

    /// Select a shard deterministically from a caller-supplied key.
    ///
    /// Same key ⇒ same shard, across calls, threads, and process restarts
    /// (the hash is unseeded). This is the Kafka/Kinesis partitioning model:
    /// route by entity key (session id, user id, …) so all records for one
    /// entity land on one shard and stay ordered. Uses CRC32C (already a logdb
    /// dependency) — deterministic, fast, well-distributed for 1..=256 shards.
    pub fn select_shard_by_key(&self, key: &[u8]) -> usize {
        crc32c::crc32c(key) as usize % self.num_shards()
    }

    /// Claim a sequence number from a caller-specified shard.
    ///
    /// Returns `(global_record_id, shard_id, local_seq)`. Used by
    /// [`LogDb::append_with_key`] after key-based shard selection. Bounds:
    /// `shard_id` must be `< num_shards()` (panics otherwise, matching
    /// [`ring`](Self::ring)'s indexing).
    #[inline]
    pub fn claim_on_shard(
        &self,
        shard_id: usize,
        policy: QueueFullPolicy,
    ) -> Result<(u64, usize, u64), AppendError> {
        let ring = &self.rings[shard_id];
        let local_seq = ring.claim(policy)?;
        let global_id = encode_record_id(shard_id, local_seq, self.shard_bits);
        Ok((global_id, shard_id, local_seq))
    }

    /// Claim a sequence number from the thread-affine-selected shard.
    ///
    /// Returns `(global_record_id, shard_id, local_seq)`.
    #[inline]
    pub fn claim(&self, policy: QueueFullPolicy) -> Result<(u64, usize, u64), AppendError> {
        let shard_id = self.select_shard();
        self.claim_on_shard(shard_id, policy)
    }

    /// Atomically reserve `n` consecutive sequences on the selected shard for a
    /// batch append. Returns `(global_first_id, shard_id, local_first_seq)`.
    /// The whole batch is reserved all-or-none (see [`Ring::claim_batch`]).
    #[inline]
    pub fn claim_batch(
        &self,
        n: u64,
        policy: QueueFullPolicy,
    ) -> Result<(u64, usize, u64), AppendError> {
        let shard_id = self.select_shard();
        let ring = &self.rings[shard_id];
        let local_seq = ring.claim_batch(n, policy)?;
        let global_id = encode_record_id(shard_id, local_seq, self.shard_bits);
        Ok((global_id, shard_id, local_seq))
    }

    /// Get the producer cursor value for the highest shard (worst-case target
    /// for flush/shutdown).
    pub fn max_producer_cursor(&self) -> u64 {
        self.rings
            .iter()
            .map(|r| r.producer_cursor_value())
            .max()
            .unwrap_or(0)
    }

    /// Get the minimum committed cursor across all shards.
    pub fn min_committed_cursor(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.rings
            .iter()
            .map(|r| r.committed_cursor.load(Ordering::Acquire))
            .min()
            .unwrap_or(0)
    }

    /// Get the minimum durable cursor across all shards.
    pub fn min_durable_cursor(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.rings
            .iter()
            .map(|r| r.durable_cursor.load(Ordering::Acquire))
            .min()
            .unwrap_or(0)
    }

    /// Per-shard producer cursors (snapshot for flush/drain).
    pub fn producer_cursors(&self) -> Vec<u64> {
        self.rings
            .iter()
            .map(|r| r.producer_cursor_value())
            .collect()
    }

    /// Per-shard durable cursors.
    pub fn durable_cursors(&self) -> Vec<u64> {
        use std::sync::atomic::Ordering;
        self.rings
            .iter()
            .map(|r| r.durable_cursor.load(Ordering::Acquire))
            .collect()
    }
}

// ── Thread-local shard key ─────────────────────────────────────────────────

thread_local! {
    /// Cached shard index for this thread. usize::MAX = not initialized.
    static THREAD_SHARD_KEY: RefCell<usize> = RefCell::new(usize::MAX);
}

/// Derive a shard index from the current thread's ID.
fn thread_shard_index(num_shards: usize) -> usize {
    // Use the thread ID's hash as a cheap proxy for thread affinity.
    // std::thread::current().id() returns a ThreadId which implements Hash
    // via its underlying u64.
    use std::hash::{Hash, Hasher};
    let tid = std::thread::current().id();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    tid.hash(&mut hasher);
    hasher.finish() as usize % num_shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_bits_computation() {
        assert_eq!(shard_bits(1), 0);
        assert_eq!(shard_bits(2), 1);
        assert_eq!(shard_bits(4), 2);
        assert_eq!(shard_bits(8), 3);
        assert_eq!(shard_bits(16), 4);
        assert_eq!(shard_bits(256), 8);
    }

    #[test]
    fn encode_decode_round_trip() {
        for shards in [2, 4, 8] {
            let sb = shard_bits(shards);
            for shard_id in 0..shards {
                for seq in [0u64, 1, 100, u32::MAX as u64] {
                    let global = encode_record_id(shard_id, seq, sb);
                    let (decoded_shard, decoded_seq) = decode_record_id(global, sb);
                    assert_eq!(
                        decoded_shard, shard_id,
                        "shards={} sb={} shard_id={} seq={} global={}",
                        shards, sb, shard_id, seq, global
                    );
                    assert_eq!(decoded_seq, seq);
                }
            }
        }
    }

    #[test]
    fn encode_decode_single_shard() {
        // shards=1 → shard_bits=0 → identity encoding
        let sb = shard_bits(1);
        assert_eq!(sb, 0);
        for seq in [0u64, 1, 42, u64::MAX] {
            let global = encode_record_id(0, seq, sb);
            assert_eq!(global, seq);
            let (shard, decoded_seq) = decode_record_id(global, sb);
            assert_eq!(shard, 0);
            assert_eq!(decoded_seq, seq);
        }
    }

    #[test]
    fn shard_map_creation() {
        let sm = ShardMap::new(4, 8192, false, 0);
        assert_eq!(sm.num_shards(), 4);
        assert_eq!(sm.shard_bits(), 2); // ceil(log2(4)) = 2

        // Each shard should have ring_size/4 slots (rounded to power of two)
        let expected_slots: usize = (8192usize / 4).next_power_of_two(); // 2048
        assert_eq!(sm.ring(0).ring_size(), expected_slots);
    }

    #[test]
    fn shard_map_single_shard() {
        let sm = ShardMap::new(1, 8192, false, 0);
        assert_eq!(sm.num_shards(), 1);
        assert_eq!(sm.shard_bits(), 0);
    }

    #[test]
    fn claim_from_shard_map() {
        let sm = ShardMap::new(4, 8192, false, 0);
        let (global_id, shard_id, local_seq) = sm.claim(QueueFullPolicy::Block).unwrap();
        assert_eq!(local_seq, 0);
        assert!(shard_id < 4);
        let (decoded_shard, decoded_seq) = decode_record_id(global_id, sm.shard_bits());
        assert_eq!(decoded_shard, shard_id);
        assert_eq!(decoded_seq, 0);
    }

    #[test]
    fn claims_are_globally_unique() {
        use std::collections::HashSet;
        let sm = ShardMap::new(4, 8192, false, 0);
        let mut ids = HashSet::new();

        for _ in 0..400 {
            let (global_id, _, _) = sm.claim(QueueFullPolicy::Block).unwrap();
            assert!(ids.insert(global_id), "duplicate global_id: {}", global_id);
        }
    }

    #[test]
    fn multi_thread_shard_selection() {
        use std::collections::HashSet;
        let sm = Arc::new(ShardMap::new(8, 8192, false, 0));
        let mut handles = vec![];

        for _ in 0..8 {
            let sm = Arc::clone(&sm);
            handles.push(std::thread::spawn(move || {
                let shard_id = sm.select_shard();
                // Same thread should always get the same shard
                for _ in 0..100 {
                    assert_eq!(
                        sm.select_shard(),
                        shard_id,
                        "thread shard selection should be stable"
                    );
                }
                shard_id
            }));
        }

        let shard_ids: Vec<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Not all threads should go to the same shard
        let unique: HashSet<usize> = shard_ids.iter().copied().collect();
        assert!(unique.len() > 1, "threads should distribute across shards");
    }

    // ── key-based routing (cr-037) ───────────────────────────────────────────

    #[test]
    fn select_shard_by_key_is_deterministic() {
        let sm = ShardMap::new(8, 8192, false, 0);
        let key = b"session-42";
        let s = sm.select_shard_by_key(key);
        for _ in 0..50 {
            assert_eq!(
                sm.select_shard_by_key(key),
                s,
                "same key must always map to the same shard"
            );
        }
    }

    #[test]
    fn select_shard_by_key_within_bounds() {
        let sm = ShardMap::new(16, 8192, false, 0);
        for i in 0..1000u32 {
            let s = sm.select_shard_by_key(&i.to_le_bytes());
            assert!(s < 16, "shard {s} out of bounds for num_shards=16");
        }
    }

    #[test]
    fn select_shard_by_key_distributes() {
        use std::collections::HashSet;
        let sm = ShardMap::new(8, 8192, false, 0);
        let mut unique = HashSet::new();
        for i in 0..1000u32 {
            unique.insert(sm.select_shard_by_key(format!("key-{i}").as_bytes()));
        }
        // 1000 distinct keys over 8 shards should exercise all of them.
        assert_eq!(
            unique.len(),
            8,
            "poor distribution: only {} of 8 shards used",
            unique.len()
        );
    }

    #[test]
    fn select_shard_by_key_single_shard() {
        let sm = ShardMap::new(1, 8192, false, 0);
        for key in [b"".as_slice(), b"a", b"some-long-session-key"] {
            assert_eq!(sm.select_shard_by_key(key), 0);
        }
    }

    #[test]
    fn claim_on_shard_uses_specified_shard() {
        let sm = ShardMap::new(4, 8192, false, 0);
        for target in 0..4 {
            let (global_id, shard_id, local_seq) =
                sm.claim_on_shard(target, QueueFullPolicy::Block).unwrap();
            assert_eq!(shard_id, target, "claim_on_shard returned the wrong shard");
            let (decoded_shard, decoded_seq) = decode_record_id(global_id, sm.shard_bits());
            assert_eq!(decoded_shard, target);
            assert_eq!(decoded_seq, local_seq);
        }
    }
}

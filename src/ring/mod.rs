//! Ring buffer — the heart of logdb's lock-free append path.
//!
//! The ring buffer provides bounded, lock-free, multi-producer slots for
//! record storage. Producers CAS-claim sequence numbers, write directly into
//! slots, then publish via Release stores. Consumers read via Acquire loads
//! and are gated by a single consume watermark.
//!
//! # Architecture
//!
//! ```text
//! Producer threads:  claim(seq) → producer_write(seq, …) → publish(seq)
//! Sealer thread:     scan published → compute hash_n → write_hash → advance sealed_cursor
//! Committer thread:  scan published/sealed → serialize → pwrite → advance committed_cursor
//!                    → fdatasync → advance durable_cursor
//!
//! consume_watermark = min(sealed_cursor, committed_cursor) if hash_enabled
//!                     else committed_cursor
//! ```
//!
//! # Single Watermark
//!
//! Slot reuse is gated by a single `consume_watermark`. A producer can only
//! claim slot for `seq` when `seq - consume_watermark < ring_size`. Since
//! content lives in the slot (not a separate arena), there is exactly one
//! resource with one watermark — no dual-watermark panic possible.

pub mod slot;

use std::hint;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use slot::Slot;

use crate::config::QueueFullPolicy;
use crate::error::AppendError;

// ── Cache-line padding ─────────────────────────────────────────────────────

/// Wraps a value with 64-byte alignment, isolating it on its own cache line.
///
/// Used for `producer_cursor` to prevent false sharing with `sealed_cursor`
/// and `committed_cursor`, which are written by background threads. Without
/// padding, a write to `committed_cursor` invalidates the cache line shared
/// with `producer_cursor`, forcing all producers to reload on their next CAS.
#[repr(align(64))]
pub(crate) struct CachePadded<T> {
    pub(crate) inner: T,
}

impl<T> CachePadded<T> {
    pub(crate) fn new(val: T) -> Self {
        Self { inner: val }
    }
}

// ── Ring ───────────────────────────────────────────────────────────────────

/// Ring buffer with CAS-based multi-producer claim.
///
/// # Cache-line layout
///
/// `producer_cursor` is isolated on its own cache line to avoid false sharing
/// with the consumer-side atomics (`sealed_cursor`, `committed_cursor`).
/// On x86-64 with 64-byte cache lines, this means `producer_cursor` occupies
/// bytes 0-63 and the remaining fields start at byte 64.
pub struct Ring {
    /// Pre-allocated slots. Power-of-two length, never resized.
    slots: Box<[Slot]>,
    /// Mask for index computation: `seq & mask` → slot index.
    mask: u64,
    /// Total number of slots (= slots.len() as u64).
    ring_size: u64,

    /// Next sequence number to be claimed by a producer. CAS-advanced.
    /// Isolated on its own cache line to prevent false sharing.
    pub(crate) producer_cursor: CachePadded<AtomicU64>,

    /// Sequence number up to which the Sealer has computed hash_n.
    pub(crate) sealed_cursor: AtomicU64,
    /// Sequence number up to which the Committer has written to page cache.
    pub(crate) committed_cursor: AtomicU64,
    /// Sequence number up to which the Committer has fsynced.
    pub(crate) durable_cursor: AtomicU64,
    /// Whether the hash chain (Sealer) is enabled.
    hash_enabled: bool,
}

impl Ring {
    /// Create a new ring buffer.
    ///
    /// # Arguments
    /// - `ring_size`: number of slots, must be a power of two ≥ 16.
    /// - `hash_enabled`: whether the Sealer thread will compute hash_n.
    /// - `initial`: the starting sequence number (0 for fresh, last_record_id+1 for recovery).
    pub fn new(ring_size: usize, hash_enabled: bool, initial: u64) -> Self {
        assert!(ring_size.is_power_of_two() && ring_size >= 16);
        let ring_size_u64 = ring_size as u64;
        Self {
            slots: (0..ring_size)
                .map(|_| Slot::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            mask: ring_size_u64 - 1,
            ring_size: ring_size_u64,
            producer_cursor: CachePadded::new(AtomicU64::new(initial)),
            sealed_cursor: AtomicU64::new(initial),
            committed_cursor: AtomicU64::new(initial),
            durable_cursor: AtomicU64::new(initial),
            hash_enabled,
        }
    }

    /// Get the number of slots in the ring.
    #[inline]
    pub fn ring_size(&self) -> usize {
        self.ring_size as usize
    }

    /// Get the current producer cursor value (next unclaimed seq).
    #[inline]
    pub fn producer_cursor_value(&self) -> u64 {
        self.producer_cursor.inner.load(Ordering::Acquire)
    }

    /// Set committed cursor (for testing).
    #[inline]
    #[doc(hidden)]
    pub fn set_committed_cursor(&self, val: u64) {
        self.committed_cursor.store(val, Ordering::Release);
    }

    /// Set sealed cursor (for testing).
    #[inline]
    #[doc(hidden)]
    pub fn set_sealed_cursor(&self, val: u64) {
        self.sealed_cursor.store(val, Ordering::Release);
    }

    /// Compute the consume watermark — the slowest consumer's progress.
    ///
    /// This is the SINGLE gate for slot reuse. When hash is enabled, we must
    /// wait for both the Sealer and Committer. Otherwise, only the Committer
    /// matters.
    ///
    /// Returns the next sequence number that the slowest consumer will process
    /// (i.e., `min(sealed, committed) + 1` equivalent: the count of fully
    /// consumed records).
    #[inline]
    pub fn consume_watermark(&self) -> u64 {
        let committed = self.committed_cursor.load(Ordering::Acquire);
        if self.hash_enabled {
            let sealed = self.sealed_cursor.load(Ordering::Acquire);
            committed.min(sealed)
        } else {
            committed
        }
    }

    /// Claim the next available sequence number for writing.
    ///
    /// Uses bounded CAS: the producer can only claim `seq` if
    /// `seq - consume_watermark < ring_size`. This ensures there is a free
    /// slot available (the slot is not still being read by a consumer).
    ///
    /// # Arguments
    /// - `policy`: what to do when the ring is full (`Block` or `Drop`).
    ///
    /// # Returns
    /// - `Ok(seq)`: the claimed sequence number. The caller has exclusive
    ///   write access to `slots[seq & mask]`.
    /// - `Err(AppendError::QueueFull)`: ring is full and policy is `Drop`.
    #[inline]
    pub fn claim(&self, policy: QueueFullPolicy) -> Result<u64, AppendError> {
        let mut spins: u32 = 0;
        loop {
            let seq = self.producer_cursor.inner.load(Ordering::Acquire);
            let wm = self.consume_watermark();

            // in_flight = number of claimed-but-not-consumed records.
            // wm ≤ seq always holds because wm tracks consumed count (starting
            // at the same `initial` as producer_cursor) and consumers never
            // advance past what producers have published.
            // u64 subtraction cannot underflow.
            if seq.wrapping_sub(wm) >= self.ring_size {
                match policy {
                    QueueFullPolicy::Drop => {
                        return Err(AppendError::QueueFull);
                    }
                    QueueFullPolicy::Block => {
                        backoff(&mut spins);
                        continue;
                    }
                }
            }

            // CAS from seq to seq+1. On success, we own this sequence number.
            match self.producer_cursor.inner.compare_exchange_weak(
                seq,
                seq + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Drop policy failure path never reaches here — the
                    // Err return is above.
                    // u64 seq is valid.
                    return Ok(seq);
                }
                Err(_) => {
                    // CAS failed — another producer claimed this seq.
                    // Spin and retry.
                    hint::spin_loop();
                    continue;
                }
            }
        }
    }

    /// Get a reference to the slot for the given sequence number.
    ///
    /// Index is computed as `seq & mask`, which wraps correctly because
    /// `ring_size` is a power of two.
    #[inline]
    pub fn slot(&self, seq: u64) -> &Slot {
        &self.slots[(seq & self.mask) as usize]
    }

    /// Atomically reserve `n` consecutive sequence numbers for a batch append.
    ///
    /// A single CAS advances `producer_cursor` by `n`, so the whole batch is
    /// reserved **all-or-none** — no partial reservation, hence no gaps of
    /// reserved-but-unwritten slots that would stall the Committer. Returns the
    /// first sequence; the caller has exclusive write access to
    /// `slots[first & mask .. (first+n) & mask]` (wrapping).
    ///
    /// Backpressure is the same gate as `claim`: the batch fits only while
    /// `in_flight + n <= ring_size`. A batch larger than the ring can never fit
    /// and returns `QueueFull` immediately.
    pub fn claim_batch(&self, n: u64, policy: QueueFullPolicy) -> Result<u64, AppendError> {
        assert!(n >= 1, "claim_batch requires n >= 1");
        if n > self.ring_size {
            return Err(AppendError::QueueFull);
        }
        let mut spins: u32 = 0;
        loop {
            let seq = self.producer_cursor.inner.load(Ordering::Acquire);
            let wm = self.consume_watermark();
            // in_flight = claimed-but-not-consumed. Wrapping-sub is safe: wm <= seq
            // always holds (watermark tracks consumed count from the same initial).
            let in_flight = seq.wrapping_sub(wm);
            if in_flight + n > self.ring_size {
                match policy {
                    QueueFullPolicy::Drop => return Err(AppendError::QueueFull),
                    QueueFullPolicy::Block => {
                        backoff(&mut spins);
                        continue;
                    }
                }
            }
            // CAS reserve [seq, seq+n).
            match self.producer_cursor.inner.compare_exchange_weak(
                seq,
                seq + n,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(seq),
                Err(_) => {
                    hint::spin_loop();
                    continue;
                }
            }
        }
    }

    /// Find the highest contiguous published sequence number starting from
    /// `from_seq` (inclusive). Used by the Committer when hash is disabled
    /// to determine how far it can commit.
    ///
    /// Returns the highest `seq` such that all slots in `[from_seq, seq]`
    /// are published. Returns `from_seq - 1` if `from_seq` itself is not
    /// published.
    #[inline]
    pub fn highest_published_contiguous(&self, from_seq: u64) -> u64 {
        let mut seq = from_seq;
        loop {
            if !self.slot(seq).is_published(seq) {
                return seq.wrapping_sub(1);
            }
            seq = seq.wrapping_add(1);
        }
    }

    /// Check whether hash chain sealing is enabled.
    #[inline]
    pub fn hash_enabled(&self) -> bool {
        self.hash_enabled
    }
}

/// Backoff strategy for producers waiting on a full ring.
///
/// Phase 1: spin (CPU-bound, low latency for quickly-resolving contention)
/// Phase 2: yield (let other threads run, including consumers)
/// Phase 3: short sleep (relinquish CPU)
#[inline]
fn backoff(spins: &mut u32) {
    *spins = spins.saturating_add(1);
    if *spins <= 64 {
        // Phase 1: tight spin
        hint::spin_loop();
    } else if *spins <= 256 {
        // Phase 2: yield to the OS scheduler
        thread::yield_now();
    } else {
        // Phase 3: short park — consumers should have had time to drain
        thread::sleep(Duration::from_micros(100));
        // Cap spins to avoid overflow on very long waits.
        // After sleeping, reset to the yield phase so we don't stay in sleep.
        *spins = 128;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn new_ring_cursors_equal_initial() {
        let ring = Ring::new(16, false, 0);
        assert_eq!(ring.producer_cursor_value(), 0);
        assert_eq!(ring.consume_watermark(), 0);
    }

    #[test]
    fn new_ring_with_nonzero_initial() {
        let ring = Ring::new(16, false, 100);
        assert_eq!(ring.producer_cursor_value(), 100);
        assert_eq!(ring.consume_watermark(), 100);
    }

    #[test]
    fn claim_advances_cursor() {
        let ring = Ring::new(16, false, 0);
        let seq0 = ring.claim(QueueFullPolicy::Block).unwrap();
        assert_eq!(seq0, 0);
        assert_eq!(ring.producer_cursor_value(), 1);

        let seq1 = ring.claim(QueueFullPolicy::Block).unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(ring.producer_cursor_value(), 2);
    }

    #[test]
    fn claim_returns_queue_full_when_drop() {
        let ring = Ring::new(16, false, 0);

        // Fill the ring
        for i in 0..16 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            assert_eq!(seq, i);
        }

        // Ring is full (16 in-flight, waterline=0 so 16 >= 16)
        let err = ring.claim(QueueFullPolicy::Drop).unwrap_err();
        assert_eq!(err, AppendError::QueueFull);
    }

    #[test]
    fn claim_unblocks_after_consume() {
        let ring = Arc::new(Ring::new(16, false, 0));

        // Fill the ring
        for i in 0..16 {
            ring.claim(QueueFullPolicy::Block).unwrap();
        }

        // Spawn a thread that advances committed_cursor after a short delay
        let r = Arc::clone(&ring);
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            // Advance committed to 10 → waterline moves to 10
            // Now in-flight = 16 - 10 = 6 < 16 → claim should succeed
            r.committed_cursor.store(10, Ordering::Release);
        });

        // This should block briefly then succeed
        let seq = ring.claim(QueueFullPolicy::Block).unwrap();
        // seq should be 16 (the 17th claim)
        assert_eq!(seq, 16);

        handle.join().unwrap();
    }

    #[test]
    fn slot_index_wraps_correctly() {
        let ring = Ring::new(16, false, 0);
        assert_eq!(ring.slot(0) as *const Slot, ring.slot(16) as *const Slot);
        assert_eq!(ring.slot(5) as *const Slot, ring.slot(21) as *const Slot);
    }

    #[test]
    fn consume_watermark_without_hash() {
        let ring = Ring::new(16, false, 0);
        assert_eq!(ring.consume_watermark(), 0);
        ring.committed_cursor.store(5, Ordering::Release);
        assert_eq!(ring.consume_watermark(), 5);
    }

    #[test]
    fn consume_watermark_with_hash() {
        let ring = Ring::new(16, true, 0);
        // sealed=0, committed=0 → min=0
        assert_eq!(ring.consume_watermark(), 0);

        ring.sealed_cursor.store(3, Ordering::Release);
        ring.committed_cursor.store(5, Ordering::Release);
        // min(3, 5) = 3 (sealer is slower)
        assert_eq!(ring.consume_watermark(), 3);

        ring.sealed_cursor.store(10, Ordering::Release);
        // min(10, 5) = 5 (committer is slower)
        assert_eq!(ring.consume_watermark(), 5);
    }

    #[test]
    fn highest_published_contiguous_all_published() {
        let ring = Ring::new(16, false, 0);

        // Publish seqs 0, 1, 2
        for seq in 0..3 {
            unsafe {
                ring.slot(seq).producer_write(seq, 0, b"x");
            }
            ring.slot(seq).publish(seq);
        }

        let hi = ring.highest_published_contiguous(0);
        assert_eq!(hi, 2);
    }

    #[test]
    fn highest_published_contiguous_with_gap() {
        let ring = Ring::new(16, false, 0);

        // Publish 0, 1, but not 2
        for seq in 0..2 {
            unsafe {
                ring.slot(seq).producer_write(seq, 0, b"x");
            }
            ring.slot(seq).publish(seq);
        }

        let hi = ring.highest_published_contiguous(0);
        assert_eq!(hi, 1);
    }

    #[test]
    fn highest_published_contiguous_from_mid() {
        let ring = Ring::new(16, false, 0);

        // Publish 0, 1, 2, 3
        for seq in 0..4 {
            unsafe {
                ring.slot(seq).producer_write(seq, 0, b"x");
            }
            ring.slot(seq).publish(seq);
        }

        let hi = ring.highest_published_contiguous(2);
        assert_eq!(hi, 3);
    }

    #[test]
    fn highest_published_contiguous_none() {
        let ring = Ring::new(16, false, 0);
        // Nothing published — from_seq=0 is not published
        let hi = ring.highest_published_contiguous(0);
        assert_eq!(hi, u64::MAX); // wrapping_sub(1) on 0
    }

    #[test]
    fn multi_thread_claim_no_duplicates() {
        // Stress test: multiple threads claim sequence numbers concurrently.
        // Verify no duplicates and no gaps.
        use std::collections::HashSet;

        let ring = Arc::new(Ring::new(1024, false, 0));
        let num_threads = 8;
        let claims_per_thread = 100;

        let mut handles = vec![];
        for _ in 0..num_threads {
            let r = Arc::clone(&ring);
            handles.push(thread::spawn(move || {
                let mut claimed = Vec::with_capacity(claims_per_thread);
                for _ in 0..claims_per_thread {
                    let seq = r.claim(QueueFullPolicy::Block).unwrap();
                    claimed.push(seq);
                }
                claimed
            }));
        }

        let mut all_seqs = HashSet::new();
        for h in handles {
            let claimed = h.join().unwrap();
            for seq in claimed {
                assert!(all_seqs.insert(seq), "duplicate seq: {}", seq);
            }
        }

        // Verify no gaps: all seqs from 0 to (num_threads * claims_per_thread - 1)
        let total = (num_threads * claims_per_thread) as u64;
        for i in 0..total {
            assert!(all_seqs.contains(&i), "missing seq: {}", i);
        }
    }

    #[test]
    fn full_write_read_cycle() {
        // Integration of claim → producer_write → publish → read
        let ring = Ring::new(16, false, 0);

        let content = b"integration test";
        let seq = ring.claim(QueueFullPolicy::Block).unwrap();
        unsafe {
            ring.slot(seq).producer_write(seq, 5000, content);
        }
        ring.slot(seq).publish(seq);

        assert!(ring.slot(seq).is_published(seq));
        unsafe {
            let view = ring.slot(seq).read();
            assert_eq!(view.record_id, seq);
            assert_eq!(view.timestamp_ns, 5000);
            assert_eq!(view.content, content);
        }
    }

    #[test]
    fn ring_size_power_of_two_assert() {
        // These should all succeed
        Ring::new(16, false, 0);
        Ring::new(32, false, 0);
        Ring::new(1024, false, 0);
        Ring::new(8192, false, 0);
    }

    #[test]
    #[should_panic]
    fn ring_size_non_power_of_two_panics() {
        Ring::new(100, false, 0);
    }

    #[test]
    #[should_panic]
    fn ring_size_too_small_panics() {
        Ring::new(8, false, 0);
    }

    #[test]
    fn producer_cursor_cache_line_isolated() {
        // Verify that producer_cursor is on a different cache line from
        // sealed_cursor and committed_cursor (false-sharing mitigation).
        let ring = Ring::new(64, false, 0);
        let base = &ring as *const Ring as usize;

        let pc_offset = unsafe {
            let pc = &ring.producer_cursor as *const CachePadded<AtomicU64> as usize;
            pc - base
        };
        let sc_offset = unsafe {
            let sc = &ring.sealed_cursor as *const AtomicU64 as usize;
            sc - base
        };

        let cache_line = 64usize;
        // producer_cursor must be on a different cache line
        assert_ne!(
            pc_offset / cache_line,
            sc_offset / cache_line,
            "producer_cursor shares cache line with sealed_cursor (false sharing!)"
        );
        // producer_cursor must be 64-byte aligned
        assert_eq!(
            pc_offset % cache_line,
            0,
            "producer_cursor is not cache-line aligned"
        );
    }
}

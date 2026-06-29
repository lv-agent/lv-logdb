//! Slot — the unit of storage in the ring buffer.
//!
//! Each slot holds one record. Small records (≤ INLINE_CAP bytes) are stored
//! inline with zero allocation. Large records spill to the heap.
//!
//! # Safety Protocol
//!
//! Slots are accessed via a CAS-based claim protocol with three phases:
//!
//! **Phase 1 — Producer exclusive write:**
//! The producer CAS-claims a sequence number `seq` from `producer_cursor`.
//! The claim guarantees exclusive access to `slots[seq & mask]`. The producer
//! calls `producer_write` (unsafe, caller upholds exclusivity), then `publish`
//! which does a Release store of `seq + 1` into `sequence`.
//!
//! **Phase 2 — Consumer read:**
//! A consumer (Sealer or Committer) checks `is_published(seq)` via an Acquire
//! load. Once true, the consumer has read access and may call `read()` (unsafe,
//! caller upholds that `seq` has been published and not yet reclaimed).
//!
//! **Phase 3 — Slot reclamation:**
//! Slot reuse is gated by `consume_watermark` — the minimum of sealed_cursor
//! and committed_cursor. A slot is only eligible for producer claim when
//! `seq - consume_watermark < ring_size`. This guarantees the consumer has
//! finished reading before the slot is overwritten.
//!
//! # Why UnsafeCell
//!
//! `SlotInner` is wrapped in `UnsafeCell` because we need interior mutability
//! through `&Slot` — the producer writes through a shared reference, and the
//! Sealer writes `hash_n` through a shared reference. The CAS claim protocol
//! guarantees these accesses never alias.
//!
//! # Sync Safety
//!
//! `Slot` is `Sync` despite containing `UnsafeCell` because concurrent access
//! to `SlotInner` is prevented by the claim protocol: for any given slot index,
//! only one thread (producer, sealer, or committer) holds a valid reference at
//! any time. The `sequence` AtomicU64 serves as the visibility and exclusion
//! mechanism.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::record::ReadView;

/// Maximum size for inline content storage.
///
/// Records ≤ 256 bytes are stored directly in the slot with **zero allocation**
/// and **zero extra copy** across threads. This is the fast path.
///
/// # Performance Boundary
///
/// Records **exceeding** this size take the **spill path**:
/// - A heap allocation (`Box<[u8]>`) is performed in the append thread
/// - A memcpy of the full content is required
/// - Tail latency (p99.9) is ~80x higher than inline due to allocator jitter
///   (observed: inline p99.9=500ns vs spill p99.9=41μs at 300B)
///
/// For latency-sensitive workloads, keep records ≤ 256B to stay on the
/// inline fast path.
///
/// # Choice of 256
///
/// 256 bytes covers the vast majority of structured log records (JSON log
/// lines, audit events, metrics samples) while keeping SlotInner cache-line
/// friendly (inline storage occupies exactly 4 cache lines).
pub const INLINE_CAP: usize = 256;

/// Where the record content lives.
///
/// Small records use `Inline` (zero allocation, zero copy).
/// Large records use `Spill` (heap allocation, noted as slow path).
enum ContentStorage {
    /// Small record: content embedded directly in the slot.
    Inline([u8; INLINE_CAP]),
    /// Large record: content spilled to the heap.
    Spill(Box<[u8]>),
}

/// The inner data of a slot.
///
/// `#[repr(C)]` ensures predictable field ordering. All fields are private
/// and only accessed through Slot's (unsafe) methods.
#[repr(C)]
pub(crate) struct SlotInner {
    /// Global record identifier assigned to this record.
    pub(crate) record_id: u64,
    /// Timestamp in nanoseconds (CLOCK_REALTIME_COARSE).
    pub(crate) timestamp_ns: u64,
    /// Length of the content in bytes.
    pub(crate) content_len: u32,
    /// Where the content is stored (inline or heap).
    storage: ContentStorage,
    /// SHA-256 hash chain value. Only populated when `hash_enabled` is true;
    /// all zeros otherwise.
    pub(crate) hash_n: [u8; 32],
}

/// A slot in the ring buffer.
///
/// # Thread Safety
///
/// `Slot` implements `Sync` (see module-level safety documentation).
pub struct Slot {
    /// The slot's data, protected by the CAS claim protocol.
    inner: UnsafeCell<SlotInner>,
    /// Visibility and exclusion mechanism.
    ///
    /// Stores `seq + 1` after publish (via Release store).
    /// 0 means the slot is empty / uninitialized.
    sequence: AtomicU64,
}

// SAFETY: See module-level documentation.
// Access to SlotInner is gated by the CAS claim protocol using the sequence
// AtomicU64. For any given slot index, at most one thread holds a valid
// reference at any time:
//   - Producer: between claim (CAS success) and publish (Release store)
//   - Sealer:    between publish (Acquire load) and write_hash
//   - Committer: between sealed (Acquire load) and consume_watermark advance
// These intervals are disjoint per slot index, so no aliasing occurs.
unsafe impl Sync for Slot {}

impl Slot {
    /// Create a new empty slot.
    pub(crate) fn new() -> Self {
        Self {
            inner: UnsafeCell::new(SlotInner {
                record_id: 0,
                timestamp_ns: 0,
                content_len: 0,
                storage: ContentStorage::Inline([0u8; INLINE_CAP]),
                hash_n: [0u8; 32],
            }),
            sequence: AtomicU64::new(0),
        }
    }

    /// Producer writes content into the slot.
    ///
    /// # Safety
    ///
    /// The caller must hold the exclusive claim for `seq` on this slot.
    /// This is guaranteed by the CAS claim in `Ring::claim()` succeeding
    /// for this `seq` before calling this method.
    #[inline]
    pub unsafe fn producer_write(&self, seq: u64, ts: u64, content: &[u8]) {
        let inner = &mut *self.inner.get();
        inner.record_id = seq;
        inner.timestamp_ns = ts;
        inner.content_len = content.len() as u32;

        if content.len() <= INLINE_CAP {
            // Fast path: inline storage, zero allocation, zero copy.
            match &mut inner.storage {
                ContentStorage::Inline(buf) => {
                    buf[..content.len()].copy_from_slice(content);
                }
                _ => {
                    // Previous record was a spill; switch back to inline.
                    // This drops the old Box<[u8]>, freeing the heap memory.
                    let mut buf = [0u8; INLINE_CAP];
                    buf[..content.len()].copy_from_slice(content);
                    inner.storage = ContentStorage::Inline(buf);
                }
            }
        } else {
            // Slow path: spill to heap.
            // NOTE: this is the ONLY allocation in the append fast path,
            // and only triggers for records > 256 bytes.
            inner.storage = ContentStorage::Spill(content.to_vec().into_boxed_slice());
        }
        // Note: sequence Release store is done separately in publish().
    }

    /// Publish the slot, making it visible to consumers.
    ///
    /// This performs a Release store of `seq + 1`, which synchronizes with
    /// the consumer's Acquire load in `is_published()`.
    ///
    /// Must be called after `producer_write` for the same `seq`.
    #[inline]
    pub fn publish(&self, seq: u64) {
        self.sequence.store(seq + 1, Ordering::Release);
    }

    /// Check whether the slot has been published for the given sequence number.
    ///
    /// Uses Acquire ordering to ensure visibility of the producer's writes.
    #[inline]
    pub fn is_published(&self, seq: u64) -> bool {
        self.sequence.load(Ordering::Acquire) == seq + 1
    }

    /// Read the published record from this slot.
    ///
    /// Returns a zero-copy `ReadView` borrowing the slot's internal storage.
    ///
    /// # Safety
    ///
    /// The caller must have confirmed `is_published(seq)` returned `true`,
    /// and must ensure the slot will not be reclaimed (i.e., the consume
    /// watermark has not advanced past `seq`) while the returned `ReadView`
    /// is alive.
    #[inline]
    pub unsafe fn read(&self) -> ReadView<'_> {
        let inner = &*self.inner.get();
        let content = match &inner.storage {
            ContentStorage::Inline(buf) => &buf[..inner.content_len as usize],
            ContentStorage::Spill(b) => &b[..inner.content_len as usize],
        };
        ReadView {
            record_id: inner.record_id,
            timestamp_ns: inner.timestamp_ns,
            content,
            hash_n: &inner.hash_n,
        }
    }

    /// Write the hash chain value into this slot.
    ///
    /// Called by the Sealer thread after reading the content.
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `is_published(seq)` is true
    /// - The Committer has not yet consumed (read) this slot
    /// - In practice: `sealed_cursor < seq` and the Sealer is the only
    ///   thread that writes hash_n
    #[inline]
    pub unsafe fn write_hash(&self, hash_n: [u8; 32]) {
        (&mut *self.inner.get()).hash_n = hash_n;
    }

    /// Get the current sequence value (for debugging/testing).
    #[inline]
    pub(crate) fn sequence_value(&self) -> u64 {
        self.sequence.load(Ordering::Relaxed)
    }
}

// SlotInner does not need to be Send (it doesn't own heap data directly),
// but the ContentStorage::Spill variant contains Box<[u8]> which is Send.
// The compiler auto-derives Send for Slot (AtomicU64: Send, UnsafeCell<SlotInner>: Send
// when SlotInner: Send).
//
// We explicitly verify in tests that Slot: Send + Sync.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_is_send_and_sync() {
        // Static assertions: if these don't compile, the safety guarantees are broken.
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Slot>();
        assert_sync::<Slot>();
    }

    #[test]
    fn inline_write_and_read() {
        let slot = Slot::new();
        let content = b"hello logdb";

        unsafe {
            slot.producer_write(42, 1000, content);
        }
        slot.publish(42);

        assert!(slot.is_published(42));
        assert!(!slot.is_published(43)); // wrong seq

        unsafe {
            let view = slot.read();
            assert_eq!(view.record_id, 42);
            assert_eq!(view.timestamp_ns, 1000);
            assert_eq!(view.content, content);
            assert_eq!(view.hash_n, &[0u8; 32]);
        }
    }

    #[test]
    fn spill_write_and_read() {
        let slot = Slot::new();
        let content = vec![0xAAu8; 300]; // > INLINE_CAP

        unsafe {
            slot.producer_write(7, 2000, &content);
        }
        slot.publish(7);

        assert!(slot.is_published(7));

        unsafe {
            let view = slot.read();
            assert_eq!(view.record_id, 7);
            assert_eq!(view.timestamp_ns, 2000);
            assert_eq!(view.content, &content[..]);
        }
    }

    #[test]
    fn switch_from_spill_to_inline() {
        let slot = Slot::new();

        // First write: spill
        let large = vec![0xBBu8; 300];
        unsafe { slot.producer_write(1, 100, &large); }
        slot.publish(1);
        unsafe {
            let view = slot.read();
            assert_eq!(view.content.len(), 300);
        }

        // Second write: inline (reuses the same slot, frees the old Box)
        let small = b"small";
        unsafe { slot.producer_write(2, 200, small); }
        slot.publish(2);
        unsafe {
            let view = slot.read();
            assert_eq!(view.content, small);
        }
    }

    #[test]
    fn empty_slot_not_published() {
        let slot = Slot::new();
        assert!(!slot.is_published(0)); // sequence starts at 0
    }

    #[test]
    fn sequence_value_new_is_zero() {
        let slot = Slot::new();
        assert_eq!(slot.sequence_value(), 0);
    }

    #[test]
    fn write_hash_persists() {
        let slot = Slot::new();
        let hash = [0x42u8; 32];

        unsafe {
            slot.producer_write(10, 500, b"test");
            slot.write_hash(hash);
        }
        slot.publish(10);

        unsafe {
            let view = slot.read();
            assert_eq!(*view.hash_n, hash);
        }
    }

    #[test]
    fn zero_length_content() {
        let slot = Slot::new();
        unsafe {
            slot.producer_write(0, 0, b"");
        }
        slot.publish(0);
        unsafe {
            let view = slot.read();
            assert_eq!(view.content.len(), 0);
        }
    }

    #[test]
    fn exact_inline_boundary() {
        let slot = Slot::new();
        let content = vec![0xCCu8; INLINE_CAP]; // exactly at boundary

        unsafe {
            slot.producer_write(5, 3000, &content);
        }
        slot.publish(5);
        unsafe {
            let view = slot.read();
            assert_eq!(view.content.len(), INLINE_CAP);
            assert_eq!(view.content, &content[..]);
        }
    }

    #[test]
    fn just_above_inline_boundary() {
        let slot = Slot::new();
        let content = vec![0xDDu8; INLINE_CAP + 1]; // spill

        unsafe {
            slot.producer_write(6, 4000, &content);
        }
        slot.publish(6);
        unsafe {
            let view = slot.read();
            assert_eq!(view.content.len(), INLINE_CAP + 1);
            assert_eq!(view.content, &content[..]);
        }
    }

    #[test]
    fn release_acquire_visibility() {
        // Test that Release/Acquire ordering correctly synchronizes
        // between producer and consumer threads.
        use std::sync::Arc;
        use std::thread;

        let slot = Arc::new(Slot::new());
        let slot_clone = Arc::clone(&slot);

        let handle = thread::spawn(move || {
            unsafe {
                slot_clone.producer_write(99, 9999, b"concurrent");
            }
            slot_clone.publish(99);
        });

        handle.join().unwrap();

        // After join, the Release store in publish() synchronizes-with
        // this Acquire load, guaranteeing visibility of the write.
        assert!(slot.is_published(99));
        unsafe {
            let view = slot.read();
            assert_eq!(view.record_id, 99);
            assert_eq!(view.content, b"concurrent");
        }
    }
}

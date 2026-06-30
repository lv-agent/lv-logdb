//! Sealer — the optional background thread that computes a BLAKE3 keyed hash
//! chain over published records.
//!
//! Uses BLAKE3 in keyed mode with `hash_init` as the key. This provides:
//! - Tamper-evident integrity (any modification breaks the chain)
//! - Resistance to pre-computation attacks (attacker without key cannot predict
//!   future hash_n values even knowing all prior content)
//! - No length-extension vulnerability (unlike SHA-256)
//!
//! # Algorithm
//!
//! ```text
//! hash_n = BLAKE3_keyed(hash_init, prev_hash || content)
//! ```
//!
//! SHA-256 is retained as a fallback for segments written before v0.2.0.
//! Segment header's `hash_algo` field distinguishes: 1=SHA256, 2=BLAKE3.
//!
//! # Feature Gate
//!
//! This module is only available when the `hash-chain` feature is enabled.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::pipeline::signal::ShutdownState;
use crate::pipeline::trigger::{Backoff, WaitStrategy};
use crate::ring::Ring;
use crate::storage::format::HASH_ALGO_BLAKE3;

/// Run the Sealer loop. Uses BLAKE3 keyed mode by default (v0.2.0+).
///
/// SHA-256 segments are verified on recovery but not produced by the Sealer.
pub fn run_sealer(
    ring: Arc<Ring>,
    hash_init: [u8; 32],
    mut last_hash: [u8; 32],
    mut next_seq: u64,
    shutdown: Arc<ShutdownState>,
    wait: WaitStrategy,
) {
    let mut backoff = Backoff::new(wait);

    loop {
        let hi = scan_published(&ring, next_seq);
        match hi {
            Some(hi) => {
                for seq in next_seq..=hi {
                    let view = unsafe { ring.slot(seq).read() };
                    let hash_n = blake3_keyed_chain(&hash_init, &last_hash, view.content);
                    unsafe {
                        ring.slot(seq).write_hash(hash_n);
                    }
                    last_hash = hash_n;
                }
                ring.sealed_cursor.store(hi + 1, Ordering::Release);
                next_seq = hi + 1;
                backoff.reset();
            }
            None => {
                if shutdown.should_stop(next_seq) {
                    return;
                }
                backoff.step();
            }
        }
    }
}

/// Compute BLAKE3_keyed(key, prev_hash || content) → [u8; 32]
#[inline]
pub(crate) fn blake3_keyed_chain(key: &[u8; 32], prev_hash: &[u8; 32], content: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(prev_hash);
    hasher.update(content);
    *hasher.finalize().as_bytes()
}

/// SHA-256 fallback for verifying legacy segments (v0.1.0).
#[inline]
pub fn sha256_chain(prev_hash: &[u8; 32], content: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(prev_hash);
    hasher.update(content);
    let result = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);
    hash
}

/// Scan for contiguous published records starting from `from_seq`.
fn scan_published(ring: &Ring, from_seq: u64) -> Option<u64> {
    if !ring.slot(from_seq).is_published(from_seq) {
        return None;
    }
    let mut seq = from_seq;
    loop {
        if !ring.slot(seq).is_published(seq) {
            return Some(seq.wrapping_sub(1));
        }
        seq = seq.wrapping_add(1);
        if seq.wrapping_sub(from_seq) >= ring.ring_size() as u64 {
            return Some(seq.wrapping_sub(1));
        }
    }
}

/// Verify a hash chain over a sequence of records (algorithm-agnostic).
pub fn verify_chain(
    records: &[(u64, &[u8], [u8; 32])], // (record_id, content, stored_hash_n)
    init_hash: [u8; 32],
    hash_algo: u8,
) -> bool {
    match hash_algo {
        HASH_ALGO_BLAKE3 => {
            // BLAKE3 keyed verification requires the key.
            // For verification we use a non-keyed BLAKE3 since the key
            // may not be available at verification time.
            // The stored hash_n was computed with the key; we recompute
            // with the same key (passed via init_hash parameter).
            let mut expected = init_hash;
            for &(_id, content, stored_hash) in records {
                let mut hasher = blake3::Hasher::new_keyed(&init_hash);
                hasher.update(&expected);
                hasher.update(content);
                expected = *hasher.finalize().as_bytes();
                if expected != stored_hash {
                    return false;
                }
            }
            true
        }
        _ => {
            // SHA-256 or unknown: fall back to SHA-256 chain
            let mut expected = init_hash;
            for &(_id, content, stored_hash) in records {
                expected = sha256_chain(&expected, content);
                if expected != stored_hash {
                    return false;
                }
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::QueueFullPolicy;

    #[test]
    fn blake3_keyed_deterministic() {
        let key = [0x42u8; 32];
        let prev = [0u8; 32];
        let a = blake3_keyed_chain(&key, &prev, b"hello");
        let b = blake3_keyed_chain(&key, &prev, b"hello");
        assert_eq!(a, b);
    }

    #[test]
    fn blake3_keyed_different_key_produces_different_hash() {
        let key1 = [0x01u8; 32];
        let key2 = [0x02u8; 32];
        let prev = [0u8; 32];
        let h1 = blake3_keyed_chain(&key1, &prev, b"data");
        let h2 = blake3_keyed_chain(&key2, &prev, b"data");
        assert_ne!(h1, h2, "different keys must produce different hashes");
    }

    #[test]
    fn blake3_keyed_chained() {
        let key = [0xAAu8; 32];
        let h0 = [0u8; 32];
        let h1 = blake3_keyed_chain(&key, &h0, b"record-0");
        let h2 = blake3_keyed_chain(&key, &h1, b"record-1");
        assert_ne!(h1, h2);

        // Recompute
        let h1b = blake3_keyed_chain(&key, &h0, b"record-0");
        let h2b = blake3_keyed_chain(&key, &h1b, b"record-1");
        assert_eq!(h2, h2b);
    }

    #[test]
    fn sha256_still_works_for_fallback() {
        let prev = [0u8; 32];
        let a = sha256_chain(&prev, b"hello");
        let b = sha256_chain(&prev, b"hello");
        assert_eq!(a, b);
        assert_ne!(a, prev);
    }

    #[test]
    fn verify_chain_blake3() {
        let key = [0u8; 32];
        let h0 = [0u8; 32];
        let h1 = blake3_keyed_chain(&key, &h0, b"a");
        let h2 = blake3_keyed_chain(&key, &h1, b"b");
        let records = vec![(0, b"a".as_ref(), h1), (1, b"b".as_ref(), h2)];
        // Note: verify_chain with HASH_ALGO_BLAKE3 uses init_hash as BLAKE3 key
        assert!(verify_chain(&records, key, HASH_ALGO_BLAKE3));
    }

    #[test]
    fn verify_chain_blake3_detects_tamper() {
        let key = [0u8; 32];
        let h0 = [0u8; 32];
        let h1 = blake3_keyed_chain(&key, &h0, b"a");
        let h2 = blake3_keyed_chain(&key, &h1, b"b");
        let mut tampered = h2;
        tampered[0] ^= 1;
        let records = vec![(0, b"a".as_ref(), h1), (1, b"b".as_ref(), tampered)];
        assert!(!verify_chain(&records, key, HASH_ALGO_BLAKE3));
    }

    #[test]
    fn scan_published_finds_contiguous() {
        let ring = Ring::new(64, true, 0);
        for seq in 0..3 {
            unsafe {
                ring.slot(seq).producer_write(seq, 0, b"x");
            }
            ring.slot(seq).publish(seq);
        }
        assert_eq!(scan_published(&ring, 0), Some(2));
    }

    #[test]
    fn scan_published_with_gap() {
        let ring = Ring::new(64, true, 0);
        unsafe {
            ring.slot(0).producer_write(0, 0, b"x");
        }
        ring.slot(0).publish(0);
        unsafe {
            ring.slot(1).producer_write(1, 0, b"x");
        }
        ring.slot(1).publish(1);
        assert_eq!(scan_published(&ring, 0), Some(1));
    }

    #[test]
    fn sealer_writes_blake3_hash_and_advances_cursor() {
        let ring = Arc::new(Ring::new(64, true, 0));
        let hash_init = [0xABu8; 32];

        for i in 0..3 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            unsafe {
                ring.slot(seq).producer_write(seq, i * 100, b"data");
            }
            ring.slot(seq).publish(seq);
        }

        let shutdown = Arc::new(ShutdownState::new());
        let wait = WaitStrategy::default();
        let r = Arc::clone(&ring);
        let s = Arc::clone(&shutdown);

        let handle = std::thread::spawn(move || {
            run_sealer(r, hash_init, [0u8; 32], 0, s, wait);
        });

        std::thread::sleep(std::time::Duration::from_millis(100));
        shutdown.start_drain();
        shutdown.drain_target.store(3, Ordering::Release);
        handle.join().unwrap();

        let sealed = ring.sealed_cursor.load(Ordering::Acquire);
        assert_eq!(sealed, 3);

        unsafe {
            let view = ring.slot(0).read();
            assert_ne!(*view.hash_n, [0u8; 32]);
        }
    }
}

//! Committer — the background thread that serializes records from ring buffers
//! to segment files on disk.
//!
//! **v1.1**: Supports multiple shards. The Committer polls all rings round-robin
//! and commits batches from the shard with the oldest pending records. Each shard
//! maintains independent cursors; the `committed_cursor`/`durable_cursor` tracked
//! here are the minimum across all shards (for flush/shutdown gating).

use std::sync::Arc;
use std::time::Instant;

use crate::config::DurabilityMode;
use crate::health::{HealthState, HEALTH_DISK_FULL, HEALTH_IO_ERROR};
use crate::platform;
use crate::ring::Ring;
use crate::storage::SegmentManager;
use crate::storage::SegmentError;

use super::signal::FlushSignal;
use super::signal::ShutdownState;
use super::trigger::{Backoff, CommitTrigger, WaitStrategy};

/// Run the Committer loop over multiple shards.
///
/// v1.1 multi-shard: processes shards round-robin. Each shard's records are
/// committed to the same segment file in the order they are processed.
/// Bit-encoded record_ids ensure global uniqueness regardless of write order.
pub fn run_committer(
    rings: Vec<Arc<Ring>>,
    mut seg_mgr: SegmentManager,
    trigger: CommitTrigger,
    flush: Arc<FlushSignal>,
    shutdown: Arc<ShutdownState>,
    health: Arc<HealthState>,
    checkpoint: Arc<std::sync::atomic::AtomicU64>,
    wait: WaitStrategy,
) {
    let num_shards = rings.len();
    let mut backoff = Backoff::new(wait);

    // Per-shard committed cursors (local sequences)
    let mut committed: Vec<u64> = rings
        .iter()
        .map(|r| r.committed_cursor.load(std::sync::atomic::Ordering::Acquire))
        .collect();
    let mut durable: Vec<u64> = rings
        .iter()
        .map(|r| r.durable_cursor.load(std::sync::atomic::Ordering::Acquire))
        .collect();

    // For flush: track the minimum durable across shards
    let min_durable = |durable: &[u64]| *durable.iter().min().unwrap_or(&0);

    let mut pending_since: Option<Instant> = None;
    let mut current_shard: usize = 0;

    loop {
        // ── Poll all shards for available records ────────────────────
        let has_any_uncommitted = (0..num_shards).any(|s| {
            let avail = available_hi(&rings[s], committed[s]);
            avail > committed[s]
        });

        let has_any_unsynced = (0..num_shards).any(|s| committed[s] > durable[s]);

        let flush_target = flush.target.load(std::sync::atomic::Ordering::Acquire);
        let need_commit_for_flush =
            flush_target != u64::MAX && min_durable(&committed) < flush_target;
        let need_sync_for_flush =
            flush_target != u64::MAX && min_durable(&durable) < flush_target;

        // ── Idle check ───────────────────────────────────────────────
        // Check if we should stop: draining + no work left
        let fully_idle = !has_any_uncommitted
            && !has_any_unsynced
            && !need_commit_for_flush
            && !need_sync_for_flush;
        if fully_idle {
            if shutdown.draining()
                && min_durable(&durable) >= min_durable(&committed)
            {
                return;
            }
            pending_since = None;
            seg_mgr.drain_pending_fsyncs();
            backoff.step();
            continue;
        }

        if has_any_uncommitted && pending_since.is_none() {
            pending_since = Some(Instant::now());
        }

        // ── Choose shard to process (round-robin with available records) ──
        let start_shard = current_shard;
        let mut chosen_shard: Option<usize> = None;
        for _ in 0..num_shards {
            let si = current_shard;
            current_shard = (current_shard + 1) % num_shards;
            let avail = available_hi(&rings[si], committed[si]);
            if avail > committed[si] {
                chosen_shard = Some(si);
                break;
            }
        }
        // If no shard has uncommitted records but we have unsynced data,
        // stay on the last shard for fsync
        if chosen_shard.is_none() && !has_any_unsynced {
            if shutdown.draining()
                && min_durable(&durable) >= min_durable(&committed)
            {
                return;
            }
            seg_mgr.drain_pending_fsyncs();
            backoff.step();
            continue;
        }

        let si = chosen_shard.unwrap_or(start_shard);
        let avail = available_hi(&rings[si], committed[si]);

        // ── Commit decision ──────────────────────────────────────────
        let batch_count = avail.saturating_sub(committed[si]);
        let batch_bytes_estimate = batch_count.saturating_mul(256) as usize;
        let time_due = pending_since.map_or(false, |t| t.elapsed() >= trigger.interval);

        let should_commit = has_any_uncommitted
            && avail > committed[si]
            && (batch_bytes_estimate >= trigger.bytes
                || batch_count >= trigger.records as u64
                || time_due
                || need_commit_for_flush
                || shutdown.draining());

        if should_commit {
            let to = choose_batch_end(committed[si], avail, seg_mgr.buf_cap());

            match seg_mgr.append_batch(&rings[si], committed[si], to) {
                Ok(last_written) => {
                    committed[si] = last_written.wrapping_add(1);
                    rings[si]
                        .committed_cursor
                        .store(committed[si], std::sync::atomic::Ordering::Release);

                    if committed[si] >= avail {
                        pending_since = None;
                    } else {
                        pending_since = Some(Instant::now());
                    }
                    health.clear_if_recovered();
                }
                Err(SegmentError::Full) => {
                    let next_base = committed[si];
                    match seg_mgr.roll(next_base, checkpoint.load(std::sync::atomic::Ordering::Acquire)) {
                        Ok(()) => continue,
                        Err(e) => {
                            health.set_error(match &e {
                                SegmentError::Io(io_err)
                                    if platform::is_enospc(io_err) =>
                                {
                                    HEALTH_DISK_FULL
                                }
                                _ => HEALTH_IO_ERROR,
                            });
                            backoff.step();
                            continue;
                        }
                    }
                }
                Err(e) => {
                    health.set_error(match &e {
                        SegmentError::Io(io_err) if platform::is_enospc(io_err) => {
                            HEALTH_DISK_FULL
                        }
                        _ => HEALTH_IO_ERROR,
                    });
                    backoff.step();
                    continue;
                }
            }
            backoff.reset();
        }

        // ── Fsync decision ───────────────────────────────────────────
        let sync_due = match trigger.durability {
            DurabilityMode::Sync => committed[si] > durable[si],
            DurabilityMode::Batch => {
                committed[si] > durable[si]
                    && (time_due
                        || batch_bytes_estimate >= trigger.bytes
                        || need_sync_for_flush
                        || shutdown.draining())
            }
            DurabilityMode::Async => need_sync_for_flush || shutdown.draining(),
        };

        if sync_due {
            // Fsync if any shard has un-fsynced data
            let max_committed = committed.iter().max().copied().unwrap_or(0);
            let min_durable_val = min_durable(&durable);
            if max_committed > min_durable_val {
                // P0-5: fsync pending (rolled) segments too, so durable_cursor
                // never advances past un-fsynced data across a segment roll.
                match seg_mgr.sync_all() {
                    Ok(()) => {
                        // Advance all shards' durable cursors to their committed values
                        for s in 0..num_shards {
                            durable[s] = committed[s];
                            rings[s]
                                .durable_cursor
                                .store(durable[s], std::sync::atomic::Ordering::Release);
                        }
                        // Persist the active segment's sparse index alongside
                        // each fsync so reads of the active segment can use it
                        // (P2-1b). Best-effort.
                        let _ = seg_mgr.save_active_index();

                        // Complete flush if target reached
                        let new_min = min_durable(&durable);
                        if flush_target != u64::MAX && new_min >= flush_target {
                            flush.complete(flush_target);
                        }
                    }
                    Err(e) => {
                        health.set_error(if platform::is_enospc(&e) {
                            HEALTH_DISK_FULL
                        } else {
                            HEALTH_IO_ERROR
                        });
                        backoff.step();
                        continue;
                    }
                }
            }
        }

        if shutdown.draining()
            && !has_any_uncommitted
            && !has_any_unsynced
        {
            return;
        }

        if !should_commit && !sync_due {
            seg_mgr.drain_pending_fsyncs();
            backoff.step();
        }
    }
}

/// Determine the highest available seq for a shard.
#[inline]
fn available_hi(ring: &Ring, committed: u64) -> u64 {
    if ring.hash_enabled() {
        ring.sealed_cursor
            .load(std::sync::atomic::Ordering::Acquire)
    } else {
        ring.highest_published_contiguous(committed)
            .wrapping_add(1)
    }
}

fn choose_batch_end(committed: u64, avail: u64, buf_capacity: usize) -> u64 {
    let max_records = (buf_capacity / 300).max(64) as u64;
    let limit = committed.saturating_add(max_records);
    limit.min(avail.saturating_sub(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use crate::config::{QueueFullPolicy, RetentionPolicy};
    use crate::ring::Ring;
    use std::time::Duration;

    #[test]
    fn multi_shard_committer_basic() {
        let dir = tempfile::tempdir().unwrap();

        // Create 2 rings
        let ring0 = Arc::new(Ring::new(64, false, 0));
        let ring1 = Arc::new(Ring::new(64, false, 0));

        // Publish records to both shards
        for i in 0..5 {
            let seq = ring0.claim(QueueFullPolicy::Block).unwrap();
            unsafe { ring0.slot(seq).producer_write(seq, i * 100, b"shard0"); }
            ring0.slot(seq).publish(seq);
        }
        for i in 0..3 {
            let seq = ring1.claim(QueueFullPolicy::Block).unwrap();
            unsafe { ring1.slot(seq).producer_write(seq, i * 100, b"shard1"); }
            ring1.slot(seq).publish(seq);
        }

        let seg_mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            10 * 1024 * 1024,
            false, false, None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        let flush = Arc::new(FlushSignal::new());
        let shutdown = Arc::new(ShutdownState::new());
        let health = Arc::new(HealthState::new());
        let trigger = CommitTrigger {
            bytes: 1,
            records: 1,
            interval: Duration::from_millis(100),
            durability: DurabilityMode::Sync,
        };
        let wait = WaitStrategy::default();

        let rings = vec![Arc::clone(&ring0), Arc::clone(&ring1)];
        let s = Arc::clone(&shutdown);

        let handle = std::thread::spawn(move || {
            run_committer(rings, seg_mgr, trigger, flush, s, health, Arc::new(std::sync::atomic::AtomicU64::new(0)), wait);
        });

        std::thread::sleep(Duration::from_millis(200));
        shutdown.start_drain();

        handle.join().unwrap();

        // Both shards should have committed records
        let c0 = ring0.committed_cursor.load(Ordering::Acquire);
        let c1 = ring1.committed_cursor.load(Ordering::Acquire);
        assert!(c0 >= 5, "shard0 committed={}", c0);
        assert!(c1 >= 3, "shard1 committed={}", c1);
    }

    #[test]
    fn committer_single_shard_still_works() {
        let dir = tempfile::tempdir().unwrap();
        let ring = Arc::new(Ring::new(64, false, 0));

        for i in 0..5 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            unsafe { ring.slot(seq).producer_write(seq, i * 100, b"test"); }
            ring.slot(seq).publish(seq);
        }

        let seg_mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            10 * 1024 * 1024,
            false, false, None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        let flush = Arc::new(FlushSignal::new());
        let shutdown = Arc::new(ShutdownState::new());
        let health = Arc::new(HealthState::new());
        let trigger = CommitTrigger {
            bytes: 1,
            records: 1,
            interval: Duration::from_millis(100),
            durability: DurabilityMode::Sync,
        };
        let wait = WaitStrategy::default();

        let rings = vec![ring.clone()];
        let s = Arc::clone(&shutdown);

        let handle = std::thread::spawn(move || {
            run_committer(rings, seg_mgr, trigger, flush, s, health, Arc::new(std::sync::atomic::AtomicU64::new(0)), wait);
        });

        std::thread::sleep(Duration::from_millis(200));
        shutdown.start_drain();
        shutdown.drain_target.store(5, Ordering::Release);

        handle.join().unwrap();

        let c = ring.committed_cursor.load(Ordering::Acquire);
        assert!(c >= 5);
    }
}

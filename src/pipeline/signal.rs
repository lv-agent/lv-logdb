//! Signaling primitives for flush and shutdown coordination.
//!
//! - [`FlushSignal`]: allows callers to request and wait for durability
//! - [`ShutdownState`]: coordinates graceful shutdown with in-flight tracking

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

// ── FlushSignal ────────────────────────────────────────────────────────────

/// Per-shard flush coordination.
///
/// `targets[s]` holds the requested durability target for shard `s` (CAS-max;
/// `u64::MAX` = no request). `completed[s]` holds the highest durable seq+1
/// fsynced for shard `s` (CAS-max). A flush is satisfied when every shard's
/// `completed[s] >= targets[s]`. This handles uneven sharded loads (each shard
/// flushed to its own producer-cursor snapshot), unlike a single cross-shard
/// min/max target which stalls when shards advance unevenly.
pub struct FlushSignal {
    targets: Box<[AtomicU64]>,
    completed: Box<[AtomicU64]>,
}

impl FlushSignal {
    /// Create a flush signal for `num_shards` shards.
    pub fn new(num_shards: usize) -> Self {
        Self {
            targets: (0..num_shards)
                .map(|_| AtomicU64::new(u64::MAX))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            completed: (0..num_shards)
                .map(|_| AtomicU64::new(0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    /// Request durability up to `per_shard[s]` for each shard (CAS-max per shard).
    pub fn request(&self, per_shard: &[u64]) {
        for (s, &t) in per_shard.iter().enumerate() {
            if s >= self.targets.len() {
                break;
            }
            let mut cur = self.targets[s].load(Ordering::Acquire);
            loop {
                let new = if cur == u64::MAX { t } else { cur.max(t) };
                match self.targets[s]
                    .compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => break,
                    Err(v) => cur = v,
                }
            }
        }
    }

    /// Mark shard `shard` as durable up to `durable` (CAS-max per shard).
    pub fn complete(&self, shard: usize, durable: u64) {
        let mut cur = self.completed[shard].load(Ordering::Acquire);
        while cur < durable {
            match self.completed[shard]
                .compare_exchange_weak(cur, durable, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    /// True when every shard's `completed >= per_shard[s]`.
    pub fn is_done(&self, per_shard: &[u64]) -> bool {
        per_shard
            .iter()
            .enumerate()
            .all(|(s, &t)| s >= self.completed.len() || self.completed[s].load(Ordering::Acquire) >= t)
    }

    /// The requested target for `shard` (u64::MAX = none).
    pub fn target(&self, shard: usize) -> u64 {
        self.targets[shard].load(Ordering::Acquire)
    }

    /// Whether any shard has a pending flush request.
    pub fn any_pending(&self) -> bool {
        self.targets.iter().any(|t| t.load(Ordering::Acquire) != u64::MAX)
    }
}

// ── ShutdownState ──────────────────────────────────────────────────────────

/// Coordinates graceful shutdown with in-flight append tracking.
///
/// # Phase transitions
///
/// - **Phase 0 (Run)**: normal operation, appends accepted
/// - **Phase 1 (Drain)**: reject new appends, wait for in-flight → 0
/// - **Phase 2 (Abort)**: force shutdown, abandon un-published data
pub struct ShutdownState {
    /// Current phase: 0=run, 1=drain, 2=abort.
    phase: AtomicU8,
    /// Target record_id+1 for drain completion; u64::MAX = not set.
    pub(crate) drain_target: AtomicU64,
    /// Number of append calls currently between claim and publish.
    pub(crate) in_flight: AtomicU64,
}

impl Default for ShutdownState {
    fn default() -> Self {
        Self {
            phase: AtomicU8::new(0),
            drain_target: AtomicU64::new(u64::MAX),
            in_flight: AtomicU64::new(0),
        }
    }
}

impl ShutdownState {
    /// Create a new shutdown state in the running phase.
    pub fn new() -> Self {
        Self::default()
    }

    /// Called by `append` before claiming a sequence number.
    ///
    /// Returns `true` if the append may proceed, `false` if the database
    /// is draining or aborted.
    ///
    /// Atomicity: reserve the in-flight slot FIRST (fetch_add), then check the
    /// phase. This closes the TOCTOU window where the old "check then add"
    /// order let a concurrent `start_drain` see `in_flight == 0` and compute
    /// `drain_target` before this append had incremented — causing the append's
    /// record to be published after the drain target and thus not flushed. With
    /// "add then check", any append that proceeds is always counted in
    /// `in_flight` before drain can observe zero; any append that sees draining
    /// backs out (decrements) and returns false without publishing.
    #[inline]
    pub fn enter(&self) -> bool {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        if self.phase.load(Ordering::Acquire) >= 1 {
            // Draining/aborted started after we reserved — back out.
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        true
    }

    /// Called by `append` after publishing (or on error).
    #[inline]
    pub fn leave(&self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }

    /// Whether the database is in the draining phase.
    #[inline]
    pub fn draining(&self) -> bool {
        self.phase.load(Ordering::Acquire) >= 1
    }

    /// Whether the database has been aborted.
    #[inline]
    pub fn aborted(&self) -> bool {
        self.phase.load(Ordering::Acquire) >= 2
    }

    /// Check whether a background thread should stop.
    ///
    /// Returns `true` if the thread has processed up to `processed_cursor`
    /// and the drain target has been reached, or if the system is aborted.
    #[inline]
    pub fn should_stop(&self, processed_cursor: u64) -> bool {
        if self.aborted() {
            return true;
        }
        if self.draining() {
            let t = self.drain_target.load(Ordering::Acquire);
            return t != u64::MAX && processed_cursor >= t;
        }
        false
    }

    /// Transition to the draining phase.
    pub fn start_drain(&self) {
        self.phase.store(1, Ordering::Release);
    }

    /// Transition to the aborted phase.
    pub fn abort(&self) {
        self.phase.store(2, Ordering::Release);
    }

    /// Get the current phase.
    pub fn phase(&self) -> u8 {
        self.phase.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── FlushSignal tests ──────────────────────────────────────────────

    #[test]
    fn flush_signal_initial_state() {
        let sig = FlushSignal::new(1);
        assert_eq!(sig.target(0), u64::MAX);
        assert!(!sig.any_pending());
        assert!(!sig.is_done(&[1]));
    }

    #[test]
    fn flush_signal_request_and_complete() {
        let sig = FlushSignal::new(1);
        sig.request(&[10]);
        assert_eq!(sig.target(0), 10);
        assert!(sig.any_pending());
        assert!(!sig.is_done(&[10]));

        sig.complete(0, 10);
        assert!(sig.is_done(&[10]));
        assert!(!sig.is_done(&[11]));
    }

    #[test]
    fn flush_signal_cas_max_request() {
        let sig = FlushSignal::new(1);
        sig.request(&[5]);
        sig.request(&[15]); // higher target should win
        sig.request(&[10]); // lower target should not reduce
        assert_eq!(sig.target(0), 15);
    }

    #[test]
    fn flush_signal_complete_is_monotonic() {
        let sig = FlushSignal::new(1);
        sig.complete(0, 20);
        sig.complete(0, 10); // should not regress
        assert!(sig.is_done(&[10]));
        assert!(sig.is_done(&[20]));
    }

    #[test]
    fn flush_signal_multi_shard_all_must_reach() {
        // Two shards: done only when BOTH reach their per-shard target.
        let sig = FlushSignal::new(2);
        sig.request(&[10, 20]);
        assert!(!sig.is_done(&[10, 20]));
        sig.complete(0, 10); // shard 0 done
        assert!(!sig.is_done(&[10, 20])); // shard 1 not yet
        sig.complete(1, 20); // shard 1 done
        assert!(sig.is_done(&[10, 20]));
    }

    // ── ShutdownState tests ────────────────────────────────────────────

    #[test]
    fn shutdown_initial_running() {
        let s = ShutdownState::new();
        assert_eq!(s.phase(), 0);
        assert!(!s.draining());
        assert!(!s.aborted());
    }

    #[test]
    fn shutdown_enter_leave_in_flight() {
        let s = ShutdownState::new();
        assert!(s.enter());
        assert!(s.enter());
        s.leave();
        s.leave();
        // in_flight should be zero
        assert_eq!(s.in_flight.load(Ordering::Acquire), 0);
    }

    #[test]
    fn shutdown_enter_rejected_after_drain() {
        let s = ShutdownState::new();
        s.start_drain();
        assert!(!s.enter());
    }

    #[test]
    fn shutdown_enter_rejected_after_abort() {
        let s = ShutdownState::new();
        s.abort();
        assert!(!s.enter());
    }

    #[test]
    fn shutdown_should_stop() {
        let s = ShutdownState::new();
        // Not draining yet
        assert!(!s.should_stop(0));

        // Start drain with target
        s.start_drain();
        s.drain_target.store(100, Ordering::Release);
        assert!(!s.should_stop(50)); // not yet
        assert!(s.should_stop(100)); // reached target
        assert!(s.should_stop(150)); // past target
    }
}

//! Signaling primitives for flush and shutdown coordination.
//!
//! - [`FlushSignal`]: allows callers to request and wait for durability
//! - [`ShutdownState`]: coordinates graceful shutdown with in-flight tracking

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

// ── FlushSignal ────────────────────────────────────────────────────────────

/// A signal mechanism for requesting and waiting for durability.
///
/// Multiple callers can concurrently request a flush. The `target` field
/// stores the maximum requested target (CAS-max). The `completed` field
/// stores the highest durable record_id+1 that has been fsynced.
///
/// `u64::MAX` is used as the "no request" sentinel.
pub struct FlushSignal {
    /// Requested durability target (record_id+1); u64::MAX = no request.
    pub(crate) target: AtomicU64,
    /// Completed durability target (record_id+1).
    pub(crate) completed: AtomicU64,
}

impl Default for FlushSignal {
    fn default() -> Self {
        Self {
            target: AtomicU64::new(u64::MAX),
            completed: AtomicU64::new(0),
        }
    }
}

impl FlushSignal {
    /// Create a new flush signal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request durability up to `target` (record_id+1).
    ///
    /// Uses CAS-max: if multiple threads request different targets,
    /// the maximum target is retained.
    pub fn request(&self, target: u64) {
        let mut cur = self.target.load(Ordering::Acquire);
        loop {
            let new = if cur == u64::MAX { target } else { cur.max(target) };
            match self
                .target
                .compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    /// Mark durability as completed up to `target` (record_id+1).
    ///
    /// Uses CAS-max to ensure monotonic progress.
    pub fn complete(&self, target: u64) {
        let mut cur = self.completed.load(Ordering::Acquire);
        while cur < target {
            match self
                .completed
                .compare_exchange_weak(cur, target, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    /// Check whether the requested flush target has been reached.
    pub fn is_done(&self, target: u64) -> bool {
        self.completed.load(Ordering::Acquire) >= target
    }

    /// Get the current requested target (u64::MAX if none).
    pub fn current_target(&self) -> u64 {
        self.target.load(Ordering::Acquire)
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
        let sig = FlushSignal::new();
        assert_eq!(sig.current_target(), u64::MAX);
        assert!(!sig.is_done(1));
    }

    #[test]
    fn flush_signal_request_and_complete() {
        let sig = FlushSignal::new();
        sig.request(10);
        assert_eq!(sig.current_target(), 10);
        assert!(!sig.is_done(10));

        sig.complete(10);
        assert!(sig.is_done(10));
        assert!(!sig.is_done(11));
    }

    #[test]
    fn flush_signal_cas_max_request() {
        let sig = FlushSignal::new();
        sig.request(5);
        sig.request(15); // higher target should win
        sig.request(10); // lower target should not reduce
        assert_eq!(sig.current_target(), 15);
    }

    #[test]
    fn flush_signal_complete_is_monotonic() {
        let sig = FlushSignal::new();
        sig.complete(20);
        sig.complete(10); // should not regress
        assert!(sig.is_done(10));
        assert!(sig.is_done(20));
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

//! Commit trigger logic and backoff/wait strategies.
//!
//! The [`CommitTrigger`] decides when the Committer should flush a batch of
//! records to disk based on byte count, record count, and time thresholds.
//!
//! Types are re-exported from [`crate::config`] for use by the pipeline.

use std::hint;
use std::thread;
use std::time::Duration;

// Re-export config types for convenience
pub use crate::config::{CommitTrigger, DurabilityMode, WaitStrategy};

/// Stateful backoff helper for a single thread.
///
/// Tracks spin/yield/sleep phase across iterations.
pub struct Backoff {
    spins: u32,
    config: WaitStrategy,
}

impl Backoff {
    /// Create a new backoff with the given strategy.
    pub fn new(config: WaitStrategy) -> Self {
        Self { spins: 0, config }
    }

    /// Execute one backoff step.
    ///
    /// Progresses through spin → yield → park phases based on call count.
    /// Call `reset()` when work is found to restart the backoff progression.
    pub fn step(&mut self) {
        self.spins = self.spins.saturating_add(1);
        if self.spins <= self.config.spin_count {
            hint::spin_loop();
        } else if self.spins <= self.config.spin_count + self.config.yield_count {
            thread::yield_now();
        } else {
            thread::sleep(self.config.park_duration);
        }
    }

    /// Reset the backoff state (call when work is found).
    pub fn reset(&mut self) {
        self.spins = 0;
    }
}

/// Producer-side backoff for when the ring is full.
#[inline]
pub fn producer_backoff(spins: &mut u32) {
    *spins = spins.saturating_add(1);
    if *spins <= 64 {
        hint::spin_loop();
    } else if *spins <= 256 {
        thread::yield_now();
    } else {
        thread::sleep(Duration::from_micros(100));
        *spins = 128;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_resets() {
        let mut bo = Backoff::new(WaitStrategy::default());
        // Spin a few times
        for _ in 0..10 {
            bo.step();
        }
        assert!(bo.spins > 0);
        bo.reset();
        assert_eq!(bo.spins, 0);
    }

    #[test]
    fn commit_trigger_defaults() {
        let t = CommitTrigger::default();
        assert_eq!(t.bytes, 256 * 1024);
        assert_eq!(t.records, 1024);
        assert_eq!(t.interval, Duration::from_millis(10));
    }
}

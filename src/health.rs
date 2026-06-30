//! Health state tracking for self-healing error conditions.
//!
//! The health state allows the system to distinguish between permanent errors
//! and transient conditions (like ENOSPC) that can resolve without restart.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

/// Health codes.
pub const HEALTH_OK: u8 = 0;
pub const HEALTH_DISK_FULL: u8 = 1;
pub const HEALTH_IO_ERROR: u8 = 2;

/// Shared health state for the database.
///
/// # Self-healing
///
/// `DiskFull` is self-healing: when the Committer successfully writes after an
/// ENOSPC (e.g., because retention freed space or the user cleaned the disk),
/// it calls `clear_if_recovered()`, and appends resume.
///
/// `IoError` for non-ENOSPC errors requires operator intervention.
pub struct HealthState {
    /// Health code: 0=OK, 1=DiskFull, 2=IoError.
    code: AtomicU8,
    /// Timestamp (nanoseconds) of the most recent error, for throttling retry probes.
    error_ts: AtomicU64,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            code: AtomicU8::new(HEALTH_OK),
            error_ts: AtomicU64::new(0),
        }
    }
}

impl HealthState {
    /// Create a new healthy state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an error.  Only transitions from OK → error; once in error,
    /// the code stays until explicitly cleared.
    pub fn set_error(&self, code: u8) {
        let _ = self
            .code
            .compare_exchange(HEALTH_OK, code, Ordering::AcqRel, Ordering::Relaxed);
        // Always update the timestamp so retry probes see a fresh time.
        self.error_ts.store(now_coarse_ns(), Ordering::Release);
    }

    /// Clear the error state (self-healing).
    pub fn clear_if_recovered(&self) {
        self.code.store(HEALTH_OK, Ordering::Release);
    }

    /// Check if the system is healthy, returning `None` if OK or `Some(code)`
    /// if in an error state.
    pub fn check(&self) -> Option<u8> {
        match self.code.load(Ordering::Relaxed) {
            HEALTH_OK => None,
            code => Some(code),
        }
    }

    /// Return the timestamp of the most recent error (0 if never).
    pub fn error_timestamp(&self) -> u64 {
        self.error_ts.load(Ordering::Relaxed)
    }
}

/// Get a coarse nanosecond timestamp.
///
/// Uses `CLOCK_REALTIME_COARSE` via vDSO — no syscall on modern kernels.
#[inline]
pub fn now_coarse_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_REALTIME_COARSE, &mut ts);
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_is_healthy() {
        let h = HealthState::new();
        assert_eq!(h.check(), None);
    }

    #[test]
    fn set_and_check_error() {
        let h = HealthState::new();
        h.set_error(HEALTH_DISK_FULL);
        assert_eq!(h.check(), Some(HEALTH_DISK_FULL));
    }

    #[test]
    fn clear_after_error() {
        let h = HealthState::new();
        h.set_error(HEALTH_IO_ERROR);
        assert_eq!(h.check(), Some(HEALTH_IO_ERROR));
        h.clear_if_recovered();
        assert_eq!(h.check(), None);
    }

    #[test]
    fn error_timestamp_is_set() {
        let h = HealthState::new();
        assert_eq!(h.error_timestamp(), 0);
        h.set_error(HEALTH_DISK_FULL);
        assert!(h.error_timestamp() > 0);
    }

    #[test]
    fn now_coarse_ns_is_monotonic() {
        let a = now_coarse_ns();
        let b = now_coarse_ns();
        assert!(b >= a);
    }
}

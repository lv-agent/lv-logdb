//! Error types for logdb.
//!
//! All errors are structured and implement `std::error::Error` via `thiserror`.

use thiserror::Error;

/// Errors that can occur during `append`.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AppendError {
    /// The ring buffer is full and the policy is `Drop`.
    #[error("ring buffer full")]
    QueueFull,

    /// Content exceeds `max_content_size` in config.
    #[error("content size {size} exceeds maximum {max}")]
    ContentTooLarge {
        /// The size of the content that was rejected.
        size: usize,
        /// The maximum allowed content size.
        max: usize,
    },

    /// The underlying disk is full (ENOSPC). May be self-healing.
    #[error("disk full")]
    DiskFull,

    /// A non-ENOSPC I/O error occurred.
    #[error("I/O error: {0}")]
    Io(String),

    /// The database is shutting down and not accepting new appends.
    #[error("shutting down")]
    ShuttingDown,
}

/// Errors that can occur during `flush`.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum FlushError {
    /// The flush did not complete within the configured timeout.
    #[error("flush timed out")]
    Timeout,

    /// The database was aborted during the flush wait.
    #[error("shutdown aborted")]
    Aborted,
}

/// Errors that can occur during `read` or `scan`.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// The requested record_id does not exist.
    #[error("record {0} not found")]
    NotFound(u64),

    /// A CRC check failed, indicating data corruption.
    #[error("CRC mismatch at record {0}")]
    CrcMismatch(u64),

    /// An I/O error occurred during reading.
    #[error("I/O error: {0}")]
    Io(String),
}

/// Errors that can occur during `shutdown`.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ShutdownError {
    /// Shutdown did not complete within the timeout.
    #[error("shutdown timed out")]
    Timeout,

    /// Background threads could not be joined.
    #[error("failed to join background threads")]
    JoinError(String),
}

/// Result of a shutdown operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownReport {
    /// All data was durably persisted before shutdown.
    Clean,
    /// Some data was committed but not fsynced before the timeout.
    PartialDurable,
    /// Shutdown timed out; some data may be lost.
    TimedOut,
}

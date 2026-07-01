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

    /// `append_batch` was called with an empty slice. Nothing was reserved.
    #[error("append_batch called with an empty batch")]
    EmptyBatch,

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

/// Errors that can occur while validating a [`Config`](crate::Config).
///
/// Returned by [`Config::validate`](crate::Config::validate). Structured (rather
/// than a `String`) so callers can react to a specific misconfiguration. Each
/// variant carries the offending value.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// `ring_size` is not a power of two or is below 16.
    #[error("ring_size must be a power of two >= 16, got {0}")]
    InvalidRingSize(usize),
    /// `shards` is outside `[1, 256]`.
    #[error("shards must be in [1, 256], got {0}")]
    InvalidShardCount(usize),
    /// `segment_size` is below the 1MB minimum.
    #[error("segment_size must be >= 1MB, got {0}")]
    SegmentTooSmall(u64),
    /// `max_content_size` is above the 64MB ceiling.
    #[error("max_content_size must be <= 64MB, got {0}")]
    ContentTooLarge(usize),
    /// `index_stride` is zero.
    #[error("index_stride must be >= 1")]
    ZeroIndexStride,
    /// `hash-chain` requires a single shard (a global chain needs single-shard
    /// order). Only present under the `hash-chain` feature.
    #[cfg(feature = "hash-chain")]
    #[error("hash-chain requires shards == 1, got shards = {shards}")]
    HashChainRequiresSingleShard {
        /// The offending shard count.
        shards: usize,
    },
}

/// Errors that can occur while opening a [`LogDb`](crate::LogDb).
///
/// Returned by [`LogDb::open`](crate::LogDb::open). Replaces the previous
/// `Result<_, String>` so callers can match on the failure category and forward
/// it through their own error types via `?`.
///
/// Only `Debug` is derived: some variants wrap an `io::Error` (not `Clone`).
#[derive(Error, Debug)]
pub enum OpenError {
    /// The provided [`Config`](crate::Config) failed validation.
    #[error("invalid configuration: {0}")]
    InvalidConfig(#[from] ConfigError),
    /// Crash recovery failed for one shard. `reason` is the underlying detail
    /// (recovery currently returns a `String`; full structuring is tracked
    /// separately).
    #[error("recovery failed for shard {shard}: {reason}")]
    Recovery {
        /// The shard index that failed recovery.
        shard: usize,
        /// The underlying failure detail.
        reason: String,
    },
    /// A segment manager could not be created (`SegmentManager::create`
    /// returned an I/O error while creating the directory / first segment).
    #[error("failed to create segment manager: {0}")]
    SegmentCreate(#[source] std::io::Error),
    /// A background thread (Committer or Sealer) could not be spawned.
    #[error("failed to spawn background thread: {0}")]
    ThreadSpawn(#[source] std::io::Error),
}

/// Errors that can occur while reading the next batch from a [`Tailer`](crate::Tailer).
///
/// Replaces the previous `Result<_, String>` on `Tailer::next_batch`, so callers
/// can forward it via `?` and match on the category. Currently every failure is
/// a read error from the underlying scan.
#[derive(Error, Debug)]
pub enum TailerError {
    /// A read error while scanning the next batch (I/O, CRC, not-found).
    #[error("tailer read error: {0}")]
    Read(#[from] ReadError),
}

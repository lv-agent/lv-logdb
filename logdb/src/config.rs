//! Configuration for logdb.
//!
//! All configuration is validated at construction time via [`Config::validate`].

use std::path::PathBuf;
use std::time::Duration;

use crate::error::ConfigError;

/// Policy when the ring buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueFullPolicy {
    /// Block (spin + backoff) until a slot becomes available.
    Block,
    /// Immediately return [`AppendError::QueueFull`](crate::AppendError::QueueFull).
    Drop,
}

/// Durability mode for the Committer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// `fdatasync` after every commit batch.
    Sync,
    /// `fdatasync` when the batch size or time threshold is met.
    Batch,
    /// `fdatasync` only on explicit `flush()` or shutdown.
    Async,
}

/// I/O backend for the Committer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoBackend {
    /// Standard `pwrite` + `fdatasync`.
    Pwrite,
    // IoUring reserved for future use.
}

/// Retention policy for old segments.
#[derive(Debug, Clone)]
pub enum RetentionPolicy {
    /// Keep all segments indefinitely.
    KeepAll,
    /// Keep at most `max_bytes` of segment data.
    MaxBytes(u64),
    /// Keep segments younger than `max_age`.
    MaxAge(Duration),
}

/// Wait strategy for background thread spinning.
#[derive(Debug, Clone, Copy)]
pub struct WaitStrategy {
    /// Number of spin-loop iterations before yielding.
    pub spin_count: u32,
    /// Number of yields before parking.
    pub yield_count: u32,
    /// Duration to park the thread.
    pub park_duration: Duration,
}

impl Default for WaitStrategy {
    fn default() -> Self {
        Self {
            spin_count: 64,
            yield_count: 16,
            park_duration: Duration::from_micros(500),
        }
    }
}

/// Commit trigger thresholds.
#[derive(Debug, Clone, Copy)]
pub struct CommitTrigger {
    /// Trigger commit when batched bytes reach this threshold.
    pub bytes: usize,
    /// Trigger commit when batched record count reaches this threshold.
    pub records: usize,
    /// Trigger commit when this interval has elapsed since the first pending record.
    pub interval: Duration,
    /// Durability mode.
    pub durability: DurabilityMode,
}

impl Default for CommitTrigger {
    fn default() -> Self {
        Self {
            bytes: 256 * 1024, // 256KB
            records: 1024,
            interval: Duration::from_millis(10),
            durability: DurabilityMode::Batch,
        }
    }
}

/// Complete configuration for a [`LogDb`](crate::LogDb) instance.
#[derive(Debug, Clone)]
pub struct Config {
    /// Directory for segment files and metadata.
    pub data_dir: PathBuf,

    /// Maximum size of a single segment file before rolling. Default: 256MB.
    pub segment_size: u64,

    /// Number of slots per ring (must be a power of two, >= 16). Default: 8192.
    pub ring_size: usize,

    /// Number of independent rings (shards). Default: 1.
    pub shards: usize,

    /// Maximum content size for a single record. Default: 1MB.
    pub max_content_size: usize,

    /// Enable SHA256 hash chain integrity protection. Default: false.
    pub hash_enabled: bool,
    /// Enable streaming zstd compression (requires "compression" feature).
    pub compression_enabled: bool,
    pub encryption_key: Option<[u8; 32]>, // Requires "encryption" feature

    /// Durability mode. Default: Batch.
    pub durability_mode: DurabilityMode,

    /// I/O backend. Default: Pwrite.
    pub io_backend: IoBackend,

    /// Policy when the ring buffer is full. Default: Block.
    pub queue_full_policy: QueueFullPolicy,

    /// Wait strategy for background thread spinning.
    pub wait_strategy: WaitStrategy,

    /// Sparse-index stride: index one anchor every `index_stride` records per
    /// segment. Smaller → faster point reads (shorter scan from anchor) at the
    /// cost of a larger `.idx` file; larger → smaller index, longer read scan.
    /// Default 1024 (~0.02% of data). Set lower (e.g. 64–256) for latency-
    /// sensitive point reads (KV/etcd-style workloads). Only affects raw
    /// (non-compressed, non-encrypted) segments.
    pub index_stride: u32,

    /// Timeout for `flush()` calls. Default: 30s.
    pub flush_timeout: Duration,

    /// Retention policy for old segments. Default: KeepAll.
    pub retention: RetentionPolicy,

    /// Remote endpoint URL for optional push. Default: None.
    pub remote_endpoint: Option<String>,

    /// Batch size for remote push. Default: 1024.
    pub push_batch_size: usize,

    /// Pusher progress save interval (in batches). Default: 10.
    pub push_progress_interval: u32,

    /// Max push retries before giving up. 0 = infinite. Default: 0.
    pub push_max_retries: u32,

    /// Base retry backoff. Default: 1s, capped at 60s.
    pub push_retry_base: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./logdb_data"),
            segment_size: 256 * 1024 * 1024, // 256MB
            ring_size: 8192,
            shards: 1,
            max_content_size: 1 * 1024 * 1024, // 1MB
            hash_enabled: false,
            compression_enabled: false,
            encryption_key: None,
            durability_mode: DurabilityMode::Batch,
            io_backend: IoBackend::Pwrite,
            queue_full_policy: QueueFullPolicy::Block,
            wait_strategy: WaitStrategy::default(),
            index_stride: 1024,
            flush_timeout: Duration::from_secs(30),
            retention: RetentionPolicy::KeepAll,
            remote_endpoint: None,
            push_batch_size: 1024,
            push_progress_interval: 10,
            push_max_retries: 0,
            push_retry_base: Duration::from_secs(1),
        }
    }
}

impl Config {
    /// Validate the configuration.
    ///
    /// Returns `Ok(())` if all constraints are satisfied, or a structured
    /// [`ConfigError`] describing the first violation. Callers can match on the
    /// variant to react to a specific misconfiguration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.ring_size.is_power_of_two() || self.ring_size < 16 {
            return Err(ConfigError::InvalidRingSize(self.ring_size));
        }
        if self.shards < 1 || self.shards > 256 {
            return Err(ConfigError::InvalidShardCount(self.shards));
        }
        if self.segment_size < 1 * 1024 * 1024 {
            return Err(ConfigError::SegmentTooSmall(self.segment_size));
        }
        if self.max_content_size > 64 * 1024 * 1024 {
            return Err(ConfigError::ContentTooLarge(self.max_content_size));
        }
        if self.index_stride == 0 {
            return Err(ConfigError::ZeroIndexStride);
        }
        // Per-shard hash chain: each shard independently sealed with its own
        // hash_init and last_hash, so multi-shard hash-chain is supported.
        // (The pre-v0.4 single-shard constraint has been removed.)
        // Note: no arena_size constraint — ContentArena has been eliminated.
        // Content lives in Slot (inline or spill), gated by the single
        // consume_watermark.  This removes the fragile ring_size * max_content_size
        // product constraint that caused panics in v1.4.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_validates() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn rejects_non_power_of_two_ring() {
        let mut c = Config::default();
        c.ring_size = 100;
        assert!(matches!(
            c.validate(),
            Err(ConfigError::InvalidRingSize(100))
        ));
    }

    #[test]
    fn rejects_small_ring() {
        let mut c = Config::default();
        c.ring_size = 8;
        assert!(matches!(c.validate(), Err(ConfigError::InvalidRingSize(8))));
    }

    #[test]
    fn rejects_zero_shards() {
        let mut c = Config::default();
        c.shards = 0;
        assert!(matches!(
            c.validate(),
            Err(ConfigError::InvalidShardCount(0))
        ));
    }

    #[test]
    fn rejects_too_many_shards() {
        let mut c = Config::default();
        c.shards = 257;
        assert!(matches!(
            c.validate(),
            Err(ConfigError::InvalidShardCount(257))
        ));
    }

    #[test]
    fn rejects_small_segment() {
        let mut c = Config::default();
        c.segment_size = 512 * 1024; // 512KB
        assert!(matches!(c.validate(), Err(ConfigError::SegmentTooSmall(_))));
    }

    #[test]
    fn rejects_huge_content() {
        let mut c = Config::default();
        c.max_content_size = 128 * 1024 * 1024; // 128MB
        assert!(matches!(c.validate(), Err(ConfigError::ContentTooLarge(_))));
    }

    #[test]
    fn rejects_zero_index_stride() {
        let mut c = Config::default();
        c.index_stride = 0;
        assert!(matches!(c.validate(), Err(ConfigError::ZeroIndexStride)));
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn allows_hash_chain_with_multiple_shards() {
        // Per-shard hash chain: multi-shard is now supported.
        let mut c = Config::default();
        c.hash_enabled = true;
        c.shards = 4;
        assert!(c.validate().is_ok());
    }

    #[cfg(feature = "hash-chain")]
    #[test]
    fn accepts_hash_chain_with_single_shard() {
        let mut c = Config::default();
        c.hash_enabled = true;
        c.shards = 1;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn no_arena_constraint() {
        // v1.4 required arena_size >= ring_size * max_content_size.
        // v1.0 has no such constraint — this should validate fine.
        let mut c = Config::default();
        c.ring_size = 8192;
        c.max_content_size = 1 * 1024 * 1024; // 1MB
                                              // No arena_size field exists — constraint is gone.
        c.validate().unwrap();
    }
}

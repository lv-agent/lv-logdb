//! # logdb — Embedded Append-Only Log Database
//!
//! logdb is an **embedded, append-only, crash-recoverable, optionally tamper-proof,
//! optionally remotely-pushable** local log database.
//!
//! ## Features
//!
//! - **High-throughput append**: lock-free fast path with CAS-based ring buffer
//! - **Crash recovery**: automatic torn-write detection and truncation on restart
//! - **Optional hash chain**: SHA-256 forward-linking for tamper detection (`hash-chain` feature)
//! - **Segment management**: automatic rolling, configurable retention
//! - **Segment pre-allocation**: next segment is pre-created at 80% capacity,
//!   reducing roll-time blocking to a single `fdatasync` call
//!
//! ## Performance: Inline vs Spill
//!
//! Records ≤ [`INLINE_CAP`](ring::slot::INLINE_CAP) bytes (256) take the **inline
//! fast path**: zero heap allocation, zero extra memcpy. p50 is typically <100ns.
//!
//! Records > 256 bytes take the **spill path**: a heap allocation in the append
//! thread. The spill path is ~4x slower in throughput with ~80x higher p99.9
//! tail latency due to allocator jitter. Keep latency-sensitive records ≤ 256B.

pub mod config;
pub mod error;
pub mod health;
pub mod platform;
pub mod record;
pub mod ring;
pub mod pipeline;
pub mod storage;
pub mod reader;
pub mod shard;

pub mod recovery;
pub mod tailer;
mod pusher;

pub use config::Config;
pub use error::{AppendError, FlushError, ReadError, ShutdownError, ShutdownReport};
pub use record::Record;

/// Recovery report returned by [`LogDb::recovery_report`].
#[derive(Debug, Clone)]
pub struct RecoveryReport {
    /// First sequence to replay (the last checkpoint).
    pub from_sequence: u64,
    /// Last durable sequence.
    pub to_sequence: u64,
    /// Number of records to replay.
    pub count: u64,
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use health::HealthState;
use pipeline::signal::{FlushSignal, ShutdownState};
use pipeline::trigger::CommitTrigger;
use ring::Ring;
use shard::ShardMap;
use storage::SegmentManager;

/// The main log database handle.
pub struct LogDb {
    inner: Arc<LogDbInner>,
}

struct LogDbInner {
    config: config::Config,
    shards: ShardMap,
    health: Arc<HealthState>,
    flush: Arc<FlushSignal>,
    shutdown: Arc<ShutdownState>,
    committer_handle: Option<std::thread::JoinHandle<()>>,
    #[cfg(feature = "hash-chain")]
    sealer_handle: Option<std::thread::JoinHandle<()>>,
    data_dir: std::path::PathBuf,
    /// WAL checkpoint: records with sequence < this are safe to truncate.
    checkpoint_sequence: Arc<AtomicU64>,
    /// Per-shard cached segment listings (one per shard dir).
    manifests: Vec<Arc<Mutex<reader::SegmentManifest>>>,
}

impl LogDb {
    /// Open or create a logdb instance.
    pub fn open(config: Config) -> Result<Self, String> {
        config.validate()?;

        let data_dir = config.data_dir.clone();
        let hash_enabled = config.hash_enabled;

        let num_shards = config.shards;
        let shard_bits = shard::shard_bits(num_shards);

        // Build per-shard SegmentManagers, recovering each shard dir independently.
        // Each shard is an independent recoverable log; within a shard, record ids
        // are monotonic with stride `1 << shard_bits`. shards=1: flat data_dir,
        // shard_bits=0 (identity encoding, stride 1). shards>1: data_dir/s<shard>/.
        let mut seg_mgrs: Vec<SegmentManager> = Vec::with_capacity(num_shards);
        let mut initial_seqs: Vec<u64> = Vec::with_capacity(num_shards);
        let mut hash_init = [0u8; 32];
        let mut last_hash = [0u8; 32];
        let mut hash_init_known = false;

        for s in 0..num_shards {
            let sdir = if num_shards == 1 {
                data_dir.clone()
            } else {
                data_dir.join(format!("s{}", s))
            };
            let has_data = sdir.exists() && sdir.join("segment-00000001.log").exists();

            if has_data {
                let st = recovery::recover_shard(
                    &sdir,
                    shard_bits,
                    config.segment_size,
                    config.retention.clone(),
                    config.encryption_key,
                )?;
                // Resume this shard's ring at the LOCAL seq after the last recovered
                // record. An empty shard (recovered_count == 0) resumes at 0.
                let initial_local = if st.recovered_count == 0 {
                    0
                } else {
                    shard::decode_record_id(st.last_sequence, shard_bits).1 + 1
                };
                if !hash_init_known {
                    hash_init = st.hash_init;
                    last_hash = st.last_hash;
                    hash_init_known = true;
                }
                seg_mgrs.push(st.segment_manager);
                initial_seqs.push(initial_local);
            } else {
                // Fresh shard. base_sequence stays 0 (the reader's `find`/
                // `segments_from` rely on `base <= first_id`, which holds for 0
                // across all shards). The first record's GLOBAL id carries the
                // shard id in its low bits, so recovery re-seeds its stride chain
                // from that first record rather than from base_sequence.
                if !hash_init_known {
                    hash_init = generate_hash_init();
                    hash_init_known = true;
                }
                let sm = SegmentManager::create(
                    sdir,
                    config.segment_size,
                    hash_enabled,
                    config.compression_enabled,
                    config.encryption_key,
                    hash_init,
                    config.retention.clone(),
                    0,
                )
                .map_err(|e| format!("create segment manager: {}", e))?;
                seg_mgrs.push(sm);
                initial_seqs.push(0);
            }
        }

        // initial_seq (shard 0's resume point) is consumed by the Sealer for
        // shards=1; read inline from initial_seqs there to avoid a dead binding
        // when the hash-chain feature is disabled. Likewise `last_hash` is only
        // read by the Sealer, so silence its unused assignment without the feature.
        #[cfg(not(feature = "hash-chain"))]
        let _ = last_hash;

        // Apply the configured sparse-index stride before the Committer starts
        // appending (active index is still empty here).
        for m in seg_mgrs.iter_mut() {
            m.set_index_stride(config.index_stride);
        }

        // Create shared state
        let shards = ShardMap::new_with_initial(
            config.shards,
            config.ring_size,
            hash_enabled,
            &initial_seqs,
        );
        let health = Arc::new(HealthState::new());
        let flush = Arc::new(FlushSignal::new(num_shards));
        let shutdown = Arc::new(ShutdownState::new());

        let trigger = CommitTrigger {
            bytes: 256 * 1024,
            records: 1024,
            interval: Duration::from_millis(10),
            durability: config.durability_mode,
        };
        let wait = config.wait_strategy;

        // Spawn Committer — passes all rings + per-shard managers for multi-shard polling
        let committer_rings = shards.all_rings().to_vec();
        let committer_flush = Arc::clone(&flush);
        let committer_shutdown = Arc::clone(&shutdown);
        let committer_health = Arc::clone(&health);
        let checkpoint = Arc::new(AtomicU64::new(Self::load_checkpoint(&data_dir)));
        let committer_checkpoint = Arc::clone(&checkpoint);
        let committer_handle = std::thread::Builder::new()
            .name("logdb-committer".into())
            .spawn(move || {
                pipeline::committer::run_committer(
                    committer_rings,
                    seg_mgrs,
                    shard_bits,
                    trigger,
                    committer_flush,
                    committer_shutdown,
                    committer_health,
                    committer_checkpoint,
                    wait,
                );
            })
            .map_err(|e| format!("spawn committer: {}", e))?;

        // Spawn Sealer (if hash enabled, single-shard only in v1.1)
        // Multi-shard hash chain requires global merge ordering — deferred to v1.2.
        #[cfg(feature = "hash-chain")]
        let sealer_handle = if hash_enabled {
            if config.shards > 1 {
                return Err(
                    "hash-chain is not supported with shards > 1 in v1.1. \
                     Use shards=1 with hash-chain, or shards>1 without hash."
                        .to_string(),
                );
            }
            let sealer_ring = Arc::clone(shards.ring(0));
            let sealer_shutdown = Arc::clone(&shutdown);
            Some(
                std::thread::Builder::new()
                    .name("logdb-sealer".into())
                    .spawn(move || {
                        pipeline::sealer::run_sealer(
                            sealer_ring,
                            hash_init,
                            last_hash,
                            initial_seqs[0],
                            sealer_shutdown,
                            wait,
                        );
                    })
                    .map_err(|e| format!("spawn sealer: {}", e))?,
            )
        } else {
            None
        };

        let manifests: Vec<Arc<Mutex<reader::SegmentManifest>>> = (0..num_shards)
            .map(|s| {
                let dir = if num_shards == 1 {
                    data_dir.clone()
                } else {
                    data_dir.join(format!("s{}", s))
                };
                Arc::new(Mutex::new(reader::SegmentManifest::new(dir)))
            })
            .collect();

        Ok(Self {
            inner: Arc::new(LogDbInner {
                config,
                shards,
                health,
                flush,
                shutdown,
                committer_handle: Some(committer_handle),
                #[cfg(feature = "hash-chain")]
                sealer_handle,
                data_dir,
                checkpoint_sequence: checkpoint,
                manifests,
            }),
        })
    }

    /// Append multiple records atomically. All records in the batch are
    /// committed together — either all visible after crash, or none.
    /// Returns the sequence number of the first record in the batch.
    ///
    /// All `contents.len()` sequences are reserved in one atomic
    /// [`claim_batch`](crate::ring::Ring::claim_batch) (no partial reservation),
    /// so consecutive batches never overwrite each other's slots.
    pub fn append_batch(&self, contents: &[&[u8]]) -> Result<u64, AppendError> {
        if contents.is_empty() { return Err(AppendError::ContentTooLarge { size: 0, max: 0 }); }
        let inner = &self.inner;
        if let Some(code) = inner.health.check() {
            return Err(match code { health::HEALTH_DISK_FULL => AppendError::DiskFull, _ => AppendError::Io("unhealthy".into()) });
        }
        // Validate ALL contents BEFORE reserving sequences. A too-large record
        // found after a partial claim_batch would leave reserved-but-unwritten
        // slots (a gap the Committer can't cross).
        for content in contents {
            if content.len() > inner.config.max_content_size {
                return Err(AppendError::ContentTooLarge { size: content.len(), max: inner.config.max_content_size });
            }
        }
        if !inner.shutdown.enter() { return Err(AppendError::ShuttingDown); }
        let _guard = scopeguard::guard((), |_| inner.shutdown.leave());

        // Reserve the whole batch atomically (producer_cursor += n).
        let n = contents.len() as u64;
        let (first_id, shard_id, local_first) =
            inner.shards.claim_batch(n, inner.config.queue_full_policy)?;
        let ts = platform::clock_realtime_coarse_ns();
        let ring = inner.shards.ring(shard_id);
        let shard_bits = inner.shards.shard_bits();
        for (i, content) in contents.iter().enumerate() {
            let local_seq = local_first + i as u64;
            let global_id = shard::encode_record_id(shard_id, local_seq, shard_bits);
            // Safety: claim_batch reserved the LOCAL range [local_first, local_first+n)
            // exclusively. Slot is indexed by LOCAL seq; record_id stores the GLOBAL id.
            unsafe { ring.slot(local_seq).producer_write(global_id, ts, content); }
            ring.slot(local_seq).publish(local_seq);
        }
        Ok(first_id)
    }

    /// Append a record to the log. Returns the global record_id.
    pub fn append(&self, content: &[u8]) -> Result<u64, AppendError> {
        let inner = &self.inner;

        // Health check (self-healing)
        if let Some(code) = inner.health.check() {
            return Err(match code {
                health::HEALTH_DISK_FULL => AppendError::DiskFull,
                _ => AppendError::Io("health check failed".into()),
            });
        }

        // Content size check
        if content.len() > inner.config.max_content_size {
            return Err(AppendError::ContentTooLarge {
                size: content.len(),
                max: inner.config.max_content_size,
            });
        }

        // Shutdown guard
        if !inner.shutdown.enter() {
            return Err(AppendError::ShuttingDown);
        }
        let _guard = scopeguard::guard((), |_| inner.shutdown.leave());

        // CAS claim via shard map (v1.1 multi-shard)
        let (global_id, shard_id, local_seq) =
            inner.shards.claim(inner.config.queue_full_policy)?;

        // Write slot (safety: claim guarantees exclusive access). Slot is indexed
        // by LOCAL seq; record_id stores the GLOBAL id so read-back by global id
        // works under sharding (shards=1: global == local, behavior unchanged).
        let ts = platform::clock_realtime_coarse_ns();
        let ring = inner.shards.ring(shard_id);
        unsafe { ring.slot(local_seq).producer_write(global_id, ts, content); }

        // Publish
        ring.slot(local_seq).publish(local_seq);

        Ok(global_id)
    }

    /// Replicate a record at an EXACT sequence number.
    ///
    /// Used by logdbd standby nodes to write records received from the primary
    /// at the primary's own sequence, preserving the global offset space so
    /// that consumers can fail over primary → standby without re-mapping
    /// offsets. Unlike [`append`](LogDb::append), this does NOT claim a fresh
    /// sequence; it writes directly to the slot for `sequence`.
    ///
    /// Constraints (standby contract):
    /// - **Single-shard only.** Replication maps the primary's linear sequence
    ///   1:1 onto shard 0, so `shards` must be 1.
    /// - **In-order.** `sequence` must equal the current producer cursor (the
    ///   next expected sequence). Gaps return an error so the caller retries.
    /// - **Idempotent.** A `sequence` already replicated (below the cursor) is
    ///   a no-op, so duplicate/replayed Sync RPCs are safe.
    /// - **Backpressured.** Refuses to overwrite a live (uncommitted) slot via
    ///   the same consume-watermark gate as `claim`, returning `QueueFull`.
    ///
    /// The record is published for the Committer to serialize and fsync like
    /// any other; `producer_cursor` is advanced so `flush`/`shutdown` compute
    /// the correct durability target.
    pub fn replicate(
        &self,
        sequence: u64,
        timestamp_ns: u64,
        content: &[u8],
    ) -> Result<(), AppendError> {
        let inner = &self.inner;

        // Replication is a linear stream onto shard 0.
        if inner.shards.num_shards() != 1 {
            return Err(AppendError::Io("replicate requires shards=1".into()));
        }
        if content.len() > inner.config.max_content_size {
            return Err(AppendError::ContentTooLarge {
                size: content.len(),
                max: inner.config.max_content_size,
            });
        }
        if let Some(code) = inner.health.check() {
            return Err(match code {
                health::HEALTH_DISK_FULL => AppendError::DiskFull,
                _ => AppendError::Io("health check failed".into()),
            });
        }
        if !inner.shutdown.enter() {
            return Err(AppendError::ShuttingDown);
        }
        let _guard = scopeguard::guard((), |_| inner.shutdown.leave());

        let ring = inner.shards.ring(0);
        let ring_size = ring.ring_size() as u64;

        // Idempotency: already replicated past this sequence.
        let cur = ring.producer_cursor.inner.load(Ordering::Acquire);
        if sequence < cur {
            return Ok(());
        }
        // In-order: sequence must be exactly the next expected slot.
        if sequence != cur {
            return Err(AppendError::Io(format!(
                "replicate out of order: expected {}, got {}",
                cur, sequence
            )));
        }
        // Backpressure: do not overwrite a slot the Committer has not drained.
        // Same invariant as Ring::claim (seq - watermark < ring_size).
        let wm = ring.consume_watermark();
        if sequence.wrapping_sub(wm) >= ring_size {
            return Err(AppendError::QueueFull);
        }

        // Safety: the standby serializes Sync RPCs externally (and local writes
        // are rejected on a standby), so this slot is not being written by any
        // other producer. `sequence` was validated to equal the monotonic
        // cursor, so the slot has not yet been consumed (it is below committed
        // only by the watermark gate above). Mirrors append()'s proof.
        unsafe { ring.slot(sequence).producer_write(sequence, timestamp_ns, content); }
        ring.slot(sequence).publish(sequence);

        // Advance the cursor so flush/shutdown target these records for fsync.
        // Single-writer on the standby (no local appends); a plain Release
        // store is correct and races are prevented by the caller's lock.
        ring.producer_cursor.inner.store(sequence + 1, Ordering::Release);

        Ok(())
    }

    /// Force all previously appended records to durable storage.
    ///
    /// Waits for `durable_cursor` (NOT committed_cursor — fix C4).
    pub fn flush(&self) -> Result<(), FlushError> {
        let inner = &self.inner;

        // Per-shard snapshot: flush completes when EVERY shard's durable reaches
        // its own producer-cursor snapshot (handles uneven sharded loads).
        let targets = inner.shards.producer_cursors();
        if targets.iter().all(|&t| t == 0) {
            return Ok(());
        }

        // If hash enabled (shards=1 only), wait for Sealer first.
        #[cfg(feature = "hash-chain")]
        if inner.config.hash_enabled {
            let target0 = targets[0];
            wait_until(
                &inner.shutdown,
                || inner.shards.ring(0).sealed_cursor.load(Ordering::Acquire) >= target0,
                inner.config.flush_timeout,
            )?;
        }

        inner.flush.request(&targets);
        wait_until(
            &inner.shutdown,
            || inner.flush.is_done(&targets),
            inner.config.flush_timeout,
        )?;

        Ok(())
    }

    /// Read a single record by `record_id` (a global id).
    ///
    /// Decodes the global id to its owning shard, gates on that shard's
    /// durable cursor (per-shard visibility), and reads from that shard's
    /// manifest. shards=1: shard 0, local == record_id (zero-regression).
    pub fn read(&self, record_id: u64) -> Result<Option<Record>, ReadError> {
        let inner = &self.inner;
        let (shard, local) = shard::decode_record_id(record_id, inner.shards.shard_bits());
        let durable_s = inner
            .shards
            .durable_cursors()
            .get(shard)
            .copied()
            .unwrap_or(0);
        if local >= durable_s {
            return Ok(None);
        }
        let reader = reader::Reader::new(
            Arc::clone(&inner.manifests[shard]),
            inner.config.encryption_key,
        );
        let r = reader.read(record_id);
        if matches!(r, Ok(None)) {
        }
        r
    }

    // ── Internal state accessors (for diagnostics/benchmarking) ─────────

    /// Get the maximum producer cursor across all shards.
    pub fn producer_cursor(&self) -> u64 {
        self.inner.shards.max_producer_cursor()
    }

    /// Get the minimum committed cursor across all shards.
    pub fn committed_cursor(&self) -> u64 {
        self.inner.shards.min_committed_cursor()
    }

    /// Get the minimum durable cursor across all shards.
    pub fn durable_cursor(&self) -> u64 {
        self.inner.shards.min_durable_cursor()
    }

    /// Get the total ring capacity across all shards.
    pub fn ring_size(&self) -> usize {
        self.inner.shards.num_shards() * self.inner.shards.ring(0).ring_size()
    }

    /// Create a named tailer (consumer) with independent read progress.
    ///
    /// Progress is tracked per shard and persisted to `tailer_<name>.dat` via
    /// `commit()`. See [`tailer`](crate::tailer) for the sharding semantics
    /// (per-shard progress, merged-batch delivery, best-effort cross-batch
    /// ordering when a shard stalls).
    pub fn new_tailer(&self, name: &str) -> crate::tailer::Tailer {
        let rings: Vec<Arc<Ring>> = self
            .inner
            .shards
            .all_rings()
            .iter()
            .map(Arc::clone)
            .collect();
        let manifests: Vec<Arc<std::sync::Mutex<reader::SegmentManifest>>> =
            self.inner.manifests.iter().map(Arc::clone).collect();
        crate::tailer::Tailer::open(
            manifests,
            rings,
            self.inner.shards.shard_bits(),
            name,
            self.inner.config.encryption_key,
            self.inner.data_dir.clone(),
        )
    }

    /// Scan records in range `[from_id, to_id)` across ALL shards, ordered by
    /// ascending global id. `shards=1` returns a single cross-segment stream;
    /// `shards>1` k-way-merges the per-shard streams. An empty range yields an
    /// empty iterator (no error).
    pub fn scan(
        &self,
        from_id: u64,
        to_id: u64,
    ) -> Result<reader::ScanIter, ReadError> {
        let manifests = self
            .inner
            .manifests
            .iter()
            .map(Arc::clone)
            .collect();
        reader::ScanIter::build(manifests, self.inner.config.encryption_key, from_id, to_id)
    }

    /// Mark `sequence` as the WAL checkpoint.
    ///
    /// Records with sequence < checkpoint are safe to delete. Old segments
    /// fully covered by the checkpoint will be truncated on the next roll.
    pub fn checkpoint(&self, sequence: u64) {
        let mut cur = self.inner.checkpoint_sequence.load(Ordering::Acquire);
        while sequence > cur {
            match self.inner.checkpoint_sequence.compare_exchange_weak(
                cur, sequence, Ordering::Release, Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
        // Persist to disk so crash recovery can read it
        let _ = save_checkpoint(&self.inner.data_dir, sequence);
    }

    /// Get the current checkpoint sequence.
    pub fn checkpoint_sequence(&self) -> u64 {
        self.inner.checkpoint_sequence.load(Ordering::Acquire)
    }

    /// Recover the checkpoint from disk (called during startup).
    pub(crate) fn load_checkpoint(data_dir: &std::path::Path) -> u64 {
        let path = data_dir.join("checkpoint.dat");
        match std::fs::read(&path) {
            Ok(data) if data.len() == 12 => {
                let seq = u64::from_le_bytes([data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]]);
                let crc = u32::from_le_bytes([data[8],data[9],data[10],data[11]]);
                if crc32c::crc32c(&data[..8]) == crc { seq } else { 0 }
            }
            _ => 0,
        }
    }

    /// Get WAL space usage: (used_bytes, total_bytes_configured).
    ///
    /// `used_bytes` is the sum of all segment file sizes — flat in `data_dir`
    /// for `shards == 1`, and across every `s<shard>/` subdir for `shards > 1`.
    pub fn wal_usage(&self) -> (u64, u64) {
        let mut total = count_log_bytes(&self.inner.data_dir);
        // shards>1 lays segments under data_dir/s<shard>/ — sum those too.
        if let Ok(entries) = std::fs::read_dir(&self.inner.data_dir) {
            for e in entries.flatten() {
                let path = e.path();
                if path.is_dir() {
                    total += count_log_bytes(&path);
                }
            }
        }
        (total, self.inner.config.segment_size)
    }

    /// Recovery report for database WAL replay.
    pub fn recovery_report(&self) -> RecoveryReport {
        let cp = self.checkpoint_sequence();
        let durable = self.durable_cursor();
        RecoveryReport {
            from_sequence: cp,
            to_sequence: durable,
            count: if durable > cp { durable - cp } else { 0 },
        }
    }

    /// Replay records from `sequence` (inclusive) to the end of the log, across
    /// all shards, ordered by ascending global id.
    pub fn replay_from(
        &self,
        sequence: u64,
    ) -> Result<reader::ScanIter, ReadError> {
        self.scan(sequence, u64::MAX)
    }

    /// Drain in-flight appends and flush all published records to durable
    /// storage — WITHOUT consuming the handle or joining background threads.
    ///
    /// This is the shared-safe drain path: unlike [`shutdown`](LogDb::shutdown)
    /// it takes `&self`, so it works when the `LogDb` is shared via `Arc`
    /// (e.g. inside a long-running service like logdbd). It enters the drain
    /// phase (rejecting new appends with `ShuttingDown`), waits for in-flight
    /// appends to publish, then waits for the Committer to fsync everything up
    /// to the producer cursor.
    ///
    /// After this returns `Ok(Clean)`, every record appended before the call is
    /// durable. The background threads keep running; the process may then exit
    /// (threads are aborted on drop, harmlessly, since data is already durable),
    /// or [`shutdown`] may be called to join them.
    pub fn drain(&self, timeout: Duration) -> Result<ShutdownReport, FlushError> {
        let inner = &self.inner;
        let deadline = Instant::now() + timeout;

        // Phase 1: Drain — reject new appends.
        inner.shutdown.start_drain();

        // Wait for all in-flight appends to publish.
        loop {
            if inner.shutdown.in_flight.load(Ordering::Acquire) == 0 {
                break;
            }
            if Instant::now() >= deadline {
                inner.shutdown.abort();
                return Err(FlushError::Timeout);
            }
            std::hint::spin_loop();
        }

        // Phase 2: flush everything published up to each shard's producer cursor.
        let targets = inner.shards.producer_cursors();
        // drain_target is a single best-effort signal (max across shards); the
        // committer drains per-shard via the FlushSignal targets.
        let max_target = targets.iter().copied().max().unwrap_or(0);
        inner.shutdown.drain_target.store(max_target, Ordering::Release);
        inner.flush.request(&targets);

        let remaining = deadline.saturating_duration_since(Instant::now());
        let durable_ok = wait_until(
            &inner.shutdown,
            || inner.flush.is_done(&targets),
            remaining,
        )
        .is_ok();

        Ok(if durable_ok {
            ShutdownReport::Clean
        } else {
            ShutdownReport::PartialDurable
        })
    }

    /// Shut down gracefully with timeout: drain (flush all to durable) then
    /// join the background threads. Consumes the handle and requires it be the
    /// only strong reference (so the Committer/Sealer can be joined). For
    /// shared handles (e.g. inside a service), use [`drain`](LogDb::drain).
    pub fn shutdown(mut self, timeout: Duration) -> Result<ShutdownReport, ShutdownError> {
        // Drain first (shared-safe path).
        let report = self.drain(timeout).map_err(|_| ShutdownError::Timeout)?;

        // Join threads — requires exclusive access.
        let inner = match Arc::get_mut(&mut self.inner) {
            Some(i) => i,
            None => return Err(ShutdownError::JoinError("LogDb still referenced".into())),
        };
        if let Some(h) = inner.committer_handle.take() {
            let _ = h.join();
        }
        #[cfg(feature = "hash-chain")]
        if let Some(h) = inner.sealer_handle.take() {
            let _ = h.join();
        }

        Ok(report)
    }
}

/// Unified wait with timeout and abort checking.
fn wait_until(
    shutdown: &ShutdownState,
    cond: impl Fn() -> bool,
    timeout: Duration,
) -> Result<(), FlushError> {
    let deadline = Instant::now() + timeout;
    let mut spins: u32 = 0;
    loop {
        if cond() {
            return Ok(());
        }
        if shutdown.aborted() {
            return Err(FlushError::Aborted);
        }
        if Instant::now() >= deadline {
            return Err(FlushError::Timeout);
        }
        spins = spins.saturating_add(1);
        if spins <= 64 {
            std::hint::spin_loop();
        } else if spins <= 256 {
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_micros(100));
            spins = 128;
        }
    }
}

fn save_checkpoint(dir: &std::path::Path, seq: u64) -> std::io::Result<()> {
    let path = dir.join("checkpoint.dat");
    let tmp = dir.join("checkpoint.tmp");
    let mut buf = [0u8; 12];
    buf[0..8].copy_from_slice(&seq.to_le_bytes());
    let crc = crc32c::crc32c(&buf[..8]);
    buf[8..12].copy_from_slice(&crc.to_le_bytes());
    let mut f = std::fs::File::create(&tmp)?;
    std::io::Write::write_all(&mut f, &buf)?;
    platform::fdatasync(&f)?;
    drop(f);
    std::fs::rename(&tmp, &path)?;
    let d = std::fs::File::open(dir)?;
    platform::sync_dir(&d)?;
    Ok(())
}

/// Sum the sizes of all `*.log` files directly in `dir` (non-recursive).
fn count_log_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if e.file_name().to_str().map_or(false, |n| n.ends_with(".log")) {
                if let Ok(meta) = e.metadata() {
                    total += meta.len();
                }
            }
        }
    }
    total
}

fn generate_hash_init() -> [u8; 32] {
    #[cfg(feature = "hash-chain")]
    {
        // BLAKE3 keyed mode uses hash_init as the key.
        // Generate it from CSPRNG-quality entropy.
        let mut hasher = blake3::Hasher::new();
        hasher.update(&platform::clock_realtime_coarse_ns().to_le_bytes());
        hasher.update(b"logdb-hash-init-v0.2.0");
        *hasher.finalize().as_bytes()
    }
    #[cfg(not(feature = "hash-chain"))]
    {
        [0u8; 32]
    }
}

impl Drop for LogDb {
    fn drop(&mut self) {
        self.inner.shutdown.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::DurabilityMode;
    use std::sync::Arc;

    #[test]
    fn open_and_append_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();
        let id = db.append(b"hello logdb").unwrap();
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));

        let record = db.read(id).unwrap().unwrap();
        assert_eq!(record.id.sequence, id);
        assert_eq!(record.content, b"hello logdb");
    }

    #[test]
    fn append_rejected_content_too_large() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.max_content_size = 100;

        let db = LogDb::open(config).unwrap();
        let err = db.append(&vec![0u8; 200]).unwrap_err();
        assert!(matches!(err, AppendError::ContentTooLarge { .. }));
    }

    #[test]
    fn shutdown_clean() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();
        for i in 0..10 {
            db.append(format!("r-{}", i).as_bytes()).unwrap();
        }
        let report = db.shutdown(Duration::from_secs(5)).unwrap();
        assert!(matches!(report, ShutdownReport::Clean));
    }

    #[test]
    fn drain_flushes_to_durable_and_rejects_appends_after() {
        // drain() is the shared-safe path logdbd uses on graceful shutdown:
        // it must flush all in-flight records to durable without consuming the
        // handle, and reject further appends once draining.
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();
        for i in 0..20 {
            db.append(format!("r-{}", i).as_bytes()).unwrap();
        }

        let report = db.drain(Duration::from_secs(5)).unwrap();
        assert!(matches!(report, ShutdownReport::Clean), "drain must complete clean");
        assert!(db.durable_cursor() >= 20, "all appended records must be durable after drain");
        for i in 0..20 {
            assert!(db.read(i).unwrap().is_some(), "record {} readable after drain", i);
        }

        // Drain phase rejects new appends.
        let err = db.append(b"after-drain").unwrap_err();
        assert!(matches!(err, AppendError::ShuttingDown), "append after drain must be rejected");
    }

    #[test]
    fn replicate_preserves_sequence_and_is_readable() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();
        // Replicate 5 records at exact sequences 0..5 with arbitrary timestamps.
        for i in 0..5u64 {
            db.replicate(i, 1_000_000 + i, format!("replica-{}", i).as_bytes()).unwrap();
        }
        assert_eq!(db.producer_cursor(), 5, "producer cursor must advance");
        db.flush().unwrap();
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(25));
            if db.durable_cursor() >= 5 { break; }
        }
        assert!(db.durable_cursor() >= 5);

        // Sequences must be EXACTLY preserved (offset semantics).
        for i in 0..5u64 {
            let rec = db.read(i).unwrap().unwrap();
            assert_eq!(rec.id.sequence, i);
            assert_eq!(rec.timestamp_ns, 1_000_000 + i);
            assert_eq!(rec.content, format!("replica-{}", i).as_bytes());
        }
    }

    #[test]
    fn replicate_rejects_out_of_order_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();
        db.replicate(0, 0, b"a").unwrap();
        // Skipping ahead to 2 (gap at 1) must error — caller retries in order.
        let err = db.replicate(2, 0, b"c").unwrap_err();
        assert!(matches!(err, AppendError::Io(_)), "expected out-of-order error");
        // Filling the gap succeeds.
        db.replicate(1, 0, b"b").unwrap();
        // Re-replicating an already-applied sequence is a no-op (idempotent).
        db.replicate(0, 0, b"REPLAY").unwrap();
        // Only seq 0 and 1 were applied (seq 2 was rejected) → cursor at 2,
        // unchanged by the idempotent replay.
        assert_eq!(db.producer_cursor(), 2);
    }

    // ── cr-003 Phase 1: sharded write/durability ─────────────────────────

    // Read back every durable record from a single shard's directory by pointing
    // the existing single-shard Reader at it. Within one shard, global ids are
    // monotonic, so the existing reader works (cross-shard routing is Phase 2).
    fn read_all_in_dir(dir: &std::path::Path) -> Vec<Vec<u8>> {
        use std::sync::{Arc, Mutex};
        let manifest = Arc::new(Mutex::new(reader::SegmentManifest::new(dir.to_path_buf())));
        let reader = reader::Reader::new(manifest, None);
        let mut out = Vec::new();
        if let Ok(iter) = reader.scan(0, u64::MAX) {
            for r in iter {
                if let Ok(rec) = r {
                    out.push(rec.content);
                }
            }
        }
        out
    }

    #[test]
    fn append_under_sharding_is_durable_per_shard() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 2;
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        // Single-threaded appends are thread-affine → all land on one shard
        // (uneven load). flush must still complete and every record be durable.
        for i in 0..6u64 {
            db.append(format!("rec-{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();

        let mut got: Vec<Vec<u8>> = (0..2u32)
            .flat_map(|s| read_all_in_dir(&dir.path().join(format!("s{}", s))))
            .collect();
        got.sort();
        let mut want: Vec<Vec<u8>> = (0..6u64).map(|i| format!("rec-{}", i).into_bytes()).collect();
        want.sort();
        assert_eq!(got, want, "all appended records must be durable per-shard under sharding");
    }

    #[test]
    fn append_batch_under_sharding_is_durable_per_shard() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 2;
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        let batch: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma", b"delta"];
        db.append_batch(&batch).unwrap();
        db.flush().unwrap(); // cr-003: previously timed out / lost the batch

        let mut got: Vec<Vec<u8>> = (0..2u32)
            .flat_map(|s| read_all_in_dir(&dir.path().join(format!("s{}", s))))
            .collect();
        got.sort();
        let mut want: Vec<Vec<u8>> = batch.iter().map(|b| b.to_vec()).collect();
        want.sort();
        assert_eq!(got, want, "append_batch records must all be durable per-shard under sharding");
    }

    // ── cr-003 Phase 2: sharded read (point lookup) ──────────────────────

    #[test]
    fn read_under_sharding_returns_record_by_global_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 2;
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        // Single-threaded appends are thread-affine → one shard. Capture each
        // returned global id and read it back by that id.
        let mut ids = Vec::new();
        for i in 0..6u64 {
            let id = db.append(format!("rec-{}", i).as_bytes()).unwrap();
            ids.push((id, format!("rec-{}", i).into_bytes()));
        }
        db.flush().unwrap();

        for (id, want) in &ids {
            let rec = db.read(*id).unwrap().expect("readable by global id");
            assert_eq!(&rec.content, want);
            assert_eq!(rec.id.sequence, *id);
        }
    }

    #[test]
    fn read_under_sharding_append_batch_first_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 2;
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        let batch: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma", b"delta"];
        let first = db.append_batch(&batch).unwrap();
        db.flush().unwrap();

        // append_batch returns the first record's global id; read it back.
        let rec = db.read(first).unwrap().expect("first batch record readable");
        assert_eq!(rec.content, b"alpha");
        assert_eq!(rec.id.sequence, first);
    }

    #[test]
    fn read_under_sharding_not_visible_before_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 2;
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Async; // fsync only on explicit flush
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        let id = db.append(b"not-yet-durable").unwrap();
        // In Async mode the committer does not fsync without a flush, so the
        // per-shard durable cursor has not advanced → read returns None.
        assert!(db.read(id).unwrap().is_none(), "not visible before durable");
        db.flush().unwrap();
        let rec = db.read(id).unwrap().expect("visible after flush");
        assert_eq!(rec.content, b"not-yet-durable");
    }

    // ── cr-003 Phase 3: cross-shard + cross-segment scan ─────────────────

    #[test]
    fn scan_under_sharding_is_complete_and_ordered() {
        // Multi-thread appends spread across shards (thread-affine routing).
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        let db = Arc::new(db);
        let mut handles = Vec::new();
        let mut all_ids = Vec::new();
        for t in 0..4u64 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                let mut ids = Vec::new();
                for i in 0..10u64 {
                    ids.push(db.append(format!("t{}-{}", t, i).as_bytes()).unwrap());
                }
                ids
            }));
        }
        for h in handles {
            all_ids.extend(h.join().unwrap());
        }
        db.flush().unwrap();

        // Every appended record must be visible; ids strictly ascending.
        let scanned: Vec<u64> = db
            .scan(0, u64::MAX).unwrap()
            .filter_map(|r| r.ok())
            .map(|r| r.id.sequence)
            .collect();
        assert_eq!(scanned.len(), all_ids.len(), "scan must see every record across shards");
        assert!(all_ids.iter().all(|id| scanned.contains(id)), "scan missing some ids");
        assert!(scanned.windows(2).all(|w| w[0] < w[1]), "scan must be strictly ascending");
    }

    #[test]
    fn scan_under_sharding_respects_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 2;
        config.ring_size = 128;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = Arc::new(LogDb::open(config).unwrap());

        let mut all_ids = Vec::new();
        let mut handles = Vec::new();
        for t in 0..2u64 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                (0..12u64).map(|i| db.append(format!("t{}-{}", t, i).as_bytes()).unwrap()).collect::<Vec<_>>()
            }));
        }
        for h in handles {
            all_ids.extend(h.join().unwrap());
        }
        db.flush().unwrap();

        all_ids.sort();
        let from = all_ids[5];
        let to = all_ids[15];
        let got: Vec<u64> = db.scan(from, to).unwrap()
            .filter_map(|r| r.ok()).map(|r| r.id.sequence).collect();
        let want: Vec<u64> = all_ids.iter().copied().filter(|&id| id >= from && id < to).collect();
        assert_eq!(got, want, "scan([from,to)) must clip to the global-id range");
    }

    #[test]
    fn scan_crosses_segment_boundary_single_shard() {
        // Force a segment roll: tiny segment_size, write >1 segment of data.
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 1;
        config.segment_size = 1 * 1024 * 1024; // 1MB minimum
        config.ring_size = 8192;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);
        let db = LogDb::open(config).unwrap();

        let payload = vec![0xA5u8; 64 * 1024]; // 64KB each -> ~17 records fill >1MB
        let n = 20u64;
        for _ in 0..n {
            db.append(&payload).unwrap();
        }
        db.flush().unwrap();

        // More than one segment file must exist (the roll happened).
        let segs = std::fs::read_dir(dir.path()).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().map_or(false, |n| n.ends_with(".log")))
            .count();
        assert!(segs >= 2, "expected a segment roll, found {} segment files", segs);

        // scan must return ALL records across both segments, ascending.
        let scanned: Vec<u64> = db.scan(0, u64::MAX).unwrap()
            .filter_map(|r| r.ok()).map(|r| r.id.sequence).collect();
        assert_eq!(scanned.len(), n as usize, "scan must cross the segment boundary");
        assert!(scanned.windows(2).all(|w| w[0] < w[1]));
        assert_eq!((0..n).collect::<Vec<_>>(), scanned);
    }

    #[test]
    fn replay_from_under_sharding_returns_tail_ordered() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = Arc::new(LogDb::open(config).unwrap());

        let mut all_ids = Vec::new();
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                (0..8u64).map(|i| db.append(format!("t{}-{}", t, i).as_bytes()).unwrap()).collect::<Vec<_>>()
            }));
        }
        for h in handles {
            all_ids.extend(h.join().unwrap());
        }
        db.flush().unwrap();

        all_ids.sort();
        let pivot = all_ids[10];
        let tail: Vec<u64> = db.replay_from(pivot).unwrap()
            .filter_map(|r| r.ok()).map(|r| r.id.sequence).collect();
        let want: Vec<u64> = all_ids.iter().copied().filter(|&id| id >= pivot).collect();
        assert_eq!(tail, want, "replay_from must return the ordered tail across shards");
    }

    // ── cr-003 Phase 4: per-shard crash recovery ──────────────────────────

    #[test]
    fn reopen_under_sharding_preserves_all_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        // Session 1: spread appends across all 4 shards (thread-affine routing).
        let mut all_ids = Vec::new();
        {
            let db = Arc::new(LogDb::open(config.clone()).unwrap());
            let mut handles = Vec::new();
            for t in 0..4u64 {
                let db = Arc::clone(&db);
                handles.push(std::thread::spawn(move || {
                    (0..10u64)
                        .map(|i| db.append(format!("t{}-{}", t, i).as_bytes()).unwrap())
                        .collect::<Vec<_>>()
                }));
            }
            for h in handles {
                all_ids.extend(h.join().unwrap());
            }
            db.flush().unwrap();
            let db = Arc::try_unwrap(db).ok().unwrap();
            db.shutdown(Duration::from_secs(5)).unwrap();
        }

        // Session 2: reopen — recovery must run per shard and preserve every record.
        let db = LogDb::open(config).unwrap();
        let scanned: Vec<u64> = db
            .scan(0, u64::MAX).unwrap()
            .filter_map(|r| r.ok())
            .map(|r| r.id.sequence)
            .collect();
        assert_eq!(scanned.len(), all_ids.len(), "reopen must preserve every record across shards");
        assert!(all_ids.iter().all(|id| scanned.contains(id)), "reopen lost some ids");
        assert!(scanned.windows(2).all(|w| w[0] < w[1]), "scanned ids must be strictly ascending");
    }

    #[test]
    fn reopen_under_sharding_single_thread_handles_empty_shards() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let ids: Vec<u64> = {
            let db = LogDb::open(config.clone()).unwrap();
            // Single-threaded appends are thread-affine → all land on ONE shard;
            // the other three shards get only an empty first segment.
            let mut ids = Vec::new();
            for i in 0..6u64 {
                ids.push(db.append(format!("r-{}", i).as_bytes()).unwrap());
            }
            db.flush().unwrap();
            db.shutdown(Duration::from_secs(5)).unwrap();
            ids
        };

        let db = LogDb::open(config).unwrap();
        let scanned: Vec<u64> = db
            .scan(0, u64::MAX).unwrap()
            .filter_map(|r| r.ok())
            .map(|r| r.id.sequence)
            .collect();
        assert_eq!(scanned, ids, "empty shards must not lose the written shard's records");
        // The empty shards resume at local 0; a fresh append must still produce a
        // collision-free global id and be readable.
        let nid = db.append(b"after-reopen").unwrap();
        db.flush().unwrap();
        assert!(db.read(nid).unwrap().is_some(), "post-reopen append must read back");
    }

    #[test]
    fn reopen_under_sharding_then_append_no_id_collision() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let mut all_ids = Vec::new();
        {
            let db = Arc::new(LogDb::open(config.clone()).unwrap());
            let mut handles = Vec::new();
            for t in 0..4u64 {
                let db = Arc::clone(&db);
                handles.push(std::thread::spawn(move || {
                    (0..8u64)
                        .map(|i| db.append(format!("t{}-{}", t, i).as_bytes()).unwrap())
                        .collect::<Vec<_>>()
                }));
            }
            for h in handles {
                all_ids.extend(h.join().unwrap());
            }
            db.flush().unwrap();
            let db = Arc::try_unwrap(db).ok().unwrap();
            db.shutdown(Duration::from_secs(5)).unwrap();
        }

        let db = Arc::new(LogDb::open(config).unwrap());
        // Append more from multiple threads after reopen — per-shard resume points
        // must keep the global id space collision-free.
        let mut new_ids = Vec::new();
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                (0..5u64)
                    .map(|i| db.append(format!("n{}-{}", t, i).as_bytes()).unwrap())
                    .collect::<Vec<_>>()
            }));
        }
        for h in handles {
            new_ids.extend(h.join().unwrap());
        }
        db.flush().unwrap();

        let mut seen = std::collections::HashSet::new();
        for id in all_ids.iter().chain(new_ids.iter()) {
            assert!(seen.insert(*id), "global id collision after reopen: {}", id);
        }
        // Every old id is still readable after the post-reopen appends.
        for id in &all_ids {
            assert!(db.read(*id).unwrap().is_some(), "old id {} lost after reopen+append", id);
        }
    }

    #[test]
    fn reopen_under_sharding_detects_torn_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 2;
        config.ring_size = 128;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        // Single-threaded → all 20 records land on ONE shard; the other is empty.
        {
            let db = LogDb::open(config.clone()).unwrap();
            for i in 0..20u64 {
                db.append(format!("r-{}", i).as_bytes()).unwrap();
            }
            db.flush().unwrap();
            db.shutdown(Duration::from_secs(5)).unwrap();
        }

        // Corrupt the non-empty shard's active segment: chop 5 bytes off the end
        // (mid-record) → the last record becomes a torn tail.
        let victim = (0..2u32)
            .map(|s| dir.path().join(format!("s{}", s)).join("segment-00000001.log"))
            .find(|p| p.exists() && std::fs::metadata(p).map(|m| m.len() > 200).unwrap_or(false))
            .expect("some shard must have received records");
        let len = std::fs::metadata(&victim).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&victim)
            .unwrap()
            .set_len(len - 5)
            .unwrap();

        // Recovery must succeed: the torn tail is truncated, the rest survives.
        // (The pre-fix stride bug truncated to a single record; this guards that.)
        let db = LogDb::open(config).unwrap();
        let scanned: Vec<Vec<u8>> = db
            .scan(0, u64::MAX).unwrap()
            .filter_map(|r| r.ok())
            .map(|r| r.content)
            .collect();
        assert!(
            scanned.len() >= 18,
            "torn-write recovery lost too many records: {}",
            scanned.len()
        );
    }

    #[test]
    fn reopen_shards1_still_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let id = {
            let db = LogDb::open(config.clone()).unwrap();
            let id = db.append(b"shards1-recovery").unwrap();
            db.flush().unwrap();
            db.shutdown(Duration::from_secs(5)).unwrap();
            id
        };
        // shards=1 goes through the same unified per-shard loop (shard_bits=0).
        let db = LogDb::open(config).unwrap();
        let rec = db.read(id).unwrap().expect("shards=1 record must survive reopen");
        assert_eq!(rec.content, b"shards1-recovery");
        // Resume + append, no collision.
        let nid = db.append(b"after").unwrap();
        db.flush().unwrap();
        assert!(db.read(nid).unwrap().is_some());
    }

    // ── cr-003 Phase 5: cross-shard tailer (per-shard progress + merge) ───

    #[test]
    fn tailer_under_sharding_reads_all_shards_merged() {
        // Multi-thread appends spread across shards (thread-affine routing).
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = Arc::new(LogDb::open(config).unwrap());

        let total = 40u64; // 4 threads × 10
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                (0..10u64)
                    .map(|i| db.append(format!("t{}-{}", t, i).as_bytes()).unwrap())
                    .collect::<Vec<_>>()
            }));
        }
        let mut all_ids = Vec::new();
        for h in handles {
            all_ids.extend(h.join().unwrap());
        }
        db.flush().unwrap();
        // Wait until every shard is durable (min durable cursor advances).
        for _ in 0..50 {
            if db.durable_cursor() >= 10 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let mut t = db.new_tailer("merge");
        let mut got: Vec<u64> = Vec::new();
        // Drain in batches; bound the wait so the test fails fast if broken.
        for _ in 0..200 {
            match t.next_batch(1000).unwrap() {
                Some(batch) => got.extend(batch.iter().map(|r| r.id.sequence)),
                None => break,
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(got.len(), total as usize, "tailer must see every record across ALL shards");
        assert!(got.windows(2).all(|w| w[0] < w[1]), "tailer batch must be ascending global id");
        assert!(all_ids.iter().all(|id| got.contains(id)), "tailer missing some ids: got={:?}", got);
        // Records must come from more than one shard (proves cross-shard merge).
        let shards_seen: std::collections::HashSet<usize> =
            got.iter().map(|&g| crate::shard::decode_record_id(g, 2).0).collect();
        assert!(
            shards_seen.len() > 1,
            "tailer should have merged multiple shards, saw {:?}",
            shards_seen
        );
    }

    #[test]
    fn tailer_under_sharding_single_shard_is_delivered() {
        // Single-thread appends all land on ONE shard (thread-affine routing).
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        let n = 60u64;
        let mut ids = Vec::new();
        for i in 0..n {
            ids.push(db.append(format!("s-{}", i).as_bytes()).unwrap());
        }
        db.flush().unwrap();
        for _ in 0..50 {
            if db.durable_cursor() >= 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let mut t = db.new_tailer("one-shard");
        let mut got: Vec<u64> = Vec::new();
        for _ in 0..200 {
            match t.next_batch(1000).unwrap() {
                Some(b) => got.extend(b.iter().map(|r| r.id.sequence)),
                None => break,
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            got.len(),
            n as usize,
            "single-thread writes (one shard) must all be delivered"
        );
        assert!(ids.iter().all(|id| got.contains(id)));
    }

    #[test]
    fn tailer_under_sharding_persists_per_shard_progress() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = Arc::new(LogDb::open(config).unwrap());

        let mut all_ids = Vec::new();
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                (0..10u64)
                    .map(|i| db.append(format!("t{}-{}", t, i).as_bytes()).unwrap())
                    .collect::<Vec<_>>()
            }));
        }
        for h in handles {
            all_ids.extend(h.join().unwrap());
        }
        db.flush().unwrap();
        for _ in 0..50 {
            if db.durable_cursor() >= 10 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // Drain 12 records, commit, then reopen.
        let mut delivered = Vec::new();
        {
            let mut t = db.new_tailer("persist");
            while delivered.len() < 12 {
                match t.next_batch(12).unwrap() {
                    Some(b) => delivered.extend(b.iter().map(|r| r.id.sequence)),
                    None => break,
                }
            }
            t.commit().unwrap();
        }
        let saved_positions = db.new_tailer("persist").positions().to_vec();
        assert!(
            saved_positions.iter().any(|&p| p > 0),
            "some shard progress must have persisted"
        );

        // Reopen and drain the rest; total must be all 40, exactly once.
        let mut t = db.new_tailer("persist");
        assert_eq!(
            t.positions(),
            saved_positions.as_slice(),
            "reopened tailer must restore per-shard positions"
        );
        let mut rest = Vec::new();
        for _ in 0..200 {
            match t.next_batch(1000).unwrap() {
                Some(b) => rest.extend(b.iter().map(|r| r.id.sequence)),
                None => break,
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let mut all = delivered.clone();
        all.extend(rest);
        all.sort();
        all_ids.sort();
        assert_eq!(all, all_ids, "commit+reopen must deliver every record exactly once");
    }

    #[test]
    fn tailer_under_sharding_resumes_without_loss() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = Arc::new(LogDb::open(config).unwrap());

        let spawn = |db: &Arc<LogDb>| -> Vec<u64> {
            let mut handles = Vec::new();
            for t in 0..4u64 {
                let db = Arc::clone(db);
                handles.push(std::thread::spawn(move || {
                    (0..8u64)
                        .map(|i| db.append(format!("t{}-{}", t, i).as_bytes()).unwrap())
                        .collect::<Vec<_>>()
                }));
            }
            let mut ids = Vec::new();
            for h in handles {
                ids.extend(h.join().unwrap());
            }
            ids
        };

        let first = spawn(&db);
        db.flush().unwrap();
        for _ in 0..50 {
            if db.durable_cursor() >= 8 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // Drain the first wave fully and commit.
        {
            let mut t = db.new_tailer("resume");
            let mut got = Vec::new();
            for _ in 0..200 {
                match t.next_batch(1000).unwrap() {
                    Some(b) => got.extend(b.iter().map(|r| r.id.sequence)),
                    None => break,
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(got.len(), first.len());
            t.commit().unwrap();
        }

        // Append a second wave.
        let second = spawn(&db);
        db.flush().unwrap();
        for _ in 0..50 {
            if db.durable_cursor() >= 8 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // Reopen: must deliver ONLY the second wave (no re-delivery, no loss).
        let mut t = db.new_tailer("resume");
        let mut got = Vec::new();
        for _ in 0..200 {
            match t.next_batch(1000).unwrap() {
                Some(b) => got.extend(b.iter().map(|r| r.id.sequence)),
                None => break,
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let mut want = second.clone();
        let mut got_sorted = got.clone();
        want.sort();
        got_sorted.sort();
        assert_eq!(
            got_sorted, want,
            "reopened tailer must deliver only the newly-appended records"
        );
    }

    #[test]
    fn tailer_crosses_segment_under_sharding() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 2;
        config.segment_size = 1 * 1024 * 1024; // 1MB → rolls quickly with big payloads
        config.ring_size = 8192;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);
        let db = Arc::new(LogDb::open(config).unwrap());

        // Spread big records across both shards so at least one shard rolls.
        let mut handles = Vec::new();
        for t in 0..2u64 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                let payload = vec![0xA5u8 + t as u8; 64 * 1024]; // 64KB each
                for _ in 0..20u64 {
                    db.append(&payload).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        db.flush().unwrap();
        for _ in 0..50 {
            if db.durable_cursor() >= 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let mut t = db.new_tailer("xseg");
        let mut count = 0usize;
        for _ in 0..300 {
            match t.next_batch(1000).unwrap() {
                Some(b) => count += b.len(),
                None => break,
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            count,
            2 * 20,
            "tailer must cross segment boundaries within each shard"
        );
    }

    // ── cr-003 Phase 6: hardening + production-ready docs ─────────────────

    #[test]
    fn wal_usage_under_sharding_sums_all_shard_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = Arc::new(LogDb::open(config).unwrap());

        let mut handles = Vec::new();
        for t in 0..4u64 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                for i in 0..10u64 {
                    db.append(format!("t{}-{}", t, i).as_bytes()).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        db.flush().unwrap();
        for _ in 0..50 {
            if db.durable_cursor() >= 10 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let (used, total) = db.wal_usage();
        assert!(total > 0, "segment_size total should be reported");
        assert!(
            used > 0,
            "wal_usage must sum segment files across ALL shard subdirs (got {})",
            used
        );
    }

    #[test]
    fn recovery_report_under_sharding_counts_across_shards() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.shards = 4;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        let db = Arc::new(LogDb::open(config).unwrap());

        let total = 40u64;
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let db = Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                for i in 0..10u64 {
                    db.append(format!("t{}-{}", t, i).as_bytes()).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        db.flush().unwrap();
        // flush() is the durability sync point; wait until every record is on disk.
        let scan_count = loop {
            let n = db.scan(0, u64::MAX).unwrap().filter_map(|r| r.ok()).count();
            if n >= total as usize {
                break n;
            }
            std::thread::sleep(Duration::from_millis(20));
        };

        // checkpoint defaults to 0 → every durable record (gid >= 0) counts.
        // count must equal the full-scan ground truth (sum across ALL shards),
        // not the min per-shard durable cursor.
        let report = db.recovery_report();
        assert_eq!(report.from_sequence, 0);
        assert_eq!(
            report.count as usize, scan_count,
            "recovery_report.count must equal total durable records across shards (got {}, scan={})",
            report.count, scan_count
        );
    }
}

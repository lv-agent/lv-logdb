//! Consumer tracking — named tailers with independent persisted progress.
//!
//! Each tailer maintains its own read position, backed by a progress file
//! (`tailer_<name>.dat`). Multiple tailers can independently read the same
//! log at different speeds without interfering with each other.
//!
//! # Sharding (`shards > 1`)
//!
//! A tailer tracks **per-shard** progress (one local sequence per shard,
//! like Kafka per-partition offsets) and `next_batch` merges every shard's
//! newly-durable records into one batch ordered by ascending global id.
//!
//! Guarantees:
//! - **Lossless**: each shard is drained against its own durable cursor; a
//!   record read but truncated out of a batch is re-read next time.
//! - **Intra-batch ascending** global-id order.
//! - **Cross-batch ordering is best-effort**: when a shard stalls, its
//!   lower-global-id records may arrive in a *later* batch than a higher-id
//!   record from another shard. A stalled shard never blocks the others.
//!   Use [`positions`](Tailer::positions) for exact per-shard progress.
//!
//! `shards == 1` collapses to a single-element progress vector and is
//! byte-for-byte equivalent to the legacy single-shard tailer.

use crate::KeyHandle;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::TailerError;
use crate::reader::{ScanIter, SegmentManifest};
use crate::record::Record;
use crate::ring::Ring;
use crate::shard::{decode_record_id, encode_record_id};

// ── Progress persistence ───────────────────────────────────────────────────
//
// `num_shards == 1` → legacy 12-byte format (u64 seq + crc32c); unchanged.
// `num_shards  > 1` → vec format: [u32 count_le][count × u64 seq_le][crc32c].

const LEGACY_PROGRESS_SIZE: usize = 12;

fn tailer_progress_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("tailer_{}.dat", name))
}

/// Load per-shard positions. `num_shards == 1` reads the legacy 12-byte file;
/// `num_shards > 1` reads the vec-format file. Missing/corrupt → all zeros.
fn load_progress(dir: &Path, name: &str, num_shards: usize) -> Vec<u64> {
    let path = tailer_progress_path(dir, name);
    let data = match fs::read(&path) {
        Ok(d) => d,
        Err(_) => return vec![0u64; num_shards],
    };
    if num_shards == 1 {
        if data.len() != LEGACY_PROGRESS_SIZE {
            return vec![0u64; num_shards];
        }
        let seq = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let crc = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
        if crc32c::crc32c(&data[..8]) != crc {
            return vec![0u64; num_shards];
        }
        return vec![seq];
    }
    // Vec format: u32 count + count*u64 seq + crc32c.
    if data.len() < 4 {
        return vec![0u64; num_shards];
    }
    let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let need = 4 + count * 8 + 4;
    if data.len() != need || count != num_shards {
        return vec![0u64; num_shards];
    }
    let body_end = 4 + count * 8;
    let crc = u32::from_le_bytes([
        data[body_end],
        data[body_end + 1],
        data[body_end + 2],
        data[body_end + 3],
    ]);
    if crc32c::crc32c(&data[..body_end]) != crc {
        return vec![0u64; num_shards];
    }
    let mut seqs = Vec::with_capacity(count);
    for i in 0..count {
        let off = 4 + i * 8;
        seqs.push(u64::from_le_bytes([
            data[off],
            data[off + 1],
            data[off + 2],
            data[off + 3],
            data[off + 4],
            data[off + 5],
            data[off + 6],
            data[off + 7],
        ]));
    }
    seqs
}

/// Persist per-shard positions. Format is chosen by `positions.len()`.
fn save_progress(dir: &Path, name: &str, positions: &[u64]) -> io::Result<()> {
    let path = tailer_progress_path(dir, name);
    let tmp = dir.join(format!(".tailer_{}.tmp", name));
    let mut f = fs::File::create(&tmp)?;
    if positions.len() == 1 {
        // Legacy 12-byte format (zero-regression for shards == 1).
        let mut buf = [0u8; LEGACY_PROGRESS_SIZE];
        buf[0..8].copy_from_slice(&positions[0].to_le_bytes());
        let crc = crc32c::crc32c(&buf[..8]);
        buf[8..12].copy_from_slice(&crc.to_le_bytes());
        f.write_all(&buf)?;
    } else {
        let count = positions.len() as u32;
        let mut body = Vec::with_capacity(4 + positions.len() * 8);
        body.extend_from_slice(&count.to_le_bytes());
        for s in positions {
            body.extend_from_slice(&s.to_le_bytes());
        }
        let crc = crc32c::crc32c(&body);
        f.write_all(&body)?;
        f.write_all(&crc.to_le_bytes())?;
    }
    crate::platform::fdatasync(&f)?;
    drop(f);
    fs::rename(&tmp, &path)?;
    let d = fs::File::open(dir)?;
    crate::platform::sync_dir(&d)?;
    Ok(())
}

// ── Tailer ─────────────────────────────────────────────────────────────────

/// A named consumer with independent read progress.
///
/// Multiple tailers can coexist — each has its own progress file and reads
/// at its own pace. Progress is persisted to disk on explicit `commit()`.
///
/// Under `shards > 1`, progress is tracked per shard (see the module docs for
/// the ordering/durability guarantees).
///
/// ```rust,no_run
/// # use logdb::{Config, LogDb};
/// # let mut config = Config::default();
/// # config.data_dir = std::path::PathBuf::from("/tmp/logdb-tailer-example");
/// # let db = LogDb::open(config).unwrap();
/// let mut t = db.new_tailer("replicator"); // returns Tailer, not a Result
/// while let Some(batch) = t.next_batch(1000).unwrap() {
///     // ...deliver `batch` to the downstream consumer...
///     t.commit().unwrap(); // persist progress
/// }
/// ```
pub struct Tailer {
    name: String,
    /// Per-shard delivered position (local seq). len == num_shards.
    positions: Vec<u64>,
    /// Top-level data dir; the (single) progress file lives here.
    data_dir: PathBuf,
    /// One manifest per shard.
    manifests: Vec<Arc<Mutex<SegmentManifest>>>,
    /// One ring per shard (for reading each shard's durable cursor).
    rings: Vec<Arc<Ring>>,
    shard_bits: u32,
    encryption_key: Option<KeyHandle>,
}

impl Tailer {
    /// Open or create a named tailer. Restores per-shard progress from disk.
    pub(crate) fn open(
        manifests: Vec<Arc<Mutex<SegmentManifest>>>,
        rings: Vec<Arc<Ring>>,
        shard_bits: u32,
        name: &str,
        encryption_key: Option<KeyHandle>,
        data_dir: PathBuf,
    ) -> Self {
        let num_shards = manifests.len();
        let positions = load_progress(&data_dir, name, num_shards);
        Self {
            name: name.to_string(),
            positions,
            data_dir,
            manifests,
            rings,
            shard_bits,
            encryption_key,
        }
    }

    /// Minimum per-shard position (the slowest shard's delivered local count),
    /// as a coarse global-progress indicator. For the exact per-shard vector
    /// use [`positions`](Tailer::positions).
    pub fn position(&self) -> u64 {
        self.positions.iter().copied().min().unwrap_or(0)
    }

    /// Per-shard delivered positions (local sequence per shard).
    pub fn positions(&self) -> &[u64] {
        &self.positions
    }

    /// Fast-forward every shard's position to `seq` (local sequence).
    pub fn seek(&mut self, seq: u64) {
        for p in self.positions.iter_mut() {
            *p = seq;
        }
    }

    /// Read the next batch of records from the current per-shard positions.
    ///
    /// Each shard is drained against its own durable cursor; the per-shard
    /// streams are merged by ascending global id and truncated to `max_count`.
    /// Returns `Ok(None)` when no shard has new durable records.
    pub fn next_batch(&mut self, max_count: usize) -> Result<Option<Vec<Record>>, TailerError> {
        let mut all: Vec<Record> = Vec::new();
        for s in 0..self.manifests.len() {
            let durable = self.rings[s]
                .durable_cursor
                .load(std::sync::atomic::Ordering::Acquire);
            let from_local = self.positions[s];
            if from_local >= durable {
                continue; // nothing new on this shard (or ahead — skip safely)
            }
            // Cap the per-shard read at max_count: the merged output is at most
            // max_count, so no shard can contribute more. Extras are re-read
            // next batch (lossless; memory-optimal merge deferred to cr-004).
            let to_local = (from_local + max_count as u64).min(durable);
            let from_gid = encode_record_id(s, from_local, self.shard_bits);
            let to_gid = encode_record_id(s, to_local, self.shard_bits);
            let iter = ScanIter::build(
                vec![Arc::clone(&self.manifests[s])],
                self.encryption_key.clone(),
                from_gid,
                to_gid,
            )?;
            for r in iter {
                match r {
                    Ok(rec) => all.push(rec),
                    Err(e) => return Err(e.into()),
                }
            }
        }
        if all.is_empty() {
            return Ok(None);
        }
        // Merge across shards by ascending global id, then truncate.
        all.sort_by_key(|r| r.id.sequence);
        if all.len() > max_count {
            all.truncate(max_count);
        }
        // Advance each shard's position past its last delivered record.
        for r in &all {
            let (shard, local) = decode_record_id(r.id.sequence, self.shard_bits);
            let next = local + 1;
            if next > self.positions[shard] {
                self.positions[shard] = next;
            }
        }
        Ok(Some(all))
    }

    /// Persist the current per-shard positions to disk.
    pub fn commit(&self) -> io::Result<()> {
        save_progress(&self.data_dir, &self.name, &self.positions)
    }

    /// Delete this tailer's progress file (reset every shard to the beginning).
    pub fn reset(&mut self) -> io::Result<()> {
        for p in self.positions.iter_mut() {
            *p = 0;
        }
        let path = tailer_progress_path(&self.data_dir, &self.name);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{Config, DurabilityMode};
    use crate::LogDb;
    use std::time::Duration;

    fn open_db(dir: &std::path::Path) -> LogDb {
        let mut config = Config::default();
        config.data_dir = dir.to_path_buf();
        config.ring_size = 128;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);
        LogDb::open(config).unwrap()
    }

    fn wait_durable(db: &LogDb, target: u64) {
        for _ in 0..50 {
            if db.durable_cursor() >= target {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn tailer_reads_from_beginning() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        for i in 0..100u64 {
            db.append(format!("msg-{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        wait_durable(&db, 100);

        let mut t = db.new_tailer("test");
        assert_eq!(t.position(), 0);
        let batch = t.next_batch(5).unwrap().unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(batch[0].content, b"msg-0");
        assert!(t.position() >= 5);
    }

    #[test]
    fn tailer_persists_progress() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        for i in 0..200u64 {
            db.append(format!("m{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        wait_durable(&db, 200);

        {
            let mut t = db.new_tailer("consumer-a");
            t.next_batch(10).unwrap();
            t.commit().unwrap();
            assert_eq!(t.position(), 10);
        }
        // Reopen (same name) — should resume from 10.
        let t2 = db.new_tailer("consumer-a");
        assert_eq!(t2.position(), 10);
    }

    #[test]
    fn multiple_tailers_independent() {
        let dir = tempfile::tempdir().unwrap();
        let db = open_db(dir.path());
        for i in 0..200u64 {
            db.append(format!("m{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        wait_durable(&db, 200);

        let mut a = db.new_tailer("a");
        let mut b = db.new_tailer("b");
        a.next_batch(5).unwrap();
        b.next_batch(15).unwrap();
        assert_eq!(a.position(), 5);
        assert_eq!(b.position(), 15);
        a.next_batch(10).unwrap();
        assert_eq!(a.position(), 15);
    }
}

//! Consumer tracking — named tailers with independent persisted progress.
//!
//! Each tailer maintains its own read position, backed by a progress file
//! (`tailer_<name>.dat`). Multiple tailers can independently read the same
//! log at different speeds without interfering with each other.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::record::Record;
use crate::ring::Ring;
use crate::reader::Reader;
use std::sync::Arc;

// ── Progress file (same format as pusher_progress.dat) ─────────────────────

const PROGRESS_SIZE: usize = 12;

fn tailer_progress_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("tailer_{}.dat", name))
}

fn load_progress(dir: &Path, name: &str) -> u64 {
    let path = tailer_progress_path(dir, name);
    match fs::read(&path) {
        Ok(data) if data.len() == PROGRESS_SIZE => {
            let seq = u64::from_le_bytes([data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]]);
            let crc = u32::from_le_bytes([data[8],data[9],data[10],data[11]]);
            if crc32c::crc32c(&data[..8]) == crc { seq } else { 0 }
        }
        _ => 0,
    }
}

fn save_progress(dir: &Path, name: &str, seq: u64) -> io::Result<()> {
    let path = tailer_progress_path(dir, name);
    let tmp = dir.join(format!(".tailer_{}.tmp", name));
    let mut buf = [0u8; PROGRESS_SIZE];
    buf[0..8].copy_from_slice(&seq.to_le_bytes());
    let crc = crc32c::crc32c(&buf[..8]);
    buf[8..12].copy_from_slice(&crc.to_le_bytes());
    let mut f = fs::File::create(&tmp)?;
    f.write_all(&buf)?;
    crate::platform::fdatasync(&f)?;
    drop(f);
    fs::rename(&tmp, &path)?;
    let d = fs::File::open(dir)?;
    crate::platform::sync_dir(&d)?;
    Ok(())
}

// ── Tailer ─────────────────────────────────────────────────────────────────

/// A named consumer with independent read position.
///
/// Multiple tailers can coexist — each has its own progress file and reads
/// at its own pace. Progress is persisted to disk on explicit `commit()`.
///
/// ```rust,no_run
/// let mut t = db.new_tailer("replicator")?;
/// while let Some(batch) = t.next_batch(1000)? {
///     send_to_replica(&batch);
///     t.commit()?;
/// }
/// ```
pub struct Tailer {
    name: String,
    position: u64,
    data_dir: PathBuf,
    manifest: Arc<std::sync::Mutex<crate::reader::SegmentManifest>>,
    ring: Arc<Ring>,
    encryption_key: Option<[u8; 32]>,
}

impl Tailer {
    /// Open or create a named tailer. Restores progress from disk.
    pub fn open(
        manifest: Arc<std::sync::Mutex<crate::reader::SegmentManifest>>,
        ring: Arc<Ring>,
        name: &str,
        encryption_key: Option<[u8; 32]>,
    ) -> Self {
        let data_dir = manifest.lock().unwrap().data_dir().to_path_buf();
        let position = load_progress(&data_dir, name);
        Self {
            name: name.to_string(),
            position,
            data_dir,
            manifest,
            ring,
            encryption_key,
        }
    }

    /// Current read position (next sequence to read).
    pub fn position(&self) -> u64 { self.position }

    /// Seek to a specific sequence. Useful for replay from a known point.
    pub fn seek(&mut self, seq: u64) { self.position = seq; }

    /// Read the next batch of records from the current position.
    /// Only reads durable (fsynced) records.
    /// Returns Ok(None) if no new records are available.
    pub fn next_batch(&mut self, max_count: usize) -> Result<Option<Vec<Record>>, String> {
        let durable = self.ring.durable_cursor.load(std::sync::atomic::Ordering::Acquire);
        if self.position >= durable {
            return Ok(None);
        }
        let to = (self.position + max_count as u64).min(durable);
        let reader = Reader::new(Arc::clone(&self.manifest), self.encryption_key);
        let iter = reader.scan(self.position, to).map_err(|e| format!("{:?}", e))?;
        let mut records = Vec::with_capacity((to - self.position) as usize);
        for r in iter {
            match r {
                Ok(rec) => records.push(rec),
                Err(e) => return Err(format!("{:?}", e)),
            }
        }
        if records.is_empty() {
            return Ok(None);
        }
        let last = records.last().unwrap();
        self.position = last.id.sequence + 1;
        Ok(Some(records))
    }

    /// Persist the current position to disk.
    pub fn commit(&self) -> io::Result<()> {
        save_progress(&self.data_dir, &self.name, self.position)
    }

    /// Delete this tailer's progress file (reset to beginning).
    pub fn reset(&mut self) -> io::Result<()> {
        self.position = 0;
        let path = tailer_progress_path(&self.data_dir, &self.name);
        if path.exists() { fs::remove_file(&path)?; }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DurabilityMode};
    use crate::LogDb;
    use std::time::Duration;

    fn manifest_for(dir: PathBuf) -> Arc<std::sync::Mutex<crate::reader::SegmentManifest>> {
        Arc::new(std::sync::Mutex::new(crate::reader::SegmentManifest::new(dir)))
    }

    #[test]
    fn tailer_reads_from_beginning() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 128;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);

        let db = LogDb::open(config).unwrap();
        for i in 0..100u64 {
            db.append(format!("msg-{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        for _ in 0..10 { std::thread::sleep(Duration::from_millis(50)); if db.durable_cursor() >= 100 { break; } }

        let ring = db.inner_ring();
        let mut t = Tailer::open(manifest_for(dir.path().to_path_buf()), ring, "test", None);
        assert_eq!(t.position(), 0);

        let batch = t.next_batch(5).unwrap().unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(batch[0].content, b"msg-0");
        let after_first = t.position();
        assert!(after_first >= 5);

        // No more records (all consumed or none left)
        // position should be at durable_cursor after consuming all
        assert!(t.position() >= 5);
    }

    #[test]
    fn tailer_persists_progress() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.ring_size = 128;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();
        for i in 0..200u64 {
            db.append(format!("m{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        for _ in 0..10 { std::thread::sleep(Duration::from_millis(50)); if db.durable_cursor() >= 200 { break; } }

        let ring = db.inner_ring();
        let mut t = Tailer::open(manifest_for(data_dir.clone()), ring.clone(), "consumer-a", None);
        t.next_batch(10).unwrap();
        t.commit().unwrap();
        assert_eq!(t.position(), 10);

        // Reopen — should resume from 10
        let t2 = Tailer::open(manifest_for(data_dir.clone()), ring, "consumer-a", None);
        assert_eq!(t2.position(), 10);
    }

    #[test]
    fn multiple_tailers_independent() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.ring_size = 128;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();
        for i in 0..200u64 {
            db.append(format!("m{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        for _ in 0..10 { std::thread::sleep(Duration::from_millis(50)); if db.durable_cursor() >= 200 { break; } }

        let ring = db.inner_ring();
        let mut a = Tailer::open(manifest_for(data_dir.clone()), ring.clone(), "a", None);
        let mut b = Tailer::open(manifest_for(data_dir.clone()), ring.clone(), "b", None);

        a.next_batch(5).unwrap();
        b.next_batch(15).unwrap();
        assert_eq!(a.position(), 5);
        assert_eq!(b.position(), 15);
        a.next_batch(10).unwrap();
        assert_eq!(a.position(), 15);
    }
}

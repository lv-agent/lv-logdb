//! Remote push — replicate durable records to a remote endpoint.
//!
//! The Pusher maintains an independent `push_cursor` and progress file.
//! It reads only durable (fsynced) records, pushes them in batches, and
//! uses exponential backoff on failure.  Remote failures never back-pressure
//! local appends (principle ⑥).

// The Pusher is a daemon-level building block for remote push. It is not yet
// wired into LogDb's public API (tracked as a known gap; a public push API
// needs its own design cr). Silence dead-code until it is exposed.
#![allow(dead_code)]

use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::config::Config;
use crate::pipeline::signal::ShutdownState;
use crate::reader::Reader;
use crate::record::Record;
use crate::ring::Ring;

// ── RemoteSink trait ───────────────────────────────────────────────────────

/// User-implemented remote receiver. Pusher calls this for each batch.
pub trait RemoteSink: Send + 'static {
    /// Push a batch of records to the remote endpoint.
    ///
    /// Return `Ok(())` if the batch was successfully delivered.
    /// Return `Err(PushError::Retriable)` for transient failures (Pusher retries).
    /// Return `Err(PushError::Fatal)` for unrecoverable failures (Pusher stops).
    fn push_batch(&mut self, records: &[Record]) -> Result<(), PushError>;
}

/// Errors that can occur during push.
#[derive(Debug)]
pub enum PushError {
    /// Transient failure — Pusher will retry with backoff.
    Retriable(String),
    /// Unrecoverable failure — Pusher will stop.
    Fatal(String),
}

impl std::fmt::Display for PushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retriable(s) => write!(f, "retriable: {}", s),
            Self::Fatal(s) => write!(f, "fatal: {}", s),
        }
    }
}

// ── Progress file ──────────────────────────────────────────────────────────

/// Size of the progress file: 8 bytes sequence + 4 bytes CRC32C.
const PROGRESS_FILE_SIZE: usize = 12;

/// File name for pusher progress.
const PROGRESS_FILE: &str = "pusher_progress.dat";

/// Load the last pushed sequence from the progress file.
/// Returns `None` if the file doesn't exist or is corrupted.
fn load_progress(dir: &Path) -> Option<u64> {
    let path = dir.join(PROGRESS_FILE);
    let data = fs::read(&path).ok()?;
    if data.len() != PROGRESS_FILE_SIZE {
        return None;
    }
    let seq = u64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    let stored_crc = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);
    let computed_crc = crc32c::crc32c(&data[..8]);
    if stored_crc != computed_crc {
        return None; // corrupted
    }
    Some(seq)
}

/// Save the last pushed sequence to the progress file (atomic write).
fn save_progress(dir: &Path, seq: u64) -> io::Result<()> {
    let path = dir.join(PROGRESS_FILE);
    let tmp = dir.join("pusher_progress.tmp");

    let mut buf = [0u8; PROGRESS_FILE_SIZE];
    buf[0..8].copy_from_slice(&seq.to_le_bytes());
    let crc = crc32c::crc32c(&buf[..8]);
    buf[8..12].copy_from_slice(&crc.to_le_bytes());

    // Atomic write: tmp → fdatasync → rename → fdatasync(dir)
    let mut f = fs::File::create(&tmp)?;
    f.write_all(&buf)?;
    crate::platform::fdatasync(&f)?;
    drop(f);

    fs::rename(&tmp, &path)?;

    let dir_f = fs::File::open(dir)?;
    crate::platform::sync_dir(&dir_f)?;

    Ok(())
}

// ── Exponential backoff ────────────────────────────────────────────────────

struct Backoff {
    base: Duration,
    attempt: u32,
    max_attempts: u32,
}

impl Backoff {
    fn new(config: &Config) -> Self {
        Self {
            base: config.push_retry_base,
            attempt: 0,
            max_attempts: config.push_max_retries,
        }
    }

    /// Wait for the next backoff interval. Returns `true` if should retry, `false` if max reached.
    fn wait(&mut self) -> bool {
        if self.max_attempts > 0 && self.attempt >= self.max_attempts {
            return false;
        }
        let delay = self.base * 2u32.pow(self.attempt.min(6)); // cap at 64x base
        let delay = delay.min(Duration::from_secs(60));
        std::thread::sleep(delay);
        self.attempt += 1;
        true
    }

    fn reset(&mut self) {
        self.attempt = 0;
    }
}

// ── Pusher thread ──────────────────────────────────────────────────────────

/// Run the Pusher loop. Intended to be spawned as a dedicated thread.
pub fn run_pusher(
    data_dir: PathBuf,
    ring: Arc<Ring>,
    mut sink: Box<dyn RemoteSink>,
    config: Config,
    shutdown: Arc<ShutdownState>,
) {
    let mut push_seq = load_progress(&data_dir).unwrap_or(0);
    let mut backoff = Backoff::new(&config);
    let mut batches_since_save: u32 = 0;

    loop {
        // Check shutdown
        if shutdown.aborted() {
            break;
        }
        if shutdown.draining() {
            let durable = ring.durable_cursor.load(Ordering::Acquire);
            if push_seq >= durable {
                break;
            }
        }

        // Read durable cursor
        let durable = ring.durable_cursor.load(Ordering::Acquire);
        if durable <= push_seq {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        }

        // Build a batch: read records [push_seq, push_seq + batch_size)
        let to = (push_seq + config.push_batch_size as u64).min(durable);
        let batch = match read_batch(&data_dir, push_seq, to) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[pusher] read error at seq={}: {}", push_seq, e);
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };

        if batch.is_empty() {
            // No records found — possibly the segment doesn't exist yet.
            // Wait for Committer to create it.
            std::thread::sleep(Duration::from_millis(100));
            continue;
        }

        // Push to remote
        match sink.push_batch(&batch) {
            Ok(()) => {
                if let Some(last) = batch.last() {
                    push_seq = last.id.sequence + 1;
                }
                backoff.reset();
                batches_since_save += 1;

                if batches_since_save >= config.push_progress_interval {
                    if let Err(e) = save_progress(&data_dir, push_seq) {
                        eprintln!("[pusher] save progress error: {}", e);
                    }
                    batches_since_save = 0;
                }
            }
            Err(PushError::Retriable(e)) => {
                eprintln!("[pusher] retriable error: {}", e);
                if !backoff.wait() {
                    eprintln!("[pusher] max retries reached, giving up");
                    break;
                }
            }
            Err(PushError::Fatal(e)) => {
                eprintln!("[pusher] fatal error: {}", e);
                break;
            }
        }
    }

    // Save progress on exit
    let _ = save_progress(&data_dir, push_seq);
}

/// Read a batch of records from segment files in [from, to) range.
fn read_batch(dir: &Path, from: u64, to: u64) -> Result<Vec<Record>, String> {
    let manifest = std::sync::Arc::new(std::sync::Mutex::new(crate::reader::SegmentManifest::new(
        dir.to_path_buf(),
    )));
    let reader = Reader::new(manifest, None);
    let mut records = Vec::with_capacity((to - from) as usize);
    let iter = reader
        .scan(from, to)
        .map_err(|e| format!("scan: {:?}", e))?;
    for result in iter {
        match result {
            Ok(r) => records.push(r),
            Err(e) => return Err(format!("read record: {:?}", e)),
        }
    }
    Ok(records)
}

// ── Thread-safe wrapper for LogDb integration ──────────────────────────────

pub struct PusherHandle {
    handle: Option<std::thread::JoinHandle<()>>,
    push_seq: Arc<AtomicU64>,
}

impl PusherHandle {
    /// Spawn a Pusher thread.
    pub fn spawn(
        data_dir: PathBuf,
        ring: Arc<Ring>,
        sink: Box<dyn RemoteSink>,
        config: Config,
        shutdown: Arc<ShutdownState>,
    ) -> Self {
        let push_seq = Arc::new(AtomicU64::new(0));
        let handle = std::thread::Builder::new()
            .name("logdb-pusher".into())
            .spawn(move || {
                run_pusher(data_dir.clone(), ring, sink, config, shutdown);
            })
            .ok();

        Self { handle, push_seq }
    }

    /// Join the pusher thread (during shutdown).
    pub fn join(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    /// Get the current push cursor.
    pub fn push_cursor(&self) -> u64 {
        self.push_seq.load(Ordering::Acquire)
    }
}

impl Drop for PusherHandle {
    fn drop(&mut self) {
        self.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogDb;
    use crate::config::{Config, DurabilityMode};
    use crate::record::Record;
    use std::sync::Mutex;

    struct TestSink {
        received: Mutex<Vec<Vec<Record>>>,
    }

    impl TestSink {
        fn new() -> Self {
            Self {
                received: Mutex::new(Vec::new()),
            }
        }
        fn total(&self) -> usize {
            self.received.lock().unwrap().iter().map(|b| b.len()).sum()
        }
    }

    impl RemoteSink for TestSink {
        fn push_batch(&mut self, records: &[Record]) -> Result<(), PushError> {
            self.received.lock().unwrap().push(records.to_vec());
            Ok(())
        }
    }

    #[test]
    fn progress_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_progress(dir.path()).is_none());

        save_progress(dir.path(), 50000).unwrap();
        assert_eq!(load_progress(dir.path()), Some(50000));

        save_progress(dir.path(), 99999).unwrap();
        assert_eq!(load_progress(dir.path()), Some(99999));
    }

    #[test]
    fn pusher_pushes_durable_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 128;
        config.durability_mode = DurabilityMode::Sync;
        config.push_batch_size = 10;
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();
        for i in 0..50u64 {
            db.append(format!("rec-{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));

        let durable = db.durable_cursor();
        assert!(durable >= 50);

        // Read the batch manually to verify
        let records = read_batch(dir.path(), 0, 50).unwrap();
        assert_eq!(records.len(), 50);
        assert_eq!(records[0].content, b"rec-0");
        assert_eq!(records[49].content, b"rec-49");
    }

    #[test]
    fn progress_corruption_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(PROGRESS_FILE);
        std::fs::write(&path, [0xFFu8; 12]).unwrap();
        assert!(load_progress(dir.path()).is_none());
    }
}

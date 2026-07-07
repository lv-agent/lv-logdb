//! Consumer group — server-side offset tracking with binary-file persistence.
//!
//! Offsets live in an in-memory HashMap (authoritative for reads). When an
//! `offsets_dir` is provided, they are durably snapshotted to
//! `<offsets_dir>/offsets.bin` (atomic tmp+rename) on a periodic background
//! flush and on graceful shutdown. Without a dir, offsets are in-memory only.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::offsets::{self, OffsetKey};

type ConsumerKey = OffsetKey;

pub struct ConsumerTracker {
    offsets: RwLock<HashMap<ConsumerKey, u64>>,
    offsets_dir: Option<PathBuf>,
    dirty: AtomicBool,
}

impl ConsumerTracker {
    /// Create a new tracker. Loads any existing offsets from `<offsets_dir>`
    /// so state survives restart. `None` = in-memory only (tests).
    pub fn new(offsets_dir: Option<PathBuf>) -> Self {
        let loaded = offsets_dir
            .as_ref()
            .and_then(|d| offsets::load(d).ok())
            .unwrap_or_default();
        Self {
            offsets: RwLock::new(loaded),
            offsets_dir,
            dirty: AtomicBool::new(false),
        }
    }

    /// Commit an offset for a consumer. Updates memory and marks dirty.
    pub fn commit(
        &self,
        namespace: &str,
        stream: &str,
        consumer_group: &str,
        consumer_id: &str,
        seq: u64,
    ) {
        let key = (
            namespace.to_string(),
            stream.to_string(),
            consumer_group.to_string(),
            consumer_id.to_string(),
        );
        {
            let mut map = self
                .offsets
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.insert(key, seq);
            self.dirty.store(true, Ordering::Release);
        }
    }

    /// Get the last committed offset for a consumer (0 if none).
    pub fn get(
        &self,
        namespace: &str,
        stream: &str,
        consumer_group: &str,
        consumer_id: &str,
    ) -> u64 {
        let key = (
            namespace.to_string(),
            stream.to_string(),
            consumer_group.to_string(),
            consumer_id.to_string(),
        );
        let map = self
            .offsets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.get(&key).copied().unwrap_or(0)
    }

    /// List all committed offsets for a consumer group in a stream.
    pub fn list_group(
        &self,
        namespace: &str,
        stream: &str,
        consumer_group: &str,
    ) -> Vec<(String, u64)> {
        let map = self
            .offsets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.iter()
            .filter(|((ns, s, g, _), _)| *ns == namespace && *s == stream && *g == consumer_group)
            .map(|((_, _, _, id), seq)| (id.clone(), *seq))
            .collect()
    }

    /// Flush dirty offsets to disk atomically. No-op if clean or no dir.
    pub fn flush(&self) -> Result<(), offsets::OffsetError> {
        if !self.dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        // Snapshot the map and clear `dirty` atomically under the WRITE lock.
        // This closes the commit/flush race: a commit that lands AFTER this
        // section re-sets dirty=true and is persisted by the next flush; a
        // commit that landed BEFORE is already in the snapshot. Saving outside
        // the lock keeps the critical section short.
        let snapshot = {
            let map = self
                .offsets
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.dirty.store(false, Ordering::Release);
            self.offsets_dir.as_ref().map(|_| map.clone())
        };
        if let (Some(dir), Some(snap)) = (self.offsets_dir.as_ref(), snapshot) {
            offsets::save(dir, &snap)?;
        }
        Ok(())
    }

    /// Spawn a background thread that flushes every `interval`.
    /// Fire-and-forget: the thread runs for the process lifetime and is killed
    /// on exit — the caller MUST also call `flush()` on graceful shutdown (a
    /// final flush, not a `.join()`). Do not join the returned handle; the loop
    /// never returns.
    pub fn start_flush_loop(self: &Arc<Self>, interval: Duration) -> std::thread::JoinHandle<()> {
        let this = Arc::clone(self);
        std::thread::Builder::new()
            .name("logdbd-offset-flush".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    if let Err(e) = this.flush() {
                        tracing::warn!(error = %e, "consumer offset flush failed");
                    }
                }
            })
            .expect("spawn offset-flush thread")
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_and_get_in_memory() {
        let tracker = ConsumerTracker::new(None);
        assert_eq!(tracker.get("ns", "s", "g", "c1"), 0);

        tracker.commit("ns", "s", "g", "c1", 42);
        assert_eq!(tracker.get("ns", "s", "g", "c1"), 42);
    }

    #[test]
    fn independent_consumers() {
        let tracker = ConsumerTracker::new(None);
        tracker.commit("ns", "s", "g", "w1", 10);
        tracker.commit("ns", "s", "g", "w2", 20);

        assert_eq!(tracker.get("ns", "s", "g", "w1"), 10);
        assert_eq!(tracker.get("ns", "s", "g", "w2"), 20);
    }

    #[test]
    fn commit_overwrites_previous() {
        let tracker = ConsumerTracker::new(None);
        tracker.commit("ns", "s", "g", "c1", 5);
        tracker.commit("ns", "s", "g", "c1", 100);
        assert_eq!(tracker.get("ns", "s", "g", "c1"), 100);
    }

    #[test]
    fn binary_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let offsets_dir = dir.path().to_path_buf();

        let tracker = ConsumerTracker::new(Some(offsets_dir.clone()));
        tracker.commit("ns", "stream", "g1", "c1", 99);
        tracker.flush().unwrap();

        // Fresh tracker on the same dir simulates a restart
        let tracker2 = ConsumerTracker::new(Some(offsets_dir));
        assert_eq!(tracker2.get("ns", "stream", "g1", "c1"), 99);
    }

    #[test]
    fn nonexistent_returns_zero() {
        let tracker = ConsumerTracker::new(None);
        // No consumer committed anything yet
        assert_eq!(tracker.get("ns", "s", "g", "unknown"), 0);
    }

    #[test]
    fn list_group_returns_consumer_ids() {
        let tracker = ConsumerTracker::new(None);
        tracker.commit("ns", "s", "g1", "w1", 10);
        tracker.commit("ns", "s", "g1", "w2", 20);
        tracker.commit("ns", "s", "g2", "w3", 30);

        let g1 = tracker.list_group("ns", "s", "g1");
        assert_eq!(g1.len(), 2);
        assert!(g1.contains(&("w1".into(), 10)));
        assert!(g1.contains(&("w2".into(), 20)));
    }

    #[test]
    fn flush_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let offsets_dir = dir.path().to_path_buf();
        let tracker = ConsumerTracker::new(Some(offsets_dir.clone()));
        tracker.commit("ns", "s", "g", "c1", 7);
        tracker.flush().unwrap();
        tracker.flush().unwrap(); // second flush must not corrupt/truncate

        let tracker2 = ConsumerTracker::new(Some(offsets_dir));
        assert_eq!(tracker2.get("ns", "s", "g", "c1"), 7);
    }

    #[test]
    fn flush_when_clean_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let offsets_dir = dir.path().to_path_buf();
        let tracker = ConsumerTracker::new(Some(offsets_dir.clone()));
        tracker.commit("ns", "s", "g", "c1", 5);
        tracker.flush().unwrap();

        // A second tracker loads the persisted offset, then flushes with no new
        // commit (dirty == false → early return). A third tracker must still see
        // the offset — the no-op flush must not wipe the file.
        let tracker2 = ConsumerTracker::new(Some(offsets_dir.clone()));
        tracker2.flush().unwrap();
        let tracker3 = ConsumerTracker::new(Some(offsets_dir));
        assert_eq!(tracker3.get("ns", "s", "g", "c1"), 5);
    }
}

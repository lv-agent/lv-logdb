//! Snapshot management for per-stream SQLite cache files.
//!
//! Snapshot lifecycle:
//!   recover_or_create → active .db file (copied from newest snapshot or fresh)
//!   create_snapshot   → copy active .db → snap_{ts}.db
//!   cleanup_snapshots → delete expired snap_{ts}.db files

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Timestamp string for snapshot filenames.
fn timestamp() -> String {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", dur.as_secs())
}

/// Build a safe filename from namespace and stream for the active db.
fn safe_db_name(ns: &str, stream: &str) -> String {
    let safe = |s: &str| s.replace(['/', '\\'], "_");
    format!("{}.{}.db", safe(ns), safe(stream))
}

/// Active db path for a stream.
fn active_path(cache_dir: &Path, ns: &str, stream: &str) -> PathBuf {
    cache_dir.join(safe_db_name(ns, stream))
}

/// Snapshot path for a stream at a given timestamp.
fn snapshot_path(cache_dir: &Path, ns: &str, stream: &str, ts: &str) -> PathBuf {
    let safe = |s: &str| s.replace(['/', '\\'], "_");
    cache_dir.join(format!("{}.{}.snap_{}.db", safe(ns), safe(stream), ts))
}

/// List all snapshot files matching a given (ns, stream) prefix.
/// Returns sorted newest-first.
fn list_snapshots(cache_dir: &Path, ns: &str, stream: &str) -> Vec<(PathBuf, u64)> {
    let safe = |s: &str| s.replace(['/', '\\'], "_");
    let prefix = format!("{}.{}.snap_", safe(ns), safe(stream));

    let mut results = Vec::new();
    if let Ok(entries) = fs::read_dir(cache_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with(&prefix) && name_str.ends_with(".db") {
                let ts_part = name_str
                    .strip_prefix(&prefix)
                    .and_then(|s| s.strip_suffix(".db"))
                    .unwrap_or("0");
                let ts: u64 = ts_part.parse().unwrap_or(0);
                results.push((entry.path(), ts));
            }
        }
    }
    results.sort_by_key(|(_, ts)| std::cmp::Reverse(*ts)); // newest first
    results
}

/// Recover the newest snapshot for (ns, stream), or return the active db path.
/// If no active db and no snapshot exist, returns the path where a fresh db should be created.
pub fn recover_or_create(cache_dir: &Path, ns: &str, stream: &str) -> PathBuf {
    let active = active_path(cache_dir, ns, stream);
    if active.exists() {
        return active;
    }

    // Look for newest snapshot
    let snapshots = list_snapshots(cache_dir, ns, stream);
    if let Some((snap_path, _ts)) = snapshots.first() {
        tracing::info!(
            ns = ns,
            stream = stream,
            snapshot = %snap_path.display(),
            "recovering cache from snapshot"
        );
        if let Err(e) = fs::copy(snap_path, &active) {
            tracing::warn!(
                error = %e,
                "failed to copy snapshot, will create fresh cache"
            );
        } else {
            return active;
        }
    }

    tracing::info!(ns = ns, stream = stream, "creating fresh cache db");
    active
}

/// Create a snapshot of the active db for (ns, stream).
/// Returns the snapshot path if successful.
pub fn create_snapshot(cache_dir: &Path, ns: &str, stream: &str) -> Option<PathBuf> {
    let active = active_path(cache_dir, ns, stream);
    if !active.exists() {
        return None;
    }

    let ts = timestamp();
    let snap = snapshot_path(cache_dir, ns, stream, &ts);

    match fs::copy(&active, &snap) {
        Ok(_) => {
            tracing::info!(
                ns = ns,
                stream = stream,
                snapshot = %snap.display(),
                "created cache snapshot"
            );
            Some(snap)
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to create cache snapshot");
            None
        }
    }
}

/// Delete old snapshots, retaining at most `retain` newest per stream.
pub fn cleanup_snapshots(cache_dir: &Path, retain: usize) {
    // Group snapshots by (ns, stream) prefix
    let mut groups: HashMap<String, Vec<PathBuf>> = HashMap::new();

    if let Ok(entries) = fs::read_dir(cache_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.contains(".snap_") || !name_str.ends_with(".db") {
                continue;
            }
            // Split at ".snap_" to get the stream prefix
            if let Some(prefix) = name_str.split(".snap_").next() {
                groups
                    .entry(prefix.to_string())
                    .or_default()
                    .push(entry.path());
            }
        }
    }

    for (_prefix, mut snaps) in groups {
        // Sort by modification time, newest first (approximate)
        snaps.sort_by_key(|p| {
            fs::metadata(p)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0)
        });
        snaps.reverse();
        for path in snaps.iter().skip(retain) {
            if let Err(e) = fs::remove_file(path) {
                tracing::warn!(path = %path.display(), error = %e, "failed to clean up old snapshot");
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn touch_db(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, b"mock sqlite db").unwrap();
    }

    #[test]
    fn recover_or_create_returns_existing_active() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path();

        let active = cache_dir.join("my-app.stream1.db");
        touch_db(&active);

        let result = recover_or_create(cache_dir, "my-app", "stream1");
        assert_eq!(result, active);
    }

    #[test]
    fn recover_or_create_uses_newest_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path();

        // Create two snapshots
        let snap1 = cache_dir.join("my-app.stream1.snap_1000.db");
        let snap2 = cache_dir.join("my-app.stream1.snap_2000.db");
        touch_db(&snap1);
        touch_db(&snap2);

        // No active db — should recover from snap_2000 (newest)
        let result = recover_or_create(cache_dir, "my-app", "stream1");
        assert!(result.exists(), "recovered db must exist");
        assert_eq!(result, cache_dir.join("my-app.stream1.db"));
    }

    #[test]
    fn recover_or_create_fresh_when_nothing_exists() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path();

        let result = recover_or_create(cache_dir, "ns", "stream");
        assert_eq!(result, cache_dir.join("ns.stream.db"));
    }

    #[test]
    fn create_snapshot_returns_none_when_no_active() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path();

        let result = create_snapshot(cache_dir, "ns", "noexist");
        assert!(result.is_none());
    }

    #[test]
    fn create_snapshot_copies_active() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path();

        let active = cache_dir.join("ns.stream.db");
        touch_db(&active);

        let snap = create_snapshot(cache_dir, "ns", "stream");
        assert!(snap.is_some());
        assert!(snap.unwrap().exists());
        // Active still there
        assert!(active.exists());
    }

    #[test]
    fn cleanup_removes_excess_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path();

        // Create 5 snapshots for one stream
        for ts in [1000, 2000, 3000, 4000, 5000] {
            let snap = cache_dir.join(format!("ns.stream.snap_{}.db", ts));
            touch_db(&snap);
        }

        // Retain only 2
        cleanup_snapshots(cache_dir, 2);

        // Count surviving snapshots
        let remaining = fs::read_dir(cache_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".snap_"))
            .count();
        assert_eq!(remaining, 2, "should retain at most 2 snapshots");
    }

    #[test]
    fn cleanup_ignores_active_dbs() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path();

        let active = cache_dir.join("ns.stream.db");
        touch_db(&active);

        for ts in [1000, 2000] {
            let snap = cache_dir.join(format!("ns.stream.snap_{}.db", ts));
            touch_db(&snap);
        }

        cleanup_snapshots(cache_dir, 1);

        // Active db must NOT be deleted
        assert!(active.exists(), "active db must survive cleanup");

        // Only 1 snapshot must survive
        let remaining = fs::read_dir(cache_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".snap_"))
            .count();
        assert_eq!(remaining, 1);
    }
}

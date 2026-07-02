//! Snapshot management — placeholder. TODO: implement.

use std::path::{Path, PathBuf};

pub fn recover_or_create(
    _cache_dir: &Path,
    _ns: &str,
    _stream: &str,
) -> PathBuf {
    PathBuf::new()
}

pub fn create_snapshot(
    _cache_dir: &Path,
    _ns: &str,
    _stream: &str,
) -> Option<PathBuf> {
    None
}

pub fn cleanup_snapshots(_cache_dir: &Path, _retain: usize) {}

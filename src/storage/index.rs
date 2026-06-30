//! Sparse index for segment files.
//!
//! Each segment can have an associated `.idx` file containing a sparse index:
//! every N records (default 1024), we store (record_id, file_offset, timestamp_ns).
//! The index enables binary-search lookup within a segment followed by a short
//! sequential scan to the target record.
//!
//! The index is a derived/rebuildable artifact. If missing or corrupted, it can
//! be reconstructed by scanning the segment file.

use std::fs;
use std::path::{Path, PathBuf};

/// An entry in the sparse index.
#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    /// Record identifier.
    pub sequence: u64,
    /// File offset of this record within the segment.
    pub file_offset: u64,
    /// Timestamp in nanoseconds.
    pub timestamp_ns: u64,
}

impl IndexEntry {
    /// Serialized size: 8 + 8 + 8 = 24 bytes per entry.
    pub const SERIALIZED_SIZE: usize = 24;

    /// Serialize to a byte buffer.
    pub fn serialize(&self, buf: &mut [u8]) {
        buf[0..8].copy_from_slice(&self.sequence.to_le_bytes());
        buf[8..16].copy_from_slice(&self.file_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.timestamp_ns.to_le_bytes());
    }

    /// Deserialize from a byte buffer.
    pub fn deserialize(buf: &[u8]) -> Self {
        let record_id = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3],
            buf[4], buf[5], buf[6], buf[7],
        ]);
        let file_offset = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11],
            buf[12], buf[13], buf[14], buf[15],
        ]);
        let timestamp_ns = u64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19],
            buf[20], buf[21], buf[22], buf[23],
        ]);
        Self { sequence: record_id, file_offset, timestamp_ns }
    }
}

/// A sparse index over a single segment file.
#[derive(Debug, Clone)]
pub struct SparseIndex {
    /// Index entries, sorted by record_id.
    entries: Vec<IndexEntry>,
    /// Number of records between index entries.
    stride: u32,
}

impl SparseIndex {
    /// Default stride: index every 1024 records.
    pub const DEFAULT_STRIDE: u32 = 1024;

    /// Create an empty sparse index.
    pub fn new(stride: u32) -> Self {
        Self {
            entries: Vec::new(),
            stride,
        }
    }

    /// Add an entry to the index.
    pub fn add(&mut self, sequence: u64, file_offset: u64, timestamp_ns: u64) {
        self.entries.push(IndexEntry { sequence, file_offset, timestamp_ns });
    }

    /// Check if a record should be indexed based on the stride.
    pub fn should_index(&self, record_count: u64) -> bool {
        record_count % self.stride as u64 == 0
    }

    /// Find the nearest index entry at or before `record_id`.
    ///
    /// Returns `(IndexEntry, index_position)` where the entry is the anchor point
    /// for a forward sequential scan. Returns `None` if the index is empty or
    /// `record_id` comes before the first indexed record.
    pub fn find_anchor(&self, record_id: u64) -> Option<(IndexEntry, usize)> {
        if self.entries.is_empty() {
            return None;
        }
        // Binary search for the largest entry with record_id <= target
        let idx = match self.entries.binary_search_by(|e| e.sequence.cmp(&record_id)) {
            Ok(pos) => pos,        // exact match
            Err(pos) if pos > 0 => pos - 1, // insertion point, take previous
            _ => return None,       // target is before the first entry
        };
        Some((self.entries[idx], idx))
    }

    /// Find the nearest index entry at or after `timestamp_ns`.
    ///
    /// Returns the index position for time-based queries.
    /// Returns `0` if the timestamp is before all entries.
    pub fn find_by_time(&self, timestamp_ns: u64) -> usize {
        match self.entries.binary_search_by(|e| e.timestamp_ns.cmp(&timestamp_ns)) {
            Ok(pos) => pos,
            Err(pos) => pos.saturating_sub(1), // go one before to ensure coverage
        }
    }

    /// Number of entries in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the index stride.
    pub fn stride(&self) -> u32 {
        self.stride
    }

    /// Save the index to an `.idx` file.
    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let mut buf = Vec::with_capacity(self.entries.len() * IndexEntry::SERIALIZED_SIZE + 4);
        // Write stride as u32 LE
        buf.extend_from_slice(&self.stride.to_le_bytes());
        // Write entries
        for entry in &self.entries {
            let mut entry_buf = [0u8; IndexEntry::SERIALIZED_SIZE];
            entry.serialize(&mut entry_buf);
            buf.extend_from_slice(&entry_buf);
        }
        fs::write(path, &buf)?;
        Ok(())
    }

    /// Load the index from an `.idx` file.
    pub fn load(path: &Path) -> std::io::Result<Self> {
        let data = fs::read(path)?;
        if data.len() < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "index file too short",
            ));
        }
        let stride = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let entry_size = IndexEntry::SERIALIZED_SIZE;
        let num_entries = (data.len() - 4) / entry_size;
        let mut entries = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let start = 4 + i * entry_size;
            entries.push(IndexEntry::deserialize(&data[start..start + entry_size]));
        }
        Ok(Self { entries, stride })
    }

    /// Get the path for the index file corresponding to a segment file.
    pub fn index_path(segment_path: &Path) -> PathBuf {
        let stem = segment_path.file_stem().unwrap().to_str().unwrap();
        segment_path.with_file_name(format!("{}.idx", stem))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_entry_round_trip() {
        let entry = IndexEntry { sequence: 42, file_offset: 1024, timestamp_ns: 5000 };
        let mut buf = [0u8; IndexEntry::SERIALIZED_SIZE];
        entry.serialize(&mut buf);
        let decoded = IndexEntry::deserialize(&buf);
        assert_eq!(decoded.sequence, 42);
        assert_eq!(decoded.file_offset, 1024);
        assert_eq!(decoded.timestamp_ns, 5000);
    }

    #[test]
    fn find_anchor_exact_match() {
        let mut idx = SparseIndex::new(10);
        for i in 0..5 {
            idx.add(i * 10, i * 100, i * 1000);
        }
        let (anchor, pos) = idx.find_anchor(20).unwrap();
        assert_eq!(anchor.sequence, 20);
        assert_eq!(pos, 2);
    }

    #[test]
    fn find_anchor_between() {
        let mut idx = SparseIndex::new(10);
        idx.add(0, 0, 0);
        idx.add(10, 100, 1000);
        idx.add(20, 200, 2000);
        // anchor for record_id=15 should be entry 10
        let (anchor, _) = idx.find_anchor(15).unwrap();
        assert_eq!(anchor.sequence, 10);
    }

    #[test]
    fn find_anchor_before_first() {
        let mut idx = SparseIndex::new(10);
        idx.add(10, 100, 1000);
        // no anchor before the first entry
        assert!(idx.find_anchor(5).is_none());
    }

    #[test]
    fn save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let idx_path = dir.path().join("test.idx");

        let mut idx = SparseIndex::new(1024);
        idx.add(0, 128, 0);
        idx.add(1024, 256000, 5000);
        idx.save(&idx_path).unwrap();

        let loaded = SparseIndex::load(&idx_path).unwrap();
        assert_eq!(loaded.stride, 1024);
        assert_eq!(loaded.entries.len(), 2);
        assert_eq!(loaded.entries[0].sequence, 0);
        assert_eq!(loaded.entries[1].sequence, 1024);
    }

    #[test]
    fn index_path_from_segment() {
        let seg = Path::new("/data/segment-00000001.log");
        let idx = SparseIndex::index_path(seg);
        assert_eq!(idx, Path::new("/data/segment-00000001.idx"));
    }
}

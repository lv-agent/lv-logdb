//! Reader — query records by id, time range, or scan.
//!
//! All reads are bounded by `durable_cursor`: only fsynced data is visible to
//! readers. This guarantees that records read will survive a crash.
//!
//! # Lookup Algorithm
//!
//! 1. Find the segment containing the target `record_id` (by checking each
//!    segment's `[base_record_id, max_record_id]` range).
//! 2. Use the sparse index to find the nearest anchor entry at or before the
//!    target record_id.
//! 3. Open the segment file, seek to the anchor's file_offset, and sequentially
//!    scan forward to the target record_id.

pub mod iter;
pub mod scan;
pub use scan::ScanIter;

use std::fs::{self, File};
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::error::ReadError;
use crate::record::Record;
use crate::storage::format::{
    deserialize_record, read_frame_header, SegmentHeader, FRAME_HEADER_SIZE, MIN_RECORD_SIZE,
    SEGMENT_HEADER_SIZE,
};
use crate::storage::index::SparseIndex;

use iter::RecordIter;

/// Decompress a zstd frame into a buffer. Returns the decompressed data.
#[cfg(feature = "compression")]
fn decompress_frame(compressed: &[u8]) -> Result<Vec<u8>, String> {
    zstd::decode_all(compressed).map_err(|e| format!("zstd decode: {}", e))
}

#[cfg(not(feature = "compression"))]
fn decompress_frame(_compressed: &[u8]) -> Result<Vec<u8>, String> {
    Err("compression feature not enabled".into())
}

/// Decrypt AES-256-GCM encrypted frame data. Input is {nonce:12B | ciphertext}.
#[cfg(feature = "encryption")]
fn decrypt_frame_data(key: &[u8; 32], encrypted: &[u8]) -> Result<Vec<u8>, String> {
    let nonce_size = crate::storage::format::ENCRYPTION_NONCE_SIZE;
    if encrypted.len() < nonce_size {
        return Err("too short".into());
    }
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Key, Nonce};
    let key = Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(&encrypted[..nonce_size]);
    cipher
        .decrypt(nonce, &encrypted[nonce_size..])
        .map_err(|_| "decryption failed".into())
}

#[cfg(not(feature = "encryption"))]
fn decrypt_frame_data(_key: &[u8; 32], _encrypted: &[u8]) -> Result<Vec<u8>, String> {
    Err("encryption feature not enabled".into())
}

/// Decode an on-disk frame payload (the bytes after the 8-byte frame header)
/// back into the raw record bytes: decrypt (if encrypted) then decompress (if
/// compressed). Shared by the Reader, RecordIter, and crash recovery so all
/// three agree on the frame layout.
pub(crate) fn decode_frame_payload(
    payload: &[u8],
    compressed: bool,
    encrypted: bool,
    key: Option<&[u8; 32]>,
) -> Result<Vec<u8>, String> {
    let plain = if encrypted {
        let k = key.ok_or_else(|| "encrypted frame but no key provided".to_string())?;
        decrypt_frame_data(k, payload)?
    } else {
        payload.to_vec()
    };
    if compressed {
        decompress_frame(&plain)
    } else {
        Ok(plain)
    }
}

// ── Segment manifest (cached directory listing) ─────────────────────────────

#[derive(Clone)]
pub(crate) struct ManifestEntry {
    segment_id: u32,
    path: PathBuf,
    base_sequence: u64,
    flags: u8,
}

impl ManifestEntry {
    /// First global record_id in this segment (monotonic per shard).
    pub(crate) fn base_sequence(&self) -> u64 {
        self.base_sequence
    }
}

/// Cached, sorted segment listing for fast `record_id → segment` lookup.
///
/// Without it, every `read()` does a full `readdir` and reads every segment
/// header — O(N) per read. The manifest caches `(segment_id, path,
/// base_sequence, flags)` per segment and is refreshed only when the data
/// directory's mtime changes (a segment is added by a roll or removed by
/// retention). Appending records to the active segment does NOT change the
/// directory mtime, and `base_sequence`/`flags` are fixed at segment creation,
/// so the cache stays valid between rolls.
pub(crate) struct SegmentManifest {
    data_dir: PathBuf,
    entries: Vec<ManifestEntry>,
    dir_mtime: Option<SystemTime>,
}

impl SegmentManifest {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            entries: Vec::new(),
            dir_mtime: None,
        }
    }

    /// Re-scan the directory into `entries` iff its mtime changed (or on first
    /// call). On filesystems where mtime is unavailable, falls back to
    /// refreshing every call (correct, just slower).
    fn refresh_if_needed(&mut self) -> Result<(), ReadError> {
        let mtime = fs::metadata(&self.data_dir).and_then(|m| m.modified()).ok();
        if mtime == self.dir_mtime && !self.entries.is_empty() {
            return Ok(());
        }
        let mut entries = Vec::new();
        let dir = fs::read_dir(&self.data_dir)
            .map_err(|e| ReadError::Io(format!("read_dir {:?}: {}", &self.data_dir, e)))?;
        for entry in dir {
            let entry = entry.map_err(|e| ReadError::Io(format!("entry: {}", e)))?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("segment-") && name.ends_with(".log") {
                    if let Ok(id) = name[8..name.len() - 4].parse::<u32>() {
                        if let Some((base_sequence, flags)) = read_header_for_manifest(&path) {
                            entries.push(ManifestEntry {
                                segment_id: id,
                                path,
                                base_sequence,
                                flags,
                            });
                        }
                    }
                }
            }
        }
        entries.sort_by_key(|e| e.segment_id);
        self.entries = entries;
        self.dir_mtime = mtime;
        Ok(())
    }

    /// Find the segment containing `seq`: the entry with the largest
    /// `base_sequence <= seq`. Returns a clone so callers don't hold the lock
    /// during file I/O. `base_sequence` is monotonic with `segment_id`, so a
    /// partition_point (binary search) locates it in O(log N).
    pub(crate) fn find(&mut self, seq: u64) -> Result<Option<ManifestEntry>, ReadError> {
        self.refresh_if_needed()?;
        let idx = self.entries.partition_point(|e| e.base_sequence <= seq);
        Ok(if idx == 0 {
            None
        } else {
            Some(self.entries[idx - 1].clone())
        })
    }

    /// All segment entries from the one containing `seq` onward, in ascending
    /// `segment_id` order. Used by cross-segment scans: the caller iterates
    /// these and stops once `base_sequence >= to_id`. Refreshes the cache first.
    pub(crate) fn segments_from(&mut self, seq: u64) -> Result<Vec<ManifestEntry>, ReadError> {
        self.refresh_if_needed()?;
        let start = self
            .entries
            .partition_point(|e| e.base_sequence <= seq)
            .saturating_sub(1);
        Ok(self.entries[start..].to_vec())
    }
}

/// Read just `(base_sequence, flags)` from a segment header (for the manifest).
fn read_header_for_manifest(path: &Path) -> Option<(u64, u8)> {
    let mut file = File::open(path).ok()?;
    let mut buf = [0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut buf).ok()?;
    let header = SegmentHeader::deserialize(&buf).ok()?;
    Some((header.base_sequence, header.flags))
}

/// Build a single-segment `RecordIter` over `entry`, anchored for `from_id`.
/// Shared by `Reader::scan` (a point entry) and `ShardScanner` (cross-segment):
/// both construct a per-segment iterator identically from a manifest entry.
pub(crate) fn iter_for_segment(
    entry: &ManifestEntry,
    from_id: u64,
    to_id: u64,
    key: Option<[u8; 32]>,
) -> Result<iter::RecordIter, ReadError> {
    let path = entry.path.clone();
    let is_compressed = entry.flags & crate::storage::format::FLAG_COMPRESSED_ZSTD != 0;
    let is_encrypted = entry.flags & crate::storage::format::FLAG_ENCRYPTED_AES256GCM != 0;

    let file_size = fs::metadata(&path)
        .map_err(|e| ReadError::Io(format!("metadata: {}", e)))?
        .len();

    // Frame-based segments need frame-aligned anchors -> start at the header
    // and let RecordIter skip records below `from_id`. Raw segments can use
    // the sparse-index anchor.
    let start_offset = if is_compressed || is_encrypted {
        SEGMENT_HEADER_SIZE as u64
    } else {
        let idx_path = SparseIndex::index_path(&path);
        if idx_path.exists() {
            match SparseIndex::load(&idx_path) {
                Ok(idx) => match idx.find_anchor(from_id) {
                    Some((e, _)) => e.file_offset,
                    None => SEGMENT_HEADER_SIZE as u64,
                },
                Err(_) => SEGMENT_HEADER_SIZE as u64,
            }
        } else {
            SEGMENT_HEADER_SIZE as u64
        }
    };

    iter::RecordIter::new(
        path,
        start_offset,
        file_size,
        from_id,
        to_id,
        is_compressed,
        is_encrypted,
        key,
    )
}

/// A reader that queries records from segment files.
///
/// Uses a shared `SegmentManifest` (cached, directory-mtime invalidated) so
/// segment-finding is O(log N) and does NOT re-`readdir` or re-read every
/// segment header on each call (P2-1: read amplification).
pub struct Reader {
    manifest: Arc<Mutex<SegmentManifest>>,
    /// Encryption key, if the database was opened with one. Required to read
    /// encrypted frames; without it encrypted records are undecryptable.
    encryption_key: Option<[u8; 32]>,
}

impl Reader {
    /// Create a new reader sharing a segment manifest (cached dir listing).
    pub(crate) fn new(
        manifest: Arc<Mutex<SegmentManifest>>,
        encryption_key: Option<[u8; 32]>,
    ) -> Self {
        Self {
            manifest,
            encryption_key,
        }
    }

    /// Read a single record by its `record_id`.
    ///
    /// Returns `Ok(Some(Record))` if found, `Ok(None)` if the record does not
    /// exist, or `Err(ReadError)` on I/O or corruption errors.
    pub fn read(&self, record_id: u64) -> Result<Option<Record>, ReadError> {
        // O(log N) segment lookup via the cached manifest (no per-call readdir
        // or header storm).
        let entry = match self.manifest.lock().unwrap().find(record_id)? {
            Some(e) => e,
            None => return Ok(None),
        };
        let path = entry.path;
        let is_compressed = entry.flags & crate::storage::format::FLAG_COMPRESSED_ZSTD != 0;
        let is_encrypted = entry.flags & crate::storage::format::FLAG_ENCRYPTED_AES256GCM != 0;

        let file_size = fs::metadata(&path)
            .map_err(|e| ReadError::Io(format!("metadata: {}", e)))?
            .len();

        // Sparse-index anchor for raw segments; frame segments start at the header.
        let start_offset = if is_compressed || is_encrypted {
            SEGMENT_HEADER_SIZE as u64
        } else {
            let idx_path = SparseIndex::index_path(&path);
            if idx_path.exists() {
                match SparseIndex::load(&idx_path) {
                    Ok(idx) => match idx.find_anchor(record_id) {
                        Some((e, _)) => e.file_offset,
                        None => SEGMENT_HEADER_SIZE as u64,
                    },
                    Err(_) => SEGMENT_HEADER_SIZE as u64,
                }
            } else {
                SEGMENT_HEADER_SIZE as u64
            }
        };

        let mut file = File::open(&path).map_err(|e| ReadError::Io(format!("open: {}", e)))?;
        let mut offset = start_offset;

        if is_compressed || is_encrypted {
            // Frame-based segment: [frame_header(8)][payload], where
            // payload = encrypt?(compress?(raw_records)). Either flag
            // triggers the frame layout (P0-1: encrypted-only also frames).
            let key = self.encryption_key.as_ref();
            while offset < file_size {
                if offset + FRAME_HEADER_SIZE as u64 > file_size {
                    break;
                }
                let mut fh_buf = [0u8; FRAME_HEADER_SIZE];
                file.seek(std::io::SeekFrom::Start(offset))
                    .map_err(|e| ReadError::Io(format!("seek frame: {}", e)))?;
                file.read_exact(&mut fh_buf)
                    .map_err(|e| ReadError::Io(format!("read frame hdr: {}", e)))?;
                let (cl, dl) = read_frame_header(&fh_buf);
                let cl = cl as usize;
                let dl = dl as usize;
                if cl == 0 || dl == 0 || offset + FRAME_HEADER_SIZE as u64 + cl as u64 > file_size {
                    break;
                }
                let mut cdata = vec![0u8; cl];
                file.read_exact(&mut cdata)
                    .map_err(|e| ReadError::Io(format!("read frame data: {}", e)))?;
                let decoded = match decode_frame_payload(&cdata, is_compressed, is_encrypted, key) {
                    Ok(d) => d,
                    Err(_) => break,
                };
                let mut doff = 0usize;
                while doff + MIN_RECORD_SIZE <= dl && doff <= decoded.len() {
                    let total = u32::from_le_bytes([
                        decoded[doff],
                        decoded[doff + 1],
                        decoded[doff + 2],
                        decoded[doff + 3],
                    ]) as usize;
                    if total < MIN_RECORD_SIZE || doff + total > dl || doff + total > decoded.len()
                    {
                        break;
                    }
                    match deserialize_record(&decoded[doff..doff + total]) {
                        Ok((record, _)) => {
                            if record.id.sequence == record_id {
                                return Ok(Some(record));
                            }
                            if record.id.sequence > record_id {
                                break;
                            }
                            doff += total;
                        }
                        Err(_) => {
                            doff += total;
                        }
                    }
                }
                offset += FRAME_HEADER_SIZE as u64 + cl as u64;
            }
        } else {
            // Uncompressed, unencrypted: record-by-record scan.
            while offset < file_size {
                if offset + 4 > file_size {
                    break;
                }
                let mut len_buf = [0u8; 4];
                file.seek(std::io::SeekFrom::Start(offset))
                    .map_err(|e| ReadError::Io(format!("seek: {}", e)))?;
                file.read_exact(&mut len_buf)
                    .map_err(|e| ReadError::Io(format!("read len: {}", e)))?;
                let total = u32::from_le_bytes(len_buf) as usize;
                if total < MIN_RECORD_SIZE {
                    offset += 1;
                    continue;
                }
                if offset + total as u64 > file_size {
                    break;
                }
                let mut record_buf = vec![0u8; total];
                file.seek(std::io::SeekFrom::Start(offset))
                    .map_err(|e| ReadError::Io(format!("seek: {}", e)))?;
                file.read_exact(&mut record_buf)
                    .map_err(|e| ReadError::Io(format!("read record: {}", e)))?;
                match deserialize_record(&record_buf) {
                    Ok((record, _)) => {
                        if record.id.sequence == record_id {
                            return Ok(Some(record));
                        }
                        if record.id.sequence > record_id {
                            break;
                        }
                        offset += total as u64;
                    }
                    Err(_) => {
                        offset += total as u64;
                    }
                }
            }
        }

        Ok(None)
    }

    /// Scan records in the range `[from_id, to_id)` within the single segment
    /// containing `from_id`. (Cross-segment / cross-shard scans live on
    /// `LogDb::scan` -> `ScanIter`; this single-segment iterator is used
    /// directly by tailer/pusher.)
    pub fn scan(&self, from_id: u64, to_id: u64) -> Result<RecordIter, ReadError> {
        // Locate the starting segment via the cached manifest (O(log N)).
        let entry = match self.manifest.lock().unwrap().find(from_id)? {
            Some(e) => e,
            None => return Err(ReadError::NotFound(from_id)),
        };
        iter_for_segment(&entry, from_id, to_id, self.encryption_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{QueueFullPolicy, RetentionPolicy};
    use crate::ring::Ring;

    #[test]
    fn read_record_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let ring = Ring::new(64, false, 0);

        let mut mgr = crate::storage::SegmentManager::create(
            dir.path().to_path_buf(),
            10 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        // Write records with specific content
        for i in 0..5 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            let content = format!("record-{}", i);
            unsafe {
                ring.slot(seq)
                    .producer_write(seq, i * 100, content.as_bytes());
            }
            ring.slot(seq).publish(seq);
        }
        mgr.append_batch(&ring, 0, 4).unwrap();
        mgr.fdatasync().unwrap();
        drop(mgr);

        let reader = Reader::new(
            Arc::new(Mutex::new(SegmentManifest::new(dir.path().to_path_buf()))),
            None,
        );
        let record = reader.read(3).unwrap().unwrap();
        assert_eq!(record.id.sequence, 3);
        assert_eq!(record.content, b"record-3");
    }

    #[test]
    fn read_nonexistent_record() {
        let dir = tempfile::tempdir().unwrap();
        let ring = Ring::new(64, false, 0);

        let mut mgr = crate::storage::SegmentManager::create(
            dir.path().to_path_buf(),
            10 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        let seq = ring.claim(QueueFullPolicy::Block).unwrap();
        unsafe {
            ring.slot(seq).producer_write(seq, 0, b"only-one");
        }
        ring.slot(seq).publish(seq);
        mgr.append_batch(&ring, 0, 0).unwrap();
        mgr.fdatasync().unwrap();
        drop(mgr);

        let reader = Reader::new(
            Arc::new(Mutex::new(SegmentManifest::new(dir.path().to_path_buf()))),
            None,
        );
        assert!(reader.read(999).unwrap().is_none());
    }
}

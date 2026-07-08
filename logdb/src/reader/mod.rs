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

use crate::KeyRing;
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
///
/// Tries every key in the ring until one authenticates — AES-GCM is an AEAD, so
/// a wrong key deterministically fails the auth tag and the first success is
/// unambiguously correct. This is what lets the reader decrypt records written
/// under a prior key after a rotation, with no on-disk key id (cr-032).
#[cfg(feature = "encryption")]
fn decrypt_frame_data(keys: &KeyRing, encrypted: &[u8]) -> Result<Vec<u8>, String> {
    let nonce_size = crate::storage::format::ENCRYPTION_NONCE_SIZE;
    if encrypted.len() < nonce_size {
        return Err("too short".into());
    }
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Key, Nonce};
    let nonce = Nonce::from_slice(&encrypted[..nonce_size]);
    let ct = &encrypted[nonce_size..];
    for (_id, k) in &keys.decrypt_keys {
        // `k` is `&Arc<Zeroizing<[u8; 32]>>`; deref Arc → Zeroizing → [u8; 32].
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&***k));
        if let Ok(plain) = cipher.decrypt(nonce, ct) {
            return Ok(plain);
        }
    }
    Err("decryption failed".into())
}

#[cfg(not(feature = "encryption"))]
fn decrypt_frame_data(_keys: &KeyRing, _encrypted: &[u8]) -> Result<Vec<u8>, String> {
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
    key_ring: Option<&KeyRing>,
) -> Result<Vec<u8>, String> {
    let plain = if encrypted {
        let kr = key_ring.ok_or_else(|| "encrypted frame but no key provided".to_string())?;
        decrypt_frame_data(kr, payload)?
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
    /// refreshing every call (correct, just slower). Returns `true` if the
    /// cache was (re)populated this call, `false` if it was served unchanged.
    fn refresh_if_needed(&mut self) -> Result<bool, ReadError> {
        let mtime = fs::metadata(&self.data_dir).and_then(|m| m.modified()).ok();
        if mtime == self.dir_mtime && !self.entries.is_empty() {
            return Ok(false);
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
        Ok(true)
    }

    /// Cached lookup (no refresh): the entry with the largest
    /// `base_sequence <= seq`, or `None`. `base_sequence` is monotonic with
    /// `segment_id`, so a partition_point (binary search) locates it in O(log N).
    fn find_in_cache(&self, seq: u64) -> Option<ManifestEntry> {
        let idx = self.entries.partition_point(|e| e.base_sequence <= seq);
        if idx == 0 {
            None
        } else {
            Some(self.entries[idx - 1].clone())
        }
    }

    /// Force a rescan ignoring the mtime cache (the cache was found to be stale).
    pub(crate) fn force_refresh(&mut self) -> Result<(), ReadError> {
        self.dir_mtime = None;
        self.refresh_if_needed()?;
        Ok(())
    }

    /// Find the segment containing `seq`: the entry with the largest
    /// `base_sequence <= seq`. Returns a clone so callers don't hold the lock
    /// during file I/O.
    ///
    /// Guards against a stale cache: a segment may have been deleted (checkpoint
    /// truncation / retention) without the directory mtime changing yet
    /// (coarse-mtime filesystems, or propagation lag), in which case
    /// `refresh_if_needed` would skip and serve an entry pointing at a now-missing
    /// file. When we served from cache (not just rescanned) and the entry's file
    /// is gone, force a rescan and re-lookup. Freshly-scanned entries are trusted.
    pub(crate) fn find(&mut self, seq: u64) -> Result<Option<ManifestEntry>, ReadError> {
        let refreshed = self.refresh_if_needed()?;
        let entry = self.find_in_cache(seq);
        if !refreshed {
            if let Some(e) = &entry {
                if !e.path.exists() {
                    self.force_refresh()?;
                    return Ok(self.find_in_cache(seq));
                }
            }
        }
        Ok(entry)
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
    key: Option<Arc<KeyRing>>,
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
    encryption_keys: Option<Arc<KeyRing>>,
}

impl Reader {
    /// Create a new reader sharing a segment manifest (cached dir listing).
    pub(crate) fn new(
        manifest: Arc<Mutex<SegmentManifest>>,
        encryption_keys: Option<Arc<KeyRing>>,
    ) -> Self {
        Self {
            manifest,
            encryption_keys,
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
        self.read_from_open_file(
            &mut file,
            file_size,
            is_compressed,
            is_encrypted,
            start_offset,
            record_id,
        )
    }

    /// Read `record_id` from an already-open segment file, scanning forward from
    /// `start_offset`. Shared by [`read`](Reader::read) (one record) and
    /// [`read_batch`](Reader::read_batch) (many records from the same open file,
    /// amortizing the open + index load across the batch).
    fn read_from_open_file(
        &self,
        file: &mut File,
        file_size: u64,
        is_compressed: bool,
        is_encrypted: bool,
        start_offset: u64,
        record_id: u64,
    ) -> Result<Option<Record>, ReadError> {
        let mut offset = start_offset;

        if is_compressed || is_encrypted {
            // Frame-based segment: [frame_header(8)][payload], where
            // payload = encrypt?(compress?(raw_records)). Either flag
            // triggers the frame layout (P0-1: encrypted-only also frames).
            let key_ring = self.encryption_keys.as_deref();
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
                let decoded = match decode_frame_payload(&cdata, is_compressed, is_encrypted, key_ring) {
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

    /// Read many records by id in one call. Cheaper than N individual
    /// [`read`](Reader::read)s when several ids land in the same segment: the
    /// segment file is opened and its sparse index loaded **once per segment**,
    /// not once per record. Records not present (or beyond the durable cursor)
    /// yield `None` at their position. Result order matches `ids`.
    ///
    /// All `ids` must belong to this `Reader`'s shard (the public entry point
    /// [`LogDb::read_batch`](crate::LogDb::read_batch) routes per shard).
    pub fn read_batch(&self, ids: &[u64]) -> Result<Vec<Option<Record>>, ReadError> {
        let mut results: Vec<Option<Record>> = vec![None; ids.len()];
        if ids.is_empty() {
            return Ok(results);
        }
        // Resolve each id -> segment entry (manifest caches the listing; O(log N)).
        // Unresolved ids stay None.
        let mut resolved: Vec<(usize, ManifestEntry)> = Vec::with_capacity(ids.len());
        {
            let mut m = self.manifest.lock().unwrap();
            for (i, &id) in ids.iter().enumerate() {
                if let Some(entry) = m.find(id)? {
                    resolved.push((i, entry));
                }
            }
        }
        // Cluster ids in the same segment, then read each cluster with one open
        // file + one sparse-index load.
        resolved.sort_by_key(|(_, e)| e.path.clone());
        let mut start = 0;
        while start < resolved.len() {
            let path = resolved[start].1.path.clone();
            let mut end = start + 1;
            while end < resolved.len() && resolved[end].1.path == path {
                end += 1;
            }
            let entry = &resolved[start].1;
            let is_compressed = entry.flags & crate::storage::format::FLAG_COMPRESSED_ZSTD != 0;
            let is_encrypted = entry.flags & crate::storage::format::FLAG_ENCRYPTED_AES256GCM != 0;

            let file_size = match fs::metadata(&path) {
                Ok(m) => m.len(),
                Err(_) => {
                    start = end;
                    continue;
                }
            };
            let mut file = match File::open(&path) {
                Ok(f) => f,
                Err(_) => {
                    start = end;
                    continue;
                }
            };
            // Load this segment's index once (raw segments only).
            let index = if is_compressed || is_encrypted {
                None
            } else {
                let idx_path = SparseIndex::index_path(&path);
                if idx_path.exists() {
                    SparseIndex::load(&idx_path).ok()
                } else {
                    None
                }
            };
            // Targets in this segment, sorted ascending by id (records are
            // stored ascending within a shard, so a single forward pass merges).
            let mut group: Vec<(usize, u64)> = (start..end)
                .map(|i| (resolved[i].0, ids[resolved[i].0]))
                .collect();
            group.sort_by_key(|&(_, id)| id);

            if !is_compressed && !is_encrypted {
                // Single forward pass: each record is read exactly once.
                let start_offset = group
                    .first()
                    .and_then(|&(_, id)| {
                        index.as_ref().and_then(|idx| idx.find_anchor(id))
                    })
                    .map(|(e, _)| e.file_offset)
                    .unwrap_or(SEGMENT_HEADER_SIZE as u64);
                for (slot, record) in
                    Self::read_many_raw(&mut file, file_size, start_offset, &group)?
                {
                    results[slot] = Some(record);
                }
            } else {
                // Frame segments: per-id seek (frames must be decoded whole).
                for (slot, id) in group {
                    results[slot] = self.read_from_open_file(
                        &mut file,
                        file_size,
                        is_compressed,
                        is_encrypted,
                        SEGMENT_HEADER_SIZE as u64,
                        id,
                    )?;
                }
            }
            start = end;
        }
        Ok(results)
    }

    /// Scan a raw segment forward from `start_offset`, reading each record once,
    /// and return the ones whose id is in `targets` (sorted ascending by id).
    /// A single pass replaces N per-id anchor-seeks — far fewer reads when
    /// several targets cluster in one segment. Records are stored ascending
    /// within a shard, so this is a merge: skip records below the next target,
    /// emit on match, stop past the last target.
    fn read_many_raw(
        file: &mut File,
        file_size: u64,
        start_offset: u64,
        targets: &[(usize, u64)],
    ) -> Result<Vec<(usize, Record)>, ReadError> {
        let mut out = Vec::with_capacity(targets.len());
        if targets.is_empty() {
            return Ok(out);
        }
        let mut t = 0usize;
        let mut offset = start_offset;
        while offset < file_size && t < targets.len() {
            if offset + 4 > file_size {
                break;
            }
            let mut len_buf = [0u8; 4];
            file.seek(std::io::SeekFrom::Start(offset))
                .map_err(|e| ReadError::Io(format!("seek: {}", e)))?;
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let total = u32::from_le_bytes(len_buf) as usize;
            if total < MIN_RECORD_SIZE {
                offset += 1; // resync past a corrupt length
                continue;
            }
            if offset + total as u64 > file_size {
                break;
            }
            let mut record_buf = vec![0u8; total];
            file.seek(std::io::SeekFrom::Start(offset))
                .map_err(|e| ReadError::Io(format!("seek rec: {}", e)))?;
            file.read_exact(&mut record_buf)
                .map_err(|e| ReadError::Io(format!("read rec: {}", e)))?;
            offset += total as u64;
            let Ok((record, _)) = deserialize_record(&record_buf) else {
                continue;
            };
            let id = record.id.sequence;
            // Merge: drop targets below this id (absent — gap/deleted).
            while t < targets.len() && targets[t].1 < id {
                t += 1;
            }
            if t < targets.len() && targets[t].1 == id {
                out.push((targets[t].0, record));
                t += 1;
            }
            // targets[t].1 > id → this record isn't targeted; keep scanning.
        }
        Ok(out)
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
        iter_for_segment(&entry, from_id, to_id, self.encryption_keys.clone())
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

    #[test]
    fn find_rescans_when_cached_segment_deleted() {
        // Reproduces the stale-manifest race behind the flaky
        // `checkpoint_truncation` test. A segment listed in the cache is
        // deleted (checkpoint truncation / retention), but the directory mtime
        // reads as UNCHANGED (coarse-mtime filesystems, or propagation lag), so
        // `refresh_if_needed` would skip and serve the stale entry — the caller
        // then opens a now-missing file (transient read miss).
        //
        // `find()` must detect a stale entry (path gone) and force a rescan.
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
            ring.slot(seq).producer_write(seq, 0, b"rec-0");
        }
        ring.slot(seq).publish(seq);
        mgr.append_batch(&ring, 0, 0).unwrap();
        mgr.fdatasync().unwrap();
        drop(mgr);

        let mut manifest = SegmentManifest::new(dir.path().to_path_buf());
        // Prime the cache: first find() scans + records dir_mtime.
        let entry = manifest.find(0).unwrap().expect("segment should be cached");
        assert!(entry.path.exists(), "segment file exists before deletion");

        // Delete the segment (as truncation/retention would).
        std::fs::remove_file(&entry.path).unwrap();

        // Simulate a coarse-mtime filesystem: pretend the deletion did not
        // change the directory mtime, so refresh_if_needed skips the rescan.
        manifest.dir_mtime =
            std::fs::metadata(dir.path()).and_then(|m| m.modified()).ok();

        // Before the fix: find() returns the stale entry (path no longer
        // exists) -> the caller's open fails. After: it force-rescans and
        // reports the segment gone.
        let stale = manifest.find(0).unwrap();
        assert!(
            stale.is_none(),
            "find() must not return a stale entry for a deleted segment"
        );
    }
}

#[cfg(all(test, feature = "encryption"))]
mod keyring_tests {
    use super::decode_frame_payload;
    use crate::KeyRing;

    /// Encrypt a plaintext with `key` in the on-disk frame layout
    /// (`nonce:12B ‖ ciphertext`) so `decode_frame_payload` can consume it.
    fn encrypted_frame(key: [u8; 32], plaintext: &[u8]) -> Vec<u8> {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key));
        let mut nonce = [0u8; 12];
        getrandom::getrandom(&mut nonce).unwrap();
        let ct = cipher
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .unwrap();
        let mut out = Vec::with_capacity(12 + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        out
    }

    /// A frame encrypted with key B must be readable through a ring whose
    /// decrypt order lists a *wrong* key (A) before B — proving the reader
    /// tries every key until one authenticates (rotation support, no disk
    /// format change; cr-032).
    #[test]
    fn decrypt_tries_keys_until_one_authenticates() {
        let key_a = [0xAAu8; 32];
        let key_b = [0xBBu8; 32];
        // active=A, decrypt order [A, B]. Encrypt with B (a record written
        // under a now-superseded key): A is tried first and skipped, B wins.
        let ring = KeyRing::new(key_a, vec![key_b]);
        let plaintext = b"hello-rotation-payload";
        let frame = encrypted_frame(key_b, plaintext);
        let decoded = decode_frame_payload(&frame, false, true, Some(ring.as_ref())).unwrap();
        assert_eq!(decoded, plaintext);
    }

    /// A frame whose key has been retired (absent from the ring) is unreadable,
    /// surfacing the standard "decryption failed" error.
    #[test]
    fn decrypt_fails_when_key_retired() {
        let ring = KeyRing::single([0xAAu8; 32]); // only A
        let frame = encrypted_frame([0xBBu8; 32], b"retired"); // written with B
        let err = decode_frame_payload(&frame, false, true, Some(ring.as_ref())).unwrap_err();
        assert!(err.contains("decryption failed"), "got: {err}");
    }
}

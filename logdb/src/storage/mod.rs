//! Segment file management.
//!
//! Segments are append-only files on disk. Each segment contains a fixed 128-byte
//! header followed by a sequence of records. When a segment reaches `segment_size`,
//! the SegmentManager rolls to a new segment.
//!
//! # Ownership
//!
//! The SegmentManager is owned exclusively by the Committer thread — no locks
//! needed (it is `!Sync` by construction).

pub mod format;
pub mod index;

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use format::{
    record_size, write_frame_header, SegmentHeader, FLAG_COMPRESSED_ZSTD, FLAG_HASH_ENABLED,
    FRAME_HEADER_SIZE, HEADER_CRC_END, SEGMENT_HEADER_SIZE,
};

use crate::config::RetentionPolicy;
use crate::platform;
use crate::ring::Ring;

// ── Error type ─────────────────────────────────────────────────────────────

/// Errors that can occur during segment operations.
#[derive(Debug)]
pub enum SegmentError {
    /// The active segment is full; caller should roll() and retry.
    Full,
    /// An I/O error occurred.
    Io(io::Error),
    /// The maximum content size was exceeded.
    ContentTooLarge { size: usize, max: usize },
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "segment full"),
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::ContentTooLarge { size, max } => {
                write!(f, "content size {} exceeds maximum {}", size, max)
            }
        }
    }
}

impl std::error::Error for SegmentError {}

impl From<io::Error> for SegmentError {
    fn from(e: io::Error) -> Self {
        SegmentError::Io(e)
    }
}

// ── ActiveSegment ──────────────────────────────────────────────────────────

/// An open segment file being actively written to.
struct ActiveSegment {
    file: File,
    path: PathBuf,
    /// Current write offset within the file (after the header).
    offset: u64,
    /// Base record_id for this segment.
    base_sequence: u64,
    /// Min timestamp seen so far (updated during append_batch).
    min_ts: u64,
    /// Max timestamp seen so far.
    max_ts: u64,
    compressed: bool,
    decompressed_offset: u64,
    frame_number: u32,
}

impl ActiveSegment {
    /// Create a new segment file with the given header.
    fn create(
        dir: &Path,
        segment_id: u32,
        header: &SegmentHeader,
        last_hash: [u8; 32],
        compressed: bool,
    ) -> io::Result<Self> {
        let filename = format!("segment-{:08}.log", segment_id);
        let path = dir.join(&filename);

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)?;

        // Write the header
        let mut header_buf = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut header_buf, last_hash);
        file.write_all(&header_buf)?;

        Ok(Self {
            file,
            path,
            offset: SEGMENT_HEADER_SIZE as u64,
            base_sequence: header.base_sequence,
            min_ts: u64::MAX,
            max_ts: 0,
            compressed,
            decompressed_offset: 0,
            frame_number: 0,
        })
    }

    /// Open an existing segment file for reading and appending (used during recovery).
    fn open_existing(
        path: PathBuf,
        _segment_id: u32,
        offset: u64,
        header: &SegmentHeader,
    ) -> io::Result<Self> {
        let file = OpenOptions::new().write(true).read(true).open(&path)?;

        Ok(Self {
            file,
            path,
            offset,
            base_sequence: header.base_sequence,
            min_ts: header.min_timestamp_ns,
            max_ts: header.max_timestamp_ns,
            compressed: header.flags & FLAG_COMPRESSED_ZSTD != 0,
            decompressed_offset: 0,
            frame_number: 0,
        })
    }

    /// Write data at the current offset and advance.
    fn write_at(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        Ok(())
    }

    /// Read data at the given offset without disturbing the file cursor.
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    /// Write data at the current offset using pwrite (positional write).
    #[cfg(target_os = "linux")]
    fn pwrite_all(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;
        let fd = self.file.as_raw_fd();
        let mut written = 0;
        while written < data.len() {
            let n = unsafe {
                libc::pwrite(
                    fd,
                    data[written..].as_ptr() as *const libc::c_void,
                    data.len() - written,
                    (offset + written as u64) as i64,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            written += n as usize;
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn pwrite_all(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        // Fallback: seek + write
        // We need &mut self for this, so we use a workaround via raw fd
        use std::os::unix::io::AsRawFd;
        let fd = self.file.as_raw_fd();
        let mut written = 0;
        while written < data.len() {
            let n = unsafe {
                libc::pwrite(
                    fd,
                    data[written..].as_ptr() as *const libc::c_void,
                    data.len() - written,
                    (offset + written as u64) as i64,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            written += n as usize;
        }
        Ok(())
    }

    /// Fdatasync the segment file.
    fn fdatasync(&self) -> io::Result<()> {
        platform::fdatasync(&self.file)
    }

    /// Whether this segment has room for `additional` more bytes.
    fn has_room(&self, additional: u64, max_size: u64) -> bool {
        self.offset + additional <= max_size
    }

    /// Update the timestamp range with a new timestamp.
    fn update_ts_range(&mut self, ts: u64) {
        if ts < self.min_ts {
            self.min_ts = ts;
        }
        if ts > self.max_ts {
            self.max_ts = ts;
        }
    }

    /// Backfill the timestamp range in the segment header.
    ///
    /// Layout (see `format.rs`): `min_timestamp_ns` @ offset 56, `max_timestamp_ns`
    /// @ offset 64, and `header_crc` @ offset 72 covering bytes `[0, 72)`. The
    /// timestamp fields therefore live INSIDE the CRC range, so after updating
    /// them we MUST recompute and rewrite the header CRC — otherwise every
    /// rolled segment's header fails CRC validation on read/recovery.
    fn backfill_header_ts(&mut self) -> io::Result<()> {
        self.write_at(56, &self.min_ts.to_le_bytes())?;
        self.write_at(64, &self.max_ts.to_le_bytes())?;

        // Recompute header CRC over [0, HEADER_CRC_END) and store at [72, 76).
        let mut crc_buf = [0u8; HEADER_CRC_END];
        self.read_at(0, &mut crc_buf)?;
        let crc = crc32c::crc32c(&crc_buf);
        self.write_at(HEADER_CRC_END as u64, &crc.to_le_bytes())?;

        Ok(())
    }
}

// ── SegmentManager ─────────────────────────────────────────────────────────

/// Manages segment files: creation, appending, rolling, retention.
///
/// Owned exclusively by the Committer thread.
pub struct SegmentManager {
    dir: PathBuf,
    active: ActiveSegment,
    active_id: u32,
    segment_size: u64,
    last_hash: [u8; 32],
    hash_init: [u8; 32],
    hash_enabled: bool,
    compressed: bool,
    encryption_key: Option<[u8; 32]>,
    retention: RetentionPolicy,
    write_buf: Vec<u8>,
    /// Sparse index being built for the active segment (raw segments only —
    /// compressed/encrypted segments store records inside frames, so per-record
    /// file offsets aren't independently seekable). None for frame segments.
    active_index: Option<index::SparseIndex>,
    /// Number of records appended to the active segment so far (for stride).
    active_index_count: u64,
    /// Pre-allocated next segment (D1 fix: eliminates roll-time file creation).
    /// Created when active segment exceeds 80% utilization.
    prepared_segment: Option<(u32, ActiveSegment)>,
    /// This manager's shard id and shard-bit width (0/0 for shards==1, i.e.
    /// identity encoding). Used to write the correct GLOBAL base_sequence into
    /// pre-allocated segment headers (local seq would break point reads,
    /// checkpoint truncation, and recovery under shards>1).
    shard_id: usize,
    shard_bits: u32,
    /// Paths of old (rolled-over) segments awaiting a deferred fdatasync.
    ///
    /// We store **paths**, not open file handles: the rolled segment's `File`
    /// is closed at roll time (releasing the fd immediately), and the deferred
    /// fdatasync reopens by path when drained. This prevents unbounded fd
    /// accumulation in Async mode (where the Committer may never go idle enough
    /// to drain, so holding open fds would exhaust the process fd limit).
    pending_fsync: Vec<PathBuf>,
}

/// Encrypt data with AES-256-GCM if key is provided, otherwise pass through.
/// Returns `{nonce:12B | ciphertext}` when encrypted, or plaintext when no key.
fn encrypt_if_enabled(_key: &Option<[u8; 32]>, plaintext: &[u8]) -> Result<Vec<u8>, SegmentError> {
    #[cfg(feature = "encryption")]
    if let Some(k) = _key {
        use aes_gcm::aead::{Aead, KeyInit};
        use aes_gcm::{Aes256Gcm, Key, Nonce};
        use format::ENCRYPTION_NONCE_SIZE;
        let key = Key::<Aes256Gcm>::from_slice(k);
        let cipher = Aes256Gcm::new(key);
        let mut nonce_bytes = [0u8; ENCRYPTION_NONCE_SIZE];
        getrandom::getrandom(&mut nonce_bytes).map_err(|e| {
            SegmentError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{:?}", e),
            ))
        })?;
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ct = cipher.encrypt(nonce, plaintext).map_err(|_| {
            SegmentError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "encryption failed",
            ))
        })?;
        let mut result = Vec::with_capacity(ENCRYPTION_NONCE_SIZE + ct.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ct);
        return Ok(result);
    }
    Ok(plaintext.to_vec())
}

/// A fresh sparse index for a segment, unless it uses the frame layout
/// (compressed/encrypted) — frame segments store records inside opaque frames,
/// so per-record file offsets aren't independently seekable and an index is
/// useless (read falls back to scanning from the segment header).
fn fresh_index(compressed: bool, encryption_key: &Option<[u8; 32]>) -> Option<index::SparseIndex> {
    if compressed || encryption_key.is_some() {
        None
    } else {
        Some(index::SparseIndex::new(index::SparseIndex::DEFAULT_STRIDE))
    }
}

/// Reopen a segment file by path and fdatasync it. Used to drain the deferred
/// fsync of rolled-over segments: their fd was released at roll time (to avoid
/// unbounded fd usage), so we reopen transiently here to flush them.
fn fsync_path(path: &Path) -> io::Result<()> {
    let f = OpenOptions::new().write(true).open(path)?;
    platform::fdatasync(&f)
}

impl SegmentManager {
    /// Create a new SegmentManager for a fresh database.
    pub fn create(
        dir: PathBuf,
        segment_size: u64,
        hash_enabled: bool,
        compressed: bool,
        encryption_key: Option<[u8; 32]>,
        hash_init: [u8; 32],
        retention: RetentionPolicy,
        base_sequence: u64,
    ) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;

        let segment_id = 1;
        let mut flags = if hash_enabled {
            format::FLAG_HASH_ENABLED
        } else {
            0
        };
        if compressed {
            flags |= format::FLAG_COMPRESSED_ZSTD;
        }
        if encryption_key.is_some() {
            flags |= format::FLAG_ENCRYPTED_AES256GCM;
        }
        let mut header = SegmentHeader::first_segment(
            hash_init,
            base_sequence,
            0,
            segment_id,
            hash_enabled,
            format::HASH_ALGO_BLAKE3,
        );
        header.flags = flags;
        let active = ActiveSegment::create(&dir, segment_id, &header, [0u8; 32], compressed)?;

        // Sync directory to ensure the new segment file is durable
        let dir_file = File::open(&dir)?;
        platform::sync_dir(&dir_file)?;

        let active_index = fresh_index(compressed, &encryption_key);
        Ok(Self {
            dir,
            active,
            active_id: segment_id,
            segment_size,
            last_hash: [0u8; 32],
            hash_init,
            hash_enabled,
            compressed,
            encryption_key,
            retention,
            write_buf: Vec::with_capacity(1024 * 1024),
            active_index,
            active_index_count: 0,
            prepared_segment: None,
            pending_fsync: Vec::new(),
            shard_id: 0,
            shard_bits: 0,
        })
    }

    /// Open an existing SegmentManager during recovery.
    pub fn open_existing(
        dir: PathBuf,
        active_path: PathBuf,
        active_id: u32,
        active_offset: u64,
        header: &SegmentHeader,
        segment_size: u64,
        last_hash: [u8; 32],
        hash_init: [u8; 32],
        hash_enabled: bool,
        encryption_key: Option<[u8; 32]>,
        retention: RetentionPolicy,
    ) -> io::Result<Self> {
        let active = ActiveSegment::open_existing(active_path, active_id, active_offset, header)?;
        let compressed = active.compressed;
        let active_index = fresh_index(compressed, &encryption_key);

        Ok(Self {
            dir,
            active,
            active_id,
            segment_size,
            last_hash,
            hash_init,
            hash_enabled,
            compressed,
            encryption_key,
            retention,
            write_buf: Vec::with_capacity(1024 * 1024),
            active_index,
            active_index_count: 0,
            prepared_segment: None,
            pending_fsync: Vec::new(),
            shard_id: 0,
            shard_bits: 0,
        })
    }

    /// Append a batch of records from the ring buffer to the active segment.
    ///
    /// Reads records for sequence numbers `from..=to` from the ring and writes
    /// them to the active segment file using positional writes.
    ///
    /// Returns `Err(SegmentError::Full)` if the entire batch won't fit and no
    /// records have been written yet — the caller should `roll()` and retry.
    /// Partial writes (some records fit, some don't) are handled by stopping
    /// early; the caller advances committed_cursor to the last written seq.
    pub fn append_batch(&mut self, ring: &Ring, from: u64, to: u64) -> Result<u64, SegmentError> {
        let start_offset = self.active.offset;
        let mut pos = 0usize;
        let mut last_written = from.wrapping_sub(1);

        for seq in from..=to {
            // Safety: caller guarantees slots [from, to] are published.
            let view = unsafe { ring.slot(seq).read() };
            let need = record_size(view.content.len());

            // Check if this record fits
            if !self
                .active
                .has_room(pos as u64 + need as u64, self.segment_size)
            {
                if pos == 0 {
                    // Even the first record doesn't fit → roll needed.
                    return Err(SegmentError::Full);
                }
                // Stop here; caller will continue from `last_written + 1` next time.
                break;
            }

            // Ensure write_buf has capacity
            if self.write_buf.len() < pos + need {
                self.write_buf.resize(pos + need, 0);
            }

            // In single-shard mode, local_seq == global sequence.
            // For multi-shard, the Committer assigns the global sequence.
            // Sparse index (raw segments): record this record's file offset so
            // reads can seek near the target instead of scanning from the
            // segment header (P2-1b).
            if let Some(idx) = self.active_index.as_mut() {
                if idx.should_index(self.active_index_count) {
                    // Key by the GLOBAL record id (view.record_id), not the local
                    // ring seq: readers and recovery look up by global id, and under
                    // sharding local seq != global id. shards=1: the two are equal,
                    // so this is unchanged.
                    idx.add(view.record_id, start_offset + pos as u64, view.timestamp_ns);
                }
                self.active_index_count += 1;
            }
            pos += format::serialize_record(&mut self.write_buf[pos..], view.record_id, &view);
            self.active.update_ts_range(view.timestamp_ns);
            last_written = seq;
        }

        if pos > 0 {
            let raw = &self.write_buf[..pos];
            if self.active.compressed || self.encryption_key.is_some() {
                // Frame format: [frame_header(8)][payload], payload = encrypt?(compress?(raw)).
                // Triggered by EITHER compression or encryption so that recovery
                // and readers can always delimit and decode batches uniformly
                // (P0-1: encrypted-only segments must be framed too, otherwise
                // the on-disk nonce+ciphertext has no self-delimiting structure).
                let payload = if self.active.compressed {
                    #[cfg(feature = "compression")]
                    {
                        let compressed = zstd::encode_all(raw, 0).map_err(|e| {
                            SegmentError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                        })?;
                        encrypt_if_enabled(&self.encryption_key, &compressed)?
                    }
                    #[cfg(not(feature = "compression"))]
                    {
                        encrypt_if_enabled(&self.encryption_key, raw)?
                    }
                } else {
                    // Encrypted but not compressed.
                    encrypt_if_enabled(&self.encryption_key, raw)?
                };
                let mut frame_buf = [0u8; FRAME_HEADER_SIZE];
                write_frame_header(&mut frame_buf, payload.len() as u32, pos as u32);
                self.active.pwrite_all(start_offset, &frame_buf)?;
                self.active
                    .pwrite_all(start_offset + FRAME_HEADER_SIZE as u64, &payload)?;
                self.active.offset = start_offset + FRAME_HEADER_SIZE as u64 + payload.len() as u64;
                self.active.decompressed_offset += pos as u64;
                self.active.frame_number += 1;
            } else {
                // Raw records, no frame.
                self.active.pwrite_all(start_offset, raw)?;
                self.active.offset = start_offset + raw.len() as u64;
            }
        }

        // D1 fix: trigger pre-allocation when segment >80% full
        self.maybe_prepare_next(last_written.wrapping_add(1))?;

        Ok(last_written)
    }

    /// Pre-allocate the next segment when the active one exceeds 80% capacity.
    ///
    /// This eliminates file creation + header fsync from the roll() hot path.
    /// See design doc D1 decision.
    ///
    /// `next_local_seq` is the LOCAL ring sequence of the first record that will
    /// land in the new segment; it is encoded to a GLOBAL record id for the
    /// segment header `base_sequence` (point reads, truncation, and recovery all
    /// key off the global id; a local seq would silently break them under
    /// `shards > 1`). For `shards == 1` (`shard_bits == 0`) the encoding is the
    /// identity, so behavior is unchanged.
    fn maybe_prepare_next(&mut self, next_local_seq: u64) -> Result<(), SegmentError> {
        if self.prepared_segment.is_some() {
            return Ok(());
        }
        let utilization_pct = self.active.offset * 100 / self.segment_size;
        if utilization_pct < 80 {
            return Ok(());
        }

        let new_id = self.active_id + 1;
        let base_sequence =
            crate::shard::encode_record_id(self.shard_id, next_local_seq, self.shard_bits);
        let mut flags = if self.hash_enabled {
            FLAG_HASH_ENABLED
        } else {
            0
        } | format::FLAG_NOT_FIRST;
        if self.compressed {
            flags |= FLAG_COMPRESSED_ZSTD;
        }
        if self.encryption_key.is_some() {
            flags |= format::FLAG_ENCRYPTED_AES256GCM;
        }
        let new_header = SegmentHeader {
            format_version: format::FORMAT_VERSION,
            flags,
            hash_algo: format::HASH_ALGO_BLAKE3,
            hash_init: self.hash_init,
            base_sequence,
            partition_id: 0,
            segment_id: new_id,
            min_timestamp_ns: u64::MAX,
            max_timestamp_ns: 0,
            prev_last_hash: self.last_hash,
            record_format: format::RECORD_FORMAT_V1,
        };

        let seg = ActiveSegment::create(
            &self.dir,
            new_id,
            &new_header,
            self.last_hash,
            self.compressed,
        )?;
        // We do NOT fsync the pre-allocated header here. The prepared segment
        // only becomes authoritative when roll() swaps it to active, and its
        // header + data are fsynced together by the next `sync_all` (which
        // gates `durable_cursor`). So durable_cursor never advances past a
        // segment whose header isn't on stable storage, regardless of when the
        // header reaches disk. Fsyncing here would add a per-segment fdatasync
        // to the Committer path (hurting Async-mode throughput, and hanging on
        // WSL2) for no correctness benefit.
        self.prepared_segment = Some((new_id, seg));
        Ok(())
    }

    /// Get the buffer capacity hint for batch size estimation.
    pub fn buf_cap(&self) -> usize {
        self.write_buf.capacity()
    }

    /// Roll to a new segment.
    ///
    /// D1 + D1-async: pre-allocation eliminates file creation from the roll
    /// path. The old segment's fdatasync is offloaded to a background thread,
    /// making the swap effectively instant from the Committer's perspective.
    pub fn roll(&mut self, next_base_sequence: u64, checkpoint: u64) -> Result<(), SegmentError> {
        log_info!(shard = self.shard_id, from_segment = self.active_id, "rolling segment");
        metric_counter!("logdb.segment.rolls", 1);
        // Backfill timestamps in the old segment header
        self.active.backfill_header_ts()?;
        // Persist the (now-sealed) segment's sparse index before swapping it out.
        let _ = self.save_active_index();

        // Use pre-prepared segment if available
        match self.prepared_segment.take() { Some((new_id, new_seg)) => {
            // Swap to pre-prepared segment (instant)
            let old_active = std::mem::replace(&mut self.active, new_seg);
            self.active_id = new_id;

            // Queue old segment for async fsync — Committer drains during idle.
            // Queue the old segment's PATH (not the open handle) for deferred
            // fsync, then drop the old ActiveSegment to release its fd now.
            self.pending_fsync.push(old_active.path.clone());
            drop(old_active);
        } _ => {
            // Fallback: create new segment inline, fsync old in background.
            let new_id = self.active_id + 1;
            let mut flags = if self.hash_enabled {
                FLAG_HASH_ENABLED
            } else {
                0
            } | format::FLAG_NOT_FIRST;
            if self.compressed {
                flags |= FLAG_COMPRESSED_ZSTD;
            }
            if self.encryption_key.is_some() {
                flags |= format::FLAG_ENCRYPTED_AES256GCM;
            }
            let new_header = SegmentHeader {
                format_version: format::FORMAT_VERSION,
                flags,
                hash_algo: format::HASH_ALGO_BLAKE3,
                hash_init: self.hash_init,
                base_sequence: next_base_sequence,
                partition_id: 0,
                segment_id: new_id,
                min_timestamp_ns: u64::MAX,
                max_timestamp_ns: 0,
                prev_last_hash: self.last_hash,
                record_format: format::RECORD_FORMAT_V1,
            };

            let new_active = ActiveSegment::create(
                &self.dir,
                new_id,
                &new_header,
                self.last_hash,
                self.compressed,
            )?;
            // Fsync new header before swap (small, fast: 128 bytes)
            new_active.fdatasync()?;
            let dir_file = File::open(&self.dir)?;
            platform::sync_dir(&dir_file)?;

            let old_active = std::mem::replace(&mut self.active, new_active);
            self.active_id = new_id;

            // Queue the old segment's PATH (not the open handle) for deferred
            // fsync, then drop the old ActiveSegment to release its fd now.
            self.pending_fsync.push(old_active.path.clone());
            drop(old_active);
        }}

        // Start a fresh sparse index for the newly-active segment.
        self.active_index = fresh_index(self.compressed, &self.encryption_key);
        self.active_index_count = 0;

        // Apply retention
        self.apply_retention()?;

        // WAL checkpoint truncation: delete old segments entirely before checkpoint
        if checkpoint > 0 {
            self.truncate_before_checkpoint(checkpoint)?;
        }

        Ok(())
    }

    /// Drain pending fsyncs for old segments (non-blocking best-effort).
    /// Called by Committer during idle — no urgency, just cleanup.
    pub fn drain_pending_fsyncs(&mut self) {
        let pending: Vec<PathBuf> = std::mem::take(&mut self.pending_fsync);
        for path in pending {
            let _ = fsync_path(&path);
        }
    }

    /// Fsync everything needed for `durable_cursor` to advance truthfully.
    ///
    /// Drains `pending_fsync` (old, rolled-over segments whose tail data has
    /// been committed but not yet fsynced) BEFORE fsyncing the active segment.
    ///
    /// Why: after a roll, the old segment sits in `pending_fsync` un-fsynced.
    /// If the Committer only fsynced the active segment and then advanced
    /// `durable_cursor` to `committed_cursor`, durable would overstate what is
    /// on stable storage — records in the old segment's tail would be reported
    /// durable but lost on crash. This is the P0-5 fix: durable may only
    /// advance once every segment containing records up to that point is
    /// fsynced.
    pub fn sync_all(&mut self) -> io::Result<()> {
        // Old (rolled) segments first — their records are below the active
        // segment's range, so they must be on stable storage before durable
        // can claim anything in the active segment. Reopen by path (the fd was
        // released at roll time) and fdatasync.
        let pending: Vec<PathBuf> = std::mem::take(&mut self.pending_fsync);
        for path in pending {
            fsync_path(&path)?;
        }
        self.active.fdatasync()
    }

    /// Number of old segments awaiting fsync (diagnostics / tests).
    #[doc(hidden)]
    pub fn pending_fsync_count(&self) -> usize {
        self.pending_fsync.len()
    }

    /// Fdatasync the active segment only.
    ///
    /// NOTE: this does NOT fsync rolled-over segments in `pending_fsync`.
    /// Callers that advance a durable cursor must use [`sync_all`] instead.
    pub fn fdatasync(&self) -> io::Result<()> {
        self.active.fdatasync()
    }

    /// Set the sparse-index stride for the active segment.
    ///
    /// Must be called BEFORE any records are appended to the active segment
    /// (i.e. right after construction, before the Committer starts) — it
    /// rebuilds the fresh index with the new stride. Smaller stride → faster
    /// point reads, larger `.idx`. No-op for frame (compressed/encrypted)
    /// segments, which don't build an index.
    pub fn set_index_stride(&mut self, stride: u32) {
        if self.active_index.is_some() {
            self.active_index = Some(index::SparseIndex::new(stride));
        }
    }

    /// Record this manager's shard id and shard-bit width, so that segment
    /// `base_sequence` headers are written as GLOBAL record ids (not local ring
    /// seqs). Called once per shard during `LogDb::open` before the Committer
    /// starts. Defaults to `0/0` (identity encoding) for `shards == 1` and for
    /// standalone/test construction.
    pub fn set_shard(&mut self, shard_id: usize, shard_bits: u32) {
        self.shard_id = shard_id;
        self.shard_bits = shard_bits;
    }

    /// Persist the active segment's sparse index to `<segment>.idx`.
    ///
    /// Called by the Committer after fsync (so the active segment is indexable
    /// too, not just sealed ones) and by `roll()` (for the segment being
    /// sealed). Best-effort for frame segments (no index built). Safe to call
    /// repeatedly — it just rewrites the small index file.
    pub fn save_active_index(&self) -> io::Result<()> {
        if let Some(idx) = &self.active_index {
            if !idx.is_empty() {
                idx.save(&index::SparseIndex::index_path(&self.active.path))?;
            }
        }
        Ok(())
    }

    /// Get the current active segment id.
    pub fn active_segment_id(&self) -> u32 {
        self.active_id
    }

    /// Get the current write offset within the active segment.
    pub fn active_offset(&self) -> u64 {
        self.active.offset
    }

    /// Get the base record_id of the active segment.
    pub fn base_sequence(&self) -> u64 {
        self.active.base_sequence
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.dir
    }

    /// Set the last hash (called when hash chain sealing completes).
    pub fn set_last_hash(&mut self, hash: [u8; 32]) {
        self.last_hash = hash;
    }

    /// Delete old segments whose records are entirely before `checkpoint`.
    ///
    /// Correctness (single linear sequence space — the only mode logdb uses for
    /// replication/offset semantics): segment N covers sequences
    /// `[base_N, base_{N+1})`. So segment N is fully before `checkpoint` iff
    /// `base_{N+1} <= checkpoint`, i.e. the NEXT segment's first sequence has
    /// already passed the checkpoint. We never touch the active (or any later)
    /// segment. Reading the next segment's `base_sequence` (rather than the
    /// current segment's max, which we don't backfill) is what makes this exact,
    /// not heuristic.
    fn truncate_before_checkpoint(&self, checkpoint: u64) -> Result<(), SegmentError> {
        let seg_files = self.list_segment_files()?;
        for (seg_id, path) in &seg_files {
            if *seg_id >= self.active_id {
                continue; // never delete the active (or any later) segment
            }
            // Segment seg_id is safe to delete iff the next segment's base
            // sequence is at/below the checkpoint (all of this segment's
            // records are then < checkpoint).
            if let Ok(next_header) = Self::read_segment_header_by_id(&self.dir, seg_id + 1) {
                if next_header.base_sequence <= checkpoint {
                    std::fs::remove_file(path).ok();
                }
            }
        }
        Ok(())
    }

    /// Read a segment header by id.
    fn read_segment_header(path: &Path) -> io::Result<SegmentHeader> {
        use std::io::Read;
        let mut file = File::open(path)?;
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        file.read_exact(&mut buf)?;
        SegmentHeader::deserialize(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn read_segment_header_by_id(dir: &Path, seg_id: u32) -> io::Result<SegmentHeader> {
        let filename = format!("segment-{:08}.log", seg_id);
        Self::read_segment_header(&dir.join(filename))
    }

    /// Apply the retention policy, deleting old segments.
    ///
    /// Semantics (L5, documented): retention is **size/age-based, NOT
    /// consumer-aware** — exactly like Kafka's log retention. A segment is
    /// eligible for deletion based only on total bytes / age, regardless of
    /// whether some Tailer/consumer has read it. Deployments with slow
    /// consumers that must not lose data should either (a) size the retention
    /// generously relative to consumer lag, or (b) checkpoint/truncate only to
    /// the minimum consumer position. The active segment is never deleted.
    fn apply_retention(&self) -> Result<(), SegmentError> {
        match &self.retention {
            RetentionPolicy::KeepAll => {}
            RetentionPolicy::MaxBytes(limit) => {
                log_debug!(shard = self.shard_id, limit = *limit, "applying MaxBytes retention");
                self.evict_until_under(*limit)?
            }
            RetentionPolicy::MaxAge(dur) => {
                log_debug!(
                    shard = self.shard_id,
                    max_age_ms = dur.as_millis() as u64,
                    "applying MaxAge retention"
                );
                self.evict_older_than(*dur)?
            }
        }
        Ok(())
    }

    /// Delete old segments until total segment data is under `max_bytes`.
    fn evict_until_under(&self, _max_bytes: u64) -> Result<(), SegmentError> {
        // List segment files, sort by id, delete oldest until under limit.
        let mut seg_files = self.list_segment_files()?;
        seg_files.sort_by_key(|(id, _)| *id);

        // Compute total size from file metadata
        let mut total_size: u64 = 0;
        for (_, path) in &seg_files {
            if let Ok(meta) = fs::metadata(path) {
                total_size += meta.len();
            }
        }
        for (seg_id, path) in &seg_files {
            if *seg_id >= self.active_id {
                break; // don't delete active or future segments
            }
            if total_size <= _max_bytes {
                break;
            }
            let file_size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            fs::remove_file(path)?;
            total_size = total_size.saturating_sub(file_size);
        }
        Ok(())
    }

    /// Delete segments older than `max_age`.
    fn evict_older_than(&self, _max_age: Duration) -> Result<(), SegmentError> {
        let seg_files = self.list_segment_files()?;
        for (seg_id, path) in &seg_files {
            if *seg_id >= self.active_id {
                break;
            }
            if let Ok(meta) = fs::metadata(path) {
                if let Ok(modified) = meta.modified() {
                    if let Ok(elapsed) = modified.elapsed() {
                        if elapsed >= _max_age {
                            fs::remove_file(path)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// List all segment files in the data directory.
    fn list_segment_files(&self) -> io::Result<Vec<(u32, PathBuf)>> {
        let mut result = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("segment-") && name.ends_with(".log") {
                    let id_str = &name[8..name.len() - 4]; // strip "segment-" and ".log"
                    if let Ok(id) = id_str.parse::<u32>() {
                        result.push((id, path));
                    }
                }
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::QueueFullPolicy;
    use crate::ring::Ring;

    fn make_ring() -> Ring {
        Ring::new(64, false, 0)
    }

    #[test]
    fn create_and_append_batch() {
        let dir = tempfile::tempdir().unwrap();
        let ring = make_ring();

        // Claim and publish some records
        for i in 0..10 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            let content = format!("record-{}", i);
            unsafe {
                ring.slot(seq)
                    .producer_write(seq, i * 100, content.as_bytes());
            }
            ring.slot(seq).publish(seq);
        }

        let mut mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            1 * 1024 * 1024, // 1MB segment
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        let last = mgr.append_batch(&ring, 0, 9).unwrap();
        assert_eq!(last, 9);
        assert!(mgr.active_offset() > SEGMENT_HEADER_SIZE as u64);
    }

    #[test]
    fn roll_creates_new_segment() {
        let dir = tempfile::tempdir().unwrap();

        let mut mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            1024, // tiny segment to force roll
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        let old_id = mgr.active_segment_id();
        assert_eq!(old_id, 1);

        mgr.roll(0, 0).unwrap();

        let new_id = mgr.active_segment_id();
        assert_eq!(new_id, 2);

        // Check both segment files exist
        let seg1 = dir.path().join("segment-00000001.log");
        let seg2 = dir.path().join("segment-00000002.log");
        assert!(seg1.exists());
        assert!(seg2.exists());
    }

    #[test]
    fn append_batch_returns_full_on_cramped_segment() {
        let dir = tempfile::tempdir().unwrap();
        let ring = make_ring();

        // Publish one record
        let seq = ring.claim(QueueFullPolicy::Block).unwrap();
        unsafe {
            ring.slot(seq).producer_write(seq, 0, b"hello");
        }
        ring.slot(seq).publish(seq);

        // Create a segment with only header room
        let mut mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            SEGMENT_HEADER_SIZE as u64 + 10, // barely larger than header
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        let result = mgr.append_batch(&ring, 0, 0);
        assert!(matches!(result, Err(SegmentError::Full)));
    }

    #[test]
    fn fdatasync_does_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            1 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        mgr.fdatasync().unwrap();
    }

    #[test]
    fn retention_keep_all() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            1 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        // Roll a few times
        mgr.roll(0, 0).unwrap();
        mgr.roll(0, 0).unwrap();

        // All segments should still exist
        let segs = mgr.list_segment_files().unwrap();
        assert_eq!(segs.len(), 3); // segments 1, 2, 3
    }

    #[test]
    fn segment_naming_convention() {
        let dir = tempfile::tempdir().unwrap();

        SegmentManager::create(
            dir.path().to_path_buf(),
            1 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();

        let seg1 = dir.path().join("segment-00000001.log");
        assert!(seg1.exists());
    }

    #[test]
    fn base_sequence_tracks_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            1 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            42,
        )
        .unwrap();

        assert_eq!(mgr.base_sequence(), 42);
    }

    #[test]
    fn write_buf_grows_as_needed() {
        let dir = tempfile::tempdir().unwrap();
        let ring = make_ring();

        // Create a large record
        let content = vec![0x42u8; 100_000];
        let seq = ring.claim(QueueFullPolicy::Block).unwrap();
        unsafe {
            ring.slot(seq).producer_write(seq, 0, &content);
        }
        ring.slot(seq).publish(seq);

        let mut mgr = SegmentManager::create(
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

        let last = mgr.append_batch(&ring, 0, 0).unwrap();
        assert_eq!(last, 0);
        assert!(mgr.write_buf.capacity() >= record_size(content.len()));
    }

    #[test]
    fn sync_all_drains_pending_after_rolls() {
        // P0-5 regression guard. Rolling queues the old segment in
        // `pending_fsync` un-fsynced. The durability fsync path (`sync_all`)
        // must drain it — otherwise `durable_cursor` would advance past
        // un-fsynced data across a segment roll and a crash would lose it.
        let dir = tempfile::tempdir().unwrap();
        let ring = make_ring();
        for i in 0..5 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            unsafe {
                ring.slot(seq).producer_write(seq, i * 10, b"roll-test");
            }
            ring.slot(seq).publish(seq);
        }

        let mut mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            1 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();
        mgr.append_batch(&ring, 0, 4).unwrap();

        // Force two rolls. With utilization well below 80%, roll() takes the
        // fallback path: create new segment, push OLD segment to pending_fsync.
        mgr.roll(5, 0).unwrap();
        mgr.roll(5, 0).unwrap();
        assert!(
            mgr.pending_fsync_count() >= 1,
            "roll must queue the old segment for fsync (pending={})",
            mgr.pending_fsync_count()
        );

        // The fix: sync_all drains pending before fsyncing active.
        mgr.sync_all().unwrap();
        assert_eq!(
            mgr.pending_fsync_count(),
            0,
            "sync_all must fsync rolled (pending) segments so durable_cursor can advance truthfully"
        );
    }

    #[test]
    fn roll_preserves_header_crc_and_timestamps() {
        // P0-6 regression guard: backfill_header_ts must write timestamps at the
        // correct offsets (56/64) and recompute the header CRC. Previously it
        // wrote at 52/60 (clobbering segment_id) and never recomputed the CRC,
        // so every rolled segment's header failed validation on read/recovery.
        let dir = tempfile::tempdir().unwrap();
        let ring = make_ring();
        for i in 0..3u64 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            unsafe {
                ring.slot(seq).producer_write(seq, 1000 + i * 100, b"ts");
            }
            ring.slot(seq).publish(seq);
        }

        let mut mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            1 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();
        mgr.append_batch(&ring, 0, 2).unwrap();
        mgr.roll(3, 0).unwrap();

        // The rolled segment's header must still deserialize (CRC valid) and
        // carry the correct segment_id + timestamp range.
        let path = dir.path().join("segment-00000001.log");
        let header = SegmentManager::read_segment_header(&path)
            .expect("rolled segment header must pass CRC validation");
        assert_eq!(
            header.segment_id, 1,
            "segment_id must not be clobbered by ts backfill"
        );
        assert_eq!(header.min_timestamp_ns, 1000);
        assert_eq!(header.max_timestamp_ns, 1200);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rolls_do_not_leak_file_descriptors() {
        // P1 regression guard: pending_fsync used to hold open File handles
        // (Vec<ActiveSegment>), so each roll leaked an fd until drain — under
        // sustained Async load (never idle) this exhausted the process fd
        // limit. Now pending_fsync stores PATHS; the rolled segment's fd is
        // released at roll time. 200 rolls must not add ~200 fds.
        let dir = tempfile::tempdir().unwrap();
        let ring = make_ring();
        for i in 0..5u64 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            unsafe {
                ring.slot(seq).producer_write(seq, i * 10, b"fd");
            }
            ring.slot(seq).publish(seq);
        }
        let mut mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            1 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();
        mgr.append_batch(&ring, 0, 4).unwrap();

        let fd_count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
        let before = fd_count();
        for _ in 0..200 {
            mgr.roll(0, 0).unwrap();
        }
        let after = fd_count();
        // Paths hold no fd: count must not scale with roll count.
        assert!(
            after < before + 32,
            "fd leak: before={} after={} after 200 rolls (paths should hold no fd)",
            before,
            after
        );
        // sync_all must still drain the accumulated paths (reopen-by-path works).
        mgr.sync_all().unwrap();
        assert_eq!(mgr.pending_fsync_count(), 0);
    }

    #[test]
    fn checkpoint_truncation_deletes_only_fully_covered_segments() {
        // P1-6 verification: a checkpoint must delete exactly the segments
        // whose records are ALL before it, and keep the straddling + active
        // segments. Segment N (records [base_N, base_{N+1})) is safe iff
        // base_{N+1} <= checkpoint.
        let dir = tempfile::tempdir().unwrap();
        let ring = make_ring();
        for i in 0..15u64 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            unsafe {
                ring.slot(seq).producer_write(seq, i * 10, b"cp");
            }
            ring.slot(seq).publish(seq);
        }
        let mut mgr = SegmentManager::create(
            dir.path().to_path_buf(),
            1 * 1024 * 1024,
            false,
            false,
            None,
            [0u8; 32],
            RetentionPolicy::KeepAll,
            0,
        )
        .unwrap();
        // seg1 base0 (recs 0..4) → seg2 base5 (recs 5..9) → seg3 base10 (recs 10..14)
        mgr.append_batch(&ring, 0, 4).unwrap();
        mgr.roll(5, 0).unwrap();
        mgr.append_batch(&ring, 5, 9).unwrap();
        mgr.roll(10, 0).unwrap();
        mgr.append_batch(&ring, 10, 14).unwrap();

        // checkpoint=7: seg1 fully before (next base 5 <= 7) → deleted.
        // seg2 straddles (next base 10 > 7) → kept. seg3 kept.
        mgr.roll(15, 7).unwrap();

        assert!(
            !dir.path().join("segment-00000001.log").exists(),
            "seg1 (all records < checkpoint 7) must be truncated"
        );
        assert!(
            dir.path().join("segment-00000002.log").exists(),
            "seg2 (straddles checkpoint) must be kept"
        );
        assert!(
            dir.path().join("segment-00000003.log").exists(),
            "seg3 must be kept"
        );
    }
}

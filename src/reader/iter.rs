//! Lazy record iterator for range scans and time queries.
//!
//! `RecordIter` reads records one at a time from a segment file, avoiding full
//! materialization in memory. It is layout-aware: raw segments are scanned
//! record-by-record; compressed/encrypted segments are scanned frame-by-frame
//! (each frame is decoded — decrypt then decompress — and its records yielded
//! in order).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use crate::error::ReadError;
use crate::record::Record;
use crate::storage::format::{
    deserialize_record, read_frame_header, FRAME_HEADER_SIZE, MIN_RECORD_SIZE,
};

use super::decode_frame_payload;

/// Read-chunk size for raw-mode scanning: amortizes per-record syscalls to
/// roughly one `read` per chunk (≈ one per 1000 small records). Larger than
/// any inline record; records above this trigger a buffer grow.
const READ_CHUNK: usize = 64 * 1024;

/// A lazy iterator over records from a single segment file.
pub struct RecordIter {
    /// File offset of the next frame (frame mode). (Raw mode uses the chunk
    /// buffer below instead.)
    offset: u64,
    file_size: u64,
    /// First record_id to yield (skip anything below this).
    from_id: u64,
    /// Exclusive end record_id (stop when record_id >= this).
    end_id: u64,
    /// The open file handle (None once exhausted).
    file: Option<File>,

    // ── Frame mode only ──
    is_compressed: bool,
    is_encrypted: bool,
    key: Option<[u8; 32]>,
    /// Decoded bytes of the current frame (raw records). Empty when no frame
    /// is loaded and we need to read the next one.
    frame_data: Vec<u8>,
    /// Cursor within `frame_data`.
    frame_pos: usize,
    /// Number of valid record bytes in `frame_data` (the frame's stored
    /// decompressed length, clamped to the actual decoded length).
    frame_len: usize,

    // ── Raw mode only: chunked read buffer ──
    /// Sliding window of file bytes (`buf.len()` == capacity; valid bytes are
    /// `buf[0..buf_len]`; cursor is `buf_pos`).
    buf: Vec<u8>,
    buf_len: usize,
    buf_pos: usize,
    /// File offset corresponding to `buf[0]`.
    base_offset: u64,
}

impl RecordIter {
    /// Create a new record iterator.
    ///
    /// For frame-based segments (compressed or encrypted), `start_offset` must
    /// be frame-aligned (typically `SEGMENT_HEADER_SIZE`) — sparse-index anchors
    /// are not frame boundaries.
    pub fn new(
        segment_path: PathBuf,
        start_offset: u64,
        file_size: u64,
        from_id: u64,
        end_id: u64,
        is_compressed: bool,
        is_encrypted: bool,
        key: Option<[u8; 32]>,
    ) -> Result<Self, ReadError> {
        let file = File::open(&segment_path).map_err(|e| {
            ReadError::Io(format!("open {:?}: {}", segment_path, e))
        })?;

        Ok(Self {
            offset: start_offset,
            file_size,
            from_id,
            end_id,
            file: Some(file),
            is_compressed,
            is_encrypted,
            key,
            frame_data: Vec::new(),
            frame_pos: 0,
            frame_len: 0,
            buf: vec![0u8; READ_CHUNK],
            buf_len: 0,
            buf_pos: 0,
            base_offset: start_offset,
        })
    }

    /// Load the next frame into `frame_data`. Returns false at EOF or on a
    /// partial/torn frame (end of usable data).
    fn load_next_frame(&mut self) -> bool {
        let file = match &mut self.file {
            Some(f) => f,
            None => return false,
        };
        if self.offset + FRAME_HEADER_SIZE as u64 > self.file_size {
            return false;
        }
        let mut fh = [0u8; FRAME_HEADER_SIZE];
        if file.seek(SeekFrom::Start(self.offset)).is_err() {
            return false;
        }
        if file.read_exact(&mut fh).is_err() {
            return false;
        }
        let (cl, dl) = read_frame_header(&fh);
        let cl = cl as usize;
        let dl = dl as usize;
        if cl == 0
            || dl == 0
            || self.offset + FRAME_HEADER_SIZE as u64 + cl as u64 > self.file_size
        {
            return false;
        }
        let mut payload = vec![0u8; cl];
        if file.read_exact(&mut payload).is_err() {
            return false;
        }
        let decoded = match decode_frame_payload(
            &payload,
            self.is_compressed,
            self.is_encrypted,
            self.key.as_ref(),
        ) {
            Ok(d) => d,
            Err(_) => return false,
        };
        self.frame_data = decoded;
        self.frame_len = dl.min(self.frame_data.len());
        self.frame_pos = 0;
        self.offset += FRAME_HEADER_SIZE as u64 + cl as u64;
        true
    }

    /// Ensure at least `n` bytes are available from the cursor (`buf_pos`)
    /// onward, compacting and refilling from the file as needed. Returns
    /// false at the file-size bound (torn tail / EOF) or on read error.
    fn ensure_raw(&mut self, n: usize) -> bool {
        // Fast path: already buffered.
        if self.buf_pos + n <= self.buf_len {
            return true;
        }
        // Compact unconsumed bytes to the front.
        if self.buf_pos > 0 {
            self.buf.copy_within(self.buf_pos..self.buf_len, 0);
            self.buf_len -= self.buf_pos;
            self.base_offset += self.buf_pos as u64;
            self.buf_pos = 0;
        }
        // Grow if a single record exceeds the buffer (records may be up to
        // max_content_size; the chunk is only READ_CHUNK).
        if n > self.buf.len() {
            let new_cap = n.next_power_of_two().max(READ_CHUNK);
            self.buf.resize(new_cap, 0);
        }
        // Refill. `file` borrows `self.file`; the buffer accesses borrow
        // `self.buf` — disjoint fields, allowed by the borrow checker.
        let file = match self.file.as_mut() {
            Some(f) => f,
            None => return false,
        };
        while self.buf_len < n {
            let remaining_in_file = self
                .file_size
                .saturating_sub(self.base_offset + self.buf_len as u64);
            if remaining_in_file == 0 {
                return false; // file bound reached without n bytes (torn tail)
            }
            let read_start = self.buf_len;
            let space = self.buf.len() - read_start;
            let want = (space as u64).min(remaining_in_file) as usize;
            if want == 0 {
                return false; // buffer full but still short (guard)
            }
            let file_pos = self.base_offset + read_start as u64;
            if file.seek(SeekFrom::Start(file_pos)).is_err() {
                self.file = None;
                return false;
            }
            match file.read(&mut self.buf[read_start..read_start + want]) {
                Ok(0) => return false,
                Ok(k) => self.buf_len += k,
                Err(_) => {
                    self.file = None;
                    return false;
                }
            }
        }
        true
    }
}

impl Iterator for RecordIter {
    type Item = Result<Record, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.file.is_none() {
            return None;
        }

        if self.is_compressed || self.is_encrypted {
            // ── Frame mode ──
            loop {
                // Yield records from the current frame until exhausted.
                while self.frame_pos + MIN_RECORD_SIZE <= self.frame_len
                    && self.frame_pos + MIN_RECORD_SIZE <= self.frame_data.len()
                {
                    let total = u32::from_le_bytes([
                        self.frame_data[self.frame_pos],
                        self.frame_data[self.frame_pos + 1],
                        self.frame_data[self.frame_pos + 2],
                        self.frame_data[self.frame_pos + 3],
                    ]) as usize;
                    if total < MIN_RECORD_SIZE
                        || self.frame_pos + total > self.frame_len
                        || self.frame_pos + total > self.frame_data.len()
                    {
                        // Corrupt record within the frame — abandon it.
                        break;
                    }
                    let rec = deserialize_record(
                        &self.frame_data[self.frame_pos..self.frame_pos + total],
                    );
                    self.frame_pos += total;
                    match rec {
                        Ok((record, _)) => {
                            if record.id.sequence < self.from_id {
                                continue;
                            }
                            if record.id.sequence >= self.end_id {
                                self.file = None;
                                return None;
                            }
                            return Some(Ok(record));
                        }
                        Err(_) => continue, // skip bad record, keep scanning frame
                    }
                }
                // Current frame exhausted — load the next one.
                if !self.load_next_frame() {
                    self.file = None;
                    return None;
                }
            }
        }

        // ── Raw mode (uncompressed, unencrypted): chunked buffer scan ──
        //
        // The file is read in READ_CHUNK-sized windows into `buf`; records are
        // parsed from the buffer via `buf_pos`. This amortizes syscalls to
        // ~one read per chunk (vs the old seek+read+seek+read per record) and
        // drops the per-record heap allocation. `ensure_raw` compacts/refills
        // (and grows for records larger than the chunk) as needed.
        loop {
            if !self.ensure_raw(4) {
                self.file = None;
                return None;
            }
            let p = self.buf_pos;
            let total = u32::from_le_bytes([
                self.buf[p],
                self.buf[p + 1],
                self.buf[p + 2],
                self.buf[p + 3],
            ]) as usize;
            if total < MIN_RECORD_SIZE {
                // Corrupt length — resync by advancing one byte (matches the
                // legacy `offset += 1` byte-scan behavior).
                self.buf_pos += 1;
                continue;
            }
            if !self.ensure_raw(total) {
                self.file = None;
                return None; // torn tail
            }
            let p = self.buf_pos;
            let rec_bytes = &self.buf[p..p + total];
            self.buf_pos += total;
            match deserialize_record(rec_bytes) {
                Ok((record, _)) => {
                    if record.id.sequence < self.from_id {
                        continue;
                    }
                    if record.id.sequence >= self.end_id {
                        self.file = None;
                        return None;
                    }
                    return Some(Ok(record));
                }
                Err(_) => continue,
            }
        }
    }
}

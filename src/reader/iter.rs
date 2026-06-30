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

/// A lazy iterator over records from a single segment file.
pub struct RecordIter {
    /// File offset of the next frame (frame mode) or next record (raw mode).
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
}

impl Iterator for RecordIter {
    type Item = Result<Record, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        let file = match &mut self.file {
            Some(f) => f,
            None => return None,
        };

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

        // ── Raw mode (uncompressed, unencrypted) ──
        loop {
            if self.offset >= self.file_size {
                self.file = None;
                return None;
            }
            if self.offset + 4 > self.file_size {
                self.file = None;
                return None;
            }
            let mut len_buf = [0u8; 4];
            if let Err(e) = file.seek(SeekFrom::Start(self.offset)) {
                self.file = None;
                return Some(Err(ReadError::Io(format!("seek: {}", e))));
            }
            if let Err(e) = file.read_exact(&mut len_buf) {
                self.file = None;
                return Some(Err(ReadError::Io(format!("read len: {}", e))));
            }
            let total = u32::from_le_bytes(len_buf) as usize;
            if total < MIN_RECORD_SIZE {
                self.offset += 1;
                continue;
            }
            if self.offset + total as u64 > self.file_size {
                self.file = None;
                return None;
            }
            let mut record_buf = vec![0u8; total];
            if let Err(e) = file.seek(SeekFrom::Start(self.offset)) {
                self.file = None;
                return Some(Err(ReadError::Io(format!("seek record: {}", e))));
            }
            if let Err(e) = file.read_exact(&mut record_buf) {
                self.file = None;
                return Some(Err(ReadError::Io(format!("read record: {}", e))));
            }
            self.offset += total as u64;
            match deserialize_record(&record_buf) {
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

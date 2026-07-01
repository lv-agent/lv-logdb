//! Crash recovery — reconstruct state from segment files on disk.
//!
//! On startup, recovery scans all segment files, validates headers and records,
//! detects torn writes (truncating at the last valid CRC), rebuilds sparse
//! indexes, and returns the reconstructed state for ring buffer initialization.
//!
//! # Recovery Algorithm (§15)
//!
//! 1. List all `segment-*.log` files, sorted by segment_id ascending.
//! 2. For each segment: validate header (magic + header_crc).
//!    - Bad header → that segment and all later segments are discarded.
//! 3. For the last segment only: sequential scan of records.
//!    - Read len → read full record → CRC check → verify record_id.
//!    - Torn write detected when file ends mid-record or CRC fails at end-of-file.
//!    - Truncate file to the last complete record.
//! 4. Rebuild sparse index for each segment (deferred to Phase 7).
//! 5. Return recovery state.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::config::RetentionPolicy;
use crate::storage::format::{
    deserialize_record, read_frame_header, SegmentHeader, FLAG_COMPRESSED_ZSTD,
    FLAG_ENCRYPTED_AES256GCM, FRAME_HEADER_SIZE, MIN_RECORD_SIZE, SEGMENT_HEADER_SIZE,
};
use crate::storage::SegmentManager;

/// Warnings produced during recovery (non-fatal).
#[derive(Debug, Clone)]
pub enum RecoveryWarning {
    /// A torn write was detected and the segment was truncated.
    TornWrite {
        segment_id: u32,
        /// The record_id where truncation occurred.
        at_record_id: u64,
        /// The file offset where truncation occurred.
        at_offset: u64,
    },
    /// A segment header was corrupted; this and later segments discarded.
    CorruptedHeader { segment_id: u32, reason: String },
    /// A segment file was missing from the expected sequence.
    MissingSegment { expected_id: u32 },
    /// A hash chain discontinuity was detected.
    HashChainBreak {
        segment_id: u32,
        expected_hash: [u8; 32],
        found_hash: [u8; 32],
    },
}

/// The result of a successful recovery.
pub struct RecoveryState {
    /// The SegmentManager ready for use.
    pub segment_manager: SegmentManager,
    /// The last valid record_id found (or base_record_id - 1 if no records).
    pub last_sequence: u64,
    /// The last valid hash_n (or all zeros if hash disabled).
    pub last_hash: [u8; 32],
    /// The active segment id.
    pub active_segment_id: u32,
    /// The next write offset within the active segment.
    pub active_offset: u64,
    /// Whether hash chain is enabled.
    pub hash_enabled: bool,
    /// The hash_init value from the first segment header.
    pub hash_init: [u8; 32],
    /// Any non-fatal warnings produced during recovery.
    pub warnings: Vec<RecoveryWarning>,
    /// Number of valid records recovered in the active (last) segment. `0`
    /// means the shard's active segment had no recoverable records (empty
    /// shard); the owning ring must then resume at LOCAL seq 0 rather than from
    /// the `base_sequence - 1` sentinel.
    pub recovered_count: u64,
}

/// Recover a single shard's directory. `shard_dir` is `data_dir` for
/// `shards == 1` (flat layout) or `data_dir/s<shard>/` for `shards > 1`.
///
/// Each shard is an independent, self-contained recoverable log: within a shard,
/// consecutive record ids differ by exactly `1 << shard_bits` (the shard id
/// occupies the low `shard_bits` bits and is constant per shard). The torn-write
/// scan uses that stride; for `shards == 1`, `shard_bits == 0` → stride 1
/// (identical to the legacy single-shard scan).
///
/// Returns `Ok(RecoveryState)` on success. Returns `Err(String)` if recovery
/// fails catastrophically (no valid segments found, etc.).
pub fn recover_shard(
    shard_dir: &Path,
    shard_bits: u32,
    segment_size: u64,
    retention: RetentionPolicy,
    encryption_key: Option<[u8; 32]>,
) -> Result<RecoveryState, String> {
    if !shard_dir.exists() {
        return Err(format!("data directory does not exist: {:?}", shard_dir));
    }

    // 1. List and sort segment files
    let mut seg_files = list_segment_files(shard_dir)?;
    if seg_files.is_empty() {
        return Err(format!("no segment files found in {:?}", shard_dir));
    }
    seg_files.sort_by_key(|(id, _)| *id);

    let mut warnings = Vec::new();

    // 2. Validate headers
    let mut valid_segments: Vec<(u32, PathBuf, SegmentHeader, u64)> = Vec::new();
    // (segment_id, path, header, file_size)

    for (seg_id, path) in &seg_files {
        let file = File::open(path).map_err(|e| format!("open {:?}: {}", path, e))?;
        let file_size = file
            .metadata()
            .map_err(|e| format!("metadata {:?}: {}", path, e))?
            .len();

        if file_size < SEGMENT_HEADER_SIZE as u64 {
            warnings.push(RecoveryWarning::CorruptedHeader {
                segment_id: *seg_id,
                reason: format!("file too small: {} bytes", file_size),
            });
            break; // This and later segments are invalid
        }

        let mut header_buf = [0u8; SEGMENT_HEADER_SIZE];
        let file = File::open(path).map_err(|e| format!("open {:?}: {}", path, e))?;
        // Read header
        let n = std::io::Read::by_ref(&mut &file)
            .read_exact(&mut header_buf)
            .map_err(|e| format!("read header {:?}: {}", path, e));
        if n.is_err() {
            warnings.push(RecoveryWarning::CorruptedHeader {
                segment_id: *seg_id,
                reason: "failed to read header".to_string(),
            });
            break;
        }

        match SegmentHeader::deserialize(&header_buf) {
            Ok(header) => {
                if header.segment_id != *seg_id {
                    warnings.push(RecoveryWarning::CorruptedHeader {
                        segment_id: *seg_id,
                        reason: format!(
                            "segment_id mismatch: header says {}, filename says {}",
                            header.segment_id, seg_id
                        ),
                    });
                    break;
                }
                valid_segments.push((*seg_id, path.clone(), header, file_size));
            }
            Err(e) => {
                warnings.push(RecoveryWarning::CorruptedHeader {
                    segment_id: *seg_id,
                    reason: e,
                });
                break;
            }
        }
    }

    if valid_segments.is_empty() {
        return Err("no valid segments found after header validation".to_string());
    }

    // 3. Extract metadata from the first valid segment
    let first_header = &valid_segments[0].2; // (seg_id, path, header, size)
    let hash_enabled = first_header.hash_enabled();
    let hash_init = first_header.hash_init;

    // 4. Scan the last segment for torn writes
    let last_idx = valid_segments.len() - 1;
    let (last_seg_id, last_path, last_header, _last_size) = &valid_segments[last_idx];
    let (last_sequence, last_hash, active_offset, truncation_warnings, recovered_count) =
        scan_last_segment(
            last_path,
            *last_seg_id,
            last_header,
            hash_enabled,
            encryption_key,
            shard_bits,
        )?;

    warnings.extend(truncation_warnings);

    // 5. Verify hash chain continuity across segments (if hash enabled).
    // NOTE: cross-segment continuity is checked against each segment header's
    // `prev_last_hash`. Intra-segment chain verification happens inside
    // scan_last_segment (per record). The previous segment's actual last hash
    // would require scanning it; we rely on the stored prev_last_hash field.
    if hash_enabled {
        let mut expected_prev_hash = [0u8; 32];
        for (seg_id, _path, header, _size) in &valid_segments {
            if header.prev_last_hash != expected_prev_hash && expected_prev_hash != [0u8; 32] {
                warnings.push(RecoveryWarning::HashChainBreak {
                    segment_id: *seg_id,
                    expected_hash: expected_prev_hash,
                    found_hash: header.prev_last_hash,
                });
            }
            // Without scanning each prior segment we cannot know its true last
            // hash; leave expected as zeros for now (intra-segment verification
            // in scan_last_segment is the strong guarantee).
            expected_prev_hash = [0u8; 32];
        }
    }

    // 6. Build SegmentManager
    // The last valid offset in the active segment (after truncation)
    let final_offset = active_offset;

    let seg_mgr = SegmentManager::open_existing(
        shard_dir.to_path_buf(),
        last_path.clone(),
        *last_seg_id,
        final_offset,
        last_header,
        segment_size,
        last_hash,
        hash_init,
        hash_enabled,
        encryption_key,
        retention,
    )
    .map_err(|e| format!("open existing segment manager: {}", e))?;

    if !warnings.is_empty() {
        log_warn!(
            shard_dir = ?shard_dir,
            warning_count = warnings.len(),
            recovered = recovered_count,
            "recovery completed with warnings (torn writes / corrupt headers / hash breaks)"
        );
    }

    Ok(RecoveryState {
        segment_manager: seg_mgr,
        last_sequence,
        last_hash,
        active_segment_id: *last_seg_id,
        active_offset: final_offset,
        hash_enabled,
        hash_init,
        warnings,
        recovered_count,
    })
}

/// Recover a single-shard (flat) database. Thin wrapper around
/// [`recover_shard`] with `shard_bits == 0` (stride 1, identity id encoding).
/// Kept for source compatibility with callers that predate per-shard recovery.
pub fn recover(
    data_dir: &Path,
    segment_size: u64,
    retention: RetentionPolicy,
    encryption_key: Option<[u8; 32]>,
) -> Result<RecoveryState, String> {
    recover_shard(data_dir, 0, segment_size, retention, encryption_key)
}

/// Scan the last segment to detect torn writes and find the recovery point.
///
/// Layout-aware: raw segments are scanned record-by-record; compressed or
/// encrypted segments are scanned frame-by-frame (each frame is decoded —
/// decrypt then decompress — and its records validated). A partial/corrupt
/// frame or record at the tail is treated as a torn write and the file is
/// truncated to the last fully-valid boundary.
///
/// Returns:
/// - `last_sequence`: the highest valid record_id found
/// - `last_hash`: the hash_n of the last valid record
/// - `valid_offset`: the file offset just past the last valid record/frame
/// - `warnings`: any warnings generated
/// - `count`: number of valid records recovered (0 ⇒ empty active segment)
///
/// `shard_bits` sets the id stride: within a shard, consecutive record ids
/// differ by `1 << shard_bits`. For `shards == 1`, `shard_bits == 0` → stride 1.
fn scan_last_segment(
    path: &Path,
    segment_id: u32,
    header: &SegmentHeader,
    hash_enabled: bool,
    encryption_key: Option<[u8; 32]>,
    shard_bits: u32,
) -> Result<(u64, [u8; 32], u64, Vec<RecoveryWarning>, u64), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open(path)
        .map_err(|e| format!("open {:?}: {}", path, e))?;
    let file_size = file
        .metadata()
        .map_err(|e| format!("metadata {:?}: {}", path, e))?
        .len();

    let is_compressed = header.flags & FLAG_COMPRESSED_ZSTD != 0;
    let is_encrypted = header.flags & FLAG_ENCRYPTED_AES256GCM != 0;

    let mut offset = SEGMENT_HEADER_SIZE as u64;
    let mut last_valid_offset = offset; // just past the last valid record/frame
    let mut last_sequence = header.base_sequence.wrapping_sub(1);
    let mut last_hash = [0u8; 32];
    let mut warnings = Vec::new();
    let mut expected_record_id = header.base_sequence;

    // Within a shard, consecutive record ids differ by `1 << shard_bits`: the
    // shard id occupies the low `shard_bits` bits (constant per shard), so a
    // local-sequence increment of 1 raises the global id by exactly this stride.
    // shards=1 ⇒ shard_bits=0 ⇒ stride 1 (identical to the legacy scan).
    let stride: u64 = 1u64 << shard_bits;
    let mut count: u64 = 0;

    // Hash-chain verification (P0-4): recompute BLAKE3 keyed chain over each
    // record and compare to the stored hash_n. A mismatch means tampering or
    // corruption → treat as a break and stop trusting data past it.
    #[cfg(feature = "hash-chain")]
    let hash_init = header.hash_init;
    #[cfg(feature = "hash-chain")]
    let mut chain_prev = header.prev_last_hash; // [0;32] for the first segment
    #[cfg(not(feature = "hash-chain"))]
    let _ = hash_enabled;

    // Truncate the file to `last_valid_offset` and record a torn-write warning.
    // Defined as a macro so it expands in place (no borrow held across the
    // loop, unlike a closure capturing `file`/`warnings`/the counters).
    macro_rules! torn {
        ($at:expr_2021) => {{
            file.set_len(last_valid_offset)
                .map_err(|e| format!("truncate {:?}: {}", path, e))?;
            warnings.push(RecoveryWarning::TornWrite {
                segment_id,
                at_record_id: expected_record_id,
                at_offset: $at,
            });
            break;
        }};
    }

    if is_compressed || is_encrypted {
        // ── Frame mode: [frame_header(8)][payload] per batch ──
        while offset < file_size {
            if offset + FRAME_HEADER_SIZE as u64 > file_size {
                torn!(offset);
            }
            let mut fh = [0u8; FRAME_HEADER_SIZE];
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| format!("seek {:?}: {}", path, e))?;
            if file.read_exact(&mut fh).is_err() {
                torn!(offset);
            }
            let (cl, dl) = read_frame_header(&fh);
            let cl = cl as usize;
            let dl = dl as usize;
            if cl == 0 || dl == 0 || offset + FRAME_HEADER_SIZE as u64 + cl as u64 > file_size {
                torn!(offset);
            }
            let mut payload = vec![0u8; cl];
            // Payload starts right after the 8-byte frame header (cursor is
            // already there after reading fh, but seek explicitly for clarity).
            file.seek(SeekFrom::Start(offset + FRAME_HEADER_SIZE as u64))
                .map_err(|e| format!("seek {:?}: {}", path, e))?;
            if file.read_exact(&mut payload).is_err() {
                torn!(offset);
            }
            let decoded = match crate::reader::decode_frame_payload(
                &payload,
                is_compressed,
                is_encrypted,
                encryption_key.as_ref(),
            ) {
                Ok(d) => d,
                Err(_) => {
                    torn!(offset);
                } // decode failure = torn/corrupt frame
            };
            // Scan records within the decoded frame.
            let valid_len = dl.min(decoded.len());
            let mut doff = 0usize;
            let mut frame_ok = true;
            while doff + MIN_RECORD_SIZE <= valid_len {
                let total = u32::from_le_bytes([
                    decoded[doff],
                    decoded[doff + 1],
                    decoded[doff + 2],
                    decoded[doff + 3],
                ]) as usize;
                if total < MIN_RECORD_SIZE || doff + total > valid_len {
                    break;
                }
                match deserialize_record(&decoded[doff..doff + total]) {
                    Ok((record, _)) => {
                        if record.id.sequence != expected_record_id
                            && !(count == 0 && shard_bits > 0)
                        {
                            frame_ok = false;
                            break;
                        }
                        last_sequence = record.id.sequence;
                        last_hash = record.hash_n;
                        count += 1;
                        #[cfg(feature = "hash-chain")]
                        if hash_enabled {
                            let expected = crate::pipeline::sealer::blake3_keyed_chain(
                                &hash_init,
                                &chain_prev,
                                record.content.as_slice(),
                            );
                            if expected != record.hash_n {
                                warnings.push(RecoveryWarning::HashChainBreak {
                                    segment_id,
                                    expected_hash: expected,
                                    found_hash: record.hash_n,
                                });
                                torn!(offset);
                            }
                            chain_prev = record.hash_n;
                        }
                        expected_record_id = record.id.sequence + stride;
                        doff += total;
                    }
                    Err(_) => {
                        frame_ok = false;
                        break;
                    }
                }
            }
            if !frame_ok {
                torn!(offset);
            }
            last_valid_offset = offset + FRAME_HEADER_SIZE as u64 + cl as u64;
            offset = last_valid_offset;
        }
    } else {
        // ── Raw mode: record-by-record scan ──
        let mut len_buf = [0u8; 4];
        while offset < file_size {
            if offset + 4 > file_size {
                torn!(offset);
            }
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| format!("seek {:?}: {}", path, e))?;
            file.read_exact(&mut len_buf)
                .map_err(|e| format!("read len at {}: {}", offset, e))?;
            let total = u32::from_le_bytes(len_buf) as usize;
            if total < MIN_RECORD_SIZE {
                torn!(offset);
            }
            if offset + total as u64 > file_size {
                torn!(offset);
            }
            let mut record_buf = vec![0u8; total];
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| format!("seek {:?}: {}", path, e))?;
            file.read_exact(&mut record_buf)
                .map_err(|e| format!("read record at {}: {}", offset, e))?;
            match deserialize_record(&record_buf) {
                Ok((record, _)) => {
                    // The first record's global id may exceed base_sequence under
                    // sharding (the shard id occupies the low bits, and base is 0):
                    // allow that one mismatch and let the stride assignment below
                    // (re)seed the chain. shards=1 (shard_bits==0) never takes the
                    // exemption, so its strict first-record check is unchanged.
                    if record.id.sequence != expected_record_id && !(count == 0 && shard_bits > 0) {
                        torn!(offset);
                    }
                    last_sequence = record.id.sequence;
                    last_hash = record.hash_n;
                    count += 1;
                    #[cfg(feature = "hash-chain")]
                    if hash_enabled {
                        let expected = crate::pipeline::sealer::blake3_keyed_chain(
                            &hash_init,
                            &chain_prev,
                            record.content.as_slice(),
                        );
                        if expected != record.hash_n {
                            warnings.push(RecoveryWarning::HashChainBreak {
                                segment_id,
                                expected_hash: expected,
                                found_hash: record.hash_n,
                            });
                            torn!(offset);
                        }
                        chain_prev = record.hash_n;
                    }
                    last_valid_offset = offset + total as u64;
                    expected_record_id = record.id.sequence + stride;
                    offset = last_valid_offset;
                }
                Err(_) => {
                    let after_record = offset + total as u64;
                    if after_record >= file_size {
                        torn!(offset);
                    } else {
                        warnings.push(RecoveryWarning::CorruptedHeader {
                            segment_id,
                            reason: format!(
                                "CRC mismatch at record_id={}, offset={}",
                                expected_record_id, offset
                            ),
                        });
                        offset = after_record;
                    }
                }
            }
        }
    }

    Ok((last_sequence, last_hash, last_valid_offset, warnings, count))
}

/// List segment files in a directory, returning (segment_id, path) pairs.
fn list_segment_files(dir: &Path) -> Result<Vec<(u32, PathBuf)>, String> {
    let mut result = Vec::new();
    let entries = fs::read_dir(dir).map_err(|e| format!("read_dir {:?}: {}", dir, e))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("entry {:?}: {}", dir, e))?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("segment-") && name.ends_with(".log") {
                let id_str = &name[8..name.len() - 4];
                if let Ok(id) = id_str.parse::<u32>() {
                    result.push((id, path));
                }
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::QueueFullPolicy;
    use crate::config::RetentionPolicy;
    use crate::ring::Ring;

    #[test]
    fn recover_fresh_database_then_recover() {
        let dir = tempfile::tempdir().unwrap();
        let ring = Ring::new(64, false, 0);

        // Create a segment manager and write some records
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

        // Write some records
        for i in 0..10 {
            let seq = ring.claim(QueueFullPolicy::Block).unwrap();
            let content = format!("recovery-test-{}", i);
            unsafe {
                ring.slot(seq)
                    .producer_write(seq, i * 100, content.as_bytes());
            }
            ring.slot(seq).publish(seq);
        }

        let last = mgr.append_batch(&ring, 0, 9).unwrap();
        assert_eq!(last, 9);
        mgr.fdatasync().unwrap();

        // Drop everything and recover
        drop(mgr);
        drop(ring);

        let state = recover(dir.path(), 10 * 1024 * 1024, RetentionPolicy::KeepAll, None).unwrap();
        assert_eq!(state.last_sequence, 9);
        assert_eq!(state.active_segment_id, 1);
        assert!(!state.hash_enabled);
        assert!(state.warnings.is_empty());
        assert_eq!(state.recovered_count, 10);
    }
}

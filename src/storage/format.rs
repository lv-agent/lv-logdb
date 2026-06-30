//! Wire format constants and serialization/deserialization.
//!
//! # Segment Header Layout (128 bytes)
//!
//! | Offset | Size | Field            | Description                              |
//! |--------|------|------------------|------------------------------------------|
//! | 0      | 4    | magic            | 0x4C474442 ("LGDB")                      |
//! | 4      | 2    | format_version   | 0x0001                                   |
//! | 6      | 1    | flags            | bit0=not-first, bit1=hash_enabled        |
//! | 7      | 1    | hash_algo        | 0=None, 1=SHA256, 2=BLAKE3               |
//! | 8      | 32   | hash_init        | CSPRNG, globally unique (hash enabled)   |
//! | 40     | 8    | base_sequence    | First sequence in this segment           |
//! | 48     | 4    | partition_id     | Logical partition identifier             |
//! | 52     | 4    | segment_id       | Monotonically increasing from 1          |
//! | 56     | 8    | min_timestamp_ns | Earliest record timestamp (backfilled)   |
//! | 64     | 8    | max_timestamp_ns | Latest record timestamp (backfilled)     |
//! | 72     | 4    | header_crc       | CRC32C of bytes [0, 72)                  |
//! | 76     | 32   | prev_last_hash   | Previous segment's final hash_n          |
//! | 108    | 1    | record_format    | Record encoding format version           |
//! | 109    | 19   | _reserved        | Future extensions                        |
//! | 128    |      |                  | END                                      |
//!
//! # Record Layout
//!
//! | Field        | Type    | Size | Notes                                    |
//! |------------- |---------|------|------------------------------------------|
//! | len          | u32 LE  | 4    | Total record bytes (incl. self + crc)    |
//! | sequence     | u64 LE  | 8    | Partition-local sequence number          |
//! | timestamp_ns | u64 LE  | 8    |                                          |
//! | content_len  | u32 LE  | 4    |                                          |
//! | content      | [u8]    | N    | Variable length                          |
//! | hash_n       | [u8;32] | 32   | Always present, zeros if hash disabled   |
//! | crc          | u32 LE  | 4    | CRC32C over bytes [len_field, crc_field) |

use crate::record::{ReadView, Record, RecordId};

// ── Magic & version ────────────────────────────────────────────────────────

/// Magic bytes: "LGDB" in ASCII.
pub const MAGIC: u32 = 0x4C47_4442;

/// Current format version.
pub const FORMAT_VERSION: u16 = 0x0001;

// ── Hash algorithms ────────────────────────────────────────────────────────

pub const HASH_ALGO_NONE: u8 = 0;
pub const HASH_ALGO_SHA256: u8 = 1;
pub const HASH_ALGO_BLAKE3: u8 = 2;

// ── Record format versions ─────────────────────────────────────────────────

pub const RECORD_FORMAT_V1: u8 = 1;

// ── Segment header ─────────────────────────────────────────────────────────

/// Size of the segment header in bytes.
pub const SEGMENT_HEADER_SIZE: usize = 128;

/// CRC computation range: bytes [0, HEADER_CRC_END).
pub const HEADER_CRC_END: usize = 72;

/// Flag: this is NOT the first segment (bit 0).
pub const FLAG_NOT_FIRST: u8 = 0x01;

/// Flag: hash chain is enabled (bit 1).
pub const FLAG_HASH_ENABLED: u8 = 0x02;

/// Flag: segment uses streaming zstd compression (bit 2).
pub const FLAG_COMPRESSED_ZSTD: u8 = 0x04;
pub const FLAG_ENCRYPTED_AES256GCM: u8 = 0x08;
pub const ENCRYPTION_NONCE_SIZE: usize = 12;

// Frame format (compressed segments)

/// Per-frame header size: compressed_len(u32 LE) + decompressed_len(u32 LE).
pub const FRAME_HEADER_SIZE: usize = 8;

pub fn write_frame_header(buf: &mut [u8; FRAME_HEADER_SIZE], compressed_len: u32, decompressed_len: u32) {
    buf[0..4].copy_from_slice(&compressed_len.to_le_bytes());
    buf[4..8].copy_from_slice(&decompressed_len.to_le_bytes());
}

pub fn read_frame_header(buf: &[u8; FRAME_HEADER_SIZE]) -> (u32, u32) {
    let cl = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let dl = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
    (cl, dl)
}

/// A parsed and validated segment header.
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub format_version: u16,
    pub flags: u8,
    pub hash_algo: u8,
    pub hash_init: [u8; 32],
    pub base_sequence: u64,
    pub partition_id: u32,
    pub segment_id: u32,
    pub min_timestamp_ns: u64,
    pub max_timestamp_ns: u64,
    pub prev_last_hash: [u8; 32],
    pub record_format: u8,
}

impl SegmentHeader {
    pub fn is_first(&self) -> bool {
        self.flags & FLAG_NOT_FIRST == 0
    }

    pub fn hash_enabled(&self) -> bool {
        self.flags & FLAG_HASH_ENABLED != 0
    }

    /// Serialize the header into a 128-byte buffer.
    pub fn serialize(&self, buf: &mut [u8; SEGMENT_HEADER_SIZE], last_hash: [u8; 32]) {
        buf.fill(0);

        buf[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        buf[4..6].copy_from_slice(&self.format_version.to_le_bytes());
        buf[6] = self.flags;
        buf[7] = self.hash_algo;
        buf[8..40].copy_from_slice(&self.hash_init);
        buf[40..48].copy_from_slice(&self.base_sequence.to_le_bytes());
        buf[48..52].copy_from_slice(&self.partition_id.to_le_bytes());
        buf[52..56].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[56..64].copy_from_slice(&self.min_timestamp_ns.to_le_bytes());
        buf[64..72].copy_from_slice(&self.max_timestamp_ns.to_le_bytes());
        // header_crc at 72..76 — filled below
        buf[76..108].copy_from_slice(&last_hash);
        buf[108] = self.record_format;
        // _reserved at 109..128 — already zero

        let crc = crc32c::crc32c(&buf[..HEADER_CRC_END]);
        buf[72..76].copy_from_slice(&crc.to_le_bytes());
    }

    /// Deserialize and validate a segment header from a buffer.
    pub fn deserialize(buf: &[u8; SEGMENT_HEADER_SIZE]) -> Result<Self, String> {
        let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != MAGIC {
            return Err(format!("bad magic: 0x{:08X}, expected 0x{:08X}", magic, MAGIC));
        }

        let stored_crc = u32::from_le_bytes([buf[72], buf[73], buf[74], buf[75]]);
        let computed_crc = crc32c::crc32c(&buf[..HEADER_CRC_END]);
        if stored_crc != computed_crc {
            return Err(format!(
                "header CRC mismatch: stored=0x{:08X}, computed=0x{:08X}",
                stored_crc, computed_crc
            ));
        }

        let format_version = u16::from_le_bytes([buf[4], buf[5]]);
        let flags = buf[6];
        let hash_algo = buf[7];

        let mut hash_init = [0u8; 32];
        hash_init.copy_from_slice(&buf[8..40]);

        let base_sequence = u64::from_le_bytes([
            buf[40], buf[41], buf[42], buf[43],
            buf[44], buf[45], buf[46], buf[47],
        ]);

        let partition_id = u32::from_le_bytes([buf[48], buf[49], buf[50], buf[51]]);

        let segment_id = u32::from_le_bytes([buf[52], buf[53], buf[54], buf[55]]);

        let min_timestamp_ns = u64::from_le_bytes([
            buf[56], buf[57], buf[58], buf[59],
            buf[60], buf[61], buf[62], buf[63],
        ]);

        let max_timestamp_ns = u64::from_le_bytes([
            buf[64], buf[65], buf[66], buf[67],
            buf[68], buf[69], buf[70], buf[71],
        ]);

        let mut prev_last_hash = [0u8; 32];
        prev_last_hash.copy_from_slice(&buf[76..108]);

        let record_format = buf[108];

        Ok(Self {
            format_version,
            flags,
            hash_algo,
            hash_init,
            base_sequence,
            partition_id,
            segment_id,
            min_timestamp_ns,
            max_timestamp_ns,
            prev_last_hash,
            record_format,
        })
    }

    /// Create a header for the very first segment.
    pub fn first_segment(
        hash_init: [u8; 32],
        base_sequence: u64,
        partition_id: u32,
        segment_id: u32,
        hash_enabled: bool,
        hash_algo: u8,
    ) -> Self {
        let mut flags = 0u8;
        if hash_enabled {
            flags |= FLAG_HASH_ENABLED;
        }
        Self {
            format_version: FORMAT_VERSION,
            flags,
            hash_algo,
            hash_init,
            base_sequence,
            partition_id,
            segment_id,
            min_timestamp_ns: u64::MAX,
            max_timestamp_ns: 0,
            prev_last_hash: [0u8; 32],
            record_format: RECORD_FORMAT_V1,
        }
    }

    /// Create a header for a subsequent segment (rolled).
    pub fn next_segment(
        &self,
        base_sequence: u64,
        segment_id: u32,
        prev_last_hash: [u8; 32],
    ) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            flags: self.flags | FLAG_NOT_FIRST,
            hash_algo: self.hash_algo,
            hash_init: self.hash_init,
            base_sequence,
            partition_id: self.partition_id,
            segment_id,
            min_timestamp_ns: u64::MAX,
            max_timestamp_ns: 0,
            prev_last_hash,
            record_format: RECORD_FORMAT_V1,
        }
    }
}

// ── Record serialization ───────────────────────────────────────────────────

/// Minimum record size: len(4) + seq(8) + ts(8) + content_len(4) + hash_n(32) + crc(4) = 60
pub const MIN_RECORD_SIZE: usize = 60;

#[inline]
pub fn record_size(content_len: usize) -> usize {
    4 + 8 + 8 + 4 + content_len + 32 + 4
}

/// Serialize a single record from a `ReadView` into a buffer.
/// Returns the number of bytes written.
///
/// The `sequence` parameter is the global sequence number (partition-local).
pub fn serialize_record(buf: &mut [u8], sequence: u64, view: &ReadView<'_>) -> usize {
    let total = record_size(view.content.len());
    assert!(buf.len() >= total, "buffer too small for record");

    // Layout:
    // [0..4)   len (placeholder)
    // [4..12)  sequence
    // [12..20) timestamp_ns
    // [20..24) content_len
    // [24..)   content
    // [...]    hash_n
    // [...]    crc

    buf[4..12].copy_from_slice(&sequence.to_le_bytes());
    buf[12..20].copy_from_slice(&view.timestamp_ns.to_le_bytes());
    let cl = view.content.len() as u32;
    buf[20..24].copy_from_slice(&cl.to_le_bytes());
    let content_start = 24;
    let content_end = content_start + view.content.len();
    buf[content_start..content_end].copy_from_slice(view.content);
    let hash_start = content_end;
    let hash_end = hash_start + 32;
    buf[hash_start..hash_end].copy_from_slice(view.hash_n);
    let crc_start = hash_end;
    let crc_end = crc_start + 4;
    buf[0..4].fill(0);
    let crc = crc32c::crc32c(&buf[..crc_start]);
    buf[crc_start..crc_end].copy_from_slice(&crc.to_le_bytes());
    let total_u32 = total as u32;
    buf[0..4].copy_from_slice(&total_u32.to_le_bytes());
    total
}

/// Deserialize a single record from a buffer.
pub fn deserialize_record(buf: &[u8]) -> Result<(Record, usize), String> {
    if buf.len() < MIN_RECORD_SIZE {
        return Err(format!("buffer too short: {} bytes", buf.len()));
    }

    let total = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if total < MIN_RECORD_SIZE {
        return Err(format!("record len too small: {}", total));
    }
    if total > buf.len() {
        return Err(format!("record len {} exceeds buffer size {}", total, buf.len()));
    }

    let sequence = u64::from_le_bytes([
        buf[4], buf[5], buf[6], buf[7],
        buf[8], buf[9], buf[10], buf[11],
    ]);

    let timestamp_ns = u64::from_le_bytes([
        buf[12], buf[13], buf[14], buf[15],
        buf[16], buf[17], buf[18], buf[19],
    ]);

    let content_len = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]) as usize;
    let expected_total = record_size(content_len);
    if total != expected_total {
        return Err(format!(
            "record size mismatch: len field says {}, content_len implies {}",
            total, expected_total
        ));
    }

    let content_start = 24;
    let content_end = content_start + content_len;
    let content = buf[content_start..content_end].to_vec();

    let hash_start = content_end;
    let hash_end = hash_start + 32;
    let mut hash_n = [0u8; 32];
    hash_n.copy_from_slice(&buf[hash_start..hash_end]);

    let crc_start = hash_end;
    let stored_crc = u32::from_le_bytes([
        buf[crc_start], buf[crc_start + 1], buf[crc_start + 2], buf[crc_start + 3],
    ]);

    let mut crc_buf = Vec::with_capacity(crc_start);
    crc_buf.extend_from_slice(&buf[..crc_start]);
    crc_buf[0..4].fill(0);
    let computed_crc = crc32c::crc32c(&crc_buf);

    if stored_crc != computed_crc {
        return Err(format!(
            "CRC mismatch at sequence {}: stored=0x{:08X}, computed=0x{:08X}",
            sequence, stored_crc, computed_crc
        ));
    }

    let id = RecordId::new(0, sequence); // partition_id from header context
    Ok((Record::new(id, timestamp_ns, content, hash_n), total))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Header tests ──────────────────────────────────────────────────

    #[test]
    fn header_round_trip() {
        let hash_init = [0xABu8; 32];
        let header = SegmentHeader::first_segment(hash_init, 0, 0, 1, false, HASH_ALGO_SHA256);

        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut buf, [0u8; 32]);

        let parsed = SegmentHeader::deserialize(&buf).unwrap();
        assert_eq!(parsed.format_version, FORMAT_VERSION);
        assert!(parsed.is_first());
        assert!(!parsed.hash_enabled());
        assert_eq!(parsed.hash_algo, HASH_ALGO_SHA256);
        assert_eq!(parsed.hash_init, hash_init);
        assert_eq!(parsed.base_sequence, 0);
        assert_eq!(parsed.partition_id, 0);
        assert_eq!(parsed.segment_id, 1);
        assert_eq!(parsed.record_format, RECORD_FORMAT_V1);
    }

    #[test]
    fn header_crc_covers_partition_id() {
        let hash_init = [0xABu8; 32];
        let mut header = SegmentHeader::first_segment(hash_init, 0, 0, 1, false, HASH_ALGO_SHA256);

        let mut buf1 = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut buf1, [0u8; 32]);

        header.partition_id = 99;
        let mut buf2 = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut buf2, [0u8; 32]);

        // CRC bytes at 72..76 must differ
        assert_ne!(
            &buf1[72..76], &buf2[72..76],
            "partition_id must be covered by header CRC"
        );
    }

    #[test]
    fn header_crc_covers_hash_algo() {
        let hash_init = [0xABu8; 32];
        let mut header = SegmentHeader::first_segment(hash_init, 0, 0, 1, false, HASH_ALGO_SHA256);

        let mut buf1 = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut buf1, [0u8; 32]);

        header.hash_algo = HASH_ALGO_BLAKE3;
        let mut buf2 = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut buf2, [0u8; 32]);

        assert_ne!(&buf1[72..76], &buf2[72..76],
            "hash_algo must be covered by header CRC");
    }

    #[test]
    fn header_crc_covers_base_sequence() {
        let hash_init = [0xABu8; 32];
        let mut header = SegmentHeader::first_segment(hash_init, 0, 0, 1, false, HASH_ALGO_SHA256);

        let mut buf1 = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut buf1, [0u8; 32]);

        header.base_sequence = 99999;
        let mut buf2 = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut buf2, [0u8; 32]);

        assert_ne!(&buf1[72..76], &buf2[72..76],
            "base_sequence must be covered by header CRC");
    }

    #[test]
    fn header_with_hash_enabled() {
        let hash_init = [0xCDu8; 32];
        let header = SegmentHeader::first_segment(hash_init, 0, 0, 1, true, HASH_ALGO_BLAKE3);

        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut buf, [0u8; 32]);
        let parsed = SegmentHeader::deserialize(&buf).unwrap();
        assert!(parsed.hash_enabled());
        assert_eq!(parsed.hash_algo, HASH_ALGO_BLAKE3);
    }

    #[test]
    fn header_not_first_segment() {
        let hash_init = [0xEFu8; 32];
        let first = SegmentHeader::first_segment(hash_init, 0, 0, 1, true, HASH_ALGO_SHA256);
        let next = first.next_segment(100, 2, [0x42u8; 32]);
        assert!(!next.is_first());
        assert_eq!(next.segment_id, 2);
        assert_eq!(next.base_sequence, 100);
        assert_eq!(next.partition_id, first.partition_id); // inherited
        assert_eq!(next.hash_algo, first.hash_algo); // inherited
        assert_eq!(next.prev_last_hash, [0x42u8; 32]);
    }

    #[test]
    fn header_bad_magic_rejected() {
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        assert!(SegmentHeader::deserialize(&buf).is_err());
    }

    #[test]
    fn header_bad_crc_rejected() {
        let hash_init = [0x11u8; 32];
        let header = SegmentHeader::first_segment(hash_init, 0, 0, 1, false, HASH_ALGO_SHA256);
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        header.serialize(&mut buf, [0u8; 32]);
        buf[10] ^= 0xFF; // corrupt a byte before CRC
        assert!(SegmentHeader::deserialize(&buf).is_err());
    }

    #[test]
    fn header_size_is_128() {
        assert_eq!(SEGMENT_HEADER_SIZE, 128);
    }

    #[test]
    fn header_crc_range_is_72() {
        assert_eq!(HEADER_CRC_END, 72);
    }

    // ── Record tests ──────────────────────────────────────────────────

    #[test]
    fn record_size_computation() {
        assert_eq!(record_size(0), MIN_RECORD_SIZE);
        assert_eq!(record_size(10), MIN_RECORD_SIZE + 10);
    }

    #[test]
    fn record_round_trip() {
        let view = ReadView { record_id: 42, timestamp_ns: 1000, content: b"hello", hash_n: &[0u8; 32] };
        let mut buf = vec![0u8; record_size(5)];
        serialize_record(&mut buf, 99, &view);
        let (record, consumed) = deserialize_record(&buf).unwrap();
        assert_eq!(consumed, record_size(5));
        assert_eq!(record.id.sequence, 99);
        assert_eq!(record.timestamp_ns, 1000);
        assert_eq!(record.content, b"hello");
    }

    #[test]
    fn record_crc_detects_corruption() {
        let view = ReadView { record_id: 1, timestamp_ns: 100, content: b"data", hash_n: &[0u8; 32] };
        let mut buf = vec![0u8; record_size(4)];
        serialize_record(&mut buf, 1, &view);
        buf[24] ^= 0x01;
        assert!(deserialize_record(&buf).is_err());
    }
}

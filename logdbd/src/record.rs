//! Record binary format.
//!
//! logdbd wraps user content with a structured header before writing to logdb.
//! logdb's per-shard hash chain (BLAKE3 keyed) covers the full record —
//! header + user content — providing tamper-evident integrity.
//!
//! # Binary format
//!
//! ```text
//! magic:           u16 LE  = 0x4C52 ("LR")
//! version:         u8      = 1
//! flags:           u8      = reserved
//! namespace_id:    u32 LE
//! stream_id:       u64 LE
//! seq:             u64 LE           -- per-stream, from 1
//! event_type_len:  u16 LE
//! event_type:      UTF-8 [event_type_len]
//! content_type_len: u16 LE
//! content_type:    UTF-8 [content_type_len]
//! metadata_count:  u8
//! metadata:        (key_len:u8, key, value_len:u16, value) * metadata_count
//! timestamp_ns:    u64 LE
//! user_content_len: u32 LE
//! user_content:    bytes [user_content_len]
//! ```

use std::collections::BTreeMap;

/// Record magic bytes ("LR").
pub const RECORD_MAGIC: u16 = 0x4C52;
/// Current record format version.
pub const RECORD_VERSION: u8 = 1;

/// Maximum metadata entries per record.
const MAX_METADATA_COUNT: usize = 16;
/// Maximum total metadata bytes (keys + values).
const MAX_METADATA_BYTES: usize = 4096;

// ── Record type ───────────────────────────────────────────────────────────────

/// A decoded record.
#[derive(Debug, Clone)]
pub struct DecodedRecord {
    pub namespace_id: u32,
    pub stream_id: u64,
    pub seq: u64,
    pub event_type: String,
    pub content_type: String,
    pub metadata: BTreeMap<String, String>,
    pub timestamp_ns: u64,
    pub user_content: Vec<u8>,
}

// ── Encoding ──────────────────────────────────────────────────────────────────

/// Encode a record into bytes for writing to logdb.
///
/// The returned bytes include header + user_content and are what logdb
/// stores and hashes via its per-shard BLAKE3 chain.
pub fn encode_record(
    namespace_id: u32,
    stream_id: u64,
    seq: u64,
    event_type: &str,
    content_type: &str,
    metadata: &BTreeMap<String, String>,
    timestamp_ns: u64,
    user_content: &[u8],
) -> Result<Vec<u8>, RecordError> {
    if metadata.len() > MAX_METADATA_COUNT {
        return Err(RecordError::MetadataTooMany(metadata.len()));
    }
    let mut meta_bytes = 0usize;
    for (k, v) in metadata {
        meta_bytes += k.len() + v.len();
    }
    if meta_bytes > MAX_METADATA_BYTES {
        return Err(RecordError::MetadataTooLarge(meta_bytes));
    }
    if user_content.len() > u32::MAX as usize {
        return Err(RecordError::ContentTooLarge(user_content.len()));
    }

    let header_size: usize = 2
        + 1
        + 1
        + 4
        + 8
        + 8
        + 2
        + event_type.len()
        + 2
        + content_type.len()
        + 1
        + metadata
            .iter()
            .map(|(k, v)| 1 + k.len() + 2 + v.len())
            .sum::<usize>()
        + 8
        + 4;

    let total = header_size + user_content.len();
    let mut buf = Vec::with_capacity(total);

    buf.extend_from_slice(&RECORD_MAGIC.to_le_bytes());
    buf.push(RECORD_VERSION);
    buf.push(0u8); // flags
    buf.extend_from_slice(&namespace_id.to_le_bytes());
    buf.extend_from_slice(&stream_id.to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());

    let et = event_type.as_bytes();
    buf.extend_from_slice(&(et.len() as u16).to_le_bytes());
    buf.extend_from_slice(et);

    let ct = content_type.as_bytes();
    buf.extend_from_slice(&(ct.len() as u16).to_le_bytes());
    buf.extend_from_slice(ct);

    buf.push(metadata.len() as u8);
    for (k, v) in metadata {
        let kb = k.as_bytes();
        buf.push(kb.len() as u8);
        buf.extend_from_slice(kb);
        let vb = v.as_bytes();
        buf.extend_from_slice(&(vb.len() as u16).to_le_bytes());
        buf.extend_from_slice(vb);
    }

    buf.extend_from_slice(&timestamp_ns.to_le_bytes());
    buf.extend_from_slice(&(user_content.len() as u32).to_le_bytes());
    buf.extend_from_slice(user_content);

    Ok(buf)
}

// ── Decoding ──────────────────────────────────────────────────────────────────

/// Decode a record from bytes read back from logdb.
pub fn decode_record(raw: &[u8]) -> Result<DecodedRecord, RecordError> {
    let pos = &mut 0usize;

    let magic = read_u16(raw, pos)?;
    if magic != RECORD_MAGIC {
        return Err(RecordError::BadMagic(magic));
    }

    let version = read_u8(raw, pos)?;
    if version != RECORD_VERSION {
        return Err(RecordError::UnsupportedVersion(version));
    }

    let _flags = read_u8(raw, pos)?;
    let namespace_id = read_u32(raw, pos)?;
    let stream_id = read_u64(raw, pos)?;
    let seq = read_u64(raw, pos)?;

    let et_len = read_u16(raw, pos)? as usize;
    let event_type = read_str(raw, pos, et_len)?.to_string();

    let ct_len = read_u16(raw, pos)? as usize;
    let content_type = read_str(raw, pos, ct_len)?.to_string();

    let meta_count = read_u8(raw, pos)? as usize;
    let mut metadata = BTreeMap::new();
    for _ in 0..meta_count {
        let k_len = read_u8(raw, pos)? as usize;
        let key = read_str(raw, pos, k_len)?.to_string();
        let v_len = read_u16(raw, pos)? as usize;
        let value = read_str(raw, pos, v_len)?.to_string();
        metadata.insert(key, value);
    }

    let timestamp_ns = read_u64(raw, pos)?;
    let content_len = read_u32(raw, pos)? as usize;
    let user_content = read_bytes(raw, pos, content_len)?;

    Ok(DecodedRecord {
        namespace_id,
        stream_id,
        seq,
        event_type,
        content_type,
        metadata,
        timestamp_ns,
        user_content,
    })
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn read_u8(raw: &[u8], pos: &mut usize) -> Result<u8, RecordError> {
    if *pos >= raw.len() {
        return Err(RecordError::Truncated);
    }
    let v = raw[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u16(raw: &[u8], pos: &mut usize) -> Result<u16, RecordError> {
    if *pos + 2 > raw.len() {
        return Err(RecordError::Truncated);
    }
    let v = u16::from_le_bytes([raw[*pos], raw[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

fn read_u32(raw: &[u8], pos: &mut usize) -> Result<u32, RecordError> {
    if *pos + 4 > raw.len() {
        return Err(RecordError::Truncated);
    }
    let v = u32::from_le_bytes([raw[*pos], raw[*pos + 1], raw[*pos + 2], raw[*pos + 3]]);
    *pos += 4;
    Ok(v)
}

fn read_u64(raw: &[u8], pos: &mut usize) -> Result<u64, RecordError> {
    if *pos + 8 > raw.len() {
        return Err(RecordError::Truncated);
    }
    let v = u64::from_le_bytes([
        raw[*pos],
        raw[*pos + 1],
        raw[*pos + 2],
        raw[*pos + 3],
        raw[*pos + 4],
        raw[*pos + 5],
        raw[*pos + 6],
        raw[*pos + 7],
    ]);
    *pos += 8;
    Ok(v)
}

fn read_str<'a>(raw: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a str, RecordError> {
    if *pos + len > raw.len() {
        return Err(RecordError::Truncated);
    }
    let s = std::str::from_utf8(&raw[*pos..*pos + len]).map_err(|_| RecordError::InvalidUtf8)?;
    *pos += len;
    Ok(s)
}

fn read_bytes(raw: &[u8], pos: &mut usize, len: usize) -> Result<Vec<u8>, RecordError> {
    if *pos + len > raw.len() {
        return Err(RecordError::Truncated);
    }
    let v = raw[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(v)
}

// ── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum RecordError {
    Truncated,
    BadMagic(u16),
    UnsupportedVersion(u8),
    InvalidUtf8,
    ContentTooLarge(usize),
    MetadataTooMany(usize),
    MetadataTooLarge(usize),
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "record truncated"),
            Self::BadMagic(m) => write!(f, "bad magic: 0x{:04X}", m),
            Self::UnsupportedVersion(v) => write!(f, "unsupported version: {}", v),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 in record field"),
            Self::ContentTooLarge(s) => write!(f, "content too large: {} bytes", s),
            Self::MetadataTooMany(n) => {
                write!(f, "too many metadata: {} (max {})", n, MAX_METADATA_COUNT)
            }
            Self::MetadataTooLarge(s) => write!(
                f,
                "metadata too large: {} bytes (max {})",
                s, MAX_METADATA_BYTES
            ),
        }
    }
}

impl std::error::Error for RecordError {}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let mut meta = BTreeMap::new();
        meta.insert("model".into(), "claude-sonnet-5".into());

        let encoded = encode_record(
            1,
            42,
            1,
            "llm.call",
            "application/json",
            &meta,
            1000000,
            b"hello world",
        )
        .unwrap();

        let decoded = decode_record(&encoded).unwrap();
        assert_eq!(decoded.namespace_id, 1);
        assert_eq!(decoded.stream_id, 42);
        assert_eq!(decoded.seq, 1);
        assert_eq!(decoded.event_type, "llm.call");
        assert_eq!(decoded.content_type, "application/json");
        assert_eq!(decoded.metadata.get("model").unwrap(), "claude-sonnet-5");
        assert_eq!(decoded.timestamp_ns, 1000000);
        assert_eq!(decoded.user_content, b"hello world");
    }

    #[test]
    fn encode_decode_no_metadata() {
        let encoded =
            encode_record(1, 1, 5, "test", "text/plain", &BTreeMap::new(), 0, b"x").unwrap();
        let decoded = decode_record(&encoded).unwrap();
        assert_eq!(decoded.metadata.len(), 0);
    }

    #[test]
    fn decode_bad_magic() {
        let data = vec![0xFFu8; 64];
        let err = decode_record(&data).unwrap_err();
        assert!(matches!(err, RecordError::BadMagic(_)));
    }

    #[test]
    fn decode_truncated() {
        // 1 byte: read_u16 fails immediately
        let err = decode_record(&[0u8; 1]).unwrap_err();
        assert!(matches!(err, RecordError::Truncated));
    }

    #[test]
    fn reject_too_many_metadata() {
        let mut meta = BTreeMap::new();
        for i in 0..20 {
            meta.insert(format!("k{}", i), "v".into());
        }
        let err = encode_record(1, 1, 1, "t", "text/plain", &meta, 0, b"x").unwrap_err();
        assert!(matches!(err, RecordError::MetadataTooMany(20)));
    }
}

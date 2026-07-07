//! Binary consumer-offset store — replaces the SQLite `consumer_offsets` table.
//!
//! One file `<offsets_dir>/offsets.bin` holds every consumer's committed seq.
//! Format (little-endian):
//!   [magic: b"LDBO" (4B)] [version: u8=1] [count: u32]
//!   per entry: [ns_len:u16][ns][stream_len:u16][stream][group_len:u16][group]
//!              [id_len:u16][id][committed_seq:u64]
//! Writes are atomic (tmp + rename). Loaded once at startup; flushed periodically.

use std::collections::HashMap;

/// (namespace, stream, consumer_group, consumer_id) -> committed seq
pub type OffsetKey = (String, String, String, String);

const MAGIC: &[u8; 4] = b"LDBO";
const VERSION: u8 = 1;

pub fn encode(map: &HashMap<OffsetKey, u64>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + map.len() * 64);
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&(map.len() as u32).to_le_bytes());
    for ((ns, stream, group, id), seq) in map {
        write_str(&mut buf, ns);
        write_str(&mut buf, stream);
        write_str(&mut buf, group);
        write_str(&mut buf, id);
        buf.extend_from_slice(&seq.to_le_bytes());
    }
    buf
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = u16::try_from(bytes.len()).expect("offset string length exceeds u16::MAX");
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

pub fn decode(bytes: &[u8]) -> Result<HashMap<OffsetKey, u64>, OffsetError> {
    if bytes.is_empty() {
        return Ok(HashMap::new());
    }
    if bytes.len() < 9 {
        return Err(OffsetError::UnexpectedEof);
    }
    if &bytes[..4] != MAGIC {
        return Err(OffsetError::BadMagic);
    }
    if bytes[4] != VERSION {
        return Err(OffsetError::UnsupportedVersion(bytes[4]));
    }
    let count = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let mut map = HashMap::with_capacity(count);
    let mut pos = 9;
    for _ in 0..count {
        let (ns, p) = read_str(bytes, pos)?;
        let (stream, p) = read_str(bytes, p)?;
        let (group, p) = read_str(bytes, p)?;
        let (id, p) = read_str(bytes, p)?;
        if p + 8 > bytes.len() {
            return Err(OffsetError::UnexpectedEof);
        }
        let seq = u64::from_le_bytes(bytes[p..p + 8].try_into().unwrap());
        map.insert((ns, stream, group, id), seq);
        pos = p + 8;
    }
    Ok(map)
}

fn read_str(bytes: &[u8], pos: usize) -> Result<(String, usize), OffsetError> {
    if pos + 2 > bytes.len() {
        return Err(OffsetError::UnexpectedEof);
    }
    let len = u16::from_le_bytes(bytes[pos..pos + 2].try_into().unwrap()) as usize;
    let start = pos + 2;
    let end = start + len;
    if end > bytes.len() {
        return Err(OffsetError::UnexpectedEof);
    }
    let s = String::from_utf8(bytes[start..end].to_vec()).map_err(|_| OffsetError::BadUtf8)?;
    Ok((s, end))
}

#[derive(Debug)]
pub enum OffsetError {
    BadMagic,
    UnsupportedVersion(u8),
    UnexpectedEof,
    BadUtf8,
    Io(String),
}

impl std::fmt::Display for OffsetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadMagic => write!(f, "offsets.bin: bad magic"),
            Self::UnsupportedVersion(v) => write!(f, "offsets.bin: unsupported version {}", v),
            Self::UnexpectedEof => write!(f, "offsets.bin: unexpected end of file"),
            Self::BadUtf8 => write!(f, "offsets.bin: invalid utf-8"),
            Self::Io(e) => write!(f, "offsets.bin: io: {}", e),
        }
    }
}
impl std::error::Error for OffsetError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let mut map = HashMap::new();
        map.insert(("ns".into(), "s".into(), "g".into(), "c1".into()), 42u64);
        map.insert(("ns".into(), "s".into(), "g".into(), "c2".into()), 100);
        map.insert(
            ("other".into(), "stream".into(), "g2".into(), "w".into()),
            7,
        );

        let bytes = encode(&map);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn decode_empty_input_returns_empty_map() {
        assert!(decode(b"").unwrap().is_empty());
    }

    #[test]
    fn empty_map_roundtrips() {
        let map = HashMap::new();
        let bytes = encode(&map);
        assert_eq!(decode(&bytes).unwrap(), map);
    }

    #[test]
    fn decode_bad_magic() {
        // 9 bytes, valid header shape, wrong magic
        assert!(matches!(
            decode(b"XXXX\x01\x00\x00\x00\x00"),
            Err(OffsetError::BadMagic)
        ));
    }

    #[test]
    fn decode_wrong_version() {
        assert!(matches!(
            decode(b"LDBO\x02\x00\x00\x00\x00"),
            Err(OffsetError::UnsupportedVersion(2))
        ));
    }

    #[test]
    fn decode_truncated_header_returns_unexpected_eof() {
        // correct magic, but header shorter than 9 bytes
        assert!(matches!(
            decode(b"LDBO\x01"),
            Err(OffsetError::UnexpectedEof)
        ));
    }

    #[test]
    fn decode_truncated_entry_returns_unexpected_eof() {
        // header claims 1 entry, but no entry data follows
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"LDBO");
        bytes.push(1); // version
        bytes.extend_from_slice(&1u32.to_le_bytes()); // count = 1
        assert!(matches!(decode(&bytes), Err(OffsetError::UnexpectedEof)));
    }

    #[test]
    fn decode_bad_utf8() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"LDBO");
        bytes.push(1); // version
        bytes.extend_from_slice(&1u32.to_le_bytes()); // count = 1
        bytes.extend_from_slice(&1u16.to_le_bytes()); // ns_len = 1
        bytes.push(0xFF); // invalid UTF-8 start byte
        assert!(matches!(decode(&bytes), Err(OffsetError::BadUtf8)));
    }

    #[test]
    fn roundtrip_multibyte_utf8_and_empty_fields() {
        let mut map = HashMap::new();
        map.insert(
            ("消费者".into(), "".into(), "grp".into(), "id-1".into()),
            7u64,
        );
        let bytes = encode(&map);
        assert_eq!(decode(&bytes).unwrap(), map);
    }
}

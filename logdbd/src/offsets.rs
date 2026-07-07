//! Binary consumer-offset store — replaces the SQLite `consumer_offsets` table.
//!
//! One file `<offsets_dir>/offsets.bin` holds every consumer's committed seq.
//! Format (little-endian):
//!   [magic: b"LDBO" (4B)] [version: u8=1] [count: u32]
//!   per entry: [ns_len:u16][ns][stream_len:u16][stream][group_len:u16][group]
//!              [id_len:u16][id][committed_seq:u64]
//! Writes are atomic (tmp + rename). Loaded once at startup; flushed periodically.

use std::collections::HashMap;
use std::path::Path;

/// (namespace, stream, consumer_group, consumer_id) -> committed seq
pub type OffsetKey = (String, String, String, String);

const MAGIC: &[u8; 4] = b"LDBO";
const VERSION: u8 = 1;
const FILENAME: &str = "offsets.bin";
const TMP_FILENAME: &str = "offsets.bin.tmp";

pub fn encode(map: &HashMap<OffsetKey, u64>) -> Result<Vec<u8>, OffsetError> {
    let mut buf = Vec::with_capacity(16 + map.len() * 64);
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&(map.len() as u32).to_le_bytes());
    for ((ns, stream, group, id), seq) in map {
        write_str(&mut buf, ns)?;
        write_str(&mut buf, stream)?;
        write_str(&mut buf, group)?;
        write_str(&mut buf, id)?;
        buf.extend_from_slice(&seq.to_le_bytes());
    }
    Ok(buf)
}

fn write_str(buf: &mut Vec<u8>, s: &str) -> Result<(), OffsetError> {
    let bytes = s.as_bytes();
    let len =
        u16::try_from(bytes.len()).map_err(|_| OffsetError::StringTooLong { len: bytes.len() })?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
    Ok(())
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

/// Load all offsets from `<dir>/offsets.bin`. Empty map if the file is absent.
pub fn load(dir: &Path) -> Result<HashMap<OffsetKey, u64>, OffsetError> {
    let path = dir.join(FILENAME);
    match std::fs::read(&path) {
        Ok(bytes) => decode(&bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(e) => Err(OffsetError::Io(e.to_string())),
    }
}

/// Atomically write all offsets to `<dir>/offsets.bin` (write tmp, then rename).
pub fn save(dir: &Path, map: &HashMap<OffsetKey, u64>) -> Result<(), OffsetError> {
    std::fs::create_dir_all(dir).map_err(|e| OffsetError::Io(e.to_string()))?;
    let tmp = dir.join(TMP_FILENAME);
    let final_path = dir.join(FILENAME);
    let bytes = encode(map)?;
    std::fs::write(&tmp, &bytes).map_err(|e| OffsetError::Io(e.to_string()))?;
    std::fs::rename(&tmp, &final_path).map_err(|e| OffsetError::Io(e.to_string()))?;
    Ok(())
}

#[derive(Debug)]
pub enum OffsetError {
    BadMagic,
    UnsupportedVersion(u8),
    UnexpectedEof,
    BadUtf8,
    StringTooLong { len: usize },
    Io(String),
}

impl std::fmt::Display for OffsetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadMagic => write!(f, "offsets.bin: bad magic"),
            Self::UnsupportedVersion(v) => write!(f, "offsets.bin: unsupported version {}", v),
            Self::UnexpectedEof => write!(f, "offsets.bin: unexpected end of file"),
            Self::BadUtf8 => write!(f, "offsets.bin: invalid utf-8"),
            Self::StringTooLong { len } => write!(
                f,
                "offsets.bin: string length {} exceeds u16::MAX (65535)",
                len
            ),
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

        let bytes = encode(&map).unwrap();
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
        let bytes = encode(&map).unwrap();
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
        let bytes = encode(&map).unwrap();
        assert_eq!(decode(&bytes).unwrap(), map);
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut map = HashMap::new();
        map.insert(("ns".into(), "s".into(), "g".into(), "c1".into()), 5u64);
        map.insert(("ns".into(), "s".into(), "g".into(), "c2".into()), 9);

        save(dir.path(), &map).unwrap();
        assert!(dir.path().join("offsets.bin").exists());
        // tmp file must not linger after atomic rename
        assert!(!dir.path().join("offsets.bin.tmp").exists());

        let loaded = load(dir.path()).unwrap();
        assert_eq!(loaded, map);
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = load(dir.path()).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn save_creates_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a/b/c");
        let map = HashMap::new();
        save(&nested, &map).unwrap();
        assert!(nested.join("offsets.bin").exists());
    }

    #[test]
    fn encode_rejects_oversized_string() {
        let mut map = HashMap::new();
        let huge = "x".repeat(70_000);
        map.insert(("ns".into(), huge, "g".into(), "id".into()), 1u64);
        assert!(matches!(
            encode(&map),
            Err(OffsetError::StringTooLong { len: 70_000 })
        ));
    }
}

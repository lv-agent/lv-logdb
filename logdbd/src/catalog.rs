//! Catalog — namespace & stream name → internal ID mapping.
//!
//! The Catalog is an in-memory index persisted to a snapshot file.
//! Namespace and stream names are validated on creation and mapped to
//! compact internal IDs (namespace_id: u32, stream_id: u64).
//!
//! Snapshot format (binary):
//!   magic:            u32 LE  = 0x4341544C ("CATL")
//!   version:          u16 LE  = 1
//!   namespace_count:  u32 LE
//!   for each namespace:
//!     id:              u32 LE
//!     name_len:        u16 LE
//!     name:            UTF-8 [name_len]
//!     stream_count:    u32 LE
//!     for each stream:
//!       id:            u64 LE
//!       name_len:      u16 LE
//!       name:          UTF-8 [name_len]
//!   crc32c:            u32 LE

use crate::config::{validate_namespace_name, validate_stream_name};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{PoisonError, RwLock};

// ── Catalog ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct Catalog {
    /// namespace name → id
    namespaces: RwLock<HashMap<String, u32>>,
    /// namespace_id → name
    namespace_names: RwLock<HashMap<u32, String>>,
    /// (namespace_id, stream_name) → stream_id
    streams: RwLock<HashMap<(u32, String), u64>>,
    /// stream_id → (namespace_id, name)
    stream_info: RwLock<HashMap<u64, (u32, String)>>,

    /// Next namespace id counter
    next_ns_id: RwLock<u32>,
    /// Next stream id counter
    next_stream_id: RwLock<u64>,

    /// Snapshot file path
    snapshot_path: PathBuf,
}

const SNAPSHOT_MAGIC: u32 = 0x4341544C; // "CATL"
const SNAPSHOT_VERSION: u16 = 1;

impl Catalog {
    /// Open the catalog, loading from snapshot if it exists.
    pub fn open(data_dir: &Path) -> Result<Self, CatalogError> {
        let snapshot_path = data_dir.join("catalog.dat");
        let mut cat = Self {
            namespaces: RwLock::new(HashMap::new()),
            namespace_names: RwLock::new(HashMap::new()),
            streams: RwLock::new(HashMap::new()),
            stream_info: RwLock::new(HashMap::new()),
            next_ns_id: RwLock::new(1),
            next_stream_id: RwLock::new(1),
            snapshot_path,
        };

        if cat.snapshot_path.exists() {
            cat.load_snapshot()?;
        }

        Ok(cat)
    }

    /// Resolve namespace + stream → (namespace_id, stream_id).
    /// Auto-creates namespace and stream if they don't exist.
    /// Persists catalog snapshot after each creation (P0-2 fix).
    pub fn resolve(&self, ns: &str, stream: &str) -> Result<(u32, u64), CatalogError> {
        validate_namespace_name(ns).map_err(|e| CatalogError::InvalidName(e))?;
        validate_stream_name(stream).map_err(|e| CatalogError::InvalidName(e))?;

        let mut created = false;

        // Lookup or create namespace
        let ns_id = {
            let ns_map = self
                .namespaces
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(&id) = ns_map.get(ns) {
                id
            } else {
                drop(ns_map);
                let mut ns_map = self
                    .namespaces
                    .write()
                    .unwrap_or_else(PoisonError::into_inner);
                if let Some(&id) = ns_map.get(ns) {
                    id
                } else {
                    let id = {
                        let mut next = self
                            .next_ns_id
                            .write()
                            .unwrap_or_else(PoisonError::into_inner);
                        let id = *next;
                        *next += 1;
                        id
                    };
                    ns_map.insert(ns.to_string(), id);
                    self.namespace_names
                        .write()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(id, ns.to_string());
                    created = true;
                    id
                }
            }
        };

        // Lookup or create stream
        let stream_id = {
            let key = (ns_id, stream.to_string());
            let stream_map = self.streams.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(&id) = stream_map.get(&key) {
                id
            } else {
                drop(stream_map);
                let mut stream_map = self.streams.write().unwrap_or_else(PoisonError::into_inner);
                if let Some(&id) = stream_map.get(&key) {
                    id
                } else {
                    let id = {
                        let mut next = self
                            .next_stream_id
                            .write()
                            .unwrap_or_else(PoisonError::into_inner);
                        let id = *next;
                        *next += 1;
                        id
                    };
                    stream_map.insert(key.clone(), id);
                    self.stream_info
                        .write()
                        .unwrap_or_else(PoisonError::into_inner)
                        .insert(id, (ns_id, stream.to_string()));
                    created = true;
                    id
                }
            }
        };

        if created {
            if let Err(e) = self.save_snapshot() {
                tracing::error!(error = %e, "failed to persist catalog snapshot after creation");
            }
        }

        Ok((ns_id, stream_id))
    }

    /// Lookup namespace name by id.
    pub fn namespace_name(&self, id: u32) -> Option<String> {
        self.namespace_names
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&id)
            .cloned()
    }

    /// Lookup stream info by id → (namespace_id, name).
    pub fn stream_info_by_id(&self, id: u64) -> Option<(u32, String)> {
        self.stream_info
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&id)
            .cloned()
    }

    /// List all namespaces (excluding _system).
    pub fn list_namespaces(&self) -> Vec<NamespaceSummary> {
        let ns_map = self
            .namespaces
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let stream_map = self.streams.read().unwrap_or_else(PoisonError::into_inner);
        ns_map
            .iter()
            .filter(|(name, _)| !name.starts_with('_'))
            .map(|(name, &id)| {
                let count = stream_map.keys().filter(|(nid, _)| *nid == id).count() as u64;
                NamespaceSummary {
                    name: name.clone(),
                    id,
                    stream_count: count,
                }
            })
            .collect()
    }

    /// List all streams in a namespace.
    pub fn list_streams(&self, ns: &str) -> Result<Vec<StreamSummary>, CatalogError> {
        let ns_map = self
            .namespaces
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let ns_id = ns_map
            .get(ns)
            .ok_or_else(|| CatalogError::NamespaceNotFound(ns.to_string()))?;

        let stream_map = self.streams.read().unwrap_or_else(PoisonError::into_inner);
        let streams: Vec<StreamSummary> = stream_map
            .iter()
            .filter(|((nid, _), _)| *nid == *ns_id)
            .map(|((_, name), &id)| StreamSummary {
                name: name.clone(),
                id,
                // seq/record_count filled later when segment storage is available
                first_seq: 0,
                durable_seq: 0,
                record_count: 0,
            })
            .collect();

        Ok(streams)
    }

    /// Serialize the in-memory catalog to bytes.
    fn serialize(
        ns_map: &HashMap<String, u32>,
        stream_map: &HashMap<(u32, String), u64>,
    ) -> Result<Vec<u8>, CatalogError> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
        buf.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        buf.extend_from_slice(&(ns_map.len() as u32).to_le_bytes());

        for (ns_name, &ns_id) in ns_map.iter() {
            buf.extend_from_slice(&ns_id.to_le_bytes());
            let ns_bytes = ns_name.as_bytes();
            buf.extend_from_slice(&(ns_bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(ns_bytes);

            let ns_streams: Vec<(&String, &u64)> = stream_map
                .iter()
                .filter(|((nid, _), _)| *nid == ns_id)
                .map(|((_, name), id)| (name, id))
                .collect();

            buf.extend_from_slice(&(ns_streams.len() as u32).to_le_bytes());
            for (sname, sid) in &ns_streams {
                buf.extend_from_slice(&sid.to_le_bytes());
                let s_bytes = sname.as_bytes();
                buf.extend_from_slice(&(s_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(s_bytes);
            }
        }

        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        Ok(buf)
    }

    /// Persist catalog to snapshot file (atomic write).
    /// Serializes under locks, releases before I/O (P1-10 fix).
    pub fn save_snapshot(&self) -> Result<(), CatalogError> {
        // Serialize under locks
        let buf = {
            let ns_map = self
                .namespaces
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            let stream_map = self.streams.read().unwrap_or_else(PoisonError::into_inner);
            let buf = Self::serialize(&ns_map, &stream_map)?;
            buf
        }; // locks dropped

        // Blocking I/O without holding catalog locks
        let tmp = self.snapshot_path.with_extension("tmp");
        let mut f = std::fs::File::create(&tmp).map_err(|e| CatalogError::Io(e))?;
        f.write_all(&buf).map_err(|e| CatalogError::Io(e))?;
        f.sync_all().map_err(|e| CatalogError::Io(e))?;
        drop(f);
        std::fs::rename(&tmp, &self.snapshot_path).map_err(|e| CatalogError::Io(e))?;

        Ok(())
    }

    fn load_snapshot(&mut self) -> Result<(), CatalogError> {
        let data = std::fs::read(&self.snapshot_path).map_err(|e| CatalogError::Io(e))?;

        if data.len() < 10 {
            return Err(CatalogError::Corrupted("file too short".into()));
        }

        // Verify CRC
        let crc_offset = data.len() - 4;
        let stored_crc = u32::from_le_bytes([
            data[crc_offset],
            data[crc_offset + 1],
            data[crc_offset + 2],
            data[crc_offset + 3],
        ]);
        let computed_crc = crc32c::crc32c(&data[..crc_offset]);
        if stored_crc != computed_crc {
            return Err(CatalogError::Corrupted("CRC mismatch".into()));
        }

        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if magic != SNAPSHOT_MAGIC {
            return Err(CatalogError::Corrupted("bad magic".into()));
        }

        let version = u16::from_le_bytes([data[4], data[5]]);
        if version != SNAPSHOT_VERSION {
            return Err(CatalogError::Corrupted(format!(
                "unsupported version {}",
                version
            )));
        }

        let mut pos = 6;
        let ns_count = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        let mut max_ns_id: u32 = 0;
        let mut max_stream_id: u64 = 0;

        for _ in 0..ns_count {
            if pos + 4 > crc_offset {
                return Err(CatalogError::Corrupted("truncated".into()));
            }
            let ns_id =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;
            max_ns_id = max_ns_id.max(ns_id);

            if pos + 2 > crc_offset {
                return Err(CatalogError::Corrupted("truncated".into()));
            }
            let name_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;

            if pos + name_len > crc_offset {
                return Err(CatalogError::Corrupted("truncated".into()));
            }
            let name = String::from_utf8(data[pos..pos + name_len].to_vec())
                .map_err(|_| CatalogError::Corrupted("invalid UTF-8".into()))?;
            pos += name_len;

            if pos + 4 > crc_offset {
                return Err(CatalogError::Corrupted("truncated".into()));
            }
            let stream_count =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;

            self.namespaces
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(name.clone(), ns_id);
            self.namespace_names
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(ns_id, name);

            for _ in 0..stream_count {
                if pos + 8 > crc_offset {
                    return Err(CatalogError::Corrupted("truncated".into()));
                }
                let stream_id = u64::from_le_bytes([
                    data[pos],
                    data[pos + 1],
                    data[pos + 2],
                    data[pos + 3],
                    data[pos + 4],
                    data[pos + 5],
                    data[pos + 6],
                    data[pos + 7],
                ]);
                pos += 8;
                max_stream_id = max_stream_id.max(stream_id);

                if pos + 2 > crc_offset {
                    return Err(CatalogError::Corrupted("truncated".into()));
                }
                let sname_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;

                if pos + sname_len > crc_offset {
                    return Err(CatalogError::Corrupted("truncated".into()));
                }
                let sname = String::from_utf8(data[pos..pos + sname_len].to_vec())
                    .map_err(|_| CatalogError::Corrupted("invalid UTF-8".into()))?;
                pos += sname_len;

                let key = (ns_id, sname.clone());
                self.streams
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(key, stream_id);
                self.stream_info
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(stream_id, (ns_id, sname));
            }
        }

        *self
            .next_ns_id
            .write()
            .unwrap_or_else(PoisonError::into_inner) = max_ns_id + 1;
        *self
            .next_stream_id
            .write()
            .unwrap_or_else(PoisonError::into_inner) = max_stream_id + 1;

        Ok(())
    }
}

// ── Public types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct NamespaceSummary {
    pub name: String,
    pub id: u32,
    pub stream_count: u64,
}

#[derive(Debug, Clone)]
pub struct StreamSummary {
    pub name: String,
    pub id: u64,
    pub first_seq: u64,
    pub durable_seq: u64,
    pub record_count: u64,
}

// ── Error ────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum CatalogError {
    Io(std::io::Error),
    Corrupted(String),
    InvalidName(String),
    NamespaceNotFound(String),
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "catalog I/O error: {}", e),
            Self::Corrupted(s) => write!(f, "catalog snapshot corrupted: {}", s),
            Self::InvalidName(s) => write!(f, "invalid name: {}", s),
            Self::NamespaceNotFound(s) => write!(f, "namespace '{}' not found", s),
        }
    }
}

impl std::error::Error for CatalogError {}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_creates_namespace_and_stream() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).unwrap();

        let (ns_id, stream_id) = cat.resolve("org-a", "user-1/session-abc").unwrap();
        assert_eq!(ns_id, 1);
        assert_eq!(stream_id, 1);

        // Second resolve returns same IDs
        let (ns_id2, stream_id2) = cat.resolve("org-a", "user-1/session-abc").unwrap();
        assert_eq!(ns_id2, ns_id);
        assert_eq!(stream_id2, stream_id);
    }

    #[test]
    fn resolve_different_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).unwrap();

        let (a_id, _) = cat.resolve("org-a", "stream-1").unwrap();
        let (b_id, _) = cat.resolve("org-b", "stream-1").unwrap();
        assert_ne!(a_id, b_id, "different namespaces get different IDs");
    }

    #[test]
    fn resolve_different_streams_same_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).unwrap();

        let (ns_id, s1) = cat.resolve("org-a", "stream-1").unwrap();
        let (ns_id2, s2) = cat.resolve("org-a", "stream-2").unwrap();
        assert_eq!(ns_id, ns_id2, "same namespace ID");
        assert_ne!(s1, s2, "different stream IDs");
    }

    #[test]
    fn list_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).unwrap();

        cat.resolve("org-a", "s1").unwrap();
        cat.resolve("org-a", "s2").unwrap();
        cat.resolve("org-b", "s1").unwrap();

        let list = cat.list_namespaces();
        assert_eq!(list.len(), 2);

        let org_a = list.iter().find(|n| n.name == "org-a").unwrap();
        assert_eq!(org_a.stream_count, 2);

        let org_b = list.iter().find(|n| n.name == "org-b").unwrap();
        assert_eq!(org_b.stream_count, 1);
    }

    #[test]
    fn list_namespaces_excludes_system() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).unwrap();

        cat.resolve("org-a", "s1").unwrap();
        // _system namespace would be hidden
        // (cannot create via resolve due to name validation)

        let list = cat.list_namespaces();
        assert!(list.iter().all(|n| !n.name.starts_with('_')));
    }

    #[test]
    fn list_streams() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).unwrap();

        cat.resolve("org-a", "user-1/session-a").unwrap();
        cat.resolve("org-a", "user-1/session-b").unwrap();

        let streams = cat.list_streams("org-a").unwrap();
        assert_eq!(streams.len(), 2);
        assert!(streams.iter().any(|s| s.name == "user-1/session-a"));
        assert!(streams.iter().any(|s| s.name == "user-1/session-b"));
    }

    #[test]
    fn list_streams_namespace_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).unwrap();
        let err = cat.list_streams("nonexistent").unwrap_err();
        assert!(matches!(err, CatalogError::NamespaceNotFound(_)));
    }

    #[test]
    fn reject_invalid_namespace_name() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).unwrap();
        let err = cat.resolve("BadName", "s1").unwrap_err();
        assert!(matches!(err, CatalogError::InvalidName(_)));
    }

    #[test]
    fn reject_invalid_stream_name() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::open(dir.path()).unwrap();
        let err = cat.resolve("org-a", "//bad-stream").unwrap_err();
        assert!(matches!(err, CatalogError::InvalidName(_)));
    }

    #[test]
    fn snapshot_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        // Create and populate
        {
            let cat = Catalog::open(dir.path()).unwrap();
            cat.resolve("org-a", "s1").unwrap();
            cat.resolve("org-a", "s2").unwrap();
            cat.resolve("org-b", "s3").unwrap();
            cat.save_snapshot().unwrap();
        }

        // Reopen and verify
        {
            let cat = Catalog::open(dir.path()).unwrap();
            let list = cat.list_namespaces();
            assert_eq!(list.len(), 2);

            let (ns_id, stream_id) = cat.resolve("org-a", "s1").unwrap();
            assert_eq!(ns_id, 1);
            assert_eq!(stream_id, 1);

            let streams = cat.list_streams("org-a").unwrap();
            assert_eq!(streams.len(), 2);

            // New IDs should continue after the restored max
            let (_, new_id) = cat.resolve("org-c", "new-stream").unwrap();
            assert!(new_id > 3, "new stream ID should exceed restored max");
        }
    }

    #[test]
    fn snapshot_corrupted_detected() {
        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("catalog.dat");

        // Write garbage
        std::fs::write(&snap_path, &[0xFFu8; 16]).unwrap();

        let err = Catalog::open(dir.path()).unwrap_err();
        assert!(matches!(err, CatalogError::Corrupted(_)));
    }
}

//! Storage layer — wraps logdb::LogDb with logdbd record encoding.
//!
//! Manages the mapping between per-stream seq and logdb's internal gid.
//! Records are encoded with logdbd headers before being written to logdb.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::record::{self, DecodedRecord};

// ── Storage ───────────────────────────────────────────────────────────────────

/// Wraps a logdb instance and manages per-stream seq → gid mapping.
pub struct Storage {
    db: Arc<logdb::LogDb>,
    /// Per-stream mapping: stream_id → (stream_seq → gid)
    seq_map: RwLock<HashMap<u64, BTreeMap<u64, u64>>>,
    /// Per-stream next seq counter
    next_seqs: RwLock<HashMap<u64, u64>>,
    /// Replicated gid cursor (updated by replication module)
    replicated_seq: AtomicU64,
    num_shards: usize,
}

impl Storage {
    /// Create storage wrapping an existing logdb instance.
    /// Rebuilds per-stream seq→gid mapping from existing durable records
    /// so point reads work after restart (P0-1 fix).
    pub fn new(db: logdb::LogDb, num_shards: usize) -> Self {
        let storage = Self {
            db: Arc::new(db),
            seq_map: RwLock::new(HashMap::new()),
            next_seqs: RwLock::new(HashMap::new()),
            replicated_seq: AtomicU64::new(0),
            num_shards,
        };
        storage.rebuild_mapping();
        storage
    }

    /// Scan all durable records and rebuild seq→gid + next_seq state.
    fn rebuild_mapping(&self) {
        let durable = self.db.durable_cursor();
        if durable == 0 {
            return;
        }
        match self.db.scan(0, u64::MAX) {
            Ok(iter) => {
                let mut map: HashMap<u64, BTreeMap<u64, u64>> = HashMap::new();
                let mut nexts: HashMap<u64, u64> = HashMap::new();
                for r in iter {
                    if let Ok(rec) = r {
                        if let Ok(decoded) = crate::record::decode_record(&rec.content) {
                            let sid = decoded.stream_id;
                            map.entry(sid)
                                .or_insert_with(BTreeMap::new)
                                .insert(decoded.seq, rec.id.sequence);
                            let cur = nexts.entry(sid).or_insert(1);
                            if decoded.seq >= *cur {
                                *cur = decoded.seq + 1;
                            }
                        }
                    }
                }
                tracing::info!(
                    streams = map.len(),
                    total_records = map.values().map(|m| m.len() as u64).sum::<u64>(),
                    "rebuilt per-stream seq→gid mapping from logdb"
                );
                *self.seq_map.write().unwrap_or_else(std::sync::PoisonError::into_inner) = map;
                *self.next_seqs.write().unwrap_or_else(std::sync::PoisonError::into_inner) = nexts;
            }
            Err(e) => {
                tracing::warn!(error = ?e, "failed to rebuild seq→gid mapping on startup; point reads may return None until new data is appended");
            }
        }
    }

    /// Get a clone of the inner Arc<LogDb> for direct use (health probes, replication, etc.)
    pub fn db_arc(&self) -> Arc<logdb::LogDb> {
        Arc::clone(&self.db)
    }

    /// Append a record and return its (gid, stream_seq).
    pub fn append(
        &self,
        namespace_id: u32,
        stream_id: u64,
        event_type: &str,
        content_type: &str,
        metadata: &BTreeMap<String, String>,
        timestamp_ns: u64,
        user_content: &[u8],
    ) -> Result<AppendResult, StorageError> {
        // Allocate next per-stream seq
        let seq = {
            let mut nexts = self.next_seqs.write().unwrap_or_else(std::sync::PoisonError::into_inner);
            let next = nexts.entry(stream_id).or_insert(1);
            let s = *next;
            *next += 1;
            s
        };

        // Encode record
        let encoded = record::encode_record(
            namespace_id, stream_id, seq,
            event_type, content_type, metadata,
            timestamp_ns, user_content,
        ).map_err(StorageError::Record)?;

        // Append to logdb
        let gid = self.db.append(&encoded)
            .map_err(|e| StorageError::LogDb(format!("append: {:?}", e)))?;

        // Store mapping
        {
            let mut map = self.seq_map.write().unwrap_or_else(std::sync::PoisonError::into_inner);
            map.entry(stream_id)
                .or_insert_with(BTreeMap::new)
                .insert(seq, gid);
        }

        Ok(AppendResult { gid, stream_seq: seq })
    }

    /// Force durable (fsync).
    pub fn flush(&self) -> Result<(), StorageError> {
        self.db.flush().map_err(|e| StorageError::LogDb(format!("flush: {:?}", e)))
    }

    /// Read a record by (stream_id, stream_seq).
    pub fn read(&self, stream_id: u64, stream_seq: u64) -> Result<Option<DecodedRecord>, StorageError> {
        let gid = {
            let map = self.seq_map.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            map.get(&stream_id)
                .and_then(|m| m.get(&stream_seq).copied())
        };
        match gid {
            None => Ok(None),
            Some(gid) => {
                let raw = self.db.read(gid)
                    .map_err(|e| StorageError::LogDb(format!("read: {:?}", e)))?;
                match raw {
                    None => Ok(None),
                    Some(rec) => Ok(Some(record::decode_record(&rec.content)?)),
                }
            }
        }
    }

    /// Scan records in gid range, decoding each.
    pub fn scan(&self, from_gid: u64, to_gid: u64) -> Result<Vec<DecodedRecord>, StorageError> {
        let iter = self.db.scan(from_gid, to_gid)
            .map_err(|e| StorageError::LogDb(format!("scan: {:?}", e)))?;
        let mut results = Vec::new();
        for r in iter {
            let rec = r.map_err(|e| StorageError::LogDb(format!("scan iter: {:?}", e)))?;
            results.push(record::decode_record(&rec.content)?);
        }
        Ok(results)
    }

    /// Get durable cursor (gid space).
    pub fn durable_gid(&self) -> u64 {
        self.db.durable_cursor()
    }

    /// Checkpoint at gid.
    pub fn checkpoint(&self, gid: u64) {
        self.db.checkpoint(gid);
    }

    /// Number of shards.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Replicated gid cursor (high water mark of standby acks).
    pub fn replicated_gid(&self) -> u64 {
        self.replicated_seq.load(Ordering::Acquire)
    }

    /// Advance the replicated cursor (called by replication module).
    pub fn advance_replicated(&self, gid: u64) {
        let mut cur = self.replicated_seq.load(Ordering::Acquire);
        while gid > cur {
            match self.replicated_seq.compare_exchange_weak(
                cur, gid, Ordering::Release, Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    /// Replicate a record from primary — write at the exact gid, rebuild mapping.
    ///
    /// Used by standby nodes. The raw bytes are the encoded logdbd record
    /// (header + user_content). After writing to logdb, the header is decoded
    /// to restore the per-stream seq→gid mapping so Read/Scan/Tail work.
    pub fn replicate(
        &self,
        gid: u64,
        timestamp_ns: u64,
        raw_content: &[u8],
    ) -> Result<(), StorageError> {
        // Write to logdb at the exact gid
        self.db.replicate(gid, timestamp_ns, raw_content)
            .map_err(|e| StorageError::LogDb(format!("replicate: {:?}", e)))?;

        // Decode header to rebuild seq→gid mapping
        let decoded = record::decode_record(raw_content)
            .map_err(|e| StorageError::Record(e))?;

        let mut map = self.seq_map.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        map.entry(decoded.stream_id)
            .or_insert_with(BTreeMap::new)
            .insert(decoded.seq, gid);
        // Update next_seq if needed
        let mut nexts = self.next_seqs.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let cur = nexts.entry(decoded.stream_id).or_insert(1);
        if decoded.seq >= *cur {
            *cur = decoded.seq + 1;
        }

        Ok(())
    }
}

// ── Types ─────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct AppendResult {
    pub gid: u64,
    pub stream_seq: u64,
}

#[derive(Debug)]
pub enum StorageError {
    Record(record::RecordError),
    LogDb(String),
}

impl From<record::RecordError> for StorageError {
    fn from(e: record::RecordError) -> Self {
        Self::Record(e)
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Record(e) => write!(f, "record: {}", e),
            Self::LogDb(e) => write!(f, "logdb: {}", e),
        }
    }
}

impl std::error::Error for StorageError {}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_storage() -> (Storage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = logdb::Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 256;
        config.durability_mode = logdb::DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        config.shards = 1;
        let db = logdb::LogDb::open(config).unwrap();
        (Storage::new(db, 1), dir)
    }

    #[test]
    fn append_and_read_single_stream() {
        let (st, _dir) = test_storage();
        let mut meta = BTreeMap::new();
        meta.insert("model".into(), "test".into());

        let r1 = st.append(1, 42, "llm.call", "application/json", &meta, 1000, b"hello").unwrap();
        let r2 = st.append(1, 42, "tool.call", "application/json", &BTreeMap::new(), 2000, b"world").unwrap();

        assert_eq!(r1.stream_seq, 1);
        assert_eq!(r2.stream_seq, 2);
        assert_ne!(r1.gid, r2.gid);

        st.flush().unwrap();
        // Wait for durable
        for _ in 0..50 {
            if st.durable_gid() >= r2.gid + 1 { break; }
            std::thread::sleep(Duration::from_millis(20));
        }

        let rec1 = st.read(42, 1).unwrap().unwrap();
        assert_eq!(rec1.namespace_id, 1);
        assert_eq!(rec1.stream_id, 42);
        assert_eq!(rec1.seq, 1);
        assert_eq!(rec1.event_type, "llm.call");
        assert_eq!(rec1.user_content, b"hello");

        let rec2 = st.read(42, 2).unwrap().unwrap();
        assert_eq!(rec2.seq, 2);
        assert_eq!(rec2.event_type, "tool.call");
    }

    #[test]
    fn scan_decodes_records() {
        let (st, _dir) = test_storage();
        for i in 0..5u64 {
            st.append(1, 1, "test", "text/plain", &BTreeMap::new(), i, format!("r-{}", i).as_bytes()).unwrap();
        }
        st.flush().unwrap();
        for _ in 0..50 {
            if st.durable_gid() >= 5 { break; }
            std::thread::sleep(Duration::from_millis(20));
        }

        let results = st.scan(0, u64::MAX).unwrap();
        assert_eq!(results.len(), 5);
        for (i, r) in results.iter().enumerate() {
            assert_eq!(r.seq, i as u64 + 1);
        }
    }
}

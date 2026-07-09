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
    /// Shard bits used to decode gid → shard_id (derived from num_shards).
    shard_bits: u32,
}

impl Storage {
    /// Create storage wrapping an existing logdb instance.
    /// Rebuilds per-stream seq→gid mapping from existing durable records
    /// so point reads work after restart (P0-1 fix).
    //
    // TODO(perf): checkpoint seq_map for fast startup.  Currently
    // `rebuild_mapping()` scans every durable record — O(N) startup
    // cost.  Persist seq_map to a binary checkpoint file alongside the
    // data dir; on startup load the checkpoint + replay only records
    // whose per-shard local-seq is beyond the checkpoint.  Needs
    // per-shard `last_seq_at_checkpoint` metadata.
    pub fn new(db: logdb::LogDb, num_shards: usize) -> Self {
        let storage = Self {
            db: Arc::new(db),
            seq_map: RwLock::new(HashMap::new()),
            next_seqs: RwLock::new(HashMap::new()),
            replicated_seq: AtomicU64::new(0),
            num_shards,
            shard_bits: logdb::shard_bits(num_shards),
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
                *self
                    .seq_map
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = map;
                *self
                    .next_seqs
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = nexts;
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
    ///
    /// `shard_key`: when `Some`, route deterministically by key (same key ⇒
    /// same shard) via [`logdb::LogDb::append_with_key`]; when `None`, fall
    /// back to legacy thread-affine routing. The broker (cr-037) sets a key so
    /// a consumer group can shard work by entity.
    pub fn append(
        &self,
        namespace_id: u32,
        stream_id: u64,
        event_type: &str,
        content_type: &str,
        metadata: &BTreeMap<String, String>,
        timestamp_ns: u64,
        user_content: &[u8],
        shard_key: Option<&str>,
    ) -> Result<AppendResult, StorageError> {
        // Allocate next per-stream seq
        let seq = {
            let mut nexts = self
                .next_seqs
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let next = nexts.entry(stream_id).or_insert(1);
            let s = *next;
            *next += 1;
            s
        };

        // Encode record
        let encoded = record::encode_record(
            namespace_id,
            stream_id,
            seq,
            event_type,
            content_type,
            metadata,
            timestamp_ns,
            user_content,
        )
        .map_err(StorageError::Record)?;

        // Append to logdb — key-routed when a shard_key is supplied.
        let gid = match shard_key {
            Some(key) => self
                .db
                .append_with_key(&encoded, key.as_bytes())
                .map_err(|e| StorageError::LogDb(format!("append: {:?}", e)))?,
            None => self
                .db
                .append(&encoded)
                .map_err(|e| StorageError::LogDb(format!("append: {:?}", e)))?,
        };

        // Store mapping
        {
            let mut map = self
                .seq_map
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.entry(stream_id)
                .or_insert_with(BTreeMap::new)
                .insert(seq, gid);
        }

        Ok(AppendResult {
            gid,
            stream_seq: seq,
        })
    }

    /// Force durable (fsync).
    pub fn flush(&self) -> Result<(), StorageError> {
        self.db
            .flush()
            .map_err(|e| StorageError::LogDb(format!("flush: {:?}", e)))
    }

    /// Read a record by (stream_id, stream_seq).
    pub fn read(
        &self,
        stream_id: u64,
        stream_seq: u64,
    ) -> Result<Option<DecodedRecord>, StorageError> {
        let gid = {
            let map = self
                .seq_map
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.get(&stream_id)
                .and_then(|m| m.get(&stream_seq).copied())
        };
        match gid {
            None => Ok(None),
            Some(gid) => {
                let raw = self
                    .db
                    .read(gid)
                    .map_err(|e| StorageError::LogDb(format!("read: {:?}", e)))?;
                match raw {
                    None => Ok(None),
                    Some(rec) => {
                        let mut decoded = record::decode_record(&rec.content)?;
                        decoded.shard_id =
                            logdb::decode_record_id(gid, self.shard_bits).0 as u32;
                        Ok(Some(decoded))
                    }
                }
            }
        }
    }

    /// Scan records in gid range, decoding each.
    pub fn scan(&self, from_gid: u64, to_gid: u64) -> Result<Vec<DecodedRecord>, StorageError> {
        let iter = self
            .db
            .scan(from_gid, to_gid)
            .map_err(|e| StorageError::LogDb(format!("scan: {:?}", e)))?;
        let mut results = Vec::new();
        for r in iter {
            let rec = r.map_err(|e| StorageError::LogDb(format!("scan iter: {:?}", e)))?;
            let gid = rec.id.sequence;
            let mut decoded = record::decode_record(&rec.content)?;
            decoded.shard_id = logdb::decode_record_id(gid, self.shard_bits).0 as u32;
            results.push(decoded);
        }
        Ok(results)
    }

    /// Get durable cursor (gid space).
    pub fn durable_gid(&self) -> u64 {
        self.db.durable_cursor()
    }

    // ── seq-map checkpoint (fast startup) ────────────────────────────────────

    /// Persist the current seq_map + per-shard cursors to `path` (atomic
    /// tmp+rename). Call periodically so the next startup can skip a full scan.
    pub fn write_seq_map_checkpoint(&self, path: &std::path::Path) -> Result<(), StorageError> {
        use std::io::Write;
        let map = self.seq_map.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let nexts = self.next_seqs.read().unwrap_or_else(std::sync::PoisonError::into_inner);

        // Compute per-shard next-to-scan (= max local_seq + 1) from every gid.
        let mut shard_max: Vec<u64> = vec![0u64; self.num_shards];
        for stream_map in map.values() {
            for &gid in stream_map.values() {
                let (shard, local) = logdb::decode_record_id(gid, self.shard_bits);
                if local + 1 > shard_max[shard] {
                    shard_max[shard] = local + 1;
                }
            }
        }

        let tmp = path.with_extension("tmp");
        let mut f = std::fs::File::create(&tmp)
            .map_err(|e| StorageError::LogDb(format!("create checkpoint: {e}")))?;

        f.write_all(b"LDSC").unwrap();
        f.write_all(&1u32.to_le_bytes()).unwrap(); // version
        f.write_all(&(self.num_shards as u32).to_le_bytes()).unwrap();
        for &m in &shard_max {
            f.write_all(&m.to_le_bytes()).unwrap();
        }
        f.write_all(&(map.len() as u64).to_le_bytes()).unwrap();
        for (&stream_id, stream_map) in map.iter() {
            f.write_all(&stream_id.to_le_bytes()).unwrap();
            f.write_all(&(stream_map.len() as u64).to_le_bytes()).unwrap();
            for (&seq, &gid) in stream_map {
                f.write_all(&seq.to_le_bytes()).unwrap();
                f.write_all(&gid.to_le_bytes()).unwrap();
            }
        }
        f.write_all(&(nexts.len() as u64).to_le_bytes()).unwrap();
        for (&stream_id, &next_seq) in nexts.iter() {
            f.write_all(&stream_id.to_le_bytes()).unwrap();
            f.write_all(&next_seq.to_le_bytes()).unwrap();
        }
        f.flush().map_err(|e| StorageError::LogDb(format!("flush checkpoint: {e}")))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| StorageError::LogDb(format!("rename checkpoint: {e}")))?;
        Ok(())
    }

    /// Try to construct `Storage` from a checkpoint, then incrementally
    /// per-shard scan for records after it. On failure the `LogDb` is dropped
    /// — the caller opens a fresh one and falls back to [`Storage::new`].
    pub fn try_new_from_checkpoint(
        db: logdb::LogDb,
        num_shards: usize,
        checkpoint_path: &std::path::Path,
    ) -> Result<Self, StorageError> {
        let shard_bits = logdb::shard_bits(num_shards);
        let raw = std::fs::read(checkpoint_path)
            .map_err(|e| StorageError::LogDb(format!("read checkpoint: {e}")))?;
        if raw.len() < 20 || &raw[0..4] != b"LDSC" {
            return Err( StorageError::LogDb("bad checkpoint magic".into()));
        }
        let ver = u32::from_le_bytes(raw[4..8].try_into().unwrap());
        if ver != 1 {
            return Err( StorageError::LogDb("unknown checkpoint version".into()));
        }
        let mut pos = 8usize;
        let n_shards = u32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if n_shards != num_shards {
            return Err( StorageError::LogDb("shard count mismatch".into()));
        }
        let mut shard_cursors: Vec<u64> = Vec::with_capacity(n_shards);
        for _ in 0..n_shards {
            shard_cursors.push(u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap()));
            pos += 8;
        }

        use std::collections::{BTreeMap, HashMap};
        let stream_count = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let mut seq_map: HashMap<u64, BTreeMap<u64, u64>> = HashMap::with_capacity(stream_count);
        for _ in 0..stream_count {
            let sid = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let n = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap()) as usize;
            pos += 8;
            let mut m = BTreeMap::new();
            for _ in 0..n {
                let seq = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let gid = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap());
                pos += 8;
                m.insert(seq, gid);
            }
            seq_map.insert(sid, m);
        }
        let next_count = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        let mut next_seqs: HashMap<u64, u64> = HashMap::with_capacity(next_count);
        for _ in 0..next_count {
            let sid = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let next = u64::from_le_bytes(raw[pos..pos + 8].try_into().unwrap());
            pos += 8;
            next_seqs.insert(sid, next);
        }

        let db_arc = std::sync::Arc::new(db);
        let storage = Self {
            db: Arc::clone(&db_arc),
            seq_map: RwLock::new(seq_map),
            next_seqs: RwLock::new(next_seqs),
            replicated_seq: std::sync::atomic::AtomicU64::new(0),
            num_shards,
            shard_bits,
        };

        // Incremental per-shard scan for records after the checkpoint cursor.
        for shard in 0..num_shards {
            let from_gid = logdb::encode_record_id(shard, shard_cursors[shard], shard_bits);
            let iter = match storage.db.scan_shard(shard, from_gid, u64::MAX) {
                Ok(i) => i,
                Err(e) => return Err(StorageError::LogDb(format!("incremental scan shard {shard}: {e:?}"))),
            };
            for r in iter {
                let rec = match r {
                    Ok(r) => r,
                    Err(e) => return Err(StorageError::LogDb(format!("scan rec: {e:?}"))),
                };
                let decoded = match crate::record::decode_record(&rec.content) {
                    Ok(d) => d,
                    Err(e) => return Err(StorageError::Record(e)),
                };
                let gid = rec.id.sequence;
                let sid = decoded.stream_id;
                {
                    let mut m = storage.seq_map.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                    m.entry(sid).or_default().insert(decoded.seq, gid);
                }
                {
                    let mut n = storage.next_seqs.write().unwrap_or_else(std::sync::PoisonError::into_inner);
                    let cur = n.entry(sid).or_insert(1);
                    if decoded.seq >= *cur {
                        *cur = decoded.seq + 1;
                    }
                }
            }
        }
        Ok(storage)
    }
}

impl Storage {
    /// Stream- + shard-scoped durable read for the Tail path (cr-037 perf):
    /// unlike [`scan`](Self::scan) (which decodes EVERY record then filters),
    /// this uses `seq_map` to jump straight to `stream_id`'s records, filters
    /// by `shard_ids` using the gid-encoded shard (no content decode), and
    /// point-reads only the surviving gids via [`LogDb::read_batch`] — which is
    /// per-shard durable-gated, so not-yet-durable records yield nothing and are
    /// picked up on a later poll.
    ///
    /// Returns durable records of `stream_id` with `seq >= from_seq` whose shard
    /// is in `shard_ids` (empty ⇒ all shards), in ascending seq order, up to
    /// `limit`.
    pub fn scan_stream_filtered(
        &self,
        stream_id: u64,
        from_seq: u64,
        shard_ids: &std::collections::HashSet<u32>,
        limit: usize,
    ) -> Result<Vec<DecodedRecord>, StorageError> {
        // 1. Collect this stream's gids (seq >= from_seq) whose shard matches.
        let gids: Vec<u64> = {
            let map = self
                .seq_map
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(stream_map) = map.get(&stream_id) else {
                return Ok(Vec::new());
            };
            stream_map
                .range(from_seq..)
                .filter(|&(_, &gid)| {
                    shard_ids.is_empty()
                        || shard_ids.contains(&(logdb::decode_record_id(gid, self.shard_bits).0 as u32))
                })
                .take(limit)
                .map(|(_, &gid)| gid)
                .collect()
        };
        if gids.is_empty() {
            return Ok(Vec::new());
        }

        // 2. Point-read only those gids (per-shard durable-gated by read_batch).
        let recs = self
            .db
            .read_batch(&gids)
            .map_err(|e| StorageError::LogDb(format!("read_batch: {:?}", e)))?;
        let mut out = Vec::with_capacity(recs.len());
        for (r, &gid) in recs.into_iter().zip(gids.iter()) {
            if let Some(rec) = r {
                let mut decoded = record::decode_record(&rec.content)?;
                decoded.shard_id = logdb::decode_record_id(gid, self.shard_bits).0 as u32;
                out.push(decoded);
            }
            // None ⇒ not durable yet (or tombstoned-gone); skip, retry next poll.
        }
        Ok(out)
    }

    /// Get committed cursor (gid space) — records that have been pwrite'd
    /// (serialized to the segment) but not necessarily fsync'd yet. This is the
    /// Query read boundary: ~≤10ms behind the producer, far fresher than the
    /// old SQLite cache path. See veps/cr-027-native-query-engine.md.
    pub fn committed_gid(&self) -> u64 {
        self.db.committed_cursor()
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
                cur,
                gid,
                Ordering::Release,
                Ordering::Acquire,
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
        self.db
            .replicate(gid, timestamp_ns, raw_content)
            .map_err(|e| StorageError::LogDb(format!("replicate: {:?}", e)))?;

        // Decode header to rebuild seq→gid mapping
        let decoded = record::decode_record(raw_content).map_err(|e| StorageError::Record(e))?;

        let mut map = self
            .seq_map
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        map.entry(decoded.stream_id)
            .or_insert_with(BTreeMap::new)
            .insert(decoded.seq, gid);
        // Update next_seq if needed
        let mut nexts = self
            .next_seqs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    fn test_storage_sharded(shards: usize) -> (Storage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = logdb::Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 256;
        config.durability_mode = logdb::DurabilityMode::Async; // avoid WSL2 fdatasync hang
        config.flush_timeout = Duration::from_secs(5);
        config.shards = shards;
        let db = logdb::LogDb::open(config).unwrap();
        (Storage::new(db, shards), dir)
    }

    #[test]
    fn append_and_read_single_stream() {
        let (st, _dir) = test_storage();
        let mut meta = BTreeMap::new();
        meta.insert("model".into(), "test".into());

        let r1 = st
            .append(
                1,
                42,
                "llm.call",
                "application/json",
                &meta,
                1000,
                b"hello",
                None,
            )
            .unwrap();
        let r2 = st
            .append(
                1,
                42,
                "tool.call",
                "application/json",
                &BTreeMap::new(),
                2000,
                b"world",
                None,
            )
            .unwrap();

        assert_eq!(r1.stream_seq, 1);
        assert_eq!(r2.stream_seq, 2);
        assert_ne!(r1.gid, r2.gid);

        st.flush().unwrap();
        // Wait for durable
        for _ in 0..50 {
            if st.durable_gid() >= r2.gid + 1 {
                break;
            }
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
            st.append(
                1,
                1,
                "test",
                "text/plain",
                &BTreeMap::new(),
                i,
                format!("r-{}", i).as_bytes(),
                None,
            )
            .unwrap();
        }
        st.flush().unwrap();
        for _ in 0..50 {
            if st.durable_gid() >= 5 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let results = st.scan(0, u64::MAX).unwrap();
        assert_eq!(results.len(), 5);
        for (i, r) in results.iter().enumerate() {
            assert_eq!(r.seq, i as u64 + 1);
        }
    }

    #[test]
    fn scan_populates_shard_id_from_gid_across_shards() {
        let shards = 4;
        let (st, _dir) = test_storage_sharded(shards);
        let bits = logdb::shard_bits(shards);

        // Append from 4 threads so thread-affine routing spreads records across
        // shards (a single thread would hit one shard, masking a "forgot to set
        // shard_id" bug if that shard happened to be 0).
        let seq_to_expected: std::sync::RwLock<BTreeMap<u64, u32>> =
            std::sync::RwLock::new(BTreeMap::new());
        std::thread::scope(|s| {
            for t in 0..4u64 {
                // Rebind to references so each `move` closure copies the ref
                // (and the Copy u64 `t`) instead of moving the owned Storage.
                let st = &st;
                let seq_to_expected = &seq_to_expected;
                s.spawn(move || {
                    for i in 0..4u64 {
                        let ar = st
                            .append(
                                1,
                                1,
                                "test",
                                "text/plain",
                                &BTreeMap::new(),
                                t * 10 + i,
                                format!("r-{t}-{i}").as_bytes(),
                                None,
                            )
                            .unwrap();
                        let expected = logdb::decode_record_id(ar.gid, bits).0 as u32;
                        seq_to_expected
                            .write()
                            .unwrap()
                            .insert(ar.stream_seq, expected);
                    }
                });
            }
        });

        st.flush().unwrap();
        for _ in 0..50 {
            if st.durable_gid() >= 16 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let results = st.scan(0, u64::MAX).unwrap();
        assert_eq!(results.len(), 16, "scan must see all 16 records");

        // Every scanned record's shard_id must equal the shard decoded from its
        // gid, and span more than one shard (proving real population, not a
        // constant default).
        let mut shards_seen = std::collections::HashSet::new();
        for r in &results {
            let expected = seq_to_expected
                .read()
                .unwrap()
                .get(&r.seq)
                .copied()
                .unwrap_or(u32::MAX);
            assert_eq!(
                r.shard_id, expected,
                "shard_id must match gid-decoded shard for seq {} (got {}, want {})",
                r.seq, r.shard_id, expected
            );
            assert!(
                (r.shard_id as usize) < shards,
                "shard_id {} out of bounds for {} shards",
                r.shard_id,
                shards
            );
            shards_seen.insert(r.shard_id);
        }
        assert!(
            shards_seen.len() > 1,
            "4 threads over 4 shards should use >1 shard, got {:?}",
            shards_seen
        );
    }

    #[test]
    fn scan_stream_filtered_is_stream_and_shard_scoped() {
        let shards = 4usize;
        let (st, _dir) = test_storage_sharded(shards);
        // Append to stream 1 with known shard_keys (deterministic routing), and
        // a couple to stream 2 to confirm stream isolation.
        let keys_s1: Vec<String> = (0..8).map(|i| format!("s1-{i}")).collect();
        for k in &keys_s1 {
            st.append(1, 1, "e", "text/plain", &BTreeMap::new(), 0, k.as_bytes(), Some(k))
                .unwrap();
        }
        st.append(1, 2, "e", "text/plain", &BTreeMap::new(), 0, b"s2-a", Some("s2-a"))
            .unwrap();
        st.append(1, 2, "e", "text/plain", &BTreeMap::new(), 0, b"s2-b", Some("s2-b"))
            .unwrap();
        st.flush().unwrap();
        for _ in 0..50 {
            if st.durable_gid() >= 8 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        let all: std::collections::HashSet<u32> = std::collections::HashSet::new();
        // Stream 1 only (no stream-2 records).
        let s1 = st.scan_stream_filtered(1, 1, &all, 100).unwrap();
        assert_eq!(s1.len(), 8, "stream 1 only");
        assert!(s1.iter().all(|r| r.stream_id == 1));

        // Shard-scoped: pick the shard of the first s1 record, filter to it.
        let target = s1[0].shard_id;
        let mut only = std::collections::HashSet::new();
        only.insert(target);
        let filtered = st.scan_stream_filtered(1, 1, &only, 100).unwrap();
        assert!(
            filtered.iter().all(|r| r.shard_id == target),
            "all returned records must be on shard {target}"
        );
        assert!(
            !filtered.is_empty(),
            "at least the first record is on shard {target}"
        );

        // from_seq gating: skip the first 3 seqs.
        let tail = st.scan_stream_filtered(1, 4, &all, 100).unwrap();
        assert!(tail.iter().all(|r| r.seq >= 4), "from_seq gating");
        assert_eq!(tail.len(), 5, "seqs 4..8");
    }
}

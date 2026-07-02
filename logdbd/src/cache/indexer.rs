//! Indexer — watches logdb committed cursor, writes decoded records
//! into per-stream SQLite databases in cache_dir.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::Connection;

use crate::catalog::Catalog;
use crate::config::CacheConfig;
use crate::record;
use crate::subscribe::SubscribeHub;

/// Build a safe filename from namespace and stream name.
/// Replaces '/' with '_' to avoid directory traversal.
pub fn db_filename(ns: &str, stream: &str) -> String {
    let safe = |s: &str| s.replace(['/', '\\'], "_");
    format!("{}.{}.db", safe(ns), safe(stream))
}

/// Create the records table and default indexes.
pub(crate) fn create_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS records (
            seq            INTEGER PRIMARY KEY,
            gid            INTEGER NOT NULL,
            ts_ns          INTEGER NOT NULL,
            event_type     TEXT NOT NULL,
            content_type   TEXT NOT NULL DEFAULT 'application/json',
            metadata_json  TEXT NOT NULL DEFAULT '{}',
            content        BLOB,
            deleted        INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_records_event_type ON records (event_type);
        CREATE INDEX IF NOT EXISTS idx_records_ts ON records (ts_ns);
        CREATE TABLE IF NOT EXISTS consumer_offsets (
            consumer_group TEXT NOT NULL,
            consumer_id    TEXT NOT NULL,
            committed_seq  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (consumer_group, consumer_id)
        );",
    )?;
    Ok(())
}

/// Insert a decoded record into the records table.
pub(crate) fn insert_record(
    conn: &Connection,
    gid: u64,
    rec: &record::DecodedRecord,
) -> Result<(), rusqlite::Error> {
    let meta_json = serde_json::to_string(&rec.metadata).unwrap_or_else(|_| "{}".into());
    conn.execute(
        "INSERT OR IGNORE INTO records (seq, gid, ts_ns, event_type, content_type, metadata_json, content, deleted)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
        rusqlite::params![
            rec.seq as i64,
            gid as i64,
            rec.timestamp_ns as i64,
            rec.event_type,
            rec.content_type,
            meta_json,
            rec.user_content,
        ],
    )?;
    Ok(())
}

/// Insert a tombstone — mark the target record as deleted.
fn insert_tombstone(conn: &Connection, gid: u64, target_seq: u64) -> Result<(), rusqlite::Error> {
    // Insert the tombstone itself with a unique seq
    conn.execute(
        "INSERT OR IGNORE INTO records (seq, gid, ts_ns, event_type, content_type, metadata_json, content, deleted)
         VALUES (?1, ?2, 0, 'logdb.tombstone', 'application/json', '{}', X'', 0)",
        rusqlite::params![-(gid as i64), gid as i64],
    )?;
    // Mark target as deleted
    conn.execute(
        "UPDATE records SET deleted = 1 WHERE seq = ?1",
        rusqlite::params![target_seq as i64],
    )?;
    Ok(())
}

/// Per-stream open SQLite connection tracked in the Indexer's stream map.
struct StreamDb {
    conn: Connection,
}

/// The Indexer runs in a background thread, chasing the logdb committed cursor
/// and writing decoded records into per-stream SQLite cache files.
pub struct Indexer {
    db: Arc<logdb::LogDb>,
    catalog: Arc<Catalog>,
    cache_dir: PathBuf,
    /// Latest gid that has been processed.
    last_gid: AtomicU64,
    /// Per-stream open connections: stream_id → StreamDb.
    /// The Indexer thread is the sole writer; reads happen via query.rs.
    streams: Mutex<HashMap<u64, StreamDb>>,
    running: AtomicBool,
    flush_interval: Duration,
    /// Per-stream metadata field indexes (stream_name → [field_names]).
    metadata_indexes: HashMap<String, Vec<String>>,
    /// Subscriber hub — publish each indexed record for real-time push.
    subscribe_hub: Arc<SubscribeHub>,
}

impl Indexer {
    /// Create a new Indexer. Does not start the background thread yet.
    pub fn new(
        db: Arc<logdb::LogDb>,
        catalog: Arc<Catalog>,
        cache_dir: PathBuf,
        config: &CacheConfig,
        subscribe_hub: Arc<SubscribeHub>,
    ) -> Self {
        std::fs::create_dir_all(&cache_dir).ok();
        let metadata_indexes: HashMap<String, Vec<String>> = config
            .indexes
            .iter()
            .map(|idx| (idx.stream.clone(), idx.fields.clone()))
            .collect();
        Self {
            db,
            catalog,
            cache_dir,
            last_gid: AtomicU64::new(0),
            streams: Mutex::new(HashMap::new()),
            running: AtomicBool::new(false),
            flush_interval: Duration::from_secs(config.flush_interval_secs),
            metadata_indexes,
            subscribe_hub,
        }
    }

    /// Start the Indexer background thread.
    pub fn start(self: Arc<Self>) {
        self.running.store(true, Ordering::Release);
        let this = Arc::clone(&self);
        std::thread::Builder::new()
            .name("logdbd-cache-indexer".into())
            .spawn(move || {
                this.run();
            })
            .expect("spawn cache indexer thread");
    }

    /// Main loop: poll committed cursor, replay new records, flush periodically.
    fn run(&self) {
        let mut last_flush = Instant::now();

        while self.running.load(Ordering::Acquire) {
            let committed = self.db.durable_cursor();
            let current = self.last_gid.load(Ordering::Acquire);

            if current < committed {
                match self.db.scan(current, committed) {
                    Ok(iter) => {
                        let mut max_gid = current;
                        for result in iter {
                            let rec = match result {
                                Ok(r) => r,
                                Err(e) => {
                                    tracing::warn!(error = %e, "cache indexer scan error");
                                    break;
                                }
                            };
                            let gid = rec.id.sequence;
                            max_gid = max_gid.max(gid + 1);
                            self.index_record(gid, &rec.content);
                        }
                        self.last_gid.store(max_gid, Ordering::Release);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "cache indexer scan failed");
                    }
                }
            }

            // Periodic WAL checkpoint
            if last_flush.elapsed() >= self.flush_interval {
                self.flush_all();
                last_flush = Instant::now();
            }

            // Sleep briefly to avoid busy-waiting
            std::thread::sleep(Duration::from_millis(10));
        }

        // Final flush before exit
        self.flush_all();
        tracing::info!(
            last_gid = self.last_gid.load(Ordering::Acquire),
            "cache indexer stopped"
        );
    }

    /// Decode and route one record to the correct stream's SQLite db.
    fn index_record(&self, gid: u64, raw: &[u8]) {
        let decoded = match record::decode_record(raw) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(gid = gid, error = %e, "cache indexer decode failed");
                return;
            }
        };

        let stream_id = decoded.stream_id;

        // Handle tombstone
        if decoded.event_type == "logdb.tombstone" {
            if let Some(target) = decoded.metadata.get("target_seq") {
                if let Ok(target_seq) = target.parse::<u64>() {
                    let streams = self.streams.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(s) = streams.get(&stream_id) {
                        if let Err(e) = insert_tombstone(&s.conn, gid, target_seq) {
                            tracing::warn!(error = %e, "cache indexer tombstone insert failed");
                        }
                    }
                }
            }
            return;
        }

        let mut streams = self.streams.lock().unwrap_or_else(|e| e.into_inner());

        let entry = streams.entry(stream_id).or_insert_with(|| {
            // Resolve stream name from catalog (for the db filename)
            let (ns_id, stream_name) = self
                .catalog
                .stream_info_by_id(stream_id)
                .unwrap_or((0, format!("unknown-{}", stream_id)));
            let ns_name = self
                .catalog
                .namespace_name(ns_id)
                .unwrap_or_else(|| format!("ns-{}", ns_id));
            let db_name = db_filename(&ns_name, &stream_name);

            let db_path = self.cache_dir.join(&db_name);
            let conn = Connection::open(&db_path).expect("open stream cache db");
            if let Err(e) = create_schema(&conn) {
                tracing::error!(error = %e, path = %db_path.display(), "failed to create cache schema");
            }
            // Apply configured metadata field indexes
            if let Some(fields) = self.metadata_indexes.get(&stream_name) {
                for field in fields {
                    let sql = format!(
                        "CREATE INDEX IF NOT EXISTS idx_records_meta_{field}
                         ON records (json_extract(metadata_json, '$.{field}'))",
                        field = field.replace('\'', "''")
                    );
                    if let Err(e) = conn.execute(&sql, []) {
                        tracing::warn!(field = field, error = %e, "failed to create metadata index");
                    }
                }
            }
            StreamDb { conn }
        });

        if let Err(e) = insert_record(&entry.conn, gid, &decoded) {
            tracing::warn!(gid = gid, stream_id = stream_id, error = %e, "cache indexer insert failed");
        }

        // Publish to subscribers (non-blocking)
        self.subscribe_hub.publish(stream_id, &decoded);
    }

    /// WAL checkpoint + fsync all open connections.
    fn flush_all(&self) {
        let streams = self.streams.lock().unwrap_or_else(|e| e.into_inner());
        for (_id, s) in streams.iter() {
            if let Err(e) = s.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
                tracing::warn!(error = %e, "cache indexer WAL checkpoint failed");
            }
        }
    }

    /// Shut down the Indexer thread gracefully.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Get the current progress (last processed gid).
    pub fn last_gid(&self) -> u64 {
        self.last_gid.load(Ordering::Acquire)
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn db_filename_replaces_slashes() {
        let name = db_filename("my-app", "user-1/session-abc");
        assert_eq!(name, "my-app.user-1_session-abc.db");
        assert!(!name.contains('/'), "filename must not contain slashes");
    }

    #[test]
    fn db_filename_replaces_backslashes() {
        let name = db_filename("ns", "a\\b");
        assert!(
            !name.contains('\\'),
            "filename must not contain backslashes"
        );
    }

    #[test]
    fn create_schema_succeeds() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();

        // Verify table exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='records'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "records table must exist");

        // Verify indexes exist
        let idx_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='records'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(idx_count, 2, "two default indexes must exist");
    }

    #[test]
    fn create_schema_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();
        create_schema(&conn).unwrap(); // second call must not error
    }

    #[test]
    fn insert_and_read_record() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();

        let rec = record::DecodedRecord {
            namespace_id: 1,
            stream_id: 42,
            seq: 1,
            event_type: "user.input".into(),
            content_type: "text/plain".into(),
            metadata: {
                let mut m = std::collections::BTreeMap::new();
                m.insert("model".into(), "claude".into());
                m
            },
            timestamp_ns: 1000,
            user_content: b"hello world".to_vec(),
        };

        insert_record(&conn, 0, &rec).unwrap();

        let row: (i64, String, String) = conn
            .query_row(
                "SELECT seq, event_type, metadata_json FROM records WHERE seq = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(row.0, 1);
        assert_eq!(row.1, "user.input");
        assert!(
            row.2.contains("claude"),
            "metadata_json should contain 'claude'"
        );
        assert!(
            row.2.contains("model"),
            "metadata_json should contain 'model'"
        );
    }

    #[test]
    fn insert_record_ignore_duplicate() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();

        let rec = record::DecodedRecord {
            namespace_id: 0,
            stream_id: 0,
            seq: 5,
            event_type: "test".into(),
            content_type: "text/plain".into(),
            metadata: std::collections::BTreeMap::new(),
            timestamp_ns: 0,
            user_content: b"first".to_vec(),
        };
        insert_record(&conn, 0, &rec).unwrap();

        // Insert same seq again — should be ignored
        let rec2 = record::DecodedRecord {
            user_content: b"second".to_vec(),
            ..rec
        };
        insert_record(&conn, 1, &rec2).unwrap();

        let content: Vec<u8> = conn
            .query_row("SELECT content FROM records WHERE seq = 5", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(content, b"first", "duplicate INSERT must be ignored");
    }

    #[test]
    fn tombstone_marks_record_deleted() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();

        // Insert target record
        let rec = record::DecodedRecord {
            namespace_id: 0,
            stream_id: 0,
            seq: 10,
            event_type: "msg".into(),
            content_type: "text/plain".into(),
            metadata: std::collections::BTreeMap::new(),
            timestamp_ns: 0,
            user_content: b"important".to_vec(),
        };
        insert_record(&conn, 100, &rec).unwrap();

        // Verify not deleted
        let deleted: i64 = conn
            .query_row("SELECT deleted FROM records WHERE seq = 10", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(deleted, 0);

        // Apply tombstone
        insert_tombstone(&conn, 200, 10).unwrap();

        // Verify marked deleted
        let deleted: i64 = conn
            .query_row("SELECT deleted FROM records WHERE seq = 10", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(deleted, 1, "tombstone must mark target as deleted");
    }

    #[test]
    fn delete_is_queryable() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();

        for i in 0..5u64 {
            let rec = record::DecodedRecord {
                namespace_id: 0,
                stream_id: 0,
                seq: i + 1,
                event_type: "test".into(),
                content_type: "text/plain".into(),
                metadata: std::collections::BTreeMap::new(),
                timestamp_ns: i,
                user_content: format!("r-{}", i).into_bytes(),
            };
            insert_record(&conn, i, &rec).unwrap();
        }

        // Tombstone seq 2
        insert_tombstone(&conn, 100, 2).unwrap();

        // Query only non-deleted
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM records WHERE deleted = 0 AND event_type != 'logdb.tombstone'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 4,
            "4 out of 5 records should be non-deleted after tombstone"
        );
    }
}

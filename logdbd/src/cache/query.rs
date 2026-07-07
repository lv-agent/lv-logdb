//! Query execution — runs read-only SQL against a stream's SQLite db.
//!
//! Connections are opened with `SQLITE_OPEN_READ_ONLY`, so the SQLite kernel
//! rejects any write (INSERT, UPDATE, DELETE, DROP, etc.) at the engine level —
//! stronger than string-based prefix checks.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use rusqlite::{Connection, params_from_iter};

/// Error returned by query execution.
#[derive(Debug)]
pub enum QueryError {
    Sqlite(rusqlite::Error),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(e) => write!(f, "sqlite error: {}", e),
        }
    }
}

impl std::error::Error for QueryError {}

/// A record queried from the cache for replay.
///
/// TODO(cr-027 phase 5): delete alongside the Indexer — zero callers after
/// phase 4 rewired Subscribe onto the segment.
#[derive(Debug)]
pub struct ReplayRecord {
    pub seq: u64,
    pub gid: u64,
    pub ts_ns: u64,
    pub event_type: String,
    pub content_type: String,
    pub metadata: BTreeMap<String, String>,
    pub content: Vec<u8>,
}

/// Replay records from the SQLite cache for Subscribe catch-up.
///
/// Much faster than segment scan — uses the primary key index.
/// Falls back gracefully: returns an empty vec if the db doesn't exist yet.
///
/// TODO(cr-027 phase 5): delete alongside the Indexer — zero callers after
/// phase 4 rewired Subscribe onto the segment.
pub fn replay_records(
    db_path: &Path,
    last_seq: u64,
    event_types: &HashSet<String>,
) -> Result<Vec<ReplayRecord>, QueryError> {
    if !db_path.exists() {
        return Ok(Vec::new());
    }
    if event_types.is_empty() {
        return Ok(Vec::new());
    }

    let conn = Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(QueryError::Sqlite)?;

    let placeholders: Vec<String> = event_types.iter().map(|_| "?".to_string()).collect();
    let sql = format!(
        "SELECT seq, gid, ts_ns, event_type, content_type, metadata_json, content
         FROM records
         WHERE seq > ?1 AND event_type IN ({})
           AND deleted = 0 AND event_type != 'logdb.tombstone'
         ORDER BY seq",
        placeholders.join(",")
    );

    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    params.push(Box::new(last_seq as i64));
    for et in event_types {
        params.push(Box::new(et.clone()));
    }

    let mut stmt = conn.prepare(&sql).map_err(QueryError::Sqlite)?;
    let rows = stmt
        .query_map(params_from_iter(params.iter().map(|p| p.as_ref())), |row| {
            let meta_str: String = row.get(5)?;
            let metadata: BTreeMap<String, String> =
                serde_json::from_str(&meta_str).unwrap_or_default();
            Ok(ReplayRecord {
                seq: row.get::<_, i64>(0)? as u64,
                gid: row.get::<_, i64>(1)? as u64,
                ts_ns: row.get::<_, i64>(2)? as u64,
                event_type: row.get(3)?,
                content_type: row.get(4)?,
                metadata,
                content: row.get(6)?,
            })
        })
        .map_err(QueryError::Sqlite)?;

    let mut results = Vec::new();
    for row in rows {
        results.push(row.map_err(QueryError::Sqlite)?);
    }
    Ok(results)
}

/// Execute a read-only SQL statement against a db file.
/// Returns rows as JSON strings.
///
/// The connection is opened read-only — any write operation is rejected by
/// the SQLite kernel with `SQLITE_READONLY`.
pub fn execute_query(db_path: &Path, sql: &str) -> Result<Vec<String>, QueryError> {
    let conn = Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(QueryError::Sqlite)?;

    let mut stmt = conn.prepare(sql).map_err(QueryError::Sqlite)?;

    let col_count = stmt.column_count();
    let col_names: Vec<String> = (0..col_count)
        .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
        .collect();

    let rows = stmt
        .query_map([], |row| {
            let mut obj = serde_json::Map::new();
            for (i, name) in col_names.iter().enumerate() {
                let val: serde_json::Value = match row.get_ref(i) {
                    Ok(rusqlite::types::ValueRef::Null) => serde_json::Value::Null,
                    Ok(rusqlite::types::ValueRef::Integer(n)) => serde_json::json!(n),
                    Ok(rusqlite::types::ValueRef::Real(f)) => serde_json::json!(f),
                    Ok(rusqlite::types::ValueRef::Text(s)) => {
                        serde_json::Value::String(String::from_utf8_lossy(s).into_owned())
                    }
                    Ok(rusqlite::types::ValueRef::Blob(b)) => {
                        serde_json::json!(blob_to_hex(b))
                    }
                    Err(_) => serde_json::Value::Null,
                };
                obj.insert(name.clone(), val);
            }
            Ok(serde_json::Value::Object(obj).to_string())
        })
        .map_err(QueryError::Sqlite)?;

    let mut results = Vec::new();
    for row in rows {
        results.push(row.map_err(QueryError::Sqlite)?);
    }
    Ok(results)
}

fn blob_to_hex(b: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        write!(&mut s, "{:02x}", byte).unwrap();
    }
    s
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    use crate::cache::indexer::{create_schema, insert_record};
    use crate::record::DecodedRecord;

    fn setup_test_db(dir: &Path) -> PathBuf {
        let db_path = dir.join("test.db");
        let conn = Connection::open(&db_path).unwrap();
        create_schema(&conn).unwrap();

        for i in 0..5u64 {
            let rec = DecodedRecord {
                namespace_id: 1,
                stream_id: 1,
                seq: i + 1,
                event_type: if i % 2 == 0 {
                    "user.input"
                } else {
                    "tool.call"
                }
                .into(),
                content_type: "text/plain".into(),
                metadata: {
                    let mut m = BTreeMap::new();
                    m.insert("turn_id".into(), format!("turn-{}", i / 2));
                    m.insert("model".into(), "claude".into());
                    m
                },
                timestamp_ns: i * 1000,
                user_content: format!("content-{}", i).into_bytes(),
            };
            insert_record(&conn, i, &rec).unwrap();
        }

        db_path
    }

    #[test]
    fn select_all_returns_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let rows = execute_query(&db_path, "SELECT * FROM records ORDER BY seq").unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn select_with_where_filters() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let rows = execute_query(
            &db_path,
            "SELECT seq, event_type FROM records WHERE event_type = 'user.input' ORDER BY seq",
        )
        .unwrap();
        assert_eq!(rows.len(), 3);

        let rows = execute_query(
            &db_path,
            "SELECT seq FROM records WHERE event_type = 'tool.call' ORDER BY seq",
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn select_count_returns_correct_count() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let rows = execute_query(&db_path, "SELECT COUNT(*) AS cnt FROM records").unwrap();
        assert!(rows[0].contains("5"), "COUNT should be 5, got: {}", rows[0]);

        let rows = execute_query(
            &db_path,
            "SELECT COUNT(*) FROM records WHERE event_type = 'tool.call'",
        )
        .unwrap();
        assert!(rows[0].contains("2"));
    }

    #[test]
    fn select_with_json_extract() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let rows = execute_query(
            &db_path,
            "SELECT seq FROM records WHERE json_extract(metadata_json, '$.turn_id') = 'turn-0' ORDER BY seq",
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn select_with_limit() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let rows = execute_query(&db_path, "SELECT seq FROM records ORDER BY seq LIMIT 2").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn select_empty_result() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let rows = execute_query(
            &db_path,
            "SELECT * FROM records WHERE event_type = 'nonexistent'",
        )
        .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn select_with_leading_whitespace() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let rows =
            execute_query(&db_path, "   SELECT seq FROM records ORDER BY seq LIMIT 1").unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn read_only_rejects_insert() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let err = execute_query(&db_path, "INSERT INTO records (seq) VALUES (99)").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("readonly") || msg.contains("READONLY"),
            "INSERT must be rejected by read-only mode, got: {}",
            msg
        );
    }

    #[test]
    fn read_only_rejects_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let err = execute_query(&db_path, "DELETE FROM records WHERE seq = 1").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("readonly") || msg.contains("READONLY"),
            "DELETE must be rejected by read-only mode, got: {}",
            msg
        );
    }

    #[test]
    fn read_only_rejects_update() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let err =
            execute_query(&db_path, "UPDATE records SET deleted = 1 WHERE seq = 1").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("readonly") || msg.contains("READONLY"),
            "UPDATE must be rejected by read-only mode, got: {}",
            msg
        );
    }

    #[test]
    fn read_only_rejects_drop() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let err = execute_query(&db_path, "DROP TABLE records").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("readonly") || msg.contains("READONLY"),
            "DROP must be rejected by read-only mode, got: {}",
            msg
        );
    }

    #[test]
    fn null_and_blob_columns() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("null_test.db");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS records (
                seq INTEGER PRIMARY KEY,
                gid INTEGER NOT NULL,
                ts_ns INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                content_type TEXT NOT NULL DEFAULT 'application/json',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                content BLOB,
                deleted INTEGER NOT NULL DEFAULT 0
            );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO records (seq, gid, ts_ns, event_type, content_type, metadata_json, content)
             VALUES (1, 0, 0, 'test', 'text/plain', '{}', NULL)",
            [],
        )
        .unwrap();

        let rows = execute_query(&db_path, "SELECT content FROM records WHERE seq = 1").unwrap();
        assert_eq!(rows.len(), 1);
        assert!(
            rows[0].contains("null"),
            "NULL content should serialize as null"
        );
    }
}

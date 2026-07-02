//! Query execution — validates and runs read-only SQL against a stream's SQLite db.

use std::path::Path;

use rusqlite::Connection;

/// Error returned by query validation or execution.
#[derive(Debug)]
pub enum QueryError {
    NotSelect,
    Sqlite(rusqlite::Error),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSelect => write!(f, "only SELECT statements are allowed"),
            Self::Sqlite(e) => write!(f, "sqlite error: {}", e),
        }
    }
}

impl std::error::Error for QueryError {}

/// Validate that `sql` is a read-only SELECT statement.
fn validate_sql(sql: &str) -> Result<(), QueryError> {
    let trimmed = sql.trim();
    let prefix = trimmed
        .chars()
        .take(6)
        .collect::<String>()
        .to_uppercase();
    if prefix != "SELECT" {
        return Err(QueryError::NotSelect);
    }
    Ok(())
}

/// Execute a validated SELECT statement against a db file.
/// Returns rows as JSON strings.
pub fn execute_query(db_path: &Path, sql: &str) -> Result<Vec<String>, QueryError> {
    validate_sql(sql)?;

    let conn = Connection::open(db_path).map_err(QueryError::Sqlite)?;

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
                        // Return blob as hex string for JSON compatibility
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

        // Insert 5 records
        for i in 0..5u64 {
            let rec = DecodedRecord {
                namespace_id: 1,
                stream_id: 1,
                seq: i + 1,
                event_type: if i % 2 == 0 { "user.input" } else { "tool.call" }.into(),
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
        assert_eq!(rows.len(), 3, "should find 3 user.input records (seq 1,3,5)");

        let rows = execute_query(
            &db_path,
            "SELECT seq FROM records WHERE event_type = 'tool.call' ORDER BY seq",
        )
        .unwrap();
        assert_eq!(rows.len(), 2, "should find 2 tool.call records (seq 2,4)");
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
        assert!(rows[0].contains("2"), "filtered COUNT should be 2");
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
        assert_eq!(rows.len(), 2, "turn-0 should match 2 records (seq 1,2)");
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
    fn reject_insert() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let err = execute_query(&db_path, "INSERT INTO records (seq) VALUES (99)").unwrap_err();
        assert!(matches!(err, QueryError::NotSelect));
    }

    #[test]
    fn reject_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let err = execute_query(&db_path, "DELETE FROM records WHERE seq = 1").unwrap_err();
        assert!(matches!(err, QueryError::NotSelect));
    }

    #[test]
    fn reject_update() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let err =
            execute_query(&db_path, "UPDATE records SET deleted = 1 WHERE seq = 1").unwrap_err();
        assert!(matches!(err, QueryError::NotSelect));
    }

    #[test]
    fn reject_drop() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let err = execute_query(&db_path, "DROP TABLE records").unwrap_err();
        assert!(matches!(err, QueryError::NotSelect));
    }

    #[test]
    fn select_with_leading_whitespace() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = setup_test_db(dir.path());

        let rows =
            execute_query(&db_path, "   SELECT seq FROM records ORDER BY seq LIMIT 1").unwrap();
        assert_eq!(rows.len(), 1, "leading whitespace should be allowed");
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
        assert!(rows[0].contains("null"), "NULL content should serialize as null");
    }
}

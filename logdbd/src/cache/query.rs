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

//! Query execution — placeholder. TODO: implement.

use std::path::Path;

#[derive(Debug)]
pub enum QueryError {
    Sqlite(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(e) => write!(f, "sqlite error: {}", e),
        }
    }
}

impl std::error::Error for QueryError {}

pub fn execute_query(_db_path: &Path, _sql: &str) -> Result<Vec<String>, QueryError> {
    Ok(vec![])
}

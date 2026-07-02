//! Consumer group — server-side offset tracking with optional SQLite persistence.
//!
//! When a `cache_dir` is provided, offsets are durably stored in each stream's
//! SQLite cache db (`consumer_offsets` table).  Without a cache dir, offsets are
//! in-memory only (useful for tests).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use rusqlite::Connection;

use crate::cache::indexer::db_filename;

/// Key for consumer offset lookup.
type ConsumerKey = (String, String, String, String); // (ns, stream, group, id)

pub struct ConsumerTracker {
    offsets: RwLock<HashMap<ConsumerKey, u64>>,
    cache_dir: Option<PathBuf>,
}

impl ConsumerTracker {
    /// Create a new tracker with optional SQLite persistence.
    pub fn new(cache_dir: Option<PathBuf>) -> Self {
        Self {
            offsets: RwLock::new(HashMap::new()),
            cache_dir,
        }
    }

    /// Commit an offset for a consumer. Persists to SQLite when available.
    pub fn commit(
        &self,
        namespace: &str,
        stream: &str,
        consumer_group: &str,
        consumer_id: &str,
        seq: u64,
    ) {
        let key = (
            namespace.to_string(),
            stream.to_string(),
            consumer_group.to_string(),
            consumer_id.to_string(),
        );
        // In-memory cache
        {
            let mut map = self
                .offsets
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.insert(key, seq);
        }
        // SQLite persistence
        if let Some(dir) = &self.cache_dir {
            let db_path = dir.join(db_filename(namespace, stream));
            if db_path.exists() {
                if let Ok(conn) = Connection::open(&db_path) {
                    let _ = conn.execute(
                        "INSERT OR REPLACE INTO consumer_offsets (consumer_group, consumer_id, committed_seq)
                         VALUES (?1, ?2, ?3)",
                        rusqlite::params![consumer_group, consumer_id, seq as i64],
                    );
                }
            }
        }
    }

    /// Get the last committed offset for a consumer. Falls back to SQLite on cache miss.
    pub fn get(
        &self,
        namespace: &str,
        stream: &str,
        consumer_group: &str,
        consumer_id: &str,
    ) -> u64 {
        let key = (
            namespace.to_string(),
            stream.to_string(),
            consumer_group.to_string(),
            consumer_id.to_string(),
        );
        // In-memory first
        {
            let map = self
                .offsets
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(&val) = map.get(&key) {
                return val;
            }
        }
        // SQLite fallback
        if let Some(dir) = &self.cache_dir {
            let db_path = dir.join(db_filename(namespace, stream));
            if db_path.exists() {
                if let Ok(conn) = Connection::open(&db_path) {
                    let result: Result<i64, _> = conn.query_row(
                        "SELECT committed_seq FROM consumer_offsets
                         WHERE consumer_group = ?1 AND consumer_id = ?2",
                        rusqlite::params![consumer_group, consumer_id],
                        |row| row.get(0),
                    );
                    if let Ok(seq) = result {
                        if seq > 0 {
                            // Cache in memory for next lookup
                            let mut map = self
                                .offsets
                                .write()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            map.insert(key, seq as u64);
                            return seq as u64;
                        }
                    }
                }
            }
        }
        0
    }

    /// List all committed offsets for a consumer group in a stream.
    pub fn list_group(
        &self,
        namespace: &str,
        stream: &str,
        consumer_group: &str,
    ) -> Vec<(String, u64)> {
        let map = self
            .offsets
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prefix = (
            namespace.to_string(),
            stream.to_string(),
            consumer_group.to_string(),
        );
        map.iter()
            .filter(|((ns, s, g, _), _)| *ns == prefix.0 && *s == prefix.1 && *g == prefix.2)
            .map(|((_, _, _, id), seq)| (id.clone(), *seq))
            .collect()
    }
}

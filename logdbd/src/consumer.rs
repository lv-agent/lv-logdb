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

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_and_get_in_memory() {
        let tracker = ConsumerTracker::new(None);
        assert_eq!(tracker.get("ns", "s", "g", "c1"), 0);

        tracker.commit("ns", "s", "g", "c1", 42);
        assert_eq!(tracker.get("ns", "s", "g", "c1"), 42);
    }

    #[test]
    fn independent_consumers() {
        let tracker = ConsumerTracker::new(None);
        tracker.commit("ns", "s", "g", "w1", 10);
        tracker.commit("ns", "s", "g", "w2", 20);

        assert_eq!(tracker.get("ns", "s", "g", "w1"), 10);
        assert_eq!(tracker.get("ns", "s", "g", "w2"), 20);
    }

    #[test]
    fn commit_overwrites_previous() {
        let tracker = ConsumerTracker::new(None);
        tracker.commit("ns", "s", "g", "c1", 5);
        tracker.commit("ns", "s", "g", "c1", 100);
        assert_eq!(tracker.get("ns", "s", "g", "c1"), 100);
    }

    #[test]
    fn sqlite_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().to_path_buf();

        // Create the db file with proper schema
        let db_path = cache_dir.join("ns.stream.db");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS consumer_offsets (
                    consumer_group TEXT NOT NULL,
                    consumer_id    TEXT NOT NULL,
                    committed_seq  INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (consumer_group, consumer_id)
                );",
            )
            .unwrap();
        }

        let tracker = ConsumerTracker::new(Some(cache_dir));

        // Commit
        tracker.commit("ns", "stream", "g1", "c1", 99);

        // Read back from a fresh tracker (simulates restart)
        let tracker2 = ConsumerTracker::new(Some(dir.path().to_path_buf()));
        assert_eq!(tracker2.get("ns", "stream", "g1", "c1"), 99);
    }

    #[test]
    fn sqlite_nonexistent_returns_zero() {
        let tracker = ConsumerTracker::new(None);
        // No consumer committed anything yet
        assert_eq!(tracker.get("ns", "s", "g", "unknown"), 0);
    }

    #[test]
    fn list_group_returns_consumer_ids() {
        let tracker = ConsumerTracker::new(None);
        tracker.commit("ns", "s", "g1", "w1", 10);
        tracker.commit("ns", "s", "g1", "w2", 20);
        tracker.commit("ns", "s", "g2", "w3", 30);

        let g1 = tracker.list_group("ns", "s", "g1");
        assert_eq!(g1.len(), 2);
        assert!(g1.contains(&("w1".into(), 10)));
        assert!(g1.contains(&("w2".into(), 20)));
    }
}

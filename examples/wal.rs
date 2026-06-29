//! logdb as a WAL (Write-Ahead Log) for a database.
//!
//! This example simulates a simple key-value store using logdb as its WAL.
//! It shows the complete lifecycle: write → flush → checkpoint → crash → recover.
//!
//! Run: cargo run --release --example wal

use std::collections::HashMap;
use std::time::Duration;

use logdb::config::{Config, DurabilityMode};
use logdb::LogDb;

/// A simulated key-value store backed by logdb WAL.
struct KvStore {
    db: LogDb,
    /// In-memory state rebuilt from WAL.
    data: HashMap<String, String>,
}

impl KvStore {
    /// Open or recover the store.
    fn open(data_dir: &str) -> Self {
        let mut config = Config::default();
        config.data_dir = data_dir.into();
        config.durability_mode = DurabilityMode::Async; // Use flush() explicitly for durability
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();
        let mut data = HashMap::new();

        // Phase 1: Recover from WAL (replay records since last checkpoint)
        let report = db.recovery_report();
        println!("Recovery report: from={} to={} count={}",
            report.from_sequence, report.to_sequence, report.count);

        for result in db.replay_from(report.from_sequence).unwrap() {
            let record = result.unwrap();
            let content = String::from_utf8_lossy(&record.content);
            // Parse "PUT key value" or "DEL key"
            let parts: Vec<&str> = content.splitn(3, ' ').collect();
            match parts.as_slice() {
                ["PUT", key, value] => { data.insert(key.to_string(), value.to_string()); }
                ["DEL", key] => { data.remove(*key); }
                _ => {}
            }
        }
        println!("Recovered {} key(s) from WAL", data.len());

        Self { db, data }
    }

    /// Put a key-value pair (durably).
    fn put(&mut self, key: &str, value: &str) {
        let wal = format!("PUT {} {}", key, value);
        self.db.append(wal.as_bytes()).unwrap();
        self.db.flush().unwrap();

        // Apply to in-memory state after WAL durable
        self.data.insert(key.to_string(), value.to_string());
        println!("PUT {} = {} (lsn={})", key, value, self.db.durable_cursor());
    }

    /// Delete a key (durably).
    fn delete(&mut self, key: &str) {
        let wal = format!("DEL {}", key);
        self.db.append(wal.as_bytes()).unwrap();
        self.db.flush().unwrap();

        self.data.remove(key);
        println!("DEL {} (lsn={})", key, self.db.durable_cursor());
    }

    /// Put multiple keys atomically.
    fn put_batch(&mut self, pairs: &[(&str, &str)]) {
        let wals: Vec<String> = pairs.iter()
            .map(|(k, v)| format!("PUT {} {}", k, v))
            .collect();
        let wal_refs: Vec<&[u8]> = wals.iter().map(|s| s.as_bytes()).collect();
        self.db.append_batch(&wal_refs).unwrap();
        self.db.flush().unwrap();

        for (k, v) in pairs {
            self.data.insert(k.to_string(), v.to_string());
        }
        println!("PUT batch {} pairs (lsn={})", pairs.len(), self.db.durable_cursor());
    }

    /// Create a checkpoint. After this, WAL before checkpoint can be truncated.
    fn checkpoint(&self) {
        let lsn = self.db.durable_cursor();
        self.db.checkpoint(lsn);
        println!("Checkpoint at lsn={}", lsn);
    }

    /// Show WAL usage.
    fn show_usage(&self) {
        let (used, total) = self.db.wal_usage();
        println!("WAL: {} / {} bytes ({:.1}%)", used, total, used as f64 / total as f64 * 100.0);
    }

    /// Graceful shutdown.
    fn close(self) {
        println!("Closing...");
        let report = self.db.shutdown(Duration::from_secs(5)).unwrap();
        println!("Shutdown: {:?}", report);
    }
}

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    println!("=== logdb WAL Demo ===\n");

    // ── Session 1: Write some data ─────────────────────────────────────
    println!("--- Session 1 ---");
    {
        let mut store = KvStore::open(path);

        store.put("name", "Alice");
        store.put_batch(&[
            ("email", "alice@example.com"),
            ("role", "admin"),
        ]);
        store.delete("role");

        // Checkpoint: everything before this is safe to delete
        store.checkpoint();
        store.show_usage();
        store.close();
    }

    // ── Session 2: Crash recovery simulation ───────────────────────────
    println!("\n--- Session 2 (after simulated crash) ---");
    {
        let store = KvStore::open(path);
        // Should have recovered name and email from WAL
        println!("name={:?}", store.data.get("name"));
        println!("email={:?}", store.data.get("email"));
        println!("role={:?}", store.data.get("role")); // deleted → None
        assert_eq!(store.data.get("name").map(String::as_str), Some("Alice"));
        assert_eq!(store.data.get("role"), None);
        println!("\nRecovery successful: data intact after crash.");
        store.close();
    }
}

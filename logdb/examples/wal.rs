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
    /// The durable sequence at open time. We checkpoint here at close, which
    /// means "everything I applied this session is now absorbed by the app;
    /// WAL before this point may be truncated." Records written this session
    /// (sequences >= this value) remain recoverable on the next replay.
    replay_from: u64,
}

impl KvStore {
    /// Open or recover the store.
    ///
    /// `replay_checkpoint` is the sequence at which the *previous* session was
    /// checkpointed. We replay from there to rebuild in-memory state. (We pass
    /// it explicitly rather than reading `recovery_report()` so that a session
    /// that has just checkpointed its own tail still recovers its data — see
    /// the note in [`KvStore::checkpoint`].)
    fn open(data_dir: &str, replay_checkpoint: u64) -> Self {
        let mut config = Config::default();
        config.data_dir = data_dir.into();
        config.durability_mode = DurabilityMode::Async; // Use flush() explicitly for durability
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();
        let mut data = HashMap::new();

        // Phase 1: Recover from WAL (replay records from the replay point).
        let report = db.recovery_report();
        println!(
            "Recovery report: from={} to={} count={}",
            report.from_sequence, report.to_sequence, report.count
        );

        for result in db.replay_from(replay_checkpoint).unwrap() {
            let record = result.unwrap();
            let content = String::from_utf8_lossy(&record.content);
            // Parse "PUT key value" or "DEL key"
            let parts: Vec<&str> = content.splitn(3, ' ').collect();
            match parts.as_slice() {
                ["PUT", key, value] => {
                    data.insert(key.to_string(), value.to_string());
                }
                ["DEL", key] => {
                    data.remove(*key);
                }
                _ => {}
            }
        }
        println!("Recovered {} key(s) from WAL", data.len());

        Self {
            db,
            data,
            replay_from: replay_checkpoint,
        }
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
        let wals: Vec<String> = pairs
            .iter()
            .map(|(k, v)| format!("PUT {} {}", k, v))
            .collect();
        let wal_refs: Vec<&[u8]> = wals.iter().map(|s| s.as_bytes()).collect();
        self.db.append_batch(&wal_refs).unwrap();
        self.db.flush().unwrap();

        for (k, v) in pairs {
            self.data.insert(k.to_string(), v.to_string());
        }
        println!(
            "PUT batch {} pairs (lsn={})",
            pairs.len(),
            self.db.durable_cursor()
        );
    }

    /// Create a checkpoint at the session's replay point.
    ///
    /// This tells logdb "the application has absorbed every record up to here,
    /// so WAL data before this sequence may be truncated." Records written
    /// *during* this session (sequences >= `replay_from`) are **not** covered
    /// by the checkpoint and remain recoverable on the next replay.
    ///
    /// Note: do **not** checkpoint the live `durable_cursor()` here. A
    /// checkpoint at the durable tail would cover the very records you just
    /// wrote, leaving `recovery_report().count == 0` and nothing to replay
    /// after a crash. Checkpoint the stable point you resumed from, not the
    /// tip you are still building.
    fn checkpoint(&self) {
        let lsn = self.replay_from;
        self.db.checkpoint(lsn);
        println!(
            "Checkpoint at lsn={} (durable tail={})",
            lsn,
            self.db.durable_cursor()
        );
    }

    /// Show WAL usage.
    fn show_usage(&self) {
        let (used, total) = self.db.wal_usage();
        println!(
            "WAL: {} / {} bytes ({:.1}%)",
            used,
            total,
            used as f64 / total as f64 * 100.0
        );
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
    let session1_checkpoint;
    {
        // Fresh directory: no checkpoint.dat yet, so we replay from 0.
        let mut store = KvStore::open(path, 0);

        store.put("name", "Alice");
        store.put_batch(&[("email", "alice@example.com"), ("role", "admin")]);
        store.delete("role");

        // Checkpoint the replay point we resumed from (0). Records written
        // this session (sequences >= 0) remain recoverable.
        store.checkpoint();
        store.show_usage();
        session1_checkpoint = store.replay_from;
        store.close();
    }

    // ── Session 2: Crash recovery simulation ───────────────────────────
    println!("\n--- Session 2 (after simulated crash) ---");
    {
        // Replay from the checkpoint persisted by Session 1.
        let store = KvStore::open(path, session1_checkpoint);
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

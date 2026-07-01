//! Tailer consumer example: durable named consumer with independent progress.
//!
//! Run with: cargo run --example tailer_consumer
//!
//! Creates a named tailer, drains the log in batches, commits progress, then
//! reopens — verifying that only the unread records are delivered (no resends).

use std::time::Duration;

use logdb::{Config, DurabilityMode, LogDb};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 256;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(config).unwrap();

    // Produce 30 records.
    for i in 0..30u64 {
        db.append(format!("rec-{}", i).as_bytes()).unwrap();
    }
    db.flush().unwrap();
    for _ in 0..50 {
        if db.durable_cursor() >= 30 {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    println!("produced 30 durable records");

    // Consume 12 with a tailer, commit, then verify only the remaining 18
    // are delivered on reopen (no loss, no duplicates).
    let name = "example-consumer";
    let mut tailer = db.new_tailer(name);
    let mut delivered = Vec::new();
    for _ in 0..100 {
        match tailer.next_batch(12).unwrap() {
            Some(batch) => delivered.extend(batch.iter().map(|r| r.id.sequence)),
            None => break,
        }
    }
    tailer.commit().unwrap();
    println!(
        "first batch: {} records delivered, position committed",
        delivered.len()
    );

    // Reopen the tailer: progress must be restored (last committed position).
    let mut tailer2 = db.new_tailer(name);
    let positions = tailer2.positions().to_vec();
    assert!(
        positions.iter().any(|&p| p > 0),
        "some shard must have advanced"
    );

    // Read the rest.
    let mut rest = Vec::new();
    for _ in 0..100 {
        match tailer2.next_batch(100).unwrap() {
            Some(batch) => rest.extend(batch.iter().map(|r| r.id.sequence)),
            None => break,
        }
    }

    let rest_len = rest.len();
    let mut all: Vec<u64> = delivered.clone();
    all.extend(rest);
    all.sort();
    let want: Vec<u64> = (0..30).collect();
    assert_eq!(
        all, want,
        "tailer must deliver each record exactly once (commit + reopen)"
    );
    println!(
        "reopened tailer: {} new records, total {} — each delivered once",
        rest_len,
        all.len()
    );
}

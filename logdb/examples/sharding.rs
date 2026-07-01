//! Sharding example: multi-ring write scalability.
//!
//! Run with: cargo run --example sharding
//!
//! Spreads writes across 4 shards (concurrent threads), then demonstrates that
//! global ids are unique and cross-shard scan returns them in ascending order.

use std::sync::Arc;
use std::time::Duration;

use logdb::{Config, DurabilityMode, LogDb};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.shards = 4;
    config.ring_size = 256;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(5);
    let db = Arc::new(LogDb::open(config).unwrap());

    // Concurrent writes from 4 threads (each lands on its own shard via
    // thread-affine routing).
    let mut handles = Vec::new();
    for t in 0..4u64 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            (0..10u64)
                .map(|i| db.append(format!("t{}-{}", t, i).as_bytes()).unwrap())
                .collect::<Vec<u64>>()
        }));
    }
    let mut all_ids = Vec::new();
    for h in handles {
        all_ids.extend(h.join().unwrap());
    }
    db.flush().unwrap();
    for _ in 0..50 {
        if db.scan(0, u64::MAX).unwrap().filter_map(|r| r.ok()).count() >= all_ids.len() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    println!("wrote {} records across {} shards", all_ids.len(), 4);

    // Each global id is unique (no collisions across shards).
    let mut sorted = all_ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), all_ids.len(), "no duplicate global ids");

    // Cross-shard scan returns everything in ascending global-id order.
    let scanned: Vec<u64> = db
        .scan(0, u64::MAX)
        .unwrap()
        .filter_map(|r| r.ok())
        .map(|r| r.id.sequence)
        .collect();
    assert_eq!(scanned.len(), all_ids.len(), "scan must see all shards");
    assert!(
        scanned.windows(2).all(|w| w[0] < w[1]),
        "scan must be ascending"
    );
    println!("cross-shard scan: {} records ascending — ok", scanned.len());

    // Reads by global id across shards.
    for &id in &all_ids {
        let rec = db.read(id).unwrap().unwrap();
        assert_eq!(rec.id.sequence, id);
    }
    println!("point reads by global id across shards: ok");

    // The global id encodes (shard, local_seq). For shards=4, shard_bits=2.
    // Here is how you decode it:
    let bits = 2u32; // ceil(log2(4)) = 2
    for &id in &all_ids {
        let (shard, _local) = logdb::decode_record_id(id, bits);
        assert!(shard < 4, "shard id must be < num_shards");
        // local may span more than 10 if multiple threads hit the same shard
        // (thread-affine routing isn't a guarantee of even distribution).
    }
    println!("all global ids decode to valid (shard, local) pairs");
}

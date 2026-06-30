//! Scan throughput measurement (not Criterion — avoids WSL2 long-run hang).
//! Run with: cargo run --release --example scan_perf
//!
//! Pre-fills the log with N small records, then measures full-scan throughput
//! (records/sec and ns/record). Used to baseline cr-004 scan optimization.

use std::time::{Duration, Instant};

use logdb::LogDb;
use logdb::{Config, DurabilityMode};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 1 << 18; // 262144 slots
    config.shards = 1; // raw-mode scan path
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(30);
    let db = LogDb::open(config).unwrap();

    let n: u64 = 300_000;
    let content = vec![0x42u8; 64];

    let t0 = Instant::now();
    for _ in 0..n {
        db.append(&content).unwrap();
    }
    db.flush().unwrap();
    while db.durable_cursor() < n {
        std::thread::sleep(Duration::from_millis(50));
    }
    println!(
        "filled {} x 64B records in {:.2}s",
        n,
        t0.elapsed().as_secs_f64()
    );

    println!("--- full scan(0, MAX), 3 runs ---");
    for run in 0..3 {
        let t0 = Instant::now();
        let mut count = 0u64;
        for r in db.scan(0, u64::MAX).unwrap() {
            if r.is_ok() {
                count += 1;
            }
        }
        let elapsed = t0.elapsed();
        println!(
            "  run {}: {} records in {:.3}s -> {:.0} rec/s ({:.0} ns/rec)",
            run,
            count,
            elapsed.as_secs_f64(),
            count as f64 / elapsed.as_secs_f64(),
            elapsed.as_nanos() as f64 / count as f64,
        );
    }
}

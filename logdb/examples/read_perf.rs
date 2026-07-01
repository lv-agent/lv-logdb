//! Point-read latency measurement (not Criterion — avoids WSL2 long-run hang).
//! Run with: cargo run --release --example read_perf
//!
//! Pre-fills the log with N small records, warms the page cache, then measures
//! point-read throughput / latency. Used to check that manifest/cache changes
//! (e.g. cr-014's stale-entry guard) do not regress the read hot path.

use std::time::{Duration, Instant};

use logdb::{Config, DurabilityMode, LogDb};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 1 << 18; // 262144 slots
    config.shards = 1; // raw-mode path
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(30);
    let db = LogDb::open(config).unwrap();

    let n: u64 = 20_000;
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

    // Warm: page-cache the segment + the manifest.
    for id in 0..n {
        let _ = db.read(id).unwrap();
    }

    println!("--- N x read(id), 3 runs ---");
    for run in 0..3 {
        let t0 = Instant::now();
        let mut ok = 0u64;
        for id in 0..n {
            if db.read(id).unwrap().is_some() {
                ok += 1;
            }
        }
        let elapsed = t0.elapsed();
        println!(
            "  run {}: {} reads in {:.3}s -> {:.0} reads/s ({:.0} ns/read) [ok={}]",
            run,
            n,
            elapsed.as_secs_f64(),
            n as f64 / elapsed.as_secs_f64(),
            elapsed.as_nanos() as f64 / n as f64,
            ok,
        );
    }

    let ids: Vec<u64> = (0..n).collect();
    println!("--- read_batch(all), 3 runs ---");
    for run in 0..3 {
        let t0 = Instant::now();
        let batch = db.read_batch(&ids).unwrap();
        let elapsed = t0.elapsed();
        let ok = batch.iter().filter(|r| r.is_some()).count() as u64;
        println!(
            "  run {}: {} reads in {:.3}s -> {:.0} reads/s ({:.0} ns/read) [ok={}]",
            run,
            n,
            elapsed.as_secs_f64(),
            n as f64 / elapsed.as_secs_f64(),
            elapsed.as_nanos() as f64 / n as f64,
            ok,
        );
    }
}

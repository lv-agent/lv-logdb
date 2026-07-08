//! Benchmarks for logdb append throughput and latency.

use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

use logdb::LogDb;
use logdb::{Config, DurabilityMode};

fn bench_db(ring_size: usize) -> LogDb {
    // keep(): retain the temp dir for the bench's lifetime (no auto-cleanup).
    // Appends don't care, but benches that flush + wait for durability (scan)
    // need the files to remain linked.
    let dir = tempfile::tempdir().unwrap().keep();
    let mut config = Config::default();
    config.data_dir = dir;
    config.ring_size = ring_size;
    config.durability_mode = DurabilityMode::Async;
    config.flush_timeout = Duration::from_secs(5);
    LogDb::open(config).unwrap()
}

// ── Single-thread append latency ───────────────────────────────────────────

fn bench_append_64b_single(c: &mut Criterion) {
    let db = bench_db(8192);
    let content = vec![0u8; 64];
    c.bench_function("append/64B/1t", |b| {
        b.iter(|| {
            db.append(black_box(&content)).unwrap();
        });
    });
}

fn bench_append_256b_single(c: &mut Criterion) {
    let db = bench_db(8192);
    let content = vec![0u8; 256];
    c.bench_function("append/256B/1t", |b| {
        b.iter(|| {
            db.append(black_box(&content)).unwrap();
        });
    });
}

fn bench_append_1k_single(c: &mut Criterion) {
    let db = bench_db(8192);
    let content = vec![0u8; 1024];
    c.bench_function("append/1024B/1t", |b| {
        b.iter(|| {
            db.append(black_box(&content)).unwrap();
        });
    });
}

// ── Multi-thread append throughput ─────────────────────────────────────────

fn bench_append_256b_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("append-throughput/256B");

    for threads in [1, 2, 4, 8].iter() {
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("{}t-rec/s", threads), |b| {
            b.iter_custom(|iters| {
                let db = Arc::new(bench_db(65536));
                let content = vec![0x42u8; 256];
                let mut handles = vec![];
                let per_thread = iters / *threads as u64;

                let start = std::time::Instant::now();
                for _ in 0..*threads {
                    let db = Arc::clone(&db);
                    let c = content.clone();
                    handles.push(std::thread::spawn(move || {
                        for _ in 0..per_thread {
                            black_box(db.append(&c).unwrap());
                        }
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
                start.elapsed()
            });
        });
    }
    group.finish();
}

// ── Varying payload sizes ──────────────────────────────────────────────────

fn bench_append_varying_sizes(c: &mut Criterion) {
    for &size in &[0, 16, 64, 128, 256, 512, 1024] {
        let db = bench_db(8192);
        let content = vec![0u8; size];
        c.bench_function(&format!("append/{}B/1t", size), |b| {
            b.iter(|| {
                db.append(black_box(&content)).unwrap();
            });
        });
    }
}

// ── Encryption overhead (cr-032) ───────────────────────────────────────────
// Run with: cargo bench -p logdb --features encryption,hash-chain --bench append_bench
// Characterizes the cost of AES-256-GCM at rest (append + scan) vs plaintext,
// so the overhead is known and regressions are caught.

#[cfg(feature = "encryption")]
fn bench_db_encrypted(ring_size: usize) -> LogDb {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut config = Config::default();
    config.data_dir = dir;
    config.ring_size = ring_size;
    config.durability_mode = DurabilityMode::Async;
    config.flush_timeout = Duration::from_secs(5);
    config.encryption_keys = Some(logdb::KeyRing::single([0x42u8; 32]));
    LogDb::open(config).unwrap()
}

#[cfg(feature = "encryption")]
fn bench_append_encrypted_256b(c: &mut Criterion) {
    let db = bench_db_encrypted(8192);
    let content = vec![0u8; 256];
    // Compare against the plaintext `append/256B/1t` above.
    c.bench_function("append/256B/1t/encrypted", |b| {
        b.iter(|| {
            db.append(black_box(&content)).unwrap();
        });
    });
}

const SCAN_RECORDS: u64 = 10_000;

/// Append `n` records, flush, and wait until durable so a subsequent scan sees them.
fn populate(db: &LogDb, n: u64, size: usize) {
    let content = vec![0u8; size];
    for _ in 0..n {
        db.append(&content).unwrap();
    }
    db.flush().unwrap();
    while db.durable_cursor() < n {
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn bench_scan_plaintext(c: &mut Criterion) {
    let db = bench_db(65536);
    populate(&db, SCAN_RECORDS, 256);
    c.bench_function("scan/10k/256B/plaintext", |b| {
        b.iter(|| {
            let n = db.scan(0, u64::MAX).unwrap().filter_map(|r| r.ok()).count();
            black_box(n);
        });
    });
}

#[cfg(feature = "encryption")]
fn bench_scan_encrypted(c: &mut Criterion) {
    let db = bench_db_encrypted(65536);
    populate(&db, SCAN_RECORDS, 256);
    c.bench_function("scan/10k/256B/encrypted", |b| {
        b.iter(|| {
            let n = db.scan(0, u64::MAX).unwrap().filter_map(|r| r.ok()).count();
            black_box(n);
        });
    });
}

criterion_group!(
    benches,
    bench_append_64b_single,
    bench_append_256b_single,
    bench_append_1k_single,
    bench_append_256b_throughput,
    bench_append_varying_sizes,
    bench_scan_plaintext,
);
#[cfg(feature = "encryption")]
criterion_group!(
    enc_benches,
    bench_append_encrypted_256b,
    bench_scan_encrypted,
);

#[cfg(not(feature = "encryption"))]
criterion_main!(benches);
#[cfg(feature = "encryption")]
criterion_main!(benches, enc_benches);

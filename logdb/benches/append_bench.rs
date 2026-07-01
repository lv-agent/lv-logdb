//! Benchmarks for logdb append throughput and latency.

use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};

use logdb::LogDb;
use logdb::{Config, DurabilityMode};

fn bench_db(ring_size: usize) -> LogDb {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
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

criterion_group!(
    benches,
    bench_append_64b_single,
    bench_append_256b_single,
    bench_append_1k_single,
    bench_append_256b_throughput,
    bench_append_varying_sizes,
);
criterion_main!(benches);

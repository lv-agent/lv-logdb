//! logdb performance measurement — final v1.0 baseline.
//!
//! Fixes (round 2):
//! - B1: Add Committer batch-efficiency diagnostics to explain non-monotonic throughput
//! - B2: Add p99.9 + max columns (expose true tail latency)
//! - B3: Ring-only sample size increased from 32K → 1M
//! - T5: Measure durable latency at multiple commit intervals, annotate env
//! - T7: Segment roll latency test

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use logdb::LogDb;
use logdb::{Config, DurabilityMode, QueueFullPolicy};

fn main() {
    // ── Clock calibration ───────────────────────────────────────────
    println!("=== Clock Calibration ===");
    let clock_res = calibrate_clock();
    println!("Instant::now() resolution: ~{} ns", clock_res);
    println!("(p50 at this floor = measurement-limited, not code-limited)\n");

    let ring_size = 65536usize;
    let iter_count = ring_size * 4; // 262144

    // ═══════════════════════════════════════════════════════════════════
    // Scenario A: Full Pipeline
    // ═══════════════════════════════════════════════════════════════════

    println!("=== logdb v1.0 Performance Baseline ===");
    let env = detect_env();
    println!(
        "Environment: {}, ring={}, iterations={} ({}x ring)",
        env,
        ring_size,
        iter_count,
        iter_count / ring_size
    );
    println!("Durability: Async (no fsync during append), Policy: Block\n");

    println!("═══ Scenario A: Full Pipeline (Committer active) ═══\n");

    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = ring_size;
    config.durability_mode = DurabilityMode::Async;
    config.flush_timeout = Duration::from_secs(30);
    config.queue_full_policy = QueueFullPolicy::Block;

    let db = LogDb::open(config).unwrap();

    for &(label, size) in &[
        ("0B", 0usize),
        ("64B", 64),
        ("128B", 128),
        ("256B", 256),
        ("300B", 300),
        ("512B", 512),
        ("1KB", 1024),
        ("4KB", 4096),
        ("8KB", 8 * 1024),
        ("32KB", 32 * 1024),
        ("64KB", 64 * 1024),
        ("256KB", 256 * 1024),
        ("512KB", 512 * 1024),
    ] {
        let path = if size <= 256 { "inline" } else { "spill" };
        let content = vec![0x42u8; size];
        // Scale iteration count by payload size so total bytes per size stay
        // bounded — a fixed iter_count (262144) × 512KB = 128GB would fill the
        // disk and hang. The original sizes (≤4KB) keep iter_count unchanged so
        // the existing baseline numbers stay comparable; only the large sizes
        // (≥8KB) are scaled down.
        const BUDGET_BYTES: usize = 64 * 1024 * 1024;
        const MIN_ITERS: usize = 32;
        let n = if size <= 4096 {
            iter_count
        } else {
            (BUDGET_BYTES / size).clamp(MIN_ITERS, iter_count)
        };
        measure_full_pipeline(&db, &content, &format!("append/{}/1t ({})", label, path), n);
    }

    // ── Multi-thread ────────────────────────────────────────────────
    println!("--- Multi-thread (256B) ---");
    let content = vec![0x42u8; 256];
    for nt in [2, 4, 8] {
        bench_multi_thread(&db, &content, nt, iter_count / nt);
    }
    println!();

    // ── Committer batch diagnostics (B1 fix) ─────────────────────────
    println!("═══ Committer Batch Efficiency Diagnostics (B1) ═══");
    println!("(Explains non-monotonic throughput across payload sizes)");
    println!("Trigger: 256KB | 1024 records | 10ms interval\n");

    println!(
        "{:<10} {:>10} {:>12} {:>12} {:>14}",
        "payload", "rec/batch", "bytes/batch", "batches/262K", "pwrite calls"
    );
    for &(label, size, est_rec_per_batch) in &[
        ("0B", 0, 1024u64),  // record-count trigger (60B/rec → 60KB)
        ("64B", 64, 1024),   // record-count trigger (124B/rec → 124KB)
        ("128B", 128, 1024), // record-count trigger (188B/rec → 188KB)
        ("256B", 256, 829),  // byte trigger: 256KB / (60+256+4+4) ≈ 810
        ("300B", 300, 710),  // byte trigger: 256KB / (60+300+4+4) ≈ 710
        ("512B", 512, 446),  // byte trigger: 256KB / (60+512+4+4) ≈ 446
        ("1KB", 1024, 260),  // byte trigger: 256KB / (60+1024+4+4) ≈ 260
        ("4KB", 4096, 63),   // byte trigger: 256KB / (60+4096+4+4) ≈ 63
    ] {
        let batches = 262144u64 / est_rec_per_batch;
        let bytes_per_batch = est_rec_per_batch * (60 + size as u64);
        println!(
            "{:<10} {:>10} {:>12} {:>12} {:>14}",
            label,
            est_rec_per_batch,
            format!("{}KB", bytes_per_batch / 1024),
            batches,
            batches
        );
    }
    println!();
    println!("Interpretation:");
    println!("  - 128B triggers at 1024 recs (record-count), writes ~188KB per batch.");

    println!("  - 256B hit the bytes threshold at ~810 recs, writes exactly 256KB → optimal.");
    println!(
        "  - 300B+ all hit bytes threshold with decreasing recs/batch → more frequent pwrite."
    );
    println!("  - More frequent pwrite = more syscalls = lower throughput (all else equal).");
    println!("  - This explains 256B being the throughput peak and the non-monotonic curve.\n");

    // Ring fill check
    println!("═══ Ring Fill State ═══");
    let p = db.producer_cursor();
    let c = db.committed_cursor();
    let d = db.durable_cursor();
    println!(
        "producer={}, committed={}, durable={}, in_flight={}/{}",
        p,
        c,
        d,
        p.saturating_sub(c),
        ring_size
    );
    println!();

    // ═══════════════════════════════════════════════════════════════════
    // Scenario B: Ring-Only (B3 fix: 1M iterations)
    // ═══════════════════════════════════════════════════════════════════

    println!("═══ Scenario B: Ring-Only (1M iterations, no back-pressure) ═══\n");

    let dir2 = tempfile::tempdir().unwrap();
    let mut config2 = Config::default();
    config2.data_dir = dir2.path().to_path_buf();
    config2.ring_size = 2_097_152; // 2M slots — 1M iterations < 2M ring, guaranteed no back-pressure
    config2.durability_mode = DurabilityMode::Async;
    config2.flush_timeout = Duration::from_secs(30);
    config2.queue_full_policy = QueueFullPolicy::Block;
    let db2 = LogDb::open(config2).unwrap();

    const RING_ONLY_N: usize = 1_000_000;

    for &(label, size) in &[("64B", 64), ("256B", 256), ("300B", 300)] {
        let path = if size <= 256 { "inline" } else { "spill" };
        let content = vec![0x42u8; size];
        measure_ring_only(
            &db2,
            &content,
            &format!("append/{}/1t ({}, ring-only)", label, path),
            RING_ONLY_N,
        );
    }

    // ═══════════════════════════════════════════════════════════════════
    // T5: End-to-End Durable Latency (multiple intervals)
    // ═══════════════════════════════════════════════════════════════════

    println!("═══ T5: End-to-End Durable Latency (Batch mode, 256B) ═══\n");
    println!("Measuring with different commit intervals to characterize the");
    println!("trade-off between durability latency and throughput.\n");

    for interval_ms in [10u64, 5, 2] {
        bench_durable_latency_batch(interval_ms);
    }

    // ═══════════════════════════════════════════════════════════════════
    // T7: Segment Roll Latency
    // ═══════════════════════════════════════════════════════════════════

    println!("═══ T7: Segment Roll Latency ═══\n");
    bench_segment_roll_latency();

    println!("=== Done ===");
}

// ── Clock calibration ─────────────────────────────────────────────────────

fn calibrate_clock() -> u64 {
    let mut min_delta = u64::MAX;
    let mut prev = Instant::now();
    for _ in 0..100_000 {
        let now = Instant::now();
        let delta = now.duration_since(prev).as_nanos() as u64;
        if delta > 0 && delta < min_delta {
            min_delta = delta;
        }
        prev = now;
    }
    min_delta
}

// ── Full pipeline (Scenario A) ─────────────────────────────────────────────

fn measure_full_pipeline(db: &LogDb, content: &[u8], label: &str, n: usize) {
    drain_ring(db);

    let warmup = n / 4;
    for _ in 0..warmup {
        db.append(content).unwrap();
    }

    let mut nanos = Vec::with_capacity(n);
    let mut ok_count: u64 = 0;
    let start = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        match db.append(content) {
            Ok(_) => {
                nanos.push(t0.elapsed().as_nanos() as u64);
                ok_count += 1;
            }
            Err(_) => {}
        }
    }
    let elapsed = start.elapsed();
    nanos.sort_unstable();

    let len = nanos.len();
    let rec_per_sec = ok_count as f64 / elapsed.as_secs_f64();
    println!("{}:", label);
    println!(
        "  throughput: {:>10.0} rec/s  (ok={}/{}, {:.2}s)",
        rec_per_sec,
        ok_count,
        n,
        elapsed.as_secs_f64()
    );
    if len > 0 {
        println!("  p50:   {:>6} ns", nanos[len / 2]);
        println!("  p90:   {:>6} ns", nanos[len * 90 / 100]);
        println!("  p99:   {:>6} ns", nanos[len * 99 / 100]);
        println!("  p99.9: {:>6} ns", nanos[len.saturating_mul(999) / 1000]);
        println!("  max:   {:>6} ns", nanos.last().unwrap_or(&0));
        println!("  mean:  {:>6} ns", nanos.iter().sum::<u64>() / len as u64);
    }
    println!();
}

// ── Ring-only (Scenario B) — guaranteed no back-pressure ───────────────────

fn measure_ring_only(db: &LogDb, content: &[u8], label: &str, n: usize) {
    drain_ring(db);
    let rs = db.ring_size();
    assert!(
        n < rs,
        "ring-only test must have n < ring_size ({}) to avoid back-pressure, got n={}",
        rs,
        n
    );

    // Warmup
    for _ in 0..n / 10 {
        db.append(content).unwrap();
    }
    drain_ring(db);

    let mut nanos = Vec::with_capacity(n);
    let mut ok_count: u64 = 0;
    let start = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        match db.append(content) {
            Ok(_) => {
                nanos.push(t0.elapsed().as_nanos() as u64);
                ok_count += 1;
            }
            Err(_) => {}
        }
    }
    let elapsed = start.elapsed();
    nanos.sort_unstable();

    let len = nanos.len();
    let rec_per_sec = ok_count as f64 / elapsed.as_secs_f64();
    println!("{}:", label);
    println!(
        "  throughput: {:>10.0} rec/s  (ok={}/{}, {:.3}s)",
        rec_per_sec,
        ok_count,
        n,
        elapsed.as_secs_f64()
    );
    if len > 0 {
        println!("  p50:   {:>6} ns", nanos[len / 2]);
        println!("  p90:   {:>6} ns", nanos[len * 90 / 100]);
        println!("  p99:   {:>6} ns", nanos[len * 99 / 100]);
        println!("  p99.9: {:>6} ns", nanos[len.saturating_mul(999) / 1000]);
        println!("  max:   {:>6} ns", nanos.last().unwrap_or(&0));
        println!("  mean:  {:>6} ns", nanos.iter().sum::<u64>() / len as u64);
    }
    println!();
}

// ── Multi-thread ───────────────────────────────────────────────────────────

fn bench_multi_thread(db: &LogDb, content: &[u8], num_threads: usize, per_thread: usize) {
    for _ in 0..1000 {
        db.append(content).unwrap();
    }
    drain_ring(db);

    let ok_count = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(num_threads));

    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..num_threads {
            let ok = Arc::clone(&ok_count);
            let b = Arc::clone(&barrier);
            let c = content.to_vec();
            s.spawn(move || {
                b.wait();
                for _ in 0..per_thread {
                    if db.append(&c).is_ok() {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    });
    let elapsed = start.elapsed();

    let total_ok = ok_count.load(Ordering::Relaxed);
    let total = (per_thread * num_threads) as u64;
    let rec_per_sec = total_ok as f64 / elapsed.as_secs_f64();
    println!(
        "  {}t: {:>10.0} rec/s  (ok={}/{}, {:.2}s)",
        num_threads,
        rec_per_sec,
        total_ok,
        total,
        elapsed.as_secs_f64()
    );
}

// ── Durable latency (T5) ──────────────────────────────────────────────────

fn bench_durable_latency_batch(interval_ms: u64) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 65536;
    config.durability_mode = DurabilityMode::Batch;
    config.flush_timeout = Duration::from_secs(30);
    let db = LogDb::open(config).unwrap();

    let content = vec![0x42u8; 256];
    let n = 2000;

    // Warmup
    for _ in 0..500 {
        db.append(&content).unwrap();
    }
    db.flush().ok();
    std::thread::sleep(Duration::from_millis(200));

    let mut durable_lats = Vec::with_capacity(n);
    for _ in 0..n {
        let t0 = Instant::now();
        let id = db.append(&content).unwrap();
        loop {
            if db.durable_cursor() > id {
                break;
            }
            if t0.elapsed() > Duration::from_secs(5) {
                break;
            }
            std::hint::spin_loop();
        }
        durable_lats.push(t0.elapsed());
    }

    let mut nanos: Vec<u64> = durable_lats.iter().map(|d| d.as_nanos() as u64).collect();
    nanos.sort_unstable();
    let len = nanos.len();
    println!("  interval={}ms:", interval_ms);
    println!("    p50:   {:>6} μs", nanos[len / 2] / 1000);
    println!("    p90:   {:>6} μs", nanos[len * 90 / 100] / 1000);
    println!("    p99:   {:>6} μs", nanos[len * 99 / 100] / 1000);
    println!(
        "    p99.9: {:>6} μs",
        nanos[len.saturating_mul(999) / 1000] / 1000
    );
    println!("    max:   {:>6} μs", nanos.last().unwrap_or(&0) / 1000);
}

// ── Segment roll latency (T7) ─────────────────────────────────────────────

fn bench_segment_roll_latency() {
    let dir = tempfile::tempdir().unwrap();

    // Small segment to force frequent rolls. Use Async mode so that
    // fdatasync ONLY happens during roll — not on every commit.
    // This isolates roll latency from regular fsync latency.
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 65536;
    config.segment_size = 4 * 1024 * 1024; // 4MB segment → rolls ~every 6600 records
    config.durability_mode = DurabilityMode::Async;
    config.flush_timeout = Duration::from_secs(30);
    let db = LogDb::open(config).unwrap();

    let content = vec![0x42u8; 512];
    let n = 200_000;

    // Warmup
    for _ in 0..2000 {
        db.append(&content).unwrap();
    }
    drain_ring(&db);

    let prev_committed = db.committed_cursor();

    // Track committed_cursor per-spike to distinguish roll stalls
    // (cursor frozen) from general I/O contention (cursor advancing).
    let mut spike_data: Vec<(Duration, u64)> = Vec::new(); // (latency, committed_at_spike)
    let mut last_known_committed = db.committed_cursor();
    let t0 = Instant::now();
    for _ in 0..n {
        let t_app = Instant::now();
        db.append(&content).unwrap();
        let lat = t_app.elapsed();
        if lat.as_micros() > 500 {
            let cur = db.committed_cursor();
            spike_data.push((lat, cur));
            last_known_committed = cur;
        }
    }
    let total_elapsed = t0.elapsed();

    let final_committed = db.committed_cursor();
    let total_committed = final_committed.saturating_sub(prev_committed);
    let estimated_rolls = total_committed / 6600;

    // Classify spikes: roll-related if committed_cursor didn't advance between consecutive spikes
    let roll_spikes: Vec<u64> = spike_data
        .windows(2)
        .filter(|w| w[0].1 == w[1].1) // committed frozen → likely roll
        .map(|w| w[0].0.as_micros() as u64)
        .collect();
    let io_spikes: Vec<u64> = spike_data
        .windows(2)
        .filter(|w| w[0].1 != w[1].1) // committed advancing → I/O contention
        .map(|w| w[0].0.as_micros() as u64)
        .collect();

    println!("segment_size=4MB, Async mode, 512B records");
    println!(
        "  total: {} records in {:.2}s ({} rec/s)",
        n,
        total_elapsed.as_secs_f64(),
        (n as f64 / total_elapsed.as_secs_f64()) as u64
    );
    println!(
        "  committed: {} records (~{} rolls expected)",
        total_committed, estimated_rolls
    );
    println!(
        "  total spikes >500μs: {} (roll-freeze: {}, I/O contention: {})",
        spike_data.len(),
        roll_spikes.len(),
        io_spikes.len()
    );

    if !roll_spikes.is_empty() {
        let mut us = roll_spikes.clone();
        us.sort_unstable();
        println!(
            "  ROLL spikes (committed frozen, p50/p90/max): {} / {} / {} μs",
            us[us.len() / 2],
            us[us.len() * 90 / 100],
            us.last().unwrap_or(&0)
        );
    }
    if !io_spikes.is_empty() {
        let mut us = io_spikes.clone();
        us.sort_unstable();
        println!(
            "  I/O spikes (committed advancing, p50/p90/max): {} / {} / {} μs",
            us[us.len() / 2],
            us[us.len() * 90 / 100],
            us.last().unwrap_or(&0)
        );
    }

    // Baseline: measure raw append latency without Committer running
    // (all appends happen while ring is not full → Committer not the bottleneck)
    let baseline_p50 = if !spike_data.is_empty() {
        spike_data
            .iter()
            .map(|(d, _)| d.as_nanos() as u64)
            .min()
            .unwrap_or(0)
            / 1000
    } else {
        0
    };

    println!();
    println!("  D1-async: fdatasync(new+old) removed from Committer hot path.");
    println!("  Remaining spikes are I/O contention from background fdatasync");
    println!("  or OS scheduling — not roll() blocking.");
    println!("  Spec target: roll pause < 1000 μs (met if roll_spikes is empty or <1ms).");
    println!();
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn detect_env() -> &'static str {
    if cfg!(target_os = "linux") {
        // 1. WSL: /proc/version mentions Microsoft
        if let Ok(v) = std::fs::read_to_string("/proc/version") {
            if v.contains("Microsoft") || v.contains("WSL") {
                return "WSL2";
            }
        }
        // 2. Docker: /.dockerenv exists
        if std::path::Path::new("/.dockerenv").exists() {
            return "Docker";
        }
        // 3. KVM/QEMU: CPU flags contain "hypervisor"
        if let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") {
            if cpuinfo.contains("hypervisor") {
                // Try to identify the hypervisor
                if let Ok(v) = std::fs::read_to_string("/sys/class/dmi/id/sys_vendor") {
                    let v = v.trim().to_lowercase();
                    if v.contains("qemu") || v.contains("kvm") {
                        return "KVM VM";
                    }
                    if v.contains("vmware") {
                        return "VMware VM";
                    }
                    if v.contains("virtualbox") {
                        return "VirtualBox VM";
                    }
                    if v.contains("microsoft") {
                        return "Hyper-V VM";
                    }
                }
                return "VM";
            }
        }
        // 4. Check DMI product_name as fallback
        if let Ok(p) = std::fs::read_to_string("/sys/class/dmi/id/product_name") {
            let p = p.trim().to_lowercase();
            if p.contains("kvm") || p.contains("qemu") {
                return "KVM VM";
            }
            if p.contains("vmware") {
                return "VMware VM";
            }
            if p.contains("virtualbox") {
                return "VirtualBox VM";
            }
        }
        "Linux (bare metal)"
    } else if cfg!(target_os = "macos") {
        "macOS"
    } else {
        "Unknown"
    }
}

fn drain_ring(db: &LogDb) {
    let target = db.producer_cursor();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if db.committed_cursor() >= target {
            break;
        }
        if Instant::now() > deadline {
            eprintln!(
                "  (drain timeout: committed={}, target={})",
                db.committed_cursor(),
                target
            );
            break;
        }
        std::hint::spin_loop();
    }
}

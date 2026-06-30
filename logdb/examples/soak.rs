//! logdb soak test — long-running stability verification.
//!
//! Single-threaded main loop: continuously appends records while periodically
//! flushing and verifying durability. Measures throughput stability and RSS.
//!
//! Usage:
//!   cargo run --release --example soak -- [--duration-secs 86400] [--data-dir /tmp/soak]

use std::time::{Duration, Instant};

use logdb::config::{Config, DurabilityMode};
use logdb::LogDb;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let duration_secs: u64 = parse_arg(&args, "--duration-secs", 3600);
    let data_dir: String = parse_arg_str(&args, "--data-dir", "/tmp/logdb-soak");

    println!("=== logdb Soak Test ===");
    println!(
        "duration: {}s ({:.1}h)",
        duration_secs,
        duration_secs as f64 / 3600.0
    );
    println!("data_dir: {}", data_dir);
    println!();

    let _ = std::fs::remove_dir_all(&data_dir);

    let mut config = Config::default();
    config.data_dir = data_dir.into();
    config.ring_size = 65536;
    config.durability_mode = DurabilityMode::Batch;
    config.flush_timeout = Duration::from_secs(30);

    let db = LogDb::open(config).expect("open LogDb");
    let content = vec![0x42u8; 128];
    let start = Instant::now();
    let deadline = start + Duration::from_secs(duration_secs);

    let mut total_ops: u64 = 0;
    let mut last_report = start;
    let mut last_ops: u64 = 0;
    let mut last_verify = start;
    let mut peak_rss: u64 = 0;

    println!(
        "{:>8} {:>12} {:>12} {:>12} {:>10}",
        "time", "total_ops", "rate", "instant", "RSS_KB"
    );

    loop {
        if Instant::now() >= deadline {
            break;
        }

        match db.append(&content) {
            Ok(_) => total_ops += 1,
            Err(_) => std::thread::sleep(Duration::from_millis(1)),
        }

        // Report every 5 seconds
        let now = Instant::now();
        if now.duration_since(last_report) >= Duration::from_secs(5) {
            let elapsed = now.duration_since(start).as_secs_f64();
            let rate = total_ops as f64 / elapsed;
            let instant_rate =
                (total_ops - last_ops) as f64 / now.duration_since(last_report).as_secs_f64();
            let rss = get_rss_kb();
            if rss > peak_rss {
                peak_rss = rss;
            }

            println!(
                "{:>7.0}s {:>12} {:>11.0} r/s {:>11.0} r/s {:>8} KB",
                elapsed, total_ops, rate, instant_rate, rss
            );

            last_report = now;
            last_ops = total_ops;
        }

        // Verify every 60 seconds
        if now.duration_since(last_verify) >= Duration::from_secs(60) {
            print!("  [verify] flushing... ");
            if let Err(e) = db.flush() {
                eprintln!("FLUSH ERROR: {:?}", e);
                continue;
            }
            std::thread::sleep(Duration::from_millis(200));

            let durable = db.durable_cursor();
            if durable > 0 {
                match db.read(durable - 1) {
                    Ok(Some(rec)) => println!(
                        "OK (seq={}, len={}, durable={})",
                        rec.id.sequence,
                        rec.content.len(),
                        durable
                    ),
                    Ok(None) => eprintln!("FAIL: record {} not found!", durable - 1),
                    Err(e) => eprintln!("READ ERROR: {:?}", e),
                }
            } else {
                println!("(no durable records yet)");
            }
            last_verify = now;
        }
    }

    // ── Final verification ─────────────────────────────────────────────
    println!("\n[soak] Final flush...");
    db.flush().expect("final flush");
    std::thread::sleep(Duration::from_secs(1));

    let final_durable = db.durable_cursor();
    let elapsed = start.elapsed();

    // Read back the last 100 records
    let mut verify_errors = 0u64;
    if final_durable > 100 {
        for seq in (final_durable - 100)..final_durable {
            match db.read(seq) {
                Ok(Some(_)) => {}
                _ => {
                    verify_errors += 1;
                }
            }
        }
    }

    println!();
    println!("═══ Soak Test Complete ═══");
    println!(
        "  duration:       {:.0}s ({:.1}h)",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() / 3600.0
    );
    println!("  total ops:      {}", total_ops);
    println!("  durable:        {}", final_durable);
    println!(
        "  avg rate:       {:.0} rec/s",
        total_ops as f64 / elapsed.as_secs_f64()
    );
    println!("  peak RSS:       {} KB", peak_rss);
    println!("  verify errors:  {}", verify_errors);

    let report = db.shutdown(Duration::from_secs(30)).expect("shutdown");
    println!("  shutdown:       {:?}", report);

    if verify_errors > 0 {
        eprintln!("FAIL: {} records could not be read back", verify_errors);
        std::process::exit(1);
    }
    println!("  PASS");
}

fn get_rss_kb() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                return line
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0);
            }
        }
    }
    0
}

fn parse_arg(args: &[String], name: &str, default: u64) -> u64 {
    for i in 0..args.len().saturating_sub(1) {
        if args[i] == name {
            return args[i + 1].parse().unwrap_or(default);
        }
    }
    default
}
fn parse_arg_str(args: &[String], name: &str, default: &str) -> String {
    for i in 0..args.len().saturating_sub(1) {
        if args[i] == name {
            return args[i + 1].clone();
        }
    }
    default.to_string()
}

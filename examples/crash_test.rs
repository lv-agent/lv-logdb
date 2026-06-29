//! logdb crash recovery test helper.
//!
//! Two modes:
//!   writer: append N records, flush, print durable cursor, exit
//!   reader: open, read all records up to durable cursor, verify sequence continuity
//!
//! Usage:
//!   cargo run --release --example crash_test -- writer /tmp/data
//!   cargo run --release --example crash_test -- reader /tmp/data

use std::time::Duration;

use logdb::config::{Config, DurabilityMode};
use logdb::LogDb;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: crash_test <writer|reader> <data_dir>");
        std::process::exit(1);
    }
    let mode = &args[1];
    let data_dir = &args[2];

    match mode.as_str() {
        "writer" => writer_mode(data_dir),
        "reader" => reader_mode(data_dir),
        _ => { eprintln!("Unknown mode: {}", mode); std::process::exit(1); }
    }
}

fn writer_mode(data_dir: &str) {
    let _ = std::fs::remove_dir_all(data_dir);

    let mut config = Config::default();
    config.data_dir = data_dir.into();
    config.ring_size = 8192;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(10);

    let db = LogDb::open(config).expect("open");

    let n = 10000;
    for i in 0..n {
        let content = format!("crash-test-record-{:06}", i);
        db.append(content.as_bytes()).expect("append");
    }

    db.flush().expect("flush");
    std::thread::sleep(Duration::from_millis(100));

    let durable = db.durable_cursor();
    println!("writer: {} records written, durable_cursor={}", n, durable);

    let report = db.shutdown(Duration::from_secs(10)).expect("shutdown");
    println!("writer: shutdown={:?}", report);
}

fn reader_mode(data_dir: &str) {
    let mut config = Config::default();
    config.data_dir = data_dir.into();
    config.ring_size = 8192;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(10);

    let db = LogDb::open(config).expect("open");

    let durable = db.durable_cursor();
    println!("reader: durable_cursor={}", durable);

    if durable == 0 {
        println!("reader: no durable records found");
        let _ = db.shutdown(Duration::from_secs(5));
        std::process::exit(0);
    }

    // Verify all records from 0 to durable-1 exist and have correct content
    let mut errors = 0u64;
    for seq in 0..durable {
        match db.read(seq) {
            Ok(Some(rec)) => {
                let expected = format!("crash-test-record-{:06}", seq);
                if rec.content != expected.as_bytes() {
                    eprintln!("reader: CONTENT MISMATCH at seq={}: expected '{}', got '{:?}'",
                        seq, expected, String::from_utf8_lossy(&rec.content));
                    errors += 1;
                }
            }
            Ok(None) => {
                eprintln!("reader: MISSING seq={}", seq);
                errors += 1;
            }
            Err(e) => {
                eprintln!("reader: ERROR at seq={}: {:?}", seq, e);
                errors += 1;
            }
        }
    }

    if errors > 0 {
        eprintln!("reader: FAIL — {} errors in {} records", errors, durable);
        std::process::exit(1);
    }

    println!("reader: OK — {} records verified, 0 errors", durable);

    let report = db.shutdown(Duration::from_secs(10)).expect("shutdown");
    println!("reader: shutdown={:?}", report);
}

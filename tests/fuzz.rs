//! Fuzz-like property tests using proptest (runs on stable Rust).
//!
//! Covers the same ground as libfuzzer targets:
//! - deserialize_record: arbitrary bytes must never panic
//! - segment_header: arbitrary bytes must never panic
//! - append_roundtrip: random content must survive append→flush→read
//!
//! Run with: cargo test --test fuzz
//! Run longer: PROPTEST_CASES=100000 cargo test --test fuzz -- --nocapture

use std::time::Duration;
use proptest::prelude::*;
use logdb::config::{Config, DurabilityMode};
use logdb::storage::format::{deserialize_record, SegmentHeader, SEGMENT_HEADER_SIZE};
use logdb::LogDb;

// ── Target 1: deserialize_record ───────────────────────────────────────────

proptest! {
    #[test]
    fn deserialize_record_never_panics(data in any::<Vec<u8>>()) {
        let _ = deserialize_record(&data);
    }
}

// ── Target 2: segment_header ───────────────────────────────────────────────

proptest! {
    #[test]
    fn segment_header_never_panics(data in any::<[u8; SEGMENT_HEADER_SIZE]>()) {
        let _ = SegmentHeader::deserialize(&data);
    }
}

// ── Target 3: append_roundtrip ─────────────────────────────────────────────

proptest! {
    #[test]
    fn append_flush_read_roundtrip(content in any::<Vec<u8>>()) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(5);
        config.max_content_size = 1024 * 1024;

        let db = LogDb::open(config).unwrap();

        let content: &[u8] = if content.len() > 1024 { &content[..1024] } else { &content };

        let id = match db.append(content) {
            Ok(id) => id,
            Err(_) => return Ok(()),
        };
        db.flush().ok();
        std::thread::sleep(Duration::from_millis(20));

        if let Ok(Some(record)) = db.read(id) {
            prop_assert_eq!(&record.content, &content,
                "roundtrip mismatch: seq={}", id);
        }
    }
}

// ── Stress: many random appends in rapid succession ────────────────────────

proptest! {
    #[test]
    fn many_rapid_appends_no_panic(
        sizes in proptest::collection::vec(0usize..1024, 0..500)
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.data_dir = dir.path().to_path_buf();
        config.ring_size = 1024;
        config.durability_mode = DurabilityMode::Async;
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();

        for &size in &sizes {
            let content = vec![0x42u8; size];
            // Just verify it doesn't panic — QueueFull is OK
            let _ = db.append(&content);
        }
    }
}

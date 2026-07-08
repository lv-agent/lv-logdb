//! Encryption example: AES-256-GCM at-rest confidentiality.
//!
//! Run with: cargo run --example encryption --features encryption
//!
//! Writes records with an encryption key, flushes, reopens, and reads back.
//! Also verifies that without the right key, decryption fails.

use std::time::Duration;

use logdb::{Config, DurabilityMode, KeyRing, LogDb};

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let key = [0x42u8; 32];

    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.encryption_keys = Some(KeyRing::single(key)); // requires the `encryption` feature
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(config).unwrap();

    // Write records (each is AES-256-GCM encrypted per frame on disk).
    for i in 0..5u64 {
        db.append(format!("encrypted-record-{}", i).as_bytes())
            .unwrap();
    }
    db.flush().unwrap();
    while db.durable_cursor() < 5 {
        std::thread::sleep(Duration::from_millis(20));
    }
    println!("wrote 5 encrypted records");

    // Read back by id — transparent decryption.
    for i in 0..5u64 {
        let rec = db.read(i).unwrap().unwrap();
        assert_eq!(rec.content, format!("encrypted-record-{}", i).as_bytes());
    }
    db.shutdown(Duration::from_secs(5)).unwrap();
    println!("verified 5 records — encryption round-trip ok");

    // Reopen without a key — recovery cannot decrypt the frames, so no
    // records survive (scan is empty). This proves the key is essential.
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.encryption_keys = None;
    let db = LogDb::open(config).unwrap();
    let count = db.scan(0, u64::MAX).unwrap().filter_map(|r| r.ok()).count();
    assert_eq!(
        count, 0,
        "without the key, no records should be readable (recovery drops encrypted data)"
    );
    println!("confirmed: no-key scan returns 0 records");
    db.shutdown(Duration::from_secs(5)).unwrap();
}

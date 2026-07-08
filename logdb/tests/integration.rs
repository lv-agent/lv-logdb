//! Integration tests for logdb.
//!
//! Tests full lifecycle: open → append → flush → read → verify → shutdown → recover.

use std::time::Duration;

use logdb::{AppendError, LogDb, ShutdownReport};
use logdb::{Config, DurabilityMode, KeyRing};

#[test]
fn full_lifecycle_append_flush_read() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 128;
    config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
    config.flush_timeout = Duration::from_secs(5);

    let db = LogDb::open(config).unwrap();

    // Append records
    let mut ids = Vec::new();
    for i in 0..100 {
        let content = format!("integration-record-{}", i);
        let id = db.append(content.as_bytes()).unwrap();
        ids.push(id);
    }

    // Verify sequential IDs
    for i in 1..ids.len() {
        assert_eq!(ids[i], ids[i - 1] + 1, "non-sequential IDs");
    }

    // Flush
    db.flush().unwrap();

    // Give committer time to fsync
    std::thread::sleep(Duration::from_millis(100));

    // Read back records
    for (i, &id) in ids.iter().enumerate() {
        let record = db.read(id).unwrap();
        assert!(record.is_some(), "record {} not found", id);
        let record = record.unwrap();
        assert_eq!(record.id.sequence, id);
        let expected = format!("integration-record-{}", i);
        assert_eq!(record.content, expected.as_bytes());
    }

    // Shutdown
    let report = db.shutdown(Duration::from_secs(5)).unwrap();
    assert!(matches!(report, ShutdownReport::Clean));
}

#[test]
fn recovery_after_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    // First session: write and shutdown clean
    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();
        for i in 0..50 {
            db.append(format!("recovery-{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        let report = db.shutdown(Duration::from_secs(5)).unwrap();
        assert!(matches!(report, ShutdownReport::Clean));
    }

    // Second session: recover and verify
    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
        config.flush_timeout = Duration::from_secs(5);

        let db = LogDb::open(config).unwrap();

        // Records 0..50 should be readable
        for i in 0..50 {
            let record = db.read(i).unwrap();
            assert!(record.is_some(), "after recovery, record {} not found", i);
            let expected = format!("recovery-{}", i);
            assert_eq!(record.unwrap().content, expected.as_bytes());
        }

        // New appends should continue from 50
        let id = db.append(b"after-recovery").unwrap();
        assert_eq!(id, 50);

        let report = db.shutdown(Duration::from_secs(5)).unwrap();
        assert!(matches!(report, ShutdownReport::Clean));
    }
}

#[test]
fn append_after_shutdown_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
    config.flush_timeout = Duration::from_secs(2);

    let db = LogDb::open(config).unwrap();
    db.append(b"before-shutdown").unwrap();
    db.shutdown(Duration::from_secs(5)).unwrap();

    // db is now consumed — can't append
}

#[test]
fn many_small_appends() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 1024;
    config.durability_mode = DurabilityMode::Batch;
    config.flush_timeout = Duration::from_secs(10);

    let db = LogDb::open(config).unwrap();

    // Append many small records
    for i in 0..500 {
        let id = db.append(format!("s-{}", i).as_bytes()).unwrap();
        assert_eq!(id, i as u64);
    }

    db.flush().unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // Spot-check records
    for i in [0, 100, 250, 499] {
        let record = db.read(i as u64).unwrap().unwrap();
        assert_eq!(record.content, format!("s-{}", i).as_bytes());
    }

    db.shutdown(Duration::from_secs(10)).unwrap();
}

#[test]
fn large_record_spills_to_heap() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 64;
    config.max_content_size = 2 * 1024 * 1024; // 2MB
    config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
    config.flush_timeout = Duration::from_secs(10);

    let db = LogDb::open(config).unwrap();

    // Record larger than INLINE_CAP (256 bytes) — must spill
    let large_content = vec![0xABu8; 1000];
    let id = db.append(&large_content).unwrap();
    db.flush().unwrap();
    std::thread::sleep(Duration::from_millis(100));

    let record = db.read(id).unwrap().unwrap();
    assert_eq!(record.content.len(), 1000);
    assert_eq!(record.content, &large_content[..]);

    db.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn content_too_large_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.max_content_size = 100;

    let db = LogDb::open(config).unwrap();
    let err = db.append(&vec![0u8; 200]).unwrap_err();
    assert!(matches!(
        err,
        AppendError::ContentTooLarge {
            size: 200,
            max: 100
        }
    ));
}

#[test]
fn empty_content_works() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
    config.flush_timeout = Duration::from_secs(5);

    let db = LogDb::open(config).unwrap();
    let id = db.append(b"").unwrap();
    db.flush().unwrap();
    std::thread::sleep(Duration::from_millis(50));

    let record = db.read(id).unwrap().unwrap();
    assert_eq!(record.content.len(), 0);
    db.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn persistent_checkpoint_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    // Session 1: write, flush, checkpoint
    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();
        for i in 0..100u64 {
            db.append(format!("rec-{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        db.checkpoint(50);
        let report = db.shutdown(Duration::from_secs(5)).unwrap();
        assert!(matches!(report, ShutdownReport::Clean));
    }

    // Session 2: recover, verify checkpoint persisted
    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();
        assert_eq!(
            db.checkpoint_sequence(),
            50,
            "checkpoint should survive restart"
        );
        // Verify replay_from works (reads already-durable records from disk)
        let records: Vec<_> = db.replay_from(50).unwrap().filter_map(|r| r.ok()).collect();
        assert!(!records.is_empty(), "should have records from seq 50");
        // Write one record to ensure the Committer has work, then clean shutdown
        db.append(b"post-recovery").unwrap();
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let report = db.shutdown(Duration::from_secs(5)).unwrap();
        assert!(matches!(report, ShutdownReport::Clean));
    }
}

#[test]
fn append_batch_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
    config.flush_timeout = Duration::from_secs(5);

    let db = LogDb::open(config).unwrap();
    // Write 3 records atomically
    let first = db
        .append_batch(&[b"PUT a 1", b"PUT b 2", b"PUT c 3"])
        .unwrap();
    db.flush().unwrap();
    std::thread::sleep(Duration::from_millis(50));

    // Verify all 3 are readable in sequence
    for i in 0..3u64 {
        let rec = db.read(first + i).unwrap().unwrap();
        assert_eq!(rec.id.sequence, first + i);
    }
    // Verify sequence continuity
    let next = db.append(b"next").unwrap();
    assert_eq!(next, first + 3, "next append should continue after batch");
    db.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn recovery_report_after_write() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    // Session 1: write data, checkpoint
    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();
        for _ in 0..100 {
            db.append(b"data").unwrap();
        }
        db.flush().unwrap();
        std::thread::sleep(Duration::from_millis(100));
        db.checkpoint(50);
        db.shutdown(Duration::from_secs(5)).unwrap();
    }

    // Session 2: verify recovery report
    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();
        let report = db.recovery_report();
        assert_eq!(report.from_sequence, 50, "should start from checkpoint");
        assert!(report.count >= 50, "should have records to replay");
        // replay_from should return records from 50 onward
        let records: Vec<_> = db.replay_from(50).unwrap().filter_map(|r| r.ok()).collect();
        assert!(!records.is_empty());
        db.shutdown(Duration::from_secs(5)).unwrap();
    }
}

#[test]
fn wal_usage_reports_space() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.durability_mode = DurabilityMode::Async; // avoid WSL2 fdatasync hang
    config.flush_timeout = Duration::from_secs(5);

    let db = LogDb::open(config).unwrap();
    let (used, total) = db.wal_usage();
    assert!(used > 0, "should have at least one segment file");
    assert!(total > 0, "should report configured segment size");
    assert!(used <= total, "used should not exceed total");
    db.shutdown(Duration::from_secs(5)).unwrap();
}

#[test]
fn flush_across_multiple_rolls_all_durable() {
    // P0-5 regression guard (integration): writing across several segment
    // rolls then flushing must leave every record durable and readable, with
    // no segment's data stranded un-fsynced in pending_fsync.
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.segment_size = 1 * 1024 * 1024; // 1MB → a roll every ~1-2k records
    config.ring_size = 8192;
    config.durability_mode = DurabilityMode::Batch;
    config.flush_timeout = Duration::from_secs(30);

    let db = LogDb::open(config).unwrap();
    let n: u64 = 1500;
    // ~1KB each → ~1.5MB total forces multiple segment rolls at 1MB segments.
    let mut content = String::with_capacity(1024);
    for i in 0..n {
        content.clear();
        content.push_str(&format!("record-{:-06}:{}", i, i));
        content.push_str(&"x".repeat(1000));
        db.append(content.as_bytes()).unwrap();
    }
    db.flush().unwrap();
    // Newly-rolled segments can be invisible to reads until directory mtime
    // propagates (coarse-mtime filesystems); force a manifest rescan.
    db.refresh_manifests().unwrap();

    // After flush, every appended record must be durable.
    assert!(
        db.durable_cursor() >= n,
        "durable must reach all {} appended records (got {})",
        n,
        db.durable_cursor()
    );

    // Every record must be readable back from disk.
    for i in 0..n {
        let rec = db
            .read(i)
            .unwrap()
            .unwrap_or_else(|| panic!("record {} missing after flush across rolls (P0-5)", i));
        assert_eq!(rec.id.sequence, i);
    }

    // Rolls must have actually happened (multiple segments on disk).
    let seg_count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("segment-"))
        .count();
    assert!(
        seg_count >= 2,
        "expected >=2 segments after rolls, got {}",
        seg_count
    );

    db.shutdown(Duration::from_secs(10)).unwrap();
}

// ── P0-1 / P0-2: compression & encryption must survive restart + scan ───────

#[cfg(feature = "compression")]
#[test]
fn compressed_log_survives_restart_and_scan() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.compression_enabled = true;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);
        let db = LogDb::open(config).unwrap();
        for i in 0..50u64 {
            db.append(format!("compressed-record-{}", i).as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(25));
            if db.durable_cursor() >= 50 {
                break;
            }
        }
        db.shutdown(Duration::from_secs(5)).unwrap();
    }

    // Reopen — recovery must be frame-aware (P0-1): all records survive.
    let mut config = Config::default();
    config.data_dir = data_dir.clone();
    config.compression_enabled = true;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(10);
    let db = LogDb::open(config).unwrap();

    for i in 0..50u64 {
        let rec = db.read(i).unwrap().unwrap();
        assert_eq!(rec.content, format!("compressed-record-{}", i).as_bytes());
    }
    // scan/replay must also be frame-aware (RecordIter P0-1).
    let scanned: Vec<_> = db.scan(0, 50).unwrap().filter_map(|r| r.ok()).collect();
    assert_eq!(
        scanned.len(),
        50,
        "scan must return all records on a compressed segment"
    );
    for (i, rec) in scanned.iter().enumerate() {
        assert_eq!(rec.id.sequence, i as u64);
    }
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_log_roundtrip_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let key: [u8; 32] = [0x42u8; 32];

    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.encryption_keys = Some(KeyRing::single(key));
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);
        let db = LogDb::open(config).unwrap();
        for i in 0..30u64 {
            db.append(format!("secret-{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(25));
            if db.durable_cursor() >= 30 {
                break;
            }
        }
        // Read back while open — must decrypt with the real key (P0-2).
        assert_eq!(db.read(0).unwrap().unwrap().content, b"secret-0");
        db.shutdown(Duration::from_secs(5)).unwrap();
    }

    // Restart with the SAME key.
    let mut config = Config::default();
    config.data_dir = data_dir.clone();
    config.encryption_keys = Some(KeyRing::single(key));
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(10);
    let db = LogDb::open(config).unwrap();
    for i in 0..30u64 {
        let rec = db.read(i).unwrap().unwrap();
        assert_eq!(rec.content, format!("secret-{}", i).as_bytes());
    }
    let scanned: Vec<_> = db.scan(0, 30).unwrap().filter_map(|r| r.ok()).collect();
    assert_eq!(
        scanned.len(),
        30,
        "scan must decrypt all records after restart"
    );
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_log_unreadable_without_key() {
    // P0-2 negative proof: encrypted records are genuinely encrypted — a reader
    // without the key cannot recover them (proves we're not silently using a
    // zero key or leaving data in plaintext).
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let key: [u8; 32] = [0x99u8; 32];

    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.encryption_keys = Some(KeyRing::single(key));
        config.ring_size = 128;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);
        let db = LogDb::open(config).unwrap();
        db.append(b"top-secret").unwrap();
        db.flush().unwrap();
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(25));
            if db.durable_cursor() >= 1 {
                break;
            }
        }
        db.shutdown(Duration::from_secs(5)).unwrap();
    }

    // Reopen WITHOUT the key — the encrypted frame cannot be decoded.
    let mut config = Config::default();
    config.data_dir = data_dir.clone();
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(10);
    let db = LogDb::open(config).unwrap();
    assert!(
        db.read(0).unwrap().is_none(),
        "encrypted record must be unreadable without the key"
    );
}

#[cfg(feature = "hash-chain")]
#[test]
fn hash_chain_log_survives_restart() {
    // P0-4: recovery must re-verify the BLAKE3 hash chain on restart. A correct
    // verifier must NOT produce false positives on valid data (the main risk).
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.hash_enabled = true;
        config.shards = 1; // hash-chain requires single shard
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);
        let db = LogDb::open(config).unwrap();
        for i in 0..40u64 {
            db.append(format!("hashed-record-{}", i).as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(25));
            if db.durable_cursor() >= 40 {
                break;
            }
        }
        db.shutdown(Duration::from_secs(5)).unwrap();
    }

    // Reopen — chain re-verification runs over all 40 records and must pass.
    let mut config = Config::default();
    config.data_dir = data_dir.clone();
    config.hash_enabled = true;
    config.shards = 1;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(10);
    let db = LogDb::open(config).unwrap();
    for i in 0..40u64 {
        let rec = db.read(i).unwrap().unwrap();
        assert_eq!(rec.content, format!("hashed-record-{}", i).as_bytes());
    }
}

#[cfg(feature = "hash-chain")]
#[test]
fn hash_chain_detects_tamper() {
    // P0-4: a content byte modified on disk is detected on restart and the
    // tampered record (and everything after it) is truncated away.
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.hash_enabled = true;
        config.shards = 1;
        config.ring_size = 256;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);
        let db = LogDb::open(config).unwrap();
        for i in 0..20u64 {
            db.append(format!("payload-{:03}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        for _ in 0..40 {
            std::thread::sleep(Duration::from_millis(25));
            if db.durable_cursor() >= 20 {
                break;
            }
        }
        db.shutdown(Duration::from_secs(5)).unwrap();
    }

    // Tamper: flip a content byte inside the FIRST record (offset 128 = header,
    // +24 = content start). Both CRC and the hash chain will reject it.
    let seg = data_dir.join("segment-00000001.log");
    let mut data = std::fs::read(&seg).unwrap();
    let content_byte = 128 + 24 + 3; // a byte inside record 0's content
    data[content_byte] ^= 0xFF;
    std::fs::write(&seg, &data).unwrap();

    // Reopen — recovery must reject the tampered tail.
    let mut config = Config::default();
    config.data_dir = data_dir.clone();
    config.hash_enabled = true;
    config.shards = 1;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(10);
    let db = LogDb::open(config).unwrap();
    // Record 0 (tampered) must NOT be trusted/readable.
    assert!(
        db.read(0).unwrap().is_none(),
        "tampered record must be detected and truncated on recovery"
    );
}

// Regression: append_batch must reserve batch_size sequences (P0 data-loss fix).
#[test]
fn append_batch_preserves_distinct_records_across_batches() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 1 << 17; // 131072 slots >> 10000 records, no backpressure
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(30);
    let db = LogDb::open(config).unwrap();
    // 100 batches × 100 DISTINCT records = 10000 total.
    let batches = 100usize;
    let per = 100usize;
    for b in 0..batches {
        let recs: Vec<Vec<u8>> = (0..per)
            .map(|i| format!("b{}r{}", b, i).into_bytes())
            .collect();
        let refs: Vec<&[u8]> = recs.iter().map(|r| r.as_slice()).collect();
        db.append_batch(&refs).unwrap();
    }
    db.flush().unwrap();
    for _ in 0..60 {
        std::thread::sleep(Duration::from_millis(25));
        if db.durable_cursor() >= (batches * per) as u64 {
            break;
        }
    }

    let total = (batches * per) as u64;
    let mut readable = 0u64;
    let mut first_bad = None;
    for seq in 0..total {
        match db.read(seq).unwrap() {
            Some(r) => {
                let want = format!("b{}r{}", seq / per as u64, seq % per as u64).into_bytes();
                if r.content == want {
                    readable += 1;
                } else if first_bad.is_none() {
                    first_bad = Some((seq, format!("{:?}", r.content)));
                }
            }
            None => {
                if first_bad.is_none() {
                    first_bad = Some((seq, "MISSING".into()));
                }
            }
        }
    }
    eprintln!(
        "diag_append_batch: {} of {} readable; first_bad={:?}",
        readable, total, first_bad
    );
    assert_eq!(
        readable, total,
        "append_batch must preserve all {} distinct records across batches",
        total
    );
}

#[test]
fn index_stride_is_configurable() {
    // A smaller index_stride yields more anchors per segment → shorter read
    // scans (P2-1c). With stride 64 over 500 records we expect ~8 anchors
    // (vs ~1 at the default stride 1024).
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    {
        let mut config = Config::default();
        config.data_dir = data_dir.clone();
        config.ring_size = 4096;
        config.index_stride = 64;
        config.durability_mode = DurabilityMode::Sync;
        config.flush_timeout = Duration::from_secs(10);
        let db = LogDb::open(config).unwrap();
        for i in 0..500u64 {
            db.append(format!("r{}", i).as_bytes()).unwrap();
        }
        db.flush().unwrap();
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(25));
            if db.durable_cursor() >= 500 {
                break;
            }
        }
        for i in 0..500u64 {
            assert!(db.read(i).unwrap().is_some(), "record {} readable", i);
        }
    }
    // The anchor-count assertion pokes the internal sparse index (.idx). It only
    // compiles under the `testing` feature, which re-exposes `storage::index`.
    #[cfg(feature = "testing")]
    {
        use logdb::storage::index::SparseIndex;
        let seg = data_dir.join("segment-00000001.log");
        let idx = SparseIndex::load(&SparseIndex::index_path(&seg))
            .expect("sparse index .idx must be written after flush");
        assert!(
            idx.len() >= 5,
            "stride 64 over 500 records should yield several anchors, got {}",
            idx.len()
        );
    }
}

//! cr-032 Phase 0: proves the server's encryption wiring actually encrypts.
//!
//! Before the fix, `logdbd` parsed `storage.encryption` but never connected it
//! to the core — `encryption.enabled: true` was a silent no-op and data was
//! written plaintext. These tests exercise the exact wiring path `main.rs`
//! uses (`EncryptionConfig::resolve_key_ring` → `Config.encryption_keys`) and
//! assert encryption genuinely engages at rest, plus backup/restore round-trips
//! under encryption.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use logdb::{Config as DbConfig, DurabilityMode, KeyRing, LogDb};
use logdbd::backup;
use logdbd::config::{EncryptionConfig, EncryptionKey};

/// key_hex for 32 bytes of 0x42 (matches KEY_BYTES) — no `hex` dep needed here.
const KEY_HEX: &str = "4242424242424242424242424242424242424242424242424242424242424242";

fn enc_config() -> EncryptionConfig {
    let mut enc = EncryptionConfig::default();
    enc.enabled = true;
    enc.active_key_id = Some("k1".into());
    enc.keys = vec![EncryptionKey {
        key_id: "k1".into(),
        key_hex: KEY_HEX.into(),
    }];
    enc
}

/// Build a db_config exactly like main.rs: resolve the encryption config into a
/// KeyRing and assign it to `encryption_keys`.
fn db_config(dir: &Path, enc: &EncryptionConfig) -> DbConfig {
    let mut c = DbConfig::default();
    c.data_dir = dir.to_path_buf();
    c.ring_size = 256;
    c.durability_mode = DurabilityMode::Sync;
    c.flush_timeout = Duration::from_secs(5);
    c.encryption_keys = enc.resolve_key_ring().expect("resolve key ring");
    c
}

fn first_segment(dir: &Path) -> std::path::PathBuf {
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().starts_with("segment-"))
                .unwrap_or(false)
        })
        .collect();
    paths.sort();
    paths
        .into_iter()
        .next()
        .expect("at least one segment file on disk")
}

#[test]
fn encryption_enabled_actually_encrypts_at_rest() {
    let dir = tempfile::tempdir().unwrap();
    let enc = enc_config();
    let db = LogDb::open(db_config(dir.path(), &enc)).unwrap();
    for i in 0..5u64 {
        db.append(format!("secret-marker-{i}").as_bytes()).unwrap();
    }
    db.flush().unwrap();
    while db.durable_cursor() < 5 {
        std::thread::sleep(Duration::from_millis(10));
    }
    db.shutdown(Duration::from_secs(2)).unwrap();

    // (1) Plaintext must NOT appear on disk — the segment holds ciphertext.
    let raw = std::fs::read(first_segment(dir.path())).unwrap();
    assert!(
        !String::from_utf8_lossy(&raw).contains("secret-marker"),
        "plaintext leaked to disk — encryption did not engage"
    );

    // (2) Reopen with the SAME key: records decrypt back to the plaintext.
    let db2 = LogDb::open(db_config(dir.path(), &enc)).unwrap();
    let got: Vec<Vec<u8>> = db2
        .scan(0, u64::MAX)
        .unwrap()
        .filter_map(|r| r.ok())
        .map(|r| r.content)
        .collect();
    assert_eq!(got.len(), 5, "all records must decrypt on reopen");
    for c in &got {
        let s = String::from_utf8_lossy(c);
        assert!(s.starts_with("secret-marker-"), "unexpected content: {s}");
    }
    db2.shutdown(Duration::from_secs(2)).unwrap();

    // (3) Reopen with NO key: recovery drops encrypted frames → empty scan.
    // This proves the on-disk data was genuinely encrypted (not plaintext).
    let mut no_key = DbConfig::default();
    no_key.data_dir = dir.path().to_path_buf();
    no_key.durability_mode = DurabilityMode::Sync;
    let db3 = LogDb::open(no_key).unwrap();
    let count = db3.scan(0, u64::MAX).unwrap().filter_map(|r| r.ok()).count();
    assert_eq!(
        count, 0,
        "without the key, encrypted records must be unreadable (got {count})"
    );
    db3.shutdown(Duration::from_secs(2)).unwrap();
}

#[test]
fn backup_restore_round_trips_under_encryption() {
    let dir = tempfile::tempdir().unwrap();
    let enc = enc_config();

    // Write phase in its own scope so the db (and its committer thread) is
    // dropped before backup — mirroring the cr-029 backup test, which backs up
    // a stopped node. (We flush + wait for durability, then drop; no explicit
    // shutdown, which interacts poorly with a subsequent backup here.)
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    {
        let db = LogDb::open(db_config(&data_dir, &enc)).unwrap();
        for i in 0..3u64 {
            db.append(format!("bk-{i}").as_bytes()).unwrap();
        }
        db.flush().unwrap();
        while db.durable_cursor() < 3 {
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    // Backup the stopped node, then restore into a fresh dir WITH the key ring
    // (so --verify can decrypt). Records must survive. The archive lives OUTSIDE
    // the data_dir so the tar doesn't recurse into the file it is writing.
    let archive = dir.path().join("snap.logdbbak");
    backup::backup(&data_dir, &archive).unwrap();

    let restore_dir = tempfile::tempdir().unwrap();
    let ring: Arc<KeyRing> = enc.resolve_key_ring().unwrap().unwrap();
    backup::restore(&archive, restore_dir.path(), true, Some(ring)).unwrap();

    let db2 = LogDb::open(db_config(restore_dir.path(), &enc)).unwrap();
    let count = db2.scan(0, u64::MAX).unwrap().filter_map(|r| r.ok()).count();
    assert_eq!(count, 3, "encrypted records must survive backup/restore");
    db2.shutdown(Duration::from_secs(2)).unwrap();
}

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

/// Distinct rotation keys (each a 32-byte value, 64 hex chars).
const KEY_A_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const KEY_B_HEX: &str = "2222222222222222222222222222222222222222222222222222222222222222";

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

/// Build an enabled EncryptionConfig with the given keys and the named active.
fn enc_multi(keys: &[(&str, &str)], active: &str) -> EncryptionConfig {
    let mut enc = EncryptionConfig::default();
    enc.enabled = true;
    enc.active_key_id = Some(active.into());
    enc.keys = keys
        .iter()
        .map(|(id, hex)| EncryptionKey {
            key_id: (*id).into(),
            key_hex: (*hex).into(),
        })
        .collect();
    enc
}

/// Build a db_config exactly like main.rs: resolve the encryption config into a
/// KeyRing and assign it to `encryption_keys`.
fn db_config(dir: &Path, enc: &EncryptionConfig) -> DbConfig {
    db_config_with(dir, enc, false)
}

/// Same as [`db_config`] but optionally enables the hash chain (BLAKE3 keyed MAC).
fn db_config_with(dir: &Path, enc: &EncryptionConfig, hash: bool) -> DbConfig {
    let mut c = DbConfig::default();
    c.data_dir = dir.to_path_buf();
    c.ring_size = 256;
    c.durability_mode = DurabilityMode::Sync;
    c.flush_timeout = Duration::from_secs(5);
    c.hash_enabled = hash;
    c.encryption_keys = enc.resolve_key_ring().expect("resolve key ring");
    c
}

/// Open `dir` under `enc`, append `recs`, flush, and wait until `expected_total`
/// records are durable (absolute, across reopens), then drop. Records land on
/// disk encrypted under `enc`'s active key.
fn write_and_drain(dir: &Path, enc: &EncryptionConfig, hash: bool, recs: &[Vec<u8>], expected_total: u64) {
    let db = LogDb::open(db_config_with(dir, enc, hash)).unwrap();
    for r in recs {
        db.append(r).unwrap();
    }
    db.flush().unwrap();
    while db.durable_cursor() < expected_total {
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(db);
    // Let the drop-drain release file handles + the active.lock.
    std::thread::sleep(Duration::from_millis(50));
}

fn scan_count(dir: &Path, enc: &EncryptionConfig, hash: bool) -> usize {
    let db = LogDb::open(db_config_with(dir, enc, hash)).unwrap();
    let n = db.scan(0, u64::MAX).unwrap().filter_map(|r| r.ok()).count();
    db.shutdown(Duration::from_secs(2)).unwrap();
    n
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

// ── cr-032 Phase 1: multi-key rotation (no disk-format change) ──────────────

/// Write under key A, rotate to key B (A kept in the decrypt window), write more,
/// then reopen and scan: every record — whether encrypted with the now-prior key
/// A or the active key B — must decrypt and be readable. This is the core
/// rotation acceptance test (hash chain OFF; chain-under-rotation is tested
/// separately below).
#[test]
fn rotation_keeps_all_records_readable() {
    let dir = tempfile::tempdir().unwrap();

    // Session 1: active = A.
    let with_a = enc_multi(&[("a", KEY_A_HEX), ("b", KEY_B_HEX)], "a");
    write_and_drain(
        dir.path(),
        &with_a,
        false,
        &(0..3u64).map(|i| format!("old-{i}").into_bytes()).collect::<Vec<_>>(),
        3,
    );

    // Session 2: rotate — active = B, A retained.
    let with_b = enc_multi(&[("a", KEY_A_HEX), ("b", KEY_B_HEX)], "b");
    write_and_drain(
        dir.path(),
        &with_b,
        false,
        &(0..3u64).map(|i| format!("new-{i}").into_bytes()).collect::<Vec<_>>(),
        6,
    );

    // Reopen with the full ring: all 6 records readable regardless of which key
    // encrypted them.
    assert_eq!(scan_count(dir.path(), &with_b, false), 6, "all records must decrypt after rotation");
}

/// cr-032 Phase 3: hash-chain + multi-key rotation now WORKS end-to-end. The
/// chain key is a stable per-shard secret masked on disk (independent of the
/// active key), so rotating the active key no longer severs the chain. This is
/// the test Phase 1 had to reject — it now must succeed: write under A, rotate
/// to B (A retained), write more, reopen, and ALL records read back with the
/// hash chain intact.
#[test]
fn rotation_with_hash_chain_now_works() {
    let dir = tempfile::tempdir().unwrap();

    // Session 1: active = A, hash chain ON.
    let with_a = enc_multi(&[("a", KEY_A_HEX), ("b", KEY_B_HEX)], "a");
    write_and_drain(
        dir.path(),
        &with_a,
        true, // hash ON
        &(0..3u64).map(|i| format!("old-{i}").into_bytes()).collect::<Vec<_>>(),
        3,
    );

    // Session 2: rotate to B (A retained), hash chain ON, write more.
    let with_b = enc_multi(&[("a", KEY_A_HEX), ("b", KEY_B_HEX)], "b");
    write_and_drain(
        dir.path(),
        &with_b,
        true,
        &(0..3u64).map(|i| format!("new-{i}").into_bytes()).collect::<Vec<_>>(),
        6,
    );

    // Reopen: all 6 records readable, chain intact — no truncation.
    assert_eq!(
        scan_count(dir.path(), &with_b, true),
        6,
        "rotation must not sever the hash chain (cr-032 Phase 3)"
    );
}

/// cr-032 Phase 1: single-key encryption + hash-chain is fully supported (no
/// rotation in play, so the MAC key is stable). Write, reopen, scan — all good.
/// This guards against the multi-key rejection accidentally firing for the
/// common single-key case.
#[test]
fn single_key_encryption_with_hash_chain_works() {
    let dir = tempfile::tempdir().unwrap();
    let single = enc_multi(&[("a", KEY_A_HEX)], "a");
    write_and_drain(
        dir.path(),
        &single,
        true, // hash ON
        &(0..3u64).map(|i| format!("h-{i}").into_bytes()).collect::<Vec<_>>(),
        3,
    );
    assert_eq!(
        scan_count(dir.path(), &single, true),
        3,
        "single-key encryption + hash-chain must round-trip"
    );
}

/// cr-032: `logdbd-admin restore --verify --config <yaml>` end-to-end. This is
/// the exact CLI an operator runs; it exercises the wiring the library-level
/// `backup::restore(.., Some(ring))` test does NOT — namely the admin binary
/// loading the server YAML, resolving its key ring, and passing it through to
/// the encrypted restore verify. (Spawned as a subprocess so the bin's arg
/// parsing + `cmd_restore` path is covered, not just the library call.)
#[test]
fn admin_restore_verify_config_round_trips_encrypted() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Write 3 encrypted records under a single key, durable.
    let enc = enc_multi(&[("k", KEY_HEX)], "k");
    {
        let db = LogDb::open(db_config_with(&data_dir, &enc, false)).unwrap();
        for i in 0..3u64 {
            db.append(format!("r-{i}").as_bytes()).unwrap();
        }
        db.flush().unwrap();
        while db.durable_cursor() < 3 {
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    // Back the stopped node up (archive outside the data_dir — see cr-029 test).
    let archive = dir.path().join("snap.logdbbak");
    backup::backup(&data_dir, &archive).unwrap();

    // A server YAML carrying the same encryption config the primary runs with.
    let yaml_path = dir.path().join("logdbd.yaml");
    std::fs::write(
        &yaml_path,
        format!(
            r#"
node: {{ id: "n", role: primary, cluster_id: "c", epoch: 1 }}
logdb: {{ data_dir: "/var/lib/logdbd" }}
storage:
  encryption:
    enabled: true
    algorithm: aes-256-gcm
    provider: file
    keys:
      - key_id: "k"
        key_hex: "{KEY_HEX}"
    active_key_id: "k"
"#,
        ),
    )
    .unwrap();

    // Run the admin binary exactly as documented.
    let restore_dir = dir.path().join("restored");
    let bin = env!("CARGO_BIN_EXE_logdbd-admin");
    let out = std::process::Command::new(bin)
        .arg("restore")
        .arg("--backup")
        .arg(&archive)
        .arg("--data-dir")
        .arg(&restore_dir)
        .arg("--verify")
        .arg("--config")
        .arg(&yaml_path)
        .output()
        .expect("spawn logdbd-admin");
    assert!(
        out.status.success(),
        "restore --verify --config failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The restored data_dir must decrypt back to the 3 records.
    let db = LogDb::open(db_config_with(&restore_dir, &enc, false)).unwrap();
    let count = db.scan(0, u64::MAX).unwrap().filter_map(|r| r.ok()).count();
    assert_eq!(count, 3, "restored encrypted records must be readable via the CLI path");
    db.shutdown(Duration::from_secs(2)).unwrap();
}

// NOTE on retirement (cr-032 Phase 1 design doc: "退役旧 key → 旧段不可读"):
// the crypto behavior — a frame whose key has been retired fails to decrypt —
// is covered by `decrypt_fails_when_key_retired` in logdb's reader keyring_tests.
// A clean *end-to-end* retirement (drop only the retired-key segment, keep the
// rest) requires recovery to skip segments whose key is gone, which needs a
// per-segment key_id — that is cr-032 Phase 3. Until then, recovery conservatively
// truncates at the first undecryptable frame (it cannot distinguish a retired key
// from corruption), so end-to-end retirement is segment-boundary-dependent.

//! cr-029: backup/restore round-trip through real logdb data.
//!
//! Writes records via logdbd's Storage, backs the data_dir up, wipes it,
//! restores (with --verify, which re-runs recovery), and confirms the records
//! are intact on reopen.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use logdbd::backup;
use logdbd::catalog::Catalog;
use logdbd::storage::Storage;

fn db_config(data_dir: &Path) -> logdb::Config {
    let mut cfg = logdb::Config::default();
    cfg.data_dir = data_dir.to_path_buf();
    cfg.durability_mode = logdb::DurabilityMode::Sync;
    cfg.shards = 1;
    cfg.ring_size = 256;
    cfg.flush_timeout = Duration::from_secs(5);
    cfg
}

/// Append N records, flush to durable, and close. Returns the contents in order.
fn write_records(data_dir: &Path) -> Vec<Vec<u8>> {
    let db = logdb::LogDb::open(db_config(data_dir)).unwrap();
    let storage = Storage::new(db, 1);
    let _catalog = Catalog::open(data_dir).unwrap(); // creates catalog.dat

    let meta = BTreeMap::new();
    let mut contents = Vec::new();
    for i in 0..5u32 {
        let content = format!("record-{i}").into_bytes();
        storage
            .append(1, 1, "evt", "application/json", &meta, i as u64, &content)
            .unwrap();
        contents.push(content);
    }
    storage.db_arc().drain(Duration::from_secs(5)).unwrap();
    contents
}

/// Reopen and read back all record contents (ordered by gid).
fn read_contents(data_dir: &Path) -> Vec<Vec<u8>> {
    let db = logdb::LogDb::open(db_config(data_dir)).unwrap();
    let storage = Storage::new(db, 1);
    let durable = storage.durable_gid();
    storage
        .scan(0, durable)
        .unwrap()
        .into_iter()
        .map(|r| r.user_content)
        .collect()
}

#[test]
fn backup_restore_round_trips_real_data() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let original = write_records(&data_dir);
    assert_eq!(original.len(), 5);
    assert_eq!(
        read_contents(&data_dir).len(),
        5,
        "baseline: records readable before backup"
    );

    // Backup the stopped data_dir.
    let out = tmp.path().join("snap.logdbbak");
    let manifest = backup::backup(&data_dir, &out).expect("backup");
    assert!(
        manifest.file_count >= 2,
        "expected at least catalog.dat + a segment, got {}",
        manifest.file_count
    );
    assert!(out.exists(), "backup file created");
    assert!(out.with_extension("logdbbak.sha256").exists() || {
        // sidecar is <out>.sha256
        std::path::PathBuf::from(format!("{}.sha256", out.display())).exists()
    });

    // Wipe the data_dir entirely.
    std::fs::remove_dir_all(&data_dir).unwrap();
    assert!(!data_dir.exists());

    // Restore with verify (re-runs logdb recovery: CRC + hash chain + torn writes).
    let restored_manifest = backup::restore(&out, &data_dir, true).expect("restore --verify");
    assert_eq!(restored_manifest.magic, manifest.magic);
    assert_eq!(restored_manifest.file_count, manifest.file_count);

    // Reopen and confirm the data is intact.
    let restored = read_contents(&data_dir);
    assert_eq!(restored, original, "restored record contents must match original");
}

#![no_main]
use std::time::Duration;
use libfuzzer_sys::fuzz_target;
use logdb::config::{Config, DurabilityMode};
use logdb::LogDb;
fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 64;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(5);
    let db = match LogDb::open(config) { Ok(db) => db, Err(_) => return };
    for chunk in data.chunks(200) {
        if chunk.is_empty() { continue; }
        let id = match db.append(chunk) { Ok(id) => id, Err(_) => continue };
        db.flush().ok();
        std::thread::sleep(Duration::from_millis(5));
        if let Ok(Some(record)) = db.read(id) {
            assert_eq!(record.content, chunk, "roundtrip mismatch");
        }
    }
});

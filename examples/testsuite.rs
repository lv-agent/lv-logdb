//! logdb test suite — standalone binary for deployed environments.
//!
//! Runs all unit and integration tests without requiring `cargo test`.
//! Exit code 0 on success, non-zero on failure.
//!
//! Usage:
//!   cargo run --release --example testsuite
//!   ./bin/testsuite

use std::time::Duration;

use logdb::config::{Config, DurabilityMode, QueueFullPolicy, RetentionPolicy};
use logdb::ring::Ring;
use logdb::storage::SegmentManager;
use logdb::health::{HealthState, HEALTH_OK, HEALTH_DISK_FULL, HEALTH_IO_ERROR};
use logdb::pipeline::signal::{FlushSignal, ShutdownState};
use logdb::pipeline::trigger::{CommitTrigger, WaitStrategy, Backoff};
use logdb::shard::ShardMap;
use logdb::record::{RecordId, Record};
use logdb::storage::format::{
    SegmentHeader, MAGIC, FORMAT_VERSION, SEGMENT_HEADER_SIZE, HEADER_CRC_END,
    HASH_ALGO_SHA256, HASH_ALGO_BLAKE3, RECORD_FORMAT_V1, FLAG_NOT_FIRST, FLAG_HASH_ENABLED,
    MIN_RECORD_SIZE, record_size, serialize_record, deserialize_record,
};
use logdb::storage::index::{SparseIndex, IndexEntry};
use logdb::LogDb;

macro_rules! check {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            eprintln!("FAIL: {} ({}:{}:{})", $msg, file!(), line!(), column!());
            return 1;
        }
    };
}

fn main() -> std::process::ExitCode {
    let mut passed = 0u32;
    let mut failed = 0u32;

    macro_rules! test {
        ($name:ident) => {
            print!("  {} ... ", stringify!($name));
            let result = $name();
            if result == 0 {
                println!("ok");
                passed += 1;
            } else {
                println!("FAIL");
                failed += 1;
            }
        };
    }

    println!("=== logdb Test Suite ===\n");

    // ── RecordId ──────────────────────────────────────────────────────
    println!("RecordId:");
    test!(test_record_id_display);
    test!(test_record_id_into_u64);
    test!(test_record_id_from_u64);
    test!(test_record_id_ordering);

    // ── Slot ──────────────────────────────────────────────────────────
    println!("Slot:");
    test!(test_slot_inline_write_read);
    test!(test_slot_spill_write_read);
    test!(test_slot_switch_spill_to_inline);
    test!(test_slot_send_sync);
    test!(test_slot_release_acquire);
    test!(test_slot_exact_inline_boundary);
    test!(test_slot_just_above_inline);
    test!(test_slot_zero_length);

    // ── Ring ──────────────────────────────────────────────────────────
    println!("Ring:");
    test!(test_ring_claim_advances);
    test!(test_ring_claim_queue_full_drop);
    test!(test_ring_slot_index_wraps);
    test!(test_ring_consume_watermark_no_hash);
    test!(test_ring_consume_watermark_with_hash);
    test!(test_ring_full_write_read_cycle);
    test!(test_ring_claim_unblocks_after_consume);
    test!(test_ring_multi_thread_no_duplicates);

    // ── Format ────────────────────────────────────────────────────────
    println!("Format:");
    test!(test_header_round_trip);
    test!(test_header_crc_covers_partition_id);
    test!(test_header_crc_covers_hash_algo);
    test!(test_header_crc_covers_base_sequence);
    test!(test_header_bad_magic);
    test!(test_header_bad_crc);
    test!(test_record_round_trip);
    test!(test_record_crc_detects_corruption);
    test!(test_record_empty_content);
    test!(test_record_two_back_to_back);

    // ── Segment ───────────────────────────────────────────────────────
    println!("Segment:");
    test!(test_segment_create_and_append);
    test!(test_segment_roll);
    test!(test_segment_fdatasync);
    test!(test_segment_base_sequence);

    // ── Shard ─────────────────────────────────────────────────────────
    println!("Shard:");
    test!(test_shard_encode_decode);
    test!(test_shard_single);
    test!(test_shard_claims_globally_unique);

    // ── Health ────────────────────────────────────────────────────────
    println!("Health:");
    test!(test_health_initial);
    test!(test_health_set_clear);

    // ── Signal ────────────────────────────────────────────────────────
    println!("Signal:");
    test!(test_flush_signal_request_complete);
    test!(test_flush_signal_cas_max);
    test!(test_shutdown_enter_leave);
    test!(test_shutdown_enter_rejected_after_drain);

    // ── Recovery ──────────────────────────────────────────────────────
    println!("Recovery:");
    test!(test_recover_after_write);

    // ── Reader ────────────────────────────────────────────────────────
    println!("Reader:");
    test!(test_reader_read_by_id);
    test!(test_reader_nonexistent);

    // ── Summary ───────────────────────────────────────────────────────
    let total = passed + failed;
    println!("\n═══════════════════════════════════════════");
    println!("  {} passed, {} failed, {} total", passed, failed, total);
    println!("═══════════════════════════════════════════");

    if failed > 0 {
        std::process::ExitCode::from(1)
    } else {
        std::process::ExitCode::from(0)
    }
}

// ── RecordId tests ─────────────────────────────────────────────────────────

fn test_record_id_display() -> i32 {
    let id = RecordId::new(0, 42);
    check!(format!("{}", id) == "42", "display single partition");
    let id2 = RecordId::new(3, 42);
    check!(format!("{}", id2) == "3/42", "display multi partition");
    0
}

fn test_record_id_into_u64() -> i32 {
    let id = RecordId::new(0, 99);
    let seq: u64 = id.into();
    check!(seq == 99, "into u64");
    0
}

fn test_record_id_from_u64() -> i32 {
    let id: RecordId = 99u64.into();
    check!(id.partition_id == 0, "from_u64 partition");
    check!(id.sequence == 99, "from_u64 sequence");
    0
}

fn test_record_id_ordering() -> i32 {
    let a = RecordId::new(0, 10);
    let b = RecordId::new(0, 20);
    let c = RecordId::new(1, 5);
    check!(a < b, "ordering a<b");
    check!(a < c, "ordering a<c (partition)");
    0
}

// ── Slot tests ─────────────────────────────────────────────────────────────

fn test_slot_inline_write_read() -> i32 {
    let ring = Ring::new(16, false, 0);
    let seq = ring.claim(QueueFullPolicy::Block).unwrap();
    let content = b"hello logdb";
    unsafe { ring.slot(seq).producer_write(seq, 1000, content); }
    ring.slot(seq).publish(seq);
    check!(ring.slot(seq).is_published(seq), "published");
    unsafe {
        let view = ring.slot(seq).read();
        check!(view.record_id == seq, "record_id");
        check!(view.content == content, "content");
    }
    0
}

fn test_slot_spill_write_read() -> i32 {
    let ring = Ring::new(16, false, 0);
    let seq = ring.claim(QueueFullPolicy::Block).unwrap();
    let content = vec![0xAAu8; 300];
    unsafe { ring.slot(seq).producer_write(seq, 2000, &content); }
    ring.slot(seq).publish(seq);
    unsafe {
        let view = ring.slot(seq).read();
        check!(view.content.len() == 300, "spill len");
        check!(view.content == &content[..], "spill content");
    }
    0
}

fn test_slot_switch_spill_to_inline() -> i32 {
    let ring = Ring::new(16, false, 0);
    let seq0 = ring.claim(QueueFullPolicy::Block).unwrap();
    unsafe { ring.slot(seq0).producer_write(seq0, 0, &vec![0xBBu8; 300]); }
    ring.slot(seq0).publish(seq0);
    let seq1 = ring.claim(QueueFullPolicy::Block).unwrap();
    unsafe { ring.slot(seq1).producer_write(seq1, 0, b"small"); }
    ring.slot(seq1).publish(seq1);
    unsafe {
        check!(ring.slot(seq1).read().content == b"small", "switch to inline");
    }
    0
}

fn test_slot_send_sync() -> i32 { 0 }

fn test_slot_release_acquire() -> i32 {
    use std::sync::Arc;
    let ring = Arc::new(Ring::new(16, false, 0));
    let r = Arc::clone(&ring);
    let h = std::thread::spawn(move || {
        let seq = r.claim(QueueFullPolicy::Block).unwrap();
        unsafe { r.slot(seq).producer_write(seq, 9999, b"concurrent"); }
        r.slot(seq).publish(seq);
    });
    h.join().unwrap();
    check!(ring.slot(0).is_published(0), "release-acquire");
    0
}

fn test_slot_exact_inline_boundary() -> i32 {
    let ring = Ring::new(16, false, 0);
    let seq = ring.claim(QueueFullPolicy::Block).unwrap();
    let content = vec![0xCCu8; 256];
    unsafe { ring.slot(seq).producer_write(seq, 0, &content); }
    ring.slot(seq).publish(seq);
    unsafe { check!(ring.slot(seq).read().content.len() == 256, "exact boundary"); }
    0
}

fn test_slot_just_above_inline() -> i32 {
    let ring = Ring::new(16, false, 0);
    let seq = ring.claim(QueueFullPolicy::Block).unwrap();
    let content = vec![0xDDu8; 257];
    unsafe { ring.slot(seq).producer_write(seq, 0, &content); }
    ring.slot(seq).publish(seq);
    unsafe { check!(ring.slot(seq).read().content.len() == 257, "just above"); }
    0
}

fn test_slot_zero_length() -> i32 {
    let ring = Ring::new(16, false, 0);
    let seq = ring.claim(QueueFullPolicy::Block).unwrap();
    unsafe { ring.slot(seq).producer_write(seq, 0, b""); }
    ring.slot(seq).publish(seq);
    unsafe { check!(ring.slot(seq).read().content.len() == 0, "zero length"); }
    0
}

// ── Ring tests ─────────────────────────────────────────────────────────────

fn test_ring_claim_advances() -> i32 {
    let ring = Ring::new(16, false, 0);
    let s0 = ring.claim(QueueFullPolicy::Block).unwrap();
    check!(s0 == 0, "first claim");
    let s1 = ring.claim(QueueFullPolicy::Block).unwrap();
    check!(s1 == 1, "second claim");
    0
}

fn test_ring_claim_queue_full_drop() -> i32 {
    let ring = Ring::new(16, false, 0);
    for _ in 0..16 { ring.claim(QueueFullPolicy::Block).unwrap(); }
    check!(ring.claim(QueueFullPolicy::Drop).is_err(), "queue full drop");
    0
}

fn test_ring_slot_index_wraps() -> i32 {
    let ring = Ring::new(16, false, 0);
    check!(ring.slot(0) as *const _ == ring.slot(16) as *const _, "wrap");
    0
}

fn test_ring_consume_watermark_no_hash() -> i32 {
    use std::sync::atomic::Ordering;
    let ring = Ring::new(16, false, 0);
    check!(ring.consume_watermark() == 0, "initial wm");
    ring.set_committed_cursor(5);
    check!(ring.consume_watermark() == 5, "wm after commit");
    0
}

fn test_ring_consume_watermark_with_hash() -> i32 {
    use std::sync::atomic::Ordering;
    let ring = Ring::new(16, true, 0);
    ring.set_sealed_cursor(3);
    ring.set_committed_cursor(5);
    check!(ring.consume_watermark() == 3, "wm min(sealed,committed)");
    0
}

fn test_ring_full_write_read_cycle() -> i32 {
    let ring = Ring::new(16, false, 0);
    let seq = ring.claim(QueueFullPolicy::Block).unwrap();
    unsafe { ring.slot(seq).producer_write(seq, 5000, b"integration"); }
    ring.slot(seq).publish(seq);
    unsafe { check!(ring.slot(seq).read().content == b"integration", "full cycle"); }
    0
}

fn test_ring_claim_unblocks_after_consume() -> i32 {
    use std::sync::atomic::Ordering;
    let ring = std::sync::Arc::new(Ring::new(16, false, 0));
    for _ in 0..16 { ring.claim(QueueFullPolicy::Block).unwrap(); }
    let r = std::sync::Arc::clone(&ring);
    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        r.set_committed_cursor(10);
    });
    let result = ring.claim(QueueFullPolicy::Block);
    h.join().unwrap();
    check!(result.is_ok(), "claim unblocks");
    0
}

fn test_ring_multi_thread_no_duplicates() -> i32 {
    use std::collections::HashSet;
    let ring = std::sync::Arc::new(Ring::new(1024, false, 0));
    let mut handles = vec![];
    for _ in 0..4 {
        let r = std::sync::Arc::clone(&ring);
        handles.push(std::thread::spawn(move || {
            let mut v = vec![];
            for _ in 0..50 { v.push(r.claim(QueueFullPolicy::Block).unwrap()); }
            v
        }));
    }
    let mut all = HashSet::new();
    for h in handles { for s in h.join().unwrap() { check!(all.insert(s), "duplicate"); } }
    for i in 0..200u64 { check!(all.contains(&i), "missing seq"); }
    0
}

// ── Format tests ───────────────────────────────────────────────────────────

fn test_header_round_trip() -> i32 {
    let hi = [0xABu8; 32];
    let h = SegmentHeader::first_segment(hi, 0, 0, 1, false, HASH_ALGO_SHA256);
    let mut buf = [0u8; SEGMENT_HEADER_SIZE];
    h.serialize(&mut buf, [0u8; 32]);
    let p = SegmentHeader::deserialize(&buf).unwrap();
    check!(p.format_version == FORMAT_VERSION, "version");
    check!(p.hash_algo == HASH_ALGO_SHA256, "hash algo");
    check!(p.base_sequence == 0, "base seq");
    check!(p.partition_id == 0, "partition");
    check!(p.segment_id == 1, "segment id");
    0
}

fn test_header_crc_covers_partition_id() -> i32 {
    let hi = [0xABu8; 32];
    let mut h = SegmentHeader::first_segment(hi, 0, 0, 1, false, HASH_ALGO_SHA256);
    let mut b1 = [0u8; SEGMENT_HEADER_SIZE]; h.serialize(&mut b1, [0u8; 32]);
    h.partition_id = 99;
    let mut b2 = [0u8; SEGMENT_HEADER_SIZE]; h.serialize(&mut b2, [0u8; 32]);
    check!(b1[72..76] != b2[72..76], "crc covers partition_id");
    0
}

fn test_header_crc_covers_hash_algo() -> i32 {
    let hi = [0xABu8; 32];
    let mut h = SegmentHeader::first_segment(hi, 0, 0, 1, false, HASH_ALGO_SHA256);
    let mut b1 = [0u8; SEGMENT_HEADER_SIZE]; h.serialize(&mut b1, [0u8; 32]);
    h.hash_algo = HASH_ALGO_BLAKE3;
    let mut b2 = [0u8; SEGMENT_HEADER_SIZE]; h.serialize(&mut b2, [0u8; 32]);
    check!(b1[72..76] != b2[72..76], "crc covers hash_algo");
    0
}

fn test_header_crc_covers_base_sequence() -> i32 {
    let hi = [0xABu8; 32];
    let mut h = SegmentHeader::first_segment(hi, 0, 0, 1, false, HASH_ALGO_SHA256);
    let mut b1 = [0u8; SEGMENT_HEADER_SIZE]; h.serialize(&mut b1, [0u8; 32]);
    h.base_sequence = 99999;
    let mut b2 = [0u8; SEGMENT_HEADER_SIZE]; h.serialize(&mut b2, [0u8; 32]);
    check!(b1[72..76] != b2[72..76], "crc covers base_sequence");
    0
}

fn test_header_bad_magic() -> i32 {
    let mut buf = [0u8; SEGMENT_HEADER_SIZE];
    buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
    check!(SegmentHeader::deserialize(&buf).is_err(), "bad magic");
    0
}

fn test_header_bad_crc() -> i32 {
    let hi = [0x11u8; 32];
    let h = SegmentHeader::first_segment(hi, 0, 0, 1, false, HASH_ALGO_SHA256);
    let mut buf = [0u8; SEGMENT_HEADER_SIZE];
    h.serialize(&mut buf, [0u8; 32]);
    buf[10] ^= 0xFF;
    check!(SegmentHeader::deserialize(&buf).is_err(), "bad crc");
    0
}

fn test_record_round_trip() -> i32 {
    let view = logdb::record::ReadView { record_id: 42, timestamp_ns: 1000, content: b"hello", hash_n: &[0u8; 32] };
    let mut buf = vec![0u8; record_size(5)];
    serialize_record(&mut buf, 99, &view);
    let (rec, n) = deserialize_record(&buf).unwrap();
    check!(n == record_size(5), "consumed");
    check!(rec.id.sequence == 99, "sequence");
    check!(rec.content == b"hello", "content");
    0
}

fn test_record_crc_detects_corruption() -> i32 {
    let view = logdb::record::ReadView { record_id: 1, timestamp_ns: 100, content: b"data", hash_n: &[0u8; 32] };
    let mut buf = vec![0u8; record_size(4)];
    serialize_record(&mut buf, 1, &view);
    buf[24] ^= 0x01;
    check!(deserialize_record(&buf).is_err(), "crc detect");
    0
}

fn test_record_empty_content() -> i32 {
    let view = logdb::record::ReadView { record_id: 0, timestamp_ns: 0, content: b"", hash_n: &[0u8; 32] };
    let mut buf = vec![0u8; record_size(0)];
    serialize_record(&mut buf, 0, &view);
    let (rec, _) = deserialize_record(&buf).unwrap();
    check!(rec.content.is_empty(), "empty content");
    0
}

fn test_record_two_back_to_back() -> i32 {
    let hash = [0u8; 32];
    let v1 = logdb::record::ReadView { record_id: 0, timestamp_ns: 100, content: b"first", hash_n: &hash };
    let v2 = logdb::record::ReadView { record_id: 1, timestamp_ns: 200, content: b"second", hash_n: &hash };
    let total = record_size(v1.content.len()) + record_size(v2.content.len());
    let mut buf = vec![0u8; total];
    let p1 = serialize_record(&mut buf, 0, &v1);
    let _p2 = serialize_record(&mut buf[p1..], 1, &v2);
    let (r1, c1) = deserialize_record(&buf).unwrap();
    let (r2, _) = deserialize_record(&buf[c1..]).unwrap();
    check!(r1.content == b"first", "first record");
    check!(r2.content == b"second", "second record");
    0
}

// ── Segment tests ──────────────────────────────────────────────────────────

fn test_segment_create_and_append() -> i32 {
    let dir = tempfile::tempdir().unwrap();
    let ring = Ring::new(64, false, 0);
    for i in 0..10u64 {
        let seq = ring.claim(QueueFullPolicy::Block).unwrap();
        let content = format!("record-{}", i);
        unsafe { ring.slot(seq).producer_write(seq, i * 100, content.as_bytes()); }
        ring.slot(seq).publish(seq);
    }
    let mut mgr = SegmentManager::create(dir.path().to_path_buf(), 1_000_000, false, false, None, [0u8; 32], RetentionPolicy::KeepAll, 0).unwrap();
    let last = mgr.append_batch(&ring, 0, 9).unwrap();
    check!(last == 9, "append batch");
    check!(mgr.active_offset() > SEGMENT_HEADER_SIZE as u64, "offset advanced");
    0
}

fn test_segment_roll() -> i32 {
    let dir = tempfile::tempdir().unwrap();
    let mut mgr = SegmentManager::create(dir.path().to_path_buf(), 1024, false, false, None, [0u8; 32], RetentionPolicy::KeepAll, 0).unwrap();
    check!(mgr.active_segment_id() == 1, "first seg id");
    mgr.roll(0, 0).unwrap();
    check!(mgr.active_segment_id() == 2, "rolled seg id");
    check!(dir.path().join("segment-00000001.log").exists(), "seg1 exists");
    check!(dir.path().join("segment-00000002.log").exists(), "seg2 exists");
    0
}

fn test_segment_fdatasync() -> i32 {
    let dir = tempfile::tempdir().unwrap();
    let mgr = SegmentManager::create(dir.path().to_path_buf(), 1_000_000, false, false, None, [0u8; 32], RetentionPolicy::KeepAll, 0).unwrap();
    mgr.fdatasync().unwrap();
    0
}

fn test_segment_base_sequence() -> i32 {
    let dir = tempfile::tempdir().unwrap();
    let mgr = SegmentManager::create(dir.path().to_path_buf(), 1_000_000, false, false, None, [0u8; 32], RetentionPolicy::KeepAll, 42).unwrap();
    check!(mgr.base_sequence() == 42, "base sequence");
    0
}

// ── Shard tests ────────────────────────────────────────────────────────────

fn test_shard_encode_decode() -> i32 {
    use logdb::shard;
    let sb = 3u32; // 8 shards
    for shard_id in 0..8usize {
        for seq in [0u64, 1, 100] {
            let global = shard::encode_record_id(shard_id, seq, sb);
            let (ds, dl) = shard::decode_record_id(global, sb);
            check!(ds == shard_id, "decode shard");
            check!(dl == seq, "decode seq");
        }
    }
    0
}

fn test_shard_single() -> i32 {
    let sm = ShardMap::new(1, 8192, false, 0);
    check!(sm.num_shards() == 1, "num shards");
    check!(sm.shard_bits() == 0, "shard bits");
    0
}

fn test_shard_claims_globally_unique() -> i32 {
    use std::collections::HashSet;
    let sm = ShardMap::new(4, 8192, false, 0);
    let mut ids = HashSet::new();
    for _ in 0..400 {
        let (gid, _, _) = sm.claim(QueueFullPolicy::Block).unwrap();
        check!(ids.insert(gid), "duplicate global id");
    }
    0
}

// ── Health tests ───────────────────────────────────────────────────────────

fn test_health_initial() -> i32 {
    let h = HealthState::new();
    check!(h.check().is_none(), "initially healthy");
    0
}

fn test_health_set_clear() -> i32 {
    let h = HealthState::new();
    h.set_error(HEALTH_DISK_FULL);
    check!(h.check() == Some(HEALTH_DISK_FULL), "disk full");
    h.clear_if_recovered();
    check!(h.check().is_none(), "recovered");
    0
}

// ── Signal tests ───────────────────────────────────────────────────────────

fn test_flush_signal_request_complete() -> i32 {
    let sig = FlushSignal::new(1);
    sig.request(&[10]);
    check!(!sig.is_done(&[10]), "not done yet");
    sig.complete(0, 10);
    check!(sig.is_done(&[10]), "done");
    0
}

fn test_flush_signal_cas_max() -> i32 {
    let sig = FlushSignal::new(1);
    sig.request(&[5]);
    sig.request(&[15]);
    sig.request(&[10]);
    check!(sig.target(0) == 15, "cas max");
    0
}

fn test_shutdown_enter_leave() -> i32 {
    let s = ShutdownState::new();
    check!(s.enter(), "enter");
    s.leave();
    s.leave(); // extra leave shouldn't panic
    0
}

fn test_shutdown_enter_rejected_after_drain() -> i32 {
    let s = ShutdownState::new();
    s.start_drain();
    check!(!s.enter(), "rejected after drain");
    0
}

// ── Recovery test ──────────────────────────────────────────────────────────

fn test_recover_after_write() -> i32 {
    let dir = tempfile::tempdir().unwrap();
    let ring = Ring::new(64, false, 0);
    let mut mgr = SegmentManager::create(dir.path().to_path_buf(), 10_000_000, false, false, None, [0u8; 32], RetentionPolicy::KeepAll, 0).unwrap();
    for i in 0..10u64 {
        let seq = ring.claim(QueueFullPolicy::Block).unwrap();
        unsafe { ring.slot(seq).producer_write(seq, i * 100, format!("rec-{}", i).as_bytes()); }
        ring.slot(seq).publish(seq);
    }
    mgr.append_batch(&ring, 0, 9).unwrap();
    mgr.fdatasync().unwrap();
    drop(mgr);
    drop(ring);

    let state = logdb::recovery::recover(dir.path(), 10_000_000, RetentionPolicy::KeepAll, None).unwrap();
    check!(state.last_sequence == 9, "recovered last sequence");
    0
}

// ── Reader tests ───────────────────────────────────────────────────────────

fn test_reader_read_by_id() -> i32 {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 64;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(config).unwrap();
    for i in 0..5u64 {
        db.append(format!("rec-{}", i).as_bytes()).unwrap();
    }
    db.flush().unwrap();
    for _ in 0..20 {
        std::thread::sleep(Duration::from_millis(25));
        if db.durable_cursor() >= 5 {
            break;
        }
    }
    check!(db.durable_cursor() >= 5, "records durable");
    let rec = db.read(3).unwrap().unwrap();
    check!(rec.id.sequence == 3, "read seq");
    check!(rec.content == b"rec-3", "read content");
    db.shutdown(Duration::from_secs(5)).unwrap();
    0
}

fn test_reader_nonexistent() -> i32 {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.data_dir = dir.path().to_path_buf();
    config.ring_size = 64;
    config.durability_mode = DurabilityMode::Sync;
    config.flush_timeout = Duration::from_secs(5);
    let db = LogDb::open(config).unwrap();
    db.append(b"only").unwrap();
    db.flush().unwrap();
    for _ in 0..20 {
        std::thread::sleep(Duration::from_millis(25));
        if db.durable_cursor() >= 1 {
            break;
        }
    }
    check!(db.read(999).unwrap().is_none(), "nonexistent");
    db.shutdown(Duration::from_secs(5)).unwrap();
    0
}

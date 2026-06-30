# Recovery

How logdb rebuilds state after a crash: automatic recovery on `open`, torn-write detection and truncation, optional hash-chain verification, the checkpoint mechanism, and the canonical WAL replay pattern.

## Contents

- [Automatic recovery on open](#automatic-recovery-on-open)
- [What the recovery algorithm does](#what-the-recovery-algorithm-does)
- [Checkpoints](#checkpoints)
- [checkpoint.dat on-disk layout](#checkpointdat-on-disk-layout)
- [Replay API: recovery_report and replay_from](#replay-api-recovery_report-and-replay_from)
- [The WAL pattern](#the-wal-pattern)
- [What survives a crash vs what is truncated](#what-survives-a-crash-vs-what-is-truncated)

## Automatic recovery on open

`LogDb::open` is the recovery entry point. If the data directory already exists and contains a `segment-00000001.log` file, logdb treats it as an existing database and runs `recovery::recover` before starting the pipeline (`src/lib.rs:98-124`):

```rust
let (mut seg_mgr, initial_seq, last_hash, hash_init) = if data_dir.exists()
    && data_dir.join("segment-00000001.log").exists()
{
    let state = recovery::recover(
        &data_dir,
        config.segment_size,
        config.retention.clone(),
        config.encryption_key,
    )?;
    let initial = state.last_sequence.wrapping_add(1);
    (state.segment_manager, initial, state.last_hash, state.hash_init)
} else {
    // …fresh database: create the first segment…
};
```

If the directory does not exist (or has no first segment), logdb creates a fresh database. There is no separate "recover" call to remember — opening an existing directory always performs recovery. The recovered state seeds the ring buffer's next sequence (`last_sequence + 1`), the hash-chain continuation, and the reopened `SegmentManager`.

You must open the database with the **same `encryption_key`** you wrote it with: encrypted segments are decoded (decrypt-then-decompress) during recovery, and the wrong key produces undecryptable frames that are treated as corrupt.

## What the recovery algorithm does

Recovery reconstructs the log from segment files on disk (`src/recovery.rs`). The algorithm (`src/recovery.rs:7-17`, §15):

> **Sharding (`shards > 1`):** recovery runs **independently per shard directory** (`data_dir/s<shard>/`; the flat `data_dir` for `shards == 1`). The steps below execute once per shard — each scans its own last segment with stride `1 << shard_bits`, detects and truncates its own torn write, and re-seeds its own ring resume point from its first recovered record. A per-shard `recovered_count` of zero yields a fresh empty shard.

1. **List and sort** all `segment-*.log` files by `segment_id` ascending.
2. **Validate each segment header** (magic + header CRC). A bad header means that segment and every later one are discarded — recovery stops there.
3. **Scan the last segment record-by-record** (or frame-by-frame for compressed/encrypted segments): read length, read the full record, check the CRC, verify the `record_id` matches the expected monotonic sequence.
4. **Torn-write detection.** A torn write — the file ends mid-record, or the trailing record fails its CRC — is repaired by truncating the file back to the last fully-valid record. The truncation point and the offending `record_id` are recorded as a `RecoveryWarning::TornWrite`.
5. **Rebuild the sparse index** for each segment (deferred to the index-build phase).
6. **Hash-chain verification** (only if the `hash-chain` feature is enabled and `hash_enabled` was set when the data was written). For each record in the last segment, recovery recomputes the BLAKE3 keyed chain hash and compares it to the stored `hash_n`. A mismatch indicates tampering or corruption; recovery records a `RecoveryWarning::HashChainBreak` and truncates at that point.

Recovery is conservative: anything it cannot fully validate is truncated. It never returns a record that might be wrong. Non-fatal problems (torn writes, header corruption of a trailing segment, hash-chain breaks) are collected as warnings in `RecoveryState::warnings` and do not fail `open`.

## Checkpoints

A **checkpoint** is the application's way of telling logdb "I have absorbed every record up to sequence `S`; WAL data before `S` may be truncated." It is the boundary between "still needed for replay" and "safe to reclaim."

```rust
impl LogDb {
    /// Mark `sequence` as the WAL checkpoint. Records with sequence <
    /// checkpoint are safe to delete. Old segments fully covered by the
    /// checkpoint will be truncated on the next roll.
    pub fn checkpoint(&self, sequence: u64);

    /// Get the current checkpoint sequence.
    pub fn checkpoint_sequence(&self) -> u64;
}
```

`checkpoint(sequence)` advances an internal atomic and persists the value to `checkpoint.dat` (see [below](#checkpointdat-on-disk-layout)) so it survives a restart — `checkpoint_sequence()` reads back the same value. The checkpoint is **monotonic**: a lower value is silently ignored.

Two important properties:

- **Checkpointing does not delete records immediately.** It records a boundary. Segments that are *fully* covered by the checkpoint (every record in the segment has `sequence < checkpoint`) are truncated when the active segment **rolls** — so checkpointing is cheap, and space reclamation is amortized into the roll.
- **Records at or after the checkpoint remain recoverable.** `recovery_report().from_sequence` equals the checkpoint; `replay_from(checkpoint)` returns the records the application still needs (those with `sequence >= checkpoint`). This is why you checkpoint a **stable absorbed point**, not the live durable tail — checkpointing the tail would leave nothing to replay (see [The WAL pattern](#the-wal-pattern)).

## checkpoint.dat on-disk layout

The checkpoint is persisted atomically so that a crash mid-write cannot corrupt it (`src/lib.rs:668-683`):

```
Offset  Size  Field
0       8     sequence    (u64, little-endian)
8       4     CRC32C      (u32, little-endian, over bytes 0..8)
```

The write sequence is tmp → write → `fdatasync` → rename → `sync_dir`, which is the standard crash-safe atomic-replace pattern:

1. Write the 12 bytes to `checkpoint.tmp`.
2. `fdatasync` the tmp file (so the data reaches stable storage).
3. `rename(checkpoint.tmp, checkpoint.dat)` (atomic on POSIX).
4. `fsync` the directory (so the rename itself is durable).

On read (`LogDb::load_checkpoint`, `src/lib.rs:513-523`), logdb reads 12 bytes; if the length is not exactly 12 or the CRC32C does not match, the checkpoint is treated as `0` (replay from the beginning). A torn `checkpoint.dat` therefore degrades gracefully rather than corrupting recovery.

## Replay API: recovery_report and replay_from

After `open`, two calls describe what to replay:

```rust
impl LogDb {
    pub fn recovery_report(&self) -> RecoveryReport;
    pub fn replay_from(&self, sequence: u64) -> Result<reader::iter::RecordIter, ReadError>;
}

/// Recovery report returned by `LogDb::recovery_report`.
pub struct RecoveryReport {
    /// First sequence to replay (the last checkpoint).
    pub from_sequence: u64,
    /// Last durable sequence.
    pub to_sequence: u64,
    /// Number of records to replay.
    pub count: u64,
}
```

- `recovery_report()` returns `{ from_sequence: checkpoint, to_sequence: durable_cursor, count: to - from }`. It is the range of records the application should replay to reconstruct state: everything from the last checkpoint up to the durable tail.
- `replay_from(sequence)` is a convenience wrapper around `scan(sequence, u64::MAX)` — it yields every durable record with `id >= sequence`, in order. Only durable records are returned, so the iterator reflects what survived recovery.

```rust
let report = db.recovery_report();
println!("replay {}..{} ({} records)", report.from_sequence, report.to_sequence, report.count);
for rec in db.replay_from(report.from_sequence)? {
    let rec = rec?;
    apply(&rec.content);
}
```

`RecoveryReport::count` is a hint, not a hard bound on the iterator — it is computed from the checkpoint and the durable cursor at report time. Iterate until the iterator ends.

## The WAL pattern

The canonical pattern — write-ahead logging for an application — is demonstrated in `examples/wal.rs`. The lifecycle is **write → flush → checkpoint → (crash) → reopen → replay**:

1. **Open** the data directory. If it pre-exists, recovery runs automatically.
2. **Replay** from the persisted checkpoint to rebuild in-memory state.
3. **Apply** writes by appending intent records (`PUT key value`, `DEL key`), and `flush()` so each intent is durable before the application mutates its in-memory state.
4. **Checkpoint** the sequence at which you resumed — the stable point up to which the application has already absorbed the log. This frees WAL space; records at or after the checkpoint survive for the next replay.
5. **Shutdown** cleanly with `shutdown(timeout)` so the final batch is fsynced. If the process crashes instead, the next `open` recovers and replay rebuilds state from the last checkpoint.

The example models this as a `KvStore` whose state is rebuilt from WAL intent records:

```rust
impl KvStore {
    fn open(data_dir: &str, replay_checkpoint: u64) -> Self {
        let mut config = Config::default();
        config.data_dir = data_dir.into();
        config.durability_mode = DurabilityMode::Async; // explicit flush() for durability
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        let mut data = HashMap::new();
        // Replay from the stable checkpoint to rebuild in-memory state.
        for result in db.replay_from(replay_checkpoint).unwrap() {
            let record = result.unwrap();
            let content = String::from_utf8_lossy(&record.content);
            let parts: Vec<&str> = content.splitn(3, ' ').collect();
            match parts.as_slice() {
                ["PUT", key, value] => { data.insert(key.to_string(), value.to_string()); }
                ["DEL", key]        => { data.remove(*key); }
                _ => {}
            }
        }
        Self { db, data, replay_from: replay_checkpoint }
    }

    fn put(&mut self, key: &str, value: &str) {
        let wal = format!("PUT {} {}", key, value);
        self.db.append(wal.as_bytes()).unwrap();
        self.db.flush().unwrap();          // durable before mutating in-memory state
        self.data.insert(key.to_string(), value.to_string());
    }

    fn checkpoint(&self) {
        // Checkpoint the stable absorbed point, NOT the live durable tail.
        self.db.checkpoint(self.replay_from);
    }
}
```

The key correctness point — which the example calls out explicitly — is that `checkpoint()` marks the **replay point** (the sequence at which the session resumed), not the live `durable_cursor()`. Checkpointing the durable tail would cover the very records just written, leaving `recovery_report().count == 0` and nothing to replay after a crash. Checkpoint the stable point you resumed from; the records you wrote this session (sequences `>= replay_from`) remain recoverable.

Running the example confirms the full loop: Session 1 writes `name`, `email`, `role`, deletes `role`, checkpoints, and shuts down cleanly; Session 2 reopens the same directory, recovery runs, and replay rebuilds `name=Alice`, `email=alice@example.com`, `role=None`.

```
--- Session 1 ---
PUT name = Alice (lsn=1)
PUT batch 2 pairs (lsn=3)
DEL role (lsn=4)
Checkpoint at lsn=0 (durable tail=4)
Closing...
Shutdown: Clean

--- Session 2 (after simulated crash) ---
Recovery report: from=0 to=4 count=4
Recovered 2 key(s) from WAL
name=Some("Alice")
email=Some("alice@example.com")
role=None
Recovery successful: data intact after crash.
```

## What survives a crash vs what is truncated

- **Survives:** every record that was `fdatasync`'d before the crash — i.e. everything up to `durable_cursor()` — *minus* any trailing torn write. Recovery re-validates the last segment and truncates a partial final record, so the surviving prefix is exactly the set of complete, CRC-valid records.
- **Truncated on recovery:** the trailing partial record of a torn write (the crash interrupted `pwrite` mid-record); any record whose CRC fails; any record following a hash-chain break (hash-chain feature only). These become `RecoveryWarning`s.
- **Discarded:** a segment whose header is corrupt, plus every segment after it. Recovery trusts the prefix up to the first bad header.
- **Lost:** records that were appended but never reached stable storage (not `fdatasync`'d). With `DurabilityMode::Sync` this is empty; with `Batch` it is bounded by the batch trigger; with `Async` it persists until the next `flush`/`shutdown` (see [Durability](durability.md#durability-modes)).
- **Checkpointed-away records** are not "lost" — they were already absorbed by the application. They may be physically truncated on the next segment roll to reclaim space; `replay_from(checkpoint)` does not need them.

## See also

- [logdb README](../README.md)
- [Durability](durability.md)
- [Writing](writing.md)
- [Reading](reading.md)
- [Concepts](concepts.md)
- [Configuration](configuration.md)

> logdb 0.2.0

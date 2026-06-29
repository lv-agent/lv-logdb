# Cookbook

Copy-paste recipes for the most common logdb workloads: a database WAL, an append-only event/log store, a tamper-evident confidential audit log, a crash-safe consumer pipeline, standby ingestion, and graceful shutdown inside a long-running service. Every snippet uses the real public API — no invented methods.

## Contents

- [Recipe: Database WAL](#recipe-database-wal)
- [Recipe: Append-only event / log store](#recipe-append-only-event--log-store)
- [Recipe: Tamper-evident confidential audit log](#recipe-tamper-evident-confidential-audit-log)
- [Recipe: Crash-safe consumer pipeline](#recipe-crash-safe-consumer-pipeline)
- [Recipe: Standby ingestion](#recipe-standby-ingestion)
- [Recipe: Graceful shutdown in a service](#recipe-graceful-shutdown-in-a-service)
- [See also](#see-also)

## Recipe: Database WAL

Use logdb as the write-ahead log for a database: append each mutation, `flush` at the commit boundary, advance a `checkpoint` once the application has absorbed the records, and `replay_from` the checkpoint on restart. This is exactly what `examples/wal.rs` does — read it for a full runnable demo.

The five building blocks, with their real signatures:

```rust
impl LogDb {
    /// Append one record; returns its global record_id.
    pub fn append(&self, content: &[u8]) -> Result<u64, AppendError>;

    /// Append multiple records atomically (all-or-nothing w.r.t. crash).
    pub fn append_batch(&self, contents: &[&[u8]]) -> Result<u64, AppendError>;

    /// Block until the durable cursor passes every appended record.
    pub fn flush(&self) -> Result<(), FlushError>;

    /// Mark `sequence` as the WAL checkpoint: records < sequence may be truncated.
    pub fn checkpoint(&self, sequence: u64);

    /// Iterate records in [sequence, end_of_log), used to rebuild state on open.
    pub fn replay_from(&self, sequence: u64) -> Result<RecordIter, ReadError>;
}
```

The skeleton (`Async` mode, because the application issues its own `flush` at commit boundaries):

```rust
use std::time::Duration;
use logdb::config::{Config, DurabilityMode};
use logdb::LogDb;

struct KvStore {
    db: LogDb,
    // replay_from is the checkpoint we resumed from this session. Records
    // written this session (sequences >= replay_from) remain recoverable.
    replay_from: u64,
}

impl KvStore {
    fn open(data_dir: &str, replay_checkpoint: u64) -> Self {
        let mut config = Config::default();
        config.data_dir = data_dir.into();
        config.durability_mode = DurabilityMode::Async; // explicit flush() at commit
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        // Rebuild in-memory state by replaying from the last checkpoint.
        for result in db.replay_from(replay_checkpoint).unwrap() {
            let record = result.unwrap();
            // ...apply the mutation encoded in record.content...
        }
        Self { db, replay_from: replay_checkpoint }
    }

    fn put(&mut self, key: &str, value: &str) {
        let wal = format!("PUT {} {}", key, value);
        self.db.append(wal.as_bytes()).unwrap();
        self.db.flush().unwrap(); // commit boundary: durable before we apply
        // ...apply to in-memory state...
    }

    fn checkpoint(&self) {
        // Checkpoint the stable point we resumed from, NOT the live durable
        // tail. A checkpoint at the durable cursor would cover the very records
        // you just wrote, leaving recovery nothing to replay.
        self.db.checkpoint(self.replay_from);
    }

    fn close(self) {
        let _ = self.db.shutdown(Duration::from_secs(5)).unwrap();
    }
}
```

Two correctness points the WAL pattern depends on:

1. **`flush` before apply.** Write the WAL entry, call `flush` (so the durable cursor passes it), and only then mutate in-memory state. A crash after `flush` replays the entry; a crash before `flush` does not.
2. **Checkpoint the resume point, not the tip.** `checkpoint(sequence)` tells logdb "records before `sequence` may be truncated." If you checkpoint the live `durable_cursor()`, you cover the records you just wrote, and the next `recovery_report().count` is `0`. See [Recovery: checkpoints](recovery.md#checkpoints) and the note in `examples/wal.rs`.

See [Recovery: the WAL pattern](recovery.md#the-wal-pattern) for the crash-replay contract.

## Recipe: Append-only event / log store

For a write-heavy event or application-log store (telemetry, access logs, audit events) you want high-throughput appends and ordered replay from the beginning (or from any offset). Use default `Batch` durability and `scan` / `replay_from` for history.

```rust
use logdb::Config;
use logdb::LogDb;

let mut config = Config::default();
config.data_dir = "./event-store".into();
let db = LogDb::open(config).unwrap();

// Writers append events. Batch mode amortizes fsync across many records,
// giving high throughput with a bounded data-at-risk window.
let id = db.append(b"{\"level\":\"info\",\"msg\":\"hello\"}").unwrap();

// Historical replay: iterate everything from the beginning.
for result in db.replay_from(0).unwrap() {
    let record = result.unwrap();
    println!("seq={} ts={} {}", record.record_id, record.timestamp_ns,
             String::from_utf8_lossy(&record.content));
}

// Range scan: iterate [from_id, to_id).
let iter = db.scan(100, 200).unwrap();
for result in iter {
    let record = result.unwrap();
    // ...
}
```

For **live consumers** that follow the tail of the log with their own crash-safe cursor, use a tailer rather than polling `scan` — see [Recipe: crash-safe consumer pipeline](#recipe-crash-safe-consumer-pipeline) below and [Tailers](tailers.md).

## Recipe: Tamper-evident confidential audit log

Combine the `hash-chain` and `encryption` features so the log is both **tamper-evident** (any after-the-fact mutation of a sealed segment is detectable on read) and **confidential** (records are encrypted at rest with AES-256-GCM). Both are transparent on read — the same `read` / `scan` / `replay_from` APIs verify the chain and decrypt transparently, with no caller-side change.

```toml
# Cargo.toml — enable both features.
[dependencies]
logdb = { version = "0.2.0", features = ["hash-chain", "encryption"] }
```

```rust
use logdb::Config;
use logdb::LogDb;

// 32-byte AES-256 key — generate and manage this out of band (KMS / vault /
// envelope encryption). Key loss is unrecoverable: records encrypted with a
// lost key cannot be decrypted.
let key: [u8; 32] = /* your key */;

let mut config = Config::default();
config.data_dir = "./audit-log".into();
config.hash_enabled = true;       // hash-chain: tamper-evident
config.encryption_key = Some(key); // encryption: confidential at rest
// hash-chain requires shards == 1 (the Sealer seals one shard at a time).
config.shards = 1;
let db = LogDb::open(config).unwrap();

// Every record is sealed into the BLAKE3 keyed chain AND encrypted with GCM.
db.append(b"2026-06-30T12:00:00Z user=alice action=login").unwrap();
db.flush().unwrap();
```

What each feature contributes:

- **`hash-chain`** seeds a per-database BLAKE3 key (`hash_init`, persisted in every segment header) and chains each record's hash with the previous one, so a byte changed anywhere in a sealed segment breaks verification of every subsequent record. The Sealer background thread runs only when `hash_enabled` and `shards == 1`. See [Features: hash-chain](features.md#hash-chain).
- **`encryption`** encrypts each frame with AES-256-GCM using a per-frame random nonce, and GCM's authentication tag detects tampering on read (surfaced like a CRC failure). See [Features: encryption](features.md#encryption).

> The hash chain detects corruption/tampering that does **not** also rebuild the chain; encryption raises the bar by hiding the plaintext and detecting GCM-tag failures. Neither alone is a full security boundary against an attacker who can both rewrite bytes and recompute the chain — for that threat model, layer external controls. See the caveats in [Features](features.md#hash-chain).

## Recipe: Crash-safe consumer pipeline

Deliver records to a downstream system (a replica, a search index, a message broker) with at-least-once delivery and a crash-safe cursor. Use `new_tailer` + `next_batch` + `commit`.

```rust
use std::time::Duration;
use logdb::LogDb;

fn run_pipeline(db: &LogDb) -> Result<(), Box<dyn std::error::Error>> {
    // new_tailer returns a Tailer, NOT a Result. If tailer_indexer.dat exists,
    // the saved position is restored; otherwise it starts at 0.
    let mut t = db.new_tailer("indexer");

    loop {
        match t.next_batch(500)? {
            Some(batch) => {
                // 1. Deliver the side effect FIRST — make it durable before
                //    advancing the cursor.
                deliver_to_downstream(&batch)?;

                // 2. Only then commit progress. A crash between next_batch and
                //    commit replays the batch (at-least-once); a crash after
                //    commit is past it.
                t.commit()?;
            }
            None => {
                // Tail is caught up; back off briefly before polling again.
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
}
```

Two invariants this recipe enforces (see [Tailers: progress is persisted only on commit](tailers.md#progress-is-persisted-only-on-commit)):

1. **Deliver, then commit.** The side effect must be durable before the cursor advances, so a crash never loses a record that has neither been delivered nor left pending.
2. **Idempotent delivery.** Because of point 1's replay-on-crash guarantee, `deliver_to_downstream` may see the same batch twice across a restart. Make it idempotent (upsert by record id, or de-duplicate via an idempotency key).

For higher throughput, batch larger (`next_batch(10_000)`); for lower latency, poll more often. The `None` branch is your back-pressure signal — there is nothing new to read. See [Tailers: a consumer-loop recipe](tailers.md#a-consumer-loop-recipe).

## Recipe: Standby ingestion

In a primary/standby setup, a standby node ingests records received from the primary at the **primary's own sequence numbers**, preserving the global offset space so consumers can fail over primary → standby without re-mapping offsets. This is what `LogDb::replicate` is for.

```rust
use logdb::Config;
use logdb::LogDb;

// replicate requires shards == 1 (it is a linear stream onto shard 0).
let mut config = Config::default();
config.data_dir = "./standby-data".into();
config.shards = 1;
let db = LogDb::open(config).unwrap();

// Ingest a record received from the primary via your Sync RPC.
let sequence = 1234u64;       // the primary's record_id for this record
let timestamp_ns = 1_700_000_000_000_000_000u64;
let content: &[u8] = b"replicated payload";
db.replicate(sequence, timestamp_ns, content).unwrap();
```

The `replicate` contract (`src/lib.rs:326-391`, see [Features: remote-push](features.md#remote-push)):

- **Single-shard.** `shards` must be `1`; otherwise `AppendError::Io("replicate requires shards=1")`.
- **In-order.** `sequence` must equal the current producer cursor; a gap returns `AppendError::Io("replicate out of order: expected {cur}, got {sequence}")`, so the caller retries the same sequence until it lands.
- **Idempotent.** A `sequence` already replicated (below the cursor) is a no-op `Ok(())`, so duplicate or replayed Sync RPCs are safe.
- **Backpressured.** Refuses to overwrite a live (uncommitted) slot, returning `AppendError::QueueFull` via the same watermark gate as `append`.

There is **no** one-line `db.push(...)` API on the primary side in v1.1 — the Pusher / `RemoteSink` plumbing is daemon-level and private. See [Features: remote-push](features.md#remote-push) for that gap and the daemon integration pattern.

## Recipe: Graceful shutdown in a service

Inside a long-running service the `LogDb` is shared (typically `Arc<LogDb>`), so you cannot call `shutdown(self, timeout)` (which consumes the only strong reference). Use `drain(&self, timeout)` instead: it flushes everything to durable storage without consuming the handle or joining threads, so it works with `Arc<LogDb>`.

```rust
use std::sync::Arc;
use std::time::Duration;
use logdb::LogDb;

// Inside your service, the handle is shared:
let db: Arc<LogDb> = /* ... */;

// On SIGTERM / drain signal: flush all in-flight appends and fsync up to the
// producer cursor. Takes &self, so it works with Arc<LogDb>.
match db.drain(Duration::from_secs(10)) {
    Ok(report) => {
        // report is a ShutdownReport: Clean | PartialDurable | TimedOut.
        // Clean => every record appended before the call is now durable.
        println!("drain: {:?}", report);
    }
    Err(e) => {
        // FlushError::Timeout | FlushError::Aborted.
        eprintln!("drain failed: {:?}", e);
    }
}

// After drain returns Ok(Clean), new appends return AppendError::ShuttingDown.
// The background threads keep running; the process may then exit (threads are
// aborted harmlessly on drop, because the data is already durable).
```

Why `drain` and not `shutdown` here:

- **`drain(&self, timeout)`** enters the drain phase (rejecting new appends with `AppendError::ShuttingDown`), waits for in-flight appends to publish, and fsyncs everything up to the producer cursor. The background threads keep running. Returns `Result<ShutdownReport, FlushError>`.
- **`shutdown(self, timeout)`** consumes `self`, drains, and *then* joins the background threads. It requires the handle be the *only* strong reference, so it does not work with `Arc<LogDb>` when other clones exist.

On WSL2, `fdatasync` latency can cause a `Clean` drain to be reported as `PartialDurable` conservatively — the classification errs on the pessimistic side. See [Durability: graceful shutdown](durability.md#graceful-shutdown) and [Errors: ShutdownReport](errors.md#shutdownreport).

## See also

- [Usage guide](README.md)
- [Writing](writing.md) — `append`, `append_batch`, backpressure.
- [Reading](reading.md) — `read`, `scan`, `replay_from`.
- [Durability](durability.md) — `flush`, `drain`, `shutdown`, `ShutdownReport`.
- [Recovery](recovery.md) — the WAL pattern, checkpoints, crash replay.
- [Tailers](tailers.md) — `new_tailer`, `next_batch`, `commit`.
- [Features](features.md) — `hash-chain`, `encryption`, `remote-push` / `replicate`.

> logdb 0.2.0

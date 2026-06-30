# Writing

How to append records to logdb: the single-record `append`, the atomic `append_batch`, the size limit, backpressure, and the error cases the writer must handle.

## Contents

- [Appending a single record](#appending-a-single-record)
- [Atomic batch append](#atomic-batch-append)
- [Content size limit](#content-size-limit)
- [Backpressure and QueueFullPolicy](#backpressure-and-queuefullpolicy)
- [Disk full, shutdown, and other errors](#disk-full-shutdown-and-other-errors)
- [When to flush](#when-to-flush)

## Appending a single record

`LogDb::append` writes one record and returns its **global record id**:

```rust
impl LogDb {
    pub fn append(&self, content: &[u8]) -> Result<u64, AppendError>;
}
```

The returned `u64` is what you pass back to [`read`](reading.md#point-reads). In the default single-partition case it equals `record.id.sequence` (see [Concepts](concepts.md#sequence-space-and-monotonicity)).

```rust
let id = db.append(b"order-created 42")?;
db.flush()?;
let rec = db.read(id)?.expect("record must exist after flush");
assert_eq!(rec.content, b"order-created 42");
```

Before claiming a slot, `append` performs three pre-checks (in order, see `src/lib.rs:264-303`):

1. **Health check** — if the database is in a degraded state (disk full / unhealthy), it returns `AppendError::DiskFull` (for ENOSPC) or `AppendError::Io("health check failed")`.
2. **Content size check** — if `content.len() > config.max_content_size`, it returns `AppendError::ContentTooLarge { size, max }`.
3. **Shutdown guard** — once the database has started shutting down or draining, it returns `AppendError::ShuttingDown`.

Only after all three pass does it CAS-claim a slot on the matching shard's ring, write the content, and publish. The record becomes visible to the background Committer immediately, but is **not** visible to readers until fsynced (see [Reading](reading.md#visibility-and-the-durable-cursor)).

## Atomic batch append

When you need several records to commit together, use `append_batch`:

```rust
impl LogDb {
    pub fn append_batch(&self, contents: &[&[u8]]) -> Result<u64, AppendError>;
}
```

`append_batch` is **atomic**: every record in the batch is committed together — after a crash either the whole batch is visible or none of it is. It returns the global record id of the **first** record in the batch; subsequent records occupy consecutive sequences.

```rust
let batch: &[&[u8]] = &[b"a", b"b", b"c"];
let first = db.append_batch(batch)?;
db.flush()?;
// The three records occupy consecutive ids: first, first+1, first+2.
assert_eq!(db.read(first)?.unwrap().content, b"a");
assert_eq!(db.read(first + 2)?.unwrap().content, b"c");
```

The atomicity guarantee rests on a single atomic reservation. All `contents.len()` sequences are reserved in **one atomic `claim_batch`** call (an atomic `producer_cursor += n` on the chosen shard), so there is no partial reservation: consecutive batches never overwrite each other's slots. Before reserving, `append_batch` validates **every** content's size up front, because a too-large record discovered after a partial claim would leave reserved-but-unwritten slots — a gap the Committer cannot cross (`src/lib.rs:226-262`).

Notes:

- An empty batch is rejected with `AppendError::ContentTooLarge { size: 0, max: 0 }`.
- The same pre-checks (health / size / shutdown) and the same `QueueFullPolicy` apply as for `append`.

## Content size limit

`config.max_content_size` (default **1 MiB**, capped at **≤ 64 MiB** by `Config::validate`) bounds a single record's content. Exceeding it fails fast with a structured error before any slot is claimed:

```rust
pub enum AppendError {
    ContentTooLarge { size: usize, max: usize },
    // ...
}
```

```rust
let mut config = Config::default();
config.data_dir = dir.path().to_path_buf();
config.max_content_size = 100;
let db = LogDb::open(config)?;
let err = db.append(&vec![0u8; 200]).unwrap_err();
assert!(matches!(err, AppendError::ContentTooLarge { size: 200, max: 100 }));
```

The same limit applies to every record inside `append_batch`; the first oversized record aborts the whole batch before any sequence is reserved.

Separately, recall the **256-byte inline/spill boundary** (`INLINE_CAP`) is a *performance* threshold, not a correctness one — records ≤ 256 bytes take the zero-alloc inline fast path, while larger records spill to the heap. See [Concepts: Inline vs spill](concepts.md#inline-vs-spill).

## Backpressure and QueueFullPolicy

Producers do not write segments directly; they write into a fixed-size ring buffer (`config.ring_size` slots per shard, default 8192), and a background Committer drains it. If producers outrun the Committer by more than `ring_size`, the ring is full and `config.queue_full_policy` decides what happens (`src/config.rs:9-15`):

```rust
pub enum QueueFullPolicy {
    /// Block (spin + backoff) until a slot becomes available.
    Block,
    /// Immediately return AppendError::QueueFull.
    Drop,
}
```

- **`Block`** (default) — the caller spins with backoff until a slot frees up. Throughput stays high under load; latency rises as the ring fills. Suitable when losing a record is unacceptable.
- **`Drop`** — `append` / `append_batch` immediately returns `AppendError::QueueFull`. The caller decides whether to retry, shed load, or back off. Suitable when you would rather drop a record than stall the producer thread.

```rust
let mut config = Config::default();
config.queue_full_policy = QueueFullPolicy::Drop;
```

A full ring almost always means the Committer (or the device's `fdatasync`) is saturated. The fix is operational, not API-level: reduce append rate, increase `ring_size`, shard out (`config.shards`), or move to faster storage. The gap between `producer_cursor()` and `committed_cursor()` quantifies Committer saturation (see [Concepts: Cursor semantics](concepts.md#cursor-semantics)).

## Disk full, shutdown, and other errors

`AppendError` in full (`src/error.rs`):

```rust
pub enum AppendError {
    /// Ring buffer full and policy is Drop.
    QueueFull,
    /// Content exceeds config.max_content_size.
    ContentTooLarge { size: usize, max: usize },
    /// Underlying disk is full (ENOSPC). May be self-healing.
    DiskFull,
    /// A non-ENOSPC I/O error occurred.
    Io(String),
    /// The database is shutting down and not accepting new appends.
    ShuttingDown,
}
```

Handling guidance:

- `QueueFull` — retry with backoff, shed load, or switch to `Block`.
- `ContentTooLarge { size, max }` — fix the producer; never silently truncate.
- `DiskFull` — the device returned ENOSPC. logdb marks itself unhealthy so subsequent appends fail fast. It can **self-heal**: once space frees up and the Committer successfully writes again, the health flag clears and appends resume. Treat this as a retryable-but-urgent condition.
- `Io(String)` — a non-ENOSPC I/O failure (also reported when the health check fails for a non-disk reason). Inspect the message; the database is likely degraded.
- `ShuttingDown` — `shutdown`, `drain`, or `Drop` has begun. Stop appending; finalize your pipeline.

## When to flush

`append` (and `append_batch`) only publishes records to the Committer. They become **durable** — and therefore visible to readers — once fsynced. With the default `DurabilityMode::Batch`, fsync happens on a size/count/time threshold; with `Async`, only on explicit `flush` / shutdown; with `Sync`, after every batch.

Call `flush` when you need a durability barrier, for example:

- after a batch you must not lose on crash (an order book's tick, a commit marker),
- before reading a record back with `read` — readers only see `record_id < durable_cursor()`,
- before checkpointing or coordinating with an external system.

```rust
db.append(b"commit-marker")?;
db.flush()?; // blocks until durable_cursor passes the marker
```

See [Durability](durability.md) for the full fsync/durability model and the trade-offs between `Sync`, `Batch`, and `Async`.

## See also

- [logdb README](../README.md)
- [Reading](reading.md)
- [Concepts](concepts.md)
- [Durability](durability.md)
- [Errors](errors.md)
- [Configuration](configuration.md)

> logdb 0.2.0

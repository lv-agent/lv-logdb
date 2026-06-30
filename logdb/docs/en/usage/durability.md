# Durability

What "durable" means in logdb, how `flush` provides a barrier, the three `DurabilityMode` policies and their throughput/latency trade-offs, and how to shut down without losing data.

## Contents

- [What "durable" means](#what-durable-means)
- [flush: the durability barrier](#flush-the-durability-barrier)
- [Durability modes](#durability-modes)
- [Choosing a mode](#choosing-a-mode)
- [Graceful shutdown](#graceful-shutdown)
- [The crash guarantee](#the-crash-guarantee)

## What "durable" means

A record goes through three stages between `append` and the disk:

1. **Published** — `append` has claimed a slot, written the content, and marked it visible to the background Committer. The record is in memory only.
2. **Committed** — the Committer has written the record into a segment file via `pwrite`, but the file has **not** been `fdatasync`'d. A power loss may or may not reach it.
3. **Durable** — the record has been `fdatasync`'d to the underlying device. It will survive a crash.

Only the third stage counts as durable. Readers are gated by the **durable cursor**: `read(record_id)` returns `Ok(None)` for any `record_id >= durable_cursor()`, so a record a reader can see is guaranteed to survive a crash (see [Reading: Visibility and the durable cursor](reading.md#visibility-and-the-durable-cursor)). The minimum durable cursor across all shards is exposed by `LogDb::durable_cursor()`.

## flush: the durability barrier

`LogDb::flush` blocks until the **durable cursor** reaches the current producer cursor — i.e. until every record appended before the call has been `fdatasync`'d:

```rust
impl LogDb {
    /// Waits for `durable_cursor` (NOT `committed_cursor` — fix C4).
    pub fn flush(&self) -> Result<(), FlushError>;
}
```

Two things to note about the signature and the implementation (`src/lib.rs:393-422`):

- **It waits on `durable_cursor`, not `committed_cursor`.** A record being committed (written by `pwrite`) is not enough; `flush` only returns once the data is on stable storage. This is the correctness fix called out in the source comment.
- **It honors `config.flush_timeout`** (default 30 s). If the Committer does not reach the target within the timeout, `flush` returns an error instead of hanging forever.

`FlushError` (`src/error.rs:36-46`):

```rust
pub enum FlushError {
    /// The flush did not complete within the configured timeout.
    Timeout,
    /// The database was aborted during the flush wait.
    Aborted,
}
```

- `Timeout` — the durable cursor did not reach the producer cursor within `config.flush_timeout`. The data is still in flight; the Committer keeps trying. Retry, raise `flush_timeout`, or investigate Committer saturation (see [Writing: Backpressure](writing.md#backpressure-and-queuefullpolicy)).
- `Aborted` — a concurrent `shutdown` / `drain` / `Drop` aborted the wait. Stop appending and finalize.

When the `hash-chain` feature is enabled and `hash_enabled` is set, `flush` first waits for the Sealer to chain the target records before requesting the fsync, so a successfully flushed record is both durable and tamper-evident.

```rust
db.append(b"commit-marker")?;
db.flush()?; // blocks until durable_cursor passes the marker
```

## Durability modes

`config.durability_mode` controls when the Committer calls `fdatasync` (`src/config.rs:17-26`). The default is `Batch`.

```rust
pub enum DurabilityMode {
    /// `fdatasync` after every commit batch.
    Sync,
    /// `fdatasync` when the batch size or time threshold is met.
    Batch,
    /// `fdatasync` only on explicit `flush()` or shutdown.
    Async,
}
```

- **`Sync`** — `fdatasync` after every commit batch. Every committed record is immediately durable. Strongest crash guarantee; highest per-record latency because every batch pays an fsync. Use when losing any committed record on crash is unacceptable and the workload can tolerate the latency.
- **`Batch`** *(default)* — `fdatasync` when the Committer's batch trigger fires: `256 KiB` of buffered data, `1024` records, or `10 ms` since the first pending record (see `CommitTrigger` in `src/config.rs:68-90`). Amortizes fsync across many records, giving high throughput with bounded window of in-flight (committed-but-not-yet-durable) data. The right default for most services.
- **`Async`** — `fdatasync` only on explicit `flush()` or shutdown. Highest throughput and lowest steady-state latency; data sits in the page cache until the application forces a barrier. Use when the application issues its own `flush` at commit boundaries (this is what the WAL example does), or when durability is provided by an external mechanism.

```rust
let mut config = Config::default();
config.data_dir = dir.path().to_path_buf();
config.durability_mode = DurabilityMode::Async; // rely on explicit flush()
config.flush_timeout = Duration::from_secs(5);
```

Independent of the mode, `flush()`, `drain()`, and `shutdown()` always force an fsync up to the producer cursor — so an explicit barrier overrides the mode.

## Choosing a mode

| Workload | Recommended mode | Why |
| --- | --- | --- |
| Financial ledger, commit log, anything where a lost committed record is a correctness bug | `Sync` | Every batch is durable before the next begins. |
| General-purpose service, telemetry, event stream | `Batch` *(default)* | High throughput, bounded data-at-risk window (≤ ~256 KiB / 10 ms). |
| Application manages its own commit points (WAL for a DB, batch pipelines) | `Async` + explicit `flush()` | Lowest overhead; the app decides exactly where the barriers go. |

The trade-off is always **data-at-risk window vs. fsync cost**. `Sync` makes the window zero at the cost of one fsync per batch; `Async` makes the window application-controlled at the cost of requiring discipline; `Batch` is the pragmatic middle.

## Graceful shutdown

There are two ways to leave a running `LogDb`, and they exist because the handle is sometimes owned and sometimes shared via `Arc`:

```rust
impl LogDb {
    /// Shared-safe drain: flush all to durable WITHOUT consuming the handle
    /// or joining threads. Takes `&self`, so works with `Arc<LogDb>`.
    pub fn drain(&self, timeout: Duration) -> Result<ShutdownReport, FlushError>;

    /// Drain, then join the background threads. Consumes the handle and
    /// requires it be the only strong reference.
    pub fn shutdown(self, timeout: Duration) -> Result<ShutdownReport, ShutdownError>;
}
```

### `shutdown(self, timeout)` — owned handle

Use `shutdown` when you **own** the `LogDb` (it is not wrapped in `Arc`, or you hold the only strong reference). It drains in-flight appends, fsyncs everything up to the producer cursor, and then joins the Committer (and Sealer, if hash-chain is enabled). Because it consumes `self`, it can take the threads down cleanly. If other strong references exist it returns `ShutdownError::JoinError("LogDb still referenced")`.

```rust
let report = db.shutdown(Duration::from_secs(5))?;
println!("shutdown: {:?}", report);
```

`ShutdownError` (`src/error.rs:64-74`):

```rust
pub enum ShutdownError {
    /// Shutdown did not complete within the timeout.
    Timeout,
    /// Background threads could not be joined.
    JoinError(String),
}
```

### `drain(&self, timeout)` — shared handle

Use `drain` when the `LogDb` is **shared**, typically `Arc<LogDb>` inside a long-running service. It takes `&self`, enters the drain phase (after which new appends return `AppendError::ShuttingDown`), waits for in-flight appends to publish, and fsyncs everything up to the producer cursor. The background threads **keep running**; the process may then exit (the threads are aborted harmlessly on drop, because the data is already durable), or `shutdown` may still be called to join them.

```rust
// Inside a service that holds `Arc<LogDb>`:
let report = shared_db.drain(Duration::from_secs(5))?;
```

### `ShutdownReport`

Both `drain` and `shutdown` return a `ShutdownReport` describing how much data was made durable (`src/error.rs:77-85`):

```rust
pub enum ShutdownReport {
    /// All data was durably persisted before shutdown.
    Clean,
    /// Some data was committed but not fsynced before the timeout.
    PartialDurable,
    /// Shutdown timed out; some data may be lost.
    TimedOut,
}
```

- `Clean` — every record appended before the call is now durable. This is the normal, successful outcome.
- `PartialDurable` — the drain timed out mid-flush: some records were published but the durable cursor did not reach the producer cursor. The background threads may still complete the fsync after the call returns. Treat as "mostly safe, investigate why the Committer fell behind."
- `TimedOut` — returned by `shutdown` when `drain` itself times out; some in-flight data may be lost.

> Note: on some configurations (notably WSL2), `fdatasync` latency can cause a `Clean` drain to be reported as `PartialDurable` even when the data has in fact reached stable storage. The classification is conservative.

## The crash guarantee

The contract logdb gives the application is precise:

- **Any record a reader can see survives a crash.** Reads are bounded by the durable cursor; a record only becomes readable after `fdatasync`. There is no "read it now, lose it on crash" window.
- **A successfully `flush`ed record survives a crash.** `flush` returns only after `durable_cursor` passes the flushed records.
- **Records appended but not yet flushed may or may not survive**, depending on whether the Committer happened to `fdatasync` them before the crash. With `Sync` this window is empty; with `Batch` it is bounded by the batch trigger; with `Async` it persists until the next explicit barrier.
- **A batch is atomic with respect to crashes.** `append_batch` reserves all its sequences in one atomic claim, so after a crash either every record in the batch is visible or none is — never a partial batch (see [Writing: Atomic batch append](writing.md#atomic-batch-append)).
- **Torn writes are repaired on restart.** If a crash interrupts a `pwrite`, the trailing partial record is detected by its CRC and truncated during recovery (see [Recovery](recovery.md)).

## See also

- [logdb README](../README.md)
- [Recovery](recovery.md)
- [Writing](writing.md)
- [Reading](reading.md)
- [Concepts](concepts.md)
- [Configuration](configuration.md)

> logdb 0.2.0

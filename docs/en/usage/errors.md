# Errors

The complete logdb error catalog, grouped by operation, with the trigger condition and recommended handling for every variant. All error types are defined in `src/error.rs` and derive `std::error::Error` via `thiserror`, so they interoperate cleanly with `?`, `anyhow`, and structured logging.

## Contents

- [Why the errors are structured this way](#why-the-errors-are-structured-this-way)
- [AppendError](#appenderror)
- [FlushError](#flusherror)
- [ReadError](#readerror)
- [ShutdownError](#shutdownerror)
- [ShutdownReport](#shutdownreport)
- [See also](#see-also)

## Why the errors are structured this way

logdb splits its errors into five enums, one per operation family, rather than one giant enum. Each variant names a distinct, actionable condition so the caller can match on it and choose the right response (retry, backpressure, degrade, or report) without parsing strings:

```rust
// src/error.rs — every variant derives std::error::Error via thiserror.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AppendError { /* ... */ }
```

Because all five derive `thiserror::Error`, you get:

- **`std::error::Error` + `Display`** for free — every variant has a human-readable message and works with `?` across `Result<_, Box<dyn Error>>`, `anyhow::Result`, and `eyre`.
- **`Clone`, `Debug`, `PartialEq`, `Eq`** — errors are cheap value types. You can compare them in tests (`assert_eq!`) and clone them into logs or metrics.
- **No `Backtrace` capture by default** — errors carry the *condition*, not a stack trace. Pair them with `tracing` spans if you need causal context.

The recommended dispatch shape is a `match` keyed on the variant:

```rust
use logdb::AppendError;

match db.append(&payload) {
    Ok(id) => /* ... */,
    Err(AppendError::QueueFull)     => /* backpressure: retry or shed load */,
    Err(AppendError::ContentTooLarge { size, max }) => /* reject the record */,
    Err(AppendError::DiskFull)      => /* alert ops; the DB may self-heal */,
    Err(AppendError::Io(msg))       => /* log and propagate */,
    Err(AppendError::ShuttingDown)  => /* stop accepting work */,
}
```

## AppendError

Returned by `append`, `append_batch`, and `replicate` (`src/error.rs:8-34`). These are the conditions a producer must handle.

```rust
pub enum AppendError {
    QueueFull,
    ContentTooLarge { size: usize, max: usize },
    DiskFull,
    Io(String),
    ShuttingDown,
}
```

| Variant | Trigger condition | Recommended handling |
|---------|-------------------|----------------------|
| `QueueFull` | The ring buffer is full **and** `queue_full_policy` is `Drop`. Returned (rather than blocking) so the caller can shed load. | **Backpressure / retry.** Either retry after a short backoff, switch the policy to `Block` (which waits for a slot), or shed load by dropping the record and incrementing a "dropped writes" metric. See [Writing: Backpressure and QueueFullPolicy](writing.md#backpressure-and-queuefullpolicy). |
| `ContentTooLarge { size, max }` | The record is larger than `config.max_content_size` (default 1 MB). `size` is the rejected record's length; `max` is the configured limit. Note: an empty `append_batch(&[])` also returns this with `{ size: 0, max: 0 }`. | **Reject the record.** This is a programming / contract error, not a transient one — do not retry blindly. Truncate or compress the payload, or raise `max_content_size`. |
| `DiskFull` | The underlying disk is full (ENOSPC), detected by the health monitor. May be **self-healing**: if space frees up, appends resume. | **Alert ops, then retry.** Log loudly, surface a metric, and retry on a backoff; if the disk is freed (log rotation, retention trim, manual cleanup) the DB recovers without a restart. |
| `Io(String)` | A non-ENOSPC I/O error occurred on the write path (e.g. a `pwrite` failure, a permission error, or `replicate` invoked with `shards > 1` or out of order — those are reported as `Io` with a descriptive message). | **Propagate.** These are usually not transient in a useful way. Log the message and surface the error; do not loop-retry without operator intervention. For `replicate` ordering errors, see [Features: remote-push](features.md#remote-push). |
| `ShuttingDown` | The database has entered the drain phase (`drain` or `shutdown` was called) and is no longer accepting new appends. | **Stop appending.** Finalize in-flight work; do not retry — the handle is shutting down for good. |

> `replicate` additionally surfaces its single-shard, in-order, idempotent contract through `AppendError`: `Io("replicate requires shards=1")` for multi-shard, `Io("replicate out of order: expected {cur}, got {sequence}")` for a gap (retry the same sequence), and `QueueFull` when the Committer has fallen behind. A `sequence` already replicated returns `Ok(())` (idempotent no-op).

## FlushError

Returned by `flush` and (transitively) by `drain` (`src/error.rs:37-46`). These describe why a durability barrier did not complete.

```rust
pub enum FlushError {
    Timeout,
    Aborted,
}
```

| Variant | Trigger condition | Recommended handling |
|---------|-------------------|----------------------|
| `Timeout` | The durable cursor did not reach the producer cursor within `config.flush_timeout` (default 30 s). The data is still in flight — the Committer keeps trying. | **Retry, raise the timeout, or investigate.** A transient timeout under heavy load may clear on a second `flush`; a persistent one means the Committer cannot keep up (slow disk, or `durability_mode` / batch settings mismatched with the write rate). Raise `flush_timeout`, check disk health, or reduce the write rate. |
| `Aborted` | A concurrent `shutdown` / `drain` / `Drop` aborted the wait while `flush` was blocked. | **Stop appending and finalize.** The handle is being torn down; do not retry the flush. See [Durability: graceful shutdown](durability.md#graceful-shutdown). |

`drain(&self, timeout)` returns `Result<ShutdownReport, FlushError>`: a drain that cannot reach the durable cursor in time returns `FlushError::Timeout` (the drain itself aborts), while a successful drain returns one of the `ShutdownReport` variants below.

## ReadError

Returned by `read`, `scan`, and `replay_from` (`src/error.rs:49-62`). These describe why a read could not satisfy the request.

```rust
pub enum ReadError {
    NotFound(u64),
    CrcMismatch(u64),
    Io(String),
}
```

| Variant | Trigger condition | Recommended handling |
|---------|-------------------|----------------------|
| `NotFound(id)` | The requested `record_id` does not exist — it is past the durable cursor (not yet visible) or beyond the end of the log. | **Degrade.** For a point read, treat as "no record yet" and retry later or surface a not-found to the caller. For a scan/tailer, this is the normal tail-of-log signal; see [Reading: visibility and the durable cursor](reading.md#visibility-and-the-durable-cursor). |
| `CrcMismatch(id)` | The CRC check failed at `record_id`, indicating on-disk corruption (bit rot, a partial write, or — with `encryption` enabled — a tampered ciphertext frame, which GCM detects like a CRC failure). | **Report and stop.** This is data corruption, not a transient condition. Log the offending `record_id`, alert ops, and do not continue scanning past it blindly — the corrupted record may be unreadable. With hash-chain enabled, a mismatch in the chain is reported through verification, not through this variant (see [Features: hash-chain](features.md#hash-chain)). |
| `Io(String)` | An I/O error occurred while opening or reading a segment file (e.g. the segment was deleted under you, or a low-level read failed). | **Propagate.** Log the message and surface the error; retry only if the underlying cause is known to be transient. |

> `read` returns `Ok(None)` (not an error) when a record exists logically but is not yet durable. `ReadError::NotFound` is reserved for IDs that are genuinely absent — past the end of the log. See [Reading: read errors](reading.md#read-errors).

## ShutdownError

Returned by `shutdown` (`src/error.rs:65-74`). These describe why an owned-handle shutdown could not complete cleanly.

```rust
pub enum ShutdownError {
    Timeout,
    JoinError(String),
}
```

| Variant | Trigger condition | Recommended handling |
|---------|-------------------|----------------------|
| `Timeout` | Shutdown did not complete within the requested timeout (the drain phase timed out, or the background threads could not be joined in time). | **Report and move on.** Some in-flight data may be lost. Log the outcome; in a process-exit path you typically cannot do much more. If you need a softer landing next time, use `drain` first (shared handle) to flush, then `shutdown` to join. |
| `JoinError(String)` | The background threads (Committer, and Sealer if hash-chain is enabled) could not be joined — most commonly because the `LogDb` handle is still referenced elsewhere (`"LogDb still referenced"`). | **Fix the reference count, then retry.** `shutdown` consumes `self` and requires it be the *only* strong reference. If the handle is shared via `Arc`, use `drain` instead, or drop the other `Arc` clones first. |

`drain` (the shared-handle path) does **not** return `ShutdownError` — it returns `Result<ShutdownReport, FlushError>`, because it does not join threads.

## ShutdownReport

Returned (as `Ok`) by both `drain` and `shutdown` to describe how much data was made durable (`src/error.rs:77-85`). `ShutdownReport` is **not** an error — it is a success value with three degrees of "how clean."

```rust
pub enum ShutdownReport {
    Clean,
    PartialDurable,
    TimedOut,
}
```

| Variant | Meaning | Recommended handling |
|---------|---------|----------------------|
| `Clean` | Every record appended before the call is now durable. This is the normal, successful outcome. | Proceed to exit. |
| `PartialDurable` | Some records were committed but not fsynced before the timeout. The background threads may still complete the fsync after the call returns. | **Investigate why the Committer fell behind.** Treat as "mostly safe" but not guaranteed; on WSL2 specifically, a `Clean` drain can be misclassified as `PartialDurable` because of `fdatasync` latency (see [Durability: graceful shutdown](durability.md#graceful-shutdown)). |
| `TimedOut` | Shutdown timed out; some in-flight data may be lost. | **Alert.** Data loss is possible. Log loudly and surface a metric so operators can correlate with the crash/exit. |

The classification is conservative: when in doubt logdb reports the more pessimistic variant rather than claiming durability it cannot prove.

## See also

- [Usage guide](README.md)
- [Writing](writing.md) — `QueueFullPolicy` and append-path errors in context.
- [Reading](reading.md) — `ReadError` and the durable-cursor visibility rule.
- [Durability](durability.md) — `FlushError`, `shutdown`, `drain`, and `ShutdownReport` in depth.
- [Recovery](recovery.md) — what `CrcMismatch` means after a crash.

> logdb 0.2.0

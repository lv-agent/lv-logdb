# Async integration

logdb is a **synchronous** library (backed by background threads — the Committer,
optionally the Sealer). It is designed to be used directly from async runtimes
(tokio, smol, async-std, …) **without** `spawn_blocking` for the common hot-path
calls.

## Why no built-in async API?

logdb's fast path (`append`, `read`, `flush`, `scan`) is **not I/O-bound in the
calling thread** — the caller writes into a lock-free ring buffer (CAS-based,
p50 < 100ns) and wakes the Committer thread; actual `pwrite`/`fdatasync`
happens on the background thread. Wrapping these in an async primitive would
add overhead with no latency benefit. For the rare long-blocking operations
(`shutdown`, very large sync scans), `tokio::task::spawn_blocking` is the
standard escape hatch.

## `Send + Sync`

`LogDb` implements `Send + Sync` and can be freely shared with `Arc<LogDb>`.
All public methods take either `&self` (shared-safe) or `self` (shutdown, which
consumes the handle). This is the same model `logdbd` uses across its gRPC RPC
handlers:

```rust
use std::sync::Arc;
use logdb::LogDb;

let db = Arc::new(LogDb::open(config)?);

// In a tokio task:
let db = Arc::clone(&db);
tokio::spawn(async move {
    let seq = db.append(b"hello")?;   // synchronous, but ~ns — direct call
    db.flush()?;                       // waits for Committer, but ~ms
    let rec = db.read(seq)?.unwrap();
    Ok::<_, Box<dyn std::error::Error + Send>>(())
});
```

## When to use `spawn_blocking`

Call `tokio::task::spawn_blocking` for operations that could occupy the calling
thread for more than a few hundred milliseconds:

- **`LogDb::shutdown(timeout)`** — waits for the Committer/Sealer threads to
  join (potentially seconds). This consumes the handle, so it fits naturally at
  process shutdown: `spawn_blocking(move || db.shutdown(timeout)).await??`.
- **Very large range scans** — `db.scan(0, u64::MAX)` over hundreds of
  millions of records could run for many seconds on the calling thread. Either
  batch the scan into smaller ranges, or run it in `spawn_blocking`.
  `ScanIter` impls `Send`, so a spawned task can emit records through a channel.
- **Recovery** — `LogDb::open` includes per-shard crash recovery and can take
  seconds on large datasets. It's often called once at startup in `main`, which
  is fine outside an async context, but if you call `open` inside a request
  handler, put it in `spawn_blocking`.

Common fast-path calls are **fine to call inline** from an async task:

| Method | Blocks for | Inline in async task? |
|--------|-----------|----------------------|
| `append` / `append_batch` | ~ns (CAS ring claim) | ✅ fine |
| `read` | ~10-100µs (file open + sparse-index + scan) | ✅ fine |
| `read_batch` | ~µs per record (single forward pass) | ✅ fine |
| `flush` | ~ms (waits for Committer fsync) | ✅ fine |
| `scan` (small range) | ~µs per record | ✅ fine |
| `scan` (very large range) | seconds+ | `spawn_blocking` |
| `shutdown(timeout)` | seconds | `spawn_blocking` |
| `drain(timeout)` | ~ms–s | inline if short timeout; `spawn_blocking` if long |

## Reference

See [`logdbd`](https://github.com/lv-agent/lv-logdb/tree/main/logdbd) for a
production example of an async gRPC service (tonic + tokio) wrapping `LogDb`
behind `Arc`. Its Tail RPC handler streams records fetched via `next_batch`
inside a `tokio::spawn`, and the `drain` on graceful shutdown is called inline
(short timeout).

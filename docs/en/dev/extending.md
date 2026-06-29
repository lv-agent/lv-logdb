# Extending logdb

How to extend logdb: add a feature flag, embed logdb in a long-running service, and integrate with the remote-push pipeline. Plus the durability guardrails every extension must respect.

> Authoritative for **logdb 0.2.0**. Verify against `Cargo.toml`, `src/lib.rs`, `src/pusher.rs`, and `src/config.rs` when the source changes.

## Adding a feature flag

logdb uses additive Cargo features (see [`Cargo.toml`](../../../Cargo.toml) `[features]`):

```toml
[features]
default = []
hash-chain  = ["sha2", "blake3"]
compression = ["zstd"]
encryption  = ["aes-gcm", "getrandom"]
remote-push = []
```

A feature must be **additive** — enabling it must never change default-build behavior. To add a new one, e.g. `metrics`, follow this walkthrough:

1. **Declare it in `Cargo.toml`.** Add the feature and any optional dependencies it pulls in:
   ```toml
   [dependencies]
   metrics = { version = "0.22", optional = true }

   [features]
   metrics = ["dep:metrics"]
   ```
2. **Gate the code.** Wrap the new code path in `#[cfg(feature = "...")]`, both the implementation and the call site. The codebase follows this pattern everywhere — e.g. `src/lib.rs:174` (`hash-chain`), `src/lib.rs:86` (`remote-push`), `src/storage/mod.rs:283` (`encryption`):
   ```rust
   #[cfg(feature = "metrics")]
   fn record_metric(&self, name: &str, value: u64) { /* ... */ }

   // at the call site:
   #[cfg(feature = "metrics")]
   self.record_metric("append", 1);
   ```
3. **Document it.** Add the feature to the dev README's feature list and the relevant usage page so users can discover it.
4. **Add feature-gated tests.** Tests behind the same `#[cfg(feature = "...")]` attribute run only when the feature is on. Confirm both the on and off paths in CI:
   ```sh
   cargo test --features metrics        # feature on
   cargo test                           # feature off (default build must still pass)
   cargo test --all-features            # whole matrix
   ```

See [Testing / Feature-gated testing](testing.md#feature-gated-testing) for the full feature-matrix commands, and [Building](building.md) for how features map onto build flags.

## Embedding in a long-running service

The public, supported way to embed logdb in a service is to hold an `Arc<LogDb>` and use the dedicated lifecycle methods.

### Graceful shutdown with `drain(timeout)`

`LogDb::drain(&self, timeout: Duration)` (defined at `src/lib.rs:574`) is the supported graceful-shutdown path for services. It runs in two phases:

1. **Drain** — reject new appends (`start_drain`), wait for all in-flight appends to publish (in-flight count drops to 0).
2. **Flush** — request a flush up to the max producer cursor and wait until durable (or until `timeout`).

It returns `ShutdownReport::Clean` if everything became durable within the timeout, or `ShutdownReport::PartialDurable` if the deadline expired first. On timeout it aborts the shutdown and returns `Err(FlushError::Timeout)`.

```rust
use std::sync::Arc;
use std::time::Duration;
use logdb::LogDb;

let db = Arc::new(LogDb::open(config)?);

// ... service runs, appends through Arc<LogDb> ...

// On SIGTERM / service stop:
match Arc::clone(&db).drain(Duration::from_secs(30)) {
    Ok(report) => log::info!("logdb drained: {:?}", report),
    Err(e)     => log::warn!("logdb drain failed: {:?}", e),
}
```

Because `drain` takes `&self`, every service task can share the same `Arc<LogDb>` without an extra `Mutex` — append, flush, read, and drain all go through the shared handle.

### Standby ingestion with `replicate`

`LogDb::replicate(&self, sequence, timestamp_ns, content)` (defined at `src/lib.rs:326`) is the supported write path for a **standby** replica. It ingests a record produced elsewhere at an explicit `(sequence, timestamp_ns)`:

- Requires `shards == 1` (replication is a linear stream onto shard 0).
- Is **idempotent** — a record whose `sequence` is already behind the producer cursor returns `Ok(())`.
- Is **in-order** — `sequence` must be exactly the next expected slot, otherwise it errors.
- Enforces the same `max_content_size` and health checks as a normal append.

This is the API to use when building a follower/standby that consumes a replicated stream, not when serving local writers.

## The remote-push pattern (internal)

logdb ships a Pusher that replicates durable records to a user-supplied remote endpoint. The Pusher maintains its **own** `push_cursor`, independent of the durable cursor, and persists it to a CRC-protected progress file (`pusher_progress.dat`). It reads only fsynced records, pushes in batches, and applies exponential backoff on failure. **Remote failures never back-pressure local appends** (principle ⑥).

The relevant types live in [`src/pusher.rs`](../../../src/pusher.rs):

- **`RemoteSink` trait** (`src/pusher.rs:25`) — user-implemented remote receiver:
  ```rust
  pub trait RemoteSink: Send + 'static {
      fn push_batch(&mut self, records: &[Record]) -> Result<(), PushError>;
  }
  ```
- **`PushError`** (`src/pusher.rs:36`) — tells the Pusher how to react:
  - `PushError::Retriable(String)` — transient failure; the Pusher retries with exponential backoff (`config.push_retry_base * 2^attempt`, capped at 60 s and at `2^6 = 64×` the base).
  - `PushError::Fatal(String)` — unrecoverable; the Pusher stops.
- **`run_pusher`** (`src/pusher.rs:141`) — the loop itself:
  ```rust
  pub fn run_pusher(
      data_dir: PathBuf,
      ring: Arc<Ring>,
      sink: Box<dyn RemoteSink>,
      config: Config,
      shutdown: Arc<ShutdownState>,
  )
  ```
- **`PusherHandle::spawn`** (`src/pusher.rs:249`) — spawns the Pusher on a dedicated named thread (`logdb-pusher`); `join()` stops it; `push_cursor()` reads the current cursor; `Drop` joins automatically.

The Pusher reads durable records, advances `push_seq` on success, and persists progress every `config.push_progress_interval` batches (plus on exit) using the atomic-write pattern (`tmp → fdatasync → rename → sync_dir`) and CRC32C over the 8-byte sequence.

### Known gap: this is internal, not public

> The `pusher` module is **private** — `src/lib.rs:37` declares `mod pusher;` (not `pub mod pusher;`). `RemoteSink`, `run_pusher`, `PusherHandle`, and `PushError` are therefore **not reachable from outside the crate**. There is **no public push API on `LogDb` today**.

This material is for two audiences only:

- **People working on logdb itself** — adding a new sink, tuning backoff, or wiring the Pusher into a build with the `remote-push` feature.
- **People building a co-resident daemon** that links logdb as a path/git dependency and can reach internal modules — *not* for downstream crates that consume the published API.

A public push API is a **known gap** and would need a separate design doc (`veps/cr-NNN-…`). Do not expose these types ad hoc; see [Contributing](contributing.md).

## Durability guardrails: do / don't

Any extension that touches cursors, on-disk structures, or the append path must respect these invariants.

### Do

- **Respect cursor/watermark invariants.** The producer cursor, durable cursor, and (if present) push cursor are monotonic and advance only on success. A durable-cursor value means every record up to that sequence is fsynced and readable.
- **Use the atomic-write pattern for metadata.** Write to a temp file → `fdatasync` → `rename` → `sync_dir` on the parent directory. The Pusher's `save_progress` (`src/pusher.rs:81`) is the canonical example. Skipping the directory sync loses the rename on a crash.
- **CRC-protect on-disk structs.** Every durable structure (segments, index entries, the pusher progress file) carries a CRC32C over its bytes and is treated as corrupt when it mismatches. New on-disk formats must do the same.
- **Honor the health self-heal contract.** Health checks (`health::check()`) gate appends and replication; treat a non-zero health code (`HEALTH_DISK_FULL`, etc.) as a hard stop, not a warning, and let the self-heal path clear it.
- **Keep the append fast path lock-free.** The publisher uses the ring buffer directly; durability work happens off the append path in the Committer/Flusher threads.

### Don't

- **Don't block the append fast path.** Appends publish to the ring and return; never do fsync, network, or heavy allocation on the calling thread of `append`.
- **Don't break durable-cursor visibility.** Never advance the durable cursor to a sequence whose records are not yet fsynced and readable — readers and the Pusher depend on "durable ⇒ readable."
- **Don't write metadata without the directory sync.** A `rename` alone is not durable across a crash; you must `sync_dir` the parent.
- **Don't back-pressure local appends on remote failures.** The Pusher isolates remote failures from the local write path on purpose; a new integration must preserve that isolation.

## See also

- [Development guide home](README.md)
- [Building](building.md) — toolchain and the full feature matrix.
- [Architecture](architecture.md) — the Committer/Flusher/Pusher split and where the durability invariants come from.
- [Storage format](storage-format.md) — CRC-protected on-disk layout referenced by the guardrails above.

> logdb 0.2.0

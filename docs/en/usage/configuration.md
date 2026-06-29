# Configuration

Every tunable in logdb: the full `Config` field reference, the validation rules enforced at construction, and concrete recipes for the four most common knobs (index stride, segment size, durability mode, retention).

## Contents

- [Building a Config](#building-a-config)
- [Field reference](#field-reference)
- [Validation rules](#validation-rules)
- [Tuning recipes](#tuning-recipes)
- [See also](#see-also)

## Building a Config

`Config` is a plain struct with public fields. Start from `Config::default()` and override what you need, then pass it to `LogDb::open` — which calls [`Config::validate()`](#validation-rules) and rejects any configuration that violates a hard constraint:

```rust
use logdb::Config;
use logdb::config::{DurabilityMode, RetentionPolicy};
use std::time::Duration;

let config = Config {
    data_dir: "/var/lib/logdb".into(),
    segment_size: 512 * 1024 * 1024,    // 512 MiB segments
    durability_mode: DurabilityMode::Sync,
    retention: RetentionPolicy::MaxAge(Duration::from_secs(60 * 60 * 24 * 7)),
    index_stride: 128,                  // denser index for point reads
    ..Config::default()
};

let db = logdb::LogDb::open(config)?; // validate() runs inside open()
```

Every field below has a documented default (from `impl Default for Config`, `src/config.rs:158-183`) and a constraint (from `Config::validate`, `src/config.rs:190-223`). The defaults are chosen to be safe and sensible for most workloads — change a field only when you have a reason.

## Field reference

All 20 fields of `Config` (`src/config.rs:94-156`). Defaults come from `impl Default` (`src/config.rs:158-183`); constraints from `validate` (`src/config.rs:190-223`).

| # | Field | Type | Default | Constraint | Notes |
|---|-------|------|---------|-----------|-------|
| 1 | `data_dir` | `PathBuf` | `./logdb_data` | — | Directory for segment files, indexes, and metadata. Must be writable. |
| 2 | `segment_size` | `u64` | `256 MiB` | `>= 1 MiB` | Max bytes per segment file before rolling. Larger → fewer, bigger files; smaller → more frequent rolls. |
| 3 | `ring_size` | `usize` | `8192` | power of two **and** `>= 16` | Slots per ring. Larger absorbs bigger write bursts before the queue policy engages. |
| 4 | `shards` | `usize` | `1` | `[1, 256]` | Independent rings. Increase for write parallelism (each shard has its own ring). |
| 5 | `max_content_size` | `usize` | `1 MiB` | `<= 64 MiB` | Hard cap on a single record's content length. Appends above this are rejected. |
| 6 | `hash_enabled` | `bool` | `false` | requires feature `hash-chain`; single-shard only (`shards == 1`) | Append a SHA-256 hash chain for tamper-evidence. |
| 7 | `compression_enabled` | `bool` | `false` | requires feature `compression` | Streaming zstd compression of segment frames. |
| 8 | `encryption_key` | `Option<[u8;32]>` | `None` | requires feature `encryption` | 256-bit key for at-rest encryption; `None` = plaintext. |
| 9 | `durability_mode` | `DurabilityMode` | `Batch` | — | `Sync` / `Batch` / `Async`. See [Choosing a durability mode](#choosing-a-durability-mode). |
| 10 | `io_backend` | `IoBackend` | `Pwrite` | — | `Pwrite` is the only implemented backend; `IoUring` is reserved. |
| 11 | `queue_full_policy` | `QueueFullPolicy` | `Block` | — | `Block` (spin + backoff) or `Drop` (return `AppendError::QueueFull`). |
| 12 | `wait_strategy` | `WaitStrategy` | spin 64 / yield 16 / park 500 µs | — | Background-thread spin/yield/park cycle. See [WaitStrategy](#waitstrategy). |
| 13 | `index_stride` | `u32` | `1024` | `>= 1`; raw segments only | Sparse-index one anchor per `index_stride` records. Smaller → faster point reads, larger `.idx`. |
| 14 | `flush_timeout` | `Duration` | `30 s` | — | How long `flush()` waits for the Committer to sync before timing out. |
| 15 | `retention` | `RetentionPolicy` | `KeepAll` | — | `KeepAll` / `MaxBytes(u64)` / `MaxAge(Duration)`. See [Retention](#retention). |
| 16 | `remote_endpoint` | `Option<String>` | `None` | — | URL for optional remote push replication. `None` disables the pusher. |
| 17 | `push_batch_size` | `usize` | `1024` | — | Records per push batch to the remote endpoint. |
| 18 | `push_progress_interval` | `u32` | `10` | — | Save pusher progress every N batches. |
| 19 | `push_max_retries` | `u32` | `0` | — | Max push retries; `0` = retry forever. |
| 20 | `push_retry_base` | `Duration` | `1 s` | capped at `60 s` | Base delay for exponential backoff between retries. |

### Enum and sub-struct reference

The non-scalar fields use these types (`src/config.rs:8-90`):

```rust
pub enum QueueFullPolicy { Block, Drop }

pub enum DurabilityMode {
    Sync,   // fdatasync after every commit batch
    Batch,  // fdatasync when the batch size or time threshold is met
    Async,  // fdatasync only on explicit flush() or shutdown
}

pub enum IoBackend { Pwrite /* IoUring reserved */ }

pub enum RetentionPolicy {
    KeepAll,
    MaxBytes(u64),
    MaxAge(Duration),
}

pub struct WaitStrategy {
    pub spin_count: u32,        // spin iterations before yielding
    pub yield_count: u32,       // yields before parking
    pub park_duration: Duration, // park time
}

pub struct CommitTrigger {       // Committer thresholds (not on Config directly)
    pub bytes: usize,
    pub records: usize,
    pub interval: Duration,
    pub durability: DurabilityMode,
}
```

## Validation rules

`Config::validate()` (`src/config.rs:190-223`) runs inside `LogDb::open` and returns an error describing the first violation. It enforces exactly these rules:

1. **`ring_size` is a power of two AND `>= 16`.** Powers of two let the ring use bitmask wrapping instead of modulo; the floor of 16 keeps the ring deep enough to amortize contention. `ring_size = 100` and `ring_size = 8` are both rejected.
2. **`shards ∈ [1, 256]`.** Zero shards is meaningless; the upper bound keeps manifest bookkeeping bounded.
3. **`segment_size >= 1 MiB`.** Smaller segments waste per-segment header/index overhead and roll too often.
4. **`max_content_size <= 64 MiB`.** Above this the inline-vs-spill layout and frame format do not apply.
5. **`index_stride >= 1`.** `0` would mean "never index", which degenerates to a full scan per point read and is rejected.

```rust
pub fn validate(&self) -> Result<(), String> {
    if !self.ring_size.is_power_of_two() || self.ring_size < 16 { /* … */ }
    if self.shards < 1 || self.shards > 256 { /* … */ }
    if self.segment_size < 1 * 1024 * 1024 { /* … */ }
    if self.max_content_size > 64 * 1024 * 1024 { /* … */ }
    if self.index_stride == 0 { /* … */ }
    Ok(())
}
```

Notable things `validate` does **not** check: feature gates (a `hash_enabled: true` Config built without the `hash-chain` feature fails at compile time, not at `validate`), the `hash-chain`-implies-single-shard rule (enforced separately during open), and any `arena_size`-style constraint (the content arena was removed — there is no `ring_size * max_content_size` product constraint anymore, by design).

## Tuning recipes

### Lower `index_stride` for latency-sensitive point reads

The sparse index in each raw segment stores one offset anchor every `index_stride` records; a point read seeks to the nearest anchor at or before the target id and then scans forward (`src/reader/mod.rs`, see [Reading: How a point read finds a record](reading.md#how-a-point-read-finds-a-record)). Smaller stride → shorter forward scan → lower read latency, at the cost of a larger `.idx` file (≈ `records / stride` 8-byte entries).

```rust
// KV / etcd-style workload: random point reads dominate, want p99 latency low.
let config = Config {
    index_stride: 128, // 8× denser than the default 1024
    ..Config::default()
};
```

Rules of thumb:

- `1024` (default) — general purpose; ~0.1% of records indexed, forward scan averages 512 records.
- `64–256` — latency-sensitive point reads (KV, cache, lookup indexes). Larger `.idx`, faster seeks.
- `4096+` — write-heavy / scan-heavy workloads where you rarely point-read; smaller index, longer point-read scans.

`index_stride` only affects **raw** segments — compressed or encrypted segments are frame-based and have no per-record sparse index, so this knob is a no-op there.

### Choose `segment_size` for roll frequency

`segment_size` caps a segment file's bytes; when a write would cross the boundary, the segment rolls. Trade-off:

- **Larger segments** (512 MiB – 1 GiB) — fewer files, less per-segment overhead, better amortization of the sparse index; good for high-throughput append workloads. Retention granularity is coarser (a 1 GiB segment is the smallest unit `MaxBytes`/`MaxAge` can drop).
- **Smaller segments** (64–128 MiB) — finer-grained retention, faster segment manifest warmup, easier to copy/archive individual segments; good when retention matters more than raw throughput.

```rust
// Lots of disk, want throughput: bigger segments.
let config = Config {
    segment_size: 1024 * 1024 * 1024, // 1 GiB
    ..Config::default()
};
```

`segment_size` must remain `>= 1 MiB` (see [Validation rules](#validation-rules)).

### Choosing a durability mode

`DurabilityMode` controls when the Committer calls `fdatasync` (`src/config.rs:17-26`):

| Mode | When it `fdatasync`s | Latency | Throughput | Crash window |
|------|---------------------|---------|-----------|--------------|
| `Sync` | After every commit batch | Highest | Lowest | None — every committed record is on disk. |
| `Batch` | When the batch byte/record/time threshold is met | Medium | High | Bounded by the batch trigger interval (default ~10 ms / 256 KiB / 1024 records). |
| `Async` | Only on explicit `flush()` or shutdown | Lowest | Highest | Up to the gap between `flush()` calls — uncommitted records may be lost. |

```rust
use logdb::config::DurabilityMode;

// Financial / metadata log: never lose a committed record.
let config = Config { durability_mode: DurabilityMode::Sync, ..Config::default() };

// High-throughput event log: tolerate losing ~10 ms on crash.
let config = Config { durability_mode: DurabilityMode::Batch, ..Config::default() };

// Bulk ingest / replay: best throughput, explicit flush at checkpoints.
let config = Config { durability_mode: DurabilityMode::Async, ..Config::default() };
```

This interacts with the [durable cursor](reading.md#visibility-and-the-durable-cursor): readers and [tailers](tailers.md) only see records the Committer has synced, so `Sync` mode also minimizes the read-after-write visibility gap.

### Retention

`RetentionPolicy` controls how old segments are dropped to bound disk usage (`src/config.rs:36-45`):

- `KeepAll` (default) — never delete. Disk grows unbounded; only suitable for bounded-write workloads or when retention is handled externally.
- `MaxBytes(u64)` — drop the oldest sealed segments while total sealed-segment bytes exceed the cap. Bounded disk footprint; retention granularity is one segment.
- `MaxAge(Duration)` — drop sealed segments older than the threshold. Time-based compliance (e.g. "keep 7 days").

```rust
use logdb::config::RetentionPolicy;
use std::time::Duration;

// Cap disk at 50 GiB.
let config = Config {
    retention: RetentionPolicy::MaxBytes(50 * 1024 * 1024 * 1024),
    ..Config::default()
};

// Keep 7 days.
let config = Config {
    retention: RetentionPolicy::MaxAge(Duration::from_secs(60 * 60 * 24 * 7)),
    ..Config::default()
};
```

Retention operates on **sealed** segments only — the currently-open segment is never deleted even if it would technically qualify. This is why smaller `segment_size` gives finer-grained retention (segments seal more often).

### WaitStrategy

`WaitStrategy` tunes the Committer's spin/yield/park cycle when it has no work (`src/config.rs:47-66`):

```rust
pub struct WaitStrategy {
    pub spin_count: u32,        // spin iterations before yielding (default 64)
    pub yield_count: u32,       // yields before parking (default 16)
    pub park_duration: Duration, // park interval (default 500 µs)
}
```

- **High-throughput, CPU-rich** — more spin, less park: `WaitStrategy { spin_count: 1024, yield_count: 64, park_duration: Duration::from_micros(100), ..WaitStrategy::default() }`. Trades CPU for lower commit latency under load.
- **Latency-tolerant, CPU-frugal** — park sooner: `WaitStrategy { spin_count: 16, yield_count: 8, park_duration: Duration::from_millis(2), ..WaitStrategy::default() }. Yields the core when idle.

The defaults (spin 64 / yield 16 / park 500 µs) are a balanced middle ground. Tune only if profiling shows the Committer either burning CPU idle or adding visible tail latency.

## See also

- [Usage guide](README.md)
- [Writing](writing.md) — how `queue_full_policy` and `max_content_size` surface on the append path.
- [Reading](reading.md) — how `index_stride` and the segment manifest shape point-read latency.
- [Durability](durability.md) — the full story behind `DurabilityMode` and `flush_timeout`.
- [Recovery](recovery.md) — what survives a crash under each durability mode.
- [Tailers](tailers.md) — the durable-cursor-driven reads that depend on `durability_mode`.

> logdb 0.2.0

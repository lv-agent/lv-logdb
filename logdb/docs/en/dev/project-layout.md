# Project layout

A map of what lives where in the logdb source tree. Use this to find the right file before changing behavior.

## Contents

- [Top-level layout](#top-level-layout)
- [The `src/` module map](#the-src-module-map)
- [See also](#see-also)

## Top-level layout

| Path         | Contents                                                                                  |
|--------------|-------------------------------------------------------------------------------------------|
| `src/`       | The library crate — every module listed below.                                            |
| `benches/`   | Benchmarks (throughput, inline-vs-spill latency, sharding).                               |
| `examples/`  | Runnable examples: basic append/read, tailers, remote push, hashing.                      |
| `fuzz/`      | `cargo-fuzz` targets for serialization, recovery, and the ring buffer.                    |
| `tests/`     | Integration tests that exercise the public API end-to-end.                                |
| `scripts/`   | Build, benchmark, and release helper scripts.                                             |

## The `src/` module map

Every module in `src/`, with its responsibility and the load-bearing types it owns.

| Module                 | Responsibility                                                                                                                       | Key types / items                                         |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| `lib` (`lib.rs`)       | The crate root. Owns the `LogDb` handle and orchestrates `open`, `append`/`append_batch`/`replicate`, `flush`, `drain`, `shutdown`, and cursor accessors. | `LogDb`, `LogDbInner`, `RecoveryReport`                  |
| `config`               | All configuration, validated at construction time.                                                                                   | `Config`, `QueueFullPolicy`, `DurabilityMode`, `CommitTrigger`, `WaitStrategy`, `RetentionPolicy` |
| `error`                | Public error types for the append, flush, read, and shutdown paths.                                                                  | `AppendError`, `FlushError`, `ReadError`, `ShutdownError`, `ShutdownReport` |
| `record`               | Logical record identity, the in-memory zero-copy read view, and the on-disk record. `RecordId` follows Kafka partition-offset semantics (shard/node ids are **not** encoded). | `RecordId`, `Record`, `ReadView`                          |
| `ring` (`ring/mod.rs`) | The lock-free ring buffer: CAS-based `claim`/`claim_batch`, cache-line-padded `producer_cursor`, and the four cursors.               | `Ring`, `CachePadded`                                     |
| `ring::slot`           | The slot — the unit of ring storage. Inline (≤ `INLINE_CAP`) vs spill, plus the CAS publish protocol and `write_hash`.                | `Slot`, `SlotInner`, `INLINE_CAP` (= 256)                 |
| `shard`                | Multi-ring support: thread-affine shard selection and bit-encoded global record ids.                                                 | `ShardMap`, `encode_record_id`, `decode_record_id`       |
| `pipeline`             | Background pipeline threads. The `committer`, `sealer`, `signal`, and `trigger` sub-modules; `committer`/`sealer` hold the thread entrypoints. | `run_committer`, `run_sealer`                            |
| `pipeline::committer`  | The always-on Committer: drains all rings round-robin, serializes to segments, fsyncs, advances committed/durable cursors.           | `run_committer`                                           |
| `pipeline::sealer`     | The optional Sealer: BLAKE3 keyed hash chain. Feature-gated by `hash-chain`.                                                         | `run_sealer`, `blake3_keyed_chain`, `sha256_chain`        |
| `pipeline::signal`     | Flush request/completion and the three-phase shutdown state machine.                                                                 | `FlushSignal`, `ShutdownState`                            |
| `pipeline::trigger`    | Commit-trigger thresholds and the spin/yield/park backoff used by pipeline threads. Re-exports config trigger types.                  | `Backoff`, `CommitTrigger`, `WaitStrategy`               |
| `storage` (`mod.rs`)   | Segment file management. The `SegmentManager` is owned exclusively by the Committer thread (`!Sync`, no locks).                       | `SegmentManager`, `SegmentError`                          |
| `storage::format`      | On-disk segment and record layout: headers, flags, framing, (de)serialization, hash-algo tags.                                       | `SegmentHeader`, record/frame serialization, flag consts  |
| `storage::index`       | The sparse index used to anchor reads inside a segment.                                                                              | `SparseIndex`, `IndexEntry`                               |
| `reader` (`mod.rs`)    | Query records by id or range. Owns the cached `SegmentManifest` and the sparse-index anchor + scan read path.                         | `Reader`, `SegmentManifest`                               |
| `reader::iter`         | The forward scan iterator over segment files.                                                                                         | `RecordIter`                                              |
| `recovery`             | Crash recovery: scans segments, validates headers, detects and truncates torn writes, rebuilds sparse indexes, returns reconstructed state. | `recover`, recovery state                                 |
| `tailer`               | Named consumers with independent, persisted read progress (`tailer_<name>.dat`).                                                      | `Tailer`                                                  |
| `pusher` (**private**) | Remote push of durable records to a user-supplied `RemoteSink`. A **daemon-level component** — the module is private (`mod pusher;`) and is **not** spawned by `LogDb::open`; it is launched by an embedding service via `PusherHandle::spawn`. | `run_pusher`, `PusherHandle`, `RemoteSink`, `PushError`  |
| `health`               | Shared health state for self-healing error conditions (e.g. ENOSPC).                                                                  | `HealthState`, `HEALTH_OK`/`HEALTH_DISK_FULL`/`HEALTH_IO_ERROR` |
| `platform`             | Platform-specific I/O and time primitives (`fdatasync`, directory sync, coarse realtime clock).                                      | `fdatasync`, `sync_dir`, `clock_realtime_coarse_ns`       |

Two facts worth calling out:

- **`pusher` is private.** It is declared `mod pusher;` (not `pub mod`) in `src/lib.rs:37`, and `run_pusher` is never called by `LogDb::open`. An embedding daemon (such as `logdbd`) spawns it through `PusherHandle::spawn`. The library itself never starts a pusher thread.
- **`sealer` is feature-gated.** The module is `#[cfg(feature = "hash-chain")]`, and `run_sealer` is only spawned on shard 0 when `shards == 1`.

## See also

- [Development guide home](README.md)
- [Architecture](architecture.md) — data flow, threads, cursors, and the read path.
- [Storage format](storage-format.md) — on-disk layout referenced by `storage::format`.

> logdb 0.2.0

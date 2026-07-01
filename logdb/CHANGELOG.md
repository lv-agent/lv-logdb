# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.3.0] — Unreleased

### Changed (BREAKING)

- **`hash-chain` with an encryption key is now a real MAC.** When both
  `hash-chain` and `encryption` are enabled and an encryption key is set, the
  chain key is **derived from the encryption key** (domain-separated BLAKE3 KDF)
  instead of seeded from wall-clock time, and is **never written to disk** (the
  segment header stores zeros). An attacker who reads the segment file but lacks
  the key can no longer recompute the chain and forge a tail. Without an
  encryption key, behavior is unchanged (clock-seeded tamper-evidence). This
  changes the stored chain hashes for `hash-chain` + `encryption` segments (no
  migration; pre-release data only).
- **Migrated to Rust edition 2024** (`logdb` and `logdbd`). **MSRV is now
  `1.85`** (was `1.74` — the previous value was unachievable under
  `--all-features` because `blake3` 1.8 uses edition 2024). The migration is
  behavior-preserving (mechanical: `unsafe fn` bodies gain explicit `unsafe {}`
  blocks, `if let … else` → `match` to keep drop order, the recovery macro's
  fragment specifier pinned to `:expr_2021`).
- **`LogDb::drop` now performs a best-effort bounded drain** (≤5 s) of
  already-published records before aborting background threads, and emits a
  `tracing` warning if it cannot reach a clean state. Previously, dropping an
  unflushed `LogDb` silently lost in-flight records. Drop is a safety net, not a
  guarantee — call `shutdown()` / `drain()` for assured durability. Skipped
  during panic unwinding.
- **License is now `Apache-2.0`** (was `MIT OR Apache-2.0`). The `LICENSE` file
  already contained only the Apache-2.0 text; the crate `license` field and the
  README statements are updated to match. Downstream license-check tooling that
  keyed on the dual SPDX expression should be updated.
- `Tailer::next_batch` now returns `Result<Option<Vec<Record>>, TailerError>`
  instead of `Result<_, String>`. `TailerError` wraps the underlying `ReadError`
  (`#[from]`), so callers can forward it via `?`. Migration: callers that
  stringified the error (`.unwrap()` / `format!(..)`) are unaffected —
  `TailerError` implements `Display`.
- **Narrowed public API surface.** Implementation modules (`config`, `error`,
  `ring`, `storage`, `pipeline`, `health`, `platform`, `reader`, `record`,
  `shard`, `recovery`, `tailer`) are now `pub(crate)` — they are no longer part
  of the supported public API / semver surface. The stable types are re-exported
  at the crate root: use `logdb::Config`, `logdb::Record`, `logdb::ScanIter`,
  `logdb::Tailer`, `logdb::AppendError`, … instead of `logdb::config::Config`,
  `logdb::ring::Ring`, etc. Migration: change `logdb::<module>::<Type>` imports
  to the crate-root re-export. (Internal types like `Ring`, `SegmentManager`,
  `ShardMap` are no longer reachable from downstream crates.)
- `LogDb::open` now returns `Result<Self, OpenError>` instead of
  `Result<Self, String>`. `OpenError` is a structured enum
  (`InvalidConfig` / `Recovery` / `SegmentCreate` / `ThreadSpawn`) so callers
  can match on the failure category and forward it via `?`. Migration: replace
  any `String`-matching on `open`'s error with `OpenError` (`.unwrap()` /
  `.map_err(|e| format!(..))` callers are unaffected — `OpenError` implements
  `Display`).
- `Config::validate` now returns `Result<(), ConfigError>` instead of
  `Result<(), String>`, with one variant per constraint
  (`InvalidRingSize` / `InvalidShardCount` / `SegmentTooSmall` /
  `ContentTooLarge` / `ZeroIndexStride` / `HashChainRequiresSingleShard`).
- `append_batch(&[])` now returns `AppendError::EmptyBatch` instead of the
  misleading `AppendError::ContentTooLarge { size: 0, max: 0 }`.

### Fixed

- **Stale-manifest read miss after segment deletion.** `SegmentManifest` caches
  the segment directory listing and refreshes it only when the directory mtime
  changes. On filesystems with coarse mtime granularity (or under mtime
  propagation lag), a segment deleted by checkpoint truncation / retention
  could remain in the cache, so `read()` returned an entry pointing at a
  now-missing file — a transient read miss. This was the root cause of the
  intermittently-failing `checkpoint_truncation` test (which retried around it).
  `find()` now detects a cache-served entry whose file is gone, force-rescans,
  and re-looks-up. The fast path adds no overhead (freshly-scanned entries are
  trusted).

### Added

- `LogDb::read_batch(&[u64]) -> Result<Vec<Option<Record>>, ReadError>` — a
  multi-get. For clustered ids it is dramatically faster than N individual
  `read()`s: a single forward pass reads each record once (with OS read-ahead)
  instead of re-seeking and re-scanning to each id. Measured ~190× on a 20k-record
  batch (~3 µs/record vs ~600 µs). Result order matches `ids`; missing /
  not-yet-durable ids yield `None`.
- `OpenError`, `ConfigError`, and `AppendError::EmptyBatch` error types.
- `tracing` feature: an off-by-default feature that emits structured events
  (segment rolls, retention, recovery warnings, flush/drain timeouts, the
  best-effort drain on drop) via the `tracing` crate. Off by default, logdb
  stays zero-extra-dependency.
- `LogDb::health_code() -> Option<u8>` — exposes the internal health state
  for readiness probes. `None` when healthy; `Some(code)` when degraded
  (e.g. disk-full). Self-heals once the fs recovers.
- `metrics` feature: an off-by-default feature that emits quantitative metrics
  via the `metrics` facade crate — install a recorder (e.g. a Prometheus
  exporter) in the host to collect them. Emits `logdb.appends` (counter),
  `logdb.segment.rolls` (counter), `logdb.flush.duration` &
  `logdb.recovery.duration` (histograms), and `LogDb::record_gauges()` samples
  `logdb.durable_lag` / `logdb.queue_depth` / `logdb.wal_bytes` (gauges).
- **CI coverage job** (`cargo llvm-cov`, non-blocking) and new "Coverage"
  section in the Dev Guide testing docs. Qualification scripts and long-soak /
  long-fuzz documentation already exist in `scripts/run-all.sh`.
- **Async integration guide** ([`docs/en/usage/async.md`](docs/en/usage/async.md)):
  documents that `LogDb` is `Send + Sync`, common hot-path calls (`append`,
  `read`, `flush`) are microseconds and safe to call inline from an async task,
  and `spawn_blocking` is only needed for `shutdown` / very large range scans.
  `logdbd` is the reference async example.
- `testing` feature: an off-by-default feature that re-exposes the internal
  modules as `#[doc(hidden)] pub`, for the deployed test binary
  (`examples/testsuite`) and the `tests/fuzz` integration target. Not a
  supported public API.
- Crate-root re-exports: `RecordId`, `ScanIter`, `Tailer`, and the shard-id
  helpers (`encode_record_id` / `decode_record_id` / `shard_bits`).
- **Security deliverables:** root [`SECURITY.md`](https://github.com/lv-agent/lv-logdb/blob/main/SECURITY.md)
  (private vulnerability disclosure + SLA), a documented
  [threat model](docs/en/security/threat-model.md) (what `encryption` /
  `hash-chain` protect against — and what they do **not**, including the
  caveat that the hash-chain key is derived from non-secret entropy and
  persisted in cleartext, so the chain is tamper-evidence not authenticity),
  and [key-management](docs/en/security/key-management.md) guidance (CSPRNG
  generation, envelope encryption, rotation).
- **License compliance:** `cargo-deny` config (`deny.toml`) enforcing a
  permissive-only allow-list (no copyleft anywhere in the graph, enforced in
  CI) and a [`THIRDPARTY.md`](THIRDPARTY.md) inventory of the 29 runtime
  dependencies.
- Cargo manifest metadata for crates.io publishing: `repository`, `homepage`,
  `documentation`, `readme`, `keywords`, `categories`, and `rust-version`
  (MSRV = 1.74, dictated by `thiserror` 2).
- `LICENSE` is now packaged inside the crate directory (it previously lived
  only at the workspace root, so `cargo package` omitted it).

### Known limitations (unchanged)

- `recovery::recover_shard` still returns `Result<_, String>` (18 internal
  error sites). It is captured by `OpenError::Recovery { shard, reason }`.
  Full structuring is deferred to a follow-up that also narrows the `recovery`
  module's visibility.

## [0.2.0] — 2026-06-30

First tagged release. logdb is an embedded, append-only, crash-recoverable,
optionally tamper-proof / compressed / encrypted / remotely-pushable local
log database.

### Added

- **Sharding (`Config.shards`, range `[1, 256]`)** — multiple independent
  rings for high-core-count write scalability. Fully production-ready as of
  0.2.0: append, point read, range scan / `replay_from`, named tailers,
  crash recovery, checkpoint truncation, and retention all work across
  shards (including non-power-of-two shard counts and segment rolls). Record
  ids are bit-packed: `global = (local_seq << shard_bits) | shard_id`,
  `shard_bits = ceil(log2(num_shards))`.
- **Features**: `hash-chain` (BLAKE3 keyed tamper-evident chain; requires
  `shards == 1`), `compression` (streaming zstd, per-frame), `encryption`
  (AES-256-GCM, per-frame), `remote-push` (async durable push; the `Pusher`
  itself is a private daemon-level building block — no public push API yet).
- **Tailers** — named consumers with independent, persisted, per-shard
  progress (`new_tailer`, `next_batch`, `commit`, `seek`, `reset`,
  `position`, `positions`). `next_batch` merges every shard's newly-durable
  records into one ascending-global-id batch (lossless; cross-batch ordering
  is best-effort, like Kafka per-partition offsets).
- **WAL checkpoint** — persistent, survives crash; old segments fully covered
  by the checkpoint are truncated on the next roll, per shard.
- **Crash recovery** — automatic on `open`: per-shard torn-write detection
  and truncation, optional hash-chain verification, sparse-index rebuild.
- **Diagnostics** — `wal_usage()`, `ring_size()`, `recovery_report()`,
  `producer_cursor()` / `committed_cursor()` / `durable_cursor()`.
- **Scan throughput optimization** — raw-mode range scan reads in 64 KB
  windows instead of one syscall per record (~12× faster on small records:
  ~1100 ns/rec → ~90 ns/rec).
- Bilingual documentation (English + Chinese): 23-page Usage Guide and
  7-page Dev Guide, mirrored.

### On-disk layout

- `shards == 1`: segment files (`segment-NNNNNNNN.log`) live flat in
  `data_dir/`.
- `shards > 1`: each shard is an independent log under `data_dir/s<shard>/`.
- Sparse index sidecars (`segment-NNNNNNNN.idx`) for raw segments; frame
  layout (length-prefixed, optionally compressed+encrypted) for
  compressed/encrypted segments. Checkpoint and tailer progress are
  CRC-protected atomic-write files.

### Fixed (during release prep)

- Rolled segment headers under encryption were missing
  `FLAG_ENCRYPTED_AES256GCM`, so every segment after the first was read as
  raw — scan/replay/tailer ground through encrypted bytes byte-by-byte
  (pathologically slow) and dropped all post-roll records. Now set on both
  roll paths.
- `wal_usage()` and `recovery_report()` were broken under `shards > 1`
  (missed `s<shard>/` subdirs; mixed per-shard local cursors with global ids).
- Pre-allocated segment headers stored the local ring seq as `base_sequence`
  instead of the global id, breaking point reads / truncation / recovery
  after a roll under `shards > 1`.

### Known limitations

- `hash-chain` requires `shards == 1` (a global chain needs single-shard
  order); multi-shard hashing is deferred.
- The `Pusher` / remote push is single-shard and not wired to a public API;
  `shards > 1` + remote push is not supported yet (a public push API will
  make it cross-shard).
- `replicate` (standby write-in) requires `shards == 1`.

[0.3.0]: https://github.com/lv-agent/lv-logdb/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/lv-agent/lv-logdb/releases/tag/v0.2.0

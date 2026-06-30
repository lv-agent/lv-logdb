# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.3.0] — Unreleased

### Changed (BREAKING)

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

### Added

- `OpenError`, `ConfigError`, and `AppendError::EmptyBatch` error types.
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

# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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

[0.2.0]: https://keepachangelog.com/en/1.1.0/

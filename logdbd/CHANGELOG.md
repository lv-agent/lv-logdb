# Changelog — logdbd

## [Unreleased]

### Added

- **Shard-aware Tail** (cr-037): `TailRequest.shard_ids` filters records to one or
  more shards (empty ⇒ all, legacy behaviour).
- **Key-based append routing** (cr-037): `AppendRequest.shard_key` (optional).
- **`DecodedRecord.shard_id`**: decoded at read/scan from the gid.
- **`scan_stream_filtered`**: stream- + shard-scoped durable read via `seq_map`
  index + `LogDb::read_batch` (cr-037 perf). Replaces full-table scan in the
  Tail for much lower per-poll CPU.
- **Long-poll Tail** (cr-037 A): Tail blocks on `Notify` woken by the subscribe
  publisher (≤10 ms wake-up instead of a fixed 100 ms sleep).
- **`StatusResponse.num_shards`** (cr-037 F): so the broker can auto-discover
  the shard count at startup.
- **Seq-map checkpoint** (cr-037 B): `Storage::write_seq_map_checkpoint` /
  `try_new_from_checkpoint` to skip a full-scan rebuild on startup.
- **logdb-broker** — symmetric-gateway consumer-group coordinator (sibling crate).

### Fixed

- **Multi-shard Tail bounded by `durable_gid` returned incomplete** (cr-037
  Phase 1): Tail's `scan(0, durable_gid)` used min durable local seq as global
  gid cap. Removed — each shard's manifest self-bounds by durable.

## [0.7.0] — 2026-07-08

### Added

- **Backup / restore for disaster recovery** (cr-029): `logdbd::backup` writes a
  self-describing `.logdbbak` tar + sha256 sidecar of a stopped node's data dir;
  `restore` verifies the checksum, refuses to overwrite a non-empty target, and
  optionally re-opens the log (`--verify`) to re-run CRC + hash-chain + torn-write
  recovery. CLI: `logdbd-admin backup --data-dir <dir> --out <file.logdbbak>`,
  `restore --backup <file> --data-dir <dir> [--verify] [--config <server.yaml>]`.
- **Encryption key management + rotation** (cr-032): the server now actually wires
  `storage.encryption` to the core. Multi-key rotation via `keys` +
  `active_key_id`; `${ENV}` interpolation for `key_hex`; rotation works with the
  hash chain enabled. `EncryptionConfig::resolve_key_ring()` builds the core
  `KeyRing`.
- **`KeyProvider` port + built-in `FileKeyProvider`** (cr-032 Phase 2): a
  `crypto::KeyProvider` trait resolves keys at startup (never on the record path),
  so the core library carries no vendor dependency. `encryption.provider`
  (`file` | `awskms` | `vault`; default `file`) selects the source; AWS KMS /
  Vault are out-of-tree opt-in crates.
- **Container / Kubernetes deployment** (cr-030): multi-stage Dockerfile
  (non-root, fixed UID/GID 65532), `docker-compose` dev setup, and a Helm chart
  (Deployment/Service/ConfigMap/PVC/optional auth Secret, exec health probe).
- **`restore --verify --config <yaml>`** (cr-032): encryption-aware restore verify
  loads the server config to resolve the key ring, so encrypted backups verify
  correctly instead of silently dropping ciphertext frames.

### Fixed

- **`storage.encryption.enabled: true` was a silent no-op** (cr-032 Phase 0): the
  server parsed the encryption config but never passed it to the core, writing
  data in plaintext despite encryption being "on". Now resolved.
- **Flaky tests stabilized** (cr-031): subscribe stress tests gated behind
  `#[ignore]`; `logdb-client` / `logdb-exporter` integration tests fixed for the
  post-SQLite API; `LogDb::refresh_manifests()` makes segment-roll timing tests
  deterministic on coarse-mtime filesystems (WSL2).

> Note: the `[0.4.0]` section below (and the intervening published 0.5.x / 0.6.x
> releases) predates this CHANGELOG being kept current; reconstruction of those
> entries is tracked separately. This `[Unreleased]` captures the
> `feat/commercial-readiness` branch (cr-029 … cr-032).

## [0.4.0] — unreleased

### Added

- **Namespace + stream data model**: two-level logical isolation. `Append` requires
  `namespace` and `stream` fields. Catalog maps names to internal IDs.
- **Per-stream hash chain**: each stream has its own independent BLAKE3 hash chain
  with stream-specific genesis hash. Logdb per-shard sealer provides storage-level
  integrity.
- **Catalog persistence**: snapshot file survives restarts. Auto-saves on new
  namespace/stream creation. Startup rebuilds name→ID mappings.
- **BatchAppend RPC**: atomic batch writes within a single namespace+stream.
  Returns all seq/gid values on success.
- **GetWatermark RPC**: exposes `oldest_seq`, `durable_seq`, `replicated_seq` per
  namespace/stream.
- **ListNamespaces / ListStreams RPCs**: catalog inspection.
- **YAML configuration**: `logdbd.yaml` with env-var substitution (`${VAR}`).
  Full config validation at startup.
- **Process lock**: `active.lock` prevents accidental dual-primary start.
- **Node identity**: `cluster_id` + `epoch` prevent cross-cluster and stale-primary
  replication corruption.
- **Replication auth token**: standby auth token passed in gRPC metadata.
- **Replication sync/async policies**: `sync_policy` (all/quorum/n),
  `sync_timeout_ms`, `on_sync_timeout` (fail/async_warn/block) all respected.
- **Replication reconnect**: cached gRPC clients with automatic reconnection
  and exponential backoff.
- **Snapshot RPC**: `PullSnapshot` streams sealed segment files to new standbys.
- **Storage recovery**: seq→gid mapping rebuilt from logdb on startup. Point
  reads work immediately after restart.
- **logdbd-admin CLI**: management tool for status, list, streams, append, ping.
- **Prometheus alert rules**: 9 production alert rules for durable lag,
  replication lag, conflicts, disk usage, exporter health.
- **Failover runbook**: complete manual failover procedure with diagnostics.
- **RWLock poisoning recovery**: all lock sites recover from poisoned locks
  rather than panicking.
- **Exporter OUT_OF_RETENTION detection**: refuses to start if progress falls
  behind retention, preventing silent data loss.
- **ClickHouse sink**: HTTP insert with `insert_deduplication_token` for
  block-level dedup. ReplacingMergeTree for long-tail dedup.
- **TLS/mTLS support**: logdbd server and exporter client support TLS and mTLS.

### Changed (BREAKING)

- **Proto**: `AppendRequest` now requires `namespace`, `stream`, `event_type` fields.
  Old `content`-only format no longer accepted.
- **Proto**: `Record` has new fields: `namespace_id`, `stream_id`, `seq`,
  `event_type`, `timestamp_ns`, `content_type`, `metadata`.
- **Proto**: `Read`/`Scan`/`Tail` require `namespace` and `stream` parameters.
- **Proto**: Replication uses `ReplicationRecord {gid, timestamp_ns, content}` for
  raw-byte transfer (independent of Record schema).
- **Config**: YAML file required. Old `LOGDBD_*` env vars deprecated (still
  work as overrides).
- **Data directory**: new `catalog.dat` snapshot file. Old data directories
  without this file will auto-initialize (existing segment files are preserved
  but point-reads by seq need mapping rebuild on first startup).
- **Auth**: `token_file` read failure now prevents startup instead of
  silently disabling auth.

### Fixed

- P0: seq→gid mapping not persisted → point reads return `None` after restart.
  Now rebuilt on startup.
- P0: catalog never saved → name→ID mappings lost. Now auto-saves on creation.
- P0: `replicate()` silently dropped decode errors → standby records invisible.
  Now returns error.
- P1: scan() errors swallowed via `unwrap_or_default()`. Now propagated as
  gRPC errors.
- P1: `token_file` read failure silently disabled auth. Now fails startup.
- P1: replication created new gRPC client every 50ms. Now cached with reconnect.
- P1: catalog locks held during file I/O. Now serialized before I/O.
- P1: primary restart reset per-stream seq to 1. Now restored from logdb scan.

## [0.2.0] — 2026-06-30

- Initial public release with basic Append/Read/Scan/Tail/Status RPCs.
- Primary-standby replication (basic).
- TLS + token auth.
- Environment-variable configuration.

# Changelog — logdbd

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

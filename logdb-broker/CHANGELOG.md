# Changelog — logdb-broker

## [0.1.0] — 2026-07-09

### Added

- **Symmetric gateway**: `Produce` and `Consume` both go through the broker
  (Pulsar model); logdbd is the storage backend. Clients talk only to the broker.
- **Round-robin shard assignment** (stop-the-world protocol, Phase 5):
  `shard i → member i % n`. Independent groups via `CoordinatorRegistry`.
  Membership is transient (consumers rejoin on restart); per-shard offsets
  are durable.
- **Data forwarding** (Phase 3): one Tail per assigned shard; each record
  stamped with its `shard_id` so consumers can commit per-shard offsets.
  Per-shard Tail avoids a single-Tail's per-stream `from_seq` vs per-shard
  offset mismatch.
- **Offset-aware Consume** (Phase 6): resumes each shard at its committed
  offset + 1. Offsets are event-sourced into logdbd's
  `logdb_broker/coord_state` meta stream and replayed on startup, so the
  broker is effectively stateless.
- **Durable per-shard offsets** (`CommitShardOffset` RPC, event-sourced).
  Restart-recovery replay: stale commits are idempotent (max seq wins).
  Broker restart preserves progress — no re-processing of already-committed
  records.
- **Rebalance signal frames** on the `Consume` stream: `RebalanceSignal`
  (pause) + `Assignment` (resume with new shards). The forward task is
  swapped transparently; the consumer sees a brief gap.
- **Consumer / producer SDK** (`logdb-client` with `broker` feature):
  `GroupConsumer` (join / consume / commit_shard / leave) +
  `BrokerProducer` (produce). Auto-rejoin on stale generation.
- **Prometheus metrics**: `broker.joins`, `leaves`, `consume_sessions`,
  `records_forwarded`, `offsets_committed`, `rebalances`. Optional
  `/metrics` endpoint (`config.metrics_addr`).
- **Graceful shutdown** on SIGTERM / Ctrl-C (`serve_with_shutdown`).
- **Docker image** (multi-stage, non-root UID 65532, no local volume — the
  broker is stateless, state lives in logdbd).
- **Structured tracing** (`tracing` + `tracing-subscriber`, `RUST_LOG`).

### Added (post-0.1.0 optimizations, merged to main)

- **Per-group leader election** (E): multiple broker instances elect a leader per
  `(ns, stream, group)` via the logdbd meta stream (`leader_claim` events,
  CAS-append). Stateful RPCs return `UNAVAILABLE: leader is at <addr>` on standbys;
  `Produce` is stateless and works on any broker.
- **Cooperative sticky rebalance** (C): recompute keeps existing shard
  assignments; only "foreign" shards (`s % n != index`) are given up. Forward
  tasks are only restarted for consumers whose shard set changed.
- **Heartbeat / session eviction** (D): `Heartbeat` RPC; stale consumers
  (`session_timeout_ms`) are evicted by a periodic liveness check, triggering
  rebalance so dead consumers don't hold shards.
- **`num_shards` auto-discovery** (F): broker queries logdbd's `Status` RPC at
  startup; falls back to config if the server is older.
- **Seq-map checkpoint** (B): `Storage` persists the `seq_map` to a binary
  checkpoint for fast startup; incremental per-shard replay via
  `LogDb::scan_shard`.
- **Long-poll Tail** (A): `Notify` wakes Tail handlers when the subscribe
  publisher advances `durable_gid`, reducing idle latency from 100 ms to ≤10 ms.
- **`BatchProduce` RPC** (1): batch append forwarded to `logdbd.BatchAppend`
  in one gRPC call.
- **Tail `batch_size` 100→500** (4): fewer poll cycles per shard.
- **Offset snapshot compaction** (2): periodic `offset_snapshot` events
  compact the meta stream so recovery doesn't replay every individual
  `offset_committed` event from history.

### Not yet done

- Multi-broker global failover / partition-level HA (per-group election is the
  foundation; global coordination pairs with cr-026 ownership ring).
- Cooperative sticky assignment upgrade (current is stop-the-world + sticky shed;
  full cooperative with rebalance delay / group protocol is future).
- `consume_throughput` criterion bench needs a native-Linux run for reliable numbers.

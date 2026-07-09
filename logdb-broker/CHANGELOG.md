# Changelog — logdb-broker

## [0.1.0] — unreleased (cr-037, branch `feat/cr-037-broker`)

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

### Not yet done (planned follow-up)

- Multi-broker HA + leader election (pairs with cr-026 ownership ring).
- Cooperative / sticky assignment (rebalance is stop-the-world).
- Heartbeat / session-timeout consumer eviction.
- Automatic `num_shards` discovery from logdbd (currently operator-configured).

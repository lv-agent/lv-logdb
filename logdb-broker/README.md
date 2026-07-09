# logdb-broker

Kafka-style consumer-group coordinator for [logdbd](../logdbd) (cr-037). It is a
**symmetric gateway** (the Pulsar model): producers `Produce` and consumers
`Consume` both talk **only** to the broker; the broker forwards appends to
logdbd and Tails logdbd per assigned shard. logdbd is the storage backend.

```
Producer ──Produce──→ Broker ──Append──→ logdbd
Consumer ──Consume──→ Broker ──Tail────→ logdbd
```

## What it does

- **Shard assignment** — round-robin assigns logdbd shards to a group's
  consumers (`shard i → member i % n`).
- **Data forwarding** — one Tail per assigned shard; records carry a stamped
  `shard_id` so consumers can commit per-shard offsets.
- **Rebalance (stop-the-world)** — on join/leave, open Consume streams receive a
  `RebalanceSignal` → `Assignment` and the broker swaps each forward task to the
  new shards.
- **Durable offsets** — committed offsets are event-sourced into logdbd's
  `logdb_broker/coord_state` meta stream and replayed on startup, so the broker
  is effectively stateless (membership is transient — consumers rejoin).
- **Offset-aware resume** — `Consume` resumes each shard at its committed
  offset + 1.

## Run

```bash
# logdbd must be reachable (shards must match num_shards).
LOGDB_BROKER_CONFIG=./logdb-broker/config.yaml cargo run -p logdb-broker
```

Config knobs: `bind_addr`, `logdbd_addr`, `num_shards`, `metrics_addr`
(optional Prometheus `/metrics`). See [`config.yaml`](./config.yaml).

## SDK

Use [`logdb-client`](../logdb-client) with the `broker` feature:

```rust
use logdb_client::broker::{BrokerProducer, GroupConsumer};

// produce
let mut p = BrokerProducer::connect("http://broker:9091").await?;
p.produce("ns", "s", "evt", b"payload", Some("session-42")).await?;

// consume
let mut c = GroupConsumer::join("http://broker:9091", "ns", "s", "g", "c1").await?;
while let Some(rec) = c.consume().await?.next().await {
    let rec = rec?;
    // ...process rec (rec.shard_id, rec.seq, rec.content)...
    c.commit_shard(rec.shard_id, rec.seq).await?;
}
```

## Container

```bash
docker build -t logdb-broker -f logdb-broker/Dockerfile .
docker run -p 9091:9091 -p 9100:9100 logdb-broker
```

No volume needed — the broker is stateless (state lives in logdbd).

## Status (cr-037)

Single-broker; multi-broker HA + leader election is future work (pairs with
cr-026). Rebalance is stop-the-world (a future cooperative/sticky assignment can
replace it). Design: `veps/cr-037-logdb-broker-design.md`.

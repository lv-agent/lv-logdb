# Sharding

Sharding splits logdb's write path across multiple independent rings so that appenders on different threads do not contend on a single queue. Set `Config.shards` to the number of rings; each shard has its own ring buffer, cursors, and producer threads. This page covers the sequence-number mapping, the v1.1 hash-chain incompatibility, and when to shard versus staying single-shard.

> **Production readiness:** as of 0.2.0, `shards > 1` is end-to-end functional and durable — append, point read, range scan/replay, crash recovery, and tailers all work correctly across shards (including non-power-of-two shard counts and segment rolls). The one remaining limitation is feature-scoped, not correctness-scoped: `hash-chain` requires `shards == 1` (see below). Remote push (`replicate`/Pusher) is single-shard today.

## Contents

- [Multiple rings](#multiple-rings)
- [Sequence mapping](#sequence-mapping)
- [hash-chain incompatibility](#hash-chain-incompatibility)
- [When to shard](#when-to-shard)
- [See also](#see-also)

## Multiple rings

`Config.shards` (`src/config.rs`, default `1`, range `[1, 256]`) sets the number of independent rings managed by the `ShardMap` (`src/shard.rs:65-163`). Each ring has:

- its own `ring_size` slots (sized as `ring_size / shards`, rounded up to a power of two with a floor of 16),
- its own producer and consumer cursors,
- its own backpressure watermark.

The Committer polls **all** rings (`ShardMap::all_rings`, `src/shard.rs:134-136`), so durability and the durable cursor are global across shards. A thread is mapped to a shard by hashing its thread ID (`ShardMap::select_shard`, `src/shard.rs:142-151`) for affinity, which keeps an appender on the same shard across calls and reduces cross-thread contention further.

```rust
use logdb::Config;

let config = Config {
    shards: 4, // four independent rings
    ..Config::default()
};
let db = logdb::LogDb::open(config)?;
```

## Sequence mapping

A record's position is a single `u64` **global sequence** returned by `LogDb::append`. The conceptual mapping, documented on `RecordId.sequence` (`src/record.rs:25-28`), interleaves sequences from different shards:

> `global_seq = local_seq * num_shards + shard_id`

The literal on-disk encoding is bit-packed for speed (`encode_record_id`, `src/shard.rs:44-52`): `global_id = (local_seq << shard_bits) | shard_id`, where `shard_bits = ceil(log2(num_shards))`. When `num_shards` is a power of two, the bit-packed form is identical to the multiplication form above (shifting left by `shard_bits` equals multiplying by `2^shard_bits = num_shards`); when it is not, the bit-packed form is what actually goes through `append` and `read`. Decode is the inverse (`decode_record_id`, `src/shard.rs:54-63`).

### Example: 4 shards

With `shards = 4`, `shard_bits = 2`, so the low 2 bits of the global sequence carry the shard id and the high bits carry the per-shard local sequence. Three records appended concurrently across shards 0, 2, and 1 (each shard's first write) get:

| Shard | `local_seq` | `global_seq` (`local_seq << 2 | shard_id`) |
|-------|------------|-------------------------------------------|
| 0 | 0 | `0b0000` = `0` |
| 2 | 0 | `0b0010` = `2` |
| 1 | 0 | `0b0001` = `1` |

The next write on shard 2 (its second record, `local_seq = 1`) maps to `(1 << 2) | 2 = 6`. Consumers reading the global sequence see an interleaved but **stable** order: within a shard the order is append-order, and the encoding never aliases two records onto the same `global_seq`.

Because the low `shard_bits` are the shard id, a point read can recover `(shard_id, local_seq)` from a bare `global_seq` with a bitmask and a shift — no separate shard lookup index is required.

## hash-chain incompatibility

The [`hash-chain`](features.md#hash-chain) feature is **incompatible with sharding** in v1.1. A hash chain across shards requires a global merge ordering — the Sealer must hash records in a single, deterministic order — but v1.1 only seals one shard at a time. With `hash-chain` enabled and `shards > 1`, `LogDb::open` returns this exact error (`src/lib.rs:176-181`):

> hash-chain is not supported with shards > 1 in v1.1. Use shards=1 with hash-chain, or shards>1 without hash.

This is a **current-version limitation**, not a permanent design choice. Multi-shard hash chaining (a global merge-ordered Sealer) is deferred to v1.2. Until then, pick one:

- `shards = 1` with `hash-chain` for tamper-evidence, or
- `shards > 1` without `hash-chain` for write throughput.

## When to shard

Sharding is a throughput/latency trade-off. Use it when appender contention is the bottleneck, not by default.

**Shard (`shards > 1`) when:**

- **High core count, many appenders.** If you have dozens of producer threads and a single ring's claim/publish is showing contention (visible as elevated p99 append latency under load), sharding spreads writes across rings so each thread's appender hits its own queue.
- **Write throughput dominates.** Sharding raises the append ceiling roughly linearly with `shards` up to the Committer's serialize/fsync throughput, because the per-ring locks no longer serialize producers.

**Stay single-shard (`shards = 1`) when:**

- **Point-read latency dominates.** A point read is a sparse-index seek plus forward scan within a segment (see [Reading](reading.md)). With one shard, sequences are dense (`0, 1, 2, …`) and the segment manifest maps cleanly to files. With many shards, the low bits are shard ids, so the manifest and scan paths do a little more work per lookup.
- **You need `hash-chain`.** Tamper-evidence requires single-shard in v1.1 (see above).
- **A single writer or low producer count.** With one or a few appenders there is nothing to spread out — extra shards just add Committer polling overhead and complicate operator tooling.

### Impact on reads and scanning

- **Point reads** (`LogDb::read`) decode the shard id from the sequence and read the right segment; the cost is the decode plus the normal sparse-index scan. The per-lookup overhead is small, but `index_stride` only helps raw, single-shard-style segments — see [Configuration](configuration.md#lower-index_stride-for-latency-sensitive-point-reads).
- **Range scans** read across shards. Because sequences are interleaved, a global range scan merges per-shard streams; the durable cursor is global, so a scan sees a consistent cut of what the Committer has fsynced across all shards.
- **Tailers** ([tailers.md](tailers.md)) follow the global durable cursor and are unaffected by shard count at the API level.

## See also

- [Usage guide](README.md)
- [Features](features.md) — `hash-chain` details and the single-shard constraint.
- [Writing](writing.md) — how `claim` produces the `(global_seq, shard_id, local_seq)` triple.
- [Reading](reading.md) — point-read and scan paths that sharding affects.
- [Configuration](configuration.md) — the `shards` field (`Config.shards`, range `[1, 256]`).

> logdb 0.2.0

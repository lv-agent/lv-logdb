# Concepts

The core model behind logdb: records and their identifiers, the sequence space, segments, the lock-free ring buffer, and — most importantly — the three cursors that govern visibility and durability.

## Contents

- [Records and RecordId](#records-and-recordid)
- [The Record struct](#the-record-struct)
- [Sequence space and monotonicity](#sequence-space-and-monotonicity)
- [Segments](#segments)
- [Ring buffer and slots](#ring-buffer-and-slots)
- [Cursor semantics](#cursor-semantics)
- [Inline vs spill](#inline-vs-spill)

## Records and RecordId

Every record in logdb has a logical position called a **`RecordId`**. It follows Kafka-style partition-offset semantics — a `(partition_id, sequence)` tuple, **not** a single packed `u64` that encodes physical topology:

```rust
pub struct RecordId {
    pub partition_id: u32, // logical partition; 0 for single-partition v1.0
    pub sequence: u64,     // partition-local monotonically increasing
}
```

For the common single-partition case (`partition_id == 0`), `RecordId` implements `Into<u64>` (returning `sequence` directly) and `From<u64>`, so an `id: u64` and an `id: RecordId` are interchangeable:

```rust
let id = RecordId::new(0, 42);
let seq: u64 = id.into();          // 42
let back: RecordId = 99u64.into(); // partition 0, sequence 99
```

Its `Display` impl shows just the sequence when `partition_id == 0`, otherwise `partition/sequence`:

```rust
assert_eq!(format!("{}", RecordId::new(0, 42)),  "42");
assert_eq!(format!("{}", RecordId::new(3, 42)),  "3/42");
```

The shard id **is** bit-packed into the global sequence (and thus into `RecordId.sequence`) when `shards > 1` — `read`/`scan`/`tailer` decode it to route to the right shard. The exact bit layout is an internal detail (see [Sharding](sharding.md)); applications treat the `u64` returned by `append` as an opaque, comparable handle and pass it back to `read`.

## The Record struct

A fully owned record read back from a segment is a `Record`:

```rust
pub struct Record {
    pub id: RecordId,     // logical identifier
    pub timestamp_ns: u64, // nanoseconds, CLOCK_REALTIME_COARSE
    pub content: Vec<u8>,  // owned record content
    pub hash_n: [u8; 32],  // SHA-256 chain value (all zeros if hashing is off)
}
```

- `timestamp_ns` is assigned by logdb at append time from `CLOCK_REALTIME_COARSE` — do not rely on the application to set it.
- `hash_n` is the forward-linking hash chain value. When the `hash-chain` feature is **off**, it is `[0u8; 32]`. When on, each record's hash chains from the previous record's hash plus this record's content, giving tamper-evidence.

A borrowed, zero-copy view (`ReadView<'a>`) exists for internal hot paths; application-facing reads return owned `Record` values.

## Sequence space and monotonicity

Sequences are **strictly monotonic** within a partition. Each successful `append` returns a sequence one greater than the previous one (no gaps from the writer's perspective; gaps only appear if the writer sees an `AppendError`).

With **sharded rings** (`config.shards > 1`), each shard owns a strided sub-space of the global sequence. Records from different shards interleave globally. The encoding is bit-packed:

```
shard_bits = ceil(log2(num_shards))
global_id  = (local_seq << shard_bits) | shard_id
```

So with `shards = 4` (`shard_bits = 2`), the first record on each shard has global ids `0, 1, 2, 3`, the second round `4, 5, 6, 7`, and so on. The mapping is a pure function: `shard_id = global_id & ((1 << shard_bits) - 1)` and `local_seq = global_id >> shard_bits`. (The simpler `local_seq * num_shards + shard_id` form is exact only when `num_shards` is a power of two.) See [Sharding](sharding.md) for the full discussion.

`RecordId.sequence` always stores the **global** sequence. `read(global_seq)` works regardless of shard count because segments index by global sequence.

## Segments

A **segment** is an append-only file holding a contiguous range of record sequences.

- **Rolling:** when the active segment reaches `config.segment_size` (default 256 MiB), it is rolled and a new segment is created. The next segment is **pre-created at 80% capacity** of the current one, so roll-time blocking is reduced to a single `fdatasync` rather than a create+fsync stall.
- **Naming:** `segment-NNNNNNNN.log`, where `NNNNNNNN` is the zero-padded segment id (starting at `00000001`).
- **Lookup:** each segment header records its `base_sequence`, so a record id maps to exactly one segment in O(log N) via a cached, directory-mtime-invalidated manifest.
- **Retention & truncation:** a `RetentionPolicy` (`KeepAll`, `MaxBytes`, `MaxAge`) bounds how much history is kept. Old segments fully below the WAL checkpoint are truncated on the next roll.

Segments are immutable once rolled, which is what makes crash recovery simple: recovery scans the tail of the active segment, detects torn writes, and truncates them.

## Ring buffer and slots

Producers do **not** write directly to segments. They write into a fixed-size **ring buffer** of slots, and a background Committer thread drains slots into the active segment.

- **Lock-free fast path:** a producer claims a slot by a CAS (compare-and-swap) on the producer cursor. There is no mutex on the append path — `append` scales with contention-free CAS throughput.
- **Slots** (`src/ring/slot.rs`) hold one record each. The producer writes the content (exclusively, per the claim), then does a `Release` store of `seq + 1` into the slot's `sequence` field to publish it.
- **Consumers** (Sealer, Committer) observe the publish via an `Acquire` load. Slot reuse is gated by a consume watermark — a slot is only reclaimed once the Committer has drained it past `ring_size` behind the producer cursor.
- **Multi-shard** (`config.shards > 1`): there is one ring per shard and the Committer polls all of them, preserving global ordering across shards.

`config.ring_size` (default 8192) is the number of slots per shard. If producers outrun the Committer by more than `ring_size`, `append` applies the `queue_full_policy` (`Block` or `Drop`).

## Cursor semantics

logdb exposes **three** cursors. Understanding the differences is essential for reasoning about visibility and crash safety:

```rust
impl LogDb {
    pub fn producer_cursor(&self) -> u64;  // max across shards
    pub fn committed_cursor(&self) -> u64; // min across shards
    pub fn durable_cursor(&self) -> u64;   // min across shards
}
```

| Cursor              | Meaning                                                              | Advanced by            |
|---------------------|----------------------------------------------------------------------|------------------------|
| `producer_cursor`   | Next sequence a producer will claim. Appends are visible to consumers once published. | `append` (CAS claim)   |
| `committed_cursor`  | Records the Committer has serialized and written to the segment file, but **not yet fsynced**. | Committer thread       |
| `durable_cursor`    | Records that have been **fsynced** to disk. Survives a crash.        | Committer after `fdatasync` |

The crucial rule for readers — from the `src/reader/mod.rs` module documentation:

> All reads are bounded by `durable_cursor`: only fsynced data is visible to readers. This guarantees that records read will survive a crash.

So `read(record_id)` returns `Ok(None)` if `record_id >= durable_cursor()`, even if the record has already been appended and committed. This is deliberate: it means **any record a reader can see will survive a crash**. There is no "read it now, lose it on crash" window.

Consequences:

- `flush()` waits for `durable_cursor` (not `committed_cursor`) to advance past your appended records — that is what makes `flush` a true durability barrier.
- After a crash and restart, recovery truncates any tail records that were committed-but-not-fsynced, restoring the invariant that the on-disk log ends exactly at the durable cursor.
- For latency monitoring, a wide gap between `producer_cursor` and `committed_cursor` means the Committer is saturated; a wide gap between `committed_cursor` and `durable_cursor` means the storage layer is fsync-bound.

## Inline vs spill

The single most important performance fact in logdb is the **256-byte boundary** (`INLINE_CAP` in `src/ring/slot.rs`):

- **Inline path** (records ≤ 256 bytes): content is embedded directly in the slot with **zero heap allocation and zero extra memcpy** across threads. p50 append latency is typically **<100 ns**.
- **Spill path** (records > 256 bytes): the append thread performs a heap allocation (`Box<[u8]>`) and a full memcpy of the content. The spill path is roughly **4× slower** in throughput with **~80× higher p99.9 tail latency** due to allocator jitter (observed: inline p99.9 ≈ 500 ns vs spill p99.9 ≈ 41 µs at 300 bytes).

256 bytes covers the vast majority of structured log records (JSON log lines, audit events, metrics samples) while keeping each slot cache-line friendly (inline storage occupies exactly 4 cache lines). For latency-sensitive workloads, **keep records ≤ 256 bytes** to stay on the inline fast path.

The boundary is exact: a 256-byte record is inline; a 257-byte record spills.

## See also

- [Usage guide overview](README.md)
- [Getting started](getting-started.md)
- [Durability](durability.md)
- [Sharding](sharding.md)

> logdb 0.2.0

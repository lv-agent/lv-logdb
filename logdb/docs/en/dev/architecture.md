# Architecture

logdb is an embedded, append-only log database. This page describes the write path, the background thread model, the cursors and watermarks that coordinate it, and the read path. It is the map you need before changing anything in `src/`.

## Contents

- [Write path](#write-path)
- [Thread model](#thread-model)
- [Cursors and watermarks](#cursors-and-watermarks)
- [Read path](#read-path)
- [See also](#see-also)

## Write path

A record travels from a producer thread to a segment file through four stages. The first three are lock-free; only the final stage touches disk, and it is owned by a single thread.

```
Many producer threads
     │  LogDb::append / append_batch / replicate
     ▼
┌──────────────────────────────────┐
│  Ring (optionally sharded)        │  ← lock-free CAS claim / claim_batch
│  Slot { content, hash_n, seq }    │  ← inline ≤ INLINE_CAP (256B) or heap spill
└─────────────┬────────────────────┘
              │  (optional) published slots
              ▼
       Sealer thread               ← BLAKE3 keyed hash chain
              │  (only with `hash-chain` feature AND shards == 1)
              ▼
       Committer thread            ← batch serialize + pwrite + fdatasync
              │
       ┌────────┴─────────┐
       │  segment-*.log    │       ← append-only, roll at segment_size
       └──────────────────┘
              │
       Reader / Pusher              ← point lookup & scan / remote push
```

Stage by stage:

1. **Claim.** A producer reserves a sequence number by CAS-advancing `producer_cursor` in the per-shard `Ring` (`src/ring/mod.rs`, `Ring::claim` / `Ring::claim_batch`). `claim_batch` reserves `n` consecutive sequences in a single CAS, so a batch is reserved all-or-none — there is never a gap of reserved-but-unwritten slots that would stall the Committer.
2. **Write + publish.** The producer holds exclusive access to `slots[seq & mask]`, calls `Slot::producer_write` (which stores content inline if ≤ `INLINE_CAP = 256` bytes, or spills to the heap), then `Slot::publish` does a `Release` store of `seq + 1` into the slot's `sequence` field.
3. **(Optional) Seal.** If the `hash-chain` feature is on **and** `shards == 1`, the Sealer thread scans published slots, computes `hash_n = BLAKE3_keyed(hash_init, prev_hash || content)`, and writes it back into the slot via `Slot::write_hash` before advancing `sealed_cursor`.
4. **Commit + fsync.** The Committer thread scans published (and, when hashing, sealed) slots, serializes a batch, performs `pwrite` into the active segment, advances `committed_cursor`, then `fdatasync`s and advances `durable_cursor`. The `SegmentManager` rolls a new segment when the active one reaches `segment_size`.

The Sealer sits between publish and commit **only** when hashing is on. When hashing is off, the Committer consumes published slots directly.

## Thread model

`LogDb::open` (`src/lib.rs`, around L90-224) constructs the shared state and spawns the background threads. The thread population is **conditional**:

| Thread     | Spawned by `open`?                                   | Entrypoint                          | Role                                                                                                                |
|------------|------------------------------------------------------|-------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| Committer  | **Always.**                                          | `pipeline::committer::run_committer` | Drains all rings round-robin, serializes to the active segment, fsyncs, advances `committed_cursor` / `durable_cursor`. |
| Sealer     | Only when `hash-chain` is enabled **and** `shards == 1`. | `pipeline::sealer::run_sealer`      | Computes the BLAKE3 keyed hash chain, advances `sealed_cursor`.                                                     |
| Pusher     | **Never by `open`.**                                 | `pusher::run_pusher`                | Pushes durable records to a remote sink. A **daemon-level** component, spawned by external services (e.g. `logdbd`).   |

Two points are easy to get wrong and worth stating explicitly:

- **The Pusher is NOT spawned by `LogDb::open`.** The `pusher` module is private (`mod pusher;` at `src/lib.rs:37`, not `pub mod`), and `run_pusher` is launched only by an embedding service through `PusherHandle::spawn`. The library itself never starts a pusher thread. Remote failures never back-pressure local appends.
- **Hashing with `shards > 1` is rejected.** A global hash chain requires single-shard ordering, so `open` returns an error if `hash_enabled && config.shards > 1` (the Sealer is only spawned on shard 0). Multi-shard hash chains are deferred.

### Shutdown coordination

`ShutdownState` (`src/pipeline/signal.rs`) is a three-phase state machine shared by appends and the background threads:

- **Phase 0 — Run.** Normal operation; `append` calls `enter()` (reserving an in-flight slot) before claiming and `leave()` after publishing.
- **Phase 1 — Drain.** Entered via `start_drain()`. New appends are rejected with `ShuttingDown`; `drain()` waits for `in_flight` to reach zero, then waits for the Committer to fsync up to the producer cursor. Background threads exit via `should_stop()` once they have processed the drain target.
- **Phase 2 — Abort.** Entered via `abort()` (also by `Drop`). Forces threads to stop without waiting for durability.

The `enter()` "add then check" ordering closes the TOCTOU window that would otherwise let `start_drain` observe `in_flight == 0` before a concurrent append had incremented it.

`drain(&self)` is the shared-safe drain path: it takes `&self`, so it works when `LogDb` is shared via `Arc` inside a long-running service. `shutdown(self)` consumes the handle, drains, and then joins the Committer (and Sealer) threads.

## Cursors and watermarks

Each `Ring` carries four monotonic cursors (`src/ring/mod.rs`):

| Cursor            | Meaning                                                            | Advanced by                  |
|-------------------|--------------------------------------------------------------------|------------------------------|
| `producer_cursor` | Next sequence a producer will claim.                               | `claim` / `claim_batch` (CAS) |
| `sealed_cursor`   | Sequences the Sealer has computed `hash_n` for (hash-chain only).  | Sealer thread                |
| `committed_cursor`| Sequences the Committer has `pwrite`-en to the active segment.     | Committer thread             |
| `durable_cursor`  | Sequences the Committer has `fdatasync`-ed. Survives a crash.      | Committer thread             |

Across shards, `LogDb` exposes aggregates: `producer_cursor()` is the **max** across shards (the worst-case durability target), while `committed_cursor()` and `durable_cursor()` are the **min** across shards (the slowest shard gates visibility). Readers are bounded by the minimum durable cursor.

### The consume watermark and the claim invariant

Slot reuse is gated by a single watermark (`Ring::consume_watermark`):

- with hash-chain: `min(sealed_cursor, committed_cursor)`
- without hash-chain: `committed_cursor`

Because content lives in the slot (there is no separate arena), there is exactly one resource and one watermark — no dual-watermark coordination is possible.

The **claim invariant** (`Ring::claim`, `src/lib.rs:226-303`) is:

> A slot for `seq` is only claimed when `seq - consume_watermark < ring_size`.

This guarantees the consumer (Sealer or Committer) has finished reading a slot before a producer may overwrite it. `claim_batch` uses the same gate with `in_flight + n <= ring_size`. The `replicate` path (standby ingest at an exact sequence) enforces the same invariant before writing. Under `QueueFullPolicy::Block` a producer spins with a spin/yield/sleep backoff until the watermark advances; under `Drop` it returns `AppendError::QueueFull` immediately.

## Read path

Reads never touch the ring buffer — they read segment files directly. The read path has three layers (`src/reader/mod.rs`):

1. **SegmentManifest — cached directory listing.** A sorted in-memory list of `(segment_id, path, base_sequence, flags)`. It is refreshed **only when the data directory's mtime changes** (a roll or a retention removal). Appending to the active segment does not change the directory mtime, so the cache stays valid between rolls. On filesystems where mtime is unavailable it falls back to refreshing every call (correct, just slower).
2. **O(log N) segment lookup.** `SegmentManifest::find(seq)` is a `partition_point` (binary search) for the largest `base_sequence <= seq`. The result is cloned out of the lock so file I/O happens without holding the manifest mutex.
3. **Sparse-index anchor + scan.** For raw segments the sparse index (`src/storage/index.rs`) provides an anchor at or before the target id; the reader seeks to that file offset and sequentially scans forward to the record. Frame-based segments (compressed or encrypted) start at the segment header and iterate frame by frame. In both cases a record is returned only if its `sequence` falls within the segment's range.

All reads are bounded by `durable_cursor`: a `read(record_id)` returns `Ok(None)` if `record_id >= min_durable_cursor`, guaranteeing that **any record a reader can see will survive a crash**.

## See also

- [Development guide home](README.md)
- [Project layout](project-layout.md) — module-by-module map of `src/`.
- [Storage format](storage-format.md) — on-disk segment, header, index, and frame layout.
- Concepts: [Cursor semantics](../usage/concepts.md#cursor-semantics)

> logdb 0.2.0

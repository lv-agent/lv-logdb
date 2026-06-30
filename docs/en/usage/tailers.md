# Tailers

Named consumers with independent, persisted read progress — the abstraction you build on for replication, downstream delivery, indexing pipelines, and any "follow the log" workload.

## Contents

- [What a tailer is](#what-a-tailer-is)
- [Creating a tailer](#creating-a-tailer)
- [The Tailer API](#the-tailer-api)
- [Progress is persisted only on `commit()`](#progress-is-persisted-only-on-commit)
- [Independent tailers](#independent-tailers)
- [A consumer-loop recipe](#a-consumer-loop-recipe)
- [Seek and replay](#seek-and-replay)
- [See also](#see-also)

## What a tailer is

A **tailer** is a named consumer of the log that maintains its own read position, separate from every other tailer and from ad-hoc [`read`](reading.md)/[`scan`](reading.md#range-scans) calls. Each tailer is identified by a string name and backed by a progress file on disk (`tailer_<name>.dat`), so a process restart resumes exactly where it left off.

Use a tailer when you want to:

- **Replicate** the log to a downstream system (another logdb, a relational DB, a search index, a message broker).
- **Build a derived view** (a projection, a materialized aggregate) by applying every record in order.
- **Stream** records to a consumer that reads at its own pace, independently of writers and of other consumers.

For one-shot history scans without persistent progress, use [`replay_from`](reading.md#replay-from-a-sequence) directly. Reach for a tailer when you need a crash-resilient *cursor*.

## Creating a tailer

`LogDb::new_tailer` opens (or creates) a named tailer and restores its saved position:

```rust
impl LogDb {
    /// Create a named tailer (consumer) with independent read progress.
    /// Progress is persisted to `tailer_<name>.dat` via `commit()`.
    pub fn new_tailer(&self, name: &str) -> Tailer;
}
```

`new_tailer` returns a `Tailer` directly — **not** a `Result`. It cannot fail: if `tailer_<name>.dat` exists and is well-formed, the saved sequence is restored; otherwise the tailer starts at sequence `0`.

```rust
let mut t = db.new_tailer("replicator");
assert_eq!(t.position(), 0); // fresh tailer
```

Reopening by the same name resumes from the last [`commit()`](#progress-is-persisted-only-on-commit):

```rust
// First process: read 100 records, persist progress.
let mut t = db.new_tailer("replicator");
let _ = t.next_batch(100).unwrap();
t.commit().unwrap();
assert_eq!(t.position(), 100);

// Later process / restart: same name resumes at 100.
let mut t2 = db.new_tailer("replicator");
assert_eq!(t2.position(), 100);
```

> Under `shards > 1`, the tailer tracks **per-shard** progress (one local sequence per shard, like Kafka per-partition offsets) and `next_batch` merges every shard's newly-durable records into one batch ordered by ascending global id. A stalled shard never blocks the others; cross-batch ordering is best-effort (a stalled shard's lower-global-id records may arrive in a later batch). The `name` must be a filesystem-safe identifier — it is interpolated directly into the progress filename `tailer_<name>.dat`. The progress file keeps the legacy 12-byte format for `shards == 1` and a `count + seqs + crc32c` vector format for `shards > 1`. `position()` returns the minimum per-shard position (a coarse progress indicator); `positions()` returns the full per-shard vector.

## The Tailer API

The full surface (`src/tailer.rs`):

| Method | Signature | Description |
|--------|-----------|-------------|
| `position` | `(&self) -> u64` | Minimum per-shard position (coarse progress); the next sequence to read for `shards == 1`. |
| `positions` | `(&self) -> &[u64]` | Full per-shard progress vector (one local sequence per shard). |
| `next_batch` | `(&mut self, max_count: usize) -> Result<Option<Vec<Record>>, String>` | Read up to `max_count` **durable** records; `Ok(None)` when none are available. |
| `commit` | `(&self) -> std::io::Result<()>` | Persist the current position to `tailer_<name>.dat`. |
| `seek` | `(&mut self, seq: u64)` | Move the position to `seq` (in-memory; not persisted until `commit`). |
| `reset` | `(&mut self) -> std::io::Result<()>` | Set position to `0` and delete the progress file. |

### `next_batch` — durable reads

`next_batch` honors the same durability rule as [`read`](reading.md#visibility-and-the-durable-cursor): it reads only up to `durable_cursor()`. A record appended but not yet flushed to disk is invisible — there is no "read it now, lose it on crash" window.

- Returns `Ok(Some(records))` with up to `max_count` records starting at `position()`. The position is advanced past the last record returned.
- Returns `Ok(None)` when there are no new durable records at or after the current position (the tail is caught up), or when the range yields no records.
- Returns `Err(String)` on an I/O or decode failure.

`next_batch` is the workhorse of a consumer loop:

```rust
match t.next_batch(500)? {
    Some(batch) => {
        deliver(&batch);
        t.commit()?;
    }
    None => std::thread::sleep(Duration::from_millis(10)),
}
```

### `commit` — persist progress

`commit()` writes `position()` to `tailer_<name>.dat` atomically (write-to-temp, `fdatasync`, rename, dir-sync). It is the **only** operation that advances the on-disk cursor — see [below](#progress-is-persisted-only-on-commit).

### `seek` — move the cursor

`seek(seq)` jumps the in-memory position to an arbitrary sequence — useful for **replay from a known point** (re-reading history after a downstream rebuild, or skipping ahead). It does no I/O and is not persisted; call `commit()` afterwards if you want the new position to survive a restart.

### `reset` — back to the start

`reset()` sets `position` to `0` and removes the progress file. Use it to fully rewind a consumer (e.g. to reprocess the whole log after fixing a downstream bug).

## Progress is persisted only on `commit()`

This is the single most important rule for tailers, and the basis of at-least-once delivery:

> The on-disk progress file is updated **only** when you call `commit()`. Reads via `next_batch` advance the *in-memory* position; without a `commit`, that advance is lost on the next open.

Concretely, the progress file (`tailer_<name>.dat`, 12 bytes: 8-byte little-endian sequence + 4-byte CRC32C, `src/tailer.rs:16-51`) is written by `save_progress`, which is called only from `Tailer::commit` (`src/tailer.rs:96-140`). `next_batch`, `seek`, and even `reset` only mutate in-memory state (except that `reset` also deletes the file).

**Crash semantics** — what this gives you:

- **If you crash after `next_batch` but before `commit`:** the records you read are *not* marked consumed. On reopen, `new_tailer` restores the last committed position, so you will **re-read** those records. This is at-least-once delivery — design `deliver()` to be idempotent, or de-duplicate downstream by record id.
- **If you crash after `commit`:** the position is durably persisted (temp-file + `fdatasync` + rename + dir-sync). On reopen you resume strictly past the last committed batch — no re-read, no skip.
- **A torn progress write is detected:** the file's trailing CRC32C covers the 8-byte sequence. If the CRC check fails (a half-written rename, bit rot), `load_progress` falls back to `0` (`src/tailer.rs:24-34`) — i.e. the tailer replays from the beginning rather than trusting a corrupt cursor. This is conservative and safe.

The recommended pattern is therefore **process → commit** within the same critical section as the side effect you are protecting:

```rust
while let Some(batch) = t.next_batch(500)? {
    deliver_to_downstream(&batch)?; // make the side effect durable
    t.commit()?;                     // then advance the cursor
}
```

Order matters: deliver first, commit second. If you commit before delivering and then crash, you lose those records permanently. See [A consumer-loop recipe](#a-consumer-loop-recipe).

## Independent tailers

Tailers are fully independent: each name gets its own progress file and its own in-memory cursor, and they do not interfere with each other or with writers. Two tailers can read the same log at very different paces — a fast one can lap a slow one without affecting it.

```rust
let mut fast = db.new_tailer("fast-forwarder");
let mut slow = db.new_tailer("slow-indexer");

// Each advances independently.
let _ = fast.next_batch(10_000).unwrap();
let _ = slow.next_batch(50).unwrap();
assert_eq!(fast.position(), 10_000);
assert_eq!(slow.position(), 50);

// The fast one pulling ahead does not move the slow one's cursor.
let _ = fast.next_batch(10_000).unwrap();
assert_eq!(slow.position(), 50);
```

This is the model for fan-out delivery: one tailer per downstream system (the replicator, the search indexer, the metrics aggregator), each with its own durability guarantees and back-pressure.

## A consumer-loop recipe

A correct, crash-safe consumer that delivers to a downstream system (a replica, a sink, a queue) and persists exactly once per delivered batch:

```rust
use std::time::Duration;

fn run_replicator(db: &LogDb) -> Result<(), Box<dyn std::error::Error>> {
    // new_tailer returns a Tailer, NOT a Result — no `?` here.
    let mut t = db.new_tailer("replicator");

    loop {
        match t.next_batch(500)? {
            Some(batch) => {
                // 1. Deliver the side effect FIRST.
                send_to_replica(&batch)?;

                // 2. Only then persist progress, so a crash before this
                //    line replays the batch rather than losing it.
                t.commit()?;
            }
            None => {
                // Tail is caught up; back off briefly before polling again.
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
}
```

Two correctness invariants this recipe enforces:

1. **Deliver, then commit** — the side effect (`send_to_replica`) is made durable before the cursor advances. A crash between `next_batch` and `commit` replays the batch; a crash after `commit` is past it. There is no window in which a record is neither delivered nor still pending.
2. **Idempotent delivery** — because of point 1's replay-on-crash guarantee, `send_to_replica` may see the same batch twice across a restart. Make it idempotent (upsert by record id, or de-duplicate via an idempotency key).

For higher throughput, batch larger (`next_batch(10_000)`); for lower latency, poll more often or sleep for shorter intervals. The `None` branch is your back-pressure signal — there is nothing new to read.

## Seek and replay

`seek(seq)` is the escape hatch for out-of-band cursor management:

- **Replay from a checkpoint:** if you have an external truth of "downstream has consumed through sequence N", `t.seek(N); t.commit()?;` snaps the tailer to it without reading the intervening records.
- **Re-read a range:** `t.seek(N)` then `next_batch` to reprocess records you have already committed past (e.g. after fixing a downstream bug).
- **Skip ahead:** `t.seek(future_seq)` to fast-forward past records you deliberately want to drop.

Because `seek` is in-memory only, the on-disk cursor is unaffected until you `commit`. To rewind *and* forget all committed progress, use `reset()` instead — it both zeroes the position and deletes the progress file.

## See also

- [Usage guide](README.md)
- [Reading](reading.md) — point reads, range scans, and the durability-bound visibility rule that tailers inherit.
- [Durability](durability.md) — what makes a record visible to `next_batch`.
- [Recovery](recovery.md) — what happens to the log (and your tailers' progress files) after a crash.
- [Configuration](configuration.md) — `flush_timeout`, `retention`, and other knobs that affect how fast a tailer can make progress.

> logdb 0.2.0

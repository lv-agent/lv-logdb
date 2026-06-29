# Reading

How to read records back from logdb: point lookups, range scans, replay, the durability-bound visibility rule, transparent decompress/decrypt, and how to handle read errors.

## Contents

- [Visibility and the durable cursor](#visibility-and-the-durable-cursor)
- [Point reads](#point-reads)
- [Range scans](#range-scans)
- [Replay from a sequence](#replay-from-a-sequence)
- [How a point read finds a record](#how-a-point-read-finds-a-record)
- [Transparent decompression and decryption](#transparent-decompression-and-decryption)
- [Read errors](#read-errors)

## Visibility and the durable cursor

The single most important rule for readers — quoted from the `src/reader/mod.rs` module documentation:

> All reads are bounded by `durable_cursor`: only fsynced data is visible to readers. This guarantees that records read will survive a crash.

Concretely, `read(record_id)` returns `Ok(None)` whenever `record_id >= durable_cursor()`, **even if the record has already been appended and committed**. There is no "read it now, lose it on crash" window: any record a reader can see will survive a crash.

If you need a record visible, call [`flush`](writing.md#when-to-flush) first. See [Concepts: Cursor semantics](concepts.md#cursor-semantics) for the full producer / committed / durable model.

## Point reads

`LogDb::read` performs a point lookup by global record id:

```rust
impl LogDb {
    pub fn read(&self, record_id: u64) -> Result<Option<Record>, ReadError>;
}
```

- Returns `Ok(Some(Record))` when the (durable) record exists.
- Returns `Ok(None)` when the record does not exist **or** is not yet durable (`record_id >= durable_cursor()`).
- Returns `Err(ReadError)` on I/O failure or detected corruption.

```rust
db.append(b"event-7")?;
db.flush()?;
let rec = db.read(id)?.expect("present after flush");
assert_eq!(rec.content, b"event-7");
```

The returned `Record` is fully owned:

```rust
pub struct Record {
    pub id: RecordId,
    pub timestamp_ns: u64,
    pub content: Vec<u8>,
    pub hash_n: [u8; 32],
}
```

See [Concepts: The Record struct](concepts.md#the-record-struct) for field semantics.

## Range scans

`LogDb::scan` returns an iterator over a **half-open** range `[from_id, to_id)`:

```rust
impl LogDb {
    pub fn scan(
        &self,
        from_id: u64,
        to_id: u64,
    ) -> Result<reader::iter::RecordIter, ReadError>;
}
```

`from_id` is inclusive, `to_id` is **exclusive** — so `scan(10, 20)` yields records `10..19`. If the starting segment for `from_id` cannot be located, `scan` returns `ReadError::NotFound(from_id)`.

```rust
let first = db.append(b"a")?;
db.append(b"b")?;
db.append(b"c")?;
db.flush()?;

for rec in db.scan(first, first + 3)? {
    println!("{}: {:?}", rec.id.sequence, rec.content);
}
```

## Replay from a sequence

`LogDb::replay_from(sequence)` is a convenience wrapper that scans from `sequence` (inclusive) to the end of the durable log:

```rust
impl LogDb {
    pub fn replay_from(&self, sequence: u64) -> Result<reader::iter::RecordIter, ReadError>;
}
```

It is implemented as `scan(sequence, u64::MAX)`, so it yields every durable record with id `>= sequence`. This is what you use to (re)process history — for example, after rebuilding a downstream view, or as the basis of a [tailer](tailers.md). Only durable records are returned, so calling `flush` first guarantees replay sees the latest writes.

```rust
for rec in db.replay_from(checkpoint)? {
    apply(rec?);
}
```

## How a point read finds a record

Reads are O(log N) in the number of segments, not O(N) per record. The lookup algorithm (from the `src/reader/mod.rs` module doc):

1. **Find the segment** containing the target `record_id` by checking each segment's `[base_record_id, max_record_id]` range. A cached, sorted segment manifest (`SegmentManifest`) makes this a binary search and is invalidated only when the data directory's mtime changes (a roll or a retention truncation), so a `read()` does **not** re-`readdir` or re-read every segment header on each call.
2. **Find a sparse-index anchor** at or before the target id (raw segments only). The `.idx` file maps every `index_stride`-th record id to its file offset.
3. **Seek and scan** — open the segment, seek to the anchor's file offset, then sequentially scan forward to the target record id.

For frame-based segments (compressed or encrypted), there is no per-record sparse index; the read starts at the segment header and decodes frames forward (see [Transparent decompression and decryption](#transparent-decompression-and-decryption)).

The full data structure and code-path detail belongs in the development guide (see [Architecture](../dev/architecture.md)); application code only needs to know that point reads are bounded and fast.

## Transparent decompression and decryption

If the database was opened with `compression_enabled` or an `encryption_key`, segments are stored compressed and/or encrypted on disk. **Reads decode transparently** — the same `read` / `scan` / `replay_from` calls return plain `Record` values, regardless of how the segment is stored on disk.

The shared decode path is `decode_frame_payload` (`src/reader/mod.rs:67-84`): given an on-disk frame payload it decrypts (if encrypted) and then decompresses (if compressed), returning the raw record bytes. This path is used by the Reader, the scan iterator, and crash recovery, so all three agree on the frame layout.

Two practical consequences:

- **Encrypted reads require the key.** A `LogDb` opened without the matching `encryption_key` cannot decrypt its segments; encrypted frames are undecryptable. Open the database with the same key you wrote it with.
- **Compression/encryption trade read CPU for disk savings.** Because frame-based segments lack a per-record sparse index, point reads on compressed/encrypted segments do a frame-aligned forward scan from the segment header rather than a sparse-index seek. For latency-sensitive point reads on large segments, weigh this against the disk/CPU trade-off of leaving segments raw.

## Read errors

`ReadError` in full (`src/error.rs`):

```rust
pub enum ReadError {
    /// The requested record_id does not exist.
    NotFound(u64),
    /// A CRC check failed, indicating data corruption.
    CrcMismatch(u64),
    /// An I/O error occurred during reading.
    Io(String),
}
```

Handling guidance:

- `NotFound(u64)` — `scan` returns this when its starting segment cannot be found; treat as "no data in range" and check the id against `durable_cursor()`. (A point `read` of a non-existent or non-durable id returns `Ok(None)` rather than an error.)
- `CrcMismatch(u64)` — a record failed its CRC check, which indicates on-disk corruption (torn writes the recovery didn't truncate, bit rot, or a hardware fault). Do not silently skip it: alert, quarantine the segment, and consult [Recovery](recovery.md). If you enabled the `hash-chain` feature, the chain gives you tamper-evidence beyond the per-record CRC.
- `Io(String)` — an I/O failure during reading (file open, seek, read). Inspect the message; transient faults may warrant retry, persistent ones indicate a storage problem.

## See also

- [logdb README](../README.md)
- [Writing](writing.md)
- [Concepts](concepts.md)
- [Durability](durability.md)
- [Recovery](recovery.md)
- [Tailers](tailers.md)
- [Errors](errors.md)

> logdb 0.2.0

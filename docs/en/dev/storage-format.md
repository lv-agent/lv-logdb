# Storage format

On-disk binary layout of a logdb data directory — segment files, segment headers, record framing, the sparse index, compressed/encrypted frames, the hash chain, and the atomic metadata files.

> Authoritative for **logdb 0.2.0**. Verify against `src/storage/format.rs` and `src/storage/index.rs` when the code changes.

logdb is an append-only, crash-safe log. Every byte on disk is either a segment (the log itself), a sparse index (a derived accelerator for raw segments), or a 12-byte metadata file. All multi-byte integers are **little-endian**. All checksums are **CRC32C** (Castagnoli) via the `crc32c` crate; the hash chain (when enabled) is **BLAKE3 keyed**.

## Data-directory layout

A data directory is a single partition's store. It contains:

| Path                         | Kind      | Source of truth? | Notes                                                                     |
|------------------------------|-----------|------------------|---------------------------------------------------------------------------|
| `segment-NNNNNNNN.log`       | Segment   | yes              | `NNNNNNNN` is the 8-digit zero-padded `segment_id`, starting at `00000001`. |
| `segment-NNNNNNNN.idx`       | Index     | no (rebuildable) | Sparse index for raw segments; absent for compressed/encrypted segments.  |
| `checkpoint.dat`             | Metadata  | yes              | Last durable sequence number.                                             |
| `tailer_<name>.dat`          | Metadata  | yes              | Per-tailer read position, one file per named tailer.                      |
| `pusher_progress.dat`        | Metadata  | yes              | Last sequence successfully pushed by a pusher.                            |

A fresh database always begins with `segment-00000001.log`. Segments roll when the active segment reaches the configured `segment_size`; each roll allocates the next `segment_id` and writes a new segment header that chains to the previous one.

## Segment header (128 bytes)

Every `.log` file begins with a fixed `SEGMENT_HEADER_SIZE = 128`-byte header (`src/storage/format.rs:58`). The header is written once at segment creation and never rewritten in place (timestamp range fields are conceptual — they are backfilled only on a future header rewrite path; the on-disk fields below are the authoritative layout).

### Byte layout

| Offset | Size | Field              | Type      | Description                                                              |
|--------|------|--------------------|-----------|--------------------------------------------------------------------------|
| 0      | 4    | `magic`            | u32 LE    | `0x4C474442` ("LGDB"). Rejects non-logdb files.                          |
| 4      | 2    | `format_version`   | u16 LE    | `0x0001` (`FORMAT_VERSION`).                                             |
| 6      | 1    | `flags`            | u8        | Bitmask, see below.                                                      |
| 7      | 1    | `hash_algo`        | u8        | `1`=SHA256, `2`=BLAKE3 (`HASH_ALGO_*`).                                  |
| 8      | 32   | `hash_init`        | [u8; 32]  | BLAKE3 keyed-mode key; CSPRNG-generated, **persisted here**, recovered on restart. |
| 40     | 8    | `base_sequence`    | u64 LE    | First sequence number stored in this segment.                            |
| 48     | 4    | `partition_id`     | u32 LE    | Logical partition identifier.                                            |
| 52     | 4    | `segment_id`       | u32 LE    | Monotonically increasing from 1.                                         |
| 56     | 8    | `min_timestamp_ns` | u64 LE    | Earliest record timestamp (backfilled).                                  |
| 64     | 8    | `max_timestamp_ns` | u64 LE    | Latest record timestamp (backfilled).                                    |
| 72     | 4    | `header_crc`       | u32 LE    | CRC32C over bytes `[0, 72)` (`HEADER_CRC_END`, `src/storage/format.rs:61`). |
| 76     | 32   | `prev_last_hash`   | [u8; 32]  | Previous segment's final `hash_n` (chain linkage; zeros for the first segment). |
| 108    | 1    | `record_format`    | u8        | Record encoding version (`1` = `RECORD_FORMAT_V1`).                     |
| 109    | 19   | `_reserved`        | 19 bytes  | Zero-filled; reserved for future extensions.                             |
| 128    |      |                    |           | END                                                                      |

`SegmentHeader::serialize` (`src/storage/format.rs:116-136`) writes this layout and fills `header_crc` last; `deserialize` (`src/storage/format.rs:139-198`) validates `magic` and recomputes the CRC over `[0, 72)` before trusting any field, so a torn or tampered header is rejected.

### `flags` bitmask

| Bit | Mask  | Constant                    | Meaning                                                              |
|-----|-------|-----------------------------|----------------------------------------------------------------------|
| 0   | 0x01  | `FLAG_NOT_FIRST`            | Set on every segment after the first (chained segment).              |
| 1   | 0x02  | `FLAG_HASH_ENABLED`         | Hash chain active; per-record `hash_n` is meaningful.                |
| 2   | 0x04  | `FLAG_COMPRESSED_ZSTD`      | Records are packed into zstd-compressed frames (frame layout).       |
| 3   | 0x08  | `FLAG_ENCRYPTED_AES256GCM`  | Records are packed into AES-256-GCM-encrypted frames (frame layout). |

> Defined at `src/storage/format.rs:64-71`. **Either `FLAG_COMPRESSED_ZSTD` or `FLAG_ENCRYPTED_AES256GCM` switches the segment to the frame layout** described below; a raw segment (neither bit set) uses plain record framing.

## Record framing (raw segments)

In a raw segment (`flags & 0x0C == 0`), records are appended one after another immediately after the 128-byte header. Each record is self-describing and self-checksumming (`src/storage/format.rs:253-359`):

| Field         | Type    | Size | Offset within record | Notes                                                       |
|---------------|---------|------|----------------------|-------------------------------------------------------------|
| `len`         | u32 LE  | 4    | 0                    | Total record size in bytes, including this field and `crc`. |
| `sequence`    | u64 LE  | 8    | 4                    | Partition-local sequence number.                            |
| `timestamp_ns`| u64 LE  | 8    | 12                   | Record timestamp.                                           |
| `content_len` | u32 LE  | 4    | 20                   | Length of `content`.                                        |
| `content`     | [u8]    | N    | 24                   | Payload bytes (`N = content_len`).                          |
| `hash_n`      | [u8;32] | 32   | 24+N                 | BLAKE3 keyed chain hash; zeros when hashing is disabled.    |
| `crc`         | u32 LE  | 4    | 56+N                 | CRC32C over `[0, 56+N)` with the `len` field zeroed.        |

The minimum record size is `MIN_RECORD_SIZE = 60` bytes (`src/storage/format.rs:254`):

```
MIN_RECORD_SIZE = 4 + 8 + 8 + 4 + 0 + 32 + 4 = 60   (zero-length content)
record_size(N)  = 4 + 8 + 8 + 4 + N + 32 + 4 = 60 + N
```

`deserialize_record` (`src/storage/format.rs:299-359`) reads `len`, rejects buffers shorter than `MIN_RECORD_SIZE`, cross-checks that `len == record_size(content_len)`, then recomputes CRC32C over the record body with the `len` field treated as zero and compares it against the stored `crc`. Any mismatch (corruption, truncation, partial write) yields an error and the reader advances past the record. The `len` field is zeroed during CRC computation so the on-disk length is itself covered indirectly through the rest of the framing.

```
 ┌──────┬──────────┬──────────────┬─────────────┬─────────┬───────────┬───────┐
 │ len  │ sequence │ timestamp_ns │ content_len │ content │  hash_n   │  crc  │
 │ u32  │   u64    │     u64      │    u32      │  [u8]N  │  [u8;32]  │ u32   │
 └──────┴──────────┴──────────────┴─────────────┴─────────┴───────────┴───────┘
  0      4           12             20            24         24+N        56+N     = len
```

## Sparse index (`.idx`)

Raw segments are paired with a sparse index (`src/storage/index.rs`) that accelerates point lookups. The index is a **derived, rebuildable artifact**: if missing or corrupted, the reader reconstructs it by scanning the segment.

### `IndexEntry` (24 bytes)

| Offset | Size | Field          | Type   | Description                                              |
|--------|------|----------------|--------|----------------------------------------------------------|
| 0      | 8    | `sequence`     | u64 LE | Record identifier at this anchor.                        |
| 8      | 8    | `file_offset`  | u64 LE | Byte offset of the record within the `.log` file.        |
| 16     | 8    | `timestamp_ns` | u64 LE | Record timestamp, for time-based queries.                |

`IndexEntry::SERIALIZED_SIZE = 24` (`src/storage/index.rs:28`).

### File layout

A `.idx` file is `[stride: u32 LE][entries: IndexEntry × M]`:

```
┌──────────┬────────────────────────────────────────────┐
│ stride   │ IndexEntry[0] | IndexEntry[1] | ...        │
│ u32 LE   │   24 bytes    |   24 bytes    | ...        │
└──────────┴────────────────────────────────────────────┘
  0          4
```

`SparseIndex::DEFAULT_STRIDE = 1024` (`src/storage/index.rs:66`): one anchor is written every 1024 records (`should_index(n)` returns true when `n % stride == 0`). The index path is derived by `SparseIndex::index_path` — `segment-00000001.log` → `segment-00000001.idx`.

### Anchored reads

`SparseIndex::find_anchor(record_id)` (`src/storage/index.rs:91-102`) binary-searches for the largest entry whose `sequence <= record_id` and returns `(entry, position)`. The reader seeks to `entry.file_offset` and scans forward record-by-record to the target. Returns `None` if the index is empty or the target precedes the first indexed record. `find_by_time` provides the analogous anchor for timestamp queries.

### Raw segments only

A sparse index is meaningful only when records sit at known, independently seekable file offsets. Compressed or encrypted segments store records inside opaque frames, so per-record offsets are not seekable and **no `.idx` is written** for them (`fresh_index` returns `None` when either flag is set, `src/storage/mod.rs:307-313`). Frame-segment reads scan from the segment header.

## Frame layout (compressed / encrypted segments)

When `flags` has `FLAG_COMPRESSED_ZSTD` or `FLAG_ENCRYPTED_AES256GCM` set, the post-header region is a sequence of **frames** instead of raw records. Each frame packs one or more records behind an 8-byte header.

### Frame header (8 bytes)

`FRAME_HEADER_SIZE = 8` (`src/storage/format.rs:77`). `read_frame_header` returns `(compressed_len, decompressed_len)` (`src/storage/format.rs:84-88`):

| Offset | Size | Field              | Type   | Description                                            |
|--------|------|--------------------|--------|--------------------------------------------------------|
| 0      | 4    | `compressed_len`   | u32 LE | On-disk payload length (`cl`).                         |
| 4      | 4    | `decompressed_len` | u32 LE | Length of the payload after decode (`dl`); bounds record scanning within the frame. |

### Frame layout

```
┌────────────────┬────────────────────────────────────────────────────────┐
│ frame_header   │  payload  (compressed_len bytes on disk)               │
│  cl, dl (8B)   │  = encrypt?( compress?( raw_records ) )                │
└────────────────┴────────────────────────────────────────────────────────┘
```

`payload` is produced by composing the transforms right-to-left during writes (`src/storage/mod.rs:466-490`):

1. Concatenate one or more raw records (the record framing above, without the segment header).
2. If `FLAG_COMPRESSED_ZSTD`: zstd-compress the concatenation.
3. If `FLAG_ENCRYPTED_AES256GCM`: encrypt the (possibly-compressed) bytes with AES-256-GCM and prepend a 12-byte nonce.

The reader inverts the pipeline (`decode_frame_payload`, `src/reader/mod.rs`): read `cl` bytes, decrypt (strip the leading nonce) if encrypted, zstd-decompress if compressed, then iterate records inside the decoded buffer up to `dl` bytes (`src/reader/mod.rs:256-268`).

### Encryption nonce

`ENCRYPTION_NONCE_SIZE = 12` (`src/storage/format.rs:72`). Each encrypted frame draws a fresh random nonce via `getrandom` and stores it as the first 12 bytes of the payload (`src/storage/mod.rs:289-298`), so the on-disk payload is `{nonce:12B | ciphertext}`. AES-256-GCM provides both confidentiality and authenticity; a frame that fails GCM tag verification is rejected.

### Framing note

A frame may batch multiple records for amortized compression/encryption overhead. The reader walks frames sequentially from the segment header — there is no per-record offset index — and parses records out of each decoded frame using the same `deserialize_record` routine as the raw path.

## Hash chain

When `FLAG_HASH_ENABLED` is set, each record carries a 32-byte `hash_n` linking it into a tamper-evident chain.

- **Algorithm:** BLAKE3 in keyed mode (`HASH_ALGO_BLAKE3`, v0.2.0 default), computed by the Sealer as `hash_n = BLAKE3_keyed(hash_init, prev_hash || content)` (`src/pipeline/sealer.rs:70-74`). SHA256 is also defined (`HASH_ALGO_SHA256`) for compatibility.
- **`hash_init` is persisted in the segment header** at bytes `[8, 40)` (`src/storage/format.rs:123, 158-159`). It is CSPRNG-generated once per database and **recovered from the segment header on restart** — it is not held only in memory, and it is not derived from a user-supplied key. Because the key is on disk alongside the data, the chain detects tampering and accidental corruption; for protection against an attacker who can read the directory, combine with `FLAG_ENCRYPTED_AES256GCM`.
- **Chain linkage:** every segment's header carries `prev_last_hash` (bytes `[76, 108)`), the previous segment's final `hash_n`, so the chain spans segment boundaries. Verification replays records starting from `hash_init` and `prev_last_hash` and checks each `hash_n`.

If hashing is disabled, `hash_n` is written as 32 zero bytes and `FLAG_HASH_ENABLED` is clear.

## Metadata files (12 bytes, atomic)

`checkpoint.dat`, `tailer_<name>.dat`, and `pusher_progress.dat` all share the same 12-byte layout:

| Offset | Size | Field | Type   | Description                                  |
|--------|------|-------|--------|----------------------------------------------|
| 0      | 8    | `seq` | u64 LE | Last durable / consumed / pushed sequence.   |
| 8      | 4    | `crc` | u32 LE | CRC32C over bytes `[0, 8)`.                  |

Sources: `src/lib.rs:668-683` (`save_checkpoint`), `src/tailer.rs:16-51` (`PROGRESS_SIZE = 12`), `src/pusher.rs:52-102` (`PROGRESS_FILE_SIZE = 12`).

### Atomic write

All three files are written with the same crash-safe sequence to avoid a torn update leaving a corrupt pointer:

```
write tmp file  →  fdatasync(tmp)  →  rename(tmp → final)  →  sync_dir(dir)
```

On load, the reader checks the length is exactly 12 and that `crc32c(seq_bytes)` matches the stored `crc`; any mismatch is treated as corruption and the file is ignored (the checkpoint falls back to scanning segments, the tailer/pusher resets to sequence 0).

## See also

- [Development guide home](README.md)
- [Architecture](architecture.md) — how the read/write paths consume this format at runtime.
- Concepts: [Durability and recovery](../usage/durability.md)

> logdb 0.2.0

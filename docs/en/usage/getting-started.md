# Getting started

A minimal walk-through: add logdb to your project, open a database, append a record, flush it to disk, and read it back.

## Contents

- [Prerequisites](#prerequisites)
- [Adding the dependency](#adding-the-dependency)
- [A minimal example](#a-minimal-example)
- [What lands in the data directory](#what-lands-in-the-data-directory)
- [Next steps](#next-steps)

## Prerequisites

logdb targets the **Rust 2021 edition**. Install a recent stable Rust toolchain (1.70 or newer recommended) via [rustup](https://rustup.rs/):

```bash
rustup default stable
rustc --version
```

logdb is a Linux-first embedded database. It uses Linux syscalls (`fdatasync`, `syncfs`, `clock_realtime_coarse`) and is developed and tested on Linux. macOS may work for development; Windows/WSL2 fdatasync behavior can be slow and is not production-targeted.

## Adding the dependency

Add logdb to your `Cargo.toml`:

```toml
[dependencies]
logdb = { version = "0.2", path = "…" }   # or a git/path source for now
```

logdb ships with **no features enabled by default** (`default = []`). Opt into the capabilities you need:

| Feature         | Enables                                  | When to use                                  |
|-----------------|------------------------------------------|----------------------------------------------|
| `hash-chain`    | SHA-256 / BLAKE3 forward-link tamper chain | You need tamper-evidence / audit trails.     |
| `compression`   | zstd frame compression for segments      | You want to trade CPU for lower disk usage.  |
| `encryption`    | AES-256-GCM at-rest encryption           | You store sensitive data and need secrecy.   |
| `remote-push`   | Reserved flag for remote push (no deps)  | Reserved for future remote replication.      |

For example, to enable hash-chain and compression:

```toml
[dependencies]
logdb = { version = "0.2", features = ["hash-chain", "compression"] }
```

## A minimal example

This example opens a database in a temporary directory, appends one record, flushes it to durable storage, then reads it back. It mirrors the `open → append → flush → read` lifecycle tested in `tests/integration.rs` and the `LogDb` module documentation in `src/lib.rs`.

```rust
use std::time::Duration;
use std::path::PathBuf;

use logdb::config::{Config, DurabilityMode};
use logdb::LogDb;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Build a config. Point data_dir at a real path for your application.
    let mut config = Config::default();
    config.data_dir = PathBuf::from("/tmp/logdb-getting-started");
    config.durability_mode = DurabilityMode::Async; // or Sync for strongest guarantees
    config.flush_timeout = Duration::from_secs(5);

    // 2. Open (creates the directory and first segment if missing).
    let db = LogDb::open(config)?;

    // 3. Append a record. Returns its global record id.
    let id = db.append(b"hello logdb")?;
    println!("appended record id = {}", id);

    // 4. Force all appended records to durable (fsynced) storage.
    db.flush()?;

    // 5. Read the record back. Only fsynced records are visible to readers.
    let record = db.read(id)?.expect("record should exist after flush");
    assert_eq!(record.id.sequence, id);
    assert_eq!(record.content, b"hello logdb");
    println!("read: {:?}", record);

    // 6. Drain in-flight appends and shut down gracefully.
    db.shutdown(Duration::from_secs(5))?;
    Ok(())
}
```

Key API signatures used above (from `src/lib.rs`):

```rust
impl LogDb {
    pub fn open(config: Config) -> Result<Self, String>;
    pub fn append(&self, content: &[u8]) -> Result<u64, AppendError>;
    pub fn flush(&self) -> Result<(), FlushError>;
    pub fn read(&self, record_id: u64) -> Result<Option<Record>, ReadError>;
}
```

Notes:

- `append` returns the **global record id** (`u64`). In the single-partition default case, this is the same value you pass back to `read` and the same as `record.id.sequence`.
- `read` returns `Ok(None)` if the record does not exist **or** has not been fsynced yet — only data below `durable_cursor()` is visible to readers (see [Concepts](concepts.md)).
- `flush` blocks until `durable_cursor` advances past the records you appended, so it is the natural synchronization point between writers and readers.

## What lands in the data directory

After the first `append`/`flush`, your `data_dir` will contain something like:

```
/tmp/logdb-getting-started/
├── segment-00000001.log   # append-only segment file (records + header)
├── segment-00000001.idx   # sparse index for fast record lookup (raw segments)
└── checkpoint.dat         # WAL checkpoint: records below this sequence are truncatable
```

- **`segment-NNNNNNNN.log`** — append-only segment files, rolled automatically when they reach `segment_size` (default 256 MiB). The next segment is pre-created at 80% capacity so roll-time blocking is reduced to a single `fdatasync`.
- **`segment-NNNNNNNN.idx`** — a sparse index that lets `read()` seek near the target record instead of scanning from the segment head (only present for uncompressed/unencrypted raw segments).
- **`checkpoint.dat`** — the durable checkpoint sequence, written atomically (temp + `fdatasync` + rename). Crash recovery uses it to bound WAL replay.

Old segments fully covered by the checkpoint are truncated on the next roll, subject to your retention policy.

## Next steps

- [Concepts](concepts.md) — the core model: records, `RecordId`, segments, ring buffers, and cursor semantics.
- [Cookbook](cookbook.md) — recipes for common tasks (batching, scanning, retention, and more).

## See also

- [Usage guide overview](README.md)
- [Concepts](concepts.md)
- [Configuration](configuration.md)

> logdb 0.2.0

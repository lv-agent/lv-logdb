# logdb

Embedded, append-only, crash-recoverable, optionally tamper-proof local log database. Built in Rust.

## Quick Start

```rust
use logdb::{Config, LogDb};

let db = LogDb::open(Config::default())?;
let seq = db.append(b"hello")?;
db.flush()?;
let record = db.read(seq)?.unwrap();
assert_eq!(record.content, b"hello");
```

## Features

- **Lock-free writes**: multi-producer CAS ring buffer, p50 < 60ns
- **Durability modes**: Sync (fsync per batch), Batch (periodic), Async (caller-driven)
- **Hash chain** (`hash-chain` feature): BLAKE3 keyed tamper-evident integrity
- **Compression** (`compression` feature): streaming zstd per-frame
- **Encryption** (`encryption` feature): AES-256-GCM per-frame
- **WAL checkpoint**: persistent, survives crash
- **Crash recovery**: torn-write detection + hash chain verification
- **Sharding**: multi-ring for high-core scalability
- **Remote push** (`remote-push` feature): async durable record push

## As a WAL

```rust
// Write
db.append_batch(&[redo1, undo, redo2])?;  // atomic
db.flush()?;
db.checkpoint(db.durable_cursor());       // persistent

// Recover after crash
let report = db.recovery_report();
for rec in db.replay_from(report.from_sequence)? {
    apply(rec?);
}
```

Full example: `cargo run --example wal`

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `hash-chain` | off | BLAKE3 keyed hash chain |
| `compression` | off | Streaming zstd compression |
| `encryption` | off | AES-256-GCM encryption |
| `remote-push` | off | Async remote push |

## Testing

```bash
cargo test                          # 103 unit tests
cargo test --features compression   # + compression
cargo test --test fuzz              # property-based (proptest)
cargo +nightly fuzz run <target>    # libfuzzer + ASan
```

## License

MIT OR Apache-2.0

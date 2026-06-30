# logdb

Embedded, append-only, crash-recoverable, optionally tamper-proof local log database. Built in Rust.

## Documentation

Full documentation lives under [`docs/`](docs/). See the [Usage Guide](docs/en/README.md) and the [Development Guide](docs/en/dev/README.md).

API reference: run `cargo doc --open` (also available on docs.rs once published).

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

- **Lock-free writes**: multi-producer CAS ring buffer, p50 < 100ns
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
// report: { from_sequence, to_sequence, count }
for rec in db.replay_from(report.from_sequence)? {
    apply(rec?);
}

// Ops monitoring
let (used, _total) = db.wal_usage();   // WAL space in use
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
cargo test                          # 140+ unit tests (default features)
cargo test --features compression   # + compression
cargo test --test fuzz              # property-based (proptest)
cargo +nightly fuzz run <target>    # libfuzzer + ASan
```

## Architecture

```
Many producer threads
     │ append(content)
     ▼
┌─────────────────────────┐
│  Ring (optionally sharded)  │  ← lock-free CAS claim, inline ≤ 256B = zero alloc
│  Slot: { content, hash } │
└──────────┬──────────────┘
           │
    (optional) Sealer thread  ← BLAKE3 keyed hash chain
           │
    Committer thread          ← batch serialize + pwrite + fdatasync
           │
    ┌──────┴──────┐
    │ Segment file │         ← append-only, rolls when full, checkpoint-truncated
    └─────────────┘
           │
    Reader / Pusher         ← point/range reads / remote push
```

## Performance baseline (SATA SSD, 8-vCPU cloud VM)

| Metric | Value |
|--------|-------|
| append(64B) p50 | 54ns |
| append(256B) p50 | 57ns |
| append(256B) p99 | 230ns |
| append(256B) 1-thread throughput | 3.79M rec/s |
| append(256B) 4-thread throughput | 4.48M rec/s |
| End-to-end durability p99 | 10.4ms (<2ms expected on NVMe) |
| Segment-roll pause | 0ms (pre-allocation + idle drain) |
| Range scan (cr-004, 64B records) | ~90 ns/rec (~12× over the per-record-syscall path) |

## License

MIT OR Apache-2.0. Third-party attributions: [`THIRDPARTY.md`](THIRDPARTY.md).

## Security

Using the `encryption` or `hash-chain` features? Read the
[threat model](docs/en/security/threat-model.md) and
[key management](docs/en/security/key-management.md) first. To report a
vulnerability, see [`../SECURITY.md`](../SECURITY.md).

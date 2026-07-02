# lv-logdb

A cargo workspace for building tamper-proof, append-only audit log infrastructure.

## Crates

| Crate | Path | What it is |
|-------|------|------------|
| [`logdb`](logdb/README.md) | `logdb/` | Embedded append-only log database (library). BLAKE3 hash chain, AES-256-GCM, zstd. |
| [`logdbd`](logdbd/README.md) | `logdbd/` | Clustered gRPC log service. Namespace/stream, per-stream hash chain, primary-standby replication, TLS+auth, consumer groups. |
| [`logdb-exporter`](logdb-exporter/README.md) | `logdb-exporter/` | CDC exporter: Scan + Tail → ClickHouse / stdout. |
| [`logdb-client`](logdb-client/) | `logdb-client/` | Rust SDK: ergonomic async client for logdbd. |

## Quick Start

```bash
# Build everything
cargo build

# Test the workspace
cargo test --workspace

# Start logdbd
cargo run --release -p logdbd -- --config logdbd/logdbd.yaml
```

```rust
// Use the SDK
use logdb_client::Client;

let mut client = Client::connect("127.0.0.1:50051").await?;
let seq = client.append("my-app", "main", "test.event", b"hello").await?;
let rec = client.read("my-app", "main", seq).await?;
```

## Documentation

| Document | Language |
|----------|----------|
| [Getting Started](logdbd/docs/zh/usage/getting-started.md) | 中文 |
| [Configuration Reference](logdbd/docs/zh/usage/configuration.md) | 中文 |
| [Development Guide](logdbd/docs/zh/dev/building.md) | 中文 |
| [Failover Runbook](deploy/failover-runbook.md) | English |
| [Design Spec](veps/logdbd-cluster-design.md) | 中文 |

## Layout

```
lv-logdb/
├── Cargo.toml              # workspace manifest
├── logdb/                  # embedded library (crates.io)
├── logdbd/                 # gRPC service + admin CLI
├── logdb-exporter/         # CDC exporter
├── logdb-client/           # Rust SDK
├── deploy/                 # systemd, alerts, runbook
├── docs/                   # upgrade guide
└── veps/                   # design documents
```

## License

Apache-2.0. See [`deny.toml`](deny.toml) for dependency license policy.

## Security

Report vulnerabilities privately — see [`SECURITY.md`](SECURITY.md).

# logdbd — Clustered Audit Log Database

Append-only, tamper-proof, replicated log service built on [logdb](https://github.com/lv-agent/logdb).

## Features

- **Multi-tenant**: namespace + stream two-level isolation, implicit creation
- **Tamper-proof**: per-stream BLAKE3 hash chain, genesis hash per stream
- **Replication**: primary-standby with sync/async, quorum, configurable timeout
- **Crash recovery**: catalog snapshot + seq→gid rebuilt from logdb on restart
- **Export**: CDC exporter with stdout and ClickHouse sinks
- **Security**: mTLS, bearer token auth, cluster_id/epoch protection
- **Observability**: Prometheus metrics + structured tracing

## Quick Start

```bash
# Install protoc
apt install protobuf-compiler

# Build
cargo build --release -p logdbd

# Start with config
cargo run --release -p logdbd -- --config logdbd/logdbd.yaml
```

## Architecture

```
Agent ──gRPC──→ logdbd (primary)
               │  namespace/stream/catalog
               │  per-stream hash chain
               │  segment files
               │
               ├──→ logdbd (standby)   ← replication
               │
               └──→ logdb-exporter     ← Tail RPC
                       └──→ ClickHouse
```

## Documentation

| Document | Language |
|----------|----------|
| [Usage Guide](docs/zh/usage/getting-started.md) | 中文 |
| [Configuration](docs/zh/usage/configuration.md) | 中文 |
| [Development Guide](docs/zh/dev/building.md) | 中文 |
| [Failover Runbook](../deploy/failover-runbook.md) | English |
| [Design Spec](../veps/logdbd-cluster-design.md) | 中文 |

## gRPC API

| RPC | Caller | Description |
|-----|--------|-------------|
| `Append` | Agent | Write a record (namespace + stream + event_type + content) |
| `BatchAppend` | Agent | Atomic batch write |
| `Read` | Agent/Exporter | Point read by namespace + stream + seq |
| `Scan` | Exporter | Range scan durable records |
| `Tail` | Exporter | Stream new records (server-side streaming) |
| `GetWatermark` | Exporter | Get oldest/durable/replicated seq |
| `ListNamespaces` | Admin | List all namespaces |
| `ListStreams` | Admin | List streams in a namespace |
| `Status` | Admin | Node status, replication lag |
| `VerifyChain` | Admin | Verify hash chain integrity |

## Tools

| Tool | Purpose |
|------|---------|
| `logdbd` | The daemon |
| `logdbd-admin` | CLI management tool |
| `logdb-exporter` | CDC export to external systems |

## License

Apache-2.0

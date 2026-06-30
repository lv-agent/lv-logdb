# logdbd — Clustered Log Service

gRPC service built on [logdb](https://github.com/lv-agent/logdb).

## Prerequisites

- Rust 1.70+
- `protoc` (protobuf compiler): `apt install protobuf-compiler`

## Quick Start

```bash
cargo run --release
# Listening on 0.0.0.0:50051
```

## API

| RPC | Description |
|-----|-------------|
| Append | Write a record |
| Read  | Read by sequence number |
| Scan  | Stream records in range |
| Tail  | Subscribe to new records (consumer-aware) |
| Status | Cluster/node status |

## Configuration

- `LOGDBD_DATA_DIR` — data directory (default: /var/lib/logdbd)
- `HOSTNAME` — node identifier

## Architecture

```
Agent ──gRPC──→ logdbd ──→ logdb (embedded)
                          ├─ Ring / Committer / Segment
                          ├─ Compression (zstd)
                          ├─ Encryption (AES-256-GCM)
                          └─ Hash chain (BLAKE3)
```

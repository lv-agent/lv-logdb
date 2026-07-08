# logdbd — Clustered Audit Log Database

Append-only, tamper-proof, replicated log service built on [logdb](https://github.com/lv-agent/logdb).

## Features

- **Multi-tenant**: namespace + stream two-level isolation, implicit creation
- **Tamper-proof**: per-stream BLAKE3 hash chain, genesis hash per stream
- **Encryption at rest + key rotation**: AES-256-GCM segments; rotate the active
  key with no downtime and no disk-format change (older records stay readable);
  hash chain stays intact across rotations. Keys via the YAML or `${ENV}`.
- **Backup / restore**: file-level disaster-recovery snapshots with checksum +
  `--verify` (re-runs CRC/hash-chain/torn-write recovery on restore).
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

## Encryption & Key Rotation

Segments are encrypted with AES-256-GCM when `storage.encryption.enabled: true`.
Configure one or more keys and which one is active:

```yaml
storage:
  encryption:
    enabled: true
    algorithm: aes-256-gcm
    provider: file                 # file (default) | awskms | vault (out-of-tree)
    keys:
      - key_id: "2026-07"
        key_hex: "${LOGDBD_ENC_KEY_HEX}"   # openssl rand -hex 32; ${ENV} substituted
      - key_id: "2026-05"          # prior key — stays readable after a rotation
        key_hex: "${LOGDBD_ENC_KEY_HEX_OLD}"
    active_key_id: "2026-07"       # encrypts new writes
```

- **Rotate**: add the new key under `keys`, set `active_key_id` to it, keep the
  prior key(s) listed. New writes use the new key; old records still decrypt. No
  restart-of-the-world, no disk-format change.
- **Retire**: drop a key from `keys` — its records become unreadable (intended).
- The `hash_chain` MAC works alongside rotation (the chain key is a stable
  per-shard secret masked on disk, independent of the active key).
- `provider: file` is built in; `awskms` / `vault` are out-of-tree opt-in crates
  — the core `logdb` library never takes a vendor dependency.

## Backup & Restore

File-level disaster recovery for a **stopped** node (backup holds the primary
`active.lock` for its duration):

```bash
# Back up a stopped primary's data dir → snap.logdbbak (+ .sha256 sidecar)
logdbd-admin backup --data-dir /var/lib/logdbd --out snap.logdbbak

# Restore into a fresh dir; --verify reopens the log (CRC + hash chain + torn
# writes). For an encrypted DB, pass --config so verify can resolve the key ring.
logdbd-admin restore --backup snap.logdbbak --data-dir /var/lib/logdbd \
    --verify --config /etc/logdbd/logdbd.yaml
```

Restore verifies the checksum sidecar, refuses to overwrite a non-empty target,
and (with `--verify`) confirms the recovered data opens cleanly.

## Deployment

Docker image and a Helm chart are included for Kubernetes:

```bash
# Container image (multi-stage, non-root, ~151 MB)
docker build -f deploy/docker/Dockerfile -t logdbd:dev .
docker compose -f deploy/docker/docker-compose.yml up       # local dev tryout

# Helm (templates logdbd.yaml from values; TLS/auth via existing Secrets)
helm install logdbd deploy/helm/logdbd -f my-values.yaml
```

See [`deploy/docker/README.md`](../deploy/docker/README.md) for the production
checklist (TLS + auth + persistence + replication).

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

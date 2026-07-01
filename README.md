# lv-logdb

A cargo workspace containing the **logdb** embedded log database and the
**logdbd** clustered gRPC service built on top of it.

## Crates

| Crate | Path | What it is |
|-------|------|------------|
| [`logdb`](logdb/README.md) | `logdb/` | Embedded, append-only, crash-recoverable, optionally tamper-proof / compressed / encrypted local log database (library). |
| [`logdbd`](logdbd/README.md) | `logdbd/` | Clustered log service: a gRPC daemon (tonic) wrapping logdb with Append/Read/Scan/Tail/Status RPCs, primary-standby replication, TLS, and auth. |

## Quick start

```bash
# Build the whole workspace.
cargo build

# Test the library (all features) and the daemon.
cargo test -p logdb --all-features
cargo test -p logdbd
```

See each crate's README for usage, configuration, and feature flags.

## Layout

```
lv-logdb/
├── Cargo.toml          # this workspace manifest
├── logdb/              # the library crate
└── logdbd/             # the gRPC service crate
```

## License

Apache-2.0. Dependency licenses are vetted with `cargo-deny`
([`deny.toml`](deny.toml)); the library's attributions are in
[`logdb/THIRDPARTY.md`](logdb/THIRDPARTY.md).

## Security

Report vulnerabilities privately — see [`SECURITY.md`](SECURITY.md). For the
encryption/hash-chain threat model and key management, see
[`logdb/docs/en/security/`](logdb/docs/en/security/).

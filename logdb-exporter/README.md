# logdb-exporter — CDC Exporter for logdbd

Streams records from logdbd to external systems via Scan + Tail, with
at-least-once semantics and progress persistence.

## Quick Start

```bash
cargo build --release -p logdb-exporter
logdb-exporter exporter.yaml
```

## Sinks

| Sink | Status |
|------|--------|
| `stdout` | Built-in (JSON lines) |
| `clickhouse` | Built-in (HTTP insert + dedup) |

## Configuration

See `exporter.yaml` for a complete example.

Key sections:
- `source` — logdbd addresses, TLS config
- `scope` — which namespace/stream to export
- `sink` — destination type and config
- `progress` — checkpoint file path and interval

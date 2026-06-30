# Usage Guide

This guide takes you from first install to running logdb in production, following the order in which most users learn the library.

Read the pages in order; each builds on the previous one.

1. [Getting started](getting-started.md) — install logdb and write your first records.
2. [Concepts](concepts.md) — the core model: logs, segments, records, indexes.
3. [Writing](writing.md) — append-only writes, batching, and write semantics.
4. [Reading](reading.md) — point lookups, range scans, and ordering guarantees.
5. [Durability](durability.md) — `fsync`, flush policies, and when data is safe.
6. [Recovery](recovery.md) — crash recovery and the redo process.
7. [Tailers](tailers.md) — following the log, tail reads, and live consumers.
8. [Configuration](configuration.md) — segment size, features, and tunables.
9. [Features](features.md) — hash-chain tamper-proofing, compression, encryption.
10. [Sharding](sharding.md) — partitioning data across multiple logs.
11. [Performance](performance.md) — throughput, latency, and benchmarks.
12. [Errors](errors.md) — error types, causes, and recovery actions.
13. [Cookbook](cookbook.md) — recipes for common tasks.

## See also

- [Reading guide home](../README.md)
- [Development Guide](../dev/README.md)

> logdb 0.2.0

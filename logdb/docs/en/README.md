# logdb

logdb is an embedded, append-only, crash-recoverable, optionally tamper-proof local log database for Rust.

This is the documentation home. From here you can reach every page of the **Usage Guide** (for application developers who use logdb) and the **Development Guide** (for contributors who work on logdb itself).

## Usage Guide

Start here if you want to use logdb in your own application.

- [Usage Guide overview](usage/README.md)
  - [Getting started](usage/getting-started.md)
  - [Concepts](usage/concepts.md)
  - [Writing](usage/writing.md)
  - [Reading](usage/reading.md)
  - [Durability](usage/durability.md)
  - [Recovery](usage/recovery.md)
  - [Tailers](usage/tailers.md)
  - [Configuration](usage/configuration.md)
  - [Features](usage/features.md)
  - [Sharding](usage/sharding.md)
  - [Performance](usage/performance.md)
  - [Errors](usage/errors.md)
  - [Cookbook](usage/cookbook.md)

## Development Guide

Read this if you are extending, debugging, or contributing to logdb itself.

- [Development Guide overview](dev/README.md)
  - [Architecture](dev/architecture.md)
  - [Storage format](dev/storage-format.md)
  - [Project layout](dev/project-layout.md)
  - [Building](dev/building.md)
  - [Testing](dev/testing.md)
  - [Extending](dev/extending.md)
  - [Contributing](dev/contributing.md)

## Security

Read this if you use the `encryption` or `hash-chain` features.

- [Security overview](security/README.md)
  - [Threat model](security/threat-model.md)
  - [Key management](security/key-management.md)

## API reference

API reference: run `cargo doc --open` (or docs.rs once published).

## Language

Language: English. ↔ [中文](../zh/README.md)

## See also

- [Usage Guide](usage/README.md)
- [Development Guide](dev/README.md)

> logdb 0.3.0

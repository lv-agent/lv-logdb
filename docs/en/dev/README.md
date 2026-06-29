# Development Guide

This guide is for developers who extend, debug, benchmark, or contribute to logdb itself. If you only want to use logdb in an application, start with the [Usage Guide](../usage/README.md) instead.

The pages are ordered from high-level understanding toward hands-on contribution.

1. [Architecture](architecture.md) — modules, data flow, and key abstractions.
2. [Storage format](storage-format.md) — on-disk segment and index layout.
3. [Project layout](project-layout.md) — what lives where in the source tree.
4. [Building](building.md) — toolchain, features, and build flags.
5. [Testing](testing.md) — unit, integration, property, and fuzz tests.
6. [Extending](extending.md) — adding features, formats, and integrations.
7. [Contributing](contributing.md) — workflow, style, and pull request process.

For the public API surface, run `cargo doc --open`.

## See also

- [Development guide home](../README.md)
- [Usage Guide](../usage/README.md)

> logdb 0.2.0

# Building

How to compile logdb — toolchain requirements, the optional feature matrix, and the release-build helper script.

> Authoritative for **logdb 0.2.0**. Verify against `Cargo.toml` when features or dependencies change.

## Toolchain

logdb builds on **stable Rust**, edition **2021**. There is no special toolchain for normal builds:

```sh
rustc --version     # any recent stable
cargo --version
```

The only path that requires a toolchain beyond stable is **fuzzing**, which uses `cargo +nightly fuzz` (libFuzzer). See [Testing](testing.md#fuzzing).

## Default build

```sh
cargo build                     # debug, default features (none)
cargo build --release           # optimized
```

`default = []` in `Cargo.toml`, so a default build pulls in only the always-on dependencies (`thiserror`, `crc32c`, `libc`, `scopeguard`). The optional features below add capabilities and their crates.

## Feature matrix

All features are opt-in and off by default. They are independent and can be combined freely.

| Feature         | Optional dependencies                              | What it enables                                     |
|-----------------|----------------------------------------------------|-----------------------------------------------------|
| `hash-chain`    | `sha2`, `blake3`                                   | Tamper-evident hash chain per segment (SHA-256 / BLAKE3 keyed). |
| `compression`   | `zstd` (`default-features = false`)                | Zstandard-compressed record frames.                 |
| `encryption`    | `aes-gcm` (with `aes`, `alloc`), `getrandom`       | AES-GCM encrypted record frames; nonce from CSPRNG. |
| `remote-push`   | *(none — feature flag only)*                       | Push-to-remote code path (no extra crates).         |

Combine features with a comma-separated `--features` list:

```sh
cargo build --features "compression,encryption"
cargo build --features "hash-chain"
cargo build --all-features          # hash-chain + compression + encryption + remote-push
```

A `cargo build --all-features` is a useful CI gate — it proves the entire feature matrix compiles together.

## Release helper: `scripts/build.sh`

[`scripts/build.sh`](../../../scripts/build.sh) builds all release binaries used by the qualification and deployment scripts. It changes to the project root and runs:

```sh
cargo build --release --example perf --example soak --example crash_test --example testsuite
```

It prints the `rustc`/`cargo` versions and host target, then lists the resulting binaries under `target/release/examples/`:

- `perf` — performance benchmark binary
- `soak` — soak test binary
- `crash_test` — crash recovery helper binary
- `testsuite` — integrated test suite binary

```sh
./scripts/build.sh                 # build all four release binaries
```

The other scripts (`benchmark.sh`, `soak-test.sh`, `crash-recovery-test.sh`, `run-all.sh`) consume these binaries; see [Testing](testing.md).

## See also

- [Development guide home](README.md)
- [Testing](testing.md) — how the test, fuzz, and benchmark targets are run.
- [Project layout](project-layout.md) — where source, tests, benches, and scripts live.

> logdb 0.2.0

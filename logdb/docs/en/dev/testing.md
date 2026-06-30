# Testing

How logdb is tested — unit tests, integration tests, proptest property tests, libFuzzer fuzz targets, Criterion benchmarks, and the qualification scripts.

> Authoritative for **logdb 0.2.0**. Verify against `Cargo.toml`, `tests/`, `fuzz/`, and `benches/` when the layout changes.

## Unit tests

Unit tests live inline in each module under `src/`, in `#[cfg(test)]` blocks next to the code they exercise. Run them with:

```sh
cargo test                         # all unit + integration + doc tests
cargo test --lib                   # library unit tests only
cargo test storage::format         # filter to a module path
```

The README's "Testing" section cites roughly 140+ unit tests on a default build; treat the exact count as approximate and confirm with `cargo test` output.

## Integration tests

Integration tests live in `tests/` and exercise the public API end to end:

- [`tests/integration.rs`](../../../tests/integration.rs) — full lifecycle: open → append → flush → read → verify → shutdown → recover.
- [`tests/fuzz.rs`](../../../tests/fuzz.rs) — proptest property tests mirroring the libFuzzer targets; covers `deserialize_record`, `segment_header`, and `append_roundtrip`.

```sh
cargo test --test integration      # lifecycle + recovery
cargo test --test fuzz             # proptest property tests
PROPTEST_CASES=100000 cargo test --test fuzz -- --nocapture   # run longer
```

## Feature-gated testing

Because features are additive, test each one in isolation and the full matrix in CI:

```sh
cargo test --features compression
cargo test --features encryption
cargo test --features hash-chain
cargo test --features "compression,encryption"
cargo test --all-features
```

## Fuzzing

Fuzzing uses **libFuzzer** via `cargo-fuzz` and requires the **nightly** toolchain. Targets live in `fuzz/fuzz_targets/` (declared in `fuzz/Cargo.toml`):

| Target (`fuzz/fuzz_targets/…`)   | What it checks                                                        |
|----------------------------------|-----------------------------------------------------------------------|
| `deserialize_record.rs`          | `deserialize_record` on arbitrary bytes never panics.                 |
| `segment_header.rs`             | `SegmentHeader::deserialize` on arbitrary 128-byte buffers never panics. |
| `append_roundtrip.rs`           | Random content survives append → flush → read.                        |

```sh
cargo +nightly fuzz run deserialize_record
cargo +nightly fuzz run segment_header
cargo +nightly fuzz run append_roundtrip
```

For memory-error coverage, run under AddressSanitizer:

```sh
cargo +nightly fuzz run --target x86_64-unknown-linux-gnu append_roundtrip -- -detect_leaks=0
```

The fuzz corpus and artifacts are excluded from version control via [`fuzz/.gitignore`](../../../fuzz/.gitignore) (ignores `target`, `corpus`, `artifacts`, `coverage`).

## Benchmarks

Two benchmark entry points exist:

- [`benches/append_bench.rs`](../../../benches/append_bench.rs) — Criterion benchmarks for append throughput/latency (`harness = false`, registered in `Cargo.toml`).
- [`benches/perf_test.rs`](../../../benches/perf_test.rs) — direct measurement binary, used where Criterion is unreliable (e.g. WSL2 tempdir overhead).

```sh
cargo bench                       # Criterion benchmarks (append_bench)
cargo run --release --example perf   # standalone perf measurement
```

Criterion is configured with `html_reports` and writes its reports under `target/criterion/`.

## Qualification scripts

`scripts/` contains runner scripts for the long-running and bare-metal tests. Most consume the release binaries produced by [`scripts/build.sh`](building.md#release-helper-scriptsbuildsh).

| Script                                | What it does                                                                                   |
|---------------------------------------|------------------------------------------------------------------------------------------------|
| `scripts/build.sh`                    | Builds the release binaries (`perf`, `soak`, `crash_test`, `testsuite`). See [Building](building.md). |
| `scripts/benchmark.sh`                | Runs the performance suite; writes a timestamped log to `OUTPUT_DIR/benchmark-*.log`.         |
| `scripts/crash-recovery-test.sh`      | Repeatedly: append → `kill -9` → recover → verify no data loss above the durable cursor.      |
| `scripts/soak-test.sh`                | Runs the soak binary for a configurable duration (default 3600 s).                            |
| `scripts/run-all.sh`                  | Master qualification runner: unit/integration → benchmark → crash recovery → (optional) soak. |
| `scripts/run-all-deployed.sh`         | Same as `run-all.sh` but against pre-built binaries (no Rust toolchain needed).               |
| `scripts/package.sh`                  | Packages release binaries + scripts into a deployable tarball (`logdb-bench-<target>-<ts>.tar.gz`). |

```sh
./scripts/build.sh
./scripts/run-all.sh                 # full qualification run
./scripts/run-all.sh --soak --soak-duration 86400 --iterations 100
./scripts/run-all-deployed.sh        # on a deployed host, no toolchain
```

## Command summary

| Task                          | Command                                                          |
|-------------------------------|------------------------------------------------------------------|
| Unit tests                    | `cargo test --lib`                                              |
| All tests                     | `cargo test`                                                    |
| Integration tests             | `cargo test --test integration`                                 |
| Property tests                | `cargo test --test fuzz`                                        |
| Feature-gated tests           | `cargo test --features compression` (etc.)                      |
| Full feature matrix           | `cargo test --all-features`                                     |
| Benchmarks (Criterion)        | `cargo bench`                                                   |
| Perf binary                   | `cargo run --release --example perf`                            |
| Fuzz a target                 | `cargo +nightly fuzz run <target>`                              |
| Build release binaries        | `./scripts/build.sh`                                            |
| Master qualification run      | `./scripts/run-all.sh`                                          |
| Crash recovery loop           | `./scripts/crash-recovery-test.sh`                              |
| Soak test                     | `./scripts/soak-test.sh`                                        |
| Benchmark suite               | `./scripts/benchmark.sh`                                        |
| Package tarball               | `./scripts/package.sh`                                          |

## See also

- [Development guide home](README.md)
- [Building](building.md) — toolchain and feature flags referenced by the build/fuzz commands above.
- [Project layout](project-layout.md) — where `tests/`, `fuzz/`, `benches/`, and `scripts/` sit in the tree.

> logdb 0.2.0

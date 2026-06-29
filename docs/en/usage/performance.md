# Performance

What makes logdb fast on the append path, why records ≤ 256 B take a zero-allocation fast path while larger records spill to the heap, how to run the bundled benchmarks, how to read the example baseline (with a hardware disclaimer), and the configuration knobs that move throughput, latency, and point-read cost.

## Contents

- [Inline vs spill: the 256 B boundary](#inline-vs-spill-the-256-b-boundary)
- [Run the benchmarks](#run-the-benchmarks)
- [Reading the example baseline](#reading-the-example-baseline)
- [Tuning knobs](#tuning-knobs)
- [See also](#see-also)

## Inline vs spill: the 256 B boundary

The single most impactful performance fact about logdb is the inline/spill split, defined by `INLINE_CAP` in `src/ring/slot.rs:68`:

```rust
/// Records ≤ 256 bytes are stored directly in the slot with zero allocation
/// and zero extra copy across threads. This is the fast path.
pub const INLINE_CAP: usize = 256;
```

Each ring slot is a fixed-size `SlotInner`. When you `append` a record:

- **≤ 256 B (inline fast path)** — the content is copied directly into a `[u8; 256]` array embedded in the slot. There is **no heap allocation** and **no extra memcpy** beyond that single copy into the slot. p50 is typically < 100 ns.
- **> 256 B (spill path)** — the append thread performs a heap allocation (`Box<[u8]>`) and copies the full content into it. This is the *only* allocation on the append fast path, and it only triggers for records over the boundary.

The cost difference is not subtle. Because the spill path goes through the allocator, its **tail latency** is dramatically higher than inline — the slot source comment records an observed ~80× gap at p99.9 (inline ≈ 500 ns vs spill ≈ 41 µs at 300 B), driven by allocator jitter rather than by logdb's own code. Throughput on the spill path is also ~4× lower than inline.

**Practical guidance:**

- For **latency-sensitive** workloads (anything that cares about p99 / p99.9 — request logs, audit events, metrics samples), keep records ≤ 256 B so they stay on the inline fast path. 256 B comfortably covers a typical JSON log line, audit event, or metrics sample, and the inline array occupies exactly four cache lines, so it is cache-friendly too.
- For **throughput-bound** workloads where a few microseconds of tail latency is acceptable (large event blobs, embedded payloads), spilling is fine — you trade allocator jitter for fewer records-per-batch on the Committer side, but correctness is unaffected.
- The boundary is sharp and exact: a 256-byte record is inline; a 257-byte record spills. If you are trimming a serialized record to fit, target 256 B exactly.

The spill path reverts to inline automatically: when a slot that previously held a spill record is reused for an inline record, the old `Box<[u8]>` is dropped and the slot switches back to the inline array, so a workload with a mix of sizes does not accumulate heap.

## Run the benchmarks

logdb ships three performance assets. Use them in this order — quick example run, full example run, scripted suite.

### `cargo run --example perf` — full standalone suite

`examples/perf.rs` is a self-contained measurement program that prints p50 / p90 / p99 / p99.9 / max latency and throughput for the full pipeline (Committer active) and a ring-only path (no back-pressure). Run it release:

```bash
cargo run --release --example perf
```

It exercises:

- **Scenario A — full pipeline:** single-thread append latency across payload sizes from 0 B to 512 KB, classifying each as `inline` (≤ 256 B) or `spill` (> 256 B); multi-thread throughput at 2 / 4 / 8 threads with 256 B records; Committer batch-efficiency diagnostics explaining the non-monotonic throughput curve.
- **Scenario B — ring-only (1 M iterations, 2 M-slot ring):** measures raw append latency with no Committer back-pressure, so the numbers reflect the producer fast path in isolation. Asserts `n < ring_size` to guarantee no back-pressure.
- **T5 — end-to-end durable latency:** in `Batch` mode with 256 B records, measures the wall-clock time from `append` to `durable_cursor() > id` at 10 ms / 5 ms / 2 ms commit intervals, reported in µs.
- **T7 — segment-roll latency:** with a 4 MB `segment_size` in `Async` mode, classifies append stalls > 500 µs as roll-freeze (committed cursor frozen) vs I/O contention (cursor advancing), and checks the spec target that roll pause < 1000 µs.

The example also prints a clock-calibration line (`Instant::now()` resolution) and an environment detection line (`WSL2`, `Docker`, `KVM VM`, `Linux (bare metal)`, etc.) — read both before comparing numbers, since p50 is measurement-limited at the clock floor.

### `cargo bench` — criterion micro-benchmarks

`benches/append_bench.rs` is a [criterion](https://bheisler.github.io/criterion.rs/book/) benchmark that produces statistical, regression-friendly numbers with HTML reports:

```bash
cargo bench
```

It runs:

- `append/64B/1t`, `append/256B/1t`, `append/1024B/1t` — single-thread append latency at fixed sizes.
- `append-throughput/256B/{1,2,4,8}t-rec/s` — multi-thread throughput with `Throughput::Elements`.
- `append/{0,16,64,128,256,512,1024}B/1t` — a payload-size sweep across the inline/spill boundary.

Results land under `target/criterion/` with criterion's comparison and regression tracking. This is the right tool for *before/after* comparisons during development; `perf` is the right tool for a *one-shot baseline*.

### `scripts/benchmark.sh` — scripted suite

`scripts/benchmark.sh` wraps the `perf` example, recording environment info (hostname, `uname`, `nproc`, memory, disk type via `df -T`, rustc / cargo versions) and writing a timestamped log:

```bash
./scripts/benchmark.sh                 # default: ./benchmark-results/
./scripts/benchmark.sh /tmp/my-bench   # custom output dir
```

It prefers a pre-built binary at `bin/perf` or `target/release/examples/perf`, otherwise runs `cargo build --release --example perf`. The full run takes about 60 seconds. The log file is `OUTPUT_DIR/benchmark-YYYYMMDD-HHMMSS.log`, and the script prints an extracted "Key Metrics" summary (inline 64 B / 256 B, spill 300 B, multi-thread throughput, 10 ms-interval durable latency) at the end.

This is the script to run when you want a captured, reproducible baseline for a given machine.

## Reading the example baseline

The performance table in `README_CN.md` was captured on a **SATA SSD, 8-vCPU cloud VM**. Treat it only as an *example* of what the suite reports on one specific machine:

| Metric | Example value |
|--------|---------------|
| append(64B) p50 | 54 ns |
| append(256B) p50 | 57 ns |
| append(256B) p99 | 230 ns |
| append(256B) single-thread throughput | 3.79 M rec/s |
| append(256B) 4-thread throughput | 4.48 M rec/s |
| end-to-end durable latency p99 | 10.4 ms (NVMe expected < 2 ms) |
| segment-roll pause | 0 ms (pre-allocation + idle drain) |

### Hardware / environment disclaimer

**These numbers are not logdb's "official" performance.** They are a single sample point from one SATA SSD on one 8-vCPU cloud VM. Treat them as a sanity check, not a specification. Your numbers will differ, and the gap can be large, because logdb's append path is fast enough that the bottleneck is usually the storage stack, not the library:

- **Disk media dominates durable latency.** `fdatasync` cost varies by orders of magnitude between a SATA SSD, an NVMe device, and a network-attached volume. The baseline's 10.4 ms durable p99 reflects a SATA SSD; on NVMe the same path is expected to land under ~2 ms, and on a fast NVMe with a write cache much lower still.
- **WSL2 inflates `fdatasync` latency.** WSL2 routes Linux filesystem calls through a 9P / virtio layer to the Windows host, so `fdatasync` is noticeably slower than on native Linux with the same physical disk. If you benchmark under WSL2, durable-latency numbers will be pessimistic; the inline append numbers (which do not touch disk) are much less affected. On WSL2 a `Clean` drain may also be classified as `PartialDurable` conservatively (see [Durability: graceful shutdown](durability.md#graceful-shutdown)).
- **vCPU count caps multi-thread scaling.** The 4-thread throughput in the baseline was measured on an 8-vCPU VM; with more cores the multi-thread numbers scale further until the Committer (a single background thread) becomes the bottleneck.
- **Clock floor limits p50.** `Instant::now()` has a measurable resolution (printed at the top of `perf`'s output). p50 values within a small multiple of that floor are *measurement-limited*, not code-limited — the true append cost is below what the clock can resolve.

When you publish your own numbers, capture them with `scripts/benchmark.sh` and record the environment block (disk type, vCPU count, bare-metal vs WSL2 vs VM) alongside them, exactly as the baseline above does.

## Tuning knobs

All knobs are fields on `Config` (`src/config.rs`) and are validated at `open` time. See [Configuration](configuration.md) for the full reference; these are the ones that matter for performance.

### `index_stride` — point-read scan length

```rust
pub index_stride: u32,  // default 1024
```

The sparse index stores one anchor every `index_stride` records per segment. A point read seeks to the nearest anchor before the target and scans forward, so the scan length is bounded by `index_stride`. **Smaller → faster point reads, larger `.idx` file; larger → smaller index, longer read scan.**

- Default 1024 indexes ~0.02% of records — a good general-purpose choice.
- For **latency-sensitive point reads** (KV / etcd-style workloads), set `index_stride` to 64–256 to shorten the read scan.
- Only affects **raw** segments; compressed and encrypted segments are frame-based and have no per-record sparse index, so this knob is a no-op there (see [Features: compression](features.md#compression)).

### `segment_size` — rollover frequency

```rust
pub segment_size: u64,  // default 256 MB, minimum 1 MB
```

Segment pre-allocation creates the next segment at 80% capacity, so a roll normally reduces to a single `fdatasync` — but a smaller `segment_size` means more frequent rolls and thus more frequent roll-time pauses. The baseline above rolls in ~0 ms because of pre-allocation plus idle drain. Set `segment_size` large enough that rolls are rare under your write rate.

### `durability_mode` — fsync cost vs data-at-risk window

```rust
pub durability_mode: DurabilityMode,  // default Batch
```

`Sync` (fsync every batch) gives the strongest guarantee at the highest per-record latency; `Batch` (default) amortizes fsync across a batch trigger (256 KiB / 1024 records / 10 ms) for high throughput with a bounded data-at-risk window; `Async` (fsync only on explicit `flush` / shutdown) gives the highest throughput and lowest steady-state latency but requires the application to issue its own barriers. See [Durability: choosing a mode](durability.md#choosing-a-mode) for the full trade-off.

### `wait_strategy` — background-thread spin

```rust
pub wait_strategy: WaitStrategy,
// default: spin_count=64, yield_count=16, park_duration=500µs
```

Controls how the background threads (Committer, Sealer) wait for work: spin, then yield, then park. More spinning burns CPU for lower signaling latency; more parking saves CPU at the cost of wake-up latency. The default is a reasonable middle ground for a service that is always writing; for a sporadic-write workload, longer `park_duration` reduces idle CPU.

## See also

- [Usage guide](README.md)
- [Configuration](configuration.md) — every `Config` field and its trade-off.
- [Durability](durability.md) — `durability_mode` and the fsync cost model.
- [Reading](reading.md) — how `index_stride` affects a point read.
- [Errors](errors.md) — what `QueueFull`, `DiskFull`, and the timeout variants mean for throughput.

> logdb 0.2.0

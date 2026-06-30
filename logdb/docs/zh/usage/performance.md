# 性能

logdb 追加路径为什么快、为什么 ≤ 256 B 的记录走零分配快速路径而更大的记录会溢出到堆、如何运行内置基准测试、如何阅读示例基线（含硬件免责声明），以及那些影响吞吐、延迟与点查询成本的配置旋钮。

## 目录

- [Inline 与 Spill：256 B 分界线](#inline-与-spill256-b-分界线)
- [运行基准测试](#运行基准测试)
- [阅读示例基线](#阅读示例基线)
- [可调旋钮](#可调旋钮)
- [相关链接](#相关链接)

## Inline 与 Spill：256 B 分界线

关于 logdb 性能最重要的一点是 inline/spill 的划分，由 `src/ring/slot.rs:68` 的 `INLINE_CAP` 定义：

```rust
/// Records ≤ 256 bytes are stored directly in the slot with zero allocation
/// and zero extra copy across threads. This is the fast path.
pub const INLINE_CAP: usize = 256;
```

每个 ring slot 是一个定长的 `SlotInner`。当你 `append` 一条记录时：

- **≤ 256 B（inline 快速路径）** — 内容被直接拷贝进 slot 内嵌的 `[u8; 256]` 数组。**没有堆分配**，也**没有额外 memcpy**（除了这次拷入 slot 的拷贝）。p50 通常 < 100 ns。
- **> 256 B（spill 路径）** — 追加线程执行一次堆分配（`Box<[u8]>`）并拷入全部内容。这是追加快速路径上**唯一**的分配，且仅在记录超过分界时触发。

代价差异并不微妙。由于 spill 路径要经过分配器，其**尾延迟**远高于 inline —— slot 源码注释记录了 p99.9 处观测到的约 80 倍差距（300 B 时 inline ≈ 500 ns，spill ≈ 41 µs），原因是分配器抖动而非 logdb 自身代码。spill 路径的吞吐也比 inline 低约 4 倍。

**实用建议：**

- 对于**延迟敏感**负载（任何关注 p99 / p99.9 的场景 —— 请求日志、审计事件、指标采样），让记录 ≤ 256 B 以保持在 inline 快速路径上。256 B 足以容纳典型的 JSON 日志行、审计事件或指标采样，且 inline 数组恰好占 4 个 cache line，对缓存友好。
- 对于**吞吐导向**负载（少量微秒的尾延迟可接受 —— 大事件体、内嵌载荷），spill 完全没问题 —— 你用分配器抖动换来了 Committer 侧更少的 records-per-batch，但正确性不受影响。
- 分界是精确且锋利的：256 字节记录走 inline，257 字节记录走 spill。如果你在裁剪序列化记录以塞进 inline，目标就定 256 B 整。

spill 路径会自动回退到 inline：当一个原本持有 spill 记录的 slot 被复用于 inline 记录时，旧的 `Box<[u8]>` 被释放，slot 切回 inline 数组，因此混合大小的负载不会累积堆内存。

## 运行基准测试

logdb 自带三套性能工具。按这个顺序使用 —— 快速示例运行、完整示例运行、脚本化套件。

### `cargo run --example perf` —— 完整独立套件

`examples/perf.rs` 是一个自包含的测量程序，打印 full pipeline（Committer 活跃）和 ring-only（无背压）路径的 p50 / p90 / p99 / p99.9 / max 延迟与吞吐。用 release 运行：

```bash
cargo run --release --example perf
```

它涵盖：

- **场景 A —— full pipeline：** 从 0 B 到 512 KB 各载荷大小的单线程追加延迟，每条按 `inline`（≤ 256 B）或 `spill`（> 256 B）分类；256 B 记录在 2 / 4 / 8 线程下的多线程吞吐；Committer 批处理效率诊断（解释非单调吞吐曲线）。
- **场景 B —— ring-only（100 万次迭代，200 万 slot 的 ring）：** 在没有 Committer 背压的情况下测量原始追加延迟，反映生产者快速路径本身。断言 `n < ring_size` 以保证无背压。
- **T5 —— 端到端持久化延迟：** 在 `Batch` 模式下用 256 B 记录，测量从 `append` 到 `durable_cursor() > id` 的墙上时间，分别在 10 ms / 5 ms / 2 ms 提交间隔下，以 µs 报告。
- **T7 —— 段滚动延迟：** 用 4 MB 的 `segment_size`、`Async` 模式，将 > 500 µs 的追加停顿分类为 roll-freeze（committed 游标冻结）或 I/O contention（游标仍在前进），并检查规格目标：滚动停顿 < 1000 µs。

示例还会打印一行时钟校准（`Instant::now()` 分辨率）和一行环境检测（`WSL2`、`Docker`、`KVM VM`、`Linux (bare metal)` 等）—— 比较数字前请先读这两行，因为 p50 在时钟下限处是受测量限制的。

### `cargo bench` —— criterion 微基准

`benches/append_bench.rs` 是一个 [criterion](https://bheisler.github.io/criterion.rs/book/) 基准，产出带统计、便于回归跟踪的数字和 HTML 报告：

```bash
cargo bench
```

它运行：

- `append/64B/1t`、`append/256B/1t`、`append/1024B/1t` —— 固定大小下的单线程追加延迟。
- `append-throughput/256B/{1,2,4,8}t-rec/s` —— 用 `Throughput::Elements` 的多线程吞吐。
- `append/{0,16,64,128,256,512,1024}B/1t` —— 横跨 inline/spill 分界的载荷大小扫描。

结果在 `target/criterion/` 下，附带 criterion 的对比与回归跟踪。这是开发期做*前后对比*的合适工具；`perf` 是做*一次性基线*的合适工具。

### `scripts/benchmark.sh` —— 脚本化套件

`scripts/benchmark.sh` 包装了 `perf` 示例，记录环境信息（主机名、`uname`、`nproc`、内存、`df -T` 磁盘类型、rustc / cargo 版本）并写入带时间戳的日志：

```bash
./scripts/benchmark.sh                 # 默认：./benchmark-results/
./scripts/benchmark.sh /tmp/my-bench   # 自定义输出目录
```

它优先使用 `bin/perf` 或 `target/release/examples/perf` 上的预编译二进制，否则执行 `cargo build --release --example perf`。完整运行约 60 秒。日志文件为 `OUTPUT_DIR/benchmark-YYYYMMDD-HHMMSS.log`，脚本末尾打印提取出的“关键指标”摘要（inline 64 B / 256 B、spill 300 B、多线程吞吐、10 ms 间隔的持久化延迟）。

当你想为某台机器抓取一份可复现的基线时，就用这个脚本。

## 阅读示例基线

`README_CN.md` 中的性能表是在 **SATA SSD、8 vCPU 云 VM** 上抓取的。把它仅当作“这套测试程序在某一台具体机器上的示例输出”：

| 指标 | 示例数值 |
|------|----------|
| append(64B) p50 | 54 ns |
| append(256B) p50 | 57 ns |
| append(256B) p99 | 230 ns |
| append(256B) 单线程吞吐 | 3.79 M rec/s |
| append(256B) 4 线程吞吐 | 4.48 M rec/s |
| 端到端持久化延迟 p99 | 10.4 ms（NVMe 预期 < 2 ms） |
| 段滚动停顿 | 0 ms（预分配 + idle drain） |

### 硬件 / 环境免责声明

**这些数字并不是 logdb 的“官方性能”。** 它们只是某一台 SATA SSD、8 vCPU 云 VM 上的一个样本点。请把它们当作合理性检查，而不是规格。你的数字会不同，而且差距可能很大 —— 因为 logdb 的追加路径已经快到瓶颈通常在存储栈、而不在库本身：

- **磁盘介质主导持久化延迟。** `fdatasync` 的代价在 SATA SSD、NVMe 设备和网络卷之间相差好几个数量级。基线里的 10.4 ms 持久化 p99 反映的是 SATA SSD；在 NVMe 上同一路径预期落在 ~2 ms 以内，在带写缓存的快速 NVMe 上更低。
- **WSL2 会抬高 `fdatasync` 延迟。** WSL2 把 Linux 文件系统调用经 9P / virtio 层转发到 Windows 宿主，因此 `fdatasync` 比同一块物理盘上的原生 Linux 明显更慢。若在 WSL2 下基准测试，持久化延迟的数字会偏悲观；而 inline 追加数字（不触盘）受影响小得多。在 WSL2 上，`Clean` 的 drain 也可能被保守地分类为 `PartialDurable`（见[持久化：优雅关闭](durability.md#优雅关闭)）。
- **vCPU 数量限制多线程扩展。** 基线中的 4 线程吞吐是在 8 vCPU VM 上测的；核数更多时多线程数字会继续扩展，直到 Committer（单一后台线程）成为瓶颈。
- **时钟下限限制 p50。** `Instant::now()` 有可测的分辨率（在 `perf` 输出顶部打印）。p50 值若在该下限的数倍以内，是*受测量限制*的，而非代码限制 —— 真实追加代价低于时钟能分辨的下限。

当你发布自己的数字时，请用 `scripts/benchmark.sh` 抓取，并把环境块（磁盘类型、vCPU 数、裸机 vs WSL2 vs VM）一并记录，就像上面的基线那样。

## 可调旋钮

所有旋钮都是 `Config`（`src/config.rs`）的字段，在 `open` 时校验。完整参考见[配置](configuration.md)；这里只列出对性能有影响的。

### `index_stride` —— 点查询扫描长度

```rust
pub index_stride: u32,  // 默认 1024
```

稀疏索引在每个段内每 `index_stride` 条记录存一个锚点。一次点查询先 seek 到目标之前的最近锚点，再向前扫描，因此扫描长度被 `index_stride` 限定。**更小 → 点查询更快、`.idx` 文件更大；更大 → 索引更小、读扫描更长。**

- 默认 1024 索引约 0.02% 的记录 —— 通用的合理选择。
- 对于**延迟敏感的点查询**（KV / etcd 风格负载），把 `index_stride` 设为 64–256 以缩短读扫描。
- 只影响 **raw** 段；压缩和加密段是 frame-based，没有逐记录稀疏索引，因此这个旋钮对它们无效（见[特性：压缩](features.md#compression)）。

### `segment_size` —— 滚动频率

```rust
pub segment_size: u64,  // 默认 256 MB，最小 1 MB
```

段预分配在 80% 容量时就建好下一段，因此一次滚动通常只剩一次 `fdatasync` —— 但更小的 `segment_size` 意味着更频繁的滚动，因此更频繁的滚动停顿。上面基线的滚动是 ~0 ms，得益于预分配加 idle drain。把 `segment_size` 设得足够大，使你的写入速率下滚动很少发生。

### `durability_mode` —— fsync 代价 vs 数据风险窗口

```rust
pub durability_mode: DurabilityMode,  // 默认 Batch
```

`Sync`（每批都 fsync）保证最强但每条记录延迟最高；`Batch`（默认）在批触发（256 KiB / 1024 条记录 / 10 ms）时摊销 fsync，吞吐高且有界的数据风险窗口；`Async`（仅在显式 `flush` / 关闭时 fsync）吞吐最高、稳态延迟最低，但要求应用自行发出屏障。完整权衡见[持久化：如何选择模式](durability.md#如何选择模式)。

### `wait_strategy` —— 后台线程自旋

```rust
pub wait_strategy: WaitStrategy,
// 默认：spin_count=64, yield_count=16, park_duration=500µs
```

控制后台线程（Committer、Sealer）如何等待工作：先自旋，再 yield，再 park。更多自旋以更高 CPU 换更低唤醒延迟；更多 park 以更低 CPU 换更高唤醒延迟。默认对持续写入的服务是合理的折中；对突发写入负载，更长的 `park_duration` 能降低空闲 CPU。

## 相关链接

- [使用指南](README.md)
- [配置](configuration.md) —— 每个 `Config` 字段及其权衡。
- [持久化](durability.md) —— `durability_mode` 与 fsync 代价模型。
- [读取](reading.md) —— `index_stride` 如何影响一次点查询。
- [错误处理](errors.md) —— `QueueFull`、`DiskFull` 与各超时变体对吞吐意味着什么。

> logdb 0.2.0

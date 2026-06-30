# 配置

logdb 的每一项可调参数：完整的 `Config` 字段参考、构造时强制执行的校验规则，以及四个最常见旋钮（index 步长、segment 大小、持久化模式、保留策略）的具体配方。

## 目录

- [构建一个 Config](#构建一个-config)
- [字段参考](#字段参考)
- [校验规则](#校验规则)
- [调参配方](#调参配方)
- [相关链接](#相关链接)

## 构建一个 Config

`Config` 是字段全部公开的普通结构体。从 `Config::default()` 起步，按需覆盖字段，再传给 `LogDb::open`——后者会调用 [`Config::validate()`](#校验规则)，拒绝任何违反硬约束的配置：

```rust
use logdb::Config;
use logdb::config::{DurabilityMode, RetentionPolicy};
use std::time::Duration;

let config = Config {
    data_dir: "/var/lib/logdb".into(),
    segment_size: 512 * 1024 * 1024,    // 512 MiB 段
    durability_mode: DurabilityMode::Sync,
    retention: RetentionPolicy::MaxAge(Duration::from_secs(60 * 60 * 24 * 7)),
    index_stride: 128,                  // 更密的索引以加速点读
    ..Config::default()
};

let db = logdb::LogDb::open(config)?; // validate() 在 open() 内运行
```

下文每个字段都有注明的默认值（出自 `impl Default for Config`，`src/config.rs:158-183`）与约束（出自 `Config::validate`，`src/config.rs:190-223`）。默认值对大多数负载都是安全且合理的——只有有理由时才去改动。

## 字段参考

`Config` 的全部 20 个字段（`src/config.rs:94-156`）。默认值出自 `impl Default`（`src/config.rs:158-183`）；约束出自 `validate`（`src/config.rs:190-223`）。

| # | 字段 | 类型 | 默认值 | 约束 | 说明 |
|---|------|------|--------|------|------|
| 1 | `data_dir` | `PathBuf` | `./logdb_data` | — | segment 文件、索引和元数据的目录，必须可写。 |
| 2 | `segment_size` | `u64` | `256 MiB` | `>= 1 MiB` | 单个 segment 文件的最大字节数，超出即滚动。越大文件越少越大；越小滚动越频繁。 |
| 3 | `ring_size` | `usize` | `8192` | 必须是 2 的幂**且** `>= 16` | 每个 ring 的槽数。越大越能吸收更大的写入突发，再触发队列策略。 |
| 4 | `shards` | `usize` | `1` | `[1, 256]` | 独立 ring 数。提升写入并行度（每个 shard 各有 ring）。 |
| 5 | `max_content_size` | `usize` | `1 MiB` | `<= 64 MiB` | 单条记录内容长度的硬上限，超出会被拒绝。 |
| 6 | `hash_enabled` | `bool` | `false` | 需 feature `hash-chain`；仅单 shard（`shards == 1`） | 追加 SHA-256 哈希链以提供防篡改能力。 |
| 7 | `compression_enabled` | `bool` | `false` | 需 feature `compression` | 对 segment 帧做流式 zstd 压缩。 |
| 8 | `encryption_key` | `Option<[u8;32]>` | `None` | 需 feature `encryption` | 256 位静态加密密钥；`None` 即明文。 |
| 9 | `durability_mode` | `DurabilityMode` | `Batch` | — | `Sync` / `Batch` / `Async`。见[选择持久化模式](#选择持久化模式)。 |
| 10 | `io_backend` | `IoBackend` | `Pwrite` | — | `Pwrite` 是唯一已实现的后端；`IoUring` 保留。 |
| 11 | `queue_full_policy` | `QueueFullPolicy` | `Block` | — | `Block`（自旋 + 退避）或 `Drop`（返回 `AppendError::QueueFull`）。 |
| 12 | `wait_strategy` | `WaitStrategy` | spin 64 / yield 16 / park 500 µs | — | 后台线程的自旋/让步/挂起周期。见 [WaitStrategy](#waitstrategy)。 |
| 13 | `index_stride` | `u32` | `1024` | `>= 1`；仅 raw segment | 每 `index_stride` 条记录打一个稀疏索引锚点。越小点读越快、`.idx` 越大。 |
| 14 | `flush_timeout` | `Duration` | `30 s` | — | `flush()` 等待 Committer 同步的超时时间。 |
| 15 | `retention` | `RetentionPolicy` | `KeepAll` | — | `KeepAll` / `MaxBytes(u64)` / `MaxAge(Duration)`。见[保留策略](#保留策略)。 |
| 16 | `remote_endpoint` | `Option<String>` | `None` | — | 可选的远程推送复制 URL；`None` 关闭推送器。 |
| 17 | `push_batch_size` | `usize` | `1024` | — | 每次推送到远端的记录数。 |
| 18 | `push_progress_interval` | `u32` | `10` | — | 每隔多少批次保存一次推送进度。 |
| 19 | `push_max_retries` | `u32` | `0` | — | 最大重试次数；`0` 即无限重试。 |
| 20 | `push_retry_base` | `Duration` | `1 s` | 上限 `60 s` | 指数退避的基础延迟。 |

### 枚举与子结构体参考

非标量字段用到的类型（`src/config.rs:8-90`）：

```rust
pub enum QueueFullPolicy { Block, Drop }

pub enum DurabilityMode {
    Sync,   // 每个提交批次后都 fdatasync
    Batch,  // 达到批次大小/时间阈值时 fdatasync
    Async,  // 仅在显式 flush() 或关停时 fdatasync
}

pub enum IoBackend { Pwrite /* IoUring 保留 */ }

pub enum RetentionPolicy {
    KeepAll,
    MaxBytes(u64),
    MaxAge(Duration),
}

pub struct WaitStrategy {
    pub spin_count: u32,        // 让步前的自旋次数
    pub yield_count: u32,       // 挂起前的让步次数
    pub park_duration: Duration, // 挂起时长
}

pub struct CommitTrigger {       // Committer 阈值（不直接挂在 Config 上）
    pub bytes: usize,
    pub records: usize,
    pub interval: Duration,
    pub durability: DurabilityMode,
}
```

## 校验规则

`Config::validate()`（`src/config.rs:190-223`）在 `LogDb::open` 内运行，返回描述首个违约的错误。它强制执行的就是以下规则：

1. **`ring_size` 是 2 的幂且 `>= 16`。** 2 的幂让 ring 用位掩码回卷代替取模；下限 16 保证 ring 足够深以摊薄竞争。`ring_size = 100` 与 `ring_size = 8` 都会被拒。
2. **`shards ∈ [1, 256]`。** 零 shard 没有意义；上限则让 manifest 簿记有界。
3. **`segment_size >= 1 MiB`。** 更小的 segment 浪费每段的头部/索引开销，且滚动过于频繁。
4. **`max_content_size <= 64 MiB`。** 超过此值，内联 vs spill 布局与帧格式都不再适用。
5. **`index_stride >= 1`。** `0` 意为“永不索引”，会让每次点读退化为全扫描，故拒绝。

```rust
pub fn validate(&self) -> Result<(), String> {
    if !self.ring_size.is_power_of_two() || self.ring_size < 16 { /* … */ }
    if self.shards < 1 || self.shards > 256 { /* … */ }
    if self.segment_size < 1 * 1024 * 1024 { /* … */ }
    if self.max_content_size > 64 * 1024 * 1024 { /* … */ }
    if self.index_stride == 0 { /* … */ }
    Ok(())
}
```

`validate` **不**检查的东西值得注意：feature 开关（不开 `hash-chain` feature 却把 `hash_enabled` 置 `true`，是在编译期而非 `validate` 时失败）、`hash-chain` 隐含单 shard 的规则（在 open 期间单独强制）、以及任何 `arena_size` 类约束（content arena 已移除——按设计不再有 `ring_size * max_content_size` 的乘积约束）。

## 调参配方

### 为延迟敏感的点读降低 `index_stride`

每个 raw segment 的稀疏索引每 `index_stride` 条记录存一个偏移锚点；点读定位到目标 id 之前最近的锚点，然后顺序前扫（`src/reader/mod.rs`，见[读取：一次点读是如何定位记录的](reading.md#一次点读是如何定位记录的)）。步长越小 → 前扫越短 → 读延迟越低，代价是 `.idx` 文件更大（≈ `records / stride` 个 8 字节条目）。

```rust
// KV / etcd 风格负载：随机点读为主，要压低 p99 延迟。
let config = Config {
    index_stride: 128, // 比默认 1024 密 8 倍
    ..Config::default()
};
```

经验值：

- `1024`（默认）——通用；约 0.1% 记录被索引，前扫平均 512 条。
- `64–256`——延迟敏感点读（KV、缓存、查找索引）。`.idx` 更大，定位更快。
- `4096+`——重写 / 重扫描负载，很少点读；索引更小，点读扫描更长。

`index_stride` 只影响 **raw** segment——压缩或加密的 segment 是基于帧的，没有每记录稀疏索引，此旋钮在那里无效。

### 按 滚动频率选 `segment_size`

`segment_size` 限制 segment 文件字节数；写跨过边界即滚动 segment。权衡：

- **更大的 segment**（512 MiB – 1 GiB）——文件更少、每段开销更低、稀疏索引摊薄更好；适合高吞吐追加负载。保留粒度更粗（`MaxBytes`/`MaxAge` 最小只能以 1 GiB 段为单位丢弃）。
- **更小的 segment**（64–128 MiB）——保留粒度更细、segment manifest 预热更快、更易单段拷贝/归档；适合保留比纯吞吐更重要的场景。

```rust
// 磁盘充裕、追求吞吐：更大的 segment。
let config = Config {
    segment_size: 1024 * 1024 * 1024, // 1 GiB
    ..Config::default()
};
```

`segment_size` 必须保持 `>= 1 MiB`（见[校验规则](#校验规则)）。

### 选择持久化模式

`DurabilityMode` 控制 Committer 何时调用 `fdatasync`（`src/config.rs:17-26`）：

| 模式 | `fdatasync` 时机 | 延迟 | 吞吐 | 崩溃窗口 |
|------|------------------|------|------|----------|
| `Sync` | 每个提交批次之后 | 最高 | 最低 | 无——每条已提交记录都已落盘。 |
| `Batch` | 达到批次的字节/记录/时间阈值时 | 中 | 高 | 受批次触发间隔约束（默认约 10 ms / 256 KiB / 1024 条）。 |
| `Async` | 仅在显式 `flush()` 或关停时 | 最低 | 最高 | 取决于两次 `flush()` 的间隔——未提交记录可能丢失。 |

```rust
use logdb::config::DurabilityMode;

// 金融 / 元数据日志：绝不丢失已提交记录。
let config = Config { durability_mode: DurabilityMode::Sync, ..Config::default() };

// 高吞吐事件日志：崩溃时容忍丢失约 10 ms。
let config = Config { durability_mode: DurabilityMode::Batch, ..Config::default() };

// 批量导入 / 重放：吞吐最高，在检查点显式 flush。
let config = Config { durability_mode: DurabilityMode::Async, ..Config::default() };
```

这与 [durable 游标](reading.md#可见性与-durable-游标)相互作用：读取者与 [tailer](tailers.md) 只能看到 Committer 已同步的记录，因此 `Sync` 模式也把读后写的可见性间隙压到最小。

### 保留策略

`RetentionPolicy` 控制如何丢弃旧 segment 以限制磁盘占用（`src/config.rs:36-45`）：

- `KeepAll`（默认）——永不删除。磁盘无界增长；只适合写入有界的负载或保留由外部处理的场景。
- `MaxBytes(u64)`——当已封存 segment 的总字节超过上限时，丢弃最旧的已封存段。磁盘占用有界；保留粒度为一段。
- `MaxAge(Duration)`——丢弃超过阈值的已封存段。基于时间的合规（例如“保留 7 天”）。

```rust
use logdb::config::RetentionPolicy;
use std::time::Duration;

// 磁盘上限 50 GiB。
let config = Config {
    retention: RetentionPolicy::MaxBytes(50 * 1024 * 1024 * 1024),
    ..Config::default()
};

// 保留 7 天。
let config = Config {
    retention: RetentionPolicy::MaxAge(Duration::from_secs(60 * 60 * 24 * 7)),
    ..Config::default()
};
```

保留策略只作用于**已封存** segment——当前打开的 segment 即便技术上符合条件也永不删除。这就是为什么更小的 `segment_size` 能带来更细粒度的保留（segment 封存得更频繁）。

### WaitStrategy

`WaitStrategy` 调参 Committer 在无工作时的自旋/让步/挂起周期（`src/config.rs:47-66`）：

```rust
pub struct WaitStrategy {
    pub spin_count: u32,        // 让步前的自旋次数（默认 64）
    pub yield_count: u32,       // 挂起前的让步次数（默认 16）
    pub park_duration: Duration, // 挂起间隔（默认 500 µs）
}
```

- **高吞吐、CPU 充裕**——多自旋、少挂起：`WaitStrategy { spin_count: 1024, yield_count: 64, park_duration: Duration::from_micros(100), ..WaitStrategy::default() }`。用 CPU 换负载下的更低提交延迟。
- **延迟可容忍、省 CPU**——更早挂起：`WaitStrategy { spin_count: 16, yield_count: 8, park_duration: Duration::from_millis(2), ..WaitStrategy::default() }`。空闲时让出核心。

默认值（spin 64 / yield 16 / park 500 µs）是均衡的中间路线。仅当 profiling 显示 Committer 要么在空闲时烧 CPU、要么给尾部延迟加了可见负担时，再去调它。

## 诊断

两个只读访问器用于监控与容量规划：

- `wal_usage() -> (u64, u64)` —— `(已用字节, segment_size)`。`已用字节` 是数据目录下所有段文件的总大小（`shards == 1` 为扁平目录，`shards > 1` 为各 `s<shard>/` 子目录之和）。`segment_size` 是配置的单段滚动阈值。
- `ring_size() -> usize` —— 所有分片的内存 ring 总容量（`num_shards * 每分片槽位`）。用于估算内存与反压行为。

```rust
let (used, seg_size) = db.wal_usage();
println!("WAL on disk: {} bytes (segment size {})", used, seg_size);
println!("ring capacity: {} slots", db.ring_size());
```

## 相关链接

- [使用指南](README.md)
- [写入](writing.md)——`queue_full_policy` 与 `max_content_size` 在追加路径上如何体现。
- [读取](reading.md)——`index_stride` 与 segment manifest 如何塑造点读延迟。
- [持久性](durability.md)——`DurabilityMode` 与 `flush_timeout` 背后的完整故事。
- [恢复](recovery.md)——各持久化模式下什么能在崩溃中幸存。
- [Tailer](tailers.md)——依赖 `durability_mode` 的、由 durable 游标驱动的读取。

> logdb 0.2.0

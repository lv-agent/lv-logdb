# 扩展 logdb

如何扩展 logdb：添加 feature flag、将 logdb 嵌入长驻服务、接入远程推送（remote-push）流水线，以及每项扩展都必须遵守的耐久性护栏。

> 对 **logdb 0.2.0** 具权威性。源码变更时请对照 `Cargo.toml`、`src/lib.rs`、`src/pusher.rs`、`src/config.rs` 核对。

## 添加 feature flag

logdb 使用可叠加（additive）的 Cargo feature（见 [`Cargo.toml`](../../../Cargo.toml) 的 `[features]`）：

```toml
[features]
default = []
hash-chain  = ["sha2", "blake3"]
compression = ["zstd"]
encryption  = ["aes-gcm", "getrandom"]
remote-push = []
```

feature 必须是**可叠加的**——开启它绝不能改变默认构建的行为。以新增一个 `metrics` feature 为例，完整步骤如下：

1. **在 `Cargo.toml` 中声明。** 添加 feature 及其可选依赖：
   ```toml
   [dependencies]
   metrics = { version = "0.22", optional = true }

   [features]
   metrics = ["dep:metrics"]
   ```
2. **用 `#[cfg(feature = "...")]` 门控代码。** 实现和调用处都要门控。代码库各处都遵循这一模式，例如 `src/lib.rs:174`（`hash-chain`）、`src/lib.rs:86`（`remote-push`）、`src/storage/mod.rs:283`（`encryption`）：
   ```rust
   #[cfg(feature = "metrics")]
   fn record_metric(&self, name: &str, value: u64) { /* ... */ }

   // 调用处：
   #[cfg(feature = "metrics")]
   self.record_metric("append", 1);
   ```
3. **写文档。** 在 dev README 的 feature 列表和相关 usage 页面中登记，让用户能发现它。
4. **添加 feature 门控的测试。** 带同样 `#[cfg(feature = "...")]` 属性的测试只在该 feature 开启时运行。CI 中要同时验证开/关两条路径：
   ```sh
   cargo test --features metrics        # 开启 feature
   cargo test                           # 关闭 feature（默认构建仍必须通过）
   cargo test --all-features            # 完整矩阵
   ```

完整 feature 矩阵命令见 [Testing / Feature-gated testing](testing.md#feature-gated-testing)，feature 与构建标志的映射见 [Building](building.md)。

## 嵌入长驻服务

把 logdb 嵌入服务的公开、受支持方式是：持有一个 `Arc<LogDb>`，并使用专门的生命周期方法。

### 用 `drain(timeout)` 优雅关闭

`LogDb::drain(&self, timeout: Duration)`（定义于 `src/lib.rs:574`）是服务受支持的优雅关闭路径，分两个阶段：

1. **Drain（排空）**——拒绝新 append（`start_drain`），等待所有在途 append 发布完成（在途计数降为 0）。
2. **Flush（落盘）**——请求 flush 到最大 producer cursor，并等待直到 durable（或超时）。

若在超时内全部 durable，返回 `ShutdownReport::Clean`；若超时，返回 `ShutdownReport::PartialDurable`。超时时它会中止关闭并返回 `Err(FlushError::Timeout)`。

```rust
use std::sync::Arc;
use std::time::Duration;
use logdb::LogDb;

let db = Arc::new(LogDb::open(config)?);

// ……服务运行，通过 Arc<LogDb> 进行 append ……

// 收到 SIGTERM / 服务停止时：
match Arc::clone(&db).drain(Duration::from_secs(30)) {
    Ok(report) => log::info!("logdb drained: {:?}", report),
    Err(e)     => log::warn!("logdb drain failed: {:?}", e),
}
```

由于 `drain` 取 `&self`，每个服务任务都能共享同一个 `Arc<LogDb>`，无需额外 `Mutex`——append、flush、read、drain 全部经由此共享句柄。

### 用 `replicate` 进行备机写入

`LogDb::replicate(&self, sequence, timestamp_ns, content)`（定义于 `src/lib.rs:326`）是 **standby（备机）** 副本受支持的写入路径。它按显式的 `(sequence, timestamp_ns)` 写入一条产自他处的记录：

- 要求 `shards == 1`（复制是一条线性流，落在 shard 0 上）。
- **幂等**——若记录的 `sequence` 已落后于 producer cursor，返回 `Ok(())`。
- **保序**——`sequence` 必须正好是下一个期望槽位，否则报错。
- 与普通 append 一样执行 `max_content_size` 与健康检查。

这是构建消费复制流的 follower/standby 时使用的 API，而非服务于本地写入者。

## 远程推送模式（内部）

logdb 自带一个 Pusher，把 durable 记录复制到用户提供的远端。Pusher 维护**自己的** `push_cursor`，与 durable cursor 相互独立，并将其持久化到受 CRC 保护的进度文件（`pusher_progress.dat`）。它只读取已 fsync 的记录，分批推送，并在失败时执行指数退避。**远端失败绝不反压本地 append**（原则 ⑥）。

相关类型位于 [`src/pusher.rs`](../../../src/pusher.rs)：

- **`RemoteSink` trait**（`src/pusher.rs:25`）——用户实现的远端接收方：
  ```rust
  pub trait RemoteSink: Send + 'static {
      fn push_batch(&mut self, records: &[Record]) -> Result<(), PushError>;
  }
  ```
- **`PushError`**（`src/pusher.rs:36`）——告诉 Pusher 如何应对：
  - `PushError::Retriable(String)`——瞬时失败；Pusher 以指数退避重试（`config.push_retry_base * 2^attempt`，上限 60 s、上限为 base 的 `2^6 = 64×`）。
  - `PushError::Fatal(String)`——不可恢复；Pusher 停止。
- **`run_pusher`**（`src/pusher.rs:141`）——循环本身：
  ```rust
  pub fn run_pusher(
      data_dir: PathBuf,
      ring: Arc<Ring>,
      sink: Box<dyn RemoteSink>,
      config: Config,
      shutdown: Arc<ShutdownState>,
  )
  ```
- **`PusherHandle::spawn`**（`src/pusher.rs:249`）——在专用命名线程（`logdb-pusher`）上启动 Pusher；`join()` 停止它；`push_cursor()` 读取当前游标；`Drop` 自动 join。

Pusher 读取 durable 记录，成功后推进 `push_seq`，并按 `config.push_progress_interval` 批次（以及退出时）持久化进度，采用原子写模式（`tmp → fdatasync → rename → sync_dir`），对 8 字节序列号做 CRC32C。

### 已知缺口：这是内部的，并非公开 API

> `pusher` 模块是**私有的**——`src/lib.rs:37` 声明的是 `mod pusher;`（而非 `pub mod pusher;`）。因此 `RemoteSink`、`run_pusher`、`PusherHandle`、`PushError` **在 crate 之外不可达**。当前 `LogDb` 上**没有公开的推送 API**。

本节内容仅面向两类读者：

- **logdb 自身的开发者**——新增 sink、调优退避，或在开启 `remote-push` feature 的构建中接入 Pusher。
- **构建常驻守护进程、以 path/git 依赖链接 logdb 从而能触及内部模块的人**——*不*适用于消费已发布 API 的下游 crate。

公开推送 API 是一项**已知缺口**，需要单独的设计文档（`veps/cr-NNN-…`）。切勿随意把这些类型暴露出去；参见 [Contributing](contributing.md)。

> **分片：**Pusher 目前是**单分片**的——`run_pusher` 只接收一个 `Ring` 和一个 `data_dir`，并只跟踪单个 `push_seq`。因此 `shards > 1` 加远程推送**目前不支持**。在设计公开推送 API 时，必须把 Pusher 跨分片化（每分片推送进度 + 合并批次交付，与 tailer 的模型一致）；在此之前不要把推送公开化。

## 耐久性护栏：宜 / 忌

任何触及游标、盘上结构或 append 路径的扩展，都必须遵守以下不变量。

### 宜

- **遵守游标/水位线不变量。** producer cursor、durable cursor、（若存在）push cursor 都是单调的，且仅在成功时推进。一个 durable-cursor 值意味着截至该序列号的每条记录都已 fsync 且可读。
- **对元数据使用原子写模式。** 写到临时文件 → `fdatasync` → `rename` → 对父目录 `sync_dir`。Pusher 的 `save_progress`（`src/pusher.rs:81`）是标准范例。跳过目录同步会在崩溃时丢失 rename。
- **对盘上结构做 CRC 保护。** 每个耐久结构（segment、index 条目、pusher 进度文件）都对其字节带 CRC32C，不匹配即视为损坏。新的盘上格式必须同样处理。
- **遵守健康自愈契约。** 健康检查（`health::check()`）门控 append 与复制；把非零健康码（`HEALTH_DISK_FULL` 等）当作硬停止而非警告，交由自愈路径清除。
- **保持 append 快路径无锁。** 发布者直接使用 ring buffer；耐久性工作发生在 append 路径之外的 Committer/Flusher 线程上。

### 忌

- **不要阻塞 append 快路径。** append 把记录发布到 ring 即返回；绝不在 `append` 的调用线程上做 fsync、网络或重分配。
- **不要破坏 durable-cursor 可见性。** 绝不把 durable cursor 推进到尚未 fsync 且不可读的序列号——reader 与 Pusher 都依赖“durable ⇒ 可读”。
- **不要在缺少目录同步的情况下写元数据。** 仅靠 `rename` 在崩溃时并不耐久；必须对父目录 `sync_dir`。
- **不要让远端失败反压本地 append。** Pusher 有意把远端失败与本地写路径隔离开；任何新集成都必须保留这种隔离。

## 相关链接

- [开发指南首页](README.md)
- [Building](building.md)——工具链与完整 feature 矩阵。
- [Architecture](architecture.md)——Committer/Flusher/Pusher 的拆分，以及上述护栏不变量的来源。
- [Storage format](storage-format.md)——护栏所引用的、受 CRC 保护的盘上布局。

> logdb 0.2.0

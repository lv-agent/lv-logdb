# 持久化

在 logdb 中“durable（已持久化）”的确切含义、`flush` 如何作为持久化屏障、三种 `DurabilityMode` 策略及其吞吐/延迟权衡，以及如何安全关闭而不丢数据。

## 目录

- [“durable”的含义](#durable的含义)
- [flush：持久化屏障](#flush持久化屏障)
- [持久化模式](#持久化模式)
- [如何选择模式](#如何选择模式)
- [优雅关闭](#优雅关闭)
- [崩溃保证](#崩溃保证)

## “durable”的含义

一条记录从 `append` 到落盘，会经历三个阶段：

1. **已发布（Published）** —— `append` 已占槽位、写入内容，并把它标记为对后台 Committer 可见。此时记录仅存在于内存。
2. **已提交（Committed）** —— Committer 已通过 `pwrite` 把记录写入段文件，但**尚未**执行 `fdatasync`。掉电时能否到达磁盘并不确定。
3. **已持久化（Durable）** —— 记录已 `fdatasync` 到底层设备。它将挺过崩溃。

只有第三阶段才算 durable。读者由 **durable 游标** 门控：当 `record_id >= durable_cursor()` 时，`read(record_id)` 返回 `Ok(None)`，因此读者能看到记录必然能挺过崩溃（见[读取：可见性与 durable 游标](reading.md#可见性与-durable-游标)）。`LogDb::durable_cursor()` 返回所有分区的最小 durable 游标。

## flush：持久化屏障

`LogDb::flush` 会阻塞，直到 **durable 游标**追上当前的 producer 游标——即直到调用之前所有 `append` 的记录都已被 `fdatasync`：

```rust
impl LogDb {
    /// Waits for `durable_cursor` (NOT `committed_cursor` — fix C4).
    pub fn flush(&self) -> Result<(), FlushError>;
}
```

关于签名与实现（`src/lib.rs:393-422`），有两点需要注意：

- **它等待的是 `durable_cursor`，而非 `committed_cursor`。** 一条记录被提交（由 `pwrite` 写入）并不够；`flush` 只有在数据真正落到稳定存储后才会返回。这正是源码注释里点明的正确性修复。
- **它遵循 `config.flush_timeout`**（默认 30 秒）。若 Committer 未在超时内追上目标，`flush` 会返回错误而不是永久挂起。

`FlushError`（`src/error.rs:36-46`）：

```rust
pub enum FlushError {
    /// The flush did not complete within the configured timeout.
    Timeout,
    /// The database was aborted during the flush wait.
    Aborted,
}
```

- `Timeout` —— durable 游标未在 `config.flush_timeout` 内追上 producer 游标。数据仍在途，Committer 会继续尝试。可重试、调大 `flush_timeout`，或排查 Committer 是否饱和（见[写入：背压](writing.md#背压与-queuefullpolicy)）。
- `Aborted` —— 并发的 `shutdown` / `drain` / `Drop` 中止了等待。停止追加并收尾。

当启用 `hash-chain` 特性且设置了 `hash_enabled` 时，`flush` 会先等待 Sealer 把目标记录链接入链，再请求 fsync，因此成功 flush 的记录既持久又具备防篡改证据。

```rust
db.append(b"commit-marker")?;
db.flush()?; // 阻塞直到 durable_cursor 越过该 marker
```

## 持久化模式

`config.durability_mode` 控制 Committer 何时调用 `fdatasync`（`src/config.rs:17-26`）。默认值为 `Batch`。

```rust
pub enum DurabilityMode {
    /// `fdatasync` after every commit batch.
    Sync,
    /// `fdatasync` when the batch size or time threshold is met.
    Batch,
    /// `fdatasync` only on explicit `flush()` or shutdown.
    Async,
}
```

- **`Sync`** —— 每个 commit 批次之后都 `fdatasync`。每条已提交记录立即持久化。崩溃保证最强；单条延迟最高，因为每个批次都要付出一次 fsync。当崩溃时丢失任何已提交记录都不可接受、且负载能承受该延迟时使用。
- **`Batch`** *（默认）* —— 当 Committer 的批次触发条件满足时执行 `fdatasync`：缓冲数据达 `256 KiB`、记录数达 `1024`，或自首条待写记录起经过 `10 ms`（见 `src/config.rs:68-90` 的 `CommitTrigger`）。把 fsync 摊销到多条记录上，吞吐高且在途（已提交但尚未持久化）数据窗口有界。是大多数服务的合适默认值。
- **`Async`** —— 仅在显式 `flush()` 或关闭时 `fdatasync`。稳态吞吐最高、延迟最低；数据停在页缓存中，直到应用强制设置屏障。当应用在提交边界自行调用 `flush`（WAL 示例即是如此），或持久化由外部机制提供时使用。

```rust
let mut config = Config::default();
config.data_dir = dir.path().to_path_buf();
config.durability_mode = DurabilityMode::Async; // 依赖显式 flush()
config.flush_timeout = Duration::from_secs(5);
```

无论哪种模式，`flush()`、`drain()` 与 `shutdown()` 始终会强制把数据 fsync 到 producer 游标——显式屏障会覆盖模式设定。

## 如何选择模式

| 负载 | 推荐模式 | 原因 |
| --- | --- | --- |
| 金融账本、提交日志，任何“丢失已提交记录即正确性 bug”的场景 | `Sync` | 每个批次在下一个开始前即已持久化。 |
| 通用服务、遥测、事件流 | `Batch` *（默认）* | 吞吐高，风险数据窗口有界（约 ≤ 256 KiB / 10 ms）。 |
| 应用自行管理提交点（数据库 WAL、批处理流水线） | `Async` + 显式 `flush()` | 开销最低；由应用精确决定屏障位置。 |

权衡永远是**风险数据窗口 vs. fsync 开销**。`Sync` 把窗口降为零，代价是每批次一次 fsync；`Async` 把窗口交给应用控制，代价是要求纪律性；`Batch` 是务实的折中。

## 优雅关闭

离开一个运行中的 `LogDb` 有两种方式，它们的存在是因为句柄有时被独占、有时通过 `Arc` 共享：

```rust
impl LogDb {
    /// Shared-safe drain: flush all to durable WITHOUT consuming the handle
    /// or joining threads. Takes `&self`, so works with `Arc<LogDb>`.
    pub fn drain(&self, timeout: Duration) -> Result<ShutdownReport, FlushError>;

    /// Drain, then join the background threads. Consumes the handle and
    /// requires it be the only strong reference.
    pub fn shutdown(self, timeout: Duration) -> Result<ShutdownReport, ShutdownError>;
}
```

### `shutdown(self, timeout)` —— 独占句柄

当你**拥有** `LogDb`（未包在 `Arc` 中，或持有唯一的强引用）时使用 `shutdown`。它会排空在途追加、把数据 fsync 到 producer 游标，然后 join Committer（若启用 hash-chain 还包括 Sealer）。由于它消费 `self`，可以干净地回收线程。若存在其他强引用，则返回 `ShutdownError::JoinError("LogDb still referenced")`。

```rust
let report = db.shutdown(Duration::from_secs(5))?;
println!("shutdown: {:?}", report);
```

`ShutdownError`（`src/error.rs:64-74`）：

```rust
pub enum ShutdownError {
    /// Shutdown did not complete within the timeout.
    Timeout,
    /// Background threads could not be joined.
    JoinError(String),
}
```

### `drain(&self, timeout)` —— 共享句柄

当 `LogDb` 被**共享**时使用 `drain`，典型场景是长跑服务中的 `Arc<LogDb>`。它取 `&self`，进入排空阶段（此后新追加会返回 `AppendError::ShuttingDown`），等待在途追加发布完成，并把数据 fsync 到 producer 游标。后台线程**继续运行**；之后进程可以直接退出（数据已持久化，线程在 drop 时被无害中止），也可以再调用 `shutdown` 来 join 它们。

```rust
// 在持有 `Arc<LogDb>` 的服务内：
let report = shared_db.drain(Duration::from_secs(5))?;
```

### `ShutdownReport`

`drain` 与 `shutdown` 都返回 `ShutdownReport`，描述有多少数据被持久化（`src/error.rs:77-85`）：

```rust
pub enum ShutdownReport {
    /// All data was durably persisted before shutdown.
    Clean,
    /// Some data was committed but not fsynced before the timeout.
    PartialDurable,
    /// Shutdown timed out; some data may be lost.
    TimedOut,
}
```

- `Clean` —— 调用之前追加的每条记录都已持久化。这是正常的成功结果。
- `PartialDurable` —— 排空在 flush 中途超时：部分记录已发布，但 durable 游标未追上 producer 游标。后台线程可能在调用返回后仍完成 fsync。应视为“基本安全，需排查 Committer 为何落后”。
- `TimedOut` —— 由 `shutdown` 在 `drain` 自身超时时返回；部分在途数据可能丢失。

> 注意：在某些配置下（尤其是 WSL2），`fdatasync` 延迟可能导致一次本应 `Clean` 的排空被上报为 `PartialDurable`，即便数据其实已到达稳定存储。该判定偏保守。

## 崩溃保证

logdb 向应用提供的契约是精确的：

- **读者能看到的任何记录都挺过崩溃。** 读取受 durable 游标门控；记录只有在 `fdatasync` 之后才变得可读。不存在“现在能读到、崩溃后丢失”的窗口。
- **成功 `flush` 的记录挺过崩溃。** `flush` 只在 `durable_cursor` 越过这些记录后才返回。
- **已追加但尚未 flush 的记录是否能挺过崩溃并不确定**，取决于 Committer 在崩溃前是否恰好对其执行了 `fdatasync`。`Sync` 下该窗口为空；`Batch` 下由批次触发条件界定；`Async` 下则持续到下一个显式屏障。
- **批写入相对崩溃是原子的。** `append_batch` 用一次原子 claim 预留全部序列，因此崩溃后整批要么全部可见、要么全不可见，绝不会出现部分批（见[写入：原子批写入](writing.md#原子批写入)）。
- **撕裂写在重启时被修复。** 若崩溃打断了 `pwrite`，尾部不完整的记录会因 CRC 失败被检测到并在恢复期间截断（见[恢复](recovery.md)）。

## 相关链接

- [logdb README](../README.md)
- [恢复](recovery.md)
- [写入](writing.md)
- [读取](reading.md)
- [核心概念](concepts.md)
- [配置](configuration.md)

> logdb 0.2.0

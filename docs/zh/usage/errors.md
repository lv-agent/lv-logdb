# 错误处理

logdb 完整的错误目录，按操作分组，列出每个变体的触发条件与推荐处理方式。所有错误类型定义于 `src/error.rs`，通过 `thiserror` 派生 `std::error::Error`，因此能与 `?`、`anyhow` 和结构化日志干净互操作。

## 目录

- [为什么错误这样设计](#为什么错误这样设计)
- [AppendError](#appenderror)
- [FlushError](#flusherror)
- [ReadError](#readerror)
- [ShutdownError](#shutdownerror)
- [ShutdownReport](#shutdownreport)
- [相关链接](#相关链接)

## 为什么错误这样设计

logdb 把错误拆成五个枚举（每个操作族一个），而不是一个大枚举。每个变体命名一个明确、可操作的 condition，使调用方可以 match 它并选择正确的响应（重试 / 反压 / 降级 / 上报），而无需解析字符串：

```rust
// src/error.rs — 每个变体都通过 thiserror 派生 std::error::Error。
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AppendError { /* ... */ }
```

由于这五个枚举都派生 `thiserror::Error`，你免费获得：

- **`std::error::Error` + `Display`** —— 每个变体都有可读消息，能跨 `Result<_, Box<dyn Error>>`、`anyhow::Result`、`eyre` 用 `?`。
- **`Clone`、`Debug`、`PartialEq`、`Eq`** —— 错误是廉价的值类型。可在测试里 `assert_eq!`，也可克隆进日志或指标。
- **默认不捕获 `Backtrace`** —— 错误承载*条件*，而非栈回溯。若需要因果上下文，请配合 `tracing` span。

推荐的分派形态是按变体键控的 `match`：

```rust
use logdb::AppendError;

match db.append(&payload) {
    Ok(id) => /* ... */,
    Err(AppendError::QueueFull)     => /* 反压：重试或丢负载 */,
    Err(AppendError::ContentTooLarge { size, max }) => /* 拒绝该记录 */,
    Err(AppendError::DiskFull)      => /* 告警 ops；DB 可能自愈 */,
    Err(AppendError::Io(msg))       => /* 记录日志并向上传播 */,
    Err(AppendError::ShuttingDown)  => /* 停止接收新工作 */,
}
```

## AppendError

由 `append`、`append_batch` 和 `replicate` 返回（`src/error.rs:8-34`）。生产者必须处理这些条件。

```rust
pub enum AppendError {
    QueueFull,
    ContentTooLarge { size: usize, max: usize },
    DiskFull,
    Io(String),
    ShuttingDown,
}
```

| 变体 | 触发条件 | 推荐处理 |
|------|----------|----------|
| `QueueFull` | ring buffer 已满**且** `queue_full_policy` 为 `Drop`。返回（而非阻塞）以便调用方丢负载。 | **反压 / 重试。** 短退避后重试、把策略改为 `Block`（等待空槽）、或丢弃记录并给“dropped writes”指标加 1。见[写入：背压与 QueueFullPolicy](writing.md#背压与-queuefullpolicy)。 |
| `ContentTooLarge { size, max }` | 记录大于 `config.max_content_size`（默认 1 MB）。`size` 是被拒记录的长度，`max` 是配置上限。注意：空的 `append_batch(&[])` 也会返回此错误，带 `{ size: 0, max: 0 }`。 | **拒绝记录。** 这是编程 / 契约错误，不是瞬时的 —— 不要无脑重试。截断或压缩载荷，或调高 `max_content_size`。 |
| `DiskFull` | 底层磁盘已满（ENOSPC），由健康监控检测。**可能自愈**：空间释放后追加会恢复。 | **告警 ops，然后重试。** 大声记日志、上报指标、退避重试；磁盘被腾出（日志轮转、retention 裁剪、手动清理）时无需重启即可恢复。 |
| `Io(String)` | 写路径上发生非 ENOSPC 的 I/O 错误（如 `pwrite` 失败、权限错误），或 `replicate` 在 `shards > 1` / 乱序时被调用 —— 这些以带描述消息的 `Io` 报告。 | **向上传播。** 通常不是有意义的瞬时错误。记下消息并上报错误；不要在无运维介入时循环重试。`replicate` 的顺序错误见[特性：remote-push](features.md#remote-push)。 |
| `ShuttingDown` | 数据库已进入 drain 阶段（`drain` 或 `shutdown` 已被调用），不再接收新追加。 | **停止追加。** 收尾在途工作；不要重试 —— 该 handle 正在永久关闭。 |

> `replicate` 额外通过 `AppendError` 体现其单分片、按序、幂等的契约：多分片时 `Io("replicate requires shards=1")`、出现间隔时 `Io("replicate out of order: expected {cur}, got {sequence}")`（重试同一 sequence）、Committer 落后时 `QueueFull`。已复制的 `sequence` 返回 `Ok(())`（幂等空操作）。

## FlushError

由 `flush` 及（间接）`drain` 返回（`src/error.rs:37-46`）。描述一次持久化屏障为何未能完成。

```rust
pub enum FlushError {
    Timeout,
    Aborted,
}
```

| 变体 | 触发条件 | 推荐处理 |
|------|----------|----------|
| `Timeout` | 在 `config.flush_timeout`（默认 30 秒）内持久化游标未追上生产者游标。数据仍在途中 —— Committer 仍在尝试。 | **重试、提高超时、或排查。** 高负载下的瞬时超时可能在第二次 `flush` 时解除；持续超时意味着 Committer 跟不上（慢盘，或 `durability_mode` / 批设置与写入速率不匹配）。提高 `flush_timeout`、检查磁盘健康、或降低写入速率。 |
| `Aborted` | `flush` 阻塞期间，并发的 `shutdown` / `drain` / `Drop` 中止了等待。 | **停止追加并收尾。** handle 正在被拆除；不要重试 flush。见[持久化：优雅关闭](durability.md#优雅关闭)。 |

`drain(&self, timeout)` 返回 `Result<ShutdownReport, FlushError>`：未能在超时内追上持久化游标的 drain 返回 `FlushError::Timeout`（drain 自身中止）；成功的 drain 返回下面 `ShutdownReport` 中的某个变体。

## ReadError

由 `read`、`scan` 和 `replay_from` 返回（`src/error.rs:49-62`）。描述读取为何无法满足请求。

```rust
pub enum ReadError {
    NotFound(u64),
    CrcMismatch(u64),
    Io(String),
}
```

| 变体 | 触发条件 | 推荐处理 |
|------|----------|----------|
| `NotFound(id)` | 请求的 `record_id` 不存在 —— 它在持久化游标之后（尚未可见）或超出日志末尾。 | **降级。** 对点查询，视为“尚无记录”，稍后重试或对调用方上报 not-found。对 scan / tailer，这是正常的日志尾信号；见[读取：可见性与 durable 游标](reading.md#可见性与-durable-游标)。 |
| `CrcMismatch(id)` | 在 `record_id` 处 CRC 校验失败，表明磁盘损坏（位腐烂、半截写入，或 —— 启用 `encryption` 时 —— 被篡改的密文帧，GCM 像校验 CRC 那样检测出来）。 | **上报并停止。** 这是数据损坏，不是瞬时状态。记下出问题的 `record_id`、告警 ops，不要盲目越过它继续扫描 —— 损坏记录可能不可读。启用 hash-chain 时，链中的不匹配通过验证上报，不通过此变体（见[特性：hash-chain](features.md#hash-chain)）。 |
| `Io(String)` | 打开或读取段文件时发生 I/O 错误（如段被从下面删除，或底层 read 失败）。 | **向上传播。** 记下消息并上报错误；仅当底层原因已知为瞬时时才重试。 |

> 当记录逻辑存在但尚未持久化时，`read` 返回 `Ok(None)`（不是错误）。`ReadError::NotFound` 专用于真正缺失的 ID —— 日志末尾之后。见[读取：读取错误](reading.md#读取错误)。

## ShutdownError

由 `shutdown` 返回（`src/error.rs:65-74`）。描述一次独占 handle 的关闭为何无法干净完成。

```rust
pub enum ShutdownError {
    Timeout,
    JoinError(String),
}
```

| 变体 | 触发条件 | 推荐处理 |
|------|----------|----------|
| `Timeout` | 关闭未在请求的超时内完成（drain 阶段超时，或后台线程未能及时 join）。 | **上报并继续。** 部分在途数据可能丢失。记录结果；在进程退出路径上通常能做的不多。若下次想更软着陆，先用 `drain`（共享 handle）刷盘，再 `shutdown` join。 |
| `JoinError(String)` | 后台线程（Committer，以及启用 hash-chain 时的 Sealer）无法 join —— 最常见的是 `LogDb` handle 仍在别处被引用（`"LogDb still referenced"`）。 | **修复引用计数，再重试。** `shutdown` 消费 `self` 且要求它是*唯一*的强引用。若 handle 经 `Arc` 共享，请改用 `drain`，或先 drop 掉其他 `Arc` 克隆。 |

`drain`（共享 handle 路径）**不**返回 `ShutdownError` —— 它返回 `Result<ShutdownReport, FlushError>`，因为它不 join 线程。

## ShutdownReport

由 `drain` 和 `shutdown`（作为 `Ok`）返回，描述有多少数据被持久化（`src/error.rs:77-85`）。`ShutdownReport` **不是**错误 —— 它是一个带三档“干净程度”的成功值。

```rust
pub enum ShutdownReport {
    Clean,
    PartialDurable,
    TimedOut,
}
```

| 变体 | 含义 | 推荐处理 |
|------|------|----------|
| `Clean` | 调用前追加的每条记录现在都已持久化。这是正常、成功的结局。 | 继续退出。 |
| `PartialDurable` | 部分记录已提交但超时前未被 fsync。调用返回后后台线程可能仍会完成 fsync。 | **排查 Committer 为何落后。** 视为“基本安全”但不保证；在 WSL2 上，由于 `fdatasync` 延迟，`Clean` 的 drain 可能被误判为 `PartialDurable`（见[持久化：优雅关闭](durability.md#优雅关闭)）。 |
| `TimedOut` | 关闭超时；部分在途数据可能丢失。 | **告警。** 可能丢数据。大声记日志并上报指标，便于运维与崩溃/退出关联。 |

分类是保守的：拿不准时 logdb 报告更悲观的变体，而不声称它无法证明的持久化。

## 相关链接

- [使用指南](README.md)
- [写入](writing.md) —— `QueueFullPolicy` 与追加路径错误的具体语境。
- [读取](reading.md) —— `ReadError` 与持久化游标可见性规则。
- [持久化](durability.md) —— `FlushError`、`shutdown`、`drain` 与 `ShutdownReport` 的深入讨论。
- [恢复](recovery.md) —— 崩溃后 `CrcMismatch` 意味着什么。

> logdb 0.2.0

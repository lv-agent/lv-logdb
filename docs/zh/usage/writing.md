# 写入

如何向 logdb 写入记录：单条 `append`、原子 `append_batch`、大小限制、背压，以及写者必须处理的错误情况。

## 目录

- [写入单条记录](#写入单条记录)
- [原子批写入](#原子批写入)
- [内容大小限制](#内容大小限制)
- [背压与 QueueFullPolicy](#背压与-queuefullpolicy)
- [磁盘满、关闭与其他错误](#磁盘满关闭与其他错误)
- [何时 flush](#何时-flush)

## 写入单条记录

`LogDb::append` 写入一条记录，并返回它的**全局 record id**：

```rust
impl LogDb {
    pub fn append(&self, content: &[u8]) -> Result<u64, AppendError>;
}
```

返回的 `u64` 就是你回传给 [`read`](reading.md#点读) 的值。在默认的单分区场景下，它等于 `record.id.sequence`（见[核心概念](concepts.md#序列空间与单调性)）。

```rust
let id = db.append(b"order-created 42")?;
db.flush()?;
let rec = db.read(id)?.expect("record must exist after flush");
assert_eq!(rec.content, b"order-created 42");
```

在占有槽之前，`append` 会依次做三项前置检查（顺序见 `src/lib.rs:264-303`）：

1. **健康检查** —— 若数据库处于降级状态（磁盘满/不健康），返回 `AppendError::DiskFull`（对应 ENOSPC）或 `AppendError::Io("health check failed")`。
2. **内容大小检查** —— 若 `content.len() > config.max_content_size`，返回 `AppendError::ContentTooLarge { size, max }`。
3. **关闭守卫** —— 一旦数据库开始关闭或排空（drain），返回 `AppendError::ShuttingDown`。

三项都通过后，才会在对应分片的环上 CAS 占有一个槽、写入内容并发布。记录立即可被后台 Committer 看到，但在 fsync 之前对读者**不可见**（见[读取](reading.md#可见性与-durable-游标)）。

## 原子批写入

当需要多条记录一起提交时，使用 `append_batch`：

```rust
impl LogDb {
    pub fn append_batch(&self, contents: &[&[u8]]) -> Result<u64, AppendError>;
}
```

`append_batch` 是**原子的**：批次中的所有记录一起提交——崩溃后要么整批可见、要么一条都不可见。它返回批次中**第一条**记录的全局 record id；后续记录占据连续的序列号。

```rust
let batch: &[&[u8]] = &[b"a", b"b", b"c"];
let first = db.append_batch(batch)?;
db.flush()?;
// 三条记录占据连续 id：first、first+1、first+2。
assert_eq!(db.read(first)?.unwrap().content, b"a");
assert_eq!(db.read(first + 2)?.unwrap().content, b"c");
```

原子性保证依赖于一次原子预留。所有 `contents.len()` 个序列号在**一次原子 `claim_batch`** 调用中被预留（即在所选分片上做一次 `producer_cursor += n`），因而不存在部分预留：连续的批次绝不会互相覆盖槽位。在预留之前，`append_batch` 会**逐条**校验内容大小，因为若在部分 claim 之后才发现某条记录过大，会留下“已预留但未写入”的槽——一个 Committer 无法跨越的空洞（`src/lib.rs:226-262`）。

注意事项：

- 空批次会被拒绝，返回 `AppendError::ContentTooLarge { size: 0, max: 0 }`。
- 与 `append` 相同的前置检查（健康/大小/关闭）和 `QueueFullPolicy` 同样适用。

## 内容大小限制

`config.max_content_size`（默认 **1 MiB**，由 `Config::validate` 限制在 **≤ 64 MiB**）约束单条记录的内容大小。超过该值会在占有任何槽之前快速失败，并返回结构化错误：

```rust
pub enum AppendError {
    ContentTooLarge { size: usize, max: usize },
    // ...
}
```

```rust
let mut config = Config::default();
config.data_dir = dir.path().to_path_buf();
config.max_content_size = 100;
let db = LogDb::open(config)?;
let err = db.append(&vec![0u8; 200]).unwrap_err();
assert!(matches!(err, AppendError::ContentTooLarge { size: 200, max: 100 }));
```

`append_batch` 中每条记录都遵循同一限制；第一条过大的记录会在预留任何序列号之前中止整批。

另外请注意，**256 字节 inline/spill 边界**（`INLINE_CAP`）是一个*性能*阈值，而非正确性阈值——记录 ≤ 256 字节走零分配的 inline 快路径，更大的记录会 spill 到堆上。见[核心概念：Inline 与 Spill](concepts.md#inline-与-spill)。

## 背压与 QueueFullPolicy

生产者不直接写段文件，而是写入一个固定大小的环形缓冲（每个分片 `config.ring_size` 个槽，默认 8192），由后台 Committer 排空。若生产者比 Committer 快超过 `ring_size`，环就满了，此时由 `config.queue_full_policy` 决定行为（`src/config.rs:9-15`）：

```rust
pub enum QueueFullPolicy {
    /// Block：自旋 + 退避，直到有空槽可用。
    Block,
    /// Drop：立即返回 AppendError::QueueFull。
    Drop,
}
```

- **`Block`**（默认）—— 调用方以退避方式自旋，直到有空槽释放。负载下吞吐保持较高；环越满延迟越高。适用于绝不能丢记录的场景。
- **`Drop`** —— `append` / `append_batch` 立即返回 `AppendError::QueueFull`。由调用方决定是重试、降载还是退避。适用于宁愿丢记录也不愿让生产者线程停滞的场景。

```rust
let mut config = Config::default();
config.queue_full_policy = QueueFullPolicy::Drop;
```

环满几乎总是意味着 Committer（或设备的 `fdatasync`）已饱和。修复办法是运维层面的，而非 API 层面：降低写入速率、增大 `ring_size`、分片（`config.shards`）、或换用更快的存储。`producer_cursor()` 与 `committed_cursor()` 之间的差距可以量化 Committer 的饱和程度（见[核心概念：游标语义](concepts.md#游标语义)）。

## 磁盘满、关闭与其他错误

完整的 `AppendError`（`src/error.rs`）：

```rust
pub enum AppendError {
    /// 环满且策略为 Drop。
    QueueFull,
    /// 内容超过 config.max_content_size。
    ContentTooLarge { size: usize, max: usize },
    /// 底层磁盘满（ENOSPC）。可能自愈。
    DiskFull,
    /// 非 ENOSPC 的 I/O 错误。
    Io(String),
    /// 数据库正在关闭，不再接受新的写入。
    ShuttingDown,
}
```

处理建议：

- `QueueFull` —— 退避重试、降载，或改用 `Block`。
- `ContentTooLarge { size, max }` —— 修正生产者；绝不要悄悄截断。
- `DiskFull` —— 设备返回了 ENOSPC。logdb 会把自身标记为不健康，使后续写入快速失败。它可以**自愈**：一旦空间释放且 Committer 再次成功写入，健康标志会清除，写入恢复。把它当作“可重试但紧急”的情况。
- `Io(String)` —— 非 ENOSPC 的 I/O 失败（健康检查因非磁盘原因失败时也会上报此错误）。检查错误信息；数据库很可能已降级。
- `ShuttingDown` —— `shutdown`、`drain` 或 `Drop` 已经开始。停止写入；收尾你的流水线。

## 何时 flush

`append`（与 `append_batch`）只把记录发布给 Committer。它们要等到 fsync 之后才会变得**持久化**——从而对读者可见。默认 `DurabilityMode::Batch` 下，fsync 按大小/数量/时间阈值触发；`Async` 下只在显式 `flush`/关闭时触发；`Sync` 下每批之后都触发。

当你需要持久化屏障时，调用 `flush`，例如：

- 在崩溃中绝不能丢失的批次之后（订单簿的 tick、提交标记），
- 在用 `read` 读回记录之前——读者只能看到 `record_id < durable_cursor()` 的记录，
- 在检查点或与外部系统协调之前。

```rust
db.append(b"commit-marker")?;
db.flush()?; // 阻塞，直到 durable_cursor 越过该标记
```

完整的 fsync/持久化模型以及 `Sync`、`Batch`、`Async` 之间的取舍见[持久化](durability.md)。

## 相关链接

- [logdb README](../README.md)
- [读取](reading.md)
- [核心概念](concepts.md)
- [持久化](durability.md)
- [错误](errors.md)
- [配置](configuration.md)

> logdb 0.2.0

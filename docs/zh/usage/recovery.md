# 恢复

logdb 如何在崩溃后重建状态：`open` 时的自动恢复、撕裂写检测与截断、可选的 hash-chain 校验、checkpoint 机制，以及标准的 WAL 重放模式。

## 目录

- [open 时的自动恢复](#open-时的自动恢复)
- [恢复算法做了什么](#恢复算法做了什么)
- [Checkpoint](#checkpoint)
- [checkpoint.dat 的磁盘布局](#checkpointdat-的磁盘布局)
- [重放 API：recovery_report 与 replay_from](#重放-apirecovery_report-与-replay_from)
- [WAL 模式](#wal-模式)
- [什么挺过崩溃、什么被截断](#什么挺过崩溃什么被截断)

## open 时的自动恢复

`LogDb::open` 是恢复的入口。若数据目录已存在且包含 `segment-00000001.log`，logdb 把它视作既有库，在启动流水线之前运行 `recovery::recover`（`src/lib.rs:98-124`）：

```rust
let (mut seg_mgr, initial_seq, last_hash, hash_init) = if data_dir.exists()
    && data_dir.join("segment-00000001.log").exists()
{
    let state = recovery::recover(
        &data_dir,
        config.segment_size,
        config.retention.clone(),
        config.encryption_key,
    )?;
    let initial = state.last_sequence.wrapping_add(1);
    (state.segment_manager, initial, state.last_hash, state.hash_init)
} else {
    // ……全新库：创建第一个段……
};
```

若目录不存在（或没有第一个段），logdb 创建全新库。无需记住单独的“recover”调用——打开既有目录总会执行恢复。恢复所得的状态用于为环形缓冲区的下一序列（`last_sequence + 1`）、hash 链续接以及重新打开的 `SegmentManager` 提供种子。

打开库时必须使用与写入时**相同的 `encryption_key`**：恢复期间会对加密段进行解码（先解密再解压），错误的密钥会得到无法解密的帧并被视作损坏。

## 恢复算法做了什么

恢复从磁盘上的段文件重建日志（`src/recovery.rs`）。算法步骤（`src/recovery.rs:7-17`，§15）：

1. **列出并按 `segment_id` 升序排序**所有 `segment-*.log` 文件。
2. **校验每个段头**（magic + header CRC）。段头损坏意味着该段及其后所有段被丢弃——恢复在此停止。
3. **逐条扫描最后一个段**（对压缩/加密段则按帧扫描）：读长度、读完整记录、校验 CRC、确认 `record_id` 与期望的单调序列一致。
4. **撕裂写检测。** 撕裂写——文件在记录中途结束，或尾部记录 CRC 失败——通过把文件截断回上一条完全有效的记录来修复。截断点与出错的 `record_id` 会被记录为 `RecoveryWarning::TornWrite`。
5. **重建稀疏索引**（延迟到索引构建阶段）。
6. **Hash 链校验**（仅当启用 `hash-chain` 特性且写数据时设置了 `hash_enabled`）。对最后一个段的每条记录，恢复重新计算 BLAKE3 keyed 链哈希并与存储的 `hash_n` 比较。不匹配表示篡改或损坏；恢复记录 `RecoveryWarning::HashChainBreak` 并在该点截断。

恢复是保守的：任何无法完整校验的内容都会被截断。它绝不会返回可能错误的记录。非致命问题（撕裂写、尾部段头损坏、hash 链断裂）作为告警收集在 `RecoveryState::warnings` 中，不会让 `open` 失败。

## Checkpoint

**checkpoint** 是应用告知 logdb“我已吸收到序列 `S` 为止的全部记录；`S` 之前的 WAL 数据可以被截断”。它是“仍需重放”与“可安全回收”之间的边界。

```rust
impl LogDb {
    /// Mark `sequence` as the WAL checkpoint. Records with sequence <
    /// checkpoint are safe to delete. Old segments fully covered by the
    /// checkpoint will be truncated on the next roll.
    pub fn checkpoint(&self, sequence: u64);

    /// Get the current checkpoint sequence.
    pub fn checkpoint_sequence(&self) -> u64;
}
```

`checkpoint(sequence)` 推进一个内部原子值，并把它持久化到 `checkpoint.dat`（见[下文](#checkpointdat-的磁盘布局)），从而挺过重启——`checkpoint_sequence()` 读回同一个值。checkpoint 是**单调**的：更小的值会被静默忽略。

两个重要性质：

- **checkpoint 不会立即删除记录。** 它只是记录一个边界。完全被 checkpoint 覆盖（段内所有记录 `sequence < checkpoint`）的段，会在活动段**滚动**时被截断——因此 checkpoint 很廉价，空间回收被摊销到滚动中。
- **checkpoint 及之后的记录仍可恢复。** `recovery_report().from_sequence` 等于 checkpoint；`replay_from(checkpoint)` 返回应用仍需要的记录（`sequence >= checkpoint`）。正因如此，你应当 checkpoint 一个**稳定的已吸收点**，而非实时的 durable 尾部——给尾部打 checkpoint 会让重放无所收获（见 [WAL 模式](#wal-模式)）。

## checkpoint.dat 的磁盘布局

checkpoint 以原子方式持久化，从而写中途崩溃不会损坏它（`src/lib.rs:668-683`）：

```
偏移   长度  字段
0      8     sequence    (u64, 小端)
8      4     CRC32C      (u32, 小端，覆盖 0..8 字节)
```

写入序列是 tmp → 写入 → `fdatasync` → rename → `sync_dir`，即标准的崩溃安全原子替换模式：

1. 把 12 字节写入 `checkpoint.tmp`。
2. 对 tmp 文件 `fdatasync`（让数据到达稳定存储）。
3. `rename(checkpoint.tmp, checkpoint.dat)`（POSIX 上原子）。
4. `fsync` 目录（让 rename 本身也持久化）。

读取时（`LogDb::load_checkpoint`，`src/lib.rs:513-523`），logdb 读取 12 字节；若长度不是恰好 12 或 CRC32C 不匹配，checkpoint 视作 `0`（从头重放）。因此撕裂的 `checkpoint.dat` 会优雅降级，而不是损坏恢复。

## 重放 API：recovery_report 与 replay_from

`open` 之后，两个调用描述要重放什么：

```rust
impl LogDb {
    pub fn recovery_report(&self) -> RecoveryReport;
    pub fn replay_from(&self, sequence: u64) -> Result<reader::iter::RecordIter, ReadError>;
}

/// Recovery report returned by `LogDb::recovery_report`.
pub struct RecoveryReport {
    /// First sequence to replay (the last checkpoint).
    pub from_sequence: u64,
    /// Last durable sequence.
    pub to_sequence: u64,
    /// Number of records to replay.
    pub count: u64,
}
```

- `recovery_report()` 返回 `{ from_sequence: checkpoint, to_sequence: durable_cursor, count: to - from }`。它是应用为重建状态而应重放的记录区间：从上一个 checkpoint 到 durable 尾部的一切。
- `replay_from(sequence)` 是 `scan(sequence, u64::MAX)` 的便捷封装——按序产出每一条 `id >= sequence` 的 durable 记录。只返回 durable 记录，因此迭代器反映了挺过恢复的内容。

```rust
let report = db.recovery_report();
println!("replay {}..{} ({} records)", report.from_sequence, report.to_sequence, report.count);
for rec in db.replay_from(report.from_sequence)? {
    let rec = rec?;
    apply(&rec.content);
}
```

`RecoveryReport::count` 是提示，而非对迭代器长度的硬性上界——它在报告时刻由 checkpoint 与 durable 游标计算得出。请迭代到结束为止。

## WAL 模式

标准模式——为应用做预写日志——在 `examples/wal.rs` 中有演示。生命周期为 **write → flush → checkpoint →（崩溃）→ reopen → replay**：

1. **打开**数据目录。若已存在，自动执行恢复。
2. **重放**从已持久化的 checkpoint 开始，重建内存状态。
3. **写入**意图记录（`PUT key value`、`DEL key`）来应用变更，并在改动内存状态前 `flush()`，确保每条意图都已持久化。
4. **Checkpoint** 你恢复时所处序列——即应用已吸收日志的稳定点。这会释放 WAL 空间；checkpoint 及之后的记录留待下次重放。
5. 用 `shutdown(timeout)` **干净关闭**，让最后一批被 fsync。若进程改为崩溃，则下次 `open` 会恢复，并从上一个 checkpoint 重放重建状态。

示例把这一过程建模为一个 `KvStore`，其状态由 WAL 意图记录重建：

```rust
impl KvStore {
    fn open(data_dir: &str, replay_checkpoint: u64) -> Self {
        let mut config = Config::default();
        config.data_dir = data_dir.into();
        config.durability_mode = DurabilityMode::Async; // 用显式 flush() 保证持久化
        config.flush_timeout = Duration::from_secs(5);
        let db = LogDb::open(config).unwrap();

        let mut data = HashMap::new();
        // 从稳定 checkpoint 重放，重建内存状态。
        for result in db.replay_from(replay_checkpoint).unwrap() {
            let record = result.unwrap();
            let content = String::from_utf8_lossy(&record.content);
            let parts: Vec<&str> = content.splitn(3, ' ').collect();
            match parts.as_slice() {
                ["PUT", key, value] => { data.insert(key.to_string(), value.to_string()); }
                ["DEL", key]        => { data.remove(*key); }
                _ => {}
            }
        }
        Self { db, data, replay_from: replay_checkpoint }
    }

    fn put(&mut self, key: &str, value: &str) {
        let wal = format!("PUT {} {}", key, value);
        self.db.append(wal.as_bytes()).unwrap();
        self.db.flush().unwrap();          // 在改动内存状态前先持久化
        self.data.insert(key.to_string(), value.to_string());
    }

    fn checkpoint(&self) {
        // checkpoint 稳定的已吸收点，而不是实时 durable 尾部。
        self.db.checkpoint(self.replay_from);
    }
}
```

关键的正确性要点（示例明确点出）：`checkpoint()` 标记的是**重放点**（会话恢复时所处的序列），而非实时 `durable_cursor()`。给 durable 尾部打 checkpoint 会覆盖刚刚写入的记录，导致 `recovery_report().count == 0`、崩溃后无可重放。请 checkpoint 你恢复时所处的稳定点；本次会话写入的记录（序列 `>= replay_from`）仍可恢复。

运行示例可验证完整闭环：会话 1 写入 `name`、`email`、`role`，删除 `role`，打 checkpoint，干净关闭；会话 2 重新打开同一目录，恢复执行，重放重建出 `name=Alice`、`email=alice@example.com`、`role=None`。

```
--- Session 1 ---
PUT name = Alice (lsn=1)
PUT batch 2 pairs (lsn=3)
DEL role (lsn=4)
Checkpoint at lsn=0 (durable tail=4)
Closing...
Shutdown: Clean

--- Session 2 (after simulated crash) ---
Recovery report: from=0 to=4 count=4
Recovered 2 key(s) from WAL
name=Some("Alice")
email=Some("alice@example.com")
role=None
Recovery successful: data intact after crash.
```

## 什么挺过崩溃、什么被截断

- **挺过：** 崩溃前已 `fdatasync` 的每条记录——即到 `durable_cursor()` 为止的一切——*减去*尾部任何撕裂写。恢复会重新校验最后一个段并截断不完整的末尾记录，因此存活前缀恰好是完整、CRC 合法的记录集合。
- **恢复时截断：** 撕裂写的尾部不完整记录（崩溃打断了记录中段的 `pwrite`）；任何 CRC 失败的记录；hash 链断裂（仅 hash-chain 特性）之后的记录。这些会变成 `RecoveryWarning`。
- **丢弃：** 段头损坏的段，及其后的所有段。恢复信任第一个坏段头之前的前缀。
- **丢失：** 已追加但从未到达稳定存储（未 `fdatasync`）的记录。`DurabilityMode::Sync` 下为空；`Batch` 下由批次触发条件界定；`Async` 下持续到下一次 `flush`/`shutdown`（见[持久化](durability.md#持久化模式)）。
- **被 checkpoint 清掉的记录**并非“丢失”——它们已被应用吸收。它们可能在下次段滚动时被物理截断以回收空间；`replay_from(checkpoint)` 并不需要它们。

## 相关链接

- [logdb README](../README.md)
- [持久化](durability.md)
- [写入](writing.md)
- [读取](reading.md)
- [核心概念](concepts.md)
- [配置](configuration.md)

> logdb 0.2.0

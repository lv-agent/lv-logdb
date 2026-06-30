# 读取

如何从 logdb 读回记录：点查、范围扫描、重放、由持久化界定的可见性规则、透明的解压/解密，以及如何处理读取错误。

## 目录

- [可见性与 durable 游标](#可见性与-durable-游标)
- [点读](#点读)
- [范围扫描](#范围扫描)
- [从某个序列重放](#从某个序列重放)
- [一次点读是如何定位记录的](#一次点读是如何定位记录的)
- [透明的解压与解密](#透明的解压与解密)
- [读取错误](#读取错误)

## 可见性与 durable 游标

对于读者而言最重要的规则——引自 `src/reader/mod.rs` 模块文档：

> 所有读取都以 `durable_cursor` 为界：只有已 fsync 的数据对读者可见。这保证了被读到的记录能在崩溃中幸存。

具体而言，只要 `record_id >= durable_cursor()`，`read(record_id)` 就会返回 `Ok(None)`，**即便记录已经被写入并提交**。不存在“现在能读到、崩溃后却丢失”的窗口：读者能看到的任何记录都能在崩溃中幸存。

如果你需要让某条记录可见，请先调用 [`flush`](writing.md#何时-flush)。完整的 producer / committed / durable 模型见[核心概念：游标语义](concepts.md#游标语义)。

## 点读

`LogDb::read` 按全局 record id 做点查：

```rust
impl LogDb {
    pub fn read(&self, record_id: u64) -> Result<Option<Record>, ReadError>;
}
```

- 当（持久化的）记录存在时返回 `Ok(Some(Record))`。
- 当记录不存在**或**尚未持久化（`record_id >= durable_cursor()`）时返回 `Ok(None)`。
- 发生 I/O 失败或检测到损坏时返回 `Err(ReadError)`。

```rust
db.append(b"event-7")?;
db.flush()?;
let rec = db.read(id)?.expect("present after flush");
assert_eq!(rec.content, b"event-7");
```

返回的 `Record` 是完全拥有的：

```rust
pub struct Record {
    pub id: RecordId,
    pub timestamp_ns: u64,
    pub content: Vec<u8>,
    pub hash_n: [u8; 32],
}
```

字段语义见[核心概念：Record 结构体](concepts.md#record-结构体)。

## 范围扫描

`LogDb::scan` 返回一个**半开**区间 `[from_id, to_id)` 上的迭代器：

```rust
impl LogDb {
    pub fn scan(
        &self,
        from_id: u64,
        to_id: u64,
    ) -> Result<reader::iter::RecordIter, ReadError>;
}
```

`from_id` 是包含的，`to_id` 是**不包含**的——因此 `scan(10, 20)` 产出记录 `10..19`。若无法定位 `from_id` 所在的起始段，`scan` 返回 `ReadError::NotFound(from_id)`。

```rust
let first = db.append(b"a")?;
db.append(b"b")?;
db.append(b"c")?;
db.flush()?;

for rec in db.scan(first, first + 3)? {
    println!("{}: {:?}", rec.id.sequence, rec.content);
}
```

## 从某个序列重放

`LogDb::replay_from(sequence)` 是一个便捷封装，从 `sequence`（包含）扫描到持久化日志的末尾：

```rust
impl LogDb {
    pub fn replay_from(&self, sequence: u64) -> Result<reader::iter::RecordIter, ReadError>;
}
```

它实现为 `scan(sequence, u64::MAX)`，因此会产出所有 id `>= sequence` 的持久化记录。它常用于（重新）处理历史——例如在重建下游视图之后，或作为 [tailer](tailers.md) 的基础。由于只返回持久化记录，先调用 `flush` 可保证重放看到最新写入。

```rust
for rec in db.replay_from(checkpoint)? {
    apply(rec?);
}
```

## 一次点读是如何定位记录的

读取复杂度关于段数为 O(log N)，而不是每条记录 O(N)。查找算法（来自 `src/reader/mod.rs` 模块文档）：

1. **定位段** —— 通过检查每个段的 `[base_record_id, max_record_id]` 范围，找到包含目标 `record_id` 的段。一个缓存且按段排序的清单（`SegmentManifest`）使其成为二分查找，并且只在数据目录 mtime 变化（滚动或保留策略截断）时失效，因此一次 `read()` 不会每次都重新 `readdir` 或重新读取每个段头部。
2. **找稀疏索引锚点** —— 在目标 id 处或之前找到锚点（仅原始段）。`.idx` 文件每隔 `index_stride` 条记录把 id 映射到文件偏移。
3. **定位并扫描** —— 打开段，seek 到锚点的文件偏移，然后顺序向前扫描到目标 record id。

对于基于帧的段（压缩或加密），没有按记录的稀疏索引；读取从段头部开始，向前解码帧（见[透明的解压与解密](#透明的解压与解密)）。

完整的数据结构与代码路径细节属于开发指南（见[架构](../dev/architecture.md)）；应用代码只需要知道点读有界且足够快。

## 透明的解压与解密

如果数据库以 `compression_enabled` 或带 `encryption_key` 打开，段在磁盘上是压缩和/或加密存储的。**读取时透明解码**——同样的 `read` / `scan` / `replay_from` 调用返回明文 `Record`，与段在磁盘上如何存储无关。

共享的解码路径是 `decode_frame_payload`（`src/reader/mod.rs:67-84`）：给定一个磁盘上的帧载荷，它会（若加密）解密、再（若压缩）解压，返回原始记录字节。Reader、扫描迭代器与崩溃恢复都使用这条路径，因此三者对帧布局的认知一致。

两点实际后果：

- **加密读取需要密钥。** 不带匹配 `encryption_key` 打开的 `LogDb` 无法解密其段；加密帧无法解密。请用写入时相同的密钥打开数据库。
- **压缩/加密用读取 CPU 换磁盘空间。** 由于基于帧的段没有按记录的稀疏索引，压缩/加密段上的点读会从段头部做按帧对齐的向前扫描，而非按稀疏索引 seek。对于大段上延迟敏感的点读，需要权衡这一点与保持段原始（不压缩）所付出的磁盘/CPU 取舍。

## 读取错误

完整的 `ReadError`（`src/error.rs`）：

```rust
pub enum ReadError {
    /// 请求的 record_id 不存在。
    NotFound(u64),
    /// CRC 校验失败，表明数据损坏。
    CrcMismatch(u64),
    /// 读取期间发生 I/O 错误。
    Io(String),
}
```

处理建议：

- `NotFound(u64)` —— `scan` 在找不到起始段时返回此错误；按“范围内无数据”处理，并把 id 对照 `durable_cursor()` 检查。（对一条不存在或未持久化的记录做点查 `read` 会返回 `Ok(None)`，而非错误。）
- `CrcMismatch(u64)` —— 某条记录 CRC 校验失败，表明磁盘损坏（恢复未截断的撕裂写、位朽或硬件故障）。绝不要悄悄跳过：告警、隔离该段，并参阅[恢复](recovery.md)。若你启用了 `hash-chain` 特性，链式哈希能在按记录 CRC 之外提供防篡改证据。
- `Io(String)` —— 读取期间的 I/O 失败（文件打开、seek、读）。检查错误信息；瞬时故障可重试，持续故障则指示存储问题。

## 相关链接

- [logdb README](../README.md)
- [写入](writing.md)
- [核心概念](concepts.md)
- [持久化](durability.md)
- [恢复](recovery.md)
- [Tailer](tailers.md)
- [错误](errors.md)

> logdb 0.2.0

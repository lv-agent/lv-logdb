# 核心概念

logdb 背后的核心模型：记录及其标识符、序列空间、段、无锁环形缓冲，以及——最关键的——决定可见性与持久性的三个游标。

## 目录

- [记录与 RecordId](#记录与-recordid)
- [Record 结构体](#record-结构体)
- [序列空间与单调性](#序列空间与单调性)
- [段（Segments）](#段segments)
- [环形缓冲与槽（Ring buffer & slots）](#环形缓冲与槽ring-buffer--slots)
- [游标语义](#游标语义)
- [Inline 与 Spill](#inline-与-spill)

## 记录与 RecordId

logdb 中每条记录都有一个逻辑位置，称为 **`RecordId`**。它采用 Kafka 式的分区-偏移语义——一个 `(partition_id, sequence)` 元组，而**不是**把物理拓扑压缩进单个 `u64`：

```rust
pub struct RecordId {
    pub partition_id: u32, // 逻辑分区；单分区 v1.0 时为 0
    pub sequence: u64,     // 分区内单调递增
}
```

对于常见的单分区场景（`partition_id == 0`），`RecordId` 实现了 `Into<u64>`（直接返回 `sequence`）与 `From<u64>`，因此 `id: u64` 与 `id: RecordId` 可以互换：

```rust
let id = RecordId::new(0, 42);
let seq: u64 = id.into();          // 42
let back: RecordId = 99u64.into(); // 分区 0，序号 99
```

其 `Display` 实现：当 `partition_id == 0` 时只显示序号，否则显示为 `partition/sequence`：

```rust
assert_eq!(format!("{}", RecordId::new(0, 42)),  "42");
assert_eq!(format!("{}", RecordId::new(3, 42)),  "3/42");
```

分片 ID 和节点 ID **不会**编码进 `RecordId` —— 它们是内部实现细节，可能在再均衡过程中变化。公开的 `append` API 返回一个 `u64`（即序号），也就是你回传给 `read` 的值。

## Record 结构体

从段文件读回的、完全拥有的记录类型是 `Record`：

```rust
pub struct Record {
    pub id: RecordId,     // 逻辑标识符
    pub timestamp_ns: u64, // 纳秒，CLOCK_REALTIME_COARSE
    pub content: Vec<u8>,  // 拥有的记录内容
    pub hash_n: [u8; 32],  // SHA-256 链值（未启用哈希时全为 0）
}
```

- `timestamp_ns` 由 logdb 在写入时从 `CLOCK_REALTIME_COARSE` 取值赋给记录——不要依赖应用层来设置。
- `hash_n` 是前向链式哈希值。当 `hash-chain` 特性**关闭**时为 `[0u8; 32]`；开启时，每条记录的哈希由上一条记录的哈希加上本条内容链接而成，从而提供防篡改能力。

内部热点路径上还有一个借用的零拷贝视图（`ReadView<'a>`）；面向应用的读取返回拥有的 `Record`。

## 序列空间与单调性

序列号在分区内**严格单调**。每次成功的 `append` 返回的序列号比上一次大 1（从写者视角看没有空隙；只有当写者收到 `AppendError` 时才会出现空隙）。

当使用**分片环形**（`config.shards > 1`）时，每个分片拥有全局序列的一个跨步子空间。来自不同分片的记录在全局上交错：

```
global_seq = local_seq * num_shards + shard_id
```

因此当 `shards = 4` 时，每个分片的第一条记录的全局序列为 `0, 1, 2, 3`，第二轮为 `4, 5, 6, 7`，依此类推。这一映射是纯函数——你可以从全局序列还原 `(local_seq, shard_id)`：`shard_id = global_seq % num_shards`，`local_seq = global_seq / num_shards`。完整讨论见[分片](sharding.md)。

`RecordId.sequence` 始终保存的是**全局**序列。无论分片数为多少，`read(global_seq)` 都能正常工作，因为段是按全局序列索引的。

## 段（Segments）

**段**是一个仅追加文件，保存一段连续的记录序列。

- **滚动：** 当活跃段达到 `config.segment_size`（默认 256 MiB）时，它会被滚动并创建一个新段。下一个段会在当前段达到 80% 容量时**预创建**，从而把滚动期的阻塞从 create+fsync 的停顿降到一次 `fdatasync`。
- **命名：** `segment-NNNNNNNN.log`，其中 `NNNNNNNN` 是零填充的段 id（从 `00000001` 开始）。
- **定位：** 每个段头部记录了它的 `base_sequence`，因此一个记录 id 可以通过一个按目录 mtime 失效的缓存清单在 O(log N) 内唯一映射到一个段。
- **保留与截断：** `RetentionPolicy`（`KeepAll`、`MaxBytes`、`MaxAge`）约束保留多少历史。完全低于 WAL 检查点的旧段会在下次滚动时被截断。

段一旦滚动就不可变，这使得崩溃恢复很简单：恢复扫描活跃段的尾部、检测撕裂写并截断它们。

## 环形缓冲与槽（Ring buffer & slots）

生产者**不**直接写段文件，而是写入一个固定大小的**环形缓冲**（由槽组成的环），由后台 Committer 线程把槽排空到活跃段中。

- **无锁快路径：** 生产者通过对生产者游标做 CAS（compare-and-swap）来占有槽。写入路径上没有互斥锁——`append` 的扩展性由无竞争的 CAS 吞吐决定。
- **槽**（`src/ring/slot.rs`）每个容纳一条记录。生产者独占地写入内容（由 claim 保证），然后对槽的 `sequence` 字段做一次 `seq + 1` 的 `Release` 写入来发布。
- **消费者**（Sealer、Committer）通过 `Acquire` 读观察到发布。槽的复用受消费水位门控——只有当 Committer 把某槽排空到落后生产者游标 `ring_size` 之外时，该槽才能被回收。
- **多分片**（`config.shards > 1`）：每个分片一个环，Committer 轮询所有环，保持分片间的全局顺序。

`config.ring_size`（默认 8192）是每个分片的槽数。如果生产者比 Committer 快超过 `ring_size`，`append` 会按 `queue_full_policy`（`Block` 或 `Drop`）处理。

## 游标语义

logdb 暴露**三个**游标。理解它们的差异对于推理可见性与崩溃安全至关重要：

```rust
impl LogDb {
    pub fn producer_cursor(&self) -> u64;  // 各分片的最大值
    pub fn committed_cursor(&self) -> u64; // 各分片的最小值
    pub fn durable_cursor(&self) -> u64;   // 各分片的最小值
}
```

| 游标                | 含义                                                                | 推进者                |
|---------------------|---------------------------------------------------------------------|-----------------------|
| `producer_cursor`   | 生产者将要占有的下一个序列。记录发布后即可被消费者看到。               | `append`（CAS 占有）  |
| `committed_cursor`  | Committer 已序列化并写入段文件、但**尚未 fsync** 的记录。             | Committer 线程        |
| `durable_cursor`    | 已**fsync** 到磁盘的记录。能在崩溃中幸存。                           | Committer 在 `fdatasync` 之后 |

对于读者而言的关键规则——来自 `src/reader/mod.rs` 模块文档：

> 所有读取都以 `durable_cursor` 为界：只有已 fsync 的数据对读者可见。这保证了被读到的记录能在崩溃中幸存。

也就是说，即便记录已经被写入并提交，只要 `record_id >= durable_cursor()`，`read(record_id)` 就会返回 `Ok(None)`。这是有意为之：它意味着**读者能看到的任何记录都能在崩溃中幸存**。不存在“现在能读到、崩溃后却丢失”的窗口。

由此带来的后果：

- `flush()` 等待的是 `durable_cursor`（而非 `committed_cursor`）越过你写入的记录——这正是 `flush` 作为真正持久化屏障的原因。
- 崩溃重启后，恢复会截断任何“已提交但未 fsync”的尾部记录，恢复“磁盘上的日志恰好结束于 durable 游标处”这一不变量。
- 用于延迟监控时：`producer_cursor` 与 `committed_cursor` 之间差距大说明 Committer 已饱和；`committed_cursor` 与 `durable_cursor` 之间差距大说明存储层受限于 fsync。

## Inline 与 Spill

logdb 中最重要的性能事实是 **256 字节边界**（`src/ring/slot.rs` 中的 `INLINE_CAP`）：

- **Inline 路径**（记录 ≤ 256 字节）：内容直接内嵌在槽中，跨线程**零堆分配、零额外 memcpy**。p50 写入延迟通常 **<100 ns**。
- **Spill 路径**（记录 > 256 字节）：写入线程执行一次堆分配（`Box<[u8]>`）并对内容做完整 memcpy。spill 路径的吞吐大约**慢 4×**，而由于分配器抖动，p99.9 尾延迟约**高 80×**（观测值：300 字节时 inline p99.9 ≈ 500 ns，spill p99.9 ≈ 41 µs）。

256 字节覆盖了绝大多数结构化日志记录（JSON 日志行、审计事件、指标采样），同时让每个槽保持缓存行友好（inline 存储恰好占用 4 个缓存行）。对于延迟敏感的负载，**请把记录保持在 ≤ 256 字节**以留在 inline 快路径上。

边界是精确的：256 字节的记录走 inline；257 字节的记录会 spill。

## 相关链接

- [使用指南总览](README.md)
- [快速开始](getting-started.md)
- [持久化](durability.md)
- [分片](sharding.md)

> logdb 0.2.0

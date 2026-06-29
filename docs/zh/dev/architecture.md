# 架构

logdb 是一个内嵌式、只追加的日志数据库。本页描述写路径、后台线程模型、协调它们的游标与水位线，以及读路径。在改动 `src/` 中任何代码之前，请先读懂这张地图。

## 目录

- [写路径](#写路径)
- [线程模型](#线程模型)
- [游标与水位线](#游标与水位线)
- [读路径](#读路径)
- [相关链接](#相关链接)

## 写路径

一条记录从生产者线程一路走到 segment 文件，共经过四个阶段。前三个阶段无锁；只有最后一个阶段触碰磁盘，且由单一线程独占。

```
多个 Producer 线程
     │  LogDb::append / append_batch / replicate
     ▼
┌──────────────────────────────────┐
│  Ring（可分片）                    │  ← 无锁 CAS claim / claim_batch
│  Slot { 内容, hash_n, seq }       │  ← inline ≤ INLINE_CAP (256B)，否则堆 spill
└─────────────┬────────────────────┘
              │  （可选）已 publish 的 slot
              ▼
       Sealer 线程                 ← BLAKE3 keyed 哈希链
              │  （仅当启用 `hash-chain` 特性 且 shards == 1）
              ▼
       Committer 线程              ← 批量序列化 + pwrite + fdatasync
              │
       ┌────────┴─────────┐
       │  segment-*.log    │       ← 只追加，达到 segment_size 即滚动
       └──────────────────┘
              │
       Reader / Pusher              ← 点查 / 范围扫描 / 远程推送
```

逐阶段说明：

1. **Claim。** 生产者通过 CAS 推进每分片 `Ring`（`src/ring/mod.rs` 的 `Ring::claim` / `Ring::claim_batch`）中的 `producer_cursor` 来预约序列号。`claim_batch` 用单次 CAS 预约连续的 `n` 个序列，整批要么全部预约要么完全不预约——绝不会留下会让 Committer 卡住的“已预约但未写入”空洞。
2. **写入 + publish。** 生产者独占访问 `slots[seq & mask]`，调用 `Slot::producer_write`（内容 ≤ `INLINE_CAP = 256` 字节则内联存储，否则 spill 到堆），随后 `Slot::publish` 对 slot 的 `sequence` 字段做 `Release` 写入 `seq + 1`。
3. **（可选）Seal。** 若启用 `hash-chain` 特性 **且** `shards == 1`，Sealer 线程扫描已 publish 的 slot，计算 `hash_n = BLAKE3_keyed(hash_init, prev_hash || content)`，通过 `Slot::write_hash` 写回 slot，随后推进 `sealed_cursor`。
4. **Commit + fsync。** Committer 线程扫描已 publish（启用哈希时还需已 sealed）的 slot，序列化一个批次，对活动 segment 执行 `pwrite`，推进 `committed_cursor`，再 `fdatasync` 并推进 `durable_cursor`。当活动 segment 达到 `segment_size` 时，`SegmentManager` 滚动到新 segment。

**只有**启用哈希时，Sealer 才位于 publish 与 commit 之间；不启用哈希时，Committer 直接消费已 publish 的 slot。

## 线程模型

`LogDb::open`（`src/lib.rs`，约 L90-224）构造共享状态并 spawn 后台线程。线程集合是**有条件的**：

| 线程     | 是否由 `open` spawn？                                | 入口函数                            | 职责                                                                                                          |
|----------|------------------------------------------------------|-------------------------------------|---------------------------------------------------------------------------------------------------------------|
| Committer | **总是 spawn。**                                     | `pipeline::committer::run_committer` | 轮询所有 ring，序列化到活动 segment，fsync，推进 `committed_cursor` / `durable_cursor`。                       |
| Sealer   | 仅当启用 `hash-chain` **且** `shards == 1` 时。      | `pipeline::sealer::run_sealer`      | 计算 BLAKE3 keyed 哈希链，推进 `sealed_cursor`。                                                              |
| Pusher   | **绝不由 `open` spawn。**                            | `pusher::run_pusher`                | 把已持久化的记录推送到远端 sink。属于**守护进程级**组件，由外部服务（如 `logdbd`）spawn。                       |

有两个点很容易弄错，特别强调：

- **Pusher 不会由 `LogDb::open` 启动。** `pusher` 模块是私有的（`src/lib.rs:37` 写的是 `mod pusher;`，而非 `pub mod`），`run_pusher` 只由嵌入它的服务通过 `PusherHandle::spawn` 启动。库自身从不启动 pusher 线程。远端失败永远不会反向压迫本地写入。
- **`shards > 1` 时启用哈希会被拒绝。** 全局哈希链需要单分片顺序，因此 `open` 会在 `hash_enabled && config.shards > 1` 时返回错误（Sealer 只在 shard 0 上 spawn）。多分片哈希链留待后续实现。

### 关停协调

`ShutdownState`（`src/pipeline/signal.rs`）是 appender 与后台线程共享的三阶段状态机：

- **阶段 0 — Run。** 正常运行；`append` 在 claim 之前调用 `enter()`（预约一个 in-flight 名额），在 publish 之后调用 `leave()`。
- **阶段 1 — Drain。** 由 `start_drain()` 进入。新的 append 以 `ShuttingDown` 被拒绝；`drain()` 等待 `in_flight` 归零，再等待 Committer 把数据 fsync 到 producer cursor。后台线程在处理完 drain 目标后通过 `should_stop()` 退出。
- **阶段 2 — Abort。** 由 `abort()` 进入（`Drop` 时也会触发）。强制线程停止，不再等待持久化。

`enter()` 采用“先加再查”的顺序，关闭了 otherwise 会让 `start_drain` 在并发 append 增计数之前就看到 `in_flight == 0` 的 TOCTOU 窗口。

`drain(&self)` 是共享安全的 drain 路径：它取 `&self`，因此当 `LogDb` 以 `Arc` 在长寿命服务内共享时也能工作。`shutdown(self)` 消费 handle、执行 drain，然后 join Committer（和 Sealer）线程。

## 游标与水位线

每个 `Ring` 携带四个单调游标（`src/ring/mod.rs`）：

| 游标               | 含义                                                          | 推进者                       |
|--------------------|---------------------------------------------------------------|------------------------------|
| `producer_cursor`  | 生产者将要 claim 的下一个序列。                                | `claim` / `claim_batch`（CAS）|
| `sealed_cursor`    | Sealer 已为其计算出 `hash_n` 的序列（仅 hash-chain）。         | Sealer 线程                  |
| `committed_cursor` | Committer 已 `pwrite` 到活动 segment 的序列。                  | Committer 线程               |
| `durable_cursor`   | Committer 已 `fdatasync` 的序列。崩溃后仍存活。                | Committer 线程               |

跨分片时，`LogDb` 暴露的是聚合值：`producer_cursor()` 是所有分片的**最大值**（最坏情况下的持久化目标），而 `committed_cursor()` 与 `durable_cursor()` 是所有分片的**最小值**（最慢的分片决定可见性）。读取以最小 durable cursor 为上界。

### 消费水位线与 claim 不变式

slot 复用由单一水位线（`Ring::consume_watermark`）控制：

- 启用 hash-chain：`min(sealed_cursor, committed_cursor)`
- 未启用 hash-chain：`committed_cursor`

因为内容就存在 slot 里（没有单独的 arena），所以只有一个资源、一个水位线——不可能出现双水位线协调问题。

**claim 不变式**（`Ring::claim`，`src/lib.rs:226-303`）：

> 只有当 `seq - consume_watermark < ring_size` 时，`seq` 对应的 slot 才会被 claim。

这保证了消费者（Sealer 或 Committer）已读完一个 slot，生产者才可能覆盖它。`claim_batch` 使用同样的判据 `in_flight + n <= ring_size`。`replicate` 路径（备机按精确序列写入）在写入前也强制同一不变式。在 `QueueFullPolicy::Block` 下，生产者用 spin/yield/sleep 退避等待水位线推进；在 `Drop` 下立即返回 `AppendError::QueueFull`。

## 读路径

读取从不触碰 ring buffer——它直接读 segment 文件。读路径分为三层（`src/reader/mod.rs`）：

1. **SegmentManifest —— 缓存的目录列表。** 一份有序的内存列表 `(segment_id, path, base_sequence, flags)`。**仅当数据目录的 mtime 发生变化时**（一次滚动或一次 retention 删除）才重新刷新。向活动 segment 追加不会改变目录 mtime，因此缓存在两次滚动之间始终有效。在 mtime 不可用的文件系统上退化为每次都刷新（正确，只是更慢）。
2. **O(log N) 的 segment 查找。** `SegmentManifest::find(seq)` 用 `partition_point`（二分）找到最大的 `base_sequence <= seq`。结果被 clone 出锁，因此文件 I/O 不在 manifest 的 mutex 内进行。
3. **稀疏索引锚点 + 顺序扫描。** 对原始 segment，稀疏索引（`src/storage/index.rs`）提供一个不晚于目标 id 的锚点；reader seek 到该文件偏移并顺序向前扫描到目标记录。基于 frame 的 segment（压缩或加密）从 segment header 开始按 frame 迭代。两种情况下，只有当记录的 `sequence` 落在该 segment 的范围内时才返回。

所有读取以 `durable_cursor` 为上界：当 `record_id >= min_durable_cursor` 时，`read(record_id)` 返回 `Ok(None)`，保证**读到的每一条记录都能在崩溃中存活**。

## 相关链接

- [开发指南首页](README.md)
- [项目结构](project-layout.md) —— `src/` 的逐模块地图。
- [存储格式](storage-format.md) —— 磁盘上的 segment、header、index 与 frame 布局。
- 概念：[游标语义](../usage/concepts.md#游标语义)

> logdb 0.2.0

# 项目结构

logdb 源码树中“什么放在哪里”的地图。改动行为之前，先用它找到正确的文件。

## 目录

- [顶层结构](#顶层结构)
- [`src/` 模块地图](#src-模块地图)
- [相关链接](#相关链接)

## 顶层结构

| 路径         | 内容                                                              |
|--------------|-------------------------------------------------------------------|
| `src/`       | 库 crate —— 下面列出的所有模块。                                  |
| `benches/`   | 基准测试（吞吐、inline vs spill 延迟、分片）。                    |
| `examples/`  | 可运行的示例：基础追加/读取、tailer、远程推送、哈希链。            |
| `fuzz/`      | `cargo-fuzz` 目标：序列化、恢复、ring buffer。                    |
| `tests/`     | 集成测试，端到端覆盖公开 API。                                    |
| `scripts/`   | 构建、基准、发布辅助脚本。                                        |

## `src/` 模块地图

`src/` 中的每一个模块，及其职责与所承载的关键类型。

| 模块                   | 职责                                                                                                                            | 关键类型 / 条目                                            |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| `lib` (`lib.rs`)       | crate 根。持有 `LogDb` handle，编排 `open`、`append`/`append_batch`/`replicate`、`flush`、`drain`、`shutdown` 及各类游标访问器。    | `LogDb`、`LogDbInner`、`RecoveryReport`                    |
| `config`               | 全部配置，构造时校验。                                                                                                          | `Config`、`QueueFullPolicy`、`DurabilityMode`、`CommitTrigger`、`WaitStrategy`、`RetentionPolicy` |
| `error`                | 追加、flush、读取、关停路径上的公开错误类型。                                                                                    | `AppendError`、`FlushError`、`ReadError`、`ShutdownError`、`ShutdownReport` |
| `record`               | 逻辑记录标识、内存中的零拷贝读视图、以及磁盘记录。`RecordId` 采用 Kafka 分区-偏移语义（**不**编码分片/节点 id）。                  | `RecordId`、`Record`、`ReadView`                           |
| `ring` (`ring/mod.rs`) | 无锁 ring buffer：基于 CAS 的 `claim`/`claim_batch`、缓存行对齐的 `producer_cursor`，以及四个游标。                              | `Ring`、`CachePadded`                                      |
| `ring::slot`           | slot —— ring 的存储单元。inline（≤ `INLINE_CAP`）vs spill，CAS publish 协议，以及 `write_hash`。                                 | `Slot`、`SlotInner`、`INLINE_CAP`（= 256）                  |
| `shard`                | 多 ring 支持：线程亲和的分片选择，以及位编码的全局 record id。                                                                    | `ShardMap`、`encode_record_id`、`decode_record_id`         |
| `pipeline`             | 后台流水线线程。包含 `committer`、`sealer`、`signal`、`trigger` 子模块；线程入口位于 `committer`/`sealer`。                        | `run_committer`、`run_sealer`                              |
| `pipeline::committer`  | 常驻的 Committer：轮询所有 ring，序列化到 segment，fsync，推进 committed/durable 游标。                                          | `run_committer`                                            |
| `pipeline::sealer`     | 可选的 Sealer：BLAKE3 keyed 哈希链。由 `hash-chain` 特性门控。                                                                    | `run_sealer`、`blake3_keyed_chain`、`sha256_chain`         |
| `pipeline::signal`     | flush 的请求/完成，以及三阶段关停状态机。                                                                                         | `FlushSignal`、`ShutdownState`                             |
| `pipeline::trigger`    | Commit 触发阈值，以及流水线线程使用的 spin/yield/park 退避。重新导出 config 中的触发类型。                                          | `Backoff`、`CommitTrigger`、`WaitStrategy`                 |
| `storage` (`mod.rs`)   | segment 文件管理。`SegmentManager` 由 Committer 线程独占（`!Sync`，无需锁）。                                                     | `SegmentManager`、`SegmentError`                           |
| `storage::format`      | 磁盘上的 segment 与 record 布局：header、标志位、frame 封装、（反）序列化、哈希算法标签。                                          | `SegmentHeader`、record/frame 序列化、各 flag 常量         |
| `storage::index`       | 用于在 segment 内为读取定位锚点的稀疏索引。                                                                                       | `SparseIndex`、`IndexEntry`                                |
| `reader` (`mod.rs`)    | 按 id 或范围查询记录。持有缓存的 `SegmentManifest`，以及稀疏索引锚点 + 顺序扫描的读路径。                                          | `Reader`、`SegmentManifest`                                |
| `reader::iter`         | 在 segment 文件上向前扫描的迭代器。                                                                                               | `RecordIter`                                               |
| `recovery`             | 崩溃恢复：扫描 segment、校验 header、检测并截断 torn write、重建稀疏索引、返回重建后的状态。                                       | `recover`、恢复状态                                        |
| `tailer`               | 命名消费者，各自独立、可持久化的读取进度（`tailer_<name>.dat`）。                                                                 | `Tailer`                                                   |
| `pusher`（**私有**）   | 把已持久化的记录推送到用户提供的 `RemoteSink`。属于**守护进程级组件**——模块为私有（`mod pusher;`），**不**由 `LogDb::open` spawn；由嵌入服务通过 `PusherHandle::spawn` 启动。 | `run_pusher`、`PusherHandle`、`RemoteSink`、`PushError`    |
| `health`               | 用于自愈错误条件（如 ENOSPC）的共享健康状态。                                                                                     | `HealthState`、`HEALTH_OK`/`HEALTH_DISK_FULL`/`HEALTH_IO_ERROR` |
| `platform`             | 平台相关的 I/O 与时间原语（`fdatasync`、目录同步、粗粒度实时时钟）。                                                              | `fdatasync`、`sync_dir`、`clock_realtime_coarse_ns`        |

两个点特别说明：

- **`pusher` 是私有的。** `src/lib.rs:37` 声明为 `mod pusher;`（而非 `pub mod`），`run_pusher` 从不被 `LogDb::open` 调用，由嵌入的守护进程（如 `logdbd`）通过 `PusherHandle::spawn` 启动。库自身从不启动 pusher 线程。
- **`sealer` 受特性门控。** 模块标注 `#[cfg(feature = "hash-chain")]`，且 `run_sealer` 仅在 `shards == 1` 时于 shard 0 上 spawn。

## 相关链接

- [开发指南首页](README.md)
- [架构](architecture.md) —— 数据流、线程、游标与读路径。
- [存储格式](storage-format.md) —— `storage::format` 对应的磁盘布局。

> logdb 0.2.0

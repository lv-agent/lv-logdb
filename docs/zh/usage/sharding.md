# 分片（Sharding）

分片把 logdb 的写入路径拆到多个独立的 ring 上，使不同线程上的写入方不必在单一队列上争用。把 `Config.shards` 设为 ring 的数量；每个分片有自己的环形缓冲区、游标和 producer 线程。本页讲解序号映射、v1.1 的 hash-chain 不兼容性，以及何时分片、何时保持单分片。

> **生产就绪：**自 0.2.0 起，`shards > 1` 已端到端可用且持久——append、点查、范围扫描/replay、崩溃恢复、tailer 在跨分片下均正确工作（含非 2 的幂分片数与段滚动）。唯一剩余的限制是特性层面的，而非正确性层面的：`hash-chain` 要求 `shards == 1`（见下文）。远程推送（`replicate`/Pusher）目前仅支持单分片。

## 目录

- [多个 ring](#多个-ring)
- [序号映射](#序号映射)
- [hash-chain 不兼容](#hash-chain-不兼容)
- [何时分片](#何时分片)
- [相关链接](#相关链接)

## 多个 ring

`Config.shards`（`src/config.rs`，默认 `1`，范围 `[1, 256]`）设置 `ShardMap`（`src/shard.rs:65-163`）管理的独立 ring 数量。每个 ring 有：

- 自己的 `ring_size` 个槽（按 `ring_size / shards` 取值，向上取整为 2 的幂且下限为 16），
- 自己的 producer 与 consumer 游标，
- 自己的背压水位。

Committer 轮询**全部** ring（`ShardMap::all_rings`，`src/shard.rs:134-136`），因此持久化与 durable 游标在分片间是全局的。线程通过对线程 ID 做哈希被映射到某个分片（`ShardMap::select_shard`，`src/shard.rs:142-151`）以保持亲和性，让同一写入方跨调用落在同一分片，从而进一步降低跨线程争用。

```rust
use logdb::Config;

let config = Config {
    shards: 4, // 四个独立 ring
    ..Config::default()
};
let db = logdb::LogDb::open(config)?;
```

## 序号映射

一条记录的位置是单个 `u64` 的**全局序号**，由 `LogDb::append` 返回。其概念性映射见 `RecordId.sequence` 文档（`src/record.rs:25-28`），把不同分片的序号交错编排：

> `global_seq = local_seq * num_shards + shard_id`

实际落盘的编码是按位打包以追求速度（`encode_record_id`，`src/shard.rs:44-52`）：`global_id = (local_seq << shard_bits) | shard_id`，其中 `shard_bits = ceil(log2(num_shards))`。当 `num_shards` 是 2 的幂时，按位打包的形式与上面的乘法形式完全一致（左移 `shard_bits` 等价于乘以 `2^shard_bits = num_shards`）；不是 2 的幂时，按位打包的形式才是 `append` 与 `read` 实际使用的形式。解码为其逆运算（`decode_record_id`，`src/shard.rs:54-63`）。

### 示例：4 个分片

`shards = 4` 时，`shard_bits = 2`，因此全局序号的低 2 位承载分片 id，高位承载分片内本地序号。在分片 0、2、1 上并发写入三条记录（每个分片的首写）得到：

| 分片 | `local_seq` | `global_seq`（`local_seq << 2 | shard_id`） |
|-------|------------|-------------------------------------------|
| 0 | 0 | `0b0000` = `0` |
| 2 | 0 | `0b0010` = `2` |
| 1 | 0 | `0b0001` = `1` |

分片 2 的下一次写入（它的第二条记录，`local_seq = 1`）映射到 `(1 << 2) | 2 = 6`。消费方读取全局序号时看到的是交错但**稳定**的顺序：在分片内是写入序，且编码不会把两条记录别名到同一个 `global_seq`。

因为低 `shard_bits` 位是分片 id，所以点读只用掩码加移位就能从裸 `global_seq` 还原 `(shard_id, local_seq)`，不需要单独的分片查找索引。

## hash-chain 不兼容

在 v1.1 中，[`hash-chain`](features.md#hash-chain) 特性与分片**不兼容**。跨分片的哈希链需要全局合并顺序——Sealer 必须按单一确定的顺序对记录哈希——但 v1.1 一次只封存一个分片。当 `hash-chain` 启用且 `shards > 1` 时，`LogDb::open` 返回这条确切的错误（`src/lib.rs:176-181`）：

> hash-chain is not supported with shards > 1 in v1.1. Use shards=1 with hash-chain, or shards>1 without hash.

这是**当前版本的限制**，并非永久的设计取向。多分片哈希链（全局合并有序的 Sealer）推迟到 v1.2。在此之前，二选一：

- 用 `shards = 1` 配 `hash-chain` 获得防篡改能力，或
- 用 `shards > 1` 但不开 `hash-chain` 获得写入吞吐。

## 何时分片

分片是吞吐/延迟的取舍。只有当写入方争用成为瓶颈时才分片，不要无脑开。

**在以下情形分片（`shards > 1`）：**

- **核多、写入方多。** 若你有几十个 producer 线程，且单 ring 的 claim/publish 显现争用（表现为负载下 p99 追加延迟升高），分片把写入摊到多个 ring，让每个线程的写入方各撞各的队列。
- **写入吞吐占主导。** 在到达 Committer 的序列化/fsync 吞吐上限之前，分片让每 ring 锁不再串行化各 producer，从而使追加上限随 `shards` 近似线性提升。

**在以下情形保持单分片（`shards = 1`）：**

- **点读延迟占主导。** 点读是在 segment 内的稀疏索引 seek 加向前扫描（见 [Reading](reading.md)）。单分片时序号稠密（`0, 1, 2, …`），segment manifest 与文件的映射干净利落。分片多时低位是分片 id，manifest 与扫描路径每次查找都要多干一点活。
- **需要 `hash-chain`。** v1.1 中防篡改要求单分片（见上文）。
- **单一写入方或 producer 很少。** 只有一两个写入方时没什么可摊的——额外的分片只会增加 Committer 轮询开销并让运维工具更复杂。

### 对读取与扫描的影响

- **点读**（`LogDb::read`）从序号解码出分片 id 并读对应 segment；代价是一次解码加上常规的稀疏索引扫描。每次查找的开销很小，但 `index_stride` 只对原始的、单分片式 segment 有帮助——见 [Configuration](configuration.md#为延迟敏感点读调低-index_stride)。
- **范围扫描**跨分片读取。由于序号交错，全局范围扫描会合并各分片流；durable 游标是全局的，因此扫描看到的是 Committer 在所有分片上已 fsync 的一致切面。
- **Tailer**（[tailers.md](tailers.md)）跟随全局 durable 游标，在 API 层面不受分片数影响。

## 相关链接

- [使用指南](README.md)
- [特性](features.md)——`hash-chain` 细节与单分片约束。
- [写入](writing.md)——`claim` 如何产出 `(global_seq, shard_id, local_seq)` 三元组。
- [读取](reading.md)——受分片影响的点读与扫描路径。
- [配置](configuration.md)——`shards` 字段（`Config.shards`，范围 `[1, 256]`）。

> logdb 0.2.0

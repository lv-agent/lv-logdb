# 存储格式

logdb 数据目录在磁盘上的二进制布局——segment 文件、segment header、记录帧、稀疏索引、压缩/加密 frame、哈希链，以及原子写入的元数据文件。

> 对 **logdb 0.2.0** 具权威性。代码变更时请以 `src/storage/format.rs` 与 `src/storage/index.rs` 为准重新核对。

logdb 是一个只追加、崩溃安全的日志。磁盘上的每一字节要么是 segment（日志本身），要么是稀疏索引（原始 segment 的可重建加速结构），要么是一个 12 字节的元数据文件。所有多字节整数均为**小端序**。所有校验和均为 **CRC32C**（Castagnoli，`crc32c` crate）；哈希链（启用时）为 **BLAKE3 keyed**。

## 数据目录布局

一个数据目录即一个分区的存储。其内容为：

| 路径                         | 类型   | 是否真源       | 说明                                                                  |
|------------------------------|--------|----------------|-----------------------------------------------------------------------|
| `segment-NNNNNNNN.log`       | Segment | 是             | `NNNNNNNN` 为 8 位零填充的 `segment_id`，自 `00000001` 起递增。       |
| `segment-NNNNNNNN.idx`       | 索引   | 否（可重建）   | 原始 segment 的稀疏索引；压缩/加密 segment 不生成此文件。             |
| `checkpoint.dat`             | 元数据 | 是             | 最近一次持久化的序列号。                                              |
| `tailer_<name>.dat`          | 元数据 | 是             | 每个 named tailer 一份，记录其独立读取位置。                          |
| `pusher_progress.dat`        | 元数据 | 是             | pusher 最近一次成功推送的序列号。                                     |

全新数据库总是从 `segment-00000001.log` 开始。当活动 segment 达到配置的 `segment_size` 时触发滚动；每次滚动分配下一个 `segment_id`，并写入一个链接到前一段的新 segment header。

## Segment header（128 字节）

每个 `.log` 文件以定长 `SEGMENT_HEADER_SIZE = 128` 字节的 header 起始（`src/storage/format.rs:58`）。该 header 在 segment 创建时写入一次，之后**不会**原地重写（时间戳范围字段为概念性字段——仅在未来的 header 重写路径中回填；下方字段为磁盘上权威布局）。

### 字节布局

| 偏移 | 大小 | 字段               | 类型      | 说明                                                                  |
|------|------|--------------------|-----------|-----------------------------------------------------------------------|
| 0    | 4    | `magic`            | u32 LE    | `0x4C474442`（"LGDB"）。用于拒绝非 logdb 文件。                       |
| 4    | 2    | `format_version`   | u16 LE    | `0x0001`（`FORMAT_VERSION`）。                                        |
| 6    | 1    | `flags`            | u8        | 位掩码，见下文。                                                      |
| 7    | 1    | `hash_algo`        | u8        | `1`=SHA256、`2`=BLAKE3（`HASH_ALGO_*`）。                             |
| 8    | 32   | `hash_init`        | [u8; 32]  | BLAKE3 keyed 模式的密钥；CSPRNG 生成，**持久化于此**，重启后恢复。    |
| 40   | 8    | `base_sequence`    | u64 LE    | 本 segment 存储的首个序列号。                                         |
| 48   | 4    | `partition_id`     | u32 LE    | 逻辑分区标识。                                                        |
| 52   | 4    | `segment_id`       | u32 LE    | 自 1 单调递增。                                                       |
| 56   | 8    | `min_timestamp_ns` | u64 LE    | 最早记录时间戳（回填）。                                              |
| 64   | 8    | `max_timestamp_ns` | u64 LE    | 最新记录时间戳（回填）。                                              |
| 72   | 4    | `header_crc`       | u32 LE    | 对字节 `[0, 72)` 计算的 CRC32C（`HEADER_CRC_END`，`src/storage/format.rs:61`）。 |
| 76   | 32   | `prev_last_hash`   | [u8; 32]  | 前一段的最终 `hash_n`（链式衔接；首段为零）。                         |
| 108  | 1    | `record_format`    | u8        | 记录编码版本（`1` = `RECORD_FORMAT_V1`）。                            |
| 109  | 19   | `_reserved`        | 19 字节   | 零填充；为未来扩展保留。                                              |
| 128  |      |                    |           | END                                                                   |

`SegmentHeader::serialize`（`src/storage/format.rs:116-136`）按此布局写入，最后才填入 `header_crc`；`deserialize`（`src/storage/format.rs:139-198`）在信任任何字段之前先校验 `magic` 并对 `[0, 72)` 重新计算 CRC，因此撕裂或被篡改的 header 会被拒绝。

### `flags` 位掩码

| 位 | 掩码 | 常量                        | 含义                                                                |
|----|------|-----------------------------|---------------------------------------------------------------------|
| 0  | 0x01 | `FLAG_NOT_FIRST`            | 除首段外每一段都置位（链式 segment）。                              |
| 1  | 0x02 | `FLAG_HASH_ENABLED`         | 启用哈希链；记录的 `hash_n` 字段有意义。                            |
| 2  | 0x04 | `FLAG_COMPRESSED_ZSTD`      | 记录以 zstd 压缩的 frame 打包（frame 布局）。                       |
| 3  | 0x08 | `FLAG_ENCRYPTED_AES256GCM`  | 记录以 AES-256-GCM 加密的 frame 打包（frame 布局）。                |

> 定义于 `src/storage/format.rs:64-71`。**只要 `FLAG_COMPRESSED_ZSTD` 或 `FLAG_ENCRYPTED_AES256GCM` 中任意一位置位，segment 即切换到下文描述的 frame 布局**；原始 segment（两者皆未置位）使用普通记录帧。

## 记录帧（原始 segment）

在原始 segment（`flags & 0x0C == 0`）中，记录在 128 字节 header 之后逐条追加。每条记录自描述且自校验（`src/storage/format.rs:253-359`）：

| 字段          | 类型    | 大小 | 记录内偏移 | 说明                                                |
|---------------|---------|------|------------|-----------------------------------------------------|
| `len`         | u32 LE  | 4    | 0          | 记录总字节数，含本字段与 `crc`。                    |
| `sequence`    | u64 LE  | 8    | 4          | 分区内序列号。                                      |
| `timestamp_ns`| u64 LE  | 8    | 12         | 记录时间戳。                                        |
| `content_len` | u32 LE  | 4    | 20         | `content` 的长度。                                  |
| `content`     | [u8]    | N    | 24         | 载荷字节（`N = content_len`）。                     |
| `hash_n`      | [u8;32] | 32   | 24+N       | BLAKE3 keyed 链哈希；未启用哈希时为零。             |
| `crc`         | u32 LE  | 4    | 56+N       | 对 `[0, 56+N)` 计算的 CRC32C（`len` 字段按零处理）。|

记录最小长度为 `MIN_RECORD_SIZE = 60` 字节（`src/storage/format.rs:254`）：

```
MIN_RECORD_SIZE = 4 + 8 + 8 + 4 + 0 + 32 + 4 = 60   （零长度 content）
record_size(N)  = 4 + 8 + 8 + 4 + N + 32 + 4 = 60 + N
```

`deserialize_record`（`src/storage/format.rs:299-359`）读取 `len`，拒绝短于 `MIN_RECORD_SIZE` 的缓冲，交叉校验 `len == record_size(content_len)`，随后将 `len` 字段视为零、对其余记录体重新计算 CRC32C 并与存储的 `crc` 比对。任何不一致（损坏、截断、半写）均返回错误，读路径越过该记录继续。计算 CRC 时将 `len` 置零，使磁盘上的长度本身通过其余帧结构间接纳入校验。

```
 ┌──────┬──────────┬──────────────┬─────────────┬─────────┬───────────┬───────┐
 │ len  │ sequence │ timestamp_ns │ content_len │ content │  hash_n   │  crc  │
 │ u32  │   u64    │     u64      │    u32      │  [u8]N  │  [u8;32]  │ u32   │
 └──────┴──────────┴──────────────┴─────────────┴─────────┴───────────┴───────┘
  0      4           12             20            24         24+N        56+N     = len
```

## 稀疏索引（`.idx`）

原始 segment 配套一份稀疏索引（`src/storage/index.rs`），用于加速点查。该索引是**派生、可重建**的产物：缺失或损坏时，读路径通过扫描 segment 重建。

### `IndexEntry`（24 字节）

| 偏移 | 大小 | 字段            | 类型   | 说明                                |
|------|------|-----------------|--------|-------------------------------------|
| 0    | 8    | `sequence`      | u64 LE | 此锚点处的记录标识。                |
| 8    | 8    | `file_offset`   | u64 LE | 该记录在 `.log` 文件中的字节偏移。  |
| 16   | 8    | `timestamp_ns`  | u64 LE | 记录时间戳，供按时间查询使用。      |

`IndexEntry::SERIALIZED_SIZE = 24`（`src/storage/index.rs:28`）。

### 文件布局

`.idx` 文件结构为 `[stride: u32 LE][entries: IndexEntry × M]`：

```
┌──────────┬────────────────────────────────────────────┐
│ stride   │ IndexEntry[0] | IndexEntry[1] | ...        │
│ u32 LE   │   24 字节     |   24 字节     | ...        │
└──────────┴────────────────────────────────────────────┘
  0          4
```

`SparseIndex::DEFAULT_STRIDE = 1024`（`src/storage/index.rs:66`）：每 1024 条记录写一个锚点（`should_index(n)` 在 `n % stride == 0` 时为真）。索引路径由 `SparseIndex::index_path` 派生——`segment-00000001.log` → `segment-00000001.idx`。

### 基于锚点的读取

`SparseIndex::find_anchor(record_id)`（`src/storage/index.rs:91-102`）用二分查找最大的 `sequence <= record_id` 条目，返回 `(entry, position)`。读路径 seek 到 `entry.file_offset` 并向前逐条扫描至目标记录。若索引为空或目标早于首个被索引记录，返回 `None`。`find_by_time` 为时间戳查询提供类似的锚点。

### 仅适用于原始 segment

稀疏索引只有在记录位于已知、可独立 seek 的文件偏移时才有意义。压缩或加密 segment 把记录封进不透明的 frame，单条记录偏移不可 seek，因此**不为它们写入 `.idx`**（`fresh_index` 在任一标志置位时返回 `None`，`src/storage/mod.rs:307-313`）。frame segment 的读取从 segment header 起扫描。

## Frame 布局（压缩 / 加密 segment）

当 `flags` 置有 `FLAG_COMPRESSED_ZSTD` 或 `FLAG_ENCRYPTED_AES256GCM` 时，header 之后的区域是一串 **frame**，而非原始记录。每个 frame 在一个 8 字节 header 之后打包一条或多条记录。

### Frame header（8 字节）

`FRAME_HEADER_SIZE = 8`（`src/storage/format.rs:77`）。`read_frame_header` 返回 `(compressed_len, decompressed_len)`（`src/storage/format.rs:84-88`）：

| 偏移 | 大小 | 字段               | 类型   | 说明                                                |
|------|------|--------------------|--------|-----------------------------------------------------|
| 0    | 4    | `compressed_len`   | u32 LE | 磁盘上的载荷长度（`cl`）。                          |
| 4    | 4    | `decompressed_len` | u32 LE | 解码后的载荷长度（`dl`）；用于限定在 frame 内扫描记录的范围。 |

### Frame 布局

```
┌────────────────┬────────────────────────────────────────────────────────┐
│ frame_header   │  payload（磁盘上 compressed_len 字节）                 │
│  cl, dl (8B)   │  = encrypt?( compress?( raw_records ) )                │
└────────────────┴────────────────────────────────────────────────────────┘
```

写入时按从右到左的顺序组合变换得到 `payload`（`src/storage/mod.rs:466-490`）：

1. 拼接一条或多条原始记录（即上文的记录帧，不含 segment header）。
2. 若 `FLAG_COMPRESSED_ZSTD`：对拼接结果做 zstd 压缩。
3. 若 `FLAG_ENCRYPTED_AES256GCM`：对（可能已压缩的）字节做 AES-256-GCM 加密，并在载荷前加 12 字节 nonce。

读路径反向执行该管线（`decode_frame_payload`，`src/reader/mod.rs`）：读取 `cl` 字节，若加密则解密（剥离前导 nonce），若压缩则 zstd 解压，随后在解码后的缓冲内按 `dl` 字节上限逐条解析记录（`src/reader/mod.rs:256-268`）。

### 加密 nonce

`ENCRYPTION_NONCE_SIZE = 12`（`src/storage/format.rs:72`）。每个加密 frame 通过 `getrandom` 取一个新随机 nonce，作为载荷的前 12 字节存储（`src/storage/mod.rs:289-298`），因此磁盘上的载荷为 `{nonce:12B | ciphertext}`。AES-256-GCM 同时提供机密性与真实性；GCM 标签校验失败的 frame 会被拒绝。

### 关于打包的说明

一个 frame 可批量打包多条记录，以摊薄压缩/加密开销。读路径从 segment header 起按 frame 顺序遍历——不存在按记录的偏移索引——并在每个解码后的 frame 内使用与原始路径相同的 `deserialize_record` 解析记录。

## 哈希链

当 `FLAG_HASH_ENABLED` 置位时，每条记录携带 32 字节的 `hash_n`，将其串接为防篡改链。

- **算法：** BLAKE3 keyed 模式（`HASH_ALGO_BLAKE3`，v0.2.0 默认），由 Sealer 计算 `hash_n = BLAKE3_keyed(hash_init, prev_hash || content)`（`src/pipeline/sealer.rs:70-74`）。为兼容亦定义了 SHA256（`HASH_ALGO_SHA256`）。
- **`hash_init` 持久化在 segment header 中**，位于字节 `[8, 40)`（`src/storage/format.rs:123, 158-159`）。它按数据库由 CSPRNG 生成一次，**重启时从 segment header 恢复**——并非仅存于内存，也不是由用户提供的密钥派生。由于密钥与数据同盘存放，该链可检测篡改与意外损坏；若要抵御能读取该目录的攻击者，请额外启用 `FLAG_ENCRYPTED_AES256GCM`。
- **链式衔接：** 每个 segment 的 header 携带 `prev_last_hash`（字节 `[76, 108)`），即上一段的最终 `hash_n`，使链跨越 segment 边界。校验时从 `hash_init` 与 `prev_last_hash` 起回放记录，逐条核对 `hash_n`。

若禁用哈希，`hash_n` 写为 32 个零字节，且 `FLAG_HASH_ENABLED` 不置位。

## 元数据文件（12 字节，原子写入）

`checkpoint.dat`、`tailer_<name>.dat`、`pusher_progress.dat` 共享相同的 12 字节布局：

| 偏移 | 大小 | 字段 | 类型   | 说明                                |
|------|------|------|--------|-------------------------------------|
| 0    | 8    | `seq`| u64 LE | 最近持久化 / 已消费 / 已推送的序列号。 |
| 8    | 4    | `crc`| u32 LE | 对字节 `[0, 8)` 计算的 CRC32C。     |

来源：`src/lib.rs:668-683`（`save_checkpoint`）、`src/tailer.rs:16-51`（`PROGRESS_SIZE = 12`）、`src/pusher.rs:52-102`（`PROGRESS_FILE_SIZE = 12`）。

### 原子写入

三者都使用同一套崩溃安全流程写入，避免撕裂更新留下损坏的指针：

```
写临时文件  →  fdatasync(tmp)  →  rename(tmp → 最终名)  →  sync_dir(dir)
```

读取时校验长度恰为 12，且 `crc32c(seq_bytes)` 与存储的 `crc` 一致；任何不符均视为损坏并忽略该文件（checkpoint 回退到扫描 segment，tailer/pusher 重置为序列号 0）。

## 相关链接

- [开发指南首页](README.md)
- [架构](architecture.md) —— 读写路径在运行时如何消费此格式。
- 概念：[持久化与恢复](../usage/durability.md)

> logdb 0.2.0

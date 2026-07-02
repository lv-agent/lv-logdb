# logdbd SQLite 查询缓存设计

为 logdbd 增加 SQL 查询能力，通过内置 SQLite 作为查询缓存，使 Agent 应用可以直接查询日志数据，无需依赖外部工具。

## 动机

lv-logdb 当前仅支持按位置读取（按 record_id 点查、范围扫描、Tailer 消费）。
Agent 应用需要 SQL 风格的查询能力（按 `turn_id`、`event_type` 过滤、`COUNT`、`EXISTS`）。
将 SQLite 作为**查询缓存**内置到 logdbd 中——不是作为另一个数据库——提供完整的 SQL 接口，
同时保持 Segment 作为唯一真相源。

## 架构

```
┌──────────────────────────────────────────────────────────────────┐
│ logdbd                                                            │
│                                                                   │
│  ┌──────────┐                                                    │
│  │ Append ──┼──────▶ Committer ──────▶ Segment（真相源）           │
│  └──────────┘        线程              data_dir/                   │
│                           │                                       │
│                           │ committed cursor 推进                  │
│                           ▼                                       │
│                      Indexer ──────▶ <stream>.db x N（查询缓存）    │
│                      独立线程          cache_dir/                  │
│                                                                   │
│  ┌──────────┐         ┌─────────────────────────────────────┐     │
│  │ Query ───┼────────▶│ <stream>.db（cache_dir/，每个 stream  │     │
│  │ (SQL)   │          │  一个独立 SQLite 文件）               │     │
│  └──────────┘         │ 丢失/损坏 → segment 重建             │     │
│                       └─────────────────────────────────────┘     │
│                                                                   │
│  一致性：最终一致。Segment 先落盘，Indexer 异步追赶。               │
│  恢复：  Segment 始终可重建 SQLite。                                │
└──────────────────────────────────────────────────────────────────┘
```

### 核心原则

1. **Segment 是唯一真相源。** SQLite 纯粹是缓存。SQLite 文件丢失或损坏时，从 Segment 数据重建。
2. **最终一致性。** Committer 先写 Segment，Indexer 独立追赶 committed cursor。两者无事务耦合。
3. **零侵入 append 路径。** Committer 线程不做额外工作。Indexer 是独立线程。
4. **按 stream 隔离。** 每个 stream 一个 SQLite 文件。stream 之间完全隔离，WAL 互不干扰。
   跨 stream 查询由上层记忆管理层负责。

## 目录布局

```
data_dir/                      # 已有：Segment 文件（不变）
├── s0/
│   ├── segment-00000001.log
│   └── ...
├── s1/
│   └── ...
└── checkpoint.dat

cache_dir/                     # 新增：SQLite 查询缓存（每个 stream 一对文件）
├── <ns>.<stream1>.db          # stream1 活跃副本，Indexer 写入
├── <ns>.<stream1>.snap_{ts}.db  # stream1 快照
├── <ns>.<stream2>.db          # stream2 活跃副本
├── <ns>.<stream2>.snap_{ts}.db  # stream2 快照
└── meta.json                  # 各 stream 的 checkpoint seq、快照列表
```

`cache_dir` 默认为 `<data_dir>/../cache`，可通过配置指定。

## SQLite Schema

每个 stream 一个独立的 SQLite 文件，包含单张 `records` 表：

```sql
CREATE TABLE IF NOT EXISTS records (
    seq            INTEGER PRIMARY KEY,
    gid            INTEGER NOT NULL,
    ts_ns          INTEGER NOT NULL,
    event_type     TEXT NOT NULL,
    content_type   TEXT NOT NULL DEFAULT 'application/json',
    metadata_json  TEXT NOT NULL DEFAULT '{}',
    content        BLOB,
    deleted        INTEGER NOT NULL DEFAULT 0  -- 1 = 标记删除
);

CREATE INDEX IF NOT EXISTS idx_records_event_type ON records (event_type);
CREATE INDEX IF NOT EXISTS idx_records_ts ON records (ts_ns);
```

按 stream 配置可声明额外的 metadata 字段索引，用于高频查询字段。

## 写路径

```
Append RPC
  → Storage::append()         # 写入 ring buffer（已有路径）
  → Committer::commit()       # fsync Segment（已有路径）
     → committed cursor 推进
  → Indexer::poll()           # 独立线程
     → 按 stream 分组，通过 scan/replay 读取新 committed 记录
     → 路由到对应 <stream>.db，执行 INSERT
     → 更新该 stream 在内存中的 checkpoint seq
```

Indexer 复用已有的 `ScanIter` / `replay_from` API，和 Tailer 一样从 Segment 读取。
这意味着：**零改动核心 logdb 库和 Committer**。

## 查询路径

```
Query RPC（namespace + stream + SQL 字符串）
  → 解析 namespace/stream，定位 <ns>.<stream>.db
  → 校验（只读：仅允许 SELECT）
  → 在目标 SQLite 文件上执行
  → 返回结果行
```

新增 gRPC 方法：`Query(QueryRequest) → QueryResponse`

```protobuf
message QueryRequest {
  string namespace = 1;  // 必填
  string stream    = 2;  // 必填
  string sql       = 3;  // SELECT 语句（在目标 stream 的 db 上执行）
}

message QueryResponse {
  repeated bytes row_json = 1;  // 每行为 JSON 编码
}
```

## 落盘与快照策略

### <stream>.db 写入

```
Indexer:
  按 stream → 持续 INSERT 到对应 <stream>.db（SQLite WAL 内存缓冲）

  周期落盘（可配置，默认 30 秒）:
    → 遍历所有活跃 stream 的 db
    → SQLite WAL checkpoint + fsync

  优雅退出:
    → 所有活跃 db 执行 WAL checkpoint + fsync + close
    → 记录各 stream 最后 checkpoint seq 到 meta.json
```

### 快照创建

当调用 `checkpoint(seq)` 时，按 stream 独立创建快照：

```
1. 先 Flush 对应 stream 的 <stream>.db（WAL checkpoint + fsync）
2. 复制 <stream>.db → <stream>.snap_{timestamp}.db
3. 记录该 stream 的快照 seq 到 meta.json
4. 删除该 stream 的过期快照（保留最近 K 个）
```

快照清理在低优先级后台线程执行。

### <stream>.db 丢失的代价

- 回退到该 stream 的最新 `snap_*.db`
- Indexer 从快照 seq 重放到当前 committed seq
- 恢复时间：与该 stream 自上次快照以来的 Segment 数据量成正比（通常秒级）
- 其他 stream 不受影响

## 恢复

logdbd 启动时，按 stream 独立恢复：

```
对于每个活跃的 stream:
  1. 选取最新快照：
     - 扫描 cache_dir/ 下该 stream 的 snap_*.db 文件
     - 选取 checkpoint seq 最大的（从 meta.json 或文件名时间戳判断）
     - 无快照 → 创建全新 SQLite

  2. 复制快照 → <stream>.db（无快照则直接创建新库）

  3. Indexer 从该 stream 的 checkpoint_seq 重放到 committed seq
     → INSERT 所有缺失记录
     → 该 stream 的 SQLite 就绪，可接受查询

  4. 该 stream 所有快照丢失/损坏:
     → Indexer 从 seq 0 全量重建该 stream
     → 该 stream 重建完成前 Query API 返回 UNAVAILABLE
     → 其他 stream 不受影响
```

## Stream 生命周期管理

```
stream 创建:
  → 首次 append 时自动创建 <stream>.db

stream 归档/删除:
  → 直接删除 <stream>.db 及其所有快照文件
  → 从 meta.json 移除该 stream 的记录
  → Segment 由 retention policy 管理，不受影响
```

## 标记删除

不是真正的 DELETE。在 SQLite 中记录 `deleted = 1`：

```
Append 一条特殊的 tombstone 事件:
  event_type = "logdb.tombstone"
  metadata   = { "target_seq": "12345" }

Indexer:
  INSERT tombstone 到对应 <stream>.db
  UPDATE target record SET deleted = 1
```

Segment 永远不删除任何内容。SQLite 保留两条记录。
查询可通过约定使用 `WHERE deleted = 0`，或暴露视图隐藏已标记删除的行。

## 配置

```yaml
# logdbd.yaml 新增配置
cache:
  dir: /var/lib/logdbd/cache       # 缓存目录
  flush_interval_secs: 30          # WAL checkpoint 间隔
  snapshot_min_interval_secs: 300  # 自动快照最小间隔
  snapshot_retain: 5               # 每个 stream 保留最近 N 个快照
  indexes:                         # 各 stream 额外索引的 metadata 字段
    - stream: "default"
      fields: ["turn_id", "session_id"]
```

## 不是什么

- **不是 ClickHouse 的替代品。** SQLite 是 OLTP 量级。大规模分析仍走现有 `logdb-exporter` → ClickHouse。
- **不是 OLAP。** 没有列存、物化视图、并行扫描。扫描百万行级别的查询会很慢。
- **不是数据库。** Segment 文件才是数据库。SQLite 是缓存层。
- **不支持跨 stream 查询。** 每个 stream 是独立的 SQLite 文件。跨 stream 查询需要上层应用自己处理。

## 风险与缓解

| 风险 | 缓解措施 |
|------|---------|
| SQLite 文件无限增长 | 快照压缩旧数据；stream 归档时直接删除对应 db 和快照 |
| Indexer 追赶不上 | 监控 lag（committed_seq - indexer_seq）；持久滞后时减少 flush_interval 或加批量写入 |
| 重建期间有查询进来 | 该 stream 返回 UNAVAILABLE，直到 Indexer 追上；客户端重试 |
| SQL 注入 | 仅允许 SELECT；SQLite prepared statement / AST 校验 |
| 大量 stream 导致文件句柄耗尽 | Indexer 使用 LRU 关闭不活跃 stream 的 db 连接；查询时按需打开 |

## 实现计划

1. 为 logdbd 添加 `rusqlite` 依赖
2. 创建 `cache/` 模块：`CacheConfig`、`Indexer`、快照管理、stream 生命周期管理
3. 为 gRPC proto 新增 `Query` RPC
4. 在 logdbd node 启动流程中集成 Indexer
5. 添加 `cache_cleanup` 后台线程（快照 + 不活跃连接）
6. 测试：segment 重建、快照轮转、标记删除、stream 归档、并发 append+query
7. 文档：Agent 场景查询示例

## 参考资料

- rqlite 架构（Raft log → SQLite）：模式相似，目的不同。rqlite 的 SQLite 就是数据库本身；我们是缓存。
- logdbd 现有恢复机制：`recovery.rs` 按 shard 扫描 segment
- logdbd 现有 checkpoint API：`checkpoint(seq)`

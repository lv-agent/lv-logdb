# SQL 查询缓存

logdbd 内置了 SQLite 查询缓存，可通过标准 SQL 的 SELECT 语句直接查询流中的数据。

## 快速开始

```bash
# 启用缓存（默认开启，配置 cache.dir 即可）
logdbd --config logdbd.yaml
```

```rust
// Rust SDK 示例
use logdb_client::Client;

let mut client = Client::connect("127.0.0.1:50051").await?;

// 查询 stream 中所有记录
let resp = client.query("my-app", "main", "SELECT * FROM records ORDER BY seq DESC LIMIT 10").await?;
for row in resp.rows {
    println!("{}", row); // JSON 格式的行
}
```

```bash
# grpcurl 示例
grpcurl -plaintext -d '{
  "namespace": "my-app",
  "stream": "main",
  "sql": "SELECT seq, event_type, json_extract(metadata_json, \"$.turn_id\") as turn_id FROM records WHERE event_type = \"llm.call\" ORDER BY seq DESC LIMIT 5"
}' 127.0.0.1:50051 logdbd.LogDbService/Query
```

## 表结构

每个 stream 对应一个独立的 SQLite 数据库，包含单张 `records` 表：

```sql
records (
    seq            INTEGER PRIMARY KEY,   -- 流内序号
    gid            INTEGER NOT NULL,      -- logdb 全局 ID
    ts_ns          INTEGER NOT NULL,      -- 纳秒时间戳
    event_type     TEXT NOT NULL,         -- 事件类型
    content_type   TEXT NOT NULL,          -- 内容类型
    metadata_json  TEXT NOT NULL,          -- JSON 格式的元数据
    content        BLOB,                  -- 原始内容
    deleted        INTEGER DEFAULT 0      -- 标记删除（1 = 已标记）
)

-- 默认索引
INDEX idx_records_event_type ON records(event_type)
INDEX idx_records_ts ON records(ts_ns)
```

## 查询模式

### 按 event_type 过滤

```sql
SELECT * FROM records
WHERE event_type = 'llm.call'
ORDER BY seq DESC LIMIT 20
```

### 按 metadata 字段过滤

```sql
SELECT seq, event_type
FROM records
WHERE json_extract(metadata_json, '$.turn_id') = 'turn-abc123'
ORDER BY seq
```

### 统计数量

```sql
SELECT event_type, COUNT(*) as cnt
FROM records
WHERE deleted = 0 AND event_type != 'logdb.tombstone'
GROUP BY event_type
ORDER BY cnt DESC
```

### EXISTS 判断

```sql
SELECT COUNT(*) FROM records
WHERE json_extract(metadata_json, '$.session_id') = 'sess-456'
  AND event_type = 'session.start'
```

### 时间范围查询

```sql
SELECT seq, event_type
FROM records
WHERE ts_ns BETWEEN 1700000000000000000 AND 1700000001000000000
ORDER BY seq
```

### 排除已删除记录

```sql
SELECT * FROM records
WHERE deleted = 0
  AND event_type != 'logdb.tombstone'
ORDER BY seq
```

## 配置 metadata 字段索引

对于高频查询的 metadata 字段，可在 `logdbd.yaml` 中配置索引以提升查询性能：

```yaml
cache:
  dir: /var/lib/logdbd/cache
  flush_interval_secs: 30
  snapshot_retain: 5
  indexes:
    - stream: "my-app/main"
      fields: ["turn_id", "session_id"]
    - stream: "my-app/audit"
      fields: ["user_id", "action"]
```

配置索引后，Indexer 会在首次写入 stream 时自动创建 `json_extract(metadata_json, '$.field')` 的索引。

## 限制

- **仅允许 SELECT**。INSERT、UPDATE、DELETE、DROP 等写操作会被拒绝。
- **缓存是最终一致的**。写入通过 Committer 落盘后，Indexer 异步追赶（通常毫秒级）。短时间内的查询可能出现延迟。
- **跨 stream 查询不支持**。每个 stream 是独立的 SQLite 文件。如需跨 stream 分析，使用 `logdb-exporter` → ClickHouse。
- **大规模分析不适合**。SQLite 是 OLTP 量级。扫描百万行级别的查询会很慢，此类场景建议走 ClickHouse 通道。

## 缓存恢复

SQLite 缓存文件（`cache_dir/` 下的 `.db` 文件）完全可以从 Segment 文件重建。如果缓存文件损坏或丢失：

1. 删除 `cache_dir/` 中的所有文件
2. 重启 logdbd
3. Indexer 自动从 Segment 全量重建缓存
4. 重建完成后 Querys API 自动恢复

## 快照

调用 `Checkpoint` RPC 时，Indexer 会为每个活跃的 stream 创建一份 SQLite 快照（`<stream>.snap_{timestamp}.db`），保留最近 K 个（通过 `snapshot_retain` 配置）。快照用于加速下次启动时的缓存恢复。

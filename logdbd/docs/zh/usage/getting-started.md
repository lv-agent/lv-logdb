# 快速开始

## 环境要求

- Rust 1.85+
- protoc（protobuf 编译器）：`apt install protobuf-compiler`

## 编译

```bash
cargo build --release -p logdbd
cargo build --release -p logdb-exporter
cargo build --release -p logdbd --bin logdbd-admin
```

## 最小配置

创建 `logdbd.yaml`：

```yaml
node:
  id: "primary-1"
  role: primary
  cluster_id: "my-cluster"
  epoch: 1

server:
  bind: "127.0.0.1:50051"

logdb:
  data_dir: /var/lib/logdbd
```

## 启动

```bash
logdbd --config /etc/logdbd/logdbd.yaml
```

## 使用 Rust SDK

在 `Cargo.toml` 中引用：

```toml
[dependencies]
logdb-client = { path = "../logdb-client" }
tokio = { version = "1", features = ["full"] }
```

### 写入

```rust
use logdb_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("127.0.0.1:50051").await?;

    // 简单写入
    let seq = client.append("my-app", "main", "user.login", b"hello").await?;
    println!("appended at seq={}", seq);

    // 带元数据的写入
    use std::collections::HashMap;
    let mut meta = HashMap::new();
    meta.insert("ip".into(), "1.2.3.4".into());

    let resp = client.append_full(
        "my-app", "main",
        "llm.call", "application/json",
        &meta, 0,
        br#"{"model":"claude-sonnet-5","tokens":1500}"#,
    ).await?;
    println!("appended: ns_id={}, stream_id={}, seq={}", resp.namespace_id, resp.stream_id, resp.seq);

    Ok(())
}
```

### 读取

```rust
// 点读
if let Some(rec) = client.read("my-app", "main", 1).await? {
    println!("seq={} event_type={}", rec.seq, rec.event_type);
    println!("content={}", String::from_utf8_lossy(&rec.content));
}

// 范围扫描
let all = client.scan_all("my-app", "main", 0).await?;
for r in &all {
    println!("[{}] {}: {} bytes", r.seq, r.event_type, r.content.len());
}
```

### 实时订阅（Tail + Consumer Group）

```rust
use logdb_client::TailOptions;

// 订阅 stream 的新记录
let mut stream = client.tail("my-app", "main")
    .consumer_group("audit-processors", "worker-1")
    .start(&mut client).await?;

println!("Subscribed to my-app/main as worker-1");

while let Some(rec) = stream.next().await? {
    println!("[{}] {}", rec.seq, rec.event_type);

    // 处理完毕后提交进度
    client.commit_offset(
        "my-app", "main",
        "audit-processors", "worker-1",
        rec.seq,
    ).await?;
}
```

消费者断线重连后，自动从未提交 offset + 1 开始继续订阅。

### 管理操作

```rust
// 列出 namespace
for ns in client.list_namespaces().await? {
    println!("{} ({} streams)", ns.name, ns.stream_count);
}

// 列出 stream
for s in client.list_streams("my-app").await? {
    println!("{} (seq 1-{})", s.name, s.durable_seq);
}

// 节点状态
let status = client.status().await?;
println!("node={} durable={}", status.node_id, status.durable_sequence);

// 验证 hash chain
let result = client.verify_chain("my-app", "main", 0, 0).await?;
if result.ok {
    println!("Hash chain OK: seq {}-{}", result.verified_from, result.verified_to);
} else {
    println!("Hash chain BROKEN at seq {}: {}", result.error_at_seq, result.error_message);
}
```

## 使用 CLI 工具

快速操作不需要写代码：

```bash
# 写入
logdbd-admin append 127.0.0.1:50051 my-app main "hello"

# 查看状态
logdbd-admin status 127.0.0.1:50051

# 列出 namespace 和 stream
logdbd-admin list 127.0.0.1:50051
logdbd-admin streams 127.0.0.1:50051 my-app
```

## 导出到 ClickHouse

1. 建表：

```sql
CREATE TABLE agent_traces (
    namespace_id  UInt32,
    stream_id     UInt64,
    seq           UInt64,
    event_type    String,
    timestamp_ns  UInt64,
    content_type  String,
    metadata      Map(String, String),
    content       String,
    inserted_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(seq)
ORDER BY (namespace_id, stream_id, seq);
```

2. 配置 `exporter.yaml`：

```yaml
source:
  addrs: ["127.0.0.1:50051"]
scope:
  namespace: "my-app"
  stream: "main"
sink:
  type: clickhouse
  clickhouse:
    url: "http://clickhouse:8123"
    database: logdb
    table: agent_traces
    batch_size: 10000
```

3. 启动：

```bash
logdb-exporter exporter.yaml
```

## 下一步

- [配置参考](configuration.md) — 完整配置项说明
- [开发指南](../dev/building.md) — 编译、测试、贡献
- [logdb-client SDK](../../../logdb-client/) — Rust SDK API 文档

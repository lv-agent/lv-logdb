# logdb-broker

[logdbd](../logdbd) 的 Kafka 风格 consumer group 协调器 (cr-037)。**对称网关**（Pulsar 模型 A）：producer 和 consumer **都只连 broker**；broker 转发 Append 到 logdbd，按分配的 shard Tail logdbd 并转发给 consumer。logdbd 是存储后端。

```
Producer ──Produce──→ Broker ──Append──→ logdbd
Consumer ──Consume──→ Broker ──Tail────→ logdbd
```

## 核心能力

- **Shard 分配** —— round-robin 将 logdbd shard 分配给 group 内的 consumer（`shard i → member i % n`）。支持 sticky 协作 rebalance（consumer 尽量保留已有 shard，只移动必须移动的）。
- **数据转发** —— 每个分配的 shard 一条 Tail，记录带 stamped `shard_id`（consumer 可按 shard 提交 offset）。
- **Rebalance（stop-the-world）** —— join/leave 时活跃 Consume 流收到 `RebalanceSignal` → `Assignment`，broker 自动切换 forward task 到新 shard。
- **持久化 offset** —— 已提交 offset 事件溯源写入 logdbd 的 `logdb_broker/coord_state` meta stream，broker 重启后重放恢复（membership 瞬态——consumer 重 join）。**broker 本身无状态**。
- **Offset 感知 Consume** —— 每个 shard 从已提交 offset+1 续读。
- **Per-group leader election** —— 多 broker 实例共连一个 logdbd，按 `(ns, stream, group)` 选举 leader。不同 group 可由不同 broker 当 leader（负载分担）。Stateful RPC（join/leave/consume/commit/heartbeat）只有 leader 处理，standby 返回 `UNAVAILABLE` + leader 地址重定向。Produce 无状态，任何 broker 都能转发。
- **Prometheus 指标** —— `broker.joins`/`leaves`/`consume_sessions`/`records_forwarded`/`offsets_committed`/`rebalances`。可选 `/metrics` endpoint。
- **优雅关闭** —— SIGTERM/Ctrl-C。**Docker 镜像** —— 多阶段构建，非 root。

## 运行

```bash
# logdbd 必须可达（shards 必须匹配 num_shards）
LOGDB_BROKER_CONFIG=./logdb-broker/config.yaml cargo run -p logdb-broker
```

Config 字段：`bind_addr`（broker gRPC 端口）、`logdbd_addr`、`num_shards`、`broker_id`（HA 实例区分）、`metrics_addr`（可选 Prometheus `/metrics`）、`session_timeout_ms`（心跳驱逐超时,0=禁用）。

见 [`config.yaml`](./config.yaml)。

## SDK

使用 [`logdb-client`](../logdb-client)，开启 `broker` feature：

```rust
use logdb_client::broker::{BrokerProducer, GroupConsumer};

// 生产
let mut p = BrokerProducer::connect("http://broker:9091").await?;
p.produce("ns", "s", "evt", b"payload", Some("session-42")).await?;

// 消费
let mut c = GroupConsumer::join("http://broker:9091", "ns", "s", "g", "c1").await?;
let mut stream = c.consume().await?;
while let Some(rec) = stream.next().await {
    let rec = rec?;
    c.commit_shard(rec.shard_id, rec.seq).await?;
}
c.leave().await?;
```

## 容器

```bash
docker build -t logdb-broker -f logdb-broker/Dockerfile .
docker run -p 9091:9091 -p 9100:9100 logdb-broker
```

无需 volume —— broker 是无状态的（状态在 logdbd meta stream 中）。

## 状态 (cr-037)

单 broker 实例（多 broker HA 通过 per-group leader election 已支持，全局协调和分区级故障转移后续搭配 cr-026 做）。Rebalance 是 stop-the-world + sticky shed 方案（未来的协作式 sticky 分配可升级）。设计文档：`veps/cr-037-logdb-broker-design.md`。

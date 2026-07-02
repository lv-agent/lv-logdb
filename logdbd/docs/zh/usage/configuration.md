# 配置参考

logdbd 使用 YAML 配置文件。配置文件路径通过 `--config` 参数指定。

## 环境变量替换

配置文件中 `${VAR_NAME}` 占位符会在加载时替换为对应的环境变量值。

```yaml
server:
  auth:
    token_file: ${LOGDBD_TOKEN_FILE}
```

## 完整配置项

### node — 节点身份

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `id` | string | — | 节点唯一 ID，建议用 hostname |
| `role` | `primary` \| `standby` | — | 节点角色 |
| `cluster_id` | string | — | 集群 ID，所有节点必须一致 |
| `epoch` | u64 | — | 写入世代，每次 failover 手动 +1 |

### server — gRPC 服务

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `bind` | string | `127.0.0.1:50051` | 监听地址 |
| `tls.mode` | `mtls` \| `tls` \| `disabled` | `disabled` | TLS 模式 |
| `tls.cert_file` | path | — | 服务端证书 |
| `tls.key_file` | path | — | 服务端密钥 |
| `tls.ca_file` | path | — | CA 证书 |
| `auth.type` | `token` \| `mtls` \| `both` | `token` | 认证方式 |
| `auth.token_file` | path | — | Bearer token 文件路径 |
| `tail_heartbeat_interval_ms` | u64 | 1000 | Tail 心跳间隔 |

### logdb — 存储引擎

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `data_dir` | path | `/var/lib/logdbd` | 数据目录 |
| `shards` | usize | 4 | 分片数（2的幂，初始化后不可变） |
| `segment_size` | u64 | 256 MiB | Segment 文件大小上限 |
| `ring_size` | usize | 65536 | Ring buffer 容量 |
| `durability_mode` | `sync` \| `batch` \| `async` | `sync` | 持久化模式 |
| `flush_timeout_ms` | u64 | 5000 | Flush 超时 |
| `backpressure.policy` | `block` \| `reject` | `block` | Ring 满时的策略 |
| `backpressure.max_in_flight` | usize | 65536 | 最大 in-flight 记录数 |

### storage — 存储配置

| 字段 | 说明 |
|------|------|
| `index_stride` | Sparse index 粒度（默认 1024） |
| `compression.enabled` | 是否启用 zstd 压缩 |
| `compression.level` | 压缩级别（1-22） |
| `encryption.enabled` | 是否启用 AES-256-GCM 加密 |
| `encryption.keys[]` | 密钥列表 `{key_id, key_hex}` |
| `encryption.active_key_id` | 当前活跃密钥 ID |

### audit — 审计配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `hash_chain` | bool | true | 是否启用 BLAKE3 hash chain |
| `hash_algorithm` | `blake3` \| `sha256` | `blake3` | Hash 算法 |

### limits — 限制

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `max_record_size` | usize | 1 MiB | 单条记录最大字节数 |
| `max_batch_records` | usize | 1000 | 批量写入最大条数 |
| `max_batch_bytes` | usize | 16 MiB | 批量写入最大字节数 |
| `max_scan_limit` | usize | 10000 | Scan 单次最大返回数 |
| `max_tail_batch_size` | usize | 10000 | Tail 单批最大条数 |

### replication — 复制

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `mode` | `sync` \| `async` | `sync` | 复制模式 |
| `sync_policy` | `all` \| `quorum` \| `n` | `all` | sync 确认策略 |
| `required_acks` | u32 | 0 | sync_policy=n 时的最小 ack 数 |
| `sync_timeout_ms` | u64 | 5000 | sync 模式下等 standby ack 超时 |
| `on_sync_timeout` | `fail` \| `async_warn` \| `block` | `fail` | 超时行为 |
| `batch_size` | usize | 1024 | 每批复制的最大记录数 |
| `batch_bytes` | usize | 256 KiB | 每批复制的最大字节数 |

每个 standby 配置：

| 字段 | 说明 |
|------|------|
| `id` | Standby 唯一 ID |
| `addr` | Standby 地址 `host:port` |
| `tls.*` | 客户端 TLS 证书 |
| `auth_token_file` | 认证 token 文件 |

### retention — 保留策略

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | true | 是否启用 |
| `max_segments` | usize | 100 | 最多保留段数 |
| `max_age_days` | u32 | 7 | 最多保留天数 |
| `require_replicated` | bool | false | 删除前要求已复制 |

### observability — 可观测性

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `health_bind` | string | `0.0.0.0:9090` | 健康检查端口 |
| `metrics` | bool | true | 启用 Prometheus |
| `metrics_bind` | string | `0.0.0.0:9091` | Prometheus 端口 |
| `log_level` | string | `info` | 日志级别 |

## 环境变量覆盖

以下环境变量可以覆盖 YAML 配置（容器部署友好）：

| 变量 | 对应配置 |
|------|----------|
| `LOGDBD_AUTH_TOKEN` | `server.auth.token_file` 的替代 |
| `LOGDBD_ALLOW_INSECURE=1` | 允许非 loopback 接口无 TLS+auth 启动 |

## 审计场景推荐配置

```yaml
logdb:
  durability_mode: sync
  backpressure:
    policy: block

replication:
  mode: sync
  sync_policy: all
  on_sync_timeout: fail

audit:
  hash_chain: true

storage:
  encryption:
    enabled: true

retention:
  require_replicated: true

server:
  tls:
    mode: mtls
  auth:
    type: both
```

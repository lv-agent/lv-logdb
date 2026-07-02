# logdbd Failover Runbook

## 故障分类

| 级别 | 症状 | 影响 | 响应时间 |
|------|------|------|----------|
| Sev1 | Primary 宕机，写入停止 | 全部写入中断 | 立即 |
| Sev2 | Standby 断开，复制滞后 | 无冗余，数据有丢失风险 | < 1小时 |
| Sev3 | 磁盘使用率 > 85% | 即将无法写入 | < 4小时 |
| Alert | Exporter 滞后 | 分析数据延迟 | < 24小时 |

---

## Sev1: Primary 宕机 — Manual Failover

### 0. 预检

```bash
# 确认旧 primary 状态
grpcurl -plaintext <old-primary>:50051 logdbd.v1.LogDb/Status
# 如果无响应 → 确认宕机

# 查询所有 standby 状态
for addr in standby-1 standby-2; do
  echo "=== $addr ==="
  grpcurl -plaintext $addr:50051 logdbd.v1.LogDb/Status
done
```

### 1. 选择新 primary

选择标准（优先级从高到低）：
1. `durable_seq` 最大
2. hash chain 无 error（`VerifyChain` 通过）
3. `replication_lag` 最小
4. 无 `conflict` 标记

```bash
# 在候选 standby 上验证 hash chain
grpcurl -plaintext <candidate>:50051 logdbd.v1.LogDb/VerifyChain
```

### 2. 切换

```bash
# 在新 primary 上修改配置
vim /etc/logdbd/logdbd.yaml
# 修改:
#   node.role: primary
#   node.epoch: <old_epoch + 1>     ← 必须手动递增！
#   replication.standbys: [其他 standby 列表]

# 重启为 primary
systemctl restart logdbd

# 验证可写
grpcurl -plaintext <new-primary>:50051 logdbd.v1.LogDb/Append
```

### 3. 更新其他 standby

```bash
# 每个 standby 修改配置
#   replication.standbys: []    ← standby 不需要复制他人
# 或者配置为其他新 standby 的地址
systemctl restart logdbd
```

### 4. 更新 Agent / LB

```bash
# 修改 Agent 连接地址或 LB 后端
# 指向新 primary
```

### 5. 验证

```bash
# 写入测试
grpcurl -plaintext <new-primary>:50051 logdbd.v1.LogDb/Append

# 复制正常
grpcurl -plaintext <new-primary>:50051 logdbd.v1.LogDb/Status
# 确认 standbys[].state == "ok"

# Exporter 正常
# 确认 exporter 日志无 error，进度在推进
```

### 6. 记录审计事件

```text
时间:
操作人:
旧 primary: node_id=<>, epoch=<>
新 primary: node_id=<>, epoch=<>
原因:
切换前各节点 durable_seq:
```

---

## Sev2: Standby 断开

### 诊断

```bash
# 在 primary 上查看复制状态
grpcurl -plaintext <primary>:50051 logdbd.v1.LogDb/Status
# 查看 standbys[].state:
#   ok          → 正常
#   degraded    → 有延迟但还在追
#   disconnected → 网络断开
#   conflict    → hash 冲突，需人工介入
```

### 修复

```bash
# 如果是网络问题 → 恢复网络，standby 自动重连
# 如果是 standby 宕机 → 重启 standby
systemctl restart logdbd

# 如果是日志落后超过 retention → standby 需要全量同步
# 在 standby 上:
systemctl stop logdbd
rm -rf /var/lib/logdbd/segments/*  # 清空旧数据
systemctl start logdbd
# standby 会自动触发 Snapshot 同步
```

---

## Sev3: 磁盘即将满

### 诊断

```bash
df -h /var/lib/logdbd
grpcurl -plaintext <primary>:50051 logdbd.v1.LogDb/Status
# 关注 wal_bytes_used / wal_bytes_total
```

### 临时措施

```bash
# 1. 检查是什么在占用空间
du -sh /var/lib/logdbd/segments/

# 2. 如果 standby 故障导致 retention 无法删除
#    临时降低 sync_policy 或修复 standby

# 3. 紧急释放空间：降低 retention
vim /etc/logdbd/logdbd.yaml
#   retention.max_segments: 50   (原 100)
#   retention.max_age_days: 3    (原 7)
systemctl restart logdbd
```

### 根本解决

- 扩容磁盘
- 调整 retention 策略
- 确保 standby 正常所以 replication 不阻塞 retention

---

## Conflict 处理

### 症状

```text
Prometheus alert: LogdbdReplicationConflict
Status 返回: standbys[].state == "conflict"
```

### 诊断

```bash
# 在 primary 和 standby 上分别验证 hash chain
grpcurl -plaintext <primary>:50051 logdbd.v1.LogDb/VerifyChain
grpcurl -plaintext <standby>:50051 logdbd.v1.LogDb/VerifyChain
# 对比 error_at_seq，确认分叉点
```

### 修复

```bash
# 方案 A: 确认 primary 数据为权威 → 重建 standby
systemctl stop logdbd  # 在 standby 上
rm -rf /var/lib/logdbd/segments/*
systemctl start logdbd
# standby 自动触发 Snapshot

# 方案 B: 确认 standby 数据为权威 → 提升 standby 为 primary
# 按 Sev1 Failover 流程操作，旧 primary 降级后重建
```

---

## Exporter 不工作

### 诊断

```bash
# 检查 exporter 进程
systemctl status logdb-exporter

# 检查进度
cat /var/lib/logdb-exporter/progress.dat
```

### 修复

```bash
# 如果 OUT_OF_RETENTION（exporter 停机太久，旧数据已删）
# 查看 exporter 日志确认丢失的 seq 范围
# 确认 ClickHouse 中已有数据的最大 seq
# 重置进度并重启
logdb-exporter --reset-progress <last_seq_in_clickhouse> /etc/logdb-exporter/exporter.yaml

# 如果不能接受数据丢失 → 从 standby 的历史备份恢复
```

---

## 健康检查命令速查

```bash
# 节点状态
grpcurl -plaintext <addr>:50051 logdbd.v1.LogDb/Status

# 写入测试
grpcurl -plaintext -d '{"namespace":"healthcheck","stream":"ping","event_type":"health","content":"cGluZw=="}' <addr>:50051 logdbd.v1.LogDb/Append

# 读取测试
grpcurl -plaintext -d '{"namespace":"healthcheck","stream":"ping","seq":1}' <addr>:50051 logdbd.v1.LogDb/Read

# 列出 namespace
grpcurl -plaintext <addr>:50051 logdbd.v1.LogDb/ListNamespaces

# 验证 hash chain
grpcurl -plaintext -d '{"namespace":"test","stream":"main"}' <addr>:50051 logdbd.v1.LogDb/VerifyChain

# Prometheus metrics
curl http://<addr>:9091/metrics | grep logdb
```

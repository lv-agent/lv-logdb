# 使用指南

本指南带你从首次安装到在生产环境中运行 logdb，按照大多数用户学习本库的顺序展开。

请按顺序阅读，每一页都建立在前一页的基础之上。

1. [快速开始](getting-started.md) — 安装 logdb 并写入你的第一批记录。
2. [核心概念](concepts.md) — 核心模型：日志、段（segment）、记录、索引。
3. [写入](writing.md) — 仅追加写入、批处理与写入语义。
4. [读取](reading.md) — 点查询、范围扫描与顺序保证。
5. [持久化](durability.md) — `fsync`、刷盘策略与数据安全时机。
6. [恢复](recovery.md) — 崩溃恢复与重做（redo）流程。
7. [尾读（Tailers）](tailers.md) — 跟随日志、尾读与实时消费者。
8. [配置](configuration.md) — 段大小、特性与可调参数。
9. [特性](features.md) — hash-chain 防篡改、压缩、加密。
10. [分片](sharding.md) — 将数据分布到多个日志。
11. [性能](performance.md) — 吞吐、延迟与基准测试。
12. [错误处理](errors.md) — 错误类型、成因与恢复动作。
13. [实践手册](cookbook.md) — 常见任务配方。

## 相关链接

- [阅读指南主页](../README.md)
- [开发指南](../dev/README.md)

> logdb 0.2.0

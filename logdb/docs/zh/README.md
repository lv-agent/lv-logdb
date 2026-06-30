# logdb

logdb 是一个嵌入式、仅追加、可崩溃恢复、可选防篡改的 Rust 本地日志数据库。

这里是文档主页。通过本页面你可以访问**使用指南**（面向在应用中使用 logdb 的开发者）和**开发指南**（面向参与 logdb 本身开发的贡献者）的全部页面。

## 使用指南

如果你打算在自己的应用中使用 logdb，请从这里开始。

- [使用指南概览](usage/README.md)
  - [快速开始](usage/getting-started.md)
  - [核心概念](usage/concepts.md)
  - [写入](usage/writing.md)
  - [读取](usage/reading.md)
  - [持久化](usage/durability.md)
  - [恢复](usage/recovery.md)
  - [尾读（Tailers）](usage/tailers.md)
  - [配置](usage/configuration.md)
  - [特性](usage/features.md)
  - [分片](usage/sharding.md)
  - [性能](usage/performance.md)
  - [错误处理](usage/errors.md)
  - [实践手册](usage/cookbook.md)

## 开发指南

如果你正在扩展、调试或参与 logdb 本身的开发，请阅读本指南。

- [开发指南概览](dev/README.md)
  - [架构](dev/architecture.md)
  - [存储格式](dev/storage-format.md)
  - [项目结构](dev/project-layout.md)
  - [构建](dev/building.md)
  - [测试](dev/testing.md)
  - [扩展](dev/extending.md)
  - [贡献](dev/contributing.md)

## API 参考

API 参考：运行 `cargo doc --open`（或在发布后访问 docs.rs）。

## 语言

语言：中文。↔ [English](../en/README.md)

## 相关链接

- [使用指南](usage/README.md)
- [开发指南](dev/README.md)

> logdb 0.2.0

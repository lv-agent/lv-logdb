# 开发指南

本指南面向扩展、调试、基准测试或参与 logdb 本身贡献的开发者。如果你只是想在应用中使用 logdb，请从[使用指南](../usage/README.md)开始阅读。

各页面按照从高层理解到动手贡献的顺序排列。

1. [架构](architecture.md) — 模块、数据流与关键抽象。
2. [存储格式](storage-format.md) — 段与索引的磁盘布局。
3. [项目结构](project-layout.md) — 源码树中各部分的位置。
4. [构建](building.md) — 工具链、特性与构建标志。
5. [测试](testing.md) — 单元测试、集成测试、属性测试与模糊测试。
6. [扩展](extending.md) — 新增特性、格式与集成。
7. [贡献](contributing.md) — 工作流、代码风格与 Pull Request 流程。

如需查看公开 API 表面，请运行 `cargo doc --open`。

## 相关链接

- [开发指南主页](../README.md)
- [使用指南](../usage/README.md)

> logdb 0.2.0

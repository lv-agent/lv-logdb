# 贡献指南

如何为 logdb 做贡献：测试驱动工作流、设计文档约定、提交与代码风格，以及 pull-request 流程。

> 对 **logdb 0.2.0** 具权威性。以下为项目的工作约定——请忠实遵守。

## 工作流：测试驱动、设计先行

logdb 采用**测试先行**的开发方式。对任何 feature 或 bugfix：

1. **先写设计文档**（在写 feature 代码之前），路径为 `veps/cr-NNN-<english-topic>.md`。
   - **文件名是英文**（例如 `cr-002-push-api.md`）；**正文是中文**——讨论与规格用中文（项目的工作语言），而所有*产物*（代码、注释、提交信息、文件名）保持英文。
   - `veps/` 目录**被 git 忽略、永不提交**。它只存放工作中的设计笔记。
2. **先写失败的测试。** 红 → 绿。在相关 `src/` 模块的 `#[cfg(test)]` 块中新增或扩展单元测试，或在 `tests/` 下加集成测试，并确认它因正确的原因失败。
3. **以最小改动让它通过。** 绿之后再重构，而非之前。
4. **保持两种语言文档同步**（见下文“文档维护”）。

测试分类与命令见 [Testing](testing.md)；新模块应放在何处见 [Project layout](project-layout.md)。

## 提交约定

- **仅用英文。** 提交的 subject 与 body 一律英文，即便讨论与设计文档是中文。
- **禁止 `Co-Authored-By`。** 不要添加 `Co-Authored-By:` 尾注——对任何人都不要，永远不要。这是项目的硬性规则。
- **提交聚焦。** 一次提交一个逻辑变更；写简洁的 subject（祈使语气），必要时在 body 中解释*为什么*。
- **文档工作只提交 `docs/`，绝不提交 `veps/`。** `veps/` 按约定被 git 忽略——请确认它没有进入你的提交。

## 代码风格

- **`rustfmt`。** 每次提交前运行 `cargo fmt`；CI 要求格式化过的代码。
- **代码与注释用英文。** 标识符、文档注释、行内注释均为英文。中文仅用于对话与 `veps/` 设计文档正文。
- **仅可叠加 feature。** 新 feature 不得改变默认构建行为；用 `#[cfg(feature = "...")]` 门控。见 [Extending / 添加 feature flag](extending.md#添加-feature-flag)。

## Pull-request 流程

1. **开一个聚焦的 PR**——一个 feature 或一个修复，不要打 bundle。小 PR 审查更快、合入更安全。
2. **通过 `cargo test`。** 至少在默认构建下：
   ```sh
   cargo test
   cargo fmt -- --check
   cargo clippy --all-targets -- -D warnings
   ```
3. **若改动是 feature 门控的，跑 feature 矩阵：**
   ```sh
   cargo test --features compression
   cargo test --features encryption
   cargo test --features hash-chain
   cargo test --all-features
   ```
4. **在相关处跑 fuzz 或 bench。** 若你触及序列化、segment 或 append 路径，运行相关的 fuzz target 或 `cargo bench`，并报告差异。见 [Testing](testing.md)。
5. **同时更新两种语言的文档。** 若改动对用户可见，在同一 PR 中更新 `docs/en/` **与** `docs/zh/`（见下文）。

## 文档维护

英文与中文文档必须**保持同步**。两棵树（`docs/en/`、`docs/zh/`）逐文件互为镜像。

- **两边都加页。** 新增 `docs/en/<area>/<page>.md` 意味着要新增 `docs/zh/<area>/<page>.md`，反之亦然。
- **同一改动两边都改。** 在英文侧修正事实错误或新增章节时，在中文侧同步（反向亦然）。
- **推送前校验对称：**
   ```sh
   diff <(cd docs/en && find . -name '*.md' | sort) \
        <(cd docs/zh && find . -name '*.md' | sort)
   # 输出为空 ⇒ 两棵树文件集合一致
   ```
- **各语言保持地道。** 代码、符号、路径、标识符在两棵树中都保持英文；只有周围叙述文字变换语言。

## 相关链接

- [开发指南首页](README.md)
- [Testing](testing.md)——上文引用的测试分类与 feature 矩阵命令。
- [Extending](extending.md)——每项改动都必须遵守的可叠加 feature 与耐久性护栏规则。
- [Building](building.md)——工具链、feature 与资格测试脚本。

> logdb 0.2.0

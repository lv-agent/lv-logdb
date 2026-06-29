# 测试

logdb 的测试方式——单元测试、集成测试、proptest 属性测试、libFuzzer 模糊目标、Criterion 基准测试，以及资格测试脚本。

> 对 **logdb 0.2.0** 具权威性。布局变更时请以 `Cargo.toml`、`tests/`、`fuzz/`、`benches/` 为准重新核对。

## 单元测试

单元测试内联于 `src/` 下各模块的 `#[cfg(test)]` 代码块中，紧邻被测代码。运行方式：

```sh
cargo test                         # 全部单元 + 集成 + 文档测试
cargo test --lib                   # 仅库单元测试
cargo test storage::format         # 按模块路径过滤
```

README 的 "Testing" 章节在默认构建下引用约 103 个单元测试；具体数字视为近似，以 `cargo test` 输出为准。

## 集成测试

集成测试位于 `tests/`，端到端地检验公开 API：

- [`tests/integration.rs`](../../../tests/integration.rs)——完整生命周期：open → append → flush → read → verify → shutdown → recover。
- [`tests/fuzz.rs`](../../../tests/fuzz.rs)——proptest 属性测试，与 libFuzzer 目标对应；覆盖 `deserialize_record`、`segment_header`、`append_roundtrip`。

```sh
cargo test --test integration      # 生命周期 + 恢复
cargo test --test fuzz             # proptest 属性测试
PROPTEST_CASES=100000 cargo test --test fuzz -- --nocapture   # 更长运行
```

## 按 feature 测试

由于 feature 是叠加的，CI 中应分别测试每一个，再测全矩阵：

```sh
cargo test --features compression
cargo test --features encryption
cargo test --features hash-chain
cargo test --features "compression,encryption"
cargo test --all-features
```

## 模糊测试（Fuzzing）

模糊测试通过 `cargo-fuzz` 使用 **libFuzzer**，需要 **nightly** 工具链。目标位于 `fuzz/fuzz_targets/`（声明于 `fuzz/Cargo.toml`）：

| 目标（`fuzz/fuzz_targets/…`）      | 检查内容                                                              |
|------------------------------------|-----------------------------------------------------------------------|
| `deserialize_record.rs`            | 对任意字节调用 `deserialize_record` 不会 panic。                      |
| `segment_header.rs`               | 对任意 128 字节缓冲调用 `SegmentHeader::deserialize` 不会 panic。     |
| `append_roundtrip.rs`             | 随机内容经 append → flush → read 后保持一致。                         |

```sh
cargo +nightly fuzz run deserialize_record
cargo +nightly fuzz run segment_header
cargo +nightly fuzz run append_roundtrip
```

为获得内存错误覆盖，可在 AddressSanitizer 下运行：

```sh
cargo +nightly fuzz run --target x86_64-unknown-linux-gnu append_roundtrip -- -detect_leaks=0
```

模糊语料与产物通过 [`fuzz/.gitignore`](../../../fuzz/.gitignore) 排除出版本控制（忽略 `target`、`corpus`、`artifacts`、`coverage`）。

## 基准测试

存在两个基准入口：

- [`benches/append_bench.rs`](../../../benches/append_bench.rs)——append 吞吐/延迟的 Criterion 基准（`harness = false`，在 `Cargo.toml` 中注册）。
- [`benches/perf_test.rs`](../../../benches/perf_test.rs)——直接测量二进制，用于 Criterion 不可靠的场景（如 WSL2 的 tempdir 开销）。

```sh
cargo bench                       # Criterion 基准（append_bench）
cargo run --release --example perf   # 独立性能测量
```

Criterion 配置了 `html_reports`，报告写入 `target/criterion/`。

## 资格测试脚本

`scripts/` 包含用于长时运行与裸机测试的运行脚本。多数脚本会消费 [`scripts/build.sh`](building.md#release-辅助脚本scriptsbuildsh) 产出的 release 二进制。

| 脚本                                 | 作用                                                                                          |
|--------------------------------------|-----------------------------------------------------------------------------------------------|
| `scripts/build.sh`                   | 构建 release 二进制（`perf`、`soak`、`crash_test`、`testsuite`）。参见[构建](building.md)。   |
| `scripts/benchmark.sh`               | 运行性能套件；将带时间戳的日志写入 `OUTPUT_DIR/benchmark-*.log`。                              |
| `scripts/crash-recovery-test.sh`     | 反复：append → `kill -9` → recover → 验证 durable 游标之上无数据丢失。                         |
| `scripts/soak-test.sh`               | 以可配置时长运行 soak 二进制（默认 3600 秒）。                                                  |
| `scripts/run-all.sh`                 | 主资格运行器：单元/集成 → 基准 → 崩溃恢复 →（可选）soak。                                       |
| `scripts/run-all-deployed.sh`        | 同 `run-all.sh`，但针对预构建二进制（无需 Rust 工具链）。                                       |
| `scripts/package.sh`                 | 将 release 二进制 + 脚本打包为可部署 tarball（`logdb-bench-<target>-<ts>.tar.gz`）。            |

```sh
./scripts/build.sh
./scripts/run-all.sh                 # 完整资格运行
./scripts/run-all.sh --soak --soak-duration 86400 --iterations 100
./scripts/run-all-deployed.sh        # 在已部署主机上，无工具链
```

## 命令汇总

| 任务                          | 命令                                                              |
|-------------------------------|-------------------------------------------------------------------|
| 单元测试                      | `cargo test --lib`                                               |
| 全部测试                      | `cargo test`                                                     |
| 集成测试                      | `cargo test --test integration`                                  |
| 属性测试                      | `cargo test --test fuzz`                                         |
| 按 feature 测试               | `cargo test --features compression`（等）                         |
| 全 feature 矩阵              | `cargo test --all-features`                                      |
| 基准测试（Criterion）         | `cargo bench`                                                    |
| 性能二进制                    | `cargo run --release --example perf`                             |
| 模糊测试某目标                | `cargo +nightly fuzz run <target>`                               |
| 构建 release 二进制          | `./scripts/build.sh`                                             |
| 主资格运行                    | `./scripts/run-all.sh`                                           |
| 崩溃恢复循环                  | `./scripts/crash-recovery-test.sh`                               |
| Soak 测试                     | `./scripts/soak-test.sh`                                         |
| 基准套件                      | `./scripts/benchmark.sh`                                         |
| 打包 tarball                  | `./scripts/package.sh`                                           |

## 相关链接

- [开发指南首页](README.md)
- [构建](building.md)——上述构建/模糊命令引用的工具链与 feature 标志。
- [项目布局](project-layout.md)——`tests/`、`fuzz/`、`benches/`、`scripts/` 在仓库树中的位置。

> logdb 0.2.0

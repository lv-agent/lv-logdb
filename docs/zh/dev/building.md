# 构建

如何编译 logdb——工具链要求、可选 feature 矩阵，以及 release 构建辅助脚本。

> 对 **logdb 0.2.0** 具权威性。feature 或依赖变更时请以 `Cargo.toml` 为准重新核对。

## 工具链

logdb 在 **stable Rust**、edition **2021** 下构建。普通构建无需特殊工具链：

```sh
rustc --version     # 任意较新的 stable
cargo --version
```

唯一需要超出 stable 工具链的路径是**模糊测试（fuzzing）**，使用 `cargo +nightly fuzz`（libFuzzer）。参见[测试](testing.md#模糊测试-fuzzing)。

## 默认构建

```sh
cargo build                     # debug，默认 feature（无）
cargo build --release           # 优化构建
```

`Cargo.toml` 中 `default = []`，因此默认构建仅引入常驻依赖（`thiserror`、`crc32c`、`libc`、`scopeguard`）。下面的可选 feature 会添加相应能力及其 crate。

## Feature 矩阵

所有 feature 均为可选且默认关闭。它们相互独立，可自由组合。

| Feature         | 可选依赖                                            | 启用的能力                                              |
|-----------------|-----------------------------------------------------|---------------------------------------------------------|
| `hash-chain`    | `sha2`、`blake3`                                    | 每个 segment 的防篡改哈希链（SHA-256 / BLAKE3 keyed）。  |
| `compression`   | `zstd`（`default-features = false`）                | Zstandard 压缩的记录帧。                                |
| `encryption`    | `aes-gcm`（含 `aes`、`alloc`）、`getrandom`         | AES-GCM 加密的记录帧；nonce 来自 CSPRNG。               |
| `remote-push`   | *（无——仅为 feature 标志）*                          | 推送到远端的代码路径（无额外 crate）。                  |

用逗号分隔的 `--features` 列表组合 feature：

```sh
cargo build --features "compression,encryption"
cargo build --features "hash-chain"
cargo build --all-features          # hash-chain + compression + encryption + remote-push
```

`cargo build --all-features` 是一个有用的 CI 闸门——它证明整个 feature 矩阵可一起编译通过。

## Release 辅助脚本：`scripts/build.sh`

[`scripts/build.sh`](../../../scripts/build.sh) 构建资格测试与部署脚本所需的全部 release 二进制。它切换到项目根目录并执行：

```sh
cargo build --release --example perf --example soak --example crash_test --example testsuite
```

它打印 `rustc`/`cargo` 版本与目标三元组，随后列出 `target/release/examples/` 下生成的二进制：

- `perf`——性能基准二进制
- `soak`——soak 测试二进制
- `crash_test`——崩溃恢复辅助二进制
- `testsuite`——集成测试套件二进制

```sh
./scripts/build.sh                 # 构建全部四个 release 二进制
```

其他脚本（`benchmark.sh`、`soak-test.sh`、`crash-recovery-test.sh`、`run-all.sh`）会消费这些二进制；参见[测试](testing.md)。

## 相关链接

- [开发指南首页](README.md)
- [测试](testing.md)——测试、模糊测试与基准目标如何运行。
- [项目布局](project-layout.md)——源码、测试、基准与脚本在仓库中的位置。

> logdb 0.2.0

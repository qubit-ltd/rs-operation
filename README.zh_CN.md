# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

为 Rust 程序设计的操作生命周期与组合机制。目前仓库仅完成初始化，尚未实现公共操作 API，也未发布到 crates.io；上方徽章不代表已有发布或覆盖率结果。

## 为什么需要这个项目

下载文件、运行转换程序、上传结果这类任务往往涉及多个操作。失败或取消之后，应用仍要等待进程停止访问临时文件，再执行清理，并区分业务结果已确定和资源已释放。项目计划统一这些生命周期规则，同时支持同步与异步调用。

## 开发入门

当前可以构建空库并生成 API 文档；暂时没有可调用的生命周期 API 示例。

```bash
cargo test --locked
cargo doc --no-deps --locked
```

## 当前状态

仅提供 Rust crate 骨架、Apache-2.0 许可证及受管理的共享基础设施。取消、操作作用域、资源依赖和后台收尾均为待设计实现的能力。

当前空库没有可执行代码的覆盖率数据，因此完整 CI 的覆盖率任务会失败，依赖它的 Pages 部署也无法完成。待实现可执行逻辑后再验证覆盖率；初始化阶段不添加占位 API 或虚构覆盖率测试。

## 测试

```bash
./update-infra.sh --check
./.infra/bin/align-ci.sh
./.infra/bin/ci-check.sh
./.infra/bin/coverage.sh
```

## 许可证

Copyright (c) 2026. Haixing Hu. All rights reserved.

采用 [Apache License 2.0](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 规范，保持文档与实现一致，并在提交前执行 `.infra/bin/align-ci.sh` 与 `.infra/bin/ci-check.sh`。

## 作者

**Haixing Hu** — *Qubit Co. Ltd.*

仓库：[qubit-ltd/rs-operation](https://github.com/qubit-ltd/rs-operation)

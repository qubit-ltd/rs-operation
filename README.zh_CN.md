# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

`qubit-operation` 为 Rust 应用提供有界 supervisor，用来管理自有且满足 `Send + 'static` 的任务。业务结果与子任务完成、资源结算可以分别观察，适合需要协作取消、同时又必须确认资源何时真正释放的处理流程。

## 安装

若从本仓库 checkout 使用，可在应用中添加本地 crate：

```toml
[dependencies]
qubit-operation = { path = "../rs-operation" }
```

本 crate 面向 Rust 1.94，没有默认 feature，也没有运行时依赖。Crates.io 徽章显示的是 registry 状态；使用尚未发布的 checkout 时请采用 path 依赖。

## 快速开始

下面的流程提交一个自有任务，读取业务结果，再等待资源结算。实际应用可在任务中执行下载、转换和发布；创建受管理资源时，应通过传入的 scope 登记对应清理责任。

```rust,no_run
use qubit_operation::report::{OperationOutcome, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};
use std::time::Duration;

let supervisor = OperationSupervisor::new(SupervisorLimits::default());
let mut operation = supervisor.start(ScopeOptions::default(), |_scope| async {
    Ok::<_, &'static str>("artifact-42")
})?;
match operation.result_blocking()? {
    OperationOutcome::Succeeded(id) => println!("发布完成：{id}"),
    other => println!("任务结果：{other:?}"),
}
let settlement = operation.released_blocking()?;
println!("资源阶段：{:?}", settlement.resource_phase);
let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
# Ok::<(), Box<dyn std::error::Error>>(())
```

业务结果说明任务做成了什么；结算报告说明受管理的子任务和资源是否到达终态。取消只是请求任务配合停止，不能据此认定阻塞调用或 native 调用已经返回。可以运行[同步示例](examples/sync_pipeline.rs)、[异步示例](examples/async_pipeline.rs)和[取消示例](examples/inference_cancel.rs)。

## 提供的能力

- 有限的 root 准入容量，以及每个 scope 的子任务和资源登记上限。
- 一次性读取类型化业务结果，并重复查看最终结算报告。
- 结构化子 scope、资源使用租约、清理依赖和类型化恢复 owner。
- 同一生命周期的同步观察与异步等待。
- 由应用显式持有、支持 Drain 或 Cancel 关闭的 supervisor。

本库不提供分布式恢复、强制终止、业务重试或补偿、进度/事件总线集成，也没有正式 Tokio 适配器。`PortableDriver` 驱动可移植的自有 Future；绑定运行时的 Future 需要应用在对应运行时中接入合适的 `ExternalDriver`。不要在异步运行时工作线程上调用阻塞观察接口。

## 延伸阅读

- [中文用户指南](doc/user_guide.zh_CN.md)与[适配器接入指南](doc/adapter-guide.zh_CN.md)
- [API 文档](https://docs.rs/qubit-operation)
- [English README](README.md)

## 测试

```bash
cargo check --examples --locked
cargo test --locked
```

## 许可证

Copyright (c) 2026. Haixing Hu. All rights reserved.

采用 [Apache License 2.0](LICENSE)。

## 贡献

欢迎贡献。请保持公开行为与双语文档同步，提交前运行 `.infra/bin/align-ci.sh` 和 `.infra/bin/ci-check.sh`。

## 作者

**Haixing Hu** — *Qubit Co. Ltd.*

仓库：[qubit-ltd/rs-operation](https://github.com/qubit-ltd/rs-operation)

# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English](https://img.shields.io/badge/README-English-blue.svg)](README.md)

`qubit-operation` 为 Rust 服务提供有界的操作生命周期管理：统一协调同步任务、异步任务、父子操作和外部 provider，分别交付业务结果与资源结算。它面向需要处理取消、后台任务和延迟清理的开发者，帮助应用保留尚未完成的收尾责任。

例如，上传服务把文档交给后台检测器后，客户端可能先取消请求，检测器却还在读取输入。如果把“请求结束”当作“输入可以释放”，就会混淆业务结果和资源使用。本库用 scope 持有资源所有者，用 `ResourceRef` 跟踪实际使用者，让结果先返回、清理稍后完成；上传协议、检测算法和恢复策略仍由应用负责。

## 安装

需要 Rust 1.94 或更高版本；crate 使用 Rust 2024 edition。在应用的 `Cargo.toml` 中添加：

```toml
[dependencies]
qubit-operation = "^0.3"
```

## 快速开始

下面以检查上传文档的 PDF 文件头为入门练习：创建管理器、提交检查、登记输入资源、消费业务结果、观察结算，最后关闭管理器。保存为 `src/main.rs` 并运行 `cargo run`：

```rust
use qubit_operation::{ManagerOptions, OperationManager, OperationOptions, OperationOutcome};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let mut operation = manager.try_submit(OperationOptions::default(), |scope| {
        let input = scope.manage(b"%PDF-1.7\n".to_vec()).map_err(|error| format!("{:?}", error.kind()))?;
        Ok::<_, String>(input.starts_with(b"%PDF-").then_some("application/pdf"))
    })?;

    let outcome = operation.wait_outcome()?;
    match outcome {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection failed: {other:?}"),
    }
    let settlement = operation.wait_settlement()?;
    println!("资源均已释放：{}", settlement.all_released());
    let _shutdown = manager.request_shutdown(qubit_operation::ShutdownMode::Drain).wait()?;
    Ok(())
}
```

正常输出包括 `MIME: application/pdf` 和 `资源均已释放：true`。换成其他文件头会得到 `MIME: no match`，表示检查完成但未匹配，应用可以拒绝上传或继续其他检测。提交返回错误表示未接纳工作；拿到 handle 后仍须检查业务 outcome。这个例子只检查固定前缀，不提供完整 MIME 检测或文件安全校验。

示例先消费结果，再等待结算。结果若携带 `ResourceRef`，还必须先释放这些引用，避免等待自身。服务化接入与完整失败处理见[开发者手册](doc/developer_guide-0.3.0.zh_CN.md)。

## 为什么需要这个项目

一个异步结果或取消标志只能描述部分事实，无法独立回答“工作是否接纳”“业务如何结束”“谁还在使用输入”“清理失败后谁继续负责”。这些事实在外部 provider 和父子任务中尤其容易分离。

本库为它们建立共同的生命周期：准入前预留容量；接纳后跟踪任务、资源与子操作；结果只消费一次；资源使用结束后才清理；无法确认释放时保留恢复责任。应用可以分别处理业务响应和收尾，避免把结果返回、停止请求或 handle 丢弃当作释放证明。

适合上传处理、批量检查、推理请求等需要管理任务与资源关系的服务。若只是调用一个短函数且没有组合、取消或延迟清理需求，普通函数或 future 通常已经足够。

## 核心能力

| 能力 | 用途 |
| --- | --- |
| 同步与异步管理器 | 在共享协调器上接纳和观察不同执行方式的操作。 |
| 有界准入与服务组 | 约束全局、父操作和组的容量，拒绝时交还原闭包或准备适配器。 |
| 父子操作组合 | 跟踪子任务，区分必需与可选子操作，并配置失败后的兄弟任务策略。 |
| Scope 资源管理 | 预留后创建资源，使用强引用延迟清理，支持自定义异步清理。 |
| 独立结果与结算 | 消费 typed outcome，另行观察子任务、清理、外部报告和恢复责任。 |
| 外部操作协议 | 对接由其他运行时驱动的任务，分别报告结果、输入释放和领域结算。 |
| 停止、预算与关闭 | 传播协作式停止，继承时钟、截止时间和预算策略，观察 drain 或 cancel 进度。 |
| 恢复与有界证据 | 观察清理失败、发起恢复尝试，或将恢复责任交给 `RecoveryStore`。 |

本库不提供 MIME 算法、文件/命令/provider 成品适配器、业务重试、事务回滚、补偿或分布式持久化恢复。停止不会强制终止任意代码；共享资源内容的同步由应用负责。有界容量限制条目和任务数量，不限制应用资源本身的字节大小。详细约束和排障留给手册。

## 延伸阅读

- 开发接入：[中文开发者手册](doc/developer_guide-0.3.0.zh_CN.md) · [English developer guide](doc/developer_guide-0.3.0.md)
- 设计原理：[中文设计文档](doc/design-0.3.0.zh_CN.md) · [English design](doc/design-0.3.0.md)
- [API 文档](https://docs.rs/qubit-operation) · [English README](README.md)
- 可运行示例：[文档检测](examples/detect_file.rs)、[父子操作与容量](examples/family_and_capacity.rs)、[提交上下文](examples/submission_context.rs)、[推理](examples/inference.rs)
- 外部接入参考：[完整模拟 provider](doc/examples-0.3.0/provider_read.rs)、[资源型结果](doc/examples-0.3.0/inference.rs)。这两份是编译为 library target 的示例，不是真实 provider 适配器。

## 测试

```bash
cargo check --examples --locked
cargo test --locked
```

## 许可证

Copyright (c) 2026. Haixing Hu. All rights reserved.

Licensed under the [Apache License 2.0](LICENSE).

## 贡献

欢迎贡献。请保持公开行为和中英文文档一致。提交前运行 `.infra/bin/align-ci.sh` 和 `.infra/bin/ci-check.sh`。

## 作者

**Haixing Hu** — *Qubit Co. Ltd.*

仓库：[qubit-ltd/rs-operation](https://github.com/qubit-ltd/rs-operation)

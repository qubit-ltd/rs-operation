# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

`qubit-operation` 可以理解为面向任务的 Java `try … finally …` 机制：执行业务，无论正常完成、失败还是请求取消，都保留对子任务收尾和资源清理的责任。它同时提供同步阻塞等待和异步等待接口，适合需要组合文件 I/O、外部命令或模型推理的 Rust 应用。

## 从 `try … finally …` 理解它

在简单函数中，离开 `try` 时执行 `finally` 即可。但当工作跨越多个任务或服务时，返回业务结果、发出取消请求，都不意味着资源已经可以清理。例如，外部进程可能仍在读取文件，native 推理可能仍在使用缓冲区。

本库协调的就是这种跨任务的收尾责任：

```text
业务执行（类似 try）
    执行任务、启动子任务、使用资源
收尾（类似 finally，可能在业务结果可用后继续进行）
    等待资源使用者真正结束
    执行已登记的清理动作
    报告已确认释放，或仍需恢复处理的责任
```

| 熟悉的概念 | 在本库中的对应方式 |
| --- | --- |
| try 中的业务代码 | 通过 `supervisor.start(...)` 提交的任务 |
| finally 中的清理代码 | 应用实现 `OwnedCleanup`，并将其登记到作用域 |
| 使用资源期间保持其存活 | 持有使用租约 `ResourceUse`，实际停止使用后归还 |
| 返回值或业务失败 | `result_blocking()` 或 `result().await` |
| 确认清理是否完成 | `released_blocking()` 或 `released().await`，并检查报告 |

调用方丢弃任务句柄后，任务管理器（supervisor）仍保留生命周期管理责任，并请求任务协作停止。这不保证清理必定成功：取消无法强制终止任意 native 调用，清理也可能失败或无法确认。因此，业务结果与资源收尾分别观察。同步和异步接口对应同一套生命周期；下面的示例使用同步阻塞等待。

## 场景：检测上传文件的 MIME 类型

用户上传一段内容，服务需要先暂存文件，再调用 `file` 识别类型；启用媒体细化时，还可能继续调用 `ffprobe`。这个流程来自 [rs-mime](https://github.com/qubit-ltd/rs-mime) 的设计。

```text
上传内容 → 暂存输入 → file 检测 → 可选的 ffprobe 细化 → MIME 结果
               └──── 外部工具不再使用输入后，才能清理 ────┘
```

困难在于失败和取消：用户取消请求时，外部进程可能仍在读取暂存文件。此时删除文件、返回一个 MIME 结果，或丢弃等待结果的句柄，都不能证明进程和资源已完成收尾。

应用通过同一个作用域（`scope`）登记任务与资源。使用输入的一方持有使用租约（`ResourceUse`），实际停止使用后归还；库据此安排清理。文件服务负责暂存和删除，命令服务负责进程退出与回收，rs-operation 负责协调这些生命周期。

## 安装

```toml
[dependencies]
qubit-operation = "^0.1"
```

需要 Rust 1.94 或更新版本。没有运行时依赖，没有默认 feature。

## 快速开始：检测结果与输入释放

下面保留上述流程的关键关系，但用内存缓冲区代替暂存文件、用 PDF 文件头检查代替外部命令。因此无需安装 `file`、`ffprobe` 或模型即可运行；这不是完整的 MIME 检测器，也没有调用尚未实现的 rs-mime 接口。

先认识三个对象：`supervisor` 是应用持有的任务管理器；`scope` 用来登记本次任务的子任务和资源；`detection` 是调用方等待结果、请求取消和观察收尾的任务句柄。示例中的 `StagedInput` 是应用实现的清理适配器，库通过 `OwnedCleanup::begin` 调用它。

把下面代码保存为 `src/main.rs`，执行 `cargo run`：

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
use qubit_operation::report::{CleanupOutcome, OperationOutcome, ResourcePhase, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};

// 相当于 finally 中的清理动作，由应用实现、由库调度。
struct StagedInput(Arc<Vec<u8>>);

impl OwnedCleanup for StagedInput {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin(async move {
            drop(self.0); // 释放清理方持有的缓冲区引用。
            CleanupOutcome::Cleaned
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let mut detection = supervisor.start(ScopeOptions::default(), |scope| async move {
        // 用内存中的文档头模拟已暂存的上传内容。
        let bytes = Arc::new(b"%PDF-1.7\n".to_vec());
        let resource = scope.register_resource(StagedInput(Arc::clone(&bytes)))
            .map_err(|_| "cannot register input cleanup")?;

        // 检查文件头期间持有使用租约，避免输入被提前清理。
        let input_use = scope.acquire_use(resource).map_err(|_| "input is closing")?;
        let mime = if bytes.starts_with(b"%PDF-") {
            Some("application/pdf")
        } else {
            None
        };
        drop(bytes);
        drop(input_use); // 检测逻辑不再访问输入，归还租约。
        Ok::<Option<&'static str>, &'static str>(mime)
    })?;

    // 相当于读取 try 的执行结果：检测到了什么类型？
    match detection.result_blocking()? {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        other => return Err(format!("unexpected detection outcome: {other:?}").into()),
    }

    // RE检查文件头期间持有使用租约，避免输入被提前清理。
    let report = detection.released_blocking()?;
    assert_eq!(report.resource_phase, ResourcePhase::Released);
    println!("input released");

    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    assert_eq!(shutdown.pending_requests, 0);
    Ok(())
}
```

预期输出：

```text
MIME: application/pdf
input released
```

这段代码做了四件事：

1. 创建输入缓冲区，把清理责任登记到 `scope`。`Arc` 让检测逻辑与清理方暂时共享同一份数据。
2. 获取使用租约，检查文件头，随后释放检测方的数据引用和租约。
3. 用 `result_blocking()` 读取检测结果。示例用 `Option` 区分识别到类型与未识别，用 `Result` 表达检测过程中的错误。
4. 用 `released_blocking()` 取得收尾报告，并检查 `ResourcePhase::Released`，确认受管理输入已释放，然后关闭任务管理器。

两次等待回答不同问题，但不保证有时间间隔：这个小例子的清理可能在读取业务结果前就已完成。真实应用还要处理 `Failed`、`Stopped` 等业务结果，以及资源状态为 `RecoveryRequired` 或 `Unconfirmed` 的情况；取得报告本身不等于释放成功。

## 接入真实的 MIME 检测流程

将示例迁移到暂存文件和外部工具时，应保留同样的关系：

| 示例中的步骤 | 实际集成的责任 |
| --- | --- |
| 创建内存输入、登记 `StagedInput` | 文件适配器创建暂存文件并登记清理责任；同一资源只登记一次 |
| 获取 `input_use`、检查文件头 | 命令适配器持有输入租约，执行 `file`，按需执行 `ffprobe` |
| 释放检测方引用与租约 | 确认进程及输入辅助任务均不再访问文件后，才归还租约 |
| `OwnedCleanup::begin` | 文件适配器删除暂存文件，并如实报告清理失败及恢复责任 |
| 分别读取结果与收尾报告 | 应用根据 MIME 结果响应请求，根据收尾报告处理未释放资源 |

取消通过任务句柄的 `request_cancel` 发出，是协作停止请求。它不能强制中断 native 推理，也不能证明进程已退出。适配器必须在真正停止使用输入后才归还租约；rs-operation 无法自动追踪任意文件句柄或外部进程。

这个例子展示的是生命周期协议。仅做一次内存文件头检查时，普通函数即可；当任务跨多个服务、需要取消并保留清理责任时，统一管理才有价值。

## 核心能力与边界

- 限制根任务数量，以及每个作用域的子任务和资源登记数量；文件字节数、进程数和模型内存等领域预算由对应服务管理。
- 一次性获取类型化业务结果，重复观察最终收尾报告；支持同步阻塞等待和异步等待。
- 通过子作用域、使用租约和清理依赖管理资源，保留需要后续恢复处理的所有权。
- 由应用显式持有任务管理器，支持 `Drain`（等待已有任务）和 `Cancel`（请求取消后收尾）关闭方式。

本库不执行 MIME 检测，不提供文件或命令适配器，也不负责业务重试、补偿或分布式恢复。`PortableDriver` 可驱动可移植的自有 Future；依赖特定运行时的 Future 需要应用接入合适的 `ExternalDriver`。不要在异步运行时工作线程上调用上述阻塞等待接口。

## 延伸阅读

- [中文用户指南](doc/user_guide.zh_CN.md) · [English user guide](doc/user_guide.md)：完整用法与错误处理
- [适配器接入指南（中文）](doc/adapter-guide.zh_CN.md)：领域服务如何接入生命周期协议
- [API 文档](https://docs.rs/qubit-operation)
- 可运行示例：[MIME 输入生命周期](examples/mime_lifecycle.rs)、[同步等待](examples/sync_pipeline.rs)、[异步等待](examples/async_pipeline.rs)、[不可中断调用的取消](examples/inference_cancel.rs)
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

# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English](https://img.shields.io/badge/README-English-blue.svg)](README.md)

`qubit-operation` 用于协调业务结果与资源清理，覆盖父子操作及其后台工作。Rust 应用可以通过同步或异步管理器使用同一套生命周期模型；每个 scope 管理自己的资源和子操作。当操作已取消或业务结果已返回、后台仍在使用数据时，框架可以继续保留收尾责任。

## 场景：检查上传文档

服务可以把上传字节登记到操作 scope，检查 PDF 文件头，再分别观察业务结果和资源结算。文件暂存、MIME 策略、外部命令及恢复决策仍由应用负责。

在应用中添加 `qubit-operation` 0.3 依赖：

```toml
[dependencies]
qubit-operation = "^0.3"
```

crate 的其他依赖由其 manifest 声明。

这个最小示例检查内存缓冲区。保存为 `src/main.rs` 后运行 `cargo run`：

```rust
use qubit_operation::{ManagerOptions, OperationManager, OperationOptions, OperationOutcome};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let mut operation = manager.try_submit(OperationOptions::default(), |scope| {
        let input = scope.manage(b"%PDF-1.7\n".to_vec()).map_err(|error| format!("{:?}", error.kind()))?;
        Ok::<_, String>(input.starts_with(b"%PDF-").then_some("application/pdf"))
    })?;

    let outcome = operation.wait_outcome()?;
    let settlement = operation.wait_settlement()?;
    match outcome {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection failed: {other:?}"),
    }
    println!("资源均已释放：{}", settlement.all_released());
    manager.request_shutdown(qubit_operation::ShutdownMode::Drain).wait()?;
    Ok(())
}
```

`wait_outcome` 回答业务执行结果，`wait_settlement` 报告资源清理和恢复状态；二者不能互相推断。如果 `T` 或 `E`（包括间接持有的对象）含有结算正等待归还的 `ResourceRef`，应使用分步 handle API：先消费并 drop 结果，再等待结算。`run_and_settle` 仅适用于结果不持有此类引用的情况。结算报告计数表示本地 scope 事实；应检查综合 `status` 和 `all_released()`，再从 `external_settlement` 读取外部恢复句柄与 effects。若两层记录了同一资源，不要安排重复清理。参见[含资源结果示例](doc/examples-0.3.0/inference.rs)和[0.2 到 0.3 迁移指南](doc/migration-0.2-to-0.3.zh_CN.md)中的报告字面量迁移说明。

真实集成中，后台或异步使用者必须持有自己的 `ResourceRef`，直到不再访问资源。通过 `scope.manager()` 提交的子操作归属于当前操作，其结果和收尾会纳入父级生命周期。

本库不识别 MIME、不提供文件或命令适配器，也不实现业务重试、补偿或分布式恢复。外部适配器实现 `PreparedExternalOperation`，并分别报告 typed outcome 与任务释放；管理器会在资源使用者归还引用前保留资源所有者。同步管理器的外部适配器使用 `async_executor` 容量和工作线程，普通同步 `try_submit` 任务仍走同步池。适配器回调应短且非阻塞。[完整模拟 provider 生命周期](doc/examples-0.3.0/provider_read.rs)展示这些责任；[leased_temp.rs](doc/examples-0.3.0/leased_temp.rs)只是辅助片段。适配器、所有权、容量、失败处理和关闭细节见[0.3.0 开发者手册](doc/developer_guide-0.3.0.zh_CN.md)。

## 延伸阅读

- [0.3.0 Developer Guide](doc/developer_guide-0.3.0.md) · [0.3.0 中文开发者手册](doc/developer_guide-0.3.0.zh_CN.md) · [0.2 API 设计](doc/design-0.2.0.zh_CN.md)
- 迁移说明：[0.2 到 0.3（English）](doc/migration-0.2-to-0.3.md) · [中文迁移指南](doc/migration-0.2-to-0.3.zh_CN.md)
- [0.2 适配器接入指南（中文）](doc/adapter-guide.zh_CN.md)
- 历史版 0.1 用户指南（介绍不兼容的 0.1 API）：[中文](doc/user_guide.zh_CN.md) · [English](doc/user_guide.md)
- [API 文档](https://docs.rs/qubit-operation)
- 0.2 可运行示例：[文件检测](examples/detect_file.rs)、[推理](examples/inference.rs)、[父子操作与容量](examples/family_and_capacity.rs)、[提交上下文](examples/submission_context.rs)
- 0.3 adapter 生命周期示例：[完整模拟 provider 生命周期](doc/examples-0.3.0/provider_read.rs)、[资源型结果](doc/examples-0.3.0/inference.rs)、[停止/结算辅助片段](doc/examples-0.3.0/leased_temp.rs)
- [English README](README.md)

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

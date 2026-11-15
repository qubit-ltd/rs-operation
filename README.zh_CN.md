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

以下依赖假设 crate 及其依赖均已发布到 crates.io：

```toml
[dependencies]
qubit-operation = "^0.2"
qubit-budget = "^0.10"
```

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

`wait_outcome` 回答业务执行结果，`wait_settlement` 报告资源清理和恢复状态；二者不能互相推断。真实集成中，后台或异步使用者必须持有自己的 `ResourceRef`，直到不再访问资源。通过 `scope.manager()` 提交的子操作归属于当前操作，其结果和收尾会纳入父级生命周期。

本库不识别 MIME、不提供文件或命令适配器，也不实现业务重试、补偿或分布式恢复。共享资源访问不具备独占性；同步机制由资源类型或应用提供。所有权、容量、失败处理和关闭细节见开发者手册。

## 延伸阅读

- [0.2.0 Developer Guide](doc/developer_guide-0.2.0.md) · [0.2.0 中文开发者手册](doc/developer_guide-0.2.0.zh_CN.md) · [0.2 API 设计](doc/design-0.2.0.zh_CN.md)
- 迁移说明：[0.1 到 0.2（English）](doc/migration-0.1-to-0.2.md) · [中文迁移指南](doc/migration-0.1-to-0.2.zh_CN.md)
- [0.2 适配器接入指南（中文）](doc/adapter-guide.zh_CN.md)
- 历史版 0.1 用户指南（介绍不兼容的 0.1 API）：[中文](doc/user_guide.zh_CN.md) · [English](doc/user_guide.md)
- [API 文档](https://docs.rs/qubit-operation)
- 0.2 可运行示例：[文件检测](examples/detect_file.rs)、[推理](examples/inference.rs)、[父子操作与容量](examples/family_and_capacity.rs)、[提交上下文](examples/submission_context.rs)
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

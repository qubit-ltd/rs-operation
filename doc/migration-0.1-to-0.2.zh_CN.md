# qubit-operation 0.1 到 0.2 迁移指南

[English migration guide](migration-0.1-to-0.2.md) · [0.2 中文开发者手册](developer_guide-0.2.0.zh_CN.md) · [历史版 0.1 中文用户指南](user_guide.zh_CN.md)

0.2 将 0.1 的 supervisor/request 协议改为 manager、operation scope 和资源引用，属于源码不兼容升级。迁移时应按新公开接口改调用点；0.1 指南仅供查阅旧代码，不代表当前用法。

## Cargo 使用本地 checkout

`qubit-operation` 0.2 发布前，请让应用依赖本地 `rs-operation` checkout。在应用的 `Cargo.toml` 中，将占位值换成该 checkout 在本机的实际路径：

```toml
[dependencies]
qubit-operation = { path = "<path-to-rs-operation>" }
```

这只是路径占位写法，Cargo 使用前必须替换为真实本地路径。crate 自身的依赖由 checkout 中的 `Cargo.toml` 声明。0.2 发布后，应用可以改用 registry 依赖 `qubit-operation = "^0.2"`。

## 主要接口对应

| 0.1 | 0.2 | 迁移要点 |
| --- | --- | --- |
| `OperationSupervisor` | `OperationManager` 或 `AsyncOperationManager` | 应用在服务启动时创建并持有 manager，再注入请求处理层。 |
| `SupervisorLimits` | `ManagerOptions` | 使用当前 manager 配置容量。 |
| `start` / `start_blocking` | `try_submit` / `submit` | 接纳后由框架创建 operation scope；`try_submit` 不等待容量。 |
| `register_resource` + `acquire_use` | `OperationScope::manage` | 返回带有使用引用的 `ResourceRef<T>`。 |
| `OperationOutcome` | `OperationOutcome` | 业务结果仍与资源收尾分开观察。 |
| `released_blocking` | `wait_settlement` | 查询新的收尾报告，并检查释放、转交或恢复状态。 |
| `shutdown_blocking(mode, timeout)` | `request_shutdown(mode)` 后调用 `wait_for(timeout)` | 关闭过程通过 handle 观察；`Pending` 后保留 handle 继续查询。 |
| `ServiceGroup` 一类的局部边界 | `OperationGroup` / `AsyncOperationGroup` | group 提供局部准入和关闭边界，并共享 manager 生命周期。 |
| async shutdown `wait().await -> ShutdownReport` | `wait().await -> Result<ShutdownReport, ObservationError>` | 传播错误或显式处理 `WouldDeadlock`。 |

## 由应用持有 manager

服务启动时创建 manager，并把 clone 或视图传给请求处理函数。某个子系统需要独立限制并发或单独关闭时，可使用 group。group 负责路由准入；请求资源仍属于每个 operation 自己的 scope。

```rust,ignore
use qubit_operation::{
    GroupOptions, ManagerOptions, OperationManager, OperationOptions,
    OperationOutcome, ShutdownMode,
};

fn setup() -> Result<(OperationManager, qubit_operation::OperationGroup), Box<dyn std::error::Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let inference = manager.group(GroupOptions { capacity: 4 })?;
    Ok((manager, inference))
}

fn submit_document(
    inference: &qubit_operation::OperationGroup,
    bytes: Vec<u8>,
) -> Result<OperationOutcome<Option<&'static str>, String>, Box<dyn std::error::Error>> {
    let mut operation = inference.try_submit(OperationOptions::default(), move |scope| {
        let input = scope.manage(bytes).map_err(|error| format!("{:?}", error.kind()))?;
        Ok::<_, String>(input.starts_with(b"%PDF-").then_some("application/pdf"))
    })?;
    Ok(operation.wait_outcome()?)
}

fn close_service(
    manager: &OperationManager,
    inference: &qubit_operation::OperationGroup,
) -> Result<(), Box<dyn std::error::Error>> {
    let group_shutdown = inference.request_shutdown(ShutdownMode::Drain);
    group_shutdown.wait()?;
    manager.request_shutdown(ShutdownMode::Drain).wait()?;
    Ok(())
}
```

`scope.manage(bytes)` 把值交给请求 scope，并返回 `ResourceRef`。接入文件、provider 请求或 native buffer 时，应把引用交给每个实际使用者，并保留到使用者确认结束。若只传出路径或裸句柄导致生命周期引用丢失，就无法依靠框架保护对应资源。

## 迁移清理与资源交接

普通 Rust 值可使用 `manage(value)`，由常规 `Drop` 释放。异步或可能失败的外部清理应使用 `manage_with(value, cleanup)`，并检查 `SettlementReport`。报告可能表示 `Released`、`Transferred` 或 `RecoveryRequired`；不能把收尾状态和业务结果混为一谈。

适配器把输入交给 worker 时，应将 `ResourceRef` 一并交给 worker，并等待实际完成信号。发出取消请求或丢弃 future 都不能证明 native provider 已停止使用输入。直到确认退出前，worker 持有的引用和执行许可都应保留。可参照[推理示例](examples-0.2.0/inference.rs)及[适配器接入指南](adapter-guide.zh_CN.md)。

## 处理异步关闭错误

异步 shutdown `wait` 现在会返回观察错误。外围函数已返回兼容的 `Result` 时可以使用 `?`；也可以明确匹配错误：

```rust,ignore
let shutdown = manager.request_shutdown(qubit_operation::ShutdownMode::Drain);
match shutdown.as_async().wait().await {
    Ok(report) => println!("closed: {:?}", report.snapshot),
    Err(qubit_operation::ObservationError::WouldDeadlock) => {
        eprintln!("move shutdown waiting outside the managed operation");
    }
    Err(error) => return Err(error.into()),
}
```

同步 `wait()` 同样返回 `Result<ShutdownReport, ObservationError>`。`wait_for(Duration::ZERO)` 只读取一次快照，可在目标操作内使用；正时长阻塞等待若依赖当前操作结束，则可能返回 `WouldDeadlock`。`wait_until` 会先检查时钟和时间域；已过期的 deadline 先返回 `Pending`，不会继续做自等待检查。

框架能检测其托管执行上下文中对匹配 manager/group 的直接自等待，也会保护可能卡住执行器进展的同步等待；它不能发现任意脱离框架的应用任务依赖环。最终 manager 关闭等待应放在服务入口，避免放在自身完成后才可能关闭的 operation 内。

## 更新调用点并验证

1. 替换 0.1 的导入和构造方式，再迁移任务提交与资源登记。
2. 在处理函数中分别处理准入错误、operation 结果和收尾报告。
3. 每个后台或 native 使用者都要持有自己的 `ResourceRef`，直到实际访问结束。
4. 把一次性关闭调用改为保留 shutdown handle，并检查 `Completed` 或 `Pending`。
5. 传播或匹配异步 `wait()` 错误，逐处检查 `wait().await`。
6. 编译所有目标并运行当前示例；完整生命周期说明见[0.2 中文开发者手册](developer_guide-0.2.0.zh_CN.md)。

0.1 文档仍保留为历史参考：[English](user_guide.md) 和[中文](user_guide.zh_CN.md)。

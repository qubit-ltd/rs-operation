# 用户指南（0.1.0）

[English README](../README.md) · [中文 README](../README.zh_CN.md) · [适配器接入指南](adapter-guide.zh_CN.md) · [API 文档](https://docs.rs/qubit-operation)

本指南面向需要管理多个 owned 操作及其资源收尾的 Rust 应用开发者。贯穿场景是处理一个下载、转换、发布任务：即使业务结果已经产生，转换器仍可能占用临时资源，因此应用需要分别判断“做成了什么”和“资源是否已释放”。

## 1. 创建 supervisor 并提交任务

使用 Rust 1.94。若从本仓库 checkout 接入，在 `Cargo.toml` 中添加 `qubit-operation = { path = "../rs-operation" }`；crate 默认不启用 feature，也没有运行时依赖。应用应在服务生命周期中创建并持有 `OperationSupervisor`，在应用关闭时显式结束它。

```rust,no_run
use qubit_operation::report::{OperationOutcome, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};
use std::time::Duration;

let supervisor = OperationSupervisor::new(SupervisorLimits::default());
let mut operation = supervisor.start(ScopeOptions::default(), |_scope| async {
    // 这里由应用实现下载、转换与发布逻辑。
    Ok::<_, &'static str>("artifact-42")
})?;

let outcome = operation.result_blocking()?;
match outcome {
    OperationOutcome::Succeeded(id) => println!("业务结果：{id}"),
    OperationOutcome::Failed(error) => eprintln!("业务失败：{error}"),
    other => eprintln!("操作未成功：{other:?}"),
}
let settlement = operation.released_blocking()?;
println!("结算阶段：{:?}", settlement.resource_phase);
let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
# Ok::<(), Box<dyn std::error::Error>>(())
```

这是无外部服务的最小练习。实际应用需要把自己的存储、下载器和转换器注入任务，并在创建资源前通过 `OperationScope::register_resource` 登记其 `OwnedCleanup` owner。成功信号是 `OperationOutcome::Succeeded`，随后结算报告的 `resource_phase` 表示资源终结状态。运行对应的完整示例：[`sync_pipeline`](../examples/sync_pipeline.rs)；异步等候见[`async_pipeline`](../examples/async_pipeline.rs)。

## 2. 业务结果与资源结算分开判断

`result_blocking` / `result().await` 只消费一次类型化业务结果；`released_blocking` / `released().await` 可重复观察最终结算。结果可能是 `Succeeded(T)`、`Failed(E)`、`ChildFailed`、`Stopped` 或 `Panicked`。子操作或资源收尾仍在进行时，业务结果可以先到。

同步调用方可使用 `OperationHandle` 的阻塞观察方法。异步调用方应 await 异步方法；不要在负责推进异步 runtime 的线程中阻塞等待。`run_to_completion` 会同时等结果与结算，适用于结果不依赖当前 scope 存活的情形。

**资源型结果要先处理所有权。** 如果 `T` 中带有当前 scope 资源的访问句柄，不能拿着该资源租约等待同一 scope 的 `released`，否则清理要等租约归还，而调用方又在等清理。应在 body 内释放租约，或先用 `transfer_resource_to` 把资源及其依赖组转交给另一个已接纳的 scope，再等待结算。转交失败时原 owner 仍由源 scope 管理。

## 3. 取消、子操作与 Drop

取消通过 `request_cancel(StopReason::UserCancelled)` 发出协作式停止请求。`Stopped` 表示业务结果未能确定；请求已发送不等于不可中断调用已退出。若调用方 Drop 尚未终结的 `OperationHandle`，handle 会请求停止并放弃 typed result 的交付；Drop 不等待、不强杀任务。observer Drop 不会取消操作。需要继续查询时可调用 `detach`，但 supervisor 仍负责该任务的关闭与收尾。

父任务在 `OperationScope` 中创建 child scope 或用 `reserve_child` 登记受管子操作。登记流程是 reserve、attach、commit：只有 commit 成功后才允许启动子操作的副作用；已提交子操作由 scope 观察结果和释放。默认策略为 `FailFast`：required 子操作失败会请求停止兄弟任务；这不是立即终止。`Collect` 允许独立工作继续，但 required 子操作失败仍不能被父操作报告为成功。适合 fallback 的可选工作应明确登记为 optional，并由父 body 自行处理其业务结果。

## 4. 容量、期限与背压

默认 `SupervisorLimits` 是有限容量：活动 root 4、排队 root 64、准入等待者 64、清理并发 4、未取业务结果 64；每 root 默认最多登记 64 个子操作和 128 个资源。限制作用于登记项和结果槽，不是进程总内存上限。`T`、`E` 和应用捕获值的大小由应用负责。

未取业务结果会继续占结果槽，直到读取或明确放弃；槽耗尽时新工作可能收到 `Busy`。超出 scope 登记容量时注册会失败，调用方应保留错误返回的 owner/input 并决定是否降级或拒绝请求。不要无限重试准入或清理来绕过背压。

期限由 `OperationContext` / `ContextOptions` 提供。准入时过期会拒绝并归还未启动 body；运行中期限发出停止请求，不能保证取消阻塞系统调用或 native 推理。等待者等待 deadline 也只限制观察时间，并不终结后台操作。调用方应保留 observer，之后继续检查结算。

## 5. 清理失败与恢复

资源先登记，再创建或暴露其句柄；使用者通过 `acquire_use` 获得不可克隆的 `ResourceUse`，完成后释放。关闭未来使用后，租约归零，清理才可进入可执行状态。资源依赖用 `cleanup_after(resource, prerequisite)` 声明；前置资源未确认释放时，后继清理不能被当成完成。

清理 owner 实现 `OwnedCleanup`，消耗 owner 并返回 `CleanupOutcome::Cleaned`、`Transferred(receipt)` 或 `FailedWithRecovery(failure, owner)`。恢复 owner 继续占资源登记配额。观察者看到恢复摘要不代表已恢复；应用应按类型化 key 执行重试、取回 owner 或显式保留责任。框架不替业务重试或补偿副作用，也不把 Unknown/Unconfirmed 当作成功释放。

## 6. 驱动器与适用边界

| 能力 | 当前接口 / 依赖 | 边界 |
| --- | --- | --- |
| 默认异步工作 | `OperationSupervisor::start`、`PortableDriver` | owned、`Send + 'static` Future；不提供特定 runtime reactor |
| 同步工作 | `start_blocking`、应用提供的 `BlockingExecutor`，可用 `ThreadExecutor` | 固定工作线程和队列；饱和时拒绝，不为每项任务无限建线程 |
| 外部运行时任务 | `ExternalDriver`、`DriverLease` | 应由应用在绑定的 runtime 中推进；本 crate 不含正式 Tokio 适配器 |
| 默认 feature | `default = []` | 不依赖 Tokio、rs-progress 或 rs-event-bus |

传入 Future 和清理 Future 必须 owned、`Send + 'static`。任务 poll、用户值析构与清理实现须符合非阻塞约定；协调器不能抢占一个阻塞 poll，也不能强行终止 native 工作。没有基准测试数据时，不据此推断吞吐或延迟优势。

## 7. 排障

- **`Busy` / 注册失败：** 检查活动、排队、结果槽和 scope 登记是否已满；确认旧结果已消费、旧恢复责任已处理。
- **取消后仍未 Released：** 检查不可中断任务是否返回、所有 `ResourceUse` 是否归还，以及清理依赖是否已有确认。
- **进入恢复状态：** 查结算报告中的失败摘要和 resource ID；使用对应 scope 的恢复 key 取回 owner或按策略重试。不要只因主业务成功就丢弃恢复责任。
- **`WouldDeadlock`：** 当前在库管理的 worker 中调用阻塞观察/等待；改用 async 方法或把等待移到外部线程。
- **关闭报告仍有 pending：** timeout 只限制本次等待。保留 shutdown observer，并继续检查请求、资源及恢复数量。

## 后续阅读

[适配器接入指南](adapter-guide.zh_CN.md) · [API 文档](https://docs.rs/qubit-operation) · [English README](../README.md) · [中文 README](../README.zh_CN.md)

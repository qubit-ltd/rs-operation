# rs-operation 0.3.0 开发者手册

[English developer guide](developer_guide-0.3.0.md) · [中文 README](../README.zh_CN.md) · [0.2 迁移指南](migration-0.2-to-0.3.zh_CN.md)

本手册介绍 0.3 API 如何把受管操作接入应用自有执行器和 provider。管理器负责准入、操作 scope、停止传播、结果交付和结算；适配器负责实际外部任务，并必须报告任务何时不再使用输入资源。

## 1. 外部操作适配器

为准备好的任务实现 `PreparedExternalOperation<T, E>`。`T` 和 `E` 必须满足 `Send + 'static`；适配器类型必须是 `Sized`，并实现 `OperationControl + 'static`。该 trait 定义四个生命周期方法：

- `bind(&self, id)` 一次性认领准备好的操作。若其他提交已认领，应返回 `ExternalBindError::AlreadyBound`。`Failed(ExecutionFailure)` 表示操作已接纳但适配器设置失败。
- `activate(self: Arc<Self>, scope)` 使用管理器提供的 `OperationScope` 启动外部工作。
- `poll_take_outcome` 在结果就绪时把唯一的 typed `OperationOutcome<T, E>` 交给管理器；不得克隆或重复发布结果。
- `abandon(self: Arc<Self>)` 释放已认领但未启动的绑定。

通过 `OperationManager::try_submit_external` 或 `AsyncOperationManager::try_submit_external` 提交。接纳后返回普通 typed handle；拒绝时返回 `SubmitError<Arc<P>>` 并保留原适配器。`AdmissionError::AlreadyBound` 是新增的穷尽匹配分支：匹配准入错误时要明确处理。

业务结果可用后，框架仍会保留操作记录。provider 报告 `Stopped` 并不能证明 worker 已归还输入。应让每个 `ResourceRef` 一直存活到外部使用者确实停止访问，并独立报告结果与资源释放事实。参见 [provider_read.rs](examples-0.3.0/provider_read.rs)。

## 2. 结果、停止与结算

`OperationHandle::request_stop` 及异步版本返回 `StopRequestStatus`，不再是 `bool`。对三个状态分别处理：`Accepted` 表示本次请求新发起停止；`AlreadyRequested` 表示停止已登记；`AlreadyFinished` 表示操作已结束。`OperationScope::request_stop` 返回相同枚举。请更新所有穷尽匹配。

业务结果和结算仍是两种独立观察。`wait_outcome` 消费唯一 typed 结果；`wait_settlement` 观察资源和子任务报告。如果 `T` 或 `E`（包括二者间接持有的对象）中包含结算正等待归还的 `ResourceRef`，必须先取出并 drop 结果及其派生引用，再等待结算。持有该资源时等待结算可能形成对自身引用的等待。同步和异步 `run_and_settle` 也有相同限制；若返回的 `T` 或 `E` 保留此类引用，可能一直等待。参见[含资源结果示例](examples-0.3.0/inference.rs)。

`SettlementReport` 的计数和明细数组表示管理器本地 scope 的事实。管理器驱动外部任务时，`external_settlement` 保留适配器的领域报告，包括恢复句柄和 effects；外层 `status` 综合两个来源（`RecoveryRequired` 优先于 `Transferred`，再优先于 `Released`），`all_released()` 也检查领域报告。应先检查 status 与 `all_released()`，再读取 `external_settlement` 判断恢复责任。外层 `recovery_count` 为零不表示没有恢复责任；两份报告若记录同一资源，也不应当作两个清理任务。手写 `SettlementReport` 字面量时，叶报告需增加 `external_settlement: None`。

## 3. Scope 资源与组

当创建资源前必须确保容量和清理责任时，使用 `OperationScope::reserve_resource`。预留失败不会创建资源；只有创建成功后才提交预留，并为每个使用者保留相应 `ResourceRef`。服务资源 scope 和普通操作 scope 共用管理器有界的清理及容量规则。

`OperationManager::with_group` 会创建受服务组约束的管理器视图，同时保留原 root 或 parent 目标。一个操作可绑定的不同组数量受 `ManagerOptions::max_groups_per_operation` 限制，默认值为 16。若应用的组拓扑已知且不同，可显式设置。超过上限会返回 `GroupBindingError::CapacityExceeded`；其他变体分别表示管理器不匹配、组已关闭或父 scope 已结束。同一组重复绑定不会重复占用名额。

## 4. Effect、恢复与关闭

`OperationScope::record_effect` 记录应用报告的事实，不证明回滚、清理或远端释放已经完成。继承的 effect 预算有界；容量耗尽时返回 `EffectRecordError`，保留已有记录并增加报告中的 overflow 计数。停止或完成后也会拒绝新记录。

结算可能报告需要恢复的资源。应保持恢复 owner 可达并按报告继续处理；诊断容量限制可能截断摘要，但不会丢弃恢复 store 持有的资源 owner。外部 worker 仍持有资源或清理、恢复尚未完成时，管理器关闭可以保持 Pending。保留 shutdown handle，并在 worker 或恢复流程取得进展后再次观察。

## 5. 能力边界与示例

本库协调生命周期，不实现 provider 协议、文件或命令适配器、业务重试、补偿或分布式恢复。应用仍须负责共享资源内容的同步，并保证适配器报告的停止和释放事实准确。仓内三个 0.3 示例由 Cargo 作为 library target 编译：provider 停止、资源型结果及租用的临时资源。

源码迁移变化见[0.2 迁移指南](migration-0.2-to-0.3.zh_CN.md)。

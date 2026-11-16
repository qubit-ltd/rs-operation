# qubit-operation 0.2 到 0.3 迁移

0.3 增加公开的外部操作适配器协议及有界多组绑定。既有受管操作仍使用相同的 outcome、资源和 settlement 模型。请更新穷尽匹配，以及绑定管理器视图到服务组的代码。

## 外部任务

为应用执行器或 provider 拥有的工作实现 `PreparedExternalOperation<T, E>`。绑定只能成功一次：同一个准备好的实例已被认领时，返回 `ExternalBindError::AlreadyBound`。通过 `OperationManager::try_submit_external` 或 `AsyncOperationManager::try_submit_external` 提交；提交拒绝时，`SubmitError` 会保留原始 `Arc<P>`。使用提供的 scope 实现 `activate`，通过 `poll_take_outcome` 发布唯一 typed outcome，并为已认领但未启动的任务实现 `abandon`。外部 worker 真正停止使用资源前，应继续持有其资源引用；提前得到 `Stopped` 结果不代表资源已经释放。

`AdmissionError` 新增 `AlreadyBound`，需要把它加入穷尽匹配。准入失败时准备好的对象仍会退还给调用方，调用方可按自己的所有权策略检查或丢弃。

## 停止请求返回值

`OperationHandle::request_stop`、`AsyncOperationHandle::request_stop` 和 `OperationScope::request_stop` 返回 `StopRequestStatus`，不再返回 `bool`：

```rust,ignore
match handle.request_stop(StopReason::UserCancelled) {
    StopRequestStatus::Accepted => { /* 本次调用新发起停止 */ }
    StopRequestStatus::AlreadyRequested => { /* 停止请求已登记 */ }
    StopRequestStatus::AlreadyFinished => { /* 操作已经结束 */ }
}
```

请在穷尽匹配中处理全部三个变体。如果调用方只需要判断是否刚刚接纳，可明确使用 `matches!(status, StopRequestStatus::Accepted)`。

## 含资源的 outcome

`T` 或 `E` 都可能直接或通过其他对象持有 `ResourceRef<T>`。使用 `wait_outcome` 取出唯一结果，完成对返回引用的使用并 drop 它们；如果结算需要等待这些引用归还，之后再等待 settlement。尚未消费的结果中仍持有资源引用时等待结算，可能一直等待调用方自己的引用。同步和异步 `run_and_settle` 也有相同限制；资源型结果应使用 `submit`/`try_submit`、`wait_outcome`，drop 结果后再调用 `wait_settlement`。普通 owned 值仍可使用 `run_and_settle`。参见[含资源结果示例](examples-0.3.0/inference.rs)。

`SettlementReport` 新增 `external_settlement: Option<Arc<SettlementReport>>`。手写叶报告字面量时设为 `None`；管理器生成的外层报告在适用时填入该字段。外层计数和明细数组仍表示本地 scope，`status` 综合 scope 与领域状态，`all_released()` 同时检查两者。应先检查 status 和 `all_released()`，再从领域报告读取恢复句柄与 effects；外层 `recovery_count` 为零不能排除领域恢复责任。同一资源出现在两层时不要重复清理。

Outcome 与 settlement 彼此独立：提前得到业务结果或停止结果，不代表后台工作已归还输入。

## Recovery 失败历史

`RecoverySnapshot` 新增 `failures_total` 和 `failures_truncated`。手写结构体字面量需要初始化这两个字段；穷尽解构需要显式列出新字段或使用 `..`。`last_failures` 现在只保留最近 64 条失败，并按从旧到新的顺序排列；成功清理会清空历史并重置两个元数据字段。`RecoveryAttemptReport.failures` 仍是尝试完成时保留的历史，不是单独的单次尝试事件列表。需要完整历史日志的应用应自行把失败写入日志系统。条目上限不限制单个错误来源的大小。

## 组上限与错误

`ManagerOptions` 新增 `max_groups_per_operation`，不同服务组默认最多 16 个。若已知的应用拓扑需要其他上限，请显式配置。`OperationManager::with_group` 返回 `Result<OperationManager, GroupBindingError>`；绑定会超过上限时，要处理新增的 `CapacityExceeded`。已有变体为 `WrongManager`、`Closed` 和 `ScopeEnded`。重复绑定同一组仍是幂等操作，不额外占用名额。

该上限控制每个操作的组扇出，不替代管理器或 scope 的资源、操作容量限制。API 与示例详见[0.3 开发者手册](developer_guide-0.3.0.zh_CN.md)。

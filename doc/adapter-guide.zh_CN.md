# 领域适配器接入指南（0.1.0）

[用户指南](user_guide.zh_CN.md) · [English README](../README.md) · [中文 README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-operation)

本文面向把进程、文件 provider、媒体探测器或模型 session 接入 `qubit-operation` 的维护者。适配器保留领域语义与真实释放证据；本库提供 scope、登记协议、租约和结算观察。请以 T10 公共接口及本仓库当前代码为准，不要把下游尚未实现的设想包装成现有保证。

## 接入前先划清责任

为每个领域 owner 明确：谁创建它、谁持有原生句柄、何时可以停止新使用、哪个事实能证明它已释放、失败后由谁继续恢复。长期领域资源由应用的 `ServiceResourceScope` 持有；一次请求的临时资源由请求的 `OperationScope` 持有。不要把所有请求挂在一个长期 root 上。

对每个副作用只指定一个登记 owner。若 facade 包装底层已登记的实现，应透传同一登记与 lease，而不是再次登记、重复清理同一个对象。

## 先登记，再启动副作用

受管子操作使用 `reserve_child` → `attach` → `commit`。适配器应按以下顺序实现：

1. 先 reserve，确认 scope 接受该子操作及其配额。
2. attach 能请求停止、报告业务状态与等待真实释放的 control。
3. 将必须与子操作共同存活的 `ResourceUse` leases 一起 attach（若适用）。
4. commit 成功后才启动领域副作用；失败时归还尚未启动的输入和许可。

不要在 executor 回调、`poll` 或内部锁中启动外部工作。取消与 commit 竞态必须有明确线性化点：取消先被接受时，不能随后提交并启动副作用；commit 先完成时，该工作必须继续受 scope 管理。领域 executor 如需单独许可，应在 commit 前取得，并在拒绝时原样归还。

## 接入资源与清理 capsule

资源 owner 实现公开 `OwnedCleanup`：

```rust,ignore
impl OwnedCleanup for NativeSession {
    fn begin(
        self: Box<Self>,
        context: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin(async move {
            // 适配器在这里异步关闭原生 session，并只在确认后返回 Cleaned。
            self.close_and_confirm(context).await
        })
    }
}
```

这是结构示意；`NativeSession::close_and_confirm` 由领域适配器实现，不是本 crate API。cleanup capsule 必须拥有其执行所需的 owner/state，不能借用已结束的业务栈。先调用 `OperationScope::register_resource(owner)`，再创建或暴露资源句柄。每个使用路径持有独立 `ResourceUse`；停止新使用后，直到所有适配器内部使用者释放 lease 才可以清理。

清理结果必须表达可证实事实：`Cleaned` 表示领域确认释放；`Transferred(receipt)` 表示另一个明确 owner 接受责任；`FailedWithRecovery` 必须同时保留可恢复 owner。若 runtime、设备或进程暂时无法确认，保留 Unconfirmed/恢复责任，不得合成成功报告。清理 panic 时的 owner 恢复方式必须由独立 capsule/注册表保留；不能只把唯一 owner 移进可能 panic 的 future。

## 线程归属和不可中断工作

在适配器设计中记录每个值的创建线程、调用线程、析构线程和 runtime 归属。线程绑定的 native session 不应被默认 `PortableDriver` 任意迁移；应由保持其线程归属的领域 executor 执行控制及清理。若使用 `ExternalDriver`，应用需保持该 driver/runtime 存活至已接纳工作释放，并能报告 driver 丢失或未确认状态。

阻塞或不可中断工作可通过 `start_blocking` 交给应用提供的 `BlockingExecutor`；对已有领域线程池不要再套线程池。取消只通知停止，容量和输入 leases 必须持续占用到领域调用真正退出。通过 `OperationScope` 的子操作 control 报告停止与释放事实。不得让管理线程直接执行会阻塞的推理、I/O 或任意用户 closure。

## 依赖、恢复与 facade

用 `cleanup_after(resource, prerequisite)` 表达有序释放。依赖失败时后续 owner 仍需留在恢复组内；单独取走前置 owner 不应假装解除了后续责任。跨 scope 转移必须一次性转交完整依赖组并通过目标配额接纳；失败时源 owner 仍归原 scope 管理。

Facade 需要给出唯一登记责任的可追踪证据：同一个 owner 在报告中只出现一个 registration、只触发一个 cleanup，底层 `ResourceUse` 由实际使用方持有到使用结束。对外导出无法约束生命周期的裸句柄时，应明确其保证边界，不能声称 lease 覆盖了调用方复制出的原生句柄。

## 适配器验收清单

- [ ] 注册容量满时，副作用尚未启动，原始输入和领域许可可由调用方收回。
- [ ] reserve/attach/commit 与取消交错时，不出现未登记的运行中工作。
- [ ] 停止请求不早于事实地报告调用退出；最后一个 use lease 归还前不清理。
- [ ] 清理返回 Cleaned 前有领域释放证据；转交和恢复保留唯一 owner。
- [ ] 清理 panic/运行时丢失/设备不可用时仍有恢复 owner 或明确的 Unconfirmed 报告。
- [ ] 线程绑定值不会在错误线程 poll、关闭或析构；不阻塞管理线程。
- [ ] facade 与底层实现没有重复登记/清理。
- [ ] 主业务错误、部分副作用与清理错误分别保留；没有自动补偿或无界重试假设。
- [ ] shutdown Drain/Cancel 后继续持有 pending observer，直到责任释放、恢复或显式转交。

记录可重复的 gate、故障注入和计数器证据，包含拒绝、取消、释放、恢复及关闭路径。不要用固定 sleep 作为竞态证明，也不要宣称框架测试替代真实进程、文件系统、provider 或 native runtime 的领域验收。

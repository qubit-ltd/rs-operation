# 领域适配器接入指南（0.2）

[0.2 中文开发者手册](developer_guide-0.2.0.zh_CN.md) · [0.2 API 设计](design-0.2.0.zh_CN.md) · [中文 README](../README.zh_CN.md) · [English README](../README.md) · [API 文档](https://docs.rs/qubit-operation)

本文面向把文件、子进程、媒体探测器、模型 session 等领域资源接入 `qubit-operation` 的维护者。适配器负责真实领域行为和释放证据；本库负责操作归属、`ResourceRef` 生命周期、准入、结果与 settlement 汇总。请使用 0.2 API；旧 0.1 的 `reserve_child`、`attach`、`commit`、`ResourceUse`、`OwnedCleanup` 和 `ServiceResourceScope` 入口不属于当前操作 API。

## 先确定 owner 和范围

接入前为每个资源回答四个问题：谁创建它，哪个 scope 登记它，哪些工作可能继续使用它，什么事实能证明它已经释放。一次请求的临时资源通常由该请求的 `OperationScope` 管理；跨请求的长期资源应放进应用明确管理的服务资源范围。一个 owner 只登记一次。Facade 若包裹已登记资源，应传递同一份 `ResourceRef`，不要再次登记或重复清理。

`scope.manage(value)` 将资源登记到当前操作的资源范围，并返回 `ResourceRef<T>`。该引用同时提供共享访问并计入使用者数量；clone 增加一份使用引用，move 不增加，Drop 归还一份。资源只会在**所属范围结束且所有 `ResourceRef` 都已归还**后进入清理。父子关系本身不会推迟无关父资源的释放；子工作要继续访问父资源，必须拿到并持有该资源的引用。

```rust,ignore
let input = scope.manage(staged_file).map_err(|error| format!("{:?}", error.kind()))?;
let child_input = input.clone();
let child = scope.manager().try_submit(OperationOptions::default(), move |_child_scope| {
    run_command(&child_input)
})?;
let _observer = child.into_observer();
```

这是局部接入片段：`staged_file`、`run_command` 和错误转换由应用定义。`child_input` 必须一直留在子闭包中，直到子进程及所有文件访问真正结束。请求已取消、父主体已返回或业务 outcome 已经产生，都不能代替这个释放事实。`scope.manager()` 绑定当前操作，所以 child 是直接子操作；不要把根 manager 传进去冒充子提交。

`ResourceRef<T>` 只提供共享 `&T`，不授予独占访问。若多个 worker 会修改同一状态，应由 T 或应用提供 `Mutex`、`RwLock`、原子操作或领域同步。框架不在整个引用持有期间加锁，也不追踪应用复制出去的裸句柄。

## 清理要有可核验的结果

如果普通析构足以释放资源，使用 `scope.manage(resource)`。需要异步关闭、远端确认或保留恢复 owner 时，使用 `scope.manage_with(resource, cleanup)` 并实现 `AsyncCleanup<T>`。成功的 `Released` 应对应适配器能够说明的释放事实；`Transferred(receipt)` 表示已由明确接收方接手责任，不代表物理资源已经释放；`Failed { failure, resource }` 必须保留可恢复的资源 owner。

```rust,ignore
impl AsyncCleanup<NativeSession> for CloseSession {
    fn cleanup(
        self: Arc<Self>,
        resource: NativeSession,
        context: CleanupContext,
    ) -> CleanupFuture<NativeSession> {
        Box::pin(async move {
            match resource.close_and_confirm(context).await {
                Ok(()) => CleanupOutcome::Released,
                Err((failure, resource)) => CleanupOutcome::Failed { failure, resource },
            }
        })
    }
}
```

`NativeSession::close_and_confirm`、错误类型和清理执行环境由领域适配器提供；片段展示 trait 形状，不是完整程序。不要在 `Drop` 中等待、阻塞或执行可能失败的异步关闭。若创建资源可能在登记前失败，先通过 `scope.reserve_resource()` 预留责任与额度；副作用失败后，使用 reservation 的 `retain_with_for_cleanup` 保留部分资源，不能把唯一 owner 丢进普通析构。

清理报告中 `DropCompleted` 只证明 Rust 析构已完成，不证明外部进程、远端 session 或设备状态已得到独立确认。需要更强保证时，由适配器返回带有证据的清理结果，并对无法确认的状态保留恢复责任。

## 把结果、收尾和容量分开观察

`wait_outcome` 回答业务执行结果，`wait_settlement` 回答操作及子树自动收尾责任是否已有稳定报告。`Succeeded` 不代表资源已释放；`wait_settlement` 得到报告也不代表报告状态一定是 Released。应用必须检查 `SettlementReport` 的状态、资源报告和清理失败，再决定是否继续、重试或转交。

同步任务使用 `OperationManager`，异步任务使用同一核心的 `manager.as_async()` / `AsyncOperationManager`。`OperationManager` 的同步闭包由配置的执行器运行；不要在该 worker 中同步等待只有同一执行器退出后才能获得容量的子任务。异步主体等待子 outcome 时，可把子 handle 转为异步视图并 `await`；异步等待不会消除逻辑上的容量依赖。

各类额度独立计算：root / 子操作、结果槽、资源登记、清理并发和执行器队列不是一个总数。子提交成功不保证资源登记成功；`ResourceRef::clone()` 不新增资源登记，但增加使用引用。容量只有在对应责任稳定归还后才可重用。业务侧的文件字节数、进程数、模型内存和连接池应由应用的领域预算控制，可参考 [rs-budget](https://github.com/qubit-ltd/rs-budget)；不要把管理器的操作额度当作业务资源预算。

## 停止、恢复和服务关闭

停止是协作式信号，不能杀死任意 native 调用，也不撤销仍存活的 `ResourceRef`。不可中断调用必须运行在适合的同步执行器或领域线程池中，并在真实返回之前保留输入引用与执行许可。不要再包一层通用线程池，也不要让管理线程直接执行 I/O、推理或清理闭包。

恢复对象必须连同仍需的资源 owner 一起保留。跨服务移交时使用接收方确认的转交协议；若使用 `RecoveryStore`，先配置 `RecoveryStoreOptions` 的组容量、资源容量、清理并发和执行器，再根据 `RecoveryHandle` 的状态重试或转交。历史 settlement 是不可变快照；后续恢复成功不会改写原报告。

服务关闭由应用持有的 manager / service owner 发起。Drain 停止新 root 准入并等待已接纳的树；Cancel 还请求活动树停止。若关闭观察返回 Pending，应保留观察句柄、执行器和恢复设施，检查尚在运行的任务、`ResourceRef`、清理和恢复责任，再继续观察。不要使用固定 sleep 推断退出或释放。

## 适配器验收清单

- [ ] 容量拒绝发生在领域副作用启动之前；原输入、许可和部分资源仍有明确 owner。
- [ ] 后台和异步使用者持有 `ResourceRef` 到真实使用结束；scope 结束且引用归零后才清理。
- [ ] 父资源不会被无关子资源的清理拖住；仍需父资源的子清理明确持有其引用。
- [ ] 停止信号、业务 outcome、执行退出、资源释放和 settlement 分别记录，不互相冒充。
- [ ] `Released` 有对应证据；转交有接收方回执；失败保留恢复对象；未确认不伪装成功。
- [ ] facade 不重复登记或清理；适配器不把未受管裸句柄的生命周期纳入虚假保证。
- [ ] 线程绑定资源只在所属领域执行器访问和关闭；管理线程不运行阻塞 I/O、模型调用或用户清理。
- [ ] 容量测试分别覆盖提交拒绝、资源登记拒绝、恢复占额和实际归还时点。
- [ ] 关闭 Drain / Cancel 后持续观察 Pending；执行环境保持可用，直到释放、恢复或显式转交。

使用 gate、显式通知和计数器记录可重复的交错及证据。fixture 只替代外部领域服务、时钟或故障源，不替代被验收的 manager、scope、资源引用或清理实现。固定 sleep 不能证明竞态或释放顺序。

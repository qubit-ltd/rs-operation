# rs-operation 0.1.0 实施契约（T01）

本文把设计与实施计划第 6 节收敛为后续任务共同遵循的类型边界。设计文件的语义优先；这里固定 Rust 名称、所有权与观察形式。T01 只提供协议、报告类型和编译门禁，不实现 supervisor、driver、操作句柄、scope 或运行时行为。

## 公共模块与导出

crate root 由集成者声明 `pub mod protocol; pub mod report;`。报告类型经 `report` 导出；三项适配器端口经 `protocol` 导出。`internal` 由 crate root 以私有模块声明。T01 worker 不修改 `src/lib.rs`、`Cargo.toml` 或 `Cargo.lock`。

报告包含：`OperationId(u128)`、由 operation/slot/generation 构成的 `ResourceId`、`ClockDomainId`、带时钟域的 `MonotonicTime`、受理与观察错误、保留未启动输入的 `StartError<F>`、泛型 `OperationOutcome<T, E>`、子操作/停止/副作用摘要、panic 摘要、清理失败与结果、不可变 `SettlementReport` 及资源状态快照。`DiagnosticSummary` 将每段诊断限制在 256 UTF-8 字节，并在字符边界截断。所有快照只存受控摘要和共享不可变数据，不接管业务 `T` 或 recovery owner。

`RecoveryKey<R>` 构造器仅 crate 内可见；它绑定完整 `ResourceId`（含 generation）与单次 nonce。`RecoveryOwner` 只能由 crate 内恢复注册表构造，不公开 `Any`、downcast 或原始 payload 访问；只有持有对应 `RecoveryKey<R>` 的 owner 能尝试取出 `R`。类型或 key 不匹配时 owner 原样归还。`TransferReceipt` 的字段不可公开构造，只能在接收方接受责任后由注册表创建。Recovery owner 与结算历史分开保存，`SettlementReport` 不因后续恢复操作而被回写。

## 固定端口签名

```rust,ignore
pub trait ManagedOperationControl: Send + Sync {
    fn request_stop(&self, reason: StopReason);
    fn snapshot(&self) -> ChildSnapshot;
    fn poll_business(&self, cx: &mut Context<'_>) -> Poll<ChildBusinessSummary>;
    fn poll_released(&self, cx: &mut Context<'_>) -> Poll<ChildSettlement>;
}

pub trait ClockSource: Send + Sync {
    fn now(&self) -> MonotonicTime;
    fn register_deadline(&self, at: MonotonicTime, wake: Waker)
        -> Box<dyn TimerRegistration>;
}

pub trait OwnedCleanup: Send + 'static {
    fn begin(self: Box<Self>, context: CleanupContext)
        -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>;
}
```

`ManagedOperationControl` 是对象安全、非阻塞的控制/观察端口。`poll_business` 不搬走领域 typed result；`poll_released` 的 Ready 只能来自领域真实释放确认。Pending 必须登记 waker。停止请求不等于停止完成。

`ClockSource` 产出的时间只在同一 `ClockDomainId` 内可比较。不同域的 deadline 必须显式换算，禁止比较 tick 数。`TimerRegistration::cancel(self: Box<Self>)` 只撤销该次注册，不能误删复用槽位上的新一代定时器。

`OwnedCleanup` 接管 cleanup owner，Future 不借用业务栈，并必须返回 `Cleaned`、`Transferred(TransferReceipt)` 或 `FailedWithRecovery(CleanupFailure, RecoveryOwner)`。T08 必须让外部 cleanup 适配器通过受控转交接口请求接收方注册表接纳 owner；只有接收方实际接纳后才能返回其签发的 `TransferReceipt`，适配器不能自行构造回执。cleanup panic 的 recovery capsule 必须仍由 supervisor 的独立注册表持有；不得只把唯一 owner 移入可能 unwind 的 Future。不能提供独立 capsule 的适配器不能承诺 panic 后可恢复。

## 状态与事实

- 业务阶段为 `Queued → Running → ResultReady → ResultTaken`；取消原因是独立事实。
- 资源阶段为 `Active / Stopping / Cleaning / Released / RecoveryRequired / Unconfirmed`。只有领域确认释放且必需清理已完成或已显式转交，才可报告 `Released`。
- `OperationOutcome<T, E>` 保持五种设计变体：`Succeeded(T)`、`Failed(E)`、`ChildFailed(ChildFailureSummary)`、`Stopped(StopReport)`、`Panicked(PanicSummary)`。框架不要求 `E: Clone`，也不把 child error 强转为父 `E`。
- `AdmissionError` 只代表 body 尚未启动前的受理失败。`StartError<F>` 原样保留并可取回 `F`。`ObservationError` 与领域业务错误分离。
- `SettlementReport` 是不可变的历史快照，独立记录子操作终结、清理失败、owner 转交、未确认数量及 `all_released`。业务成功可与清理失败并存；清理事实不能覆盖业务结果。
- 只在诊断中使用有界摘要。默认不记录路径、命令、内容或模型特征；摘要不能伪称外部事实已被框架验证。

## 内部端口与后续接口承诺

`src/internal/mod.rs` 的 Driver、BlockingExecutor、CleanupPort、ChildRegistry 及 `ChildReservation → attach → ChildCommit::commit` 均为 crate-private、对象安全端口。`ChildOptions` 固定 Required/Optional。Driver 在注册和启动前拒绝时用 `DriverRejected` 返还原 future；BlockingExecutor 用 `BlockingRejected` 返还未执行闭包；CleanupPort 用 `CleanupRejected` 返还未启动 cleanup owner。root admission 必须先预检 driver 可用性并在调用 body 前失败，以 `StartError<F>` 返还原始 `F`；执行器接受后的运行失败成为 operation 结果，不能声称 body 尚未执行。reservation Drop 释放未使用登记槽；attach 成功后控制对象归 registry 所有，commit 是副作用启动唯一许可。取消先被状态机接受时 commit 失败且不得启动；若启动先线性化，registry 负责向已附接 control 发出停止请求。清理端口仅接收已关闭新使用且租约归零的 ready owner。T07/T08/T09 可按这些约束拆分内部接口，不能把普通 spawn 后补登记视为等价协议。

后续公共 API 应保持如下签名形态；此处只是跨任务契约，本提交不提供占位实现：

```rust,ignore
pub fn start<F, Fut, T, E>(&self, options: ScopeOptions, body: F)
    -> Result<OperationHandle<T, E>, StartError<F>>
where
    F: FnOnce(OperationScope) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T, E>> + Send + 'static,
    T: Send + 'static,
    E: std::error::Error + Send + Sync + 'static;

pub fn start_blocking<F, T, E>(
    &self, executor: &dyn BlockingExecutor, options: ScopeOptions, body: F,
) -> Result<OperationHandle<T, E>, StartError<F>>
where
    F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
    T: Send + 'static,
    E: std::error::Error + Send + Sync + 'static;

impl<T, E> OperationHandle<T, E> {
    pub fn request_cancel(&self, reason: CancelReason);
    pub fn observer(&self) -> OperationObserver;
    pub async fn result(&mut self) -> Result<OperationOutcome<T, E>, ObservationError>;
    pub fn result_blocking(&mut self) -> Result<OperationOutcome<T, E>, ObservationError>;
    pub async fn released(&self) -> SettlementReport;
    pub fn released_blocking(&self) -> Result<SettlementReport, ObservationError>;
    pub fn detach(self) -> OperationObserver;
}
```

`result` consumes typed output once; dropping its wait future does not cancel the handle. `released` is repeatable and only resolves for a stable terminal settlement. `released_blocking` can report `WouldDeadlock`. `observe_until` uses its deadline only to bound observation and returns a snapshot plus an observer that can continue observing. `start` is eager, bounded admission; it is not a lazy async function. No API in this crate reports business progress or adapts to an event bus. Business phase messages remain the caller's responsibility.

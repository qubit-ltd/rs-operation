# rs-operation 0.2.0 设计：操作管理、资源租约与收尾协议

- 状态：待评审；本文定义目标接口和行为，不表示这些接口已经实现。
- 日期：2026-11-13。
- Cargo 包：`qubit-operation`；目标版本：`0.2.0`。
- 设计依据：[0.1 设计](design-0.1.0.zh_CN.md)、[0.1 实施契约](implementation-contract-0.1.0.zh_CN.md)、当前公开实现及[中文用户手册](user_guide.zh_CN.md)。三者不一致时，本文明确选择 0.2 的目标行为，不直接继承未核实的承诺。
- 本版允许破坏性变更，不保留旧接口、兼容别名或双套术语。本文只讨论设计；实施方案另行编写。

## 1. 定位与设计目标

`qubit-operation` 管理一次业务操作及其子操作、资源使用和清理过程。业务结果可用之后，操作仍可能占用进程、文件、缓冲区或模型资源，因此结果和收尾必须分别观察。

0.2 保留 0.1 的核心保证：已接纳的操作始终有人负责，取消不等于释放，业务错误与清理错误分别保存。重新设计的重点是让公开 API 直接表达这些保证，并清楚区分应用接口和适配器接口。

调用者应能从代码中理解以下动作：

1. 将操作交给管理器，取得结果接收句柄。
2. 在作用域中管理资源，为每个使用者取得租约。
3. 将子操作纳入同一生命周期，再启动外部工作。
4. 分别等待业务结果与收尾报告。
5. 取消、失败或关闭后，继续持有未释放资源和恢复对象。

这些概念适用于文件检测、外部命令、文件发布和模型推理，不依赖某个具体业务库。

## 2. 设计原则与不变量

### 2.1 用名称表达实际行为

- `try_` 表示立即尝试，不等待容量；失败时归还尚未启动的输入。
- `submit` 表示提交操作，返回句柄；成功不等于主体已开始执行。
- `run` 表示提交并等待业务结果；`run_and_settle` 还等待收尾报告。
- `wait_` 表示等待某个事实；`snapshot` 表示立即读取当前快照。
- `request_stop` 只请求停止，不承诺任务已退出。
- `lease` 表示使用资格，`permit` 表示容量名额，`id` 只表示身份。
- `cleanup` 表示实际清理动作，`settlement` 表示收尾报告，`released` 只用于已确认释放的事实。
- 查询方法使用 `contains`、`is_` 等名称，不用 `keep` 暗示状态变更。

### 2.2 生命周期不变量

1. 每个已接纳的操作和清理对象都有明确持有者；丢弃观察入口不会丢弃后台责任。
2. 停止请求、业务结果和资源释放分别记录，不能相互推导。
3. 尚有有效租约时，不执行会破坏该资源的清理。
4. 提交子操作与请求停止有唯一裁决点；不存在已启动但未纳管的窗口。
5. `Drop` 不执行阻塞等待或任意用户清理动作。
6. 业务结果只能取出一次，业务摘要和收尾报告可以重复观察。
7. 清理失败不覆盖业务错误，停止不抹去已经发生的副作用。
8. 容量不足时拒绝或有界等待，控制通知和清理不依赖普通队列空位。
9. 对释放的确认必须来自实际使用者或清理实现；驱动器丢失不等于释放。
10. 持锁时不 poll 用户 future、不调用领域控制代码，也不析构任意用户值。

## 3. 概念模型与公开层次

### 3.1 核心对象

| 对象 | 职责 | 所有权与 Drop |
| --- | --- | --- |
| `OperationManager` | 管理多个操作的准入、驱动、结果存储和关闭 | Clone 共享管理状态；最后一个公共句柄 Drop 请求停止，不阻塞 |
| `OperationScope` | 组织一次操作的子操作、资源和上下文 | 管理器持有后台状态；业务句柄 Drop 不直接清理资源 |
| `OperationHandle<T, E>` | 接收一次业务结果，请求停止和观察收尾 | 不可 Clone；未结束时 Drop 请求停止并放弃结果 |
| `OperationObserver` | 读取快照并等待收尾 | 可 Clone、无 T/E 泛型；Drop 不停止操作 |
| `ResourceRegistration` | 已交给作用域管理的资源登记 | 可 Clone；Drop 不等于关闭资源或归还使用租约 |
| `ResourceLease` | 表示某个使用者仍在使用资源 | 不可 Clone；Drop 仅归还这份租约 |
| `ResourceCleanup` | 应用实现的资源清理动作 | 登记后交给管理器持有，清理时移入独立 future |
| `RecoveryHandle` | 查询并显式重试一个恢复组 | 可 Clone；不是资源访问权；Drop 不丢弃恢复组 |
| `OperationGroup` | 一组相关操作的准入与关闭边界 | 不占活动根任务名额，不建立永久业务 root |
| `ServiceResourceScope` | 管理跨请求的长期资源 | 使用独立有限配额，由所属组关闭流程管理 |

### 3.2 为什么不采用 ResourceGuard 或 ResourceManager 作为顶层名称

顶层对象同时管理操作提交、业务结果、子操作和资源收尾，`OperationManager` 能覆盖这些职责。`ResourceManager` 只突出资源，`ResourceGuard` 则容易暗示局部 RAII 清理。

资源层使用 `ResourceRegistration` 与 `ResourceLease`，区分登记和实际使用。租约被丢弃后，清理可能仍需等待其他使用者；因此不以 guard 的析构暗示资源已经释放。

### 3.3 模块

| 模块 | 目标公开内容 |
| --- | --- |
| crate 根 | 常用类型重导出：manager、scope、handle、observer、租约、清理 trait、核心结果和配置 |
| `manager` | 准入、组、服务资源和关闭 |
| `operation` | 句柄、观察者、业务结果和快照 |
| `scope` | 作用域、子操作要求和失败策略 |
| `resource` | 登记、租约、依赖、转移、清理结果和恢复 |
| `context` | 停止、时钟域、期限和预算策略 |
| `adapter` | 暂停启动、控制协议、底层资源登记和释放确认端口 |
| `executor` | portable 执行器、阻塞执行器及外部任务驱动协议 |

不再使用一个 `report` 模块收纳所有 ID、配置、错误和结果。每种类型在其所属模块定义，常用类型可在根模块重导出。底层清理调度器、重复 permit 和内部结果容量对象不作为应用入口公开。

## 4. 典型调用：MIME 检测

### 4.1 基本用法

依赖约定：假设本版和相关依赖已发布到 crates.io，依赖写成 `qubit-operation = "^0.2"`。

以下代码展示目标 API，不是当前版本的可运行示例。`StagedInput` 是应用清理实现；这里仍以缓冲区代替文件，以便突出资源关系。

```rust,ignore
use std::sync::Arc;
use qubit_operation::{
    ManagerOptions, OperationManager, OperationOptions, OperationOutcome,
};

let manager = OperationManager::new(ManagerOptions::default())?;
let mut detection = manager.try_submit(OperationOptions::default(), |scope| async move {
    let bytes = Arc::new(b"%PDF-1.7\n".to_vec());
    let input = scope.manage_resource(StagedInput(Arc::clone(&bytes)))?;
    let _lease = input.acquire_lease()?;

    let mime = bytes.starts_with(b"%PDF-").then_some("application/pdf");
    Ok::<_, DetectionError>(mime)
})?;

let outcome = detection.wait_outcome().await;
let settlement = detection.wait_settlement().await;
// 两个结果都取得后，再按业务规则决定响应和恢复。
match outcome? {
    OperationOutcome::Succeeded(mime) => respond(mime),
    other => report_detection_failure(other),
}
if !settlement.all_released() {
    record_settlement(&settlement);
}
```

闭包返回时，局部变量会按 Rust 的规则自动释放：`bytes` 归还检测器持有的缓冲区引用，`_lease` 归还本次使用租约。这里不需要显式调用 `drop`。`_lease` 是有名字的绑定，会持有租约到所在作用域结束；不能写成 `let _ = input.acquire_lease()?`，后者不会保留租约。

局部变量释放与资源清理是两个环节。租约归还后，管理器还需确认新使用已关闭、其他使用者已结束且清理依赖已满足，才会调用 `ResourceCleanup::cleanup`。调用方通过 `wait_settlement` 等待这部分收尾。只有需要在闭包结束之前提前归还租约时，才显式调用 `drop`；如果租约已移交给子操作，则由子操作持有到实际使用结束。

`StagedInput`、`DetectionError` 和响应函数由应用提供。文件及命令适配器将这个例子的文件头检查替换为真实操作，保持租约到真实使用结束后才归还。示例中省略的关闭入口在第 14 节定义。

### 4.2 不可中断推理验证场景

模型适配器在 native 推理开始前完成子操作提交，并持有 feature 租约与模型执行许可。取消后可以停止后续工作和结果交付，但不能提前销毁输入或归还执行许可。只有 native 调用退出且输入 helper 结束后，控制对象才能确认释放。

这个场景用于验证公共接口没有与 MIME、文件路径或进程特性绑定。

## 5. 操作提交与执行

### 5.1 管理器配置

```rust,ignore
pub struct ManagerOptions {
    pub limits: ManagerLimits,
    pub scope_limits: ScopeLimits,
    pub executor: PortableExecutorOptions,
    pub clock: Option<Arc<dyn ClockSource>>,
}

pub struct OperationOptions {
    pub limits: ScopeLimits,
    pub child_failure_policy: ChildFailurePolicy,
    pub context: ContextOptions,
}
```

`OperationManager::new(options)` 返回 `Result<Self, ManagerCreateError>`，显式报告零容量、不合理限制、线程创建失败等问题。构造失败不得留下无法由调用者关闭的后台线程。没有全局默认实例。

manager 配置规定每 root 的上限；`OperationOptions.limits` 只能申请上限内的额度，超出时拒绝。子作用域从同一 root 的有限额度中切分，不为每个后代重新创建一套完整额度。

### 5.2 异步主体的接口

```rust,ignore
impl OperationManager {
    pub fn try_submit<F, Fut, T, E>(
        &self, options: OperationOptions, body: F,
    ) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub async fn submit<F, Fut, T, E>(
        &self, options: OperationOptions, body: F,
    ) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub fn blocking(&self) -> BlockingManager<'_>;
}
```

- `try_submit` 立即尝试接纳，可接纳进入有限排队区。返回成功前完成容量预留及后台责任交接。
- `submit` 等待准入，成功后立即返回句柄，不等待业务结果。等待者数量有界，且计入业务总期限。
- async 方法未被 poll 时没有提交副作用。准入前丢弃等待 future 会退出队列并释放预留；捕获值按非阻塞析构约定处理。
- 准入完成与丢弃等待 future 的竞争必须唯一裁决。已接纳工作若未交付句柄，执行 handle Drop 语义，不得成为无主任务。
- `SubmitError<F>` 包含 `reason()` 和 `into_parts()`，原样归还未调用的 F。返回成功之后发生的执行故障进入 `OperationOutcome::ExecutionFailed`，不能再声称主体未启动。

### 5.3 分开表达工作类型和等待方式

阻塞任务的执行器通过选择入口指定，调用线程是否阻塞则通过 `.blocking()` 指定，避免同一个 `_blocking` 后缀表达两件不同的事。

```rust,ignore
// portable future，立即尝试准入。
manager.try_submit(options, async_body)?;
// portable future，异步等待准入。
manager.submit(options, async_body).await?;
// portable future，在当前线程等待准入。
manager.blocking().submit(options, async_body)?;

// 同步闭包，由指定阻塞执行器执行；立即尝试准入。
manager.on_blocking(&executor).try_submit(options, sync_body)?;
// 同步闭包，异步等待准入。
manager.on_blocking(&executor).submit(options, sync_body).await?;
// 同步闭包，在当前线程等待准入。
manager.on_blocking(&executor).blocking().submit(options, sync_body)?;
```

`on_blocking` 返回借用 manager 与 executor 的 `BlockingTaskSubmitter<'_, X>`，它只选择执行方式。借用不进入后台主体；F、T、E 仍要求 `Send + 'static`。

阻塞执行器先取得可回滚的提交许可，管理器再完成 root 与结果槽接纳；任一步失败都归还临时许可和原始 F。等待阶段不得持有部分 root 容量或领域许可，避免相互等待。等待由可用性通知唤醒，不忙重试。

本版阻塞任务不进入 manager 的 root 排队区：须同时取得活动 root、结果槽和 executor 许可后才成功接纳；executor 自身可以有有界队列。`.submit` 等待这些条件，`.try_submit` 则立即失败。这一差异是准入契约，不隐藏在名字中。

### 5.4 运行便利接口

`run(options, body)` 提交并返回业务 outcome；`run_and_settle(options, body)` 返回 `OperationCompletion<T, E> { outcome, settlement }`。异步入口在首次 poll 时提交，阻塞入口位于 `.blocking()`；阻塞任务入口沿用 `.on_blocking(...)`。

`run` 不保证资源已完成收尾。`run_and_settle` 的输出不得携带依赖本 scope 存活的租约，否则会等待自身输出释放。便利调用的 future Drop 会丢弃内部 handle，按相同协议请求停止。

## 6. 句柄、观察者和结果

### 6.1 目标接口

```rust,ignore
impl<T: Send + 'static, E: Send + 'static> OperationHandle<T, E> {
    pub fn id(&self) -> OperationId;
    pub fn request_stop(&self, reason: StopReason) -> StopRequestStatus;
    pub fn observer(&self) -> OperationObserver;
    pub fn snapshot(&self) -> OperationSnapshot;
    pub async fn wait_outcome(&mut self)
        -> Result<OperationOutcome<T, E>, OutcomeError>;
    pub fn try_take_outcome(&mut self)
        -> Result<Option<OperationOutcome<T, E>>, OutcomeError>;
    pub async fn wait_settlement(&self) -> Arc<SettlementReport>;
    pub fn blocking(&mut self) -> BlockingOperation<'_, T, E>;
    pub fn into_observer(self) -> OperationObserver;
}

impl OperationObserver {
    pub fn id(&self) -> OperationId;
    pub fn snapshot(&self) -> OperationSnapshot;
    pub async fn wait_settlement(&self) -> Arc<SettlementReport>;
    pub async fn wait_settlement_until(&self, deadline: Deadline)
        -> Result<SettlementWait, ObservationError>;
    pub fn blocking(&self) -> BlockingObserver<'_>;
}
```

观察者不包含 T/E，也没有业务结果接收权或请求停止权。应用需要停止时保留 handle 或经授权的 `OperationContext`，不把只读观察能力变成控制能力。

`BlockingOperation` 提供同步的 `wait_outcome`、`wait_settlement`；`BlockingObserver` 提供同步收尾等待。阻塞等待均返回可能包含 `WouldDeadlock` 的错误，不能强求与无该检测错误的 async 返回形态相同。阻塞带期限等待使用 `Duration` 作为本次等待预算；async 入口使用带时钟域的 `Deadline`。

### 6.2 结果交付与执行结果分开

业务 outcome 不包含“结果已被取走”这种观察状态：

```rust,ignore
pub enum OperationOutcome<T, E> {
    Succeeded(T),
    Failed(E),
    ChildFailed(ChildFailureSummary),
    Stopped(StopReport),
    Panicked(PanicSummary),
    ExecutionFailed(ExecutionFailure),
}

pub enum OutcomeAvailability { Pending, Available, Taken, Discarded }
pub enum ExecutionPhase { Queued, Running, Finished }
```

`ExecutionFailed` 表达已接纳之后的执行基础设施故障，与 E 的领域失败、panic 和人为停止分开。`ExecutionPhase` 只描述该操作主体；`Finished` 表示主体不再执行，不证明子操作或资源已释放。

`wait_outcome` 取结果一次；后续返回 `OutcomeError::AlreadyTaken`。等待 future 被丢弃不丢结果，handle 可再次等待。`try_take_outcome` 未就绪返回 `Ok(None)`，已取走则报错，不能把两者混为 None。

成功 outcome 只有在主体成功且全部必要子操作的业务结果已成功确定后发布。主体失败、停止、panic 或执行故障可先发布，后续子操作信息进入收尾报告。先发布的 outcome 不回写；结果发布与停止接受在同一状态单元裁决，副作用事实独立保留。

### 6.3 放弃结果和 Drop

`into_observer` 消费 handle，放弃未取业务结果，返回只读 observer；操作继续被 manager 管理，也仍受服务关闭约束。它本身不请求停止。转换释放结果槽，并记录 Discarded，不能伪装成业务已消费。

handle 未完成时 Drop 请求停止并放弃结果，不等待、不直接销毁正在使用的资源。observer Drop 只丢弃观察入口。最后一个公共 manager Drop 请求 Cancel，后台状态继续由活动责任持有；不会在 Drop 中 join worker。

## 7. 停止、上下文、时钟与预算

只保留 `StopReason`、`request_stop`、`Stopped` 一套术语。`UserCancelled` 是原因之一，其他原因为 Timeout、ParentStopped、ManagerShutdown、RequiredChildFailed。第一个已接受原因是主因，后续原因只进入有界诊断。

`StopRequestStatus` 区分 Accepted、AlreadyRequested、AlreadyFinished，只表示请求的裁决，不表示实际停止完成。

`OperationContext` 提供 `request_stop`、`stop_reason`、`stop_requested().await`、`deadline` 和子上下文派生。`stop_requested` 只等待请求被接受，任务实际退出由控制协议另行确认。

`OperationContext::new` 和 `child` 都返回 `Result`，在创建时验证时钟与 deadline，避免根上下文延迟到执行时才发现配置无效。子级继承并收紧期限与预算，不能扩张；显式子预算与父预算同时检查。

`Deadline`、`ClockSource`、`ClockDomainId` 和 `MonotonicTime` 保留。timer 注册必须覆盖“检查时未到期、注册期间已到期”的竞争，并在到期时唤醒。每个注册独立撤销，不能误取消复用槽上的新定时器。

`BudgetPolicy` 取代只提供检查却称为租约的概念，接口为 `fn check(&self) -> Result<(), BudgetError>`。这是对调用方预算策略的引用，不授予或归还容量；领域真正的模型、字节和执行许可由领域实现持有。

清理使用独立 `CleanupContext`，不继承已取消的业务 token 或已过期的业务 deadline。清理等待预算到期只产生观察快照，不丢弃正在使用的资源。

### 7.1 作用域的上下文访问

`OperationScope::id()` 返回操作 ID，`context()` 返回该作用域的共享 `OperationContext`，`snapshot()` 返回当前 `OperationSnapshot`，`request_stop(reason)` 委托同一上下文请求停止。克隆 context 不创建第二套停止状态。

上下文权限跟随派生关系。子上下文能停止其子树，不能直接修改祖先或兄弟的停止状态；父级是否停止兄弟由失败策略决定。时钟和预算是应用注入的策略接口，不在 context 中放入文件、进程或消息总线句柄。

## 8. 资源管理与清理

### 8.1 高层接口

```rust,ignore
impl OperationScope {
    pub fn manage_resource<C: ResourceCleanup>(&self, cleanup: C)
        -> Result<ResourceRegistration, ManageResourceError<C>>;
    pub fn reserve_resource(&self) -> Result<ResourceReservation, ResourceCapacityError>;
}

impl ResourceReservation {
    pub fn commit<C: ResourceCleanup>(self, cleanup: C)
        -> Result<ResourceRegistration, ResourceCommitError<C>>;
    pub fn retain_for_cleanup<C: ResourceCleanup>(self, cleanup: C)
        -> ResourceRegistration;
}

impl ResourceRegistration {
    pub fn id(&self) -> ResourceId;
    pub fn acquire_lease(&self) -> Result<ResourceLease, ResourceAccessError>;
    pub fn close_for_use(&self) -> Result<(), ResourceAccessError>;
    pub fn cleanup_after(&self, prerequisite: &ResourceRegistration)
        -> Result<(), CleanupDependencyError>;
    pub fn transfer_to(&self, destination: &OperationScope)
        -> Result<ResourceTransfer, ResourceTransferError>;
}
```

`manage_resource` 接管的是资源的清理对象。返回的 registration 用于控制登记，不直接暴露原生资源。应用通过自己的文件或缓冲区访问对象执行实际工作，并在访问期间持有 lease。

如果创建资源可能产生需清理的部分状态，先 `reserve_resource`，预留登记、控制和失败后保存清理对象的容量，再创建资源并 commit。

停止若在预留后到达，commit 返回 `ResourceCommitError<C>`。其 `into_parts()` 返回原因、原 C 和仍有效的 `ResourceReservation`。应用随后调用 `reservation.retain_for_cleanup(cleanup)`，把对象交给管理器清理。该入口使用已预留容量，不因业务已关闭或已停止而拒绝，返回处于关闭使用状态的登记，不授予新租约。创建部分失败时也走这一入口。

`retain_for_cleanup` 只保证接管责任，实际清理仍可能失败或未确认。仅在尚未产生任何资源时，reservation 才可以直接 Drop 归还名额；临时对象中的普通用户 Drop 仍须非阻塞。

`ManageResourceError<C>` 原样归还 C，不能只返回字符串。便利 `manage_resource` 适用于已有清理退路的对象；无法安全 Drop 的 owner 必须走预留路径及适配器拒绝处理。预留不会令任意用户闭包成为不可取消的事务。

### 8.2 租约与关闭

`ResourceRegistration` 可 Clone，复制登记句柄不增加使用次数；`ResourceLease` 不可 Clone，必须为每条独立使用路径分别获取。归还 lease 只结束这份使用，不证明未登记的裸句柄已关闭。

`close_for_use` 幂等地拒绝新租约，不使已有租约失效，也不直接执行清理。清理启动条件同时包括：新使用已关闭、租约为零、全部清理前置条件满足、清理执行容量已取得。

正常情况下，主体返回后关闭该主体的新子操作登记，已提交子操作可在原授权内继续使用及展开已授权子 scope。整个使用阶段结束后关闭新租约。失败或停止时关闭新登记和新租约，现有使用者仍持有租约直到实际退出。

共享资源由共同父 scope 或服务资源 scope 持有。不同 root 不能直接创建互相等待的清理边；服务资源通过受控租约跨请求使用。

### 8.3 清理 trait

```rust,ignore
pub trait ResourceCleanup: Send + 'static {
    fn cleanup(self: Box<Self>, context: CleanupContext)
        -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>;
}

pub enum CleanupOutcome {
    Released,
    Transferred(TransferReceipt),
    Failed { failure: CleanupFailure, retry: Box<dyn ResourceCleanup> },
}
```

trait 名称表达“资源清理”，方法 `cleanup` 表达动作，self 接管约束由签名说明。Released 只能在实际确认释放后返回。Failed 必须同时交回可继续清理的对象，不允许仅交回错误而丢失资源。

公开 trait 不返回一个外部无法合法构造的 `RecoveryOwner`。Failed 中的重试对象由框架转入恢复组，再向应用暴露恢复句柄。它可以是拥有原生资源的对象，也可以是能在原线程上操作资源的控制对象。

清理 panic 时不能保证恢复被移入 future 的唯一对象。需要 panic 后恢复的适配器必须用独立 capsule/注册表保留真实 owner，future 只持控制引用。未能保留可恢复对象时记录 Unconfirmed，不能伪造 Failed-with-owner 或 Released。

### 8.4 依赖与转移

`directory.cleanup_after(&file)` 明确文件先于目录。设置时检查身份、作用域边界、自环和循环；失败不改变原图。互不依赖的清理可有限并发，同等条件下按资源 ID 确定启动顺序。

前置失败时，后继清理不得越过依赖启动。框架将阻塞资源及其必要前置组成恢复组，保留所有重试对象和边。单独取走某个路径不解除后继依赖。

`transfer_to` 一次转移完整依赖组，先验证目标接纳和容量，再原子改变归属；失败时源登记不变。`ResourceTransfer` 返回 `receipt`、对应目标登记和成员 ID 映射，不以一个 ResourceId 假装完整组转交。源 registration 转为已转交状态，拒绝后续新租约；已有 lease 关联的实际使用持续计入目标组。

成功转交表示责任已被接管，不表示物理释放。回执只能由接收方接纳协议签发，字段不可由应用任意构造。

## 9. 收尾报告、未确认与恢复

### 9.1 等待契约

`wait_settlement` 等待本次自动收尾得出可报告的稳定结果：已确认释放、已被合格接收方接管，或恢复组已被管理器可靠保留。它不承诺每个资源都已物理释放。

如果仍有不可中断使用者、驱动器丢失且不能确认使用结束，或清理正在运行而未返回，则报告尚未完成，等待继续 Pending。`wait_settlement_until` 到期返回 Pending 快照，不把 Unconfirmed 当作稳定成功或放弃 owner。

```rust,ignore
pub enum SettlementWait {
    Ready(Arc<SettlementReport>),
    Pending(OperationSnapshot),
}

pub enum SettlementStatus { Released, Transferred, RecoveryRequired }

pub struct SettlementReport {
    pub operation_id: OperationId,
    pub status: SettlementStatus,
    pub released_count: usize,
    pub transferred_count: usize,
    pub recovery_count: usize,
    pub children: Arc<[ChildSettlement]>,
    pub cleanup_failures: Arc<[CleanupFailure]>,
    pub transfers: Arc<[TransferReceipt]>,
    pub recoveries: Arc<[RecoveryHandle]>,
    pub effects: Arc<[EffectRecord]>,
    pub effect_overflow: usize,
}
```

`all_released()` 只在全部责任已确认释放时为 true；包含转交或恢复均为 false。`status` 聚合优先级为 RecoveryRequired、Transferred、Released，但明细与计数始终保留。转交成功后源 scope 可以完成自动收尾，应用若需物理释放证据，应继续观察目标登记或回执的接收方。

已发布报告是不可变历史事实。报告中的 recovery handle 可查询后续恢复，但旧 report 的 status 和计数不回写，不能将历史失败抹除。

### 9.2 当前快照

```rust,ignore
pub enum ResourceProgress { Active, Closing, Cleaning, Unconfirmed }
pub struct OperationSnapshot {
    pub operation_id: OperationId,
    pub execution: ExecutionPhase,
    pub outcome: OutcomeAvailability,
    pub stop: Option<StopReport>,
    pub resources: Option<ResourceProgress>,
    pub settlement: Option<Arc<SettlementReport>>,
}
```

有 settlement 时 resources 为 None，以报告解释自动收尾结果；无 settlement 时 resources 为 Some，描述当前资源进展。Unconfirmed 表示缺少释放证据，后续可在证据恢复后继续推进。观察超时既不改变这份状态，也不修改业务 deadline。

### 9.3 恢复接口

```rust,ignore
impl RecoveryHandle {
    pub fn id(&self) -> RecoveryId;
    pub fn snapshot(&self) -> RecoverySnapshot;
    pub fn try_retry(&self) -> Result<RecoveryAttempt, RetryError>;
    pub fn transfer_to(&self, destination: &RecoveryStore)
        -> Result<RecoveryTransfer, RecoveryTransferError>;
}
```

`try_retry` 在 manager 中提交一个有界恢复组重试，按原依赖与执行器归属清理。只有清理容量、运行环境及组状态允许时才接纳。`RecoveryAttempt` 提供 async 与 blocking 结果观察，Drop 只放弃本次观察，不丢恢复对象，也不重复启动清理。

同组并发重试只有一个成功接纳，其余得到 AlreadyRunning；重复调用不能并行删除同一资源。成功后更新 RecoverySnapshot 为 Resolved，失败则保留新的重试对象。未确认仍有使用者的资源不进入可重试组，避免通过重试销毁仍在使用的对象。

高层不提供“取走任意单个 owner”的接口。原生接管需要把完整依赖组原子转入有界 `RecoveryStore`，取得回执后才解除源 manager 的责任。RecoveryStore 是独立、显式持有的进程内接收方；接收失败归属不变。其未解决组在最后一个公共句柄 Drop 后也不能静默析构：内部责任持有者继续保留，并向已注册诊断入口报告未关闭状态。

`RecoveryStore::new(options)` 返回 `Result<RecoveryStore, RecoveryStoreCreateError>`，接收方具有固定资源/组容量和清理执行环境。`contains(id)` 查询登记，`snapshot()` 返回待处理组计数，`close_admission()` 停止接收转交，`request_shutdown()` 返回可继续观察的关闭句柄。关闭接纳后仍可处理已有组，但未解决组会阻止关闭完成。store 不提供丢弃仍在使用的资源的 force-close 接口。转入 store 的 RecoveryHandle 查询和重试都路由到新持有者，原 handle 不失效或自行接管原生对象。

底层适配器若需取回具体类型，使用受限的 `RecoveryKey<R>` 与 typed payload 接口。key 验证注册表、resource generation 和 nonce，取回失败归还 key，owner 不变；成功接管必须同时建立新的责任持有者。不能从历史报告或只读 observer downcast 资源。

manager 内恢复对象继续占资源配额，并阻止关闭完成；已可靠转入独立 RecoveryStore 的组只记转交历史，不再阻止源 manager 关闭。进程退出后的恢复需要应用持久化信息，不由这些内存对象保证。

## 10. 子操作与适配器协议

### 10.1 策略和范围

`ChildRequirement::{Required, Optional}` 保留。失败策略使用 `ChildFailurePolicy::{StopSiblings, ContinueSiblings}`，明确描述动作，不使用容易暗示收集 typed result 的 Collect。

策略只控制必要子操作失败后的兄弟处理。必要失败始终禁止父级成功。可选失败保留在报告中，父主体自行读取领域结果并决定降级。停止请求不触发 fallback；已经取消的流程不能为“恢复成功”启动新后端。

`OperationScope::child_scope(options)` 创建逻辑子作用域，不隐式执行任务。`wait_settlement()` 替代意义过宽的 join，等待本 scope 的子操作和资源报告。完整操作的 outcome 仍由相应 OperationHandle 接收，不在 scope 上再次提供一份 T。

### 10.2 控制与两阶段启动

```rust,ignore
pub trait OperationControl: Send + Sync {
    fn request_stop(&self, reason: StopReason);
    fn snapshot(&self) -> ChildSnapshot;
    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary>;
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease>;
    fn poll_settlement(&self, cx: &mut Context<'_>)
        -> Poll<Arc<SettlementReport>>;
}
```

控制协议区分三种事实：`poll_outcome` 是业务摘要，`poll_release` 表示不再使用调用方所附带的资源，`poll_settlement` 表示子操作自身的资源收尾报告。`ChildRelease` 包含 child ID 和停止使用的确认，不包含尚未证实的资源释放成功声明。

框架在 `poll_release` 确认后归还附接的输入租约和领域使用许可，但仍保留子操作登记，直到取得其 settlement。父作用域汇总子报告中的恢复和转交，不能在仅收到输入释放确认时就宣告子操作全部收尾。无额外自有资源的适配器可以在同一时刻发布 release 和已释放的 settlement，但必须分别响应这两个观察入口。

三个 poll 的 Pending 均须登记唤醒，Ready 均须为稳定事实，不能因领域 typed result 已被取走而丢失。runtime 丢失而无法确认输入已停止使用时，`poll_release` 不能 Ready，租约继续保留。

适配器入口位于 `adapter`：`reserve_child(requirement)` → `reservation.attach(control, leases)` → `reservation.commit()`。attach 原子接收 control 与 leases，重复 attach 错误必须归还传入值。commit 返回 `ChildStartPermit`，表明登记已生效，而不是仅返回 `()` 后依靠调用者记住顺序。

`ChildStartPermit` 不可 Clone，适配器必须消费它来激活暂停操作；它是启动凭证，不是线程 spawn 接口。Drop 未消费凭证时，已登记控制对象进入“未启动即终止”协议，发布 Stopped/释放事实并归还资源，不留下永远 Pending 的子操作。暂停适配器必须同时支持 activate 和 abandon 两条路径，这两条路径均非阻塞且至多执行一次。

commit 与停止请求串行裁决：停止先被接受则不返回启动凭证，未启动输入归还；commit 先成功则 control 已归管理器负责，随后停止可在激活前或执行中到达。激活端检查该停止状态，不能让后到取消造成未登记工作。

领域执行器许可在 commit 前准备，失败可以回滚。已运行的旧接口不能靠补登记获得相同保证；0.2 不为其提供“严格受管”标记或隐式降级路径。

## 11. 执行模型

### 11.1 PortableExecutor

`PortableExecutor` 推进不依赖特定运行时 reactor 的 owned、Send future。工作线程数、任务容量、每轮 poll 预算都显式有限。`spawn` 返回 `TaskId`，它只是身份，不提供结果、取消或生命周期所有权；高层受管工作通过 OperationManager 提交。

协调器与 portable worker 仅推进非阻塞 future。不得在持锁期间执行用户代码或释放用户 T/E。用户 poll 和普通 Drop 仍须及时返回，框架不能抢占任意阻塞实现。

### 11.2 阻塞执行器

`BlockingExecutor` 提供有界、可回滚的提交预留与激活协议，许可类型统一为 `ExecutorPermit`。只有提交已被接纳之后，执行器才能调用同步闭包。拒绝时归还未执行闭包。

内建 `ThreadPoolExecutor` 使用固定线程和有限队列，避免为每个操作建线程。显式 `close`/`shutdown` 返回线程退出状态；析构不 join 本线程，不在管理线程等待阻塞任务。需要等待线程退出的动作必须由应用显式调用。实现必须保证 executor 公共句柄消失后，已接纳闭包仍有明确持有者。

关闭 manager 后，再关闭相关 executor；取消同步闭包不等于强制退出。已有领域池直接实现该协议，不再套线程池。

### 11.3 外部执行环境

外部任务使用 `ExternalTaskDriver` 提供非阻塞 wake 与存活查询，`DriverLease` 记录 Active、Released、Lost。一次性状态转换保持共享原子状态，不能从 Lost 再改为 Released；后续领域释放证据走独立恢复确认协议。

`ExternalDriverGuard` 的职责是持有外部执行责任，命名为 `ExternalTaskGuard`；未确认释放而 Drop 时报告 Lost，不执行阻塞释放。它与 ResourceLease 不同，不能相互替代。

运行时绑定的 future 始终在原运行时执行，线程绑定 session 的创建、调用和销毁保持线程归属。外部运行时提前丢失时，保留 Unconfirmed 与恢复所需状态，不在别的线程继续 poll 同一对象。正式运行时适配器仍需独立领域验收。

## 12. 容量、背压和诊断

默认基线保留有限值，但字段只描述可见对象：活动根操作 4、排队根操作 64、准入等待者 64、结果槽 64、清理并发 4、诊断事件 256；每 root 子操作 64、资源 128、副作用记录 64。

结果槽在接纳之前预留，结果发布不能再等新槽。取走或放弃结果归还槽；资源尚未释放时仍保留对应执行/资源责任。队列容量与结果槽不能简单相加为总接纳保证。

子作用域从 root 配额切分，服务资源及恢复存储独立有界。恢复组占用原资源槽或已接纳目标的容量，不能以移动到无界容器绕过背压。

manager 的统计使用 `ManagerSnapshot` 一次返回各类计数，避免 `cleanup_active`、`cleanup_tasks_active` 等含义接近的散落 getter。字段明确区分结果未取、正在清理、待恢复和未确认使用者。

取消、提交裁决、释放通知和清理唤醒通过预留控制状态与专用通知推进，不把控制消息作为普通作业塞进已满队列。需要领域 worker 的清理还必须有领域可用通道；manager 清理槽不等于领域执行许可。

诊断仅保存有界 ID、阶段、摘要和溢出计数，不默认记录路径、命令或模型内容。业务结果、重试对象和恢复 key 不通过可丢失的诊断队列交付。`EffectRecord` 保留应用声明的 Known/Unknown 副作用及来源，框架不替应用验证外部状态。

## 13. 操作组和长期资源

`OperationGroup` 是应用定义的操作归属与准入范围，不以 Service 前缀限制用途。`manager.group(GroupOptions)` 创建组，其 `try_submit`、`submit`、blocking 和 on_blocking 入口与 manager 语义一致。

组中每次新请求占独立 root；已有请求内的领域调用作为该请求的 child，不重复创建 root。组自身不占活动 root，也不把所有请求挂进永久父 scope。

`group.resource_scope(ServiceResourceOptions)` 创建长期资源作用域。模型 session、provider 等由它管理，操作持有被授权的资源租约。服务关闭先停止新授权，等请求停止使用，再清理长期资源。

低层组准入返回 `GroupPermit`，不称为 Request。高层组提交直接返回相同 `OperationHandle<T, E>`，组许可由后台状态持有至本组责任完成，避免增加只转发方法的第二种业务句柄。

组关闭只停止本组接纳及受其控制的工作，不直接停止同一父请求的其他服务。必要子失败仍可经父 scope 策略传播。manager 关闭覆盖全部组与已放弃结果的操作。

## 14. 关闭协议

```rust,ignore
impl OperationManager {
    pub fn close_admission(&self);
    pub fn request_shutdown(&self, mode: ShutdownMode) -> ShutdownHandle;
}

pub enum ShutdownMode { Drain, Cancel }

impl ShutdownHandle {
    pub fn snapshot(&self) -> ShutdownSnapshot;
    pub async fn wait(&self) -> ShutdownReport;
    pub async fn wait_until(&self, deadline: Deadline)
        -> Result<ShutdownWait, ObservationError>;
    pub fn blocking(&self) -> BlockingShutdown<'_>;
}
```

请求关闭和等待关闭分开，关闭开始时即取得可保存的 handle。`ShutdownHandle` 可 Clone，Drop 不撤销关闭。`BlockingShutdown::wait_for(Duration)` 返回 Completed 或 Pending 快照；async 超时返回同样含义的枚举。使用同一状态机，不维护两套关闭进度。

Drain 关闭新 root 准入，已接纳操作仍能在预留范围内登记完成所需子工作。Cancel 进一步关闭新子操作提交并传播停止。Drain 可升级到 Cancel，不可恢复接纳或反向降级。

关闭快照分别记录活动/排队操作、未确认使用者、正在清理、恢复组和服务资源。未取业务结果不阻止资源关闭完成；它仍占结果槽并可由持有 handle 的调用方取出。独立 RecoveryStore 已签收的组不再占源 manager 的待恢复数。

`wait` 只在 manager 不再持有活动或未解决责任时完成。恢复组虽能产生 operation settlement，仍会阻止 manager 关闭，直到恢复成功或被合格接收方接管。超时只限制本次观察，manager 不强杀 native 调用，也不丢弃 owner。

后台退出依据内部责任计数，不以自持 Arc 构成永久环。已确认无责任且关闭后可以退出；原生调用永久挂起或释放无法确认时允许持续 Pending，并提供诊断，不能自动报告成功。

## 15. 错误与命名约束

| 类型 | 所属阶段与契约 |
| --- | --- |
| `ManagerCreateError` | 构造前验证和 worker 创建失败 |
| `SubmitError<F>` | 尚未启动时拒绝，保留 F |
| `AdmissionError` | Busy、Closed、DeadlineExpired、CapacityExceeded、ExecutorUnavailable 等准入原因 |
| `OutcomeError` | AlreadyTaken、Discarded 等结果接收错误 |
| `ObservationError` | 观察期限配置错误或阻塞死锁风险，不放入业务 E |
| `ManageResourceError<C>` | 保留便利登记失败的具体清理对象 C |
| `ResourceCommitError<C>` | 保留 C 和原 reservation，可用预留入口接管清理 |
| `ResourceAccessError` | 未登记、已关闭或已转交，不能继续取得租约 |
| `CleanupDependencyError` | 无效依赖、跨边界或循环 |
| `ResourceTransferError` | 目标拒绝、容量不足或资源组状态不允许；源保持不变 |
| `RetryError` | 恢复执行环境不可用、组已在运行、无容量等，owner 不变 |
| `ExecutionFailure` | 接纳之后的驱动故障，不冒充可取回未启动 F |

错误里保留的 F、C、许可和恢复对象都使用 `#[must_use]`。领域 E 不要求 Clone 或统一为字符串，子错误以有界摘要进入父结果，不强转为父 E。panic 摘要不泄露任意 payload，只对 unwind 处理，不承诺从 abort 或 native 崩溃恢复。

Release、settlement、outcome 和 shutdown 在类型及文档中保持同一含义。名称中不再使用没有实际租约生命周期的 BudgetLease、只有编号的 TaskHandle，或只执行查询的 keep。内部需要容量对象时统一按用途命名，不向应用导出两个不同的 CleanupPermit。

## 16. 下游对接约束

| 组件 | 需要满足的协议 |
| --- | --- |
| rs-command | 先纳管再启动；进程和输入 helpers 结束前保留租约；进程退出与回收分别确认 |
| rs-fs / rs-local-files | 资源创建失败也保留部分清理对象；facade 透传同一登记；发布事实独立记录 |
| rs-mime | 在同一总期限下组合后端；可选细化使用 Optional；停止后不启动 fallback |
| rs-magika | 保持 Session 线程归属；不可中断推理退出前保留 features 和模型许可 |

应用注入共享 manager；领域服务管理自己的执行器和组，不关闭应用共享 manager。跨库包装不重复登记一个 owner，不因路径或业务 ID 相同而自行确认释放。

本版不加入业务进度、事件总线或补偿 DSL。应用的进度消息仍由业务层发送，消息回执不代替操作释放证据。

## 17. 验收标准

| 验证面 | 必须证明的行为 |
| --- | --- |
| 公共命名 | 应用代码能区分提交、等待、使用、清理与确认；不存在保留的旧别名 |
| 编译用例 | manager / group / blocking facade 的完整签名可组合，T/E 不要求 Clone；observer 无 T/E |
| 接纳与等待取消 | 未 poll 不提交，未接纳 Drop 归还槽；接纳竞争后无主任务数为零 |
| 阻塞准入 | root、结果槽和 executor 许可均满足才接纳；等待不占部分许可形成循环 |
| 结果读取 | 一次取走、重复错误、取消等待后仍可重试读取 |
| 结果与收尾 | outcome 可先到，租约未归还时不清理；两套报告不相互覆盖 |
| 停止竞争 | 主因唯一且稳定；成功或副作用事实不被后到停止抹去 |
| 子操作启动 | commit / stop / permit Drop / activate 交错均无未登记工作或永久未启动 Pending |
| 多使用者 | 最后一个使用者实际结束后才清理，registration Clone 不冒充租约 |
| 资源预留 | 容量不足不创建资源；创建部分失败、取消后提交失败仍有清理去向 |
| 清理依赖 | 自环、跨 root、环路被拒绝；前置失败后完整恢复组保留 |
| 转交 | 全组原子接纳及 ID 映射；失败不改变源，旧登记不产生新租约 |
| 恢复 | 并发只启动一次，失败保留新 owner；历史 report 不回写；组外接管有回执 |
| 未确认 | driver 丢失不报告 Released；无期限等待继续 Pending，有期限返回可继续观察的快照 |
| Drop | handle 请求停止，observer 不停止，into_observer 继续运行；普通 Drop 不 join 或执行清理 |
| 关闭 | Drain 允许已接纳子工作；Cancel 阻止新 commit；超时不丢 owner；恢复阻止关闭直至解决或转交 |
| 同步异步 | 同一操作同一状态，无重复执行；库 worker 阻塞等待返回 WouldDeadlock |
| 线程与运行时 | 绑定对象不迁移到错误线程，清理环境丢失仍保留责任 |
| 饱和与通知 | 满队列仍能停止及清理；状态发布、waker 注册与撤销无丢唤醒 |
| 隐私与有界性 | 诊断溢出可见，不丢 typed 结果和 owner；不默认记录敏感内容 |

测试使用闸门、可控时钟、故障注入、释放计数和执行许可计数。不能用固定 sleep 证明竞态。设计验收还需对 MIME 检测与不可中断推理两个场景检查：所有租约都有实际归还点，所有失败路径都有 owner 持有者，所有成功释放都有领域证据。

本文经确认后，再编写独立的实施文档；当前文档不授权执行代码重构或下游改动。

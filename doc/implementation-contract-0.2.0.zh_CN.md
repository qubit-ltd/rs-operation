# rs-operation 0.2.0 实施契约（冻结候选）

> 本文把 `design-0.2.0.zh_CN.md` 和 `developer_guide-0.2.0.zh_CN.md` 已确定的契约整理为实施门禁。`STABLE` 表示源规范已决定，或由已接受规则推出且限于 crate-private 实现接缝；本文不扩大 crate 根公开 API。

## 状态与验证约定

本任务不运行代码测试。每个条目的“文档审查检查”是本交付的 RED/FIRST_CHECK/GREEN 替代：对照设计 §8 的签名、设计正文和手册场景，检查签名一致、所有权责任可追踪、示例可表达。实现模块仍需主控安排编译契约和行为验证。

| 阶段 | 本文证据 |
| --- | --- |
| RED | 尚无统一契约文件；逐项列出 C01—C10 缺口与规范不变量 |
| FIRST_CHECK | 对照设计 §8.1—§8.8 和手册的提交、资源、恢复、关闭示例 |
| GREEN | 本文逐项记录稳定内容和具体验证场景；父绑定 group 语义已由主控选定 |

## 1. 稳定公共契约摘录（C01）

以下签名以设计 §8 为准；字段可见性、默认值和 bounds 不能由模块自行改变。完整公共根重导出由主控在实现接线时统一核对。设计 §8.1—§8.8 的完整公共清单也涵盖 ManagerCreate/Submit/Run/Outcome/Observation 错误、sync/async handles 与 observers、operation/resource/recovery/group/shutdown snapshots/reports、resource reservation/errors、RecoveryStore/attempt、groups/service scopes、contexts/clocks/budgets、IDs 和 transfer receipts；未在本段重复的签名仍以对应 §8 条目逐字为准，不得另行设计。

### 管理器与提交

```rust,ignore
pub struct ManagerOptions {
    pub limits: ManagerLimits,
    pub scope_limits: ScopeLimits,
    pub async_executor: PortableExecutorOptions,
    pub sync_executor: ThreadPoolExecutorOptions,
    pub cleanup_executor: ThreadPoolExecutorOptions,
    pub clock: Option<Arc<dyn ClockSource>>,
}
pub struct OperationOptions {
    pub requirement: ChildRequirement,
    pub limits: ScopeLimits,
    pub child_failure_policy: ChildFailurePolicy,
    pub context: ContextOptions,
}
pub struct ManagerLimits {
    pub active_operations: usize, pub queued_operations: usize,
    pub admission_waiters: usize, pub unconsumed_outcomes: usize,
    pub cleanup_concurrency: usize, pub diagnostic_events: usize,
}
pub struct ScopeLimits {
    pub child_operations: usize, pub resources: usize, pub effect_records: usize,
}
pub enum ChildFailurePolicy { StopSiblings, ContinueSiblings }
pub enum ChildRequirement { Required, Optional }
pub enum AdmissionError {
    Busy, Closed, ScopeEnded, Stopped, DeadlineExpired, CapacityExceeded,
    ExecutorUnavailable, WouldDeadlock,
}
pub struct SubmitError<F> { /* reason + uncalled body, fields private */ }
impl<F> SubmitError<F> {
    pub fn reason(&self) -> AdmissionError;
    pub fn into_parts(self) -> (AdmissionError, F);
}
```

容量默认值为 manager：active 4、queued 64、admission waiters 64、未取 outcomes 64、清理并行 4、diagnostic events 256；scope：后代 child 数 64（含其后代、不含自身）、资源登记 128、副作用记录 64（含其后代）。`OperationOptions.requirement` 默认 Required，仅有父级时参与父成功判定。

**冻结的标准库 `Default` 实现：** `ManagerLimits`、`ScopeLimits`、`PortableExecutorOptions`、`ThreadPoolExecutorOptions`、`ContextOptions`、`ManagerOptions`、`OperationOptions` 均实现 `Default`。四个设计 §8 配置 struct 的默认值如下：

| 类型/字段 | 默认值 |
| --- | ---: |
| `ManagerLimits.active_operations` | 4 |
| `ManagerLimits.queued_operations` | 64 |
| `ManagerLimits.admission_waiters` | 64 |
| `ManagerLimits.unconsumed_outcomes` | 64 |
| `ManagerLimits.cleanup_concurrency` | 4 |
| `ManagerLimits.diagnostic_events` | 256 |
| `ScopeLimits.child_operations` | 64 |
| `ScopeLimits.resources` | 128 |
| `ScopeLimits.effect_records` | 64 |
| `OperationOptions.requirement` | `ChildRequirement::Required` |
| `OperationOptions.limits` | `ScopeLimits::default()` |
| `OperationOptions.child_failure_policy` | `ChildFailurePolicy::StopSiblings` |
| `OperationOptions.context` | `ContextOptions::default()`（clock/deadline/budget 均为 `None`） |
| `ManagerOptions.limits` | `ManagerLimits::default()` |
| `ManagerOptions.scope_limits` | `ScopeLimits::default()` |
| `ManagerOptions.clock` | `None` |
| `ManagerOptions.async_executor` | `worker_count=4, task_capacity=68, polls_per_turn=16` |
| `ManagerOptions.sync_executor` | `worker_count=4, queue_capacity=64` |
| `ManagerOptions.cleanup_executor` | `worker_count=4, queue_capacity=64` |

`PortableExecutorOptions::default()` 与 `ThreadPoolExecutorOptions::default()` 分别等于表中的 async、thread-pool 配置；`ContextOptions::default()` 三个字段均为 None。async/sync worker 数与默认 active_operations=4 对齐；异步 task_capacity=68 对应 4 个活动槽加 64 个排队槽；polls_per_turn=16 沿用 0.1 driver 的公平轮转配置。`ResourceScopeOptions`、`GroupOptions`、`ServiceResourceOptions` 没有冻结 `Default` 实现；按 §8 使用显式 `capacity` 字段。执行器默认值属于本次 crate 默认配置，不是兼容承诺；应用可通过 ManagerOptions 显式覆盖。

```rust,ignore
impl OperationManager {
    pub fn new(options: ManagerOptions) -> Result<Self, ManagerCreateError>;
    pub fn try_submit<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationHandle<T, E>, SubmitError<F>>
    where F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
    pub fn submit<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationHandle<T, E>, SubmitError<F>>
    where F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
    pub fn as_async(&self) -> AsyncOperationManager;
}
impl AsyncOperationManager {
    pub fn new(options: ManagerOptions) -> Result<Self, ManagerCreateError>;
    pub fn try_submit<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where F: FnOnce(OperationScope) -> Fut + Send + 'static,
          Fut: Future<Output = Result<T, E>> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
    pub async fn submit<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where F: FnOnce(OperationScope) -> Fut + Send + 'static,
          Fut: Future<Output = Result<T, E>> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
    pub fn as_sync(&self) -> OperationManager;
}
```

两种 manager 的 clone/转换共享核心与固定 `SubmissionTarget`；根构造绑定根，`scope.manager()` 绑定该 scope 的操作。父关闭返回 `ScopeEnded`，停止后返回 `Stopped`，绝不退化成根提交。scope 不 Clone，也没有 `scope.submit` 等子提交快捷入口。`run`、`run_and_settle` 采用设计 §8.1 同形签名，`OperationCompletion<T,E>` 含 `OperationOutcome<T,E>` 和 `Arc<SettlementReport>`。

### 结果、范围与资源

稳定结果签名：`OperationOutcome<T,E> = Succeeded(T) | Failed(E) | ChildFailed(ChildFailureSummary) | Stopped(StopReport) | Panicked(PanicSummary) | ExecutionFailed(ExecutionFailure)`。同步 handle 的 `wait_outcome(&mut self) -> Result<OperationOutcome<T,E>, OutcomeError>`、`try_take_outcome`、`wait_settlement(&self) -> Result<Arc<SettlementReport>, ObservationError>`；异步 handle 对应 async wait，异步 settlement wait 返回 `Arc<SettlementReport>`。outcome 独占取一次，settlement 可重复观察且不可回写。

```rust,ignore
impl OperationScope {
    pub fn id(&self) -> OperationId;
    pub fn manager(&self) -> OperationManager;
    pub fn context(&self) -> OperationContext;
    pub fn snapshot(&self) -> OperationSnapshot;
    pub fn request_stop(&self, reason: StopReason) -> StopRequestStatus;
    pub fn manage<T: Send + Sync + 'static>(&self, resource: T)
        -> Result<ResourceRef<T>, ManageResourceError<T>>;
    pub fn manage_with<T, C>(&self, resource: T, cleanup: C)
        -> Result<ResourceRef<T>, ManageWithError<T, C>>
    where T: Send + Sync + 'static, C: AsyncCleanup<T>;
    pub fn get_resource<T: Send + Sync + 'static>(&self, key: ResourceKey<T>)
        -> Result<ResourceRef<T>, ResourceLookupError>;
    pub fn resource_scope(&self, options: ResourceScopeOptions)
        -> Result<ResourceScope, ResourceScopeError>;
    pub fn cleanup_after(&self, resource: ResourceId, prerequisite: ResourceId)
        -> Result<(), CleanupDependencyError>;
    pub fn transfer_resource(&self, resource: ResourceId, destination: &OperationScope)
        -> Result<ResourceTransfer, ResourceTransferError>;
}
pub struct ResourceRef<T: Send + Sync + 'static> { /* private */ }
impl<T: Send + Sync + 'static> Clone for ResourceRef<T> { fn clone(&self) -> Self; }
impl<T: Send + Sync + 'static> Deref for ResourceRef<T> {
    type Target = T; fn deref(&self) -> &T;
}
impl<T: Send + Sync + 'static> ResourceRef<T> {
    pub fn id(&self) -> ResourceId;
    pub fn key(&self) -> ResourceKey<T>;
}
```

`ResourceRef` 只给 `&T`，不提供 `DerefMut`、`into_inner` 或可升级 Weak。其 clone 加一份使用引用，move 不变，drop 减一。释放充要门槛是所属范围 Ended 且 user refs 为 0；Open+0 仍可用 key 重新 get。T 必须 `Send + Sync + 'static`，框架不保护 T 的业务并发访问，调用方通过 T 内部同步实现协调。`OperationScope` 不 Clone；保存 scope/manager 不延长执行帧开放期。

`AsyncCleanup<T>` 固定为 `Send + Sync + 'static` 策略，`cleanup(self: Arc<Self>, resource: T, context: CleanupContext) -> CleanupFuture<T>`；future 为 `Pin<Box<dyn Future<Output=CleanupOutcome<T>> + Send + 'static>>`。Outcome 为 `Released | Transferred(TransferReceipt) | Failed { failure, resource }`。失败必须保留 T 和可重试策略。

## 2. C01—C10 决策与验收

### C01 — 公共签名、默认值、错误与重导出 — STABLE（重导出核对待主控）

以上类型签名以及设计 §8 的 observer、resource scope、reservation、report、recovery、group、shutdown、context 类型构成唯一来源；各模块不得另造同名类型或改 bounds。关键默认值见上。错误携带闭包、资源、策略或 reservation 时必须保留原 owner，不能只变成字符串；F/T/E 不要求 Clone/Debug。

**验证用例：** 一个编译契约应从 crate 根同时导入 `OperationManager`、`AsyncOperationManager`、`OperationScope`、`ResourceRef`、`OperationOutcome`、`SettlementReport`、错误和配置类型，并编译设计中两个 submit 闭包的 bounds；断言七个冻结类型的 `Default` 输出字段逐一匹配上表；另以 compile-fail 检查 scope 不可 Clone、ResourceRef 不可 `DerefMut`。`ResourceScopeOptions` 等 capacity 配置以显式字段构造，不新增无文档依据的 Default。根重导出列表由主控逐项对照设计 §8 实际声明，本文不以“待实现”冒充已导出。

### C02 — adapter 预留/启动完整入口 — STABLE（内部端口，主控审查）

已稳定事实：领域 adapter 采用 `reserve_child → attach(control) → commit`，取得不可 Clone 的 `ChildStartPermit` 后才 activate 暂停任务；未消费 permit 必须 abandon，stop/commit 串行，activate/abandon 至多一次且非阻塞；`OperationControl` 精确签名见设计 §8.8；control 分辨 outcome、input release、settlement。普通工作仍只经 manager 的统一 submit。

内部端口统一如下且不在 crate 根导出。reservation 持有 child 名额和未启动 operation record；attach 绑定 control；commit 产生唯一 permit。失败 payload 原样退还尚未启动对象及容量凭证。

```rust,ignore
pub(crate) struct AdapterReservation { /* !Clone, owns child admission */ }
pub(crate) struct AttachedChild { /* !Clone, owns control and unstarted task */ }
pub(crate) struct ChildStartPermit { /* !Clone, one-shot */ }
pub(crate) enum AdapterErrorKind { Stopped, ScopeEnded, CapacityExceeded, AlreadyAttached, AlreadyCommitted }
pub(crate) struct AdapterError<P> { pub(crate) reason: AdapterErrorKind, pub(crate) payload: P }
pub(crate) trait ChildAdapter: Send + Sync {
    fn reserve_child(&self, parent: &OperationScope, options: &OperationOptions)
        -> Result<AdapterReservation, AdapterError<()>>;
    fn attach<P: Send + 'static>(&self, reservation: AdapterReservation,
        control: Arc<dyn OperationControl>, payload: P)
        -> Result<AttachedChild, AdapterError<(P, AdapterReservation)>>;
    fn commit(&self, child: AttachedChild) -> Result<ChildStartPermit, AdapterError<AttachedChild>>;
}
impl ChildStartPermit {
    pub(crate) fn activate(self) -> Result<(), AdapterError<Self>>;
    pub(crate) fn abandon(self);
}
```

activate 与 abandon 对同一 operation state 做一次性原子裁决，均非阻塞；permit 尚未消费时 Drop 等价于一次 abandon。activate 后调度失败转为 ExecutionFailed，control 仍持有输入引用直至确认不再使用。
### C03 — Cleaning 资源转交与 TransferReceipt — STABLE（内部接收端口，主控审查）

已稳定事实：开放 scope 间 `transfer_resource` 原子移动完整依赖组的 scope 持有；同 manager 内 ResourceId 和 user count 稳定，失败源不变。RecoveryStore 的整组签收使用独立 `RecoveryTransferReceipt`。清理策略只有返回真实接收凭据后才能产出 `CleanupOutcome::Transferred(receipt)`；不能把正在 cleaning 的资源当作开放资源 transfer。

接收者为强 owner。只可接收完整依赖组的 CleanupCapsule，其中包括 typed-erased T、清理策略、ResourceId 与业务 permit；先登记容量和 capsule，再签发 receipt。失败必须退还 capsule：

```rust,ignore
pub(crate) struct CleanupCapsule { /* !Clone; complete group and permits */ }
pub(crate) struct CleanupTransferError { pub(crate) reason: CleanupTransferErrorKind, pub(crate) capsule: CleanupCapsule }
pub(crate) enum CleanupTransferErrorKind { Closed, CapacityExceeded, InvalidGroup }
pub(crate) trait CleanupReceiver: Send + Sync {
    fn accept(&self, capsule: CleanupCapsule) -> Result<TransferReceipt, CleanupTransferError>;
}
```

receipt 是接收成功凭据，不是 owner。无接收者时 cleanup 返回 Failed 并保留 T，不能报告 Transferred。RecoveryStore 仍使用不同的 RecoveryTransferReceipt。

**文档审查检查 / 用例：** receiver 拒绝时源仍持有资源且 settlement 不能报告 Transferred；接纳后旧 owner 可释放强持有但目标仍有 owner；receipt 只绑定 resource/group、source、target，不能跨组或伪造。
### C04 — Required 子孙失败与摘要映射 — STABLE

`ChildOutcomeSummary` 只含 operation id、`ChildOutcomeKind` 和 bounded `DiagnosticSummary`；`ChildFailureSummary` 表示直接失败孩子。孙级 Required 失败必须沿父链映射成每个必要祖先的 `ChildFailed`，不得将孩子自身的 `ChildFailed` 降格为普通成功或只看直接子闭包返回值。Optional 子业务失败不阻止父成功；父的 `StopSiblings`/`ContinueSiblings` 只控制同级继续策略，不改变 Required 语义。完整 typed `T/E` 只属于对应 handle 的唯一 outcome slot；摘要用于父级判定、快照和汇总，不复制/保留 typed outcome。

**验证用例：** A(Required)→B(Required)→C(Required)，C 返回 Failed，验证 B、A outcome 均 ChildFailed 且 C 摘要可追溯；同树 Optional C 失败而 A 自身成功时 A 可 Succeeded；`ContinueSiblings` 下兄弟仍运行但 A 仍 ChildFailed。

### C05 — 有界报告与责任不丢失 — STABLE（报告字段增补，主控审查）

已稳定事实：资源/子操作/效果记录各有额度；报告不能因无限串行子提交无限增长；diagnostic text 最大 256 UTF-8 字节并在字符边界截断；子资源恢复或转交事实汇总到父报告，历史报告不可回写。settlement 发布前先释放 root execution permit；对应未取 outcome 的容量依计划保留到 outcome 被取走或放弃。这样调用方观察到 settlement 后可立即重试新 root，重复 start 不会被已完成 execution 错误地判 Busy。

children、cleanup failures、resource evidence、transfers、recoveries 的摘要数组各自最多保留 `diagnostic_events` 项；effects 最多保留当前子树 `effect_records` 项。SettlementReport 增字段：`children_total/children_truncated`、`cleanup_failures_total/cleanup_failures_truncated`、`resource_reports_total/resource_reports_truncated`、`transfers_total/transfers_truncated`、`recoveries_total/recoveries_truncated`、`effects_total`。保留 `effect_overflow`，定义为未保留 effect 数。计数饱和至 `usize::MAX` 且置 truncated，不环绕。

截断仅影响诊断数组，不影响责任 owner。完整 cleanup T/策略/permit 始终由资源 cleanup owner 或 RecoveryGroup/RecoveryStore 强持有；父报告只保留有限摘要和 recovery handle。不能可靠保存 owner 时，settlement 和关闭保持 Pending。diagnostic_events=0 合法，摘要数组为空、累计字段照常计数。

**文档审查检查 / 用例：** 超过报告上限的串行子任务仍能给出准确累计数、截断标志；被省略的 cleanup failure 仍可通过恢复入口找到，管理器关闭仍等待其解决/签收；普通 effects 可截断但 overflow 增长；等待 settlement 返回后立即重试 root，不得因 execution permit 延迟退还而 Busy，而未取 outcome 仍占用 outcome 槽。
### C06 — Context、deadline、预算与拒绝映射 — STABLE

manager 默认 clock 与 context clock 校验同一 clock domain；child deadline 只能收紧；跨域 earliest 返回 `ClockDomainMismatch`。无 clock 而提供 deadline 返回 `ContextError::ClockUnavailable`；context 构造/child budget reject 为 `ContextError::BudgetRejected(BudgetError)`。准入阶段期限已过映射 `AdmissionError::DeadlineExpired`，祖先停止映射 `Stopped`。BudgetPolicy rejection 映射 `AdmissionError::CapacityExceeded`，原 BudgetError summary 进入 bounded diagnostic event；SubmitError 继续保留未调用 body F。

清理使用独立 `CleanupContext { resource_id, context }`，由 manager cleanup executor 建立，不继承已取消/超时的业务 context；清理期限为独立 cleanup 策略配置（未配置则 None），预算不用于拒绝释放责任。停止/关闭并发的拒绝由原子裁决结果确定：全局关闭先完成为 Closed，stop 先接受为 Stopped，父执行帧先结束为 ScopeEnded，期限已过为 DeadlineExpired。ClockSource 注册期间到期仍须唤醒。

**验证用例：** 无时钟 deadline、跨域 deadline、注册期间到期、预算拒绝保留 F 且生成诊断、父 scope end 与 submit 竞争、Cancel 与 commit 竞争、业务 deadline 到期但 cleanup 仍推进。
### C07 — group 与 service resource 计数边界 — STABLE（主控已决：A）

Group capacity 是该 group 当前未归还的已接纳操作数；操作在自身 settlement 稳定且 outcome 被取走/放弃后归还 group 名额。子操作额度沿祖先链计数：祖先 scope 的 `child_operations` 统计整棵子树尚未归还的后代，不含自身；不因嵌套 group 重置额度。service resource scope 使用独立 capacity，成功清理/接收转交/进入可靠 recovery owner 后归还登记容量；仅主体请求结束不归还仍存活资源容量。

Group Drain 停止该组新准入，但允许已接纳操作按剩余祖先额度提交子工作；Cancel 另禁新 commit 并向组内操作传播停止。二者不关闭共享 manager 核心；manager shutdown 作用于核心所有 root/group。service resource scope 在 group shutdown 时结束范围持有，既有 ResourceRef 继续保护资源。

父绑定 group 继承固定 parent target，不延长父 scope 开放期。父 scope 结束后拒绝新的 group submit，返回 `ScopeEnded` 且不得 root fallback；已经接纳的 group 工作继续按自身生命周期完成。该选择由设计 §3.2 的固定绑定规则与 §8.7 group 保留创建视图 target 合并推出，并经主控确认。

**验证用例：** group capacity 满时拒绝/等待，满足归还条件才释放；group Cancel 不影响另一 group 或全局 root；service scope end 后引用存活不清理、最后引用归还才清理；父结束后新 group submit 得 ScopeEnded，既有工作继续完成。
### C08 — 恢复/副作用/混合进展诊断 — STABLE（字段增补，主控审查）

RecoveryStoreCreateError 由 `InvalidOptions { field }`、`ExecutorUnavailable`、`ThreadSpawn { source }` 构成，与 ManagerCreateError 的构造失败保持一致。副作用通过 `OperationScope::record_effect(resource: Option<ResourceId>, fact: EffectFact, description: DiagnosticSummary) -> Result<(), EffectRecordError>` 登记；该方法只登记事实，不拥有/释放业务资源。effect_records 配额满时返回 `EffectRecordError::CapacityExceeded`，不丢已登记记录；settlement 数组达诊断上限则累计 effects_total/effect_overflow 并置截断。

ManagerSnapshot 混合资源进展计数增补 `scope_open_resources`、`waiting_for_users_resources`，与已有 `cleaning_resources`、`unconfirmed_users` 并列；这是互斥的资源当前阶段计数，不用一个 ResourceProgress 代表多个资源。恢复组数与正在执行恢复尝试分别计数；`RecoveryStoreSnapshot` 已有 groups/resources/running_attempts。

**验证用例：** RecoveryStore worker 无法启动时构造错误保留原因且不泄漏容量；effect 满额明确拒绝，report 截断仍更新累计；不同资源并发处于四种阶段时 snapshot 四个计数均准确。
### C09 — Unconfirmed 责任、重接与关闭 — STABLE（未确认保持 Pending）

`Unconfirmed` 是进展，不是 settlement terminal。导致 Unconfirmed 的实际 owner/capsule 必须继续由 manager 的 `UnconfirmedRegistry` 强持有，和弱观察索引分开；registry entry 包含 ResourceId、当前 adapter 标识、最后诊断、已持有的 cleanup capsule/permit（若有）及 reattach 状态。没有可恢复 capsule 时也不得清空 entry。

最小恢复入口为 crate-private `UnconfirmedRegistry::reattach(resource_id, capsule) -> Result<(), ReattachError>`，验证 resource id、manager id 和唯一 nonce 后将 capsule 交回原恢复组；错误携带 capsule，源责任不变。成功前清理仍 Pending。公共观察通过 ManagerSnapshot 的 unconfirmed_users 与 OperationSnapshot.resources=Unconfirmed；diagnostic event 给出 ResourceId 和最近失败摘要。关闭 wait 持续 Pending，wait_for/wait_until 返回 Pending(snapshot)，丢弃 shutdown handle 不撤销关闭或释放 owner。没有 adapter reattach 能力时条目保持 Pending，直到进程外部诊断/恢复路径明确签收；不能假报完成。

**验证用例：** adapter panic 后无确认回执，settlement 与 shutdown 均 Pending 且计数非零；有效 capsule 重接后 owner 继续恢复；错误 resource id capsule 原样退还；最后一个公共 observer/handle Drop 不改变 owner 与关闭快照。
### C10 — 内部 ports、锁序、强弱持有与锁外边界 — STABLE（内部接缝）

**稳定所有权图：** manager/observer 强持有核心与操作元数据，但不增加 ResourceRef user count；执行帧强持有开放 scope owner；scope owner 持有 T；每个 ResourceRef 有独立用户计数；资源 ReadyForCleanup 后唯一 cleanup owner 接管 T、策略及业务 permit；RecoveryStore/合法 transfer receiver 签收后成为实际强 owner。弱索引只用于 lookup/通知，不能用于复活资源。scope end 和 refs==0 的并发只由单次状态转换发放一个 cleanup owner。RecoveryHandle 是恢复组观察/重试权限，不能代替组中真实 owner。

**稳定端口：** 内部 `OperationControl: Send + Sync` 固定签名：`request_stop(&self, reason: StopReason)`、`snapshot(&self) -> ChildSnapshot`、`poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary>`、`poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease>`、`poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>>`。ChildRelease 只确认不再使用附带输入，不替代 settlement。timer registration 的 cancel 消费 `Box<Self>`；ClockSource 注册返回 owning registration；BudgetPolicy::check 不接管 policy；cleanup strategy Arc 保留在记录侧。

**锁序：** 所有涉及多锁的转换按固定级别递增取得：ManagerState → operation/ScopeState（按 OperationId）→ ResourceRegistry（按 ResourceId）→ RecoveryRegistry（按 RecoveryId）；禁止反向嵌套。跨资源操作按 ResourceId 升序锁定。各级只保护状态变更，不跨锁调用其他 owner 的方法；需要跨模块动作时先在锁内取得一次性事件/强 owner，再释放锁后投递，失败时按 owner 协议退回。停止与 commit、scope end 与 get、最后引用 Drop 与清理接管各自在同一线性化状态单元裁决。

任何代码不得持有内部状态锁调用 user body、AsyncCleanup、Drop、Waker wake/clone/drop、executor/adapter/receiver 回调或外部 timer/budget 方法。waiter 注销和通知登记可以在状态锁内更新内部列表，但唤醒动作必须移出锁。`src/internal/ownership.md` 必须保持此全序并列出持锁集合；新增锁先映射到上述层级，不得引入同级反向锁序。

**验证用例：** Barrier 交错并发 end/get/last-ref Drop 只能触发一次清理；多资源依赖 transfer 以逆序请求仍无死锁且 ID 稳定；Drop/cleanup/waker 探针重入 manager 不在锁内回调；弱索引失效后 observer 仍可读既有报告，但不能重新获得 T。

## 3. outcome、settlement 与报告承诺

outcome 是单次业务结果；settlement 是不可变的收尾报告。Required 子孙失败映射入 outcome，但 settlement 仍必须等所有已接纳子操作的稳定报告。settlement 的 Released/Transferred/RecoveryRequired 分别代表完成所选清理策略、真实接收方签收、责任进入可恢复 owner；报告存在不等于物理释放。Unconfirmed 不属于稳定 settlement。

资源清理逐项推进，不等无关 sibling resource；父资源可先于子资源清理，除非 cleanup dependency 或实际 ResourceRef 显式维持依赖。根名额保持到整树稳定 settlement；child 名额在 child settlement 稳定且 outcome 已取走或放弃后归还。适用业务 permit 跟随 T 到真实释放/接管，不可因报告截断或 waiter 放弃而提前释放。

## 4. 待主控审查的冻结项

C02/C03/C05/C06/C08/C09/C10 是从已接受生命周期规则推导的 crate-private 接缝和报告字段，不改变 crate 根公共 API；主控审查时若认为某一推导不符合现规范，应指出具体冲突条款后修订本文。当前没有未决调用方行为选择：父绑定 group 已按 A 冻结为父关闭后拒绝新提交 `ScopeEnded`，已接受工作继续。

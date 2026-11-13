# rs-operation 0.1.0 详细设计：受管理的操作生命周期与组合

- 状态：待评审设计，公共 API 尚未实现；本文不是使用现有 API 的教程。
- 初稿日期：2026-11-11；综合评审修订：2026-11-12。
- 初始化提交：`3ad0fd0`；本设计文档在初始化提交之后新增，供评审。
- 仓库：<https://github.com/qubit-ltd/rs-operation>。
- Cargo 包：`qubit-operation`；首个目标版本：`0.1.0`。
- 适用范围：单进程内拥有所有权的操作组合，同时支持同步、异步调用。

## 1. 这个库是什么

rs-operation 把多个操作及其占用的资源纳入共同管理的生命周期。它统一取消与期限传播、子操作登记、结果观察、资源使用依赖和退出收尾，使调用方不必在每个业务流程中重新编写一套“失败之后谁继续负责”的协议。

它管理的是操作，不限定操作的执行方式。一个操作可能是同步函数、后台文件写入、异步网络读取、外部进程或不能立即中断的模型推理。同步调用方阻塞等待，异步调用方等待 Future；两者观察同一套状态，不重复执行。

核心价值不是把若干 future 顺序 await。Rust 控制流已经能够表达先后关系；本库补足的是多个操作共享资源时的终结责任，特别是在调用方失败、取消、超时或直接丢弃 future 之后。

## 2. 要解决的痛点

### 2.1 请求结束，工作却没有结束

Web 请求取消不意味着后台文件写入停止，丢弃等待 future 不意味着子进程退出，推理超时也不意味着 native Session 已经可以释放。若把这些事件当作同一个“完成”，会过早删除输入、释放容量或销毁仍在被使用的对象。

### 2.2 清理顺序存在依赖

外部程序仍在读文件时不能清理临时资源。多个并行步骤共享一个文件时，仅仅按登记顺序逆序执行析构也不够：必须等全部使用者都确认释放资源。

### 2.3 每个库都有自己的取消与关闭协议

文件库知道发布和恢复，命令库知道进程与管道，模型库知道 Session，但应用需要把它们组合。若每个业务库再实现一个 supervisor、取消 token、deadline 和恢复注册表，容易产生不同的语义和重复线程。

### 2.4 错误被收尾错误覆盖

转换失败后删除文件又失败，应用需要同时保留两件事。反过来，业务成功也不能掩盖必需清理失败。单个字符串错误不足以表达这类结果。

### 2.5 取消不等于撤销副作用

文件已经发布、对象已经上传或邮件已经发出，随后取消不能把事实改写成“没有发生”。框架必须保留部分完成事实，而不是自动承诺回滚。

## 3. 使用者与非目标

面向基础设施库作者、CLI/批处理作者，以及组合文件、进程、网络和模型能力的服务开发者。业务流程仍由 Rust 控制流表达；领域库提供适配器和真实状态。

0.1.0 不提供分布式工作流、跨进程恢复、持久化任务队列、自动重试、自动业务补偿、数据库事务、任意线程强杀、进程沙箱或通用 DAG 业务调度语言。进程崩溃、断电和 kill -9 后的可靠恢复需要外部持久化机制，不由内存 supervisor 保证。

资源依赖可以构成图，但图只管理操作使用资源及收尾先后，不负责推断任意业务的数据流或自动并行化业务代码。

### 3.1 进度与业务消息由调用方负责

rs-operation 不提供进度指标、阶段权重、百分比、ETA、进度 Reporter、事件总线适配器或可选 progress feature，也不依赖 qubit-progress / qubit-event-bus。OperationContext 不携带这些组件的句柄，supervisor 不自动发出业务阶段消息。内部状态快照、终结报告和必要诊断仅服务生命周期正确性，不扩展为业务进度系统。

业务若需要展示“正在下载/转换/上传”，在自己的操作实现或组合流程中调用 rs-progress，或通过 rs-event-bus 发布自定义阶段消息。阶段名称、工作量、关联 operation ID、上报频率和失败策略均由业务决定；不要求为了汇报进度向本库登记额外的控制协议。

已核对当前本地版本：qubit-progress 0.9.3 提供 Progress / Reporter 和 AsyncProgress / AsyncReporter；qubit-event-bus 0.21.0 提供 EventBus 与 AsyncEventBus，后者已有异步 publish、subscribe 和 shutdown。两者都能用于应用自己的同步或异步汇报；因此无需以“缺少异步消息能力”为由新增 rs-event-bus 下一版设计。具体调用以对应版本公共 API 为准。

进度终态和消息发布回执不替代本库的释放确认。AsyncEventBus 的发布回执表示受理，不代表订阅者已执行完成；应用还必须持续驱动 AsyncSubscription::run。上报失败是否影响业务结果由应用明确选择，本库不自动将它升级为取消、也不自动吞掉。需要可靠发布的业务步骤应由应用作为普通受管理操作组合，投递和持久性保证仍由事件总线 provider 决定。

## 4. 贯穿示例：下载、转换、上传

### 4.1 业务场景

服务下载一个远程对象，将内容暂存到本地，调用转换程序，然后上传输出。远程源可能在读取中变化，转换程序可能挂起，上传可能已经成功但客户端中断连接。

```text
下载源对象 → 暂存输入 → 转换进程 → 暂存输出 → 上传目标
                  └─输入租约─┘          └─输出租约─┘
```

由文件库负责私密暂存与发布，由命令库负责进程与管道，由网络库负责对象版本与上传，由本库登记操作和资源依赖。临时资源清理不是上传回滚，上传完成的目标默认保留。

### 4.2 正常路径

1. supervisor 受理请求并预留子操作/资源登记配额。
2. 建立作用域和请求 context，传递总期限与取消信号。
3. 下载并登记输入临时资源；获取输入使用租约，启动转换。
4. 转换进程及全部输入 helper 释放后归还输入租约。
5. 上传持有输出租约，上传终结后归还。
6. scope 结束，清理不再使用的临时资源；发布业务结果与完整终结报告。

### 4.3 失败与取消

- 下载失败：不启动转换，清理已创建的暂存；保留下载错误及清理失败。
- 转换失败：不上传；请求其后代和 I/O helpers 终结，再清理文件。
- 转换进程终结未确认：业务失败可以确定，但输入仍保留，scope 标为未释放，容量继续占用。
- 上传成功后取消：报告上传已成功，记录取消请求；不自动删除远程对象。
- 调用方丢弃句柄：scope 所有权转给 supervisor，请求停止并继续收尾；应用可通过 operation ID 观察。

### 4.4 示意调用

以下为目标协议的业务伪代码，`files`、`commands`、`objects` 均为领域适配器，不是本仓库已经提供的 API。

```rust,ignore
let mut operation = supervisor.start(options, move |scope| async move {
    let input = files.download_to_temp(&scope, source).await?;
    let input_use = scope.acquire_use(input.resource_id())?;
    let converted = commands.convert(&scope, input, input_use).await?;
    let output_use = scope.acquire_use(converted.resource_id())?;
    let receipt = objects.upload(&scope, converted, output_use).await?;
    Ok(receipt)
})?;
let outcome = operation.result().await?;
let settlement = operation.released().await;
// 应用根据 outcome 和 settlement 分别决定响应与运维处理。
```

业务 future 的返回值不是子进程已回收的证明；领域适配器必须在实际 ResourceReleased 后归还租约。

## 5. 第二个验证场景：异步采样和不可中断推理

```text
异步读取有限 ranges → 提取 owned features → 模型 worker 推理 → 返回分类
```

provider/文件库保证采样范围与对象一致性，模型库管理 CPU worker 和 Session。本库传递 deadline，登记推理操作，并保持 feature 与模型许可存活至推理真正退出。

推理开始前取消可以移除排队作业；推理开始后若后端不支持中断，取消只停止交付与后续步骤，等待 native 调用返回后才释放资源。framework 不调用 unsafe 销毁 Session 来追求表面上的及时取消。

这个场景不需要文件或命令，能够验证公共协议是否被 MIME 特有细节绑住。

## 6. 设计不变量

1. 每个受理的操作有唯一终结责任人；Drop 不造成无主任务。
2. 取消请求、业务结果、资源释放是独立事实。
3. 只有领域执行器能够确认其工作已停止访问资源。
4. 资源仍有使用者时，不启动破坏该资源的清理。
5. 登记与启动之间不能存在“已启动但未纳管”的窗口。
6. Drop 不执行阻塞等待、递归清理或任意用户清理闭包。
7. 主业务错误与收尾错误分别保存，后者不覆盖前者。
8. 已经发生的副作用不能被取消状态抹掉。
9. 控制通知不因普通任务队列满而丢失。
10. 任务、登记项、结果和诊断均有容量约束，不默认无界。

## 7. 模块与依赖边界

| 模块 | 公开职责 |
| --- | --- |
| `context` | 请求标识、取消树、单调期限、观测上下文 |
| `operation` | 句柄、状态快照、一次性结果与释放观察 |
| `scope` | 子操作登记、资源登记、关闭与失败传播策略 |
| `resource` | 使用租约、清理依赖、恢复所有权 |
| `supervisor` | 后台终结责任、容量、关闭和未观察结果 |
| `driver` | 受管理 Future 驱动与领域操作观察适配 |
| `report` | 类型化结果之外的终结、清理和诊断事实 |

0.1 核心基于 std，包括 Future、Waker、线程同步、Instant 和有界注册表；不默认绑定 Tokio/async-std，不直接依赖 rs-command、rs-local-files、rs-mime 或 rs-magika。领域适配器放在领域库或独立集成 crate，避免循环依赖。

同步与异步观察接口共享相同状态，不通过两个不同 feature 复制状态机。特定运行时适配器后续单独提供；时间与预算适配到 qubit-clock/qubit-budget 时通过小接口完成，不在本库重新实现领域字节计量。

## 8. 核心公共对象与 API 摘要

以下签名为设计摘要，省略普通 getter/builder，不代表当前 crate 已导出这些符号。

```rust,ignore
pub struct OperationContext { /* immutable shared request state */ }
pub struct OperationSupervisor { /* shared service handle */ }
pub struct OperationScope { /* handle to supervisor-owned scope state */ }
pub struct OperationHandle<T, E> { /* single result consumer */ }
pub struct OperationObserver { /* cloneable snapshots and release observation */ }
pub struct ResourceUse { /* owned non-Clone use lease */ }

impl OperationSupervisor {
    pub fn start<F, Fut, T, E>(&self, options: ScopeOptions, body: F)
        -> Result<OperationHandle<T, E>, StartError<F>>
    where F: FnOnce(OperationScope) -> Fut + Send + 'static,
          Fut: Future<Output = Result<T, E>> + Send + 'static,
          T: Send + 'static, E: std::error::Error + Send + Sync + 'static;
    pub fn start_blocking<F, T, E>(&self, executor: &dyn BlockingExecutor,
        options: ScopeOptions, body: F) -> Result<OperationHandle<T, E>, StartError<F>>
    where F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
          T: Send + 'static, E: std::error::Error + Send + Sync + 'static;
}
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

`start` 是立即有界受理并提交，不是惰性 async 函数；返回成功后 supervisor 就有责任。另提供 `start_wait` 与 `start_wait_blocking` 等待受理，等待者也有限，且受总期限约束；受理失败归还未启动 body。便利 `run_async` 在首次 poll 时调用 start，未 poll 丢弃无副作用。

StartError<F> 包含 AdmissionError 和未启动的 body F，允许调用方回收捕获的输入或重试；执行器受理后的失败则成为 operation 的结果，不再声称 body 尚未执行。

start_wait 的等待 Future 在受理前被丢弃时退出等待队列并归还预留；因 Future 已拥有 F，其普通非阻塞析构在此发生，不承诺自动向已消失的调用方归还 F。需要重试输入的调用方应保留独立所有权或等待显式受理错误；受理成功与等待取消竞争时必须由状态机唯一裁决，已受理工作按 handle Drop 规则交给 supervisor，不能遗失在完成通知中。

OperationHandle 不 Clone，防止多个消费者争抢 T。result 输出只取一次，后续返回 AlreadyTaken；等待 result 的 Future 被丢弃，句柄仍可再次等待，不丢结果。released 可重复观察不可变终结报告。OperationObserver 可 Clone，不读取 T，只观察状态、停止请求及终结摘要。

## 9. 上下文、取消和期限

OperationContext 包含 OperationId、父 ID、CancelToken、可选绝对 Deadline、追踪标签和预算租约引用。子 context 收紧权限和预算，不扩张父期限。取消向子树传播，子操作取消不自动取消兄弟；兄弟处理由 scope 策略决定。

request_cancel 只登记幂等请求并唤醒管理者，不能执行 I/O。保留第一个停止原因，后续原因作为有限附加诊断。Deadline 到达触发停止请求，与人工取消统一经过状态机，但报告区分 Timeout 和 UserCancelled。

Deadline 使用同一单调时钟域；跨服务传递绝对时刻或剩余时长时附带计算基准，不重置排队时间。内部 ClockSource 注入用于测试，不能把彼此不可比较的假时钟 Instant 混合。清理使用独立服务收尾预算，不复用已经到期的业务期限立即放弃资源。

Context 不把所有领域预算强行合成一个整数。公共协议仅管理租约存活及已使用/已保留摘要，字节、磁盘、Session、进程数量分别由对应执行器计量。同一个 owned allocation 的共享引用只计一次；复制必须申请新的领域预算。

## 10. 业务状态与资源状态是两个维度

业务状态：Queued、Running、ResultReady、ResultTaken。停止原因是独立字段，不构造一个覆盖所有状态的 Cancelled 位。

资源状态：Active、Stopping、Cleaning、Released、RecoveryRequired、Unconfirmed。只有 Released 表示已确认操作不再访问登记资源，且必需清理已按策略完成或资源已显式转交。

OperationOutcome 为 Succeeded(T)、Failed(E)、ChildFailed(ChildFailureSummary)、Stopped(StopReport)、Panicked(PanicSummary)。Failed(E) 保留 body 的领域错误；ChildFailed 表达未被 body 显式处理的 required 子操作失败，不能将任意子 E 强转为父 E。Stopped 仅在尚未确定业务结果且 scope 确认停止交付时成立，不覆盖已经成功发布的领域结果。部分副作用通过领域错误 E 或 EffectRecord 表达；框架不知道文件是否发布，不能凭取消自行构造 NotPublished。

SettlementReport 独立包含子操作终结摘要、清理失败、资源转交事实和是否全部释放。成功进入 ResultReady 的前提是 body 与 required 子操作的业务结果均已确定；不能在 required 子操作仍可能失败时先发布整体成功。失败、停止或 panic 可先发布，不必等待不可中断子操作的业务结果；其后到结果进入终结报告，不重写已发布主结果。子操作业务完成后仍可能有资源收尾，因此 ResultReady 仍可早于 Released。optional 后台工作也必须保留终结责任。

业务失败和清理失败可并存；业务成功但必需清理失败时 result 仍保留业务成功事实，released 报告 RecoveryRequired。便利 `run_to_completion` 返回两者组合，不只返回 T。

结果发布与停止接受在同一状态单元串行裁决：先发布的成功不被稍晚取消覆盖；先接受停止则后到的普通计算结果不能再发布成功。领域已提交的外部副作用始终独立保留在 EffectRecord 中，即便最终结果是 Stopped。相互竞争的失败取首次被状态机接受者为主因，其余进入有界次级摘要，溢出计数可见。

`released` 只有稳定终结（Released 或 RecoveryRequired 且恢复 owner 已转交管理者）才返回；Unconfirmed 是观察快照，不伪造终结。另提供 `observe_until(deadline)`，到期返回当前快照与可继续观察的 observer，不销毁后台状态。

## 11. 子操作协议：接纳、停止、确认释放

领域适配器实现 object-safe 的 ManagedOperationControl。协议摘要：

```rust,ignore
trait ManagedOperationControl: Send + Sync {
    fn request_stop(&self, reason: StopReason); // 非阻塞、幂等
    fn snapshot(&self) -> ChildSnapshot;        // 有限且不执行 I/O
    fn poll_business(&self, cx: &mut Context<'_>)
        -> Poll<ChildBusinessSummary>;         // 不消费领域 typed 结果
    fn poll_released(&self, cx: &mut Context<'_>)
        -> Poll<ChildSettlement>;              // Ready 必须是领域确认
}
```

业务完成和资源释放分别通知，不能等 poll_released 才发现 required 子操作已失败。ChildBusinessSummary 只含成功/失败/停止/panic、操作 ID 和有界摘要，不搬走 T/E；它是可重复观察的稳定事实，不能因调用方先取 result 而消失。两个 poll 入口必须分别支持通知登记。

轮询不是忙轮询，Pending 必须登记 Waker；状态变化使用发布/获取同步并唤醒，防止丢通知。领域业务输出保持类型，不强制把所有结果变成 Any；注册表仅擦除控制和释放观察对象。

登记使用两阶段协议：`scope.reserve_child()` 先取得登记槽，领域 executor 受理成功后 `reservation.attach(control)`，再允许领域操作真正启动。适配器必须支持 suspended start/commit，或在其原子 submit 过程中完成纳管。启动失败归还槽。reservation Drop 只释放未启动槽，不能把已经启动的工作遗弃。commit 必须检查附接后可能已经到达的取消，停止请求和 commit 之间有明确线性化点；取消先被接受则不允许启动，启动先提交则由已附接的 control 负责停止。

不具备上述启动能力的旧 API 不能声称完全纳管；提供 LegacyUnmanaged 标记只用于迁移诊断，默认严格 scope 拒绝它。框架不能在任意 spawn 后加一行 register 就保证没有竞态。

子操作释放后归还使用租约与活动许可；业务结果交付不释放仍在 native 调用或 helper 中使用的资源。Control 通知以预登记原子标志/专用唤醒完成，不向可能已满的业务队列发送普通“取消任务”。

## 12. 资源登记、使用租约与清理依赖

资源 owner 在 `scope.register_resource` 时被转交给 supervisor 管理；业务获得资源访问 handle，真正原生句柄由领域库按权限导出。注册失败必须归还 owner。先申请登记槽，再创建需要清理的资源；创建中途失败也要交回部分资源/恢复 owner。

`scope.acquire_use(resource_id)` 只在资源开放使用时创建非 Clone 的 ResourceUse。子操作启动时移动租约至适配器；子操作确认不再访问该资源时才归还。资源 CloseForUse 后拒绝新租约。body 正常返回后先关闭该 body 的新子操作登记，已 commit 的子 scope 可在其原有授权和配额内继续展开工作；已 commit 的子操作仍可使用其授权资源直至业务完成；全部子操作结束使用阶段后再关闭新租约。失败/取消时立即关闭新登记与新租约，已获租约保留至领域确认释放。清理仅在所有租约归零且依赖满足后启动。不得在 body 刚返回时销毁仍需由后台子操作访问的资源。

资源 A 可声明 `cleanup_after(B)`，表示 B 确认清理成功后才允许清理 A，边方向固定为 B → A。登记时检查自环、跨 supervisor 引用和环路，失败归还新 owner，不接收无法终结的图。多个互不依赖的清理可以有限并发；默认按资源 ID 保持确定的启动顺序，结果不依赖不确定的 HashMap 顺序。

若 B 清理失败或转入 recovery，A 不能视为依赖已满足：将 A 及必要依赖组成恢复组保留 owner，报告 DependencyBlocked。单独取走 B 的恢复 owner 不解锁 A；只有领域确认依赖满足或整个依赖组被原子转交才解除本 scope 的责任。清理可执行任务须同时满足使用租约归零及全部前置依赖成功，避免用拓扑顺序替代真实释放确认。

共享资源跨子 scope 使用时，由最近共同父 scope 拥有，子 scope 只持租约。0.1 不允许资源依赖任意无共同父作用域的独立 root；避免两个 root 关闭时互等。业务需要更长寿命时显式 promote/transfer，由父 scope 接受配额后完成原子转交。

若业务结果 T 返回访问 handle，调用方必须先取 result，并在等待 released 前释放租约或显式 transfer/promote 到已接纳的更长寿命 scope。不能一边持有当前 scope 的输出资源租约一边等待该 scope 清理完毕；run_to_completion 只适用于不依赖本 scope 存活的结果，资源型结果须在 body 内完成所有权转交。转交失败保留原 owner 和配额，不能先删除原登记再申请目标配额。

ResourceUse Drop 仅归还“这个租约的责任”；不能证明外部裸句柄没有被复制。适配器必须持有租约直到其内部全部使用者释放，导出不受控原生句柄会使保证失效，应显式标记协议边界。

## 13. 清理、恢复和业务补偿

清理是关闭句柄、删除临时文件、回收进程等资源责任。补偿是撤销业务结果，例如删除已上传对象；两者使用不同 API 和报告类别。

0.1 只内建 cleanup，不自动重试有副作用的业务步骤，不自动运行补偿。业务可以显式登记补偿为普通受管理操作，负责幂等性、权限、重复执行及失败处理；框架不把它宣传成事务回滚。

清理动作实现 OwnedCleanup，由 owner 构造独立 owned Future 并交给驱动服务；禁止借用已销毁的业务栈。cleanup 必须返回 Cleaned、Transferred 或 FailedWithRecovery，清理失败时仍持有恢复能力。只返回错误字符串而丢 owner 的适配器不符合协议。

清理不继承业务取消 token，但接受独立的服务关闭信号和收尾期限。期限到达后返回 Unconfirmed 快照，管理责任继续保留；不得强制 Drop 仍会阻塞析构的 owner。恢复注册表负责保留 owner，应用可 retry_cleanup、take_recovery 或显式 keep。重试只由策略/用户触发，默认不无限重试。

恢复 owner 占资源登记配额，应用不处理恢复会形成背压，这是可观察的状态，不以无界队列隐藏。take_recovery 需要类型化 RecoveryKey<R> 验证类型并单次取走；对外 observer 只有摘要，没有任意 downcast 与资源访问权限。

## 14. 作用域组合与失败传播

默认 FailFast：body 或已声明为 required 的子操作失败，停止启动后续子操作，向全部活动子操作（包括 optional）请求停止，等待它们释放，再清理。失败快速传播指快速请求停止，不承诺立即终结不可中断操作。

另提供 Collect：允许独立子操作继续，聚合结果，适合批处理。策略必须在 scope 创建时固定，不能由某个子操作临时改变兄弟行为。optional 子操作失败只进入报告；required/optional 是显式声明，不能根据错误类型猜测。

required/optional 在登记时固定。需要在 body 内捕获子错误并执行 fallback 的步骤应登记为 optional，由 body 显式映射其 typed 结果；required 的失败一经接受就触发父策略，事后读取或吞掉 E 不能撤销。Collect 只改变是否停止兄弟，required 失败仍禁止父成功；body 返回 Ok(T) 但存在 required 失败时发布 ChildFailed，并在内部锁外按非阻塞析构约定释放 T。

body 执行顺序继续使用普通 Rust；并行分支使用 `scope.join` 登记两个 owned 子 scope，join 全部释放后归并报告，不单纯依靠 future::join 的返回判定释放。0.1 支持顺序和有限并行，不提供自动重试、race first-success 和复杂路由 DSL。

父 scope 只有在全部子 scope 的终结责任已释放或明确转交后才能 Released。父取消传播给全部子 scope；子独立失败按父策略处理。部分结果保留归属的 operation ID，不覆盖更早的主业务错误。

## 15. Supervisor、驱动器与后台收尾

OperationSupervisor 是应用显式创建并持有的服务，clone 共享同一注册表；没有默认全局实例。它拥有控制协调线程、定时通知、操作状态和恢复注册表。协调线程只 poll 非阻塞 Future、更新状态、触发适配器，不执行同步文件/网络/推理代码。

默认 `PortableDriver` 使用标准 Waker 和有界就绪队列驱动不绑定外部运行时的 Send Future。同步 start_blocking 通过应用注入的 BlockingExecutor 承载；0.1 可提供固定容量 `ThreadExecutor`，不会为每个请求创建线程。领域已有 executor 时直接适配其控制/释放协议，不再包一层工作线程。

依赖 Tokio reactor 或特定线程上下文的 future，必须用 `ExternalDriver` 在对应运行时内执行。不能把它移到 PortableDriver 然后宣称兼容。ExternalDriver 返回受管理 task 控制和 driver 存活租约；应用必须先关闭 supervisor，再关闭 runtime。运行时提前销毁返回 DriverLost/Unconfirmed，不伪造资源已释放，也不把绑定 runtime 的 future 换到另一线程“继续执行”。

本版可提供 Driver trait 和测试用适配器，Tokio 等正式集成包单独验收；核心既不启动隐藏 Tokio runtime，也不对非 Send future 提供后台托管。0.1 所有可托管 body/清理都要求 Send + 'static。用户 body、结果 T/E 及其捕获值的普通析构还须遵守非阻塞约定；Rust trait 无法静态证明任意 Drop 不阻塞。需要耗时析构的领域 owner 必须由适配器转交 cleanup 协议，不能直接作为普通捕获值让协调线程析构。框架报告和诊断也不得在内部锁下释放任意用户值。

协调循环设置每轮 poll 预算，避免单个大量 ready 任务饿死取消与清理。自定义 Future 的 poll 仍必须遵守非阻塞契约；框架不能抢占违规阻塞的用户 poll。控制线程也不能实现一个公开任意闭包执行的逃生口。

## 16. 同步与异步 API 的共同机制

同步观察使用 Condvar/park 唤醒，异步观察使用 Waker；两者都读取同一个状态单元。结果赋值后先发布状态再唤醒，支持先完成后等待和先登记后完成。持锁期间不执行用户 Future、析构或领域 callback。

同步调用方可调用 start 提交异步 body，然后 result_blocking/released_blocking；前提是选择的 driver 能独立推进。异步调用方可以等待领域 executor 中的同步任务，但不能在 poll 中直接调用 result_blocking。

框架自己的协调/worker 线程调用会等待同一服务的阻塞接口时，返回 WouldDeadlock；外部任意异步运行时是否会死锁无法可靠检测，文档禁止在其驱动线程阻塞等待。同步-only 应用无需引入特定异步运行时，PortableDriver 由本库明确管理。

0.1 不提供跨取消边界托管非静态借用的 scoped thread/future。只在当前线程执行的同步闭包可以由应用自行组织，若不进入本库托管就不承诺 Drop 后收尾。未来若提供借用 scope，需要单独证明其取消与生存期安全。

## 17. Drop、分离与关闭协议

OperationHandle Drop：若 scope 未终结，请求停止并转交 supervisor；它不 Drop 领域 owner，不 join。observer Drop 不取消操作。detach 显式保留操作继续运行，observer 返回可用于查询；所有权仍在 supervisor 中，不脱离服务关闭管理。

丢弃 result/released 的等待 future 不取消 handle；便利 run_async 的内部 handle 随其 future Drop 请求停止。已确定业务结果但资源还未释放时，handle Drop 同样触发剩余收尾，不能因为有 T 就跳过清理。

shutdown 支持 Drain 和 Cancel，首先关闭新 root 受理，再处理已登记 scope，包括 detached 操作。Drain 允许已受理 root 按原配额继续登记其业务所需子操作，否则排队/执行中的工作无法完成；Cancel 关闭新子操作 commit 并向全部 scope 请求停止。两种模式均保留清理、释放确认及恢复操作入口。Drain 期间可单向升级为 Cancel，不能重新开放受理；并发关闭共享同一关闭状态。服务有两个观察入口 shutdown_async 与 shutdown_blocking，使用同一状态。等待期限只限制调用方等待：超期返回 Pending 清单及服务 observer，服务后台继续持有 owner。

最后一个公共 supervisor handle Drop 发出 Cancel，不阻塞。后台共享状态不得由自身 worker 的 Arc 形成永久环；活动任务持有内部 lease，结束后减少存活计数，只有控制关闭且活动计数为零时协调线程退出。native 永久挂起可能导致服务永远未完全释放，明确交给应用运维策略，不“强制成功关闭”。

## 18. 容量、准入和回收预留

建议初始默认值如下，属于目标 API 行为，不代表当前实现已经验证其性能：

| 维度 | 默认上限 | 说明 |
| --- | --- | --- |
| 活动 root scope | 4 | 不含只是排队的 body |
| 已受理排队 root | 64 | 不为排队项创建线程 |
| 等待受理者 | 64 | 超出立即 Busy |
| 每 root 子操作登记项 | 64 | 含已登记但尚未释放项 |
| 每 root 资源登记项 | 128 | recovery 继续占位 |
| 清理并发 | 4 | 不共享普通业务准入槽 |
| 未观察事件条数 | 256 | 有界并有溢出计数 |
| 未取业务结果条数 | 64 | 背压，不静默丢 typed 结果 |

scope 可在服务允许范围内申请不同登记配额。未取结果槽必须在 root 受理时预留，正常完成时不能再等待结果槽，否则已运行作业会因消费者不取旧结果而无法终结。取走或显式放弃结果才归还此槽；结果未取仍占槽但不占已释放的活动执行许可。默认 64 个结果槽同时约束全部已受理且仍需交付结果的 root，因此表中活动/排队上限不是可相加保证。嵌套结果从 root 预留的有限子操作配额切分。

受理时预留资源与子操作登记的存储/控制容量，取消和清理标记更新不要求普通任务队列有空位。嵌套 scope 从父配额切分，不为每个子 scope 再占一个 root 槽，避免父占满 root 后全部等待子任务准入。

T/E 和用户闭包可能拥有任意大小内存，核心不能在 Rust 中精确追踪其所有隐藏分配。因此默认上限约束登记与对象数量；业务 payload 必须由领域预算租约约束，服务不能声称总 RSS 被严格限定。未取结果保留在有界结果槽内，消费者不取会阻塞后续准入；supervisor 的显式 discard_result(id) 可放弃业务值但仍不放弃恢复资源，仍持有句柄的调用方随后得到 ResultDiscarded。handle Drop 或 detach 表示调用方放弃 typed 结果，服务可按配置释放 T，但先提取受控失败摘要；资源 owner 不随 T 一并销毁，必须事先登记到 scope。默认报告中记录结果已被放弃，不能当成已经被业务成功消费。

清理需要领域 worker 时必须具备该领域预登记清理通道/许可；本库清理并发槽不是领域 executor 一定有空闲的证明。适配器验收应证明业务队列满时取消可送达且当前不可中断调用退出后清理能推进。

## 19. 错误、事实与诊断

AdmissionError 表达 Busy、Closed、DeadlineExpired、ReservationLimit、DriverUnavailable；失败必须归还尚未启动 body/输入。ObservationError 表达 AlreadyTaken、ResultDiscarded、WouldDeadlock、InvalidObserver，不能混入领域业务错误 E。

领域失败保持 E。ChildSettlement 和 CleanupFailure 存储 operation/resource ID、阶段、类型化 source（Box<dyn Error + Send + Sync>）及受控摘要。框架 PanicSummary 不默认泄露任意 panic payload；catch_unwind 仅适用于 unwind，不能从 abort 或 native 崩溃恢复。

EffectRecord 记录领域声明的对象修改/发布/外部动作及 Known/Unknown 事实。框架不会验证远程实际状态；报告必须注明事实来源，不能将“停止请求已发送”记成“停止已完成”。

未观察事件进入有界诊断流，溢出只丢诊断并累计计数。typed 业务结果和 recovery owner 走独立有限注册表，不通过诊断队列传递。默认事件只含 ID、阶段、耗时和大小；路径、命令、内容和模型 features 不自动记录，调试标签必须经过调用方显式提供。

## 20. 与五个下游库的契约对齐

| 仓库 | 继续负责 | 可以共享本库的部分 |
| --- | --- | --- |
| rs-command | 进程树、管道、退出与部分输出、helper 释放 | context、控制句柄、scope 登记、终结观察 |
| rs-fs | provider 能力、版本与部分副作用、通用文件 SPI | 公共 context/handle、租约与 cleanup/recovery 协议 |
| rs-local-files | authority、发布、持久性、临时资源与恢复 | 资源登记、使用租约、cleanup 作用域和观察 |
| rs-mime | 输入范围、候选策略、file/ffprobe 组合与结果映射 | 组合 scope、总期限、取消传播与后台收尾 |
| rs-magika | 采样一致性、特征预算、Session 和推理容量 | 请求 context、不可中断操作登记、释放观察 |

文件 I/O、命令执行和模型推理保留独立 executor；不把所有工作塞到同一个线程池。本库 supervisor 的协调可替代各业务库重复的生命周期协调，不能替代领域控制线程和平台 reaper。

五份下游设计已在原 Agent 的后续讨论中调整为复用本库，并明确先实现、验收 rs-operation 再实施下游。本节以这些最终文档为对照，不再沿用各库独立实现通用 supervisor 的早期路线。应用注入唯一共享 supervisor；领域服务只管理自己的 worker/reaper 和受理范围，不关闭应用的共享服务。

| 设计基线 | 对接重点 |
| --- | --- |
| rs-command 0.12.0 | CommandControl；进程退出后 helpers 未释放仍持租约 |
| rs-local-files 0.6.0 | LocalIoControl；流/temp 访问 handle、发布事实、恢复 owner |
| rs-fs 0.4.0 | provider 中立的托管 SPI；不因 facade 包装重复登记 |
| rs-mime 0.22.0 | 组合子 scope；取消不触发 fallback，总期限不重置 |
| rs-magika 0.19.0 | 特征桥、Session 线程归属、native 推理真实释放 |

领域服务需要长期受理请求，不能把所有请求塞进一个默认 64 子操作的长期 root。服务受理组只标记归属并提供停止受理/批量观察，不执行 body、不持活动 root 许可；应用的独立请求各自申请 root，已有请求内的领域调用登记为子操作。服务组关闭与请求 commit 串行裁决，只停止该组受理及控制其登记的工作，不取消同一父请求下其他领域服务；公共父 scope 策略仍可按 required 失败传播。领域 Session 等长期资源由显式服务资源 scope 持有，租约不得越过关闭后的授权边界；跨请求使用通过同一 supervisor 的服务所有权授权，不新增任意 root 间 cleanup 依赖。此服务资源 scope 不占活动业务 root 许可，单独配置有限资源/恢复配额。

集成顺序为 operation → command/local-files/fs → 适配器与 mime → magika。每个领域操作只选择一个登记责任人：rs-fs-local 包装已纳管 LocalIoControl 时透传同一登记与租约，不能再次 reserve/cleanup 同一 owner。各库实施前固定本库验收版本或不可变提交；正式运行时适配器尚未通过验收的路径显式 Unsupported，不用隐藏 runtime 或阻塞 poll 补齐。

五份基线分别位于同组织对应仓库的 `doc/design-<上述版本>.zh_CN.md`。本次只修订本库设计，不修改下游实现或设计；跨仓库路径不作为本库文档构建的强制依赖。

## 21. 实现分层与交付顺序

1. 状态单元、ID、取消树、Deadline、同步/异步通知和结果一次性获取。
2. supervisor 准入、PortableDriver、阻塞执行器协议、关闭与后台生存计数。
3. 子操作两阶段登记、资源租约、有限依赖图和清理/恢复注册表。
4. 受管理顺序/并行子 scope、FailFast/Collect 和组合报告。
5. 服务受理组与长期资源 scope、Drain/Cancel 对等观察，用假文件/命令/模型适配器验证两类贯穿场景，再在领域仓库集成。

第一版不以实现丰富 DSL 为完成条件；必须先证明不变量。任何借用输入、特定运行时适配器或持久化能力都应单独设计，不能通过 undocumented 行为混入 0.1。

## 22. 验证矩阵

| 测试面 | 必需断言 |
| --- | --- |
| 正常组合 | 结果正确，全部资源恰好清理一次 |
| result 与 released 分离 | 可先收到业务结果，资源仍存活，稍后释放 |
| 等待取消安全 | 丢等待 future 后可重试取结果，不丢 T |
| handle Drop / detach | 前者请求停止，后者继续运行，均仍被服务关闭管理 |
| 启动竞态 | register/commit/drop/取消交错，不产生无主工作 |
| 多使用者 | 并行任务最后一个释放后才清理共享资源 |
| 资源图 | 自环、跨 root、环路被拒绝且归还 owner |
| 不可中断推理 | 取消后容量和 features 保留至 native 退出 |
| 部分副作用 | 发布后取消仍报告已发布 |
| 清理失败 | 主错误保留，recovery owner 可单次取回 |
| 容量满 | 通知/取消不丢、清理能推进、嵌套 scope 不死锁 |
| 驱动器丢失 | 返回 DriverLost/Unconfirmed，不伪造释放 |
| 关闭超期 | 可再次观察，owner 仍存在 |
| 同步异步对等 | 同一操作结果/状态一致，只等待机制不同 |
| 通知竞争 | 完成/登记 waker/取消并发无 lost wakeup |
| required 失败 | 业务失败通知早于 released；父 E 不匹配时发布 ChildFailed；Collect 仍禁止假成功 |
| 结果与取消竞争 | 首次裁决稳定，发布事实不被 Stopped 抹除 |
| 清理依赖失败 | 前置失败阻止后继清理，恢复组不丢 owner |
| 结果槽耗尽 | 受理前预留，终结不等待新槽，取走/放弃后准确归还 |
| 服务与 Drain | 长期服务不占活动 root；Drain 允许已有请求建子任务，Cancel 禁止 commit |
| facade 叠加 | fs-local/local-files 同一 owner 只登记和清理一次 |
| panic | unwind 请求进入受控失败与收尾，abort 不作恢复承诺 |

使用闸门、可控时钟、故障注入、线程计数与内存计数，不能用 sleep 猜测竞态。对注册/通知/释放状态机做确定性交错测试，必要时用模型检查工具作为 dev-only 验证。性能报告包括普通操作开销、登记项规模、唤醒次数、队列延迟、Drop 耗时及清理延迟，不在没有测量时承诺零成本或更快。

## 23. 评审通过与实现完成条件

设计评审应确认：两个贯穿场景不需要绕过协议；同步/异步不重复执行；平台未确认收尾能持续观察；资源所有权始终有去向；不可中断操作没有虚假保证；核心未侵入领域语义。

实现完成需通过核心契约测试和至少两个独立领域适配器的集成测试，提供可运行同步/异步示例、中文用户指南、错误与限制说明，以及依赖版本和 feature 矩阵。本次仓库初始化及此文档生成不意味着这些能力已经实现，也不授权开始重构其他仓库。

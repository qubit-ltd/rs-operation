# rs-operation 0.2.0 API 设计

- 状态：待评审设计，目标 API 尚未实现。
- 日期：2026-11-13；Cargo 包：`qubit-operation`。
- 基线：[0.1 设计](design-0.1.0.zh_CN.md)、当前实现及[中文用户手册](user_guide.zh_CN.md)。本文件定义 0.2 的目标契约，不以基线中的同名接口替代本版定义。
- 允许破坏性变更，不保留旧接口或兼容别名。本文不包含迁移和实施计划。

## 1. 设计范围与核心决策

本版统一操作的执行、停止、业务结果和资源收尾。设计必须处理以下情况：主体已退出，但其他线程、异步任务或后台调用仍在使用该主体创建的资源。

核心决策如下：

1. OperationManager 提供同步接口，AsyncOperationManager 提供异步接口，二者采用同一状态机。所有层级统一使用 try_submit/submit；scope.manager() 返回绑定当前操作的 manager，新提交自动成为该操作的子操作。
2. 每次操作有独立的 OperationScope，由框架创建并传给一次性业务闭包；scope 不实现 Clone，普通方法借用 &self。
3. 资源通过 scope.manage(T) 移交管理，返回 ResourceRef<T>。访问对象与使用引用是同一个对象，不另设无访问能力的租约。
4. 资源释放由两个独立条件共同决定：**所属资源作用域已结束 AND 使用引用数为 0**。
5. manage 使用 Rust 析构，T 不必显式实现 Drop；需要异步或可报告失败的清理时，通过独立策略扩展，不改变资源引用模型。
6. 框架不提供资源内容的独占访问或读写同步。它只同步内部登记、引用计数和生命周期状态。
7. 业务结果、实际停止使用和资源清理结果分别建模，不能用一个“完成”状态代替。

## 2. 对象关系与所有权模型

### 2.1 对象职责

| 类型 | 设计职责 | 复制与权限 |
| --- | --- | --- |
| OperationManager / AsyncOperationManager | 准入、执行环境选择、操作登记、整体关闭 | 可 Clone；共享管理核心，并保留绑定的提交位置 |
| OperationScope | 单次操作的资源与子操作管理权限 | 不可 Clone；Send + Sync；方法借用 &self |
| OperationHandle<T, E> / AsyncOperationHandle<T, E> | 唯一业务结果接收权，以及停止和观察权限 | 不可 Clone；同步/异步转换消费原句柄 |
| OperationObserver / AsyncOperationObserver | 只读状态和收尾观察 | 可 Clone；无 T/E，不接收业务结果，不发停止请求 |
| ResourceScope | 可显式结束的嵌套资源所属范围 | 不可 Clone；end/Drop 结束持有，不阻塞清理 |
| ResourceRef<T> | 对 T 的共享访问和一份使用持有 | Clone 增计数，move 不增计数，Drop 减计数 |
| ResourceKey<T> | 资源身份与类型凭据 | 可 Copy/Clone，不持有 T，不增加使用计数 |
| AsyncCleanup<T> | 普通析构之外的可报告清理策略 | 由框架保留，可在失败后再次使用 |
| RecoveryHandle | 恢复组观察与显式重试权限 | 可 Clone，Drop 不丢失恢复组 |

### 2.2 scope 的身份和生命周期

OperationScope 不是线程、执行器或锁，而是一次操作的管理入口。每次成功提交建立新的 OperationId 和内部操作记录，闭包执行时接收关联的 scope。一次提交的 F 至多调用一次；等待重试、结果观察和恢复操作都不重新调用 F。

scope 的生命周期状态由执行帧维护，不由 scope 对象的引用数维护。主体返回、unwind 完成或 async future 实际销毁后，默认资源作用域结束。将 scope 对象移出主体，不能延长其原主体的开放期；主体结束后，旧 scope 不能登记新资源，通过其 manager 也不能再提交新操作。

manage/get_resource/manager 方法都借用 &self，可以在同一次操作中多次调用。辅助代码借用 &OperationScope；独立后台工作持有 ResourceRef。受管子操作拥有新建的子 scope，而不是父 scope 的 clone。

ResourceScope 是资源所属范围，不是另一项业务操作。其 owner end/Drop，或父主体实际结束，都会结束该范围。ServiceResourceScope 的结束由服务关闭协议控制，不占永久业务 root。

### 2.3 资源所有权

资源记录包含三个相互独立的部分：所属范围的持有、使用者引用、清理状态。manager 和 observer 可以持有元数据，但不能因此永久占用一份使用引用。

```text
操作执行帧 ──控制──> 默认资源作用域 ──持有──> T
                                             ↑
                              ResourceRef<T> ─┤ 使用引用 1
                              ResourceRef<T> ─┘ 使用引用 2

范围结束：归还所属范围的持有
最后一个使用引用归还：用户计数变为 0
两项都完成：T 的唯一所有权交给清理执行环境
```

可以使用 Arc 实现共享持有，但不得用混合了元数据和观察者的 strong_count 推导使用者数量。计数归零与范围结束必须通过一次性状态转换取得唯一清理权。

### 2.4 资源访问与并发边界

ResourceRef<T> 实现 Deref<Target = T>，只提供共享 &T。T 要求 Send + Sync + 'static，框架不提供 DerefMut、into_inner 或可升级的 Weak。

get_resource(key) 在同一个开放的所属范围内验证身份、代次、类型和权限，原子增加计数并返回另一份 ResourceRef。使用计数曾为 0 时，只要范围还开放，仍允许获取；范围结束后禁止获取，既有引用仍可访问和 clone。

多个线程可通过有生命周期约束的 &OperationScope 借用获取同一资源，也可分别持有 ResourceRef。**取得引用不意味着独占资源。** Mutex、RwLock、原子操作及领域同步由 T 或应用提供；框架不会在整个使用期间隐式加锁。非 Sync 的原生会话应通过线程安全控制端口访问，实际对象保留在线程内。

### 2.5 父子资源归属与清理顺序

父操作有自己的默认资源范围，子操作有新的默认资源范围。子操作可以持有父资源的 ResourceRef，也可以 manage 自己创建的资源。持有父资源引用不改变资源归属，不在子范围重复登记同一对象。

父主体返回，只结束父主体默认资源范围的持有，不表示所有子操作已结束。父范围内的每个资源分别按“该所属范围已结束 AND 该资源使用引用为 0”判断能否清理；**不额外要求全部子操作结束，也不要求全部子资源已经释放。**

若子操作仍持有父资源 P 的引用，P 必须继续保留。若子操作已归还 P，但仍在处理自己的资源 C，P 可以先进入清理。P 不应被与它无关的子资源清理或恢复拖住。如果清理 C 本身也需要 P，C 或其清理状态就必须继续持有 P 的 ResourceRef，直到清理不再访问 P；此时 P 的计数仍非零，自然不能提前清理。其他必要释放顺序须由显式依赖约束表达，不能从父子关系自动推断。

父操作的整体 settlement 则需要自己的资源与全部子操作的收尾结果都已确定。子报告为 RecoveryRequired 或 Transferred 时，父报告要保留并汇总这些事实，而不是要求所有子资源都已物理释放才能发布报告。父资源已经清理，与父整体收尾尚未完成可以同时成立。

因此本文固定区分“父主体退出”“父默认资源范围结束”“单个父资源清理”和“父操作整体收尾完成”，不把它们统称为 scope 结束。

### 2.6 父子操作关系的状态契约

父子关系在子提交成功时建立，并在父主体退出后继续存在。每个操作只有一个直接父操作，根操作没有父操作；不同 scope 不共享资源所有权，只能通过 ResourceRef 显式共享访问。父子关系同时承担停止传播、必要子结果汇总和收尾汇总三项职责，不自动建立资源之间的清理依赖。

| 事件 | 对直接子操作和后代的约束 | 对父资源的影响 |
| --- | --- | --- |
| 父主体正常返回 | 已接纳子操作继续运行；仍开放的子主体可提交后代 | 父默认资源范围结束，逐资源检查使用引用 |
| 父成功结果发布 | 必须已满足全部 Required 子操作的业务成功；Optional 可以仍在执行 | 不表示父子资源均已清理 |
| 父请求停止被接受 | 向后代传播停止，阻止之后的新子提交；已接纳工作按真实退出状态收尾 | 不撤销已有 ResourceRef，不提前清理 |
| 单个子操作被停止 | 传播到该子树；不会直接取消其祖先 | 归还引用仍以实际退出为准；Required 失败可按父策略影响父与兄弟 |
| 子主体结束 | 结束它自己的默认资源范围，不结束父范围 | 它持有的父资源引用在实际销毁时归还 |
| 子 settlement 发布 | 父级记录子报告；子报告可以包含恢复或转交 | 父资源依旧逐个判断，不等全部子资源物理释放 |
| 父 settlement 发布 | 自身收尾与所有已接纳子操作的稳定报告均已具备 | 报告保留仍需恢复或已转交的责任 |

Required/Optional 只决定子业务失败是否阻止父成功，不改变资源责任。停止一个子操作后，Required 失败可能经父失败策略触发兄弟停止；这属于失败处理，不是子停止请求无条件向上传播。父 handle 的 Drop 与 into_observer 仍按第 3.4 节处理，不能把丢弃 handle 当作脱离父子管理的方式。

## 3. 提交接口及结果契约

### 3.1 同步和异步外观

同步 manager 接收 `FnOnce(OperationScope) -> Result<T, E>`；异步 manager 接收返回 owned Future 的闭包。F、future、T、E 均满足相应的 Send + 'static，T/E 不要求 Clone。

| 接口 | 同步外观 | 异步外观 | 返回含义 |
| --- | --- | --- | --- |
| try_submit | 立即返回 | 普通方法，立即返回 | 已接纳的操作句柄，或保留 F 的拒绝 |
| submit | 阻塞等待准入 | async 等待准入 | 取得句柄，不等待业务完成 |
| run | 阻塞等待业务结果 | async 等待业务结果 | OperationOutcome |
| run_and_settle | 阻塞等结果和收尾 | async 等结果和收尾 | OperationCompletion |

同步接口不等于原地执行闭包。闭包由配置的同步执行器运行，调用线程等待的是准入或结果。异步 try_submit 本身不返回 Future，只有主体及后续等待为异步。

单独调用两个 new 创建独立核心；as_async/as_sync 只建立同一核心的另一外观，共享容量、状态和关闭，并保留相同的提交位置。句柄的 into_async/into_sync 消费原对象并转移唯一结果权，不执行取消逻辑或重跑主体。

### 3.2 统一提交入口与父级绑定

OperationManager 和 AsyncOperationManager 都包含同一管理核心与一个固定的提交位置。new 创建根提交位置；scope.manager() 返回同步 OperationManager，绑定 scope 对应操作为后续提交的直接父级。异步代码使用 scope.manager().as_async()。不论闭包位于第几层，try_submit、submit、run、run_and_settle 的签名与选项均相同。

| 创建或转换 | 新操作的父级 | 核心与容量 |
| --- | --- | --- |
| OperationManager::new / AsyncOperationManager::new | 无，提交根操作 | 新建核心 |
| scope.manager() | 该 scope 对应的操作 | 共享原核心与祖先额度 |
| manager.clone() | 与原视图相同 | 不增加容量 |
| manager.as_async() / as_sync() | 与转换前相同 | 不改变执行归属 |

归属只由提交位置决定，不根据线程、调用栈或异步任务推断。相同辅助函数接收 &OperationManager，使用统一的 try_submit，就能由调用方传入根 manager 或任意层级 scope 的 manager。把原始根 manager 捕获进操作后提交，仍会创建独立根操作；这项工作不会自动继承当前操作的停止、预算和收尾关系。

scope.manager() 返回一个 owned 视图，不移动 scope，也不借用其栈帧。重复取得、复制或转为异步视图，都不延长父主体的开放期、不持有资源使用引用。创建视图本身不做准入，即使主体已结束仍可取得视图用于观察核心；之后的提交必须检查父记录是否仍开放。父范围已结束或父记录已移除时返回 AdmissionError::ScopeEnded，停止已接受时返回 Stopped；不能退回根提交位置，也不能重新打开父范围。

父状态检查、容量预留和子关系建立必须与父范围关闭、停止接受串行裁决。submit 在等待期间同样受这些约束；父关闭或停止后要退出等待并返回未执行闭包。两个方法都只表示准入，返回 handle 后由新主体接收新的 scope。这个新 scope 的 manager 绑定新主体；继续使用旧 manager 提交则仍是原父级的另一个孩子。

OperationOptions.requirement 统一表达新操作与父级的 Required/Optional 关系，默认 Required；根提交没有父级，因此该字段不参与根结果判断。其余 limits、context、child_failure_policy 分别约束新操作的子树、上下文和它自己的必要子失败处理，不因提交层级改变含义。

绑定只影响提交位置。snapshot、close_admission、request_shutdown 始终作用于共享 manager 核心，其中 close_admission 按 Drain 语义关闭新根准入；request_shutdown(Cancel) 还停止已接纳树并禁止新子提交。局部操作的停止和观察使用 scope/handle/observer。as_async/as_sync 不改变该管理范围，不能把父绑定视图的全局 shutdown 解释为只关闭该子树。

### 3.3 准入与执行责任交接

成功接纳前必须预留结果槽、控制状态和适用的执行许可；根位置另预留 root 名额，父绑定位置另预留祖先链的子名额。异步根任务可以进入有限排队区；同步根任务要求同时取得活动 root、结果槽和同步执行器许可，执行器自身可有有界队列。

try_submit 不能等待空位。submit 等待者有界，等待期间不持有部分许可形成循环。取得多个许可失败时回滚。SubmitError<F> 原样保留未调用的 F；成功接纳后发生的驱动故障进入 ExecutionFailed，不能再声称主体尚未执行。

异步 submit 未 poll 时无副作用，准入前 Drop 退出等待并归还预留。已接纳但句柄未交付时发生 Drop，则执行 handle Drop 协议，不留下无主任务。

### 3.4 业务结果和观察权限

OperationOutcome 表示业务成功、领域失败、必要子失败、停止、panic 或执行故障。wait_outcome 消费一次结果；重复读取报 AlreadyTaken。等待 future 被丢弃不消耗结果，原 handle 仍可重新等待。try_take_outcome 未就绪返回 None，已取走返回错误。

结果发布与停止接受在同一状态单元裁决。成功须主体和全部 Required 子业务均成功；失败可先发布，后续子事实进报告。已发布结果不可修改。副作用独立保留，不因停止抹除。

into_observer 放弃结果交付，但不发停止请求。handle 未完成时 Drop 请求停止并放弃结果，不阻塞、不直接销毁资源。observer 无控制权限，其 Drop 只丢观察入口。

### 3.5 outcome 与 settlement 的区别及组合契约

outcome 是业务执行结果，回答“操作做成了什么”；settlement 是资源收尾报告，回答“相关子操作和资源收尾到了什么结果”。二者关联同一个 OperationId，但分别发布、分别观察，不能相互推导。

| 维度 | outcome | settlement |
| --- | --- | --- |
| 类型 | OperationOutcome<T, E> | SettlementReport |
| 内容 | 成功值 T、领域错误 E、必要子失败、停止、panic 或执行故障 | 子操作收尾、资源释放证据、清理失败、转交和恢复记录 |
| 完成条件 | 业务结果已确定；成功还须满足全部 Required 子业务成功 | 本轮涉及的全部责任已按策略完成，或已可靠保存为恢复组/转交，并形成稳定报告 |
| 访问权限 | handle 独占结果接收权，取出一次 | handle/observer 可重复观察报告 |
| 后续变化 | 发布后不被后到停止或清理失败覆盖 | 历史报告不回写，后续恢复由恢复句柄另行查询 |
| 不保证的事实 | 不能证明资源已释放 | 不能证明业务成功，也不能仅凭报告存在证明物理释放 |

同步 wait_outcome 返回 Result<OperationOutcome<T, E>, OutcomeError>，同步 wait_settlement 返回 Result<Arc<SettlementReport>, ObservationError>。异步 wait_outcome 仍返回带 OutcomeError 的结果，异步 wait_settlement 直接返回 Arc<SettlementReport>。观察错误不是领域 E，也不是清理失败本身。

以下组合均合法，实现不得将其合并为单一成功/失败状态：

| 业务结果 | 收尾结果或进展 | 必须保留的事实 |
| --- | --- | --- |
| Succeeded | Released | 业务成功，资源按所选清理策略完成 |
| Succeeded | RecoveryRequired | 成功值保留，另有清理失败和恢复对象 |
| Failed / ChildFailed / Panicked | Released | 失败原因保留，资源已完成清理，不把业务改为成功 |
| Stopped | 尚在等待 | 停止结果可以先交付；不可中断使用者、引用或清理仍可能未结束 |
| 任一已确定 outcome | Transferred | 接收方已签收资源责任，不表示原生资源已物理释放 |

wait_settlement 完成只表示“自动收尾报告已发布”。Released、Transferred、RecoveryRequired 都可以产生报告，应用必须再解释 status、证据和恢复记录。Unconfirmed 是仍缺释放证据的进展，不是 SettlementStatus 的稳定终态；此时无期限等待继续 Pending，带期限观察可返回当前快照。

两次等待是独立观察，不保证有可见时间间隔；读取 outcome 时 settlement 可能已经就绪。未读取 outcome 一般不阻止后台清理，但若 T 自身持有当前操作的 ResourceRef，结果槽也算使用者，必须先取出并结束该资源使用，再等待其收尾。

这两种结果都由框架发布，等待方法只是观察入口，不是“读取一次 outcome 才开始清理”。关闭完成又是另一层条件：RecoveryRequired 报告可以已经返回，但管理器仍需等待恢复解决或可靠转交，不能据此直接完成 shutdown。

## 4. 资源 API、状态转换与清理契约

### 4.1 manage 的唯一基础语义

`manage<T>(&self, resource: T)` 接管一个已经存在的 T，返回 ResourceRef<T>。它不创建文件、会话或缓冲区，不执行 T 的业务方法，也不要求 T 显式实现 Drop。Rust 自动析构字段；类型需要额外释放行为时，才显式实现 Drop。

成功时建立所属范围持有和第一份使用引用。失败时不接管 T，ManageResourceError<T> 保存原值；错误被丢弃时，原值也按 Rust 规则析构。需要回收或恢复的失败资源通过错误接口取回处理。

默认 manage 的最终动作是执行 T 的普通析构。析构被安排到独立、有界的阻塞清理环境，不在最后一次 ResourceRef Drop、scope end、协调线程或内部锁中执行任意用户析构。

### 4.2 两个释放条件

| 所属范围状态 | 使用引用数 | 允许的状态转换 |
| --- | ---: | --- |
| Open | 0 | 保留 T，允许经 key 获取引用 |
| Open | 大于 0 | 保留 T，允许使用和获取引用 |
| Ended | 大于 0 | 等待使用者，拒绝 key 获取，既有引用有效 |
| Ended | 0 | ReadyForCleanup，等待依赖和执行容量 |

ReadyForCleanup 通过唯一裁决进入 Cleaning，不可返回 Open。成功后按释放证据记录 Released；失败保留恢复能力，或在无法确认时保持 Unconfirmed。

clone 共享同一个 T，不要求 T: Clone；move 不增计数；Drop 归还一份。作用域结束和最后一次 Drop 的任意先后、并发都只能触发一次清理。复制路径或裸句柄不会自动增加计数，实际使用者必须继续持有 ResourceRef。

scope 结束前引用归零不释放资源；scope 结束后仍有引用也不释放。引用环、mem::forget、永久后台工作可能使引用不归零，框架提供诊断而不实现循环垃圾回收。

### 4.3 获取、转移和依赖

get_resource 与 end 原子裁决：获取先成功则新引用保护 T；end 先发生则失败。停止禁止新的 key 获取与资源登记，但不撤销既有引用。已有引用的 clone 不会复活已经进入清理的资源，因为复制者本身就是有效持有者。

ResourceKey 只包含身份、代次和类型凭据，不持有 T。ResourceId 用于报告和依赖，不独自授予访问权。转交后只有目标所属范围可通过 key 获取。

cleanup_after(resource, prerequisite) 设置清理先后，检查自环、循环和范围边界；不能代替各自的两项释放条件。前置失败时后继不能启动，应保留完整恢复组。

transfer_resource 原子转移完整依赖组的 scope 持有。先检查目标开放与容量，再改变归属；失败源不变。同一 manager 中 T、使用计数和资源 ID 保持稳定，释放条件改为等待目标范围结束。已经清理或恢复的对象不得通过此接口重新开放。

### 4.4 创建预留与部分失败

创建过程可能产生需清理的部分状态时，先 reserve_resource，再执行领域创建。commit 接管 T；停止或范围结束导致 commit 失败时，错误归还 T 和 reservation。

retain_for_cleanup 使用已预留容量，接管被拒绝或部分创建的资源，归到已结束的拒绝清理范围，使用计数初始为 0。该责任交接不能因原业务已停止而再次拒绝。未创建任何资源时，reservation 可直接 Drop。

### 4.5 异步清理扩展

manage_with(T, C) 的资源访问、引用计数和结束条件与 manage 完全相同，只把默认析构策略替换为显式 AsyncCleanup<T>。T 不是为了管理而必须实现某个清理 trait；C 才实现异步策略接口。

取得唯一 T 后，框架将它和独立 CleanupContext 交给 C。策略返回 Released、带回执的 Transferred，或包含原 T 的 Failed。框架独立保留 Arc<C> 供重试，失败不能只返回字符串而丢失 T。

T 的最终 Drop 仍会执行，因此异步策略和 Drop 不得各自无条件释放同一外部资源。策略完成可失败的释放协议，Drop 负责剩余本地状态或明确幂等的后备动作。

普通析构只能证明 DropCompleted，不能证明 Drop 内吞掉的文件删除错误。异步策略提供外部 Confirmed 证据。报告保留证据来源，不能将析构结束宣传为已验证外部状态。

需要清理 panic 后恢复的适配器必须另外持有真实 owner/capsule。框架无法从已移入 future 后损失的唯一对象重新构造资源；缺少证据时进入 Unconfirmed。

## 5. 停止、执行与整体收尾

### 5.1 停止及上下文

request_stop 只请求停止，不伪造主体退出或使用引用归零。OperationContext 共享原因、期限和预算；stop_requested 等待请求接受，不表示工作停止完成。

同步不可中断闭包仍运行时，其默认资源范围仍开放。异步主体只有在 future 实际完成或销毁后，默认范围才结束。已独立开始的后台工作持有自己的 ResourceRef，直到真正不再访问资源。

new/child 验证时钟与期限并返回 Result。子期限只能收紧，父子预算同时检查。Deadline 属于一个单调时钟域；ClockSource 的注册必须处理注册期间到期竞争并正确唤醒。观察超时不改变业务期限，清理使用独立上下文。

### 5.2 子操作和适配器

scope.manager().try_submit 将同步后台工作登记为当前操作的子操作，scope.manager().as_async().try_submit 对应异步主体；submit 提供相同位置的等待准入形式。与直接调用函数不同，子操作有独立 scope、业务结果、停止控制和收尾记录；父级按 Required/Optional 与失败策略跟踪它。普通顺序函数继续通过 &OperationScope 复用当前范围，不要求每次函数调用都创建子操作。

子操作可以再提交子操作，每次都得到自己的 scope。只有发起提交的那段业务代码还在运行、没有收到停止请求且有剩余名额时，才能继续创建。

父业务函数返回，不会自动取消已经提交的子任务。例如父函数创建 B 后返回，B 还在运行，B 就仍可以通过自己的 scope 创建 C；B 不能再通过已经结束的父资源范围获取新引用，只能使用提前取得的父资源引用或管理自己的资源。

整次请求及它创建的任务共用一套名额。例如一次请求允许登记 64 个子操作，那么 B 和 C 都从这 64 个名额中分配，不是 B 创建后又多出一套 64 个。后代也不再申请新的根操作名额。

如果调用方停止整个请求，框架会通知 B、C 等已经创建的后代，并拒绝继续创建新任务。正在执行且无法中断的调用仍要等实际返回，不能因此提前释放引用。正常返回和请求停止是两种不同事件。

必要子失败按父子关系逐级影响成功结果；ContinueSiblings 只允许兄弟继续执行，不将必要失败改成成功。Optional 的结果由业务决定降级。同步子主体不得在同一执行服务内阻塞等下一层，应由框架汇总，或在异步主体中等待。放弃 typed 结果但保留运行应使用 into_observer，普通 handle Drop 会请求停止。

领域已有执行器采用 reserve_child → attach(control) → commit，获得 ChildStartPermit 后激活暂停任务。执行状态本身持有 ResourceRef，不再附加独立租约。

commit/stop 串行裁决，未消费启动凭证执行 abandon，确认未启动即终止。激活和放弃均至多一次且非阻塞。control 分别观察业务、不再使用输入以及子资源收尾，不能用信号已发送代替真实退出。

PortableExecutor 只推进正确唤醒、非阻塞的 Send future。同步工作使用有界 ThreadPoolExecutor 或领域池。线程绑定对象通过安全控制端口操作；runtime 丢失时保留输入引用和 Unconfirmed，不把绑定对象移到另一线程继续执行。

### 5.3 收尾、恢复和关闭

wait_settlement 等待本轮自动收尾产生稳定报告：按策略释放、责任被接收方签收，或恢复组已可靠保留。仍有使用者或状态未确认时继续 Pending，带期限接口只返回快照，不丢责任。

恢复以依赖组为单位。try_retry 显式接纳一次有界重试，并发得到 AlreadyRunning；失败继续保留 T 与策略。原报告不可回写，后续状态通过 RecoveryHandle 查询。

RecoveryStore 接管需整组和回执。源 manager 内未解决组阻止关闭，独立 store 签收后解除源责任；store 自身继续持有未解决对象，不因公共观察句柄消失而静默析构。

OperationGroup/AsyncOperationGroup 管理一组独立操作的准入和关闭，不创建永久 root。ServiceResourceScope 持有跨请求资源，关闭时结束范围持有，等待所有引用归还。

Drain 关闭新 root，允许已接纳操作在额度内完成子工作；Cancel 进一步禁止新 commit 并传播停止。可升级 Cancel，不可重新开放。关闭句柄可继续观察，超时不强杀调用、不销毁 owner；未取业务结果仍占结果槽，但不阻止已完成资源的关闭。

### 5.4 容量与内部同步

#### 5.4.1 默认配置与限制范围

64 是默认配置值，不是写死的上限，也不是根据性能测试确定的最佳值。ManagerLimits 配置 manager 的容量，ManagerOptions.scope_limits 配置每个根操作的额度上限；应用应根据负载调整这些值。OperationOptions.limits 可以在该上限内进一步收紧一次请求的额度。

初始默认值为：活动 root 4、排队 root 64、准入等待者 64、未取结果 64、清理并发 4、诊断 256；每 root 子操作 64、资源 128、副作用 64。整棵子操作树共享根操作的额度，创建子 scope 不会增加容量。不同限制分别记账，不能把“允许 64 个子操作”理解为“每种资源都允许 64 个”。

额度按以下范围检查。任何局部额度都不能放宽其祖先或 manager 的限制；有效上限按对应字段取配置值与继承上限的较小值。OperationOptions.limits 的默认值也参与这一规则，因此父级收紧到 2 时，子级默认的 64 不会导致额外取得 64 个名额。

| 限制 | 计数范围 | 子提交是否占用 |
| --- | --- | --- |
| ManagerLimits.active_operations / queued_operations | manager 下的根操作 | 不申请新的 root 名额；子主体仍需执行器容量 |
| ManagerLimits.admission_waiters | manager 下等待根或子准入的调用 | submit 共享等待者上限，try_submit 不登记等待者 |
| ManagerLimits.cleanup_concurrency | manager 下实际运行的清理步骤 | 父子清理共享，不按子树扩容 |
| ManagerLimits.unconsumed_outcomes | manager 下所有根操作和子操作的 typed 结果槽 | 每个返回 typed handle 的子操作也预留一个槽 |
| ScopeLimits.child_operations | 当前操作的全部后代，当前操作自身不计入 | 每个后代同时受根及路径上各祖先的子树上限约束 |
| ScopeLimits.resources | 当前操作及其全部后代登记的资源 | 父、子、孙的资源合并检查；共享同一 ResourceRef 不重复登记 |
| ScopeLimits.effect_records | 当前操作及其全部后代保留的副作用记录 | 受相同的祖先额度约束 |
| ResourceScopeOptions.capacity | 一个局部资源范围 | 另受祖先资源额度约束，不能借新建范围增加总额 |

例如根额度为 2：根提交 B 占一个子名额，B 提交 C 再占一个根子名额，同时占 B 的一个后代名额。B 再提交 D 时，根池已经没有名额，即使 B 的局部池仍有余量也不能接纳。各子树共享根池，同时可以有更小的局部上限；不在创建子 scope 时预扣其声明的全部上限。

这些限制约束尚未退还的容量，不是请求一生最多执行多少次。子名额在子操作已有稳定 settlement，且 typed 结果已取走或明确放弃后归还；仅主体返回、结果可用或 handle 被销毁，都不能跳过尚未完成的收尾。父级保留有界的汇总事实，不以无限保留完整已结束子记录的方式消耗内存。业务若要限制一生累计提交次数，应另用 ResourceBudget 消耗记账。

try_submit 在任意提交位置遇到临时容量或执行许可不足时返回 AdmissionError::Busy，不等待；配置无效或请求要求无法满足的容量约束返回 CapacityExceeded。manage 的资源登记额度不足返回 ResourceManageErrorKind::CapacityExceeded，错误仍拥有原资源。上述失败都不建立部分父子关系或部分资源登记。子准入失败时 SubmitError<F> 保留闭包，若闭包捕获资源引用，这些引用也随闭包保留，直到调用方重试、取回或放弃它。

#### 5.4.2 使用 rs-budget 管理容量

0.2 设计采用 rs-budget 的 ManagedResourcePool 和 ManagedResourcePermit 管理可归还的容量，避免自行维护同类加减计数逻辑。ManagerLimits 和 ScopeLimits 继续作为配置接口，内部根据配置创建容量池；本设计的依赖为 qubit-budget = "^0.10"。

| 需要控制的量 | 使用的 rs-budget 类型 | 含义 |
| --- | --- | --- |
| 活动任务、排队位置、结果槽、资源登记位置 | ManagedResourcePool | 申请得到 permit，所代表的占用结束后归还 |
| 应用限制的模型内存、连接数等占用 | ManagedResourcePool | permit 随实际资源存活，不随操作主体返回而归还 |
| 一次请求累计处理的字节数、调用次数 | ResourceBudget | 消耗后不归还；共享使用时由应用同步访问 |
| 单次输入大小等独立测量 | ResourceLimit | 只校验这次测量，不记录累计占用 |

复制 ManagedResourcePool 只会复制指向同一容量池的句柄，不会复制额度。父操作和后代使用同一个根额度池；OperationScope 仍不需要实现 Clone。rs-budget 不负责操作树、等待队列、公平调度或停止传播，这些仍由 rs-operation 协调。

一次准入需要多个名额时，rs-operation 必须先取得所需 permit，再发布已接纳状态。任一申请失败，都归还本次已取得的 permit，不能留下部分登记。rs-budget 的单池 try_acquire 不等于多池原子准入；等待者登记、容量归还通知和取消竞争也由 rs-operation 处理，避免检查容量后错过唤醒。

#### 5.4.3 额度归还必须对应实际生命周期

等待准入的调用在成功接纳、被拒绝或取消等待时归还等待者名额；根、子提交共享同一等待者池。不得为了等待子准入而部分占用多个许可，能够确定等待依赖当前主体或祖先退出时返回 WouldDeadlock。异步等待不占用 worker，但仍可能存在逻辑上的容量依赖；调用方应限制扇出并配置期限，不能假设 await 可以消除这类依赖。清理并发许可在一次清理步骤实际结束后归还；进入恢复的资源仍占资源登记额度，但不永久占用一个清理 worker。诊断事件容量只限制诊断保留量，不能因诊断已满而丢弃清理或恢复责任。

根操作从取得活动名额开始，到自身及整棵子树形成稳定 settlement 为止，一直占用该活动 root 名额；仅根主体返回不归还。因此活动 root 上限约束尚在执行或自动收尾的请求树，而非执行线程数。报告进入 RecoveryRequired 后，恢复对象仍保留其资源登记及业务预算，不能因活动名额归还而一并释放。

不同 permit 由不同的内部对象持有：结果槽在接纳前预留，读取或放弃结果时归还；排队位置在出队或撤销时归还；资源登记额度在该登记不再承担清理或恢复责任时归还。资源进入恢复状态后仍占资源额度。结果发布不能临时等待新的结果槽，取消、提交裁决、释放和清理通知也不能依赖普通业务队列空位。

业务资源预算与框架登记额度分开管理。例如应用从模型内存池取得 permit，再将 permit 和模型放入同一个资源 struct，通过 scope.manage() 登记这个 struct。ResourceRef 的复制只增加使用引用，不再次扣减模型内存。只有资源所属范围结束且使用引用归零后，框架才安排销毁；实际模型释放后才归还对应 permit。资源 struct 的字段销毁顺序或清理实现必须保证这一顺序。

需要异步清理或外部释放确认时，permit 必须一直保留到释放得到确认；清理失败后由恢复对象继续持有。转交资源时，permit 随资源一起转交，不能因源 manager 已收到转交回执就归还全局资源池的容量。源 manager 的登记额度与实际资源的预算 permit 是两种独立责任。

ContextOptions.budget 中的 BudgetPolicy::check() 只是附加策略检查，不申请或归还容量，也不能代替 permit。对于并发的资源申请，必须使用容量池的 try_acquire 完成检查和扣减，不能先 check 再另行扣减。这些数量与显式预算只能约束已登记的占用，不自动构成进程总 RSS 上限。

不在状态锁内 poll 用户 future、调用领域控制代码或析构用户值。库 worker 中相互依赖的阻塞等待返回 WouldDeadlock。执行器关闭显式等待，普通 Drop 不 join worker。后台责任计数不得形成无任务却自持的永久环。

## 6. 设计验证场景

本章的代码是接口组合与状态验证，不是入门教程。场景中的领域函数明确由适配器提供；设计先满足前述契约，示例再验证契约可表达。

### 6.1 后台持有与父主体结束

```rust,ignore
let operation = manager.try_submit(options, |scope| {
    let input = scope.manage(input_resource)?;
    let background_input = input.clone();
    let child = scope.manager().try_submit(OperationOptions::default(), move |_child_scope| {
        backend.consume(background_input)
    })?;
    child.into_observer();
    Ok::<_, SubmitBusinessError>(receipt)
})?;
```

backend.consume 接收 owned ResourceRef，保留到真实使用结束。验证交错：父返回使所属范围 Ended，后台引用仍为 1，不启动清理；后台退出归还最后引用后才转 ReadyForCleanup。必要子业务成功仍是父成功 outcome 的前置，与父主体结束时刻分开。

### 6.2 不可中断推理

模型 fixture 的接口为：`predict_owned(ResourceRef<Features>, Sender<()>) -> Result<Prediction, ModelError>`。started 通知表示适配器已经不可撤回地进入执行阶段；引用持续持有到 native 调用及 helpers 结束。

```rust,ignore
let (started_tx, started_rx) = std::sync::mpsc::channel();
let mut inference = manager.try_submit(options, move |scope| {
    let features = scope.manage(feature_vector)?;
    let native_input = features.clone();
    let child = scope.manager().try_submit(OperationOptions::default(), move |_child_scope| {
        model.predict_owned(native_input, started_tx)
    })?;
    child.into_observer();
    Ok::<_, PrepareError>(request_id)
})?;

started_rx.recv()?; // 应用线程，不是执行同一服务的 worker。
inference.request_stop(StopReason::UserCancelled);
let outcome = inference.wait_outcome();
let settlement = inference.wait_settlement();
```

PrepareError 负责资源登记和子提交错误转换，模型 fixture 由领域测试提供。验证取消后 outcome 可先成为 Stopped，但输入仍存活、执行许可仍占用；native 返回后引用归零，才允许清理。结果发布若先于停止，保留已发布结果。执行池需有运行父子任务的容量，测试使用闸门和通知，不用固定 sleep 推测交错。

### 6.3 引用先归零与再次获取

```rust,ignore
let range = scope.resource_scope(resource_options)?;
let key;
{
    let input = range.manage(resource)?;
    key = input.key();
}
let input_again = range.get_resource(key)?;
```

验证第一次引用归零时 range 仍 Open，T 未析构；获取成功再次增计数。end 与 get 并发时只允许“返回有效引用”或“ScopeEnded”之一，不能返回即将清理的 T。ResourceRef 不提供独占访问，读写同步不在验证目标内自动成立。

### 6.4 提交位置与辅助函数复用

使用同一个接收 &OperationManager 的辅助函数进行以下验证，不依赖调用线程的身份或异步任务局部变量：

| 输入视图或竞争 | 必须观察到的结果 |
| --- | --- |
| new 创建的 manager | 辅助函数提交新的 root，建立独立根额度 |
| 父 scope.manager() | 同一辅助函数提交该父的直接孩子，计入该根及祖先额度 |
| 子 scope.manager() | 同一辅助函数提交孙操作，而不是另一个根或原父的孩子 |
| clone/as_async/as_sync 后提交 | 继续绑定转换前的父级，不改变额度和停止关系 |
| 父主体返回后，通过先前保存的视图提交 | ScopeEnded，闭包未调用，未建立操作记录 |
| 父绑定视图等待 submit 时父停止 | 等待退出，返回 Stopped 及原闭包，不遗留许可 |
| 父关闭与子准入并发 | 只允许关闭前完整接纳，或关闭后完整拒绝；没有半登记 |
| 根 manager 在操作内部被调用 | 仍是独立 root，不隐式挂接当前操作 |
| manager 核心处于 Drain | 根位置拒绝新工作；开放的已接纳子树仍可按额度提交 |
| 通过绑定视图请求全局关闭 | 关闭同一核心，而非只关闭绑定父子树 |

## 7. 验收与模块边界

| 验证面 | 必须成立 |
| --- | --- |
| 同步异步 | 视图共享状态，句柄转换不重跑或复制结果 |
| 提交位置 | 所有层级使用同一组方法；scope.manager 返回父绑定视图，复制和转换保留绑定，过期拒绝且不退回 root |
| scope | 不可 Clone，反复借用，提交独立创建，主体退出后不再开放 |
| 释放条件 | Open+0/Open+非0/Ended+非0 均不清理，Ended+0 才进入调度 |
| 引用 | clone 共享、move 不增计数、Drop 归还，get/end 竞争正确 |
| 父子资源 | 子仍持父引用时保留父资源；子已归还引用但其自有资源尚未清理时，父资源可清理，父 settlement 仍等待子报告 |
| 并发 | 内部状态同步不提供资源独占权，写入由应用同步 |
| 拒绝与创建 | 失败保留资源、策略及预留，部分创建有清理去向 |
| 启动 | 准入/Drop、commit/stop、activate/abandon 无主任务数为零 |
| 结果与清理 | 结果一次交付、等待取消不丢结果、业务错误不被清理错误覆盖 |
| 证据 | DropCompleted 与外部 Confirmed 分开，未确认不伪装成功 |
| 恢复 | 同组一次重试、失败保留对象、历史报告不回写 |
| 转交 | 整组原子签收、失败源不变、既有引用稳定 |
| 关闭 | Drain/Cancel 语义一致，超时可继续观察，未解决责任不丢失 |
| 执行 | 不在锁内执行用户代码，满队列仍能停止和清理，无丢唤醒 |

模块按职责划分为 manager、operation、scope、resource、context、adapter、executor，常用类型在根模块重导出。底层容量和控制协议不与应用级对象混为一个入口。文件、命令和模型服务提供领域证据，上层封装不得重复管理同一资源。

诊断有界且溢出可见，不用可丢失诊断队列承载结果和 owner。快照关联操作/资源 ID，不默认记录敏感路径、命令和模型内容。EffectRecord 是应用声明，停止不撤销外部事实。

## 8. 核心 API 与数据结构参考

本章是目标公共接口契约。管理器等对象内部字段私有，配置、结果和报告列出公开字段或变体。验证场景中的 fixture 和领域接口不属于框架 API。关键输入、输出和泛型约束在此集中定义，示例不得自行扩展或改变契约。

### 8.0 核心对象的内部持有结构

以下结构定义职责和持有关系，字段不公开。私有容器的分片或队列实现可以变化，但不得改变结果唯一接收、作用域结束和资源引用的契约。这里的内部结构与后面的公共接口是两层设计，不要求应用访问内部字段。

```rust,ignore
pub struct OperationManager {
    core: Arc<ManagerCore>,
    target: SubmissionTarget,
}
pub struct AsyncOperationManager {
    core: Arc<ManagerCore>,
    target: SubmissionTarget,
}
#[derive(Clone)]
enum SubmissionTarget {
    Root,
    Parent { id: OperationId, scope: Weak<ScopeCore> },
}
pub struct OperationScope { // 不实现 Clone
    core: Arc<ScopeCore>,
    manager: Arc<ManagerCore>,
}

struct ManagerCore {
    state: Mutex<ManagerState>,
    executors: ExecutorSet,
    notify: LifecycleNotify,
}
struct ManagerState {
    phase: ManagerPhase,
    operations: HashMap<OperationId, Arc<OperationCore>>,
    groups: HashMap<GroupId, Arc<GroupCore>>,
    capacity: CapacityState,
}
enum ManagerPhase { Open, Draining, Cancelling, Closed }
struct ScopeCore {
    id: OperationId,
    parent: Option<Weak<ScopeCore>>,
    context: OperationContext,
    state: Mutex<ScopeState>,
}
struct ScopeState {
    body: ExecutionPhase,
    accepting: bool,
    resources: HashMap<ResourceScopeId, Arc<ResourceScopeCore>>,
    children: HashMap<OperationId, Arc<dyn OperationControl>>,
}
struct OperationCore {
    id: OperationId,
    scope: Arc<ScopeCore>,
    observation: Mutex<OperationSnapshot>,
    notify: LifecycleNotify,
}
struct OutcomeSlot<T, E> {
    state: Mutex<OutcomeSlotState<T, E>>,
    notify: LifecycleNotify,
}
enum OutcomeSlotState<T, E> {
    Pending,
    Ready(OperationOutcome<T, E>),
    Taken,
    Discarded,
}

pub struct OperationHandle<T, E> {
    core: Arc<OperationCore>,
    outcome: Arc<OutcomeSlot<T, E>>,
}
pub struct AsyncOperationHandle<T, E> {
    core: Arc<OperationCore>,
    outcome: Arc<OutcomeSlot<T, E>>,
}
pub struct OperationObserver { core: Arc<OperationCore> }
pub struct AsyncOperationObserver { core: Arc<OperationCore> }
```

manager 视图通过 SubmissionTarget 保存父级绑定；Weak 失效视为 ScopeEnded，不能解释为 Root。OperationScope 持有核心供 manager() 创建视图，但 ScopeCore 不反向强持有 ManagerCore，避免核心的操作表形成所有权环。manager() 构造的视图只弱引用 ScopeCore，不能持住其资源或开放状态。

执行协调器持有同一个 OutcomeSlot 的内部写端。两个公共 handle 类型只是不同等待外观，不能同时持有两份接收权；转换必须 move 两个字段。OperationCore 的快照不包含 T/E，因此 observer 不需要泛型，也不能取走结果。

```rust,ignore
pub struct ResourceScope {
    core: Arc<ResourceScopeCore>,
}
struct ResourceScopeCore {
    id: ResourceScopeId,
    state: Mutex<ResourceScopeState>,
}
struct ResourceScopeState {
    ended: bool,
    owners: HashMap<ResourceId, Box<dyn ErasedScopeOwner>>,
}

pub struct ResourceRef<T: Send + Sync + 'static> {
    value: Option<Arc<T>>,
    entry: Arc<ResourceEntry>,
    cleanup: Arc<CleanupStrategy<T>>,
}
struct ScopeResourceOwner<T: Send + Sync + 'static> {
    value: Option<Arc<T>>,
    entry: Arc<ResourceEntry>,
    cleanup: Arc<CleanupStrategy<T>>,
}
enum CleanupStrategy<T: Send + Sync + 'static> {
    Drop,
    Async(Arc<dyn AsyncCleanup<T>>),
}
struct ResourceEntry {
    id: ResourceId,
    state: Mutex<ResourceState>,
    dispatch: CleanupDispatch,
}
struct ResourceState {
    owner_scope: ResourceScopeId,
    owner_scope_ended: bool,
    user_references: usize,
    phase: ResourceLifecycle,
    prerequisites: Vec<ResourceId>,
}
enum ResourceLifecycle {
    Live, ReadyForCleanup, Cleaning,
    Released, Transferred, RecoveryRequired, Unconfirmed,
}
pub struct ResourceKey<T: Send + Sync + 'static> {
    manager: ManagerId,
    resource: ResourceId,
    resource_type: PhantomData<fn() -> T>,
}
```

ScopeResourceOwner 通过私有 ErasedScopeOwner 端口保存在 owners 中，持有所属范围的那份 Arc<T>；每个 ResourceRef 另持一份。两类持有者归还时必须走统一的内部释放路径，不能普通 Drop 最后一个 Arc 而在当前线程析构 T。设计采用 Arc::into_inner 竞争取得最后一份 T，再交给已预留的清理记录；每个内部强引用都必须由这两类 owner 封装，不能泄漏普通 Arc<T> 到其他内部记录。

user_references 只统计 ResourceRef，不统计 ScopeResourceOwner。范围结束时先原子标记 ended，再取出并归还所属持有，清理依赖与执行容量仍可能使 T 在清理记录中等待。ResourceEntry、观察记录和 CleanupStrategy 不额外持有 Arc<T>。get_resource 只在开放范围的锁定状态下，通过受控的内部类型检查复制对应 owner 的 Arc，建立一份新的 ResourceRef。

ErasedScopeOwner 只用于异构资源表的内部类型擦除，不公开任意 downcast。CleanupDispatch 是已预留的非阻塞接管通道；控制状态与存活计数必须避免 manager → scope → manager 的永久强引用环。ResourceRef 的内部 Option 只用于 Drop 时移出 Arc，不在活跃引用中提前清空，因此 Deref 得到的 &T 始终由当前引用保护。

所有 Manager/Operation/Group/ResourceScope 的内部身份字段由框架生成；ResourceKey 不要求 T: Copy/Clone。内部 core、状态、执行环境与通知对象的字段用于固定职责和持有方向，不构成额外公共访问入口。

### 8.1 管理器、配置和提交错误

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
    pub requirement: ChildRequirement, // 默认 Required；仅存在父级时参与父成功判定
    pub limits: ScopeLimits,
    pub child_failure_policy: ChildFailurePolicy,
    pub context: ContextOptions,
}
```

```rust,ignore
pub struct ManagerLimits {
    pub active_operations: usize,        // 默认 4
    pub queued_operations: usize,        // 默认 64
    pub admission_waiters: usize,        // 默认 64
    pub unconsumed_outcomes: usize,      // 默认 64
    pub cleanup_concurrency: usize,      // 默认 4
    pub diagnostic_events: usize,        // 默认 256
}
pub struct ScopeLimits {
    pub child_operations: usize,         // 默认 64；当前子树尚未退还名额的后代数，不含自身
    pub resources: usize,                // 默认 128；当前操作及其后代的资源登记数
    pub effect_records: usize,           // 默认 64；当前操作及其后代保留的副作用记录数
}
pub struct ResourceScopeOptions { pub capacity: usize }
pub struct ContextOptions {
    pub clock: Option<Arc<dyn ClockSource>>,
    pub deadline: Option<Deadline>,
    pub budget: Option<Arc<dyn BudgetPolicy>>,
}
pub enum ChildFailurePolicy { StopSiblings, ContinueSiblings }
pub enum ManagerCreateError {
    InvalidOptions { field: &'static str },
    ExecutorUnavailable,
    ThreadSpawn { source: Arc<dyn std::error::Error + Send + Sync> },
}

impl OperationManager {
    pub fn new(options: ManagerOptions) -> Result<Self, ManagerCreateError>;
}
impl AsyncOperationManager {
    pub fn new(options: ManagerOptions) -> Result<Self, ManagerCreateError>;
}
```

```rust,ignore
impl OperationManager {
    pub fn try_submit<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub fn submit<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub fn as_async(&self) -> AsyncOperationManager;
}
```

```rust,ignore
impl AsyncOperationManager {
    pub fn try_submit<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub async fn submit<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub fn as_sync(&self) -> OperationManager;
}
```

```rust,ignore
pub struct SubmitError<F> { reason: AdmissionError, body: F }
impl<F> SubmitError<F> {
    pub fn reason(&self) -> AdmissionError;
    pub fn into_parts(self) -> (AdmissionError, F);
}
pub enum AdmissionError {
    Busy, Closed, ScopeEnded, Stopped, DeadlineExpired, CapacityExceeded,
    ExecutorUnavailable, WouldDeadlock,
}
pub enum RunError<F> {
    Submit(SubmitError<F>),
    Outcome(OutcomeError),
    Observation(ObservationError),
}
pub struct OperationCompletion<T, E> {
    pub outcome: OperationOutcome<T, E>,
    pub settlement: Arc<SettlementReport>,
}
```

运行便利方法的完整签名如下：

```rust,ignore
impl OperationManager {
    pub fn run<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationOutcome<T, E>, RunError<F>>
    where F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
    pub fn run_and_settle<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationCompletion<T, E>, RunError<F>>
    where F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
}
impl AsyncOperationManager {
    pub async fn run<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationOutcome<T, E>, RunError<F>>
    where F: FnOnce(OperationScope) -> Fut + Send + 'static,
          Fut: Future<Output = Result<T, E>> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
    pub async fn run_and_settle<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationCompletion<T, E>, RunError<F>>
    where F: FnOnce(OperationScope) -> Fut + Send + 'static,
          Fut: Future<Output = Result<T, E>> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
}
```

### 8.2 操作句柄、观察者和业务结果

```rust,ignore
impl<T: Send + 'static, E: Send + 'static> OperationHandle<T, E> {
    pub fn id(&self) -> OperationId;
    pub fn request_stop(&self, reason: StopReason) -> StopRequestStatus;
    pub fn observer(&self) -> OperationObserver;
    pub fn snapshot(&self) -> OperationSnapshot;
    pub fn wait_outcome(&mut self) -> Result<OperationOutcome<T, E>, OutcomeError>;
    pub fn try_take_outcome(&mut self)
        -> Result<Option<OperationOutcome<T, E>>, OutcomeError>;
    pub fn wait_settlement(&self) -> Result<Arc<SettlementReport>, ObservationError>;
    pub fn into_observer(self) -> OperationObserver;
    pub fn into_async(self) -> AsyncOperationHandle<T, E>;
}

impl<T: Send + 'static, E: Send + 'static> AsyncOperationHandle<T, E> {
    pub fn id(&self) -> OperationId;
    pub fn request_stop(&self, reason: StopReason) -> StopRequestStatus;
    pub fn observer(&self) -> AsyncOperationObserver;
    pub fn snapshot(&self) -> OperationSnapshot;
    pub async fn wait_outcome(&mut self)
        -> Result<OperationOutcome<T, E>, OutcomeError>;
    pub fn try_take_outcome(&mut self)
        -> Result<Option<OperationOutcome<T, E>>, OutcomeError>;
    pub async fn wait_settlement(&self) -> Arc<SettlementReport>;
    pub fn into_observer(self) -> AsyncOperationObserver;
    pub fn into_sync(self) -> OperationHandle<T, E>;
}
```

```rust,ignore
impl OperationObserver {
    pub fn id(&self) -> OperationId;
    pub fn snapshot(&self) -> OperationSnapshot;
    pub fn wait_settlement(&self) -> Result<Arc<SettlementReport>, ObservationError>;
    pub fn wait_settlement_for(&self, timeout: Duration)
        -> Result<SettlementWait, ObservationError>;
    pub fn as_async(&self) -> AsyncOperationObserver;
}
impl AsyncOperationObserver {
    pub fn id(&self) -> OperationId;
    pub fn snapshot(&self) -> OperationSnapshot;
    pub async fn wait_settlement(&self) -> Arc<SettlementReport>;
    pub async fn wait_settlement_until(&self, deadline: Deadline)
        -> Result<SettlementWait, ObservationError>;
    pub fn as_sync(&self) -> OperationObserver;
}
```

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

```rust,ignore
pub enum OutcomeError { AlreadyTaken, Discarded, WouldDeadlock }
pub enum StopReason {
    UserCancelled, Timeout, ParentStopped,
    ManagerShutdown, RequiredChildFailed,
}
pub enum StopRequestStatus { Accepted, AlreadyRequested, AlreadyFinished }
```

```rust,ignore
pub struct StopReport {
    pub reason: StopReason,
    pub accepted_at: Option<MonotonicTime>,
}
pub enum ChildOutcomeKind { Succeeded, Failed, Stopped, Panicked, ExecutionFailed }
pub struct ChildOutcomeSummary {
    pub operation_id: OperationId,
    pub kind: ChildOutcomeKind,
    pub summary: DiagnosticSummary,
}
pub struct ChildFailureSummary {
    pub operation_id: OperationId,
    pub outcome: ChildOutcomeSummary,
}
pub struct PanicSummary {
    pub operation_id: OperationId,
    pub phase: ExecutionPhase,
    pub summary: DiagnosticSummary,
}
pub struct ExecutionFailure {
    pub operation_id: OperationId,
    pub summary: DiagnosticSummary,
    pub source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}
```

### 8.3 操作范围与子操作

```rust,ignore
pub enum ChildRequirement { Required, Optional }

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
```

### 8.4 资源引用、资源范围和拒绝处理

```rust,ignore
// Clone/Copy 不要求 T: Clone/Copy。
impl<T: Send + Sync + 'static> Clone for ResourceRef<T> {
    fn clone(&self) -> Self;
}
impl<T: Send + Sync + 'static> Deref for ResourceRef<T> {
    type Target = T;
    fn deref(&self) -> &T;
}
impl<T: Send + Sync + 'static> ResourceRef<T> {
    pub fn id(&self) -> ResourceId;
    pub fn key(&self) -> ResourceKey<T>;
}

pub struct ResourceScopeObserver { /* private; Clone */ }
pub struct AsyncResourceScopeObserver { /* private; Clone */ }
impl ResourceScope {
    pub fn manage<T: Send + Sync + 'static>(&self, resource: T)
        -> Result<ResourceRef<T>, ManageResourceError<T>>;
    pub fn manage_with<T, C>(&self, resource: T, cleanup: C)
        -> Result<ResourceRef<T>, ManageWithError<T, C>>
    where T: Send + Sync + 'static, C: AsyncCleanup<T>;
    pub fn get_resource<T: Send + Sync + 'static>(&self, key: ResourceKey<T>)
        -> Result<ResourceRef<T>, ResourceLookupError>;
    pub fn end(&self);
    pub fn observer(&self) -> ResourceScopeObserver;
    pub fn async_observer(&self) -> AsyncResourceScopeObserver;
}
impl ResourceScopeObserver {
    pub fn snapshot(&self) -> ResourceScopeSnapshot;
    pub fn wait_settlement(&self) -> Result<Arc<ResourceScopeReport>, ObservationError>;
    pub fn as_async(&self) -> AsyncResourceScopeObserver;
}
impl AsyncResourceScopeObserver {
    pub fn snapshot(&self) -> ResourceScopeSnapshot;
    pub async fn wait_settlement(&self) -> Arc<ResourceScopeReport>;
    pub fn as_sync(&self) -> ResourceScopeObserver;
}

pub struct ManageResourceError<T> { reason: ResourceManageErrorKind, resource: T }
impl<T> ManageResourceError<T> {
    pub fn reason(&self) -> ResourceManageErrorKind;
    pub fn into_resource(self) -> T;
}
pub struct ManageWithError<T, C> { reason: ResourceManageErrorKind, resource: T, cleanup: C }
impl<T, C> ManageWithError<T, C> {
    pub fn into_parts(self) -> (ResourceManageErrorKind, T, C);
}
pub enum ResourceManageErrorKind { CapacityExceeded, ScopeEnded, Stopped }
pub enum ResourceLookupError { ScopeEnded, Stopped, WrongScope, StaleKey }
```

ResourceKey 绑定 manager、资源代次及 T；字段不公开。get 验证当前所属范围，转交后只允许目标获取。不存在通过已结束范围或 Weak 复活资源的接口。

### 8.5 异步清理与资源创建预留

```rust,ignore
pub type CleanupFuture<T> =
    Pin<Box<dyn Future<Output = CleanupOutcome<T>> + Send + 'static>>;

pub trait AsyncCleanup<T: Send + Sync + 'static>: Send + Sync + 'static {
    fn cleanup(self: Arc<Self>, resource: T, context: CleanupContext) -> CleanupFuture<T>;
}
pub enum CleanupOutcome<T> {
    Released,
    Transferred(TransferReceipt),
    Failed { failure: CleanupFailure, resource: T },
}
pub enum CleanupEvidence { DropCompleted, Confirmed }
pub struct ResourceCleanupReport {
    pub resource_id: ResourceId,
    pub evidence: CleanupEvidence,
}

pub struct ResourceReservation { /* private; 持有范围与已预留容量；not Clone */ }
pub struct ResourceCommitError<T> {
    reason: ResourceManageErrorKind,
    resource: T,
    reservation: ResourceReservation,
}
pub struct ResourceCommitWithError<T, C> {
    reason: ResourceManageErrorKind,
    resource: T,
    cleanup: C,
    reservation: ResourceReservation,
}
impl OperationScope {
    pub fn reserve_resource(&self) -> Result<ResourceReservation, ResourceCapacityError>;
}
impl ResourceReservation {
    pub fn commit<T: Send + Sync + 'static>(self, resource: T)
        -> Result<ResourceRef<T>, ResourceCommitError<T>>;
    pub fn commit_with<T, C>(self, resource: T, cleanup: C)
        -> Result<ResourceRef<T>, ResourceCommitWithError<T, C>>
    where T: Send + Sync + 'static, C: AsyncCleanup<T>;
    pub fn retain_for_cleanup<T: Send + Sync + 'static>(self, resource: T) -> ResourceId;
    pub fn retain_with_for_cleanup<T, C>(self, resource: T, cleanup: C) -> ResourceId
    where T: Send + Sync + 'static, C: AsyncCleanup<T>;
}
impl<T> ResourceCommitError<T> {
    pub fn into_parts(self) -> (ResourceManageErrorKind, T, ResourceReservation);
}
impl<T, C> ResourceCommitWithError<T, C> {
    pub fn into_parts(self) -> (ResourceManageErrorKind, T, C, ResourceReservation);
}
```

管理器将 C 保存为 `Arc<dyn AsyncCleanup<T>>`，调用时只移动其副本，自己仍保留策略。因此 Failed 只需归还 T 与失败信息，重试时可继续使用同一策略；不能把唯一策略移入可能失败的 future 后丢失。T 或策略的内部恢复能力仍受 panic 边界约束。

默认 manage 的析构不调用 AsyncCleanup，manage_with 则先执行策略，再结束它持有的 T；两者共享引用和作用域模型。内部按类型擦除保存清理任务，应用不需要 downcast。

### 8.6 快照与收尾报告

```rust,ignore
pub enum ResourceProgress { ScopeOpen, WaitingForUsers, Cleaning, Unconfirmed }
pub struct OperationSnapshot {
    pub operation_id: OperationId,
    pub execution: ExecutionPhase,
    pub outcome: OutcomeAvailability,
    pub stop: Option<StopReport>,
    pub resources: Option<ResourceProgress>,
    pub settlement: Option<Arc<SettlementReport>>,
}
```

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
    pub resource_reports: Arc<[ResourceCleanupReport]>,
    pub transfers: Arc<[TransferReceipt]>,
    pub recoveries: Arc<[RecoveryHandle]>,
    pub effects: Arc<[EffectRecord]>,
    pub effect_overflow: usize,
}
```

```rust,ignore
pub struct ResourceScopeSnapshot {
    pub owner_scope_ended: bool,
    pub user_references: usize,
    pub cleanup_started: usize,
    pub pending_resources: usize,
}
pub struct ResourceScopeReport {
    pub resources: Arc<[ResourceCleanupReport]>,
    pub failures: Arc<[CleanupFailure]>,
    pub recoveries: Arc<[RecoveryHandle]>,
}
```

SettlementReport.resource_reports 保存每个成功清理项的证据。Released 表示按所选策略完成，Transferred 表示被接管，RecoveryRequired 表示仍需恢复；聚合优先级依次为恢复、转交、释放，计数不丢失。all_released 不把转交和恢复算作释放；DropCompleted 项仍不代表外部动作已经独立验证。

有 settlement 时，OperationSnapshot.resources 为 None；没有报告时描述当前资源进展。scope-open、waiting-for-users、cleaning 和 unconfirmed 的明细计数另在 ManagerSnapshot 中保留，不用单个枚举隐藏混合状态。

```rust,ignore
pub struct ChildSettlement {
    pub operation_id: OperationId,
    pub outcome: ChildOutcomeSummary,
    pub settlement: Arc<SettlementReport>,
}
pub struct ChildSnapshot {
    pub operation_id: OperationId,
    pub execution: ExecutionPhase,
    pub stop: Option<StopReport>,
    pub outcome: Option<ChildOutcomeSummary>,
    pub input_released: bool,
    pub settlement: Option<Arc<SettlementReport>>,
}
pub struct ChildRelease {
    pub operation_id: OperationId,
    pub confirmed_at: Option<MonotonicTime>,
}
pub enum CleanupStage { Drop, AsyncRelease, Dependency, Transfer }
pub struct CleanupFailure {
    pub resource_id: ResourceId,
    pub stage: CleanupStage,
    pub summary: DiagnosticSummary,
    pub source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}
pub enum EffectFact { Known, Unknown }
pub struct EffectRecord {
    pub operation_id: OperationId,
    pub resource_id: Option<ResourceId>,
    pub fact: EffectFact,
    pub description: DiagnosticSummary,
}
pub struct DiagnosticSummary { text: String, truncated: bool }
impl DiagnosticSummary {
    pub fn new(text: &str) -> Self;
    pub fn as_str(&self) -> &str;
    pub fn is_truncated(&self) -> bool;
}
impl SettlementReport {
    pub fn all_released(&self) -> bool;
}
```

DiagnosticSummary 最多保存 256 个 UTF-8 字节，在字符边界截断并标记 truncated。ChildSnapshot 是即时事实，ChildSettlement 是不可变的子报告引用；不重新复制整棵报告树。

### 8.7 恢复、操作组与关闭

```rust,ignore
pub struct RecoveryHandle { /* private; Clone; 共享恢复组状态 */ }
pub struct RecoveryStore { /* private; Clone; 独立接收方 */ }
pub struct RecoveryAttempt { /* private; 仅观察一次已提交的尝试 */ }
pub struct AsyncRecoveryAttempt { /* private; 同一尝试的异步观察 */ }
pub enum RecoveryPhase { Pending, Running, Resolved, Transferred, Unconfirmed }
pub struct RecoverySnapshot {
    pub id: RecoveryId,
    pub phase: RecoveryPhase,
    pub resources: Arc<[ResourceId]>,
    pub attempts: usize,
    pub last_failures: Arc<[CleanupFailure]>,
}
pub struct RecoveryAttemptReport {
    pub recovery: RecoverySnapshot,
    pub failures: Arc<[CleanupFailure]>,
}
pub struct RecoveryStoreOptions {
    pub group_capacity: usize,
    pub resource_capacity: usize,
    pub cleanup_concurrency: usize,
    pub cleanup_executor: ThreadPoolExecutorOptions,
}
pub struct RecoveryStoreSnapshot {
    pub accepting: bool,
    pub groups: usize,
    pub resources: usize,
    pub running_attempts: usize,
}
pub enum RetryError { AlreadyRunning, AlreadyResolved, NoCapacity, ExecutorUnavailable }
impl RecoveryStore {
    pub fn new(options: RecoveryStoreOptions) -> Result<Self, RecoveryStoreCreateError>;
    pub fn contains(&self, id: RecoveryId) -> bool;
    pub fn snapshot(&self) -> RecoveryStoreSnapshot;
    pub fn close_admission(&self);
    pub fn request_shutdown(&self) -> ShutdownHandle;
}
impl RecoveryAttempt {
    pub fn wait(&self) -> Result<RecoveryAttemptReport, ObservationError>;
    pub fn as_async(&self) -> AsyncRecoveryAttempt;
}
impl AsyncRecoveryAttempt {
    pub async fn wait(&self) -> RecoveryAttemptReport;
    pub fn as_sync(&self) -> RecoveryAttempt;
}
```

```rust,ignore
impl RecoveryHandle {
    pub fn id(&self) -> RecoveryId;
    pub fn snapshot(&self) -> RecoverySnapshot;
    pub fn try_retry(&self) -> Result<RecoveryAttempt, RetryError>;
    pub fn transfer_to(&self, destination: &RecoveryStore)
        -> Result<RecoveryTransfer, RecoveryTransferError>;
}
```

RecoveryStore 具有固定容量和独立清理执行环境，new 返回构造错误；contains/snapshot 查询，不自行启动重试。close_admission 停止接收转交，request_shutdown 返回可继续等待的关闭句柄。组转交先检查容量再原子签收，失败源不变，旧 RecoveryHandle 路由到新持有者。

OperationGroup/AsyncOperationGroup 提供对应 manager 的 try_submit、submit、run、run_and_settle，以及 as_sync/as_async。manager.group 接受 GroupOptions 并返回相应组，组保留创建视图的提交位置，其视图转换也不得丢失父级绑定；group.resource_scope 接受 ServiceResourceOptions，返回提供相同 manage/manage_with/get_resource 的 ServiceResourceScope。

```rust,ignore
pub struct OperationGroup {
    core: Arc<GroupCore>,
    target: SubmissionTarget,
}
pub struct AsyncOperationGroup {
    core: Arc<GroupCore>,
    target: SubmissionTarget,
}
pub struct ServiceResourceScope { core: Arc<ResourceScopeCore> }
pub struct GroupSnapshot {
    pub id: GroupId,
    pub accepting: bool,
    pub active_operations: usize,
    pub service_resource_scopes: usize,
}
impl OperationGroup {
    pub fn resource_scope(&self, options: ServiceResourceOptions)
        -> Result<ServiceResourceScope, ResourceScopeError>;
    pub fn snapshot(&self) -> GroupSnapshot;
    pub fn request_shutdown(&self, mode: ShutdownMode) -> ShutdownHandle;
}
impl AsyncOperationGroup {
    pub fn resource_scope(&self, options: ServiceResourceOptions)
        -> Result<ServiceResourceScope, ResourceScopeError>;
    pub fn snapshot(&self) -> GroupSnapshot;
    pub fn request_shutdown(&self, mode: ShutdownMode) -> AsyncShutdownHandle;
}
impl ServiceResourceScope {
    pub fn manage<T: Send + Sync + 'static>(&self, resource: T)
        -> Result<ResourceRef<T>, ManageResourceError<T>>;
    pub fn manage_with<T, C>(&self, resource: T, cleanup: C)
        -> Result<ResourceRef<T>, ManageWithError<T, C>>
    where T: Send + Sync + 'static, C: AsyncCleanup<T>;
    pub fn get_resource<T: Send + Sync + 'static>(&self, key: ResourceKey<T>)
        -> Result<ResourceRef<T>, ResourceLookupError>;
    pub fn end(&self);
    pub fn observer(&self) -> ResourceScopeObserver;
    pub fn async_observer(&self) -> AsyncResourceScopeObserver;
}
```

```rust,ignore
impl OperationGroup {
    pub fn try_submit<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub fn submit<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub fn as_async(&self) -> AsyncOperationGroup;
}
```

```rust,ignore
impl AsyncOperationGroup {
    pub fn try_submit<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub async fn submit<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static, E: Send + 'static;

    pub fn as_sync(&self) -> OperationGroup;
}
```

```rust,ignore
impl OperationGroup {
    pub fn run<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationOutcome<T, E>, RunError<F>>
    where F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
    pub fn run_and_settle<F, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationCompletion<T, E>, RunError<F>>
    where F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
}
impl AsyncOperationGroup {
    pub async fn run<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationOutcome<T, E>, RunError<F>>
    where F: FnOnce(OperationScope) -> Fut + Send + 'static,
          Fut: Future<Output = Result<T, E>> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
    pub async fn run_and_settle<F, Fut, T, E>(&self, options: OperationOptions, body: F)
        -> Result<OperationCompletion<T, E>, RunError<F>>
    where F: FnOnce(OperationScope) -> Fut + Send + 'static,
          Fut: Future<Output = Result<T, E>> + Send + 'static,
          T: Send + 'static, E: Send + 'static;
}
```

```rust,ignore
pub struct GroupOptions { pub capacity: usize }
pub struct ServiceResourceOptions { pub capacity: usize }
impl OperationManager {
    pub fn group(&self, options: GroupOptions) -> Result<OperationGroup, AdmissionError>;
    pub fn snapshot(&self) -> ManagerSnapshot;
}
impl AsyncOperationManager {
    pub fn group(&self, options: GroupOptions) -> Result<AsyncOperationGroup, AdmissionError>;
    pub fn snapshot(&self) -> ManagerSnapshot;
}
pub struct ManagerSnapshot {
    pub active_operations: usize,
    pub queued_operations: usize,
    pub admission_waiters: usize,
    pub unconsumed_outcomes: usize,
    pub cleaning_resources: usize,
    pub recovery_groups: usize,
    pub unconfirmed_users: usize,
    pub diagnostic_overflow: usize,
}
```

```rust,ignore
impl OperationManager {
    pub fn close_admission(&self);
    pub fn request_shutdown(&self, mode: ShutdownMode) -> ShutdownHandle;
}
impl AsyncOperationManager {
    pub fn close_admission(&self);
    pub fn request_shutdown(&self, mode: ShutdownMode) -> AsyncShutdownHandle;
}

pub enum ShutdownMode { Drain, Cancel }

impl ShutdownHandle {
    pub fn snapshot(&self) -> ShutdownSnapshot;
    pub fn wait(&self) -> Result<ShutdownReport, ObservationError>;
    pub fn wait_for(&self, timeout: Duration) -> Result<ShutdownWait, ObservationError>;
    pub fn as_async(&self) -> AsyncShutdownHandle;
}
impl AsyncShutdownHandle {
    pub fn snapshot(&self) -> ShutdownSnapshot;
    pub async fn wait(&self) -> ShutdownReport;
    pub async fn wait_until(&self, deadline: Deadline)
        -> Result<ShutdownWait, ObservationError>;
    pub fn as_sync(&self) -> ShutdownHandle;
}
```

```rust,ignore
pub enum ShutdownWait { Completed(ShutdownReport), Pending(ShutdownSnapshot) }
pub struct ShutdownReport { pub snapshot: ShutdownSnapshot }
pub struct ShutdownSnapshot {
    pub active_operations: usize,
    pub queued_operations: usize,
    pub unconfirmed_users: usize,
    pub cleaning_resources: usize,
    pub recovery_groups: usize,
    pub service_resources: usize,
}
```

ShutdownReport 只在上述责任均已完成或被合格接收方接管时返回。Pending 保留观察入口；丢弃关闭 handle 不撤销关闭。RecoveryStore 内尚未解决的 owner 不得因最后一个公共句柄丢弃而静默析构，仍由内部责任持有者保留并报告。

### 8.8 上下文、适配器与其他核心约束

```rust,ignore
pub struct PortableExecutorOptions {
    pub worker_count: usize,
    pub task_capacity: usize,
    pub polls_per_turn: usize,
}
pub struct ThreadPoolExecutorOptions {
    pub worker_count: usize,
    pub queue_capacity: usize,
}
pub struct OperationContext { /* private; Clone; 共享停止节点和继承策略 */ }
pub struct CleanupContext {
    pub resource_id: ResourceId,
    pub context: OperationContext,
}
pub struct Deadline { at: MonotonicTime }
pub struct MonotonicTime { domain: ClockDomainId, ticks: u64 }
impl Deadline {
    pub fn at(at: MonotonicTime) -> Self;
    pub fn time_point(self) -> MonotonicTime;
    pub fn earliest(self, other: Self) -> Result<Self, ClockDomainMismatch>;
}
impl MonotonicTime {
    pub fn new(domain: ClockDomainId, ticks: u64) -> Self;
    pub fn domain(self) -> ClockDomainId;
    pub fn ticks(self) -> u64;
}
pub trait TimerRegistration: Send {
    fn cancel(self: Box<Self>);
}
```

ClockDomainId 由 ClockDomainId::new_unique() 签发，供外部时钟实现创建自己的域；不是应用随意填写的可比较整数。manager 默认使用同一配置时钟，ticks 的单位和精度由时钟实现定义并保持一致。


```rust,ignore
pub struct BudgetError { pub summary: DiagnosticSummary }
pub enum ContextError {
    ClockUnavailable, ClockDomainMismatch, Stopped, BudgetRejected(BudgetError),
}
pub struct ClockDomainMismatch {
    pub left: ClockDomainId,
    pub right: ClockDomainId,
}

pub trait BudgetPolicy: Send + Sync {
    fn check(&self) -> Result<(), BudgetError>;
}
impl OperationContext {
    pub fn new(options: ContextOptions) -> Result<Self, ContextError>;
    pub fn child(&self, options: ContextOptions) -> Result<Self, ContextError>;
    pub fn request_stop(&self, reason: StopReason) -> StopRequestStatus;
    pub fn stop_reason(&self) -> Option<StopReason>;
    pub async fn stop_requested(&self) -> StopReport;
    pub fn deadline(&self) -> Option<Deadline>;
}
pub trait ClockSource: Send + Sync {
    fn now(&self) -> MonotonicTime;
    fn register_deadline(&self, at: MonotonicTime, wake: Waker)
        -> Box<dyn TimerRegistration>;
}
```

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

ChildRelease 只确认不再使用附带输入，不能代替子操作自身的 settlement。三个 poll 都必须正确登记唤醒并返回稳定事实。adapter 的 reservation、ChildStartPermit 和执行器许可均不可 Clone，未使用时有明确回滚或 abandon 行为。

OperationId、ResourceId、RecoveryId、ClockDomainId 为不可伪造的身份类型；回执字段私有，只由接收方接纳协议创建。ResourceTransfer 包含同一 manager 内的稳定成员 ID、来源、目标和回执，不返回一份复制的数据。

```rust,ignore
// 字段私有，统一实现 Copy/Clone/Eq/Hash/Debug。
pub struct ManagerId(u128);
pub struct OperationId(u128);
pub struct GroupId(u128);
pub struct ResourceScopeId(u128);
pub struct RecoveryId(u128);
pub struct RecoveryStoreId(u128);
pub struct ClockDomainId(u128);
pub struct ResourceId {
    manager: ManagerId,
    slot: u64,
    generation: u64,
}
impl ClockDomainId {
    pub fn new_unique() -> Self;
}

pub struct TransferReceipt {
    resource: ResourceId,
    source_scope: ResourceScopeId,
    target_scope: ResourceScopeId,
    nonce: u64,
}
pub struct ResourceTransfer {
    pub members: Arc<[ResourceId]>,
    pub source_scope: ResourceScopeId,
    pub target_scope: ResourceScopeId,
    pub receipt: TransferReceipt,
}
pub struct RecoveryTransfer {
    pub recovery: RecoveryId,
    pub receiver: RecoveryStoreId,
    pub receipt: RecoveryTransferReceipt,
}
pub struct RecoveryTransferReceipt {
    recovery: RecoveryId,
    receiver: RecoveryStoreId,
    nonce: u64,
}
pub enum ObservationError {
    WouldDeadlock, ClockUnavailable, ClockDomainMismatch,
}
pub enum ResourceCapacityError { ScopeEnded, Stopped, CapacityExceeded }
pub enum ResourceScopeError { ScopeEnded, Stopped, InvalidOptions, CapacityExceeded }
pub enum CleanupDependencyError {
    UnknownResource, WrongManager, WrongScope, SelfDependency, Cycle, CleanupStarted,
}
pub enum ResourceTransferError {
    UnknownResource, WrongManager, SameScope, TargetEnded,
    TargetStopped, CapacityExceeded, CleanupStarted, IncompleteDependencyGroup,
}
```

TransferReceipt 为完整组的接纳凭据，resource 标识组内被请求转交的锚点，members 给出全部成员；不是每个成员各签一份可拆开的责任。RecoveryTransferReceipt 单独绑定恢复组和接收方。两类回执只由框架接纳协议创建，不能混用。

ManagerCreateError、ObservationError、资源预留、依赖、转移与重试错误归属各自模块。观察错误与业务 E 分开，错误携带的 F、资源、策略和 permit 均为 must_use。错误及其原因枚举实现 Debug/Display，错误实现适用的 std::error::Error，报告实现 Debug；用户 T/E 和捕获的 F 不因错误实现而被额外要求 Clone 或 Debug。

以上是目标设计。文档确认后再单独编写实施文档，不在本次设计中安排迁移或代码重构。

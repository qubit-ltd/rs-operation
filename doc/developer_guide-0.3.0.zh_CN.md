# rs-operation 0.3.0 开发者手册

[English](developer_guide-0.3.0.md) · [设计文档](design-0.3.0.zh_CN.md) · [README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-operation)

## 目录

- [1. 本手册要帮助你完成什么](#1-本手册要帮助你完成什么)
- [2. 安装与应用生命周期](#2-安装与应用生命周期)
- [3. 从上传字节到检查结果](#3-从上传字节到检查结果)
- [4. 选择提交方式并处理拒绝](#4-选择提交方式并处理拒绝)
- [5. 正确观察结果、停止与结算](#5-正确观察结果停止与结算)
- [6. 把多个检查组合成父子操作](#6-把多个检查组合成父子操作)
- [7. 资源登记、预留与异步清理](#7-资源登记预留与异步清理)
- [8. 服务组与跨请求共享资源](#8-服务组与跨请求共享资源)
- [9. 配置容量与背压](#9-配置容量与背压)
- [10. 截止时间、预算与 effects](#10-截止时间预算与-effects)
- [11. 解释结算并处理清理失败](#11-解释结算并处理清理失败)
- [12. 接入外部 provider](#12-接入外部-provider)
- [13. 关闭服务与恢复服务](#13-关闭服务与恢复服务)
- [14. 排障与开发验证](#14-排障与开发验证)

## 1. 本手册要帮助你完成什么

本手册面向把 `qubit-operation` 接入 Rust 服务的开发者，包括普通操作调用方、资源清理策略作者、外部 provider 适配器作者。默认你熟悉 Rust 所有权、`Arc` 和 future，但不要求先理解框架内部实现。

贯穿场景是上传文档检查：服务接收字节，执行检测，返回结果；请求可能被取消，检测也可能移交给外部执行器。接入完成的标准不是“拿到一个结果”，而是业务结果已有处理、每个资源使用者均可追踪、清理失败有后续责任方，服务能解释关闭为何完成或仍在等待。

第一次使用按第 2—5 节完成接入，再按需阅读资源、预算、外部任务和恢复专题。维护底层机制时结合[设计文档](design-0.3.0.zh_CN.md)阅读。下文“结算”指生命周期责任的归结，可能是释放、转移或恢复持有，不等于业务成功。

## 2. 安装与应用生命周期

使用 Rust 1.94 或更高版本，建议应用采用 Rust 2024 edition：

```toml
[dependencies]
qubit-operation = "^0.3"
```

管理器应由应用的启动入口创建，并注入上传服务。请求处理只负责提交操作、处理 outcome 并把结算交给确定的观察路径。停止服务时先停止接收新请求，再发起管理器关闭；不要每次请求都创建、销毁整套执行器。

管理器 clone 共享协调器与容量，不会创建独立线程池。`as_async()` / `as_sync()` 也只是视图转换；管理器级限制仍然共享。操作需要子任务时，必须从当前 `scope.manager()` 获取子提交入口，不能用另一个根管理器冒充子操作。

## 3. 从上传字节到检查结果

下面是完整、可运行的应用边界示例，也保存于 [upload_inspection.rs](../examples/upload_inspection.rs)。运行 `cargo run --example upload_inspection --locked`。`InspectionService` 与 `Inspection` 是应用定义的类型，不属于本库；HTTP 路由、存储与真正的检测器可在这个边界外接入。

```rust
use qubit_operation::{
    ManagerOptions, OperationManager, OperationOptions, OperationOutcome, SettlementReport,
    ShutdownMode,
};
use std::sync::Arc;

// Application-owned service boundary; no HTTP or MIME library is implied.
struct InspectionService {
    manager: OperationManager,
}

struct Inspection {
    mime: Option<&'static str>,
    settlement: Arc<SettlementReport>,
}

impl InspectionService {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            manager: OperationManager::new(ManagerOptions::default())?,
        })
    }

    fn inspect(&self, bytes: Vec<u8>) -> Result<Inspection, Box<dyn std::error::Error>> {
        let mut handle = self
            .manager
            .try_submit(OperationOptions::default(), move |scope| {
                let input = scope.manage(bytes).map_err(|error| {
                    let (kind, bytes) = error.into_parts();
                    drop(bytes); // Vec needs no remote or asynchronous cleanup.
                    format!("input registration failed: {kind:?}")
                })?;
                Ok::<_, String>(input.starts_with(b"%PDF-").then_some("application/pdf"))
            })?;
        // This result owns no ResourceRef; either branch can now be settled.
        let outcome = handle.wait_outcome()?;
        let settlement = handle.wait_settlement()?;
        match outcome {
            OperationOutcome::Succeeded(mime) => Ok(Inspection { mime, settlement }),
            other => Err(std::io::Error::other(format!(
                "inspection failed: {other:?}; settlement: {:?}",
                settlement.status
            ))
            .into()),
        }
    }

    fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        let report = self.manager.request_shutdown(ShutdownMode::Drain).wait()?;
        println!("shutdown: {:?}", report.snapshot);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = InspectionService::new()?;
    for bytes in [b"%PDF-1.7\n".to_vec(), b"plain text".to_vec()] {
        let result = service.inspect(bytes)?;
        println!(
            "MIME: {:?}; released: {}",
            result.mime,
            result.settlement.all_released()
        );
    }
    service.shutdown()?;
    Ok(())
}
```

第一组输入返回 `Some("application/pdf")`，第二组返回 `None`，两次结算均应显示 `released: true`。`None` 是已完成检查后的未匹配，不能当成执行故障。应用随后决定接受文件、拒绝上传或尝试其他检测器。

示例中：管理器在服务启动时创建；上传字节 move 进闭包，登记到操作 scope；局部 `ResourceRef` 随闭包退出释放；返回值不持有资源；调用方消费 outcome，再等结算；最后 drain 管理器。资源登记失败时取回 `Vec` 并 drop，因为它只需要内存释放。文件租约或远端句柄不能照搬这种处置方式，应按第 7 节保留清理路径。

这仍是简化服务示例：它没有 HTTP、持久化、完整 MIME 检测或生产重试策略。生产请求发生失败时，还应把结算报告交给日志、恢复或后台监控路径，而不能只将错误转成响应文本。

## 4. 选择提交方式并处理拒绝

### 4.1 即时尝试还是等待准入

`try_submit` 不等待容量；`submit` 可以等待准入。同步版本阻塞调用线程，异步版本返回需要 await 的 future。拒绝时 `SubmitError<F>` 保留未调用的闭包，`into_parts()` 取回原因和闭包；外部提交则保留 `Arc<P>`。是否重试、何时重试以及是否重新构造输入由应用决定。

| 拒绝原因 | 含义与下一步 |
| --- | --- |
| `Busy` | 即时准入所需容量正在占用；稍后重试或向上游施加背压。 |
| `CapacityExceeded` | 队列或等待者等配置额度不足；检查消费速度与配置。 |
| `Closed` / `ScopeEnded` | 管理器或提交目标已关闭；不能把无限重试当作修复。 |
| `Stopped` / `DeadlineExpired` | 停止或时间约束阻止准入；结束该请求。 |
| `ExecutorUnavailable` | 执行器不能接纳；检查执行器生命周期与容量。 |
| `AlreadyBound` | 外部准备适配器已认领；新建实例。 |
| `WouldDeadlock` | 当前 worker 不适合阻塞等待准入；改用即时提交或调整组合。 |

准入失败与 `OperationOutcome::Failed(E)` 是不同阶段：前者没有接纳该工作，后者是已接纳操作的业务失败。不要在没有区分阶段时重新执行可能已经产生副作用的业务。

### 4.2 异步请求入口

下面是独立的集成函数，可放在请求服务模块；调用方在自己的异步运行时中 await 它。传入 `manager.as_async()` 得到的视图，实际操作 future 由管理器异步执行器驱动。它与前述同步示例采用相同的输入和结果判定。

```rust
use qubit_operation::{AsyncOperationManager, OperationOptions, OperationOutcome};

pub async fn inspect_async(manager: &AsyncOperationManager, bytes: Vec<u8>) -> Result<bool, String> {
    let mut handle = manager.try_submit(OperationOptions::default(), move |scope| async move {
        let input = scope.manage(bytes).map_err(|error| format!("{:?}", error.kind()))?;
        Ok::<_, String>(input.starts_with(b"%PDF-"))
    }).map_err(|error| format!("admission: {:?}", error.reason()))?;
    let outcome = handle.wait_outcome().await.map_err(|error| format!("{error:?}"))?;
    let report = handle.wait_settlement().await;
    match outcome {
        OperationOutcome::Succeeded(found) if report.all_released() => Ok(found),
        other => Err(format!("outcome: {other:?}; settlement: {:?}", report.status)),
    }
}
```

异步等待不会使阻塞 I/O 自动变成非阻塞。同步阻塞工作应放在普通同步操作或应用自己的 provider 执行路径中；不要在异步闭包和适配器 poll 中阻塞等待。同样，不要在异步请求任务中调用同步 `wait_*` 来替代 await。

## 5. 正确观察结果、停止与结算

### 5.1 业务结果只消费一次

handle 是唯一 typed 接收端。`wait_outcome` 或 `try_take_outcome` 取走 `OperationOutcome<T,E>`；重复获取返回 `OutcomeError::AlreadyTaken`。外层接收错误与内层业务结果要分别处理：

| Outcome | 应用应做什么 |
| --- | --- |
| `Succeeded(T)` | 使用结果，但仍检查结算。 |
| `Failed(E)` | 处理业务错误，并安排结算观察。 |
| `ChildFailed` | 查看必需子任务失败摘要及父子策略。 |
| `Stopped` | 根据停止原因结束业务流程，继续等待实际释放。 |
| `Panicked` | 记录 panic 摘要；不据此推断清理完成。 |
| `ExecutionFailed` | 检查执行器或外部适配器协议故障。 |

`observer()` 创建额外只读观察者，不取走结果。`into_async()` / `into_sync()` 转移唯一接收端而不取消。`into_observer()` 丢弃 typed 结果但不请求停止；直接 drop 未结束操作的 handle 则请求用户取消并丢弃结果。需要持续后台观察时应明确保留 handle 或 observer。

### 5.2 停止是请求，不是释放证明

handle 和 scope 的 `request_stop` 返回 `Accepted`、`AlreadyRequested` 或 `AlreadyFinished`。它们分别表示首次登记、此前已登记、执行已结束；最后一种也不能证明结算完成。停止原因采用首次接受的原因，父停止会向后代传播。

操作代码要在适当工作边界检查 `scope.context().check()`，异步代码可等待 `stop_requested()`。provider 要把停止请求交给自身运行时，并继续报告输入使用结束。框架不会强行终止任意同步函数、系统调用或 SDK 工作。

### 5.3 先释放结果中的引用

若 `T`、`E` 或它们间接持有的对象包含本次结算正在等待的 `ResourceRef`，必须先取结果、处理并 drop 引用，然后再等待结算。包括结果 clone 出来的引用也必须释放。否则等待条件依赖调用方仍持有的对象。

`run_and_settle` 在返回结果前等待结算，因此只适用于结果和错误不会保留这类引用的操作。[资源型结果示例](examples-0.3.0/inference.rs)展示成功值和错误值都可能持有引用的处理顺序。

从外部调用线程使用 `wait_settlement_for(timeout)` 可以得到 `Ready(report)` 或 `Pending(snapshot)`。超时只是结束本次观察，不会取消操作或释放资源。同步操作 worker 内的结算等待会返回 `ObservationError::WouldDeadlock`；应把等待放在调用方，避免同池阻塞。

## 6. 把多个检查组合成父子操作

上传检查可能拆成格式检测、内容扫描和可选元数据提取。父闭包通过 `scope.manager()` 提交子任务，它们共享管理器并进入当前父操作的生命周期。原根管理器上的提交不会因为出现在父闭包内部就自动变成子任务。可运行组合见 [family_and_capacity.rs](../examples/family_and_capacity.rs) 与 [submission_context.rs](../examples/submission_context.rs)。

`OperationOptions::requirement` 配置该子操作是否必需，默认 `Required`。可选子操作失败会被报告，但自身不直接使父结果失败。父操作的 `child_failure_policy` 决定必需子任务失败时，是否请求兄弟停止：默认 `StopSiblings`，也可选 `ContinueSiblings`。后者允许继续工作，不会把必需子失败改成成功。

父业务结果在操作体结束且所有 Required 子操作结果就绪后才发布；Optional 子操作不阻塞这道结果门控，但仍参与父结算。

父闭包返回不等于父操作可立即结算。框架继续等待其生命周期所需的子结果、输入释放和子结算；应用也应消费或丢弃子 handle 的结果，防止长期占用结果或路径容量。不要为了“让框架接管”而直接丢弃未结束的子 handle，这会请求取消。

scope 不是可 clone 的长期提交许可。`scope.manager()` 视图保留父目标但不会延长父 scope 的开放时间；父 scope 结束后不能继续提交。组合场景中应先完成子任务接纳，再退出父操作体。

下面是可独立编译的异步组合函数，替换第 4 节的单操作入口。它显式配置父策略和子 requirement，消费子结果后再由外部调用方等待父结算；没有在同步 worker 里阻塞等待。示例只有一个子任务，ContinueSiblings 的作用需在加入兄弟任务时观察。

```rust
use qubit_operation::{AsyncOperationManager, ChildFailurePolicy, ChildRequirement,
    OperationOptions, OperationOutcome};

pub async fn inspect_with_child(manager: &AsyncOperationManager, bytes: Vec<u8>) -> Result<bool, String> {
    let options = OperationOptions {
        child_failure_policy: ChildFailurePolicy::ContinueSiblings,
        ..OperationOptions::default()
    };
    let mut parent = manager.try_submit(options, move |scope| async move {
        let children = scope.manager().as_async();
        let child_options = OperationOptions {
            requirement: ChildRequirement::Required,
            ..OperationOptions::default()
        };
        let mut child = children.try_submit(child_options, move |child_scope| async move {
            let input = child_scope.manage(bytes).map_err(|error| format!("{:?}", error.kind()))?;
            Ok::<_, String>(input.starts_with(b"%PDF-"))
        }).map_err(|error| format!("child admission: {:?}", error.reason()))?;
        match child.wait_outcome().await.map_err(|error| format!("{error:?}"))? {
            OperationOutcome::Succeeded(found) => Ok::<_, String>(found),
            other => Err(format!("child: {other:?}")),
        }
    }).map_err(|error| format!("parent admission: {:?}", error.reason()))?;
    let outcome = parent.wait_outcome().await.map_err(|error| format!("{error:?}"))?;
    let _settlement = parent.wait_settlement().await;
    match outcome {
        OperationOutcome::Succeeded(found) => Ok(found),
        other => Err(format!("parent: {other:?}")),
    }
}
```

将子任务配置为 Optional 适用于“失败不应决定父成功”的辅助工作。若格式检测是接收文档的前置条件，应保留 Required。可选仅改变结果依赖，不解除资源或停止责任。

## 7. 资源登记、预留与异步清理

### 7.1 已有资源和使用引用

`scope.manage(value)` 把已有值登记到操作资源 scope；失败时错误持有原值。资源要求 `Send + Sync + 'static`。返回的 `ResourceRef<T>` 可解引用为共享 `&T`；clone 产生独立使用引用，每份都延迟清理。共享可变内容要由应用通过锁等方式同步。

`ResourceKey<T>` 是带 scope 和 generation 的查找键，不持有资源。`get_resource` 会校验 scope、类型和代次；关闭 scope 后，现存引用仍可读，但不能把旧键当作重新开放查找或复活资源的能力。引用保证资源存在，不保证 scope 接受新工作。

### 7.2 必须先确保容量才能创建

先调用 `reserve_resource()`，创建成功后再 `commit`。下面是独立的操作辅助函数，可在第 3 节闭包中替换直接 `manage`。示例用内存分配代表创建阶段：

```rust
use qubit_operation::{OperationScope, ResourceRef};

pub fn prepare_input(scope: &OperationScope) -> Result<ResourceRef<Vec<u8>>, String> {
    let reservation = scope.reserve_resource().map_err(|error| format!("{error:?}"))?;
    let input = vec![0u8; 1024]; // Replace with application resource creation.
    match reservation.commit(input) {
        Ok(input) => Ok(input),
        Err(error) => {
            let (kind, input, reservation) = error.into_parts();
            let _resource_id = reservation.retain_for_cleanup(input);
            Err(format!("commit rejected: {kind:?}"))
        }
    }
}
```

预留同时保留资源容量和清理派发许可，未消费的预留在 drop 时归还。资源创建失败且尚无资源时，直接释放预留即可。创建和提交之间可能停止；提交错误会退回资源及预留，应使用保留的清理路径或应用自身可靠的清理方式。自定义清理对应 `commit_with` 和 `retain_with_for_cleanup`，不能丢掉清理策略。

### 7.3 自定义异步清理

资源需要异步释放时，用 `scope.resource_scope(options)` 创建附属资源 scope，再调用其 `manage_with`，或在预留上 `commit_with`。不要假设 `OperationScope` 上存在同名 `manage_with` 方法。下面是可独立编译的策略与登记函数；示例只释放字节，真实远端关闭逻辑由应用替换。

```rust
use std::sync::Arc;
use qubit_operation::{AsyncCleanup, CleanupContext, CleanupFuture, CleanupOutcome,
    OperationScope, ResourceScopeOptions};

struct ReleaseInput;
impl AsyncCleanup<Vec<u8>> for ReleaseInput {
    fn cleanup(self: Arc<Self>, input: Vec<u8>, _context: CleanupContext) -> CleanupFuture<Vec<u8>> {
        Box::pin(async move {
            drop(input);
            CleanupOutcome::Released
        })
    }
}

pub fn register_input(scope: &OperationScope) -> Result<(), String> {
    let resources = scope.resource_scope(ResourceScopeOptions { capacity: 2 })
        .map_err(|error| format!("{error:?}"))?;
    let input = resources.manage_with(vec![1, 2, 3], ReleaseInput)
        .map_err(|error| format!("{error:?}"))?;
    drop(input);
    resources.end();
    Ok(())
}
```

附属资源 scope 的报告参与操作结算。`end()` 关闭新登记并启动收尾；最后一个引用释放后才具备清理条件。普通 `manage` 使用默认 drop，`manage_with` 使用 `AsyncCleanup<T>`，策略返回 `CleanupFuture<T>`：

- `Released`：已确认释放，不能仅凭“请求已经发送”返回此值。
- `Failed { failure, resource }`：带回失败诊断和仍拥有的资源，以便后续恢复。
- `Transferred(receipt)`：接收方已接受所有权的凭据；不是描述“计划转移”的标志。

当前公开 API 没有面向下游的任意资源依赖图配置或整组资源转移入口。内部存在相关机制与报告类型，不代表应用可以构造合法转移凭据。公开、可用的恢复责任移交路径是第 11 节的 `RecoveryHandle::transfer_to`。

清理上下文独立于业务停止树，避免业务取消直接取消必要收尾。策略仍须如实处理清理自身的失败和不确定性；panic 或 future 中断不能当作释放成功。

## 8. 服务组与跨请求共享资源

管理器全局容量不能表达所有租户或服务边界时，使用 `manager.group(GroupOptions { capacity })`。组的容量约束操作接纳，组本身不是单独执行器。`with_group(&group)` 让现有 root/parent 视图额外受该组约束，保留原父目标。

`ManagerOptions::max_groups_per_operation` 默认 16。重复绑定同一组不重复计数；超限返回 `GroupBindingError::CapacityExceeded`。其他错误区分不同管理器、组已关闭、父 scope 已结束。组的 `close_admission()` 关闭直接组提交，已接纳父操作仍可通过绑定 scope 提交后代；管理器 Drain 拒绝新根提交，但允许已接纳且仍开放的父 scope 提交后代；Cancel 进一步拒绝新的普通子提交。

跨请求共享的模型或连接信息可由 `group.resource_scope(ServiceResourceOptions { capacity })` 管理。该容量独立于每个请求的资源条目；每个使用者领取自己的 `ResourceRef`。关闭服务时先停止新使用，再 `close()` 服务资源 scope，最后释放现存引用。`close()` 本身不等待最后一个使用者退出，也不会让现存引用失效。完整行为可参考 `tests/v02_service_resource_tests.rs`。

下面的独立同步函数模拟跨请求规则资源的领取、使用和关闭，可从应用调用线程传入根管理器执行。String 只是模型标记，不加载真实模型。正常返回 true；所有请求使用引用都释放后，才关闭资源与组。生产失败分支应把清理责任交回服务生命周期，不能在仍有后台使用者时强行退出。

```rust
use qubit_operation::{GroupOptions, OperationManager, OperationOptions,
    OperationOutcome, ServiceResourceOptions, ShutdownMode};

pub fn inspect_with_shared_model(manager: &OperationManager) -> Result<bool, String> {
    let group = manager.group(GroupOptions { capacity: 2 }).map_err(|error| format!("{error:?}"))?;
    let resources = group.resource_scope(ServiceResourceOptions { capacity: 1 })
        .map_err(|error| format!("{error:?}"))?;
    let model = resources.manage(String::from("PDF signature rule"))
        .map_err(|error| format!("{error:?}"))?;
    let worker_model = model.clone();
    let mut task = group.try_submit(OperationOptions::default(), move |_scope| {
        Ok::<_, String>(worker_model.contains("PDF"))
    }).map_err(|error| format!("{:?}", error.reason()))?;
    let outcome = task.wait_outcome().map_err(|error| format!("{error:?}"))?;
    let _settlement = task.wait_settlement().map_err(|error| format!("{error:?}"))?;
    resources.close();
    drop(model);
    let _shutdown = group.request_shutdown(ShutdownMode::Drain).wait()
        .map_err(|error| format!("{error:?}"))?;
    match outcome {
        OperationOutcome::Succeeded(found) => Ok(found),
        other => Err(format!("{other:?}")),
    }
}
```

## 9. 配置容量与背压

容量不是一个“并发数”。不同许可在不同阶段归还：

| 配置或许可 | 何时需要关注 |
| --- | --- |
| `limits.active_operations` | 当前实现的根操作生命周期额度，结算时归还；不是简单 CPU worker 数。 |
| `limits.queued_operations` / `admission_waiters` | 排队任务和等待准入者；等待本身也有界。 |
| `limits.unconsumed_outcomes` | 结果消费或丢弃前持有；不要积压 handle。 |
| 父级 `child_operations` | 子路径容量沿祖先记账，结算且结果消费/丢弃后归还。 |
| 组容量 | 同时受结算和结果消费/丢弃约束，结果可用后仍可能占用。 |
| `resources` / `resource_scopes` / `effect_records` | scope 树中的资源、附属 scope 和事实条目。 |
| 执行器配置 | 同步、异步和清理各自的 worker/队列或 task capacity。 |

管理器默认值：active 4、queued 64、waiters 64、unconsumed outcomes 64、cleanup concurrency 4、diagnostic events 256。默认 scope：children 64、resources 128、resource scopes 64、effects 64。默认异步执行器：4 workers、task capacity 68、每轮 16 polls；同步和清理执行器默认各 4 workers、queue capacity 64。默认值是起点，应依据持有时间、峰值输入和清理延迟配置。

`OperationOptions::limits` 等于整个 `ScopeLimits::default()` 时，使用管理器的整组 `scope_limits`；任一字段不同则整组替换，不逐字段合并。子操作请求的额度仍受每个祖先有效额度约束。因此不能表达“显式请求恰好等于默认结构，同时不继承管理器值”。调整单字段前先复制你希望继承的完整配置。

例如为上传服务构造完整配置，而不是从不相关默认值逐字段猜测：

```rust
use qubit_operation::{ManagerOptions, OperationOptions, ScopeLimits};

pub fn inspection_options() -> (ManagerOptions, OperationOptions) {
    let limits = ScopeLimits {
        child_operations: 8, resources: 20, resource_scopes: 4, effect_records: 8,
    };
    let manager = ManagerOptions { scope_limits: limits, ..ManagerOptions::default() };
    // Copy the entire intended structure before changing a field.
    let operation = OperationOptions {
        limits: ScopeLimits { effect_records: 4, ..limits },
        ..OperationOptions::default()
    };
    (manager, operation)
}
```

该操作明确请求 resources=20、effects=4；返回的管理器配置可传入 `OperationManager::new`，操作配置传入提交方法。父 scope 更小时，子请求仍不能绕过它。

资源条目上限不限制单个 `Vec`、错误 source 或 provider 内存的大小。遇到 Busy 先排查未消费结果、长寿命引用、慢清理和组占用；增加 worker 数不能替代释放这些许可。同步管理器的外部驱动使用异步执行器容量，尤其不要只调整同步池。

## 10. 截止时间、预算与 effects

`ContextOptions` 可指定 `clock`、`deadline` 和 `budget`。截止时间需要兼容的 `ClockSource`，不同 clock domain 不能混算。子截止时间取不晚于父级的有效值；子预算策略加入继承链，不替代父预算。

`context.check()` 检查停止、过期和全部继承策略；过期会登记 `StopReason::Timeout`。该检查不等于预留任务或资源容量。长操作应在可中断边界重复检查；`stop_requested()` 提供停止信号，`deadline_signal()` 在具备时钟和 deadline 时提供定时信号。不要假设设置 deadline 能打断任意阻塞代码。

`scope.record_effect(EffectRecord { operation_id: scope.id(), resource_id, fact, description })` 记录应用报告的副作用。`EffectFact::Known` 表示应用确认发生；`Unknown` 表示证据不足。比如 provider 已接受写入，但连接断开，应用可记录不确定事实用于后续查询，而不能直接把超时解释为“没有写入”。

下面是独立的操作辅助函数，应在 scope 尚开放且已得到相应远端证据时调用。返回错误必须交给业务诊断路径处理；它只记录不确定性，不查询 provider，也不执行补偿。

```rust
use qubit_operation::{DiagnosticSummary, EffectFact, EffectRecord, EffectRecordError, OperationScope};

// Call while the scope is open, when the application cannot confirm a remote write.
pub fn record_uncertain_write(scope: &OperationScope) -> Result<(), EffectRecordError> {
    scope.record_effect(EffectRecord {
        operation_id: scope.id(),
        resource_id: None,
        fact: EffectFact::Unknown,
        description: DiagnosticSummary::new("provider accepted request; write confirmation lost"),
    })
}
```

effects 在继承预算内有界。容量耗尽返回错误、保留原记录、增加 overflow；停止或 scope 结束后不接受追加。应用必须处理返回错误，不能把缺少记录当作没有副作用。框架不执行补偿，effects 也不是持久事务日志。

## 11. 解释结算并处理清理失败

### 11.1 先明确报告层级

`SettlementReport` 本层计数和明细描述本地 scope；`external_settlement` 保留该操作适配器的原始领域报告。外层 `status` 合并本地与外部，优先级为 `RecoveryRequired`、`Transferred`、`Released`；`all_released()` 检查本层与外部报告。

**它不会遍历 `children`。** 子报告位于 `children[i].settlement`，其资源计数和状态不会扁平累加到父层。判断整棵已保留报告树时要递归，并关注子明细是否截断。例如下面的独立辅助函数只在已保留树完整且全部释放时返回 true：

```rust
use qubit_operation::SettlementReport;

// True only when the retained tree confirms release and no child details are missing.
pub fn retained_tree_released(report: &SettlementReport) -> bool {
    report.all_released()
        && !report.children_truncated
        && report.children.iter().all(|child| retained_tree_released(&child.settlement))
        && report.external_settlement.as_ref()
            .is_none_or(|external| retained_tree_released(external))
}
```

这不是完整审计证明：其他明细可能截断，外部证据也依赖适配器诚实报告。需要完整恢复索引时应持续维护恢复 owner/handle，不能只依赖父报告保留的诊断数组。外层 `recovery_count == 0` 也不能排除子层或外部恢复责任。同一资源在两层出现时按身份去重，不重复清理。

对于 `children`、`cleanup_failures`、`resource_reports`、`transfers` 和 `recoveries`，同时读明细、total 与 truncated；`effects` 还要读 overflow。明细为空不必然等于从未发生，稳定报告也不是持续刷新的恢复状态。后续恢复看 handle/store 的新 snapshot。

### 11.2 重试与移交

清理失败后，保留报告给出的 `RecoveryHandle`。clone 共享同一组恢复状态；不是第二次独立清理。`try_retry()` 只接纳尝试，必须保留返回的 attempt 并调用等待方法推进。下面两个独立函数分别展示直接重试一次和移交后从 store 重试：

```rust
use qubit_operation::{RecoveryHandle, RecoveryPhase, RecoveryStore};

pub fn retry_once(handle: &RecoveryHandle) -> Result<RecoveryPhase, String> {
    let attempt = handle.try_retry().map_err(|error| format!("{error:?}"))?;
    let report = attempt.wait().map_err(|error| format!("{error:?}"))?;
    Ok(report.recovery.phase)
}

pub fn hand_off(handle: &RecoveryHandle, store: &RecoveryStore) -> Result<(), String> {
    let transfer = handle.transfer_to(store).map_err(|error| format!("{error:?}"))?;
    let id = transfer.receipt.recovery();
    let attempt = store.try_retry(id).map_err(|error| format!("{error:?}"))?;
    let report = attempt.wait().map_err(|error| format!("{error:?}"))?;
    println!("recovery: {:?}", report.recovery.phase);
    Ok(())
}
```

`RecoveryStore` 在应用恢复服务启动时用 `RecoveryStore::new(RecoveryStoreOptions::default())` 创建。移交成功才有 receipt，store 接管重试路径；移交失败没有凭据，原 handle 仍可用。不要收到错误后丢弃唯一的责任入口。

| RecoveryPhase | 应用决策 |
| --- | --- |
| `Pending` | 清理未解决，可按应用策略尝试。 |
| `Running` | 已有尝试运行，不要并行再清理。 |
| `Resolved` | 已确认完成，无须重试。 |
| `Transferred` | store 已接管，使用 store 的重试路径。 |
| `Unconfirmed` | 状态无法确认且无自动动作，保留责任并升级处理。 |

`RetryError` 区分 `AlreadyRunning`、`AlreadyResolved`、`NoCapacity`、`ExecutorUnavailable`。普通失败可回到 Pending；panic 或直接驱动 future 的中断可成为 Unconfirmed，不能承诺每个失败都能自动重试。

直接驱动模式中，第一个等待者推进清理；丢弃正在驱动的 Pending future 会保留未完成动作并记录 Unconfirmed。若已派发到 store worker，取消观察不停止 worker。不要为了重试超时而立即创建第二个清理动作；先确认已有 attempt 和资源状态。

### 11.3 恢复历史和持久化边界

每个快照最多保留最近 64 条清理失败，按旧到新排列；`failures_total` 从上次确认成功后累计且饱和计数，`failures_truncated` 标记较早记录被淘汰。成功清理清空历史并重置字段。attempt report 的 failures 是完成时保留的历史，不是本次单独事件列表。

条目截断不会自动丢弃恢复 owner，但这也不是崩溃恢复或磁盘持久化保证。进程重启、远端副作用核实、重试间隔与人工介入由应用设计。需要完整历史时，在事件发生时写应用日志，而非事后依赖快照补齐。

## 12. 接入外部 provider

### 12.1 何时使用适配器

普通闭包直接执行任务时不需要适配器。只有外部 SDK、执行器或 provider 拥有工作执行权，而管理器仍须跟踪其生命周期时，才实现 `PreparedExternalOperation<T,E>`。适配器实现 `OperationControl + 'static`，因此也满足其 `Send + Sync` 要求；提交接口使用具体 `Sized` 类型，`T/E` 要求 `Send + 'static`。

准备实例最多绑定一次，且跨管理器也成立。用管理器分配的 OperationId 关联摘要和报告。通过同步或异步管理器的 `try_submit_external(options, Arc<P>)` 提交；拒绝保留原 Arc。不要在提交前绕过容量直接启动不可撤销的 provider 工作，再期待拒绝能撤销它。

### 12.2 实现两套互补端口

| 方法 | 责任 |
| --- | --- |
| `bind(&self, id)` | 一次性认领身份；AlreadyBound 拒绝准入；Failed 是已接纳的设置失败。 |
| `activate(self: Arc<Self>, scope)` | 接收管理器 scope，启动工作或移交输入所有权。 |
| `abandon(self: Arc<Self>)` | 释放已认领但未启动的准备状态。 |
| `poll_take_outcome` | 交出唯一 typed 结果。 |
| `request_stop` | 把协作式停止请求交给 provider。 |
| `snapshot` | 返回当前身份与生命周期事实。 |
| `poll_outcome` | 发布与 typed 结果一致的身份及结果种类摘要。 |
| `poll_release` | 独立报告输入不再被使用。 |
| `poll_settlement` | 独立提供领域结算报告。 |

所有返回 Pending 的 poll 都登记最新 waker；对应事实变化后必须唤醒。使用同一锁或等效同步将检查状态与登记 waker 协调起来，避免“检查未就绪—事件到达—才登记”的丢失唤醒。尽量在锁外 wake，回调应短且非阻塞。

typed 结果只发布一次，但摘要与释放、结算各自可独立就绪。驱动器核对身份和结果种类，不匹配会形成执行故障。输入释放或领域轮询失败不能伪造成功清理；错误适配器可能导致结算长期 Pending。不要用 panic 作为取消协议。

### 12.3 按时间线核对完整接入

[provider_read.rs](examples-0.3.0/provider_read.rs) 包含完整模拟 provider，Cargo 作为 library target 编译；`tests/v03_external_tests.rs` 执行其生命周期演示。它不调用真实 SDK，但完整覆盖：

1. bind 记录管理器身份，activate 保存 scope 并登记输入引用。
2. 应用通过 handle 请求停止，provider 记录请求。
3. provider 在仍持有输入时发布 Stopped 摘要与 typed outcome。
4. 调用方取得结果，此时输入 drop 计数仍为零，结算仍在等待。
5. 模拟输入使用结束，provider 释放自己的引用并发布 ChildRelease。
6. provider 独立发布领域报告，管理器保留在 external_settlement。
7. 本地清理和领域事实齐备后得到结算报告，再关闭管理器。

真实 provider 中，工作线程、SDK 回调或设备队列上的每个实际使用者都要持有对应 ResourceRef。停止通知、已取消响应、typed outcome 都不能替代真实访问结束。

领域报告不能等待管理器最终报告：后者正依赖领域报告，会形成循环。普通同步任务仍走同步池；同步管理器外部驱动走 `async_executor`，避免占据其依赖的同步 worker，但不保证任意应用等待环都能解决。

## 13. 关闭服务与恢复服务

关闭顺序应对应资源依赖：停止应用新请求，关闭直接准入，完成或停止已接纳工作，停止共享资源的新使用，关闭服务资源 scope，释放引用，再推进清理和恢复。

`request_shutdown(Drain)` 关闭新根准入，让已接纳且仍开放的父 scope 继续提交后代并完成；`Cancel` 另外请求未完成操作停止。保留 shutdown handle，调用 wait 或限时等待观察。关闭不能强制停止 provider，也不能替应用解决恢复。

`ShutdownSnapshot` 提供 active、queued、unconfirmed users、cleaning resources、recovery groups、service resources 等诊断字段。当前实现的 service_resources 按服务资源 scope 计数，unconfirmed_users 来自 Unconfirmed 恢复记录，并非全部 ResourceRef 的数量；应结合具体 scope 和恢复快照判断。两种关闭模式都会结束管理器拥有的服务资源 scope。根据非零项找责任方：请求已返回但 shutdown Pending 时，先查资源使用者和恢复，不要只盯业务队列。

将恢复责任成功交给独立 store 后，应用要继续拥有该 store。store 的 `request_shutdown()` 关闭新接纳；等待 handle 才推进执行器停止与 join，且不会自动替所有未解决组重试。先推进或明确处置未解决组，再完成恢复服务关闭。不要把应用退出当作已释放证明。

## 14. 排障与开发验证

| 症状 | 检查步骤与处理 |
| --- | --- |
| 结果已到、结算等待 | 检查 T/E、clone 和 provider 是否保留 ResourceRef；结束访问再释放。 |
| 父 all_released 为 true，但子任务需恢复 | 检查 children 嵌套报告与 truncated；该方法不递归子树。 |
| 业务已完成，新提交仍 Busy | 检查未结算根额度、未消费结果、组与子路径许可。 |
| provider 已停止但仍等待 | 分别检查 outcome、release、domain settlement 和 waker；不能补造释放事实。 |
| 外部提交满、同步池很空 | 检查 async_executor 的 task capacity 和驱动回调。 |
| 预留成功、commit 失败 | 检查停止/结束竞态；从错误取回资源和预留并安排清理。 |
| effects 或报告缺明细 | 检查 total、truncated、overflow，结合应用日志。 |
| 重试返回 ExecutorUnavailable | 不只查线程池，也检查是否 Unconfirmed 且没有安全重试动作。 |
| 关闭一直 Pending | 根据 shutdown snapshot 找实际使用者、清理或恢复责任方。 |

从 crate 根目录执行：

```bash
cargo run --example upload_inspection --locked
cargo run --example family_and_capacity --locked
cargo check --examples --locked
cargo test --locked
```

适配器开发重点参考 `tests/v03_external_tests.rs`；scope/组与 effects 参考 `tests/v03_scope_tests.rs`；生命周期分离参考 `tests/v03_lifecycle_tests.rs`。文档中的精确签名以当前 crate 根公开 API 为准，私有 `src/v02/` 目录名不表示需要导入旧版模块。

# 用户手册（0.1.0）

[中文 README](../README.zh_CN.md) · [API 文档](https://docs.rs/qubit-operation) · [适配器接入指南](adapter-guide.zh_CN.md)

本文面向有经验的 Rust 开发者，以 MIME 检测为例，说明 `qubit-operation` 的核心 API、资源归属和错误处理方式。

## 1. 概述：管理任务执行和资源清理

`qubit-operation` 用于管理任务的执行、取消和收尾，支持组合子任务、限制任务数量，并在任务结束后协调资源清理。

可以把它理解为跨任务的 `try … finally …`：任务返回业务结果后，任务管理器仍会跟踪子任务和资源的收尾情况。清理成功时，库记录释放结果；清理失败时，则保留恢复所需的对象和状态。

在 Rust 中，值离开作用域时通常会调用 `Drop`。不过，本地句柄被丢弃时，其他线程、外部进程或原生代码可能仍在使用资源，应用需要等这些使用者结束后才能清理。

以下场景都需要处理这个问题：

- **文件检测：** 用户取消上传请求时，`file` 或 `ffprobe` 可能仍在读取暂存文件，应用需要等进程停止使用后再删除文件。
- **命令执行：** 业务结果已经可用，但管道、输入辅助任务或进程回收尚未完成。应用需要分别检查结果和收尾状态。
- **模型推理：** 原生推理调用无法强制中断，即使调用方放弃结果，输入缓冲区、模型租约和执行名额也需保留到推理结束。
- **服务关闭：** 服务停止接收新请求后，已有请求仍需收尾；如果清理失败或关闭超时，应用还需继续处理。

本库用一组接口协调这些工作：

| 接口 | 作用 |
| --- | --- |
| `OperationOutcome` | 返回业务结果 |
| `ResourceUse` | 用租约标记资源仍在使用 |
| `OwnedCleanup` | 由应用实现，执行资源清理 |
| `SettlementReport` | 报告资源收尾结果 |

应用或领域服务负责执行具体操作并确认资源是否已释放，本库据此安排收尾。取消只是请求停止，应用仍需等待使用者退出，并检查清理结果。

本文对应 0.1.0。第 2 节通过示例展示核心 API，第 3 节介绍主要组件；第 4—7 节展开任务组合、资源管理、执行模型和服务关闭，第 8—10 节供诊断和排障时查阅。

## 2. 典型场景：MIME 检测和输入清理

### 2.1 场景和资源归属

上传服务先暂存文件，再调用 `file` 识别类型；需要细化媒体类型时，继续调用 `ffprobe`，也可以根据配置选择模型后端。

返回 MIME 结果后，输入文件可能仍在使用，因此应用必须等所有检测后端停止访问输入后再清理文件。

```text
上传 → 暂存输入 → file 检测 → 可选 ffprobe → MIME 结果
          └──── 输入租约覆盖实际使用期 ────┘
                           ↓
                 使用者结束 → 清理 → 结算报告
```

下面的示例用内存缓冲区代替暂存文件，用 PDF 文件头检查代替外部命令。这样无需安装外部工具，也能展示资源登记、租约、结果读取和清理之间的关系。第 2.4 节说明实际服务的接入方式。

### 2.2 依赖和完整示例

需要 Rust 1.94 或更新版本。crate 没有默认 feature，也没有运行时依赖。

```toml
[dependencies]
qubit-operation = "^0.1"
```

以下代码可作为应用的 `src/main.rs`，也可通过仓库中的 [`mime_lifecycle.rs`](../examples/mime_lifecycle.rs) 运行：在 crate 目录执行 `cargo run --example mime_lifecycle --locked` 即可。

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
use qubit_operation::report::{CleanupOutcome, OperationOutcome, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};

// 应用提供的清理对象，持有清理所需的数据。
struct StagedInput(Arc<Vec<u8>>);

impl OwnedCleanup for StagedInput {
    fn begin(self: Box<Self>, _: CleanupContext)
        -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>
    {
        Box::pin(async move {
            drop(self.0);
            CleanupOutcome::Cleaned
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let mut detection = supervisor.start(ScopeOptions::default(), |scope| async move {
        let bytes = Arc::new(b"%PDF-1.7\n".to_vec());
        let resource = scope.register_resource(StagedInput(Arc::clone(&bytes)))
            .map_err(|_| "cannot register input cleanup")?;
        let input_use = scope.acquire_use(resource).map_err(|_| "input is closing")?;
        let mime = bytes.starts_with(b"%PDF-").then_some("application/pdf");
        drop(bytes);
        drop(input_use);
        Ok::<Option<&'static str>, &'static str>(mime)
    })?;

    // 即使业务失败，也等待收尾完成。
    let outcome = detection.result_blocking();
    let settlement = detection.released_blocking();
    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));

    match outcome? {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection outcome: {other:?}"),
    }
    let report = settlement?;
    if report.all_released {
        println!("input released");
    } else {
        eprintln!("input settlement: {:?}; failures: {:?}",
            report.resource_phase, report.cleanup_failures);
    }
    if !shutdown.is_complete() {
        eprintln!("shutdown pending: {shutdown:?}");
    }
    Ok(())
}
```

正常输出：

```text
MIME: application/pdf
input released
```

`StagedInput` 持有一份缓冲区引用，并在清理时释放它。缓冲区会随最后一个 `Arc` 的释放而释放，因此应用必须确保其他引用已经结束使用。

这个内存对象登记失败后可以直接丢弃，而文件或原生资源可能仍需清理。如何取回未被接管的清理对象，见第 5.1 节。

### 2.3 核心 API 的作用

| 调用 | 在本例中的作用 |
| --- | --- |
| `OperationSupervisor::new` | 创建任务管理器，设置容量限制 |
| `start(options, body)` | 提交任务，并把作用域传给任务闭包 |
| `register_resource(owner)` | 登记输入的清理对象 |
| `acquire_use(id)` | 获取输入租约，标记检测器仍在使用输入 |
| 释放 `ResourceUse` | 归还租约，表示本使用者已停止访问输入 |
| `result_blocking()` | 读取一次业务结果 |
| `released_blocking()` | 等待收尾报告 |
| `shutdown_blocking(...)` | 停止接收新任务，等待已有工作并返回关闭状态 |

读取结果和等待收尾是两个独立操作，但两者之间未必有时间间隔：本例可能在调用方读取结果之前就已完成清理。

将输入改为不以 `%PDF-` 开头的字节，会得到 `Succeeded(None)`，但仍需清理输入。业务失败、停止或发生 panic 时，同样要处理收尾。

如果取消请求与任务完成同时发生，调用方可能读到已经确定的业务结果，因此发出取消请求后不一定得到 `Stopped`。

### 2.4 接入实际服务

应用在服务启动时创建任务管理器，并在运行期间持有它；每次上传提交一个独立操作，同时将暂存服务和命令服务传给任务。

清理对象必须持有清理所需的数据，不能依赖已经返回的函数中的局部变量。命令适配器则应持有输入租约，直到进程和输入辅助任务停止访问输入。

| 组件 | 负责的工作 |
| --- | --- |
| 文件服务 | 暂存和删除文件，提供清理对象，报告删除结果 |
| 命令服务 | 管理进程、管道和进程回收，报告实际状态 |
| MIME 服务 | 选择检测后端，处理未匹配结果和媒体细化结果 |
| 模型服务 | 在推理返回前保留输入和执行名额 |
| 应用 | 处理业务结果、清理失败和后续恢复 |

rs-fs、rs-local-files、rs-command、rs-mime 和 rs-magika 的设计说明了这些组件的分工。本文示例只使用当前 rs-operation 的公开 API。

实现适配器时，还需遵循[适配器接入协议](adapter-guide.zh_CN.md)。主要要求是：提交成功后再启动外部工作；同一资源只登记一次；实际释放后再确认完成。

## 3. 核心流程和组件

### 3.1 创建、准入和提交

`OperationSupervisor` 管理任务数量、结果存储和清理并发数，并在接收任务时检查容量。这个检查并接纳任务的过程称为“准入”。

`start` 接受 `FnOnce(OperationScope) -> Future<Output = Result<T, E>>`，其中闭包和 future 必须持有执行所需的数据，且闭包、future、`T` 和 `E` 都须满足 `Send + 'static`。

成功返回任务句柄只表示任务已被接纳，此时任务可能仍在排队。

| 接口 | 用途 |
| --- | --- |
| `start` | 尝试接纳异步任务；容量不足时返回 `StartError<F>` |
| `start_wait` / `start_wait_blocking` | 等待准入；等待者数量也有限制 |
| `start_blocking` | 把同步任务提交给指定执行器；需要立即取得根任务执行名额 |
| `run_to_completion` | 等待业务结果和收尾报告；结果不应持有当前作用域的资源租约 |
| `try_admit_body` / `start_wait_body` | 供需要分别处理准入和执行的底层集成使用 |

下面的配置限制活动任务、排队任务和未读取结果的数量。应用应根据实际负载设置这些值。

```rust
use qubit_operation::report::SupervisorLimits;
use qubit_operation::supervisor::OperationSupervisor;

let supervisor = OperationSupervisor::new(SupervisorLimits {
    active_roots: 8,
    queued_roots: 32,
    unobserved_results: 32,
    ..SupervisorLimits::default()
});
```

提交失败时，`StartError<F>` 会保留尚未执行的任务闭包。调用 `reason()` 可查询失败原因，调用 `into_parts()` 则可同时取回原因和闭包。

调用方可以拒绝请求，也可以保留闭包供稍后重试，但仍需处理闭包捕获的输入和资源。

### 3.2 作用域、配置和上下文

`OperationScope` 将一次操作的子任务和资源组织在一起，并跟踪它们的收尾。任务执行仍由执行器负责。

创建作用域时，用 `ScopeOptions` 设置容量、子操作失败策略和上下文。以下配置允许独立子任务继续执行：

```rust
use qubit_operation::scope::{ScopeOptions, ScopePolicy};

let mut options = ScopeOptions::default();
options.policy = ScopePolicy::Collect;
options.limits.child_operations = 16;
options.limits.resources = 32;
// 将 options 传给 supervisor.start(options, body)。
```

`OperationContext` 在相关操作之间传递停止请求、期限和预算约束，创建或继承上下文时使用 `ContextOptions`。输出格式、后端选择和文件权限等业务配置仍由相应服务管理。

下面的函数用于配置检测任务。应用提供时钟和绝对截止时间，函数返回可传给 `start` 的配置。

```rust
use std::sync::Arc;
use qubit_operation::context::{ContextOptions, Deadline};
use qubit_operation::protocol::ClockSource;
use qubit_operation::scope::ScopeOptions;

fn detection_options(clock: Arc<dyn ClockSource>, deadline: Deadline) -> ScopeOptions {
    ScopeOptions {
        context: ContextOptions {
            clock: Some(clock),
            deadline: Some(deadline),
            ..ContextOptions::default()
        },
        ..ScopeOptions::default()
    }
}
```

`Deadline` 表示某个单调时钟上的绝对截止时间，不同时钟域的时间刻度不能直接比较。`ClockSource` 需要提供当前时间，并在截止时间到达时唤醒等待者。

子上下文只能收紧继承的期限，不能通过新建子操作重置总时限。接纳任务时若期限已过，库会拒绝任务；若在执行期间到期，则会请求停止，但不能保证中断同步系统调用。

可选的 `BudgetLease` 由调用方实现预算检查。内存、字节数等具体用量由应用或相应服务统计。

### 3.3 业务结果、观察者和收尾报告

任务句柄 `OperationHandle<T, E>` 用于读取业务结果、请求取消和等待收尾。业务结果保留 `T` 或 `E` 的具体类型，但只能取出一次。

`OperationObserver<T, E>` 可以反复查看状态和收尾结果，但不会获得第二份业务结果。调用句柄的 `observer()` 方法可创建观察者，同时保留原句柄。

| 接口 | 返回结果 |
| --- | --- |
| `result().await` / `result_blocking()` | `Result<OperationOutcome<T, E>, ObservationError>`；业务结果只能取出一次 |
| `released().await` | 直接返回 `SettlementReport` |
| `released_blocking()` | 返回 `Result<SettlementReport, ObservationError>`；可能检测到阻塞死锁风险 |
| 观察者的收尾接口 | 可反复查看收尾结果，不取走业务结果 |

下面的函数接收检测任务句柄，依次等待业务结果和收尾报告。即使读取业务结果时出错，也会等收尾完成后再返回错误。

```rust
use qubit_operation::operation::OperationHandle;
use qubit_operation::report::{ObservationError, OperationOutcome, SettlementReport};

async fn collect_detection(
    mut detection: OperationHandle<Option<&'static str>, &'static str>,
) -> Result<(OperationOutcome<Option<&'static str>, &'static str>, SettlementReport), ObservationError> {
    let outcome = detection.result().await;
    let settlement = detection.released().await;
    Ok((outcome?, settlement))
}
```

| `OperationOutcome` | 含义 |
| --- | --- |
| `Succeeded(T)` | 业务执行成功 |
| `Failed(E)` | 业务执行失败，错误保留在 `E` 中 |
| `ChildFailed` | 必要子操作失败 |
| `Stopped` | 操作停止，未得到确定的业务结果 |
| `Panicked` | 业务执行发生 panic，结果包含摘要 |

观察失败和业务失败是两类错误，应用应分别处理；业务结果也不能代替资源收尾报告。

### 3.4 取消、丢弃句柄和放弃结果

`request_cancel(reason)` 请求任务配合停止，但不可中断的调用仍需自行返回，在此之前不能提前归还输入租约或执行名额。

丢弃观察者不会取消操作，而丢弃尚未结束的任务句柄会请求停止，并放弃接收业务结果。这个过程不会阻塞等待任务结束。

下面的函数适用于调用方取消请求、应用继续在后台等待收尾的情况。函数返回观察者，供应用保存和使用。

```rust
use qubit_operation::operation::{OperationHandle, OperationObserver};
use qubit_operation::report::StopReason;

fn cancel_detection<T: Send + 'static, E: Send + 'static>(
    detection: OperationHandle<T, E>,
) -> OperationObserver<T, E> {
    detection.request_cancel(StopReason::UserCancelled);
    detection.detach()
}
```

`detach` 放弃接收业务结果并返回观察者，但本身不会请求取消，所以上例先调用 `request_cancel`。

如果仍需读取结果，应保留任务句柄，另外创建观察者。只丢弃一次等待所用的 future，也不会因此取消仍由句柄持有的操作。

### 3.5 返回值中的资源

如果返回值携带当前作用域的 `ResourceUse`，调用方不能持有租约并等待同一作用域完成收尾。清理要等租约归还，调用方又在等清理，双方会一直等待。

应用可以在任务内部结束资源使用，也可以先将资源及其清理责任转交给另一个已接纳的作用域，再返回供调用方使用的句柄。

复制路径或原生句柄不会转移资源的所有权。转交失败时，原作用域仍负责管理资源。

## 4. 子操作组合和执行策略

### 4.1 子作用域和外部操作

`child_scope(options)` 创建子作用域，沿用父级的生命周期关系。应用仍需自行提交要执行的任务。`join().await` 用于等待并汇总子任务与资源的收尾结果。

接入领域服务已有的任务时，使用 `reserve_child` 和 `ManagedOperationControl`，让作用域能够请求任务停止并反复查询状态。控制接口中的方法应快速返回，避免阻塞调用线程。

| 方法 | 适配器的职责 |
| --- | --- |
| `request_stop` | 请求停止；重复调用应安全且不阻塞 |
| `snapshot` | 返回大小受限的状态快照，不执行 I/O |
| `poll_business` | 返回稳定的业务摘要，不取走具体类型的业务结果 |
| `poll_released` | 资源确认释放后返回结果；未完成时登记唤醒并返回 `Pending` |

进程适配器应区分“已发送终止信号”和“已回收进程”。模型适配器应等原生推理实际返回后，再确认相关资源已释放。

### 4.2 预留、绑定和提交

登记分为三步：预留名额、绑定尚未启动的控制对象、提交登记。提交成功后，才可以启动进程等外部工作。

下面的函数供命令适配器调用。适配器实现 `control`，并传入输入资源的使用租约。进程由适配器在登记成功后启动。

```rust
use std::sync::Arc;
use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::resource::ResourceUse;
use qubit_operation::scope::{ChildRequirement, OperationScope, ScopeError};

fn register_command(
    scope: &OperationScope,
    control: Arc<dyn ManagedOperationControl>,
    input_use: ResourceUse,
) -> Result<(), ScopeError> {
    let mut reservation = scope.reserve_child(ChildRequirement::Required)?;
    reservation.attach_with_uses(control, vec![input_use])?;
    reservation.commit()
}
```

适配器按以下顺序调用：

1. 取得执行许可，准备尚未启动的控制对象。
2. 调用登记函数。
3. 提交成功后，通过执行器启动工作。
4. 实际停止使用并释放资源后，更新控制对象并唤醒观察者。

准备或提交失败时，适配器应放弃启动，并归还输入和执行许可。控制对象还需处理“提交后、启动前”收到的停止请求。

丢弃尚未提交的 reservation 会归还预留名额。如果操作已取消、作用域已关闭，或尚未绑定控制对象，`commit` 会失败。

上例取得了输入租约，失败返回时会随局部变量一起释放；适配器持有的其他输入和许可则需要另行处理。

### 4.3 必要子操作、可选子操作和失败策略

| 配置 | 必要子操作失败时 | 对父操作的影响 |
| --- | --- | --- |
| 默认 `FailFast` | 请求兄弟任务停止 | 父操作不能报告成功 |
| `Collect` | 独立子操作可以继续 | 父操作仍不能报告成功 |
| `ChildRequirement::Optional` | 该子操作不作为父操作成功的必要条件 | 父任务自行处理其结果，决定是否降级 |

例如，`ffprobe` 只用于细化已识别的媒体类型。如果允许细化失败后返回基本类型，可以将它设为可选子操作，并在 MIME 任务中实现降级处理。

仅设置 `Collect` 不会把必要子操作变成可选子操作。无论采用哪种策略，已登记的任务和资源都仍需收尾。

## 5. 资源依赖、转移和恢复

### 5.1 登记资源和获取租约

应用通过 `OwnedCleanup` 实现资源清理，并调用 `register_resource` 登记清理对象，取得 `ResourceId`。之后获取租约、设置清理依赖和转移资源时，都使用这个 ID。

应用应先完成登记，再把资源交给其他使用者。同一资源只登记一次，上层封装应复用底层的登记信息。

每个使用者通过 `acquire_use` 单独取得不可克隆的 `ResourceUse`，并在使用结束后归还。调用 `close_for_use(id)` 后，不能再获取新租约，但原有租约仍然有效。等这些租约全部归还，资源才可以清理。

登记失败时，`RegisterError` 保留未被接管的清理对象，应用应取回它，另行清理、保存或转交。下面的函数返回失败原因和清理对象。

```rust
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::resource::{RegisterError, RegisterErrorKind};

fn retain_rejected_cleanup(error: RegisterError)
    -> (RegisterErrorKind, Box<dyn OwnedCleanup>)
{
    (error.kind(), error.into_owner())
}
```

调用方应将返回对象交给负责后续处理的组件。如果自行调用 `begin`，则需要提供 `CleanupContext`，驱动返回的清理 future，并处理结果。保存文件路径只是在保留处理线索，文件是否已删除仍需确认。

### 5.2 清理接口和结果

`OwnedCleanup::begin` 接管清理对象的所有权，返回的 future 必须持有清理所需的状态，并满足 `Send + 'static`。

`CleanupContext` 为清理提供独立上下文，因此即使业务上下文已经取消，也仍需执行清理。阻塞清理动作应交给相应执行器，避免阻塞管理线程。

| `CleanupOutcome` | 含义和处理方式 |
| --- | --- |
| `Cleaned` | 资源已确认释放；只发送关闭请求还不能返回此结果 |
| `Transferred(receipt)` | 接收方已接受清理责任；保留转交凭据 |
| `FailedWithRecovery(failure, owner)` | 清理失败，并保留恢复对象；应用继续处理 |

如果清理 future 可能发生 panic，适配器应另行保留恢复所需的对象或状态。唯一对象被移入 future 后，框架无法保证在 panic 后重新构造它。可使用独立的清理封装对象（capsule）或注册表，具体要求见适配器指南。

### 5.3 设置清理顺序

如果暂存目录必须在其中的文件清理后才能删除，调用 `cleanup_after(directory, file)`。第一个参数是后清理的资源，第二个参数是前置资源。

```rust
use qubit_operation::report::ResourceId;
use qubit_operation::resource::DependencyError;
use qubit_operation::scope::OperationScope;

fn order_staging_cleanup(
    scope: &OperationScope,
    directory: ResourceId,
    file: ResourceId,
) -> Result<(), DependencyError> {
    scope.cleanup_after(directory, file)
}
```

应用在资源登记完成后设置依赖，参数使用对应登记返回的 ID。如果依赖无效或形成循环，调用会失败，应用应先处理错误，再按预期顺序清理。

前置资源清理失败或状态未确认时，后续资源仍需管理。此时整体不能视为 `Released`。

### 5.4 跨作用域转移资源

`transfer_resource_to(id, destination)` 转交资源及其依赖组，返回目标作用域中的资源 ID。源和目标必须使用不同的注册表，目标还需有足够的登记容量。

```rust
use qubit_operation::report::ResourceId;
use qubit_operation::resource::TransferError;
use qubit_operation::scope::OperationScope;

fn hand_over_input(
    source: &OperationScope,
    destination: &OperationScope,
    input: ResourceId,
) -> Result<ResourceId, TransferError> {
    source.transfer_resource_to(input, destination)
}
```

转交成功后，调用方使用新的资源 ID，并保持目标作用域存活，直到相关工作完成收尾。转交失败时，原清理对象仍由源作用域管理。

转交成功表示责任已由目标接管，资源本身可能仍在使用。涉及释放证明时，使用 `TransferReceipt` 和 `ReleaseReceipt`。普通业务 ID 不能代替这些凭据。

### 5.5 保存和取回恢复对象

`RecoveryRegistry` 保存后续恢复所需的对象，容量在创建时指定。登记成功后返回 `RecoveryKey<R>`，之后取回对象时需要使用这份 key。

key 同时标识注册表、资源、登记代次和对象类型，不能用自行构造的编号或其他类型的 key 代替。

下例使用独立恢复表保存删除失败的文件路径。应用之后可取回路径，再尝试删除。

```rust
use std::path::PathBuf;
use qubit_operation::report::{RecoveryKey, ResourceId};
use qubit_operation::resource::RecoveryRegistry;

fn remember_failed_delete(
    recovery: &RecoveryRegistry,
    resource: ResourceId,
    path: PathBuf,
) -> Result<RecoveryKey<PathBuf>, PathBuf> {
    recovery.register(resource, path)
}

fn claim_failed_delete(
    recovery: &RecoveryRegistry,
    key: RecoveryKey<PathBuf>,
) -> Result<PathBuf, RecoveryKey<PathBuf>> {
    recovery.take_recovery(key)
}
```

应用用 `RecoveryRegistry::new(capacity)` 创建恢复表，并保存登记返回的 key。容量不足时，`register` 归还原值。取回对象后，应用负责重试、保留或转交。

对于作用域内部清理失败，应使用原作用域的 `take_recovery_key` 和 `take_recovery`。其他注册表的 key 不能用于查询它。

`keep` 检查恢复对象是否仍在表中。使用 `retry_cleanup` 时，适配器需要提供重试动作，并继续驱动返回的 future。重试仍可能失败。

结算报告是历史快照，后续恢复不会修改旧报告，因此应用需要另行保存恢复结果和资源归属记录。

## 6. 执行模型和适配器集成

### 6.1 PortableDriver

普通 `start` 通过 `PortableDriver` 推进 future，要求 future 持有执行所需的数据、满足 `Send + 'static`，并在状态变化时正确唤醒等待者。

如果 future 依赖特定运行时的 I/O reactor，应由对应运行时驱动。阻塞文件 I/O 或耗时推理应交给专门的执行器，避免阻塞 `poll`。

### 6.2 BlockingExecutor 和 ThreadExecutor

同步任务通过 `start_blocking` 提交给执行器，例如提供固定工作线程数和有限等待队列的 `ThreadExecutor`。

以下示例展示同步文件头检查的提交和结果读取。接入真实进程时，还需要登记相关子操作和资源。

```rust
use std::time::Duration;
use qubit_operation::driver::ThreadExecutor;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let executor = ThreadExecutor::new(2, 8)?;
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let mut detection = supervisor.start_blocking(&executor, ScopeOptions::default(), |_scope| {
        Ok::<_, &'static str>(b"%PDF-1.7".starts_with(b"%PDF-"))
    })?;
    let outcome = detection.result_blocking();
    let settlement = detection.released_blocking();
    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    println!("{outcome:?}; {settlement:?}; {shutdown:?}");
    Ok(())
}
```

`start_blocking` 需要立即取得根任务执行名额，如果只有排队名额，会返回 `Busy`。执行器饱和时也会拒绝提交，并保留尚未执行的闭包。

不要在同一执行器的工作线程，或库的 portable 工作线程中，执行可能相互等待的阻塞提交和观察。

取消不能强制终止已经执行的同步闭包。丢弃 `ThreadExecutor` 时，它会关闭队列并等待工作线程退出。因此，不能在它自己的工作线程上销毁它，也不能在运行中任务退出所依赖的线程上销毁它。

已有线程池可以实现 `BlockingExecutor`，直接复用现有执行能力。

### 6.3 ExternalDriver 和 DriverLease

接入外部运行时时，适配器通过 `ExternalDriver` 提供任务唤醒和运行时状态查询。适配器还负责在对应运行时中推进已接纳的任务。

`DriverLease` 记录外部执行状态，克隆出的 lease 共享同一状态。

下面的函数在适配器收到运行时结束事件后调用。只有确认任务退出且资源已释放，才能把 `confirmed_release` 设为 true。

```rust
use qubit_operation::driver::{DriverLease, ExternalDriverError};

fn record_runtime_end(lease: &DriverLease, confirmed_release: bool)
    -> Result<(), ExternalDriverError>
{
    if confirmed_release {
        lease.mark_released()
    } else {
        lease.mark_lost()
    }
}
```

`Active` 只能转换一次，目标状态为 `Released` 或 `DriverLost`；重复设置最终状态会返回错误。`DriverLost` 表示释放未确认，同一 lease 之后也不能再改成 `Released`。

如果会话必须在特定线程使用，适配器要保证创建、poll、关闭和析构都在正确线程执行。运行时应保持可用，直到已接纳的工作完成收尾。接入 Tokio 时，目前需要应用自行实现适配器。

## 7. 容量、背压和服务生命周期

### 7.1 容量限制

| 默认限制 | 数值 | 限制的对象 |
| --- | --- | --- |
| `active_roots` | 4 | 正在执行的根任务 |
| `queued_roots` | 64 | 已接纳、等待执行的根任务 |
| `admission_waiters` | 64 | 等待准入的调用方 |
| `cleanup_concurrency` | 4 | 同时执行的清理工作 |
| `unobserved_results` | 64 | 尚未取走的业务结果 |
| `unobserved_events` | 256 | 保存的诊断事件 |
| 每个作用域的子操作 / 资源 | 64 / 128 | 登记项 |

这些限制控制任务和登记项的数量，上传字节数、进程数、模型内存，以及 `T` 和 `E` 的大小仍需由应用管理。

未取走的结果会占用结果槽，直到读取或明确放弃；恢复对象也会占用相应登记容量。队列满时，应用应减少提交、等待容量或拒绝请求，避免无限重试。

`active_roots()`、`queued_roots()` 和 `reserved_results()` 可用于定位容量占用。扩大结果槽不能解决恢复对象无人处理的问题。

### 7.2 长期服务资源

模型会话或文件服务实例通常会被多个请求复用，可以用 `ServiceGroup` 管理这些请求，用 `ServiceResourceScope` 持有长期资源，而每个请求的临时输入仍由各自的作用域管理。

下面的示例展示服务组如何提交请求、读取结果并关闭。

```rust
use std::time::Duration;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(8)?;
    let mut detection = service.start(ScopeOptions::default(), |_scope| async {
        Ok::<_, &'static str>(Some("application/pdf"))
    })?;
    let outcome = detection.result_blocking();
    let settlement = detection.released_blocking();
    let service_report = service.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    let report = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    println!("{outcome:?}; {settlement:?}; {service_report:?}; {report:?}");
    Ok(())
}
```

`service.resource_scope(capacity)` 创建服务资源作用域，使用其 `register_resource` 和 `acquire_use` 接口可登记并使用长期资源。相应的清理对象仍需实现 `OwnedCleanup`。

`ServiceOperationHandle` 提供结果读取、取消和收尾观察接口。请求名额会一直保留到操作释放。调用方不再等待结果时，名额不一定已归还。

手动调用 `try_admit` 会获得请求许可，而调用 `service.start` 时由库管理许可。使用时应明确由谁持有许可，避免重复计数。

### 7.3 关闭和超时后的等待

`Drain` 停止接收新任务，并等待已有工作完成。`Cancel` 还会请求已有工作停止，然后等待收尾。`close()` 只停止接收新任务，返回时已有工作可能仍在运行。

关闭报告通过 `pending_requests`、`pending_recovery` 和 `pending_resources` 分别记录待处理工作，调用 `is_complete()` 可检查是否已全部完成。

如果等待结束后仍有工作未完成，下例返回一个观察者。应用保存它后，可通过 `wait_blocking` 或异步接口继续等待。

```rust
use std::time::Duration;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode, ShutdownObserver};

fn begin_shutdown(supervisor: &OperationSupervisor) -> Option<ShutdownObserver> {
    let observer = supervisor.shutdown_observer();
    let report = supervisor.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
    if report.is_complete() {
        None
    } else {
        eprintln!("shutdown pending: {report:?}");
        Some(observer)
    }
}
```

超时只结束本次等待，不会强制结束任务，也不会把未确认资源标记为 `Released`。

如果关闭持续未完成，应分别检查任务是否仍在运行、资源释放是否已确认，以及恢复对象是否仍待处理。应用需要继续保存相关对象，或明确转交后续处理工作。

## 8. 错误和诊断

| 出错环节 | 类型或状态 | 处理方式 |
| --- | --- | --- |
| 准入和提交 | `AdmissionError`、`StartError<F>` | 检查容量、关闭状态和期限，取回尚未执行的闭包 |
| 资源登记 | `RegisterError` | 检查原因，取回清理对象并安排处理 |
| 作用域、依赖或转移 | `ScopeError`、`DependencyError`、`TransferError` | 暂停启动外部工作，保留资源，再处理错误 |
| 业务执行 | `OperationOutcome::Failed(E)` 等 | 按业务规则处理，并继续等待收尾 |
| 结果观察 | `ObservationError` | 处理观察错误，另行确认业务和资源状态 |
| 资源清理 | `CleanupFailure` 和恢复对象 | 保存资源 ID、失败阶段、摘要及原始错误，安排恢复 |
| 释放未确认 | `ResourcePhase::Unconfirmed` | 继续观察，或保留后续处理所需的资源 |

`SettlementReport` 包含操作 ID、资源阶段、整体释放标志、子操作摘要、清理失败、转交记录和副作用记录。日志中应保留操作和资源的 ID，便于关联问题。

应用可以用以下函数检查报告：

```rust
use qubit_operation::report::SettlementReport;

fn inspect_settlement(report: &SettlementReport) {
    if !report.all_released {
        eprintln!("operation={:?}, phase={:?}, unconfirmed={}",
            report.operation_id, report.resource_phase, report.unconfirmed_count);
        for failure in report.cleanup_failures.iter() {
            eprintln!("cleanup: {failure:?}");
        }
    }
    if report.effect_overflow != 0 {
        eprintln!("omitted effects: {}", report.effect_overflow);
    }
}
```

诊断摘要和列表都有容量限制，需要完整历史时，应用应另外记录日志。列表也可能为空，因此读取前应先检查，或直接遍历。

应用通过 `record_effect` 记录已经发生的副作用，形成 `EffectRecord`，并负责保证记录准确。停止任务不会撤销已完成的写入，业务失败时也可能已有部分操作生效，应用应据此决定是否重试或补偿。

## 9. 排障

| 症状 | 检查和处理顺序 |
| --- | --- |
| `Busy` | 检查活动和排队任务数，再查未取结果、等待准入的调用方数量和执行器容量；读取旧结果、减少提交或拒绝请求 |
| 取消后仍未释放 | 检查不可中断调用是否返回，再查租约、输入辅助任务、清理依赖和驱动器状态 |
| 业务成功，但仍需恢复 | 检查 `cleanup_failures` 和资源 ID；用原作用域或注册表的 key 取回对象，再重试、保留或转交 |
| `WouldDeadlock` | 检查是否在库管理的工作线程上阻塞提交或等待；改用异步等待，或移到独立应用线程 |
| 关闭一直未完成 | 分别检查请求、服务资源和恢复对象数量；确认运行时仍能推进清理 |
| `Unconfirmed` / `DriverLost` | 检查执行端是否仍可用，以及能否确认资源释放；确认前继续保留相关资源和状态 |
| 登记或转移失败 | 检查容量、关闭状态、资源 ID 和所属注册表；处理归还的清理对象或输入 |

## 10. 限制和使用建议

- 任务和清理 future 必须持有执行所需的数据，并满足接口要求的 `Send + 'static`。
- `poll`、控制接口和用户值的析构应及时返回。阻塞工作应交给相应执行器。
- 租约需要使用者配合。应用另外复制的裸句柄和数据引用，不会自动受到租约约束。
- 只有实际确认释放后，才能报告 `Released`。`RecoveryRequired` 和 `Unconfirmed` 都需要继续处理。
- 转移和恢复接口管理进程内的资源归属。需要跨进程恢复时，应用还需持久化必要信息。
- 默认容量可作为初始配置。实际取值应结合工作负载、吞吐和延迟要求评估。

## 11. 延伸阅读

- [中文 README](../README.zh_CN.md)：库的定位和核心场景。
- [API 文档](https://docs.rs/qubit-operation)：方法签名、配置字段和错误类型。
- [适配器接入指南](adapter-guide.zh_CN.md)：提交顺序、线程要求、清理对象和验收方法。
- 示例：[MIME 生命周期](../examples/mime_lifecycle.rs)、[异步观察](../examples/async_pipeline.rs)、[不可中断工作取消](../examples/inference_cancel.rs)。后两者补充说明执行和取消行为。
- 相关设计：rs-command 0.12.0、rs-local-files 0.6.0、rs-mime 0.22.0、rs-magika 0.19.0、rs-fs 0.4.0。可参考其中的组件分工，具体 API 以对应实现为准。

英文手册见[User guide](user_guide.md)，覆盖相同的主要操作路径；类型签名和错误细节以源码及 API 文档为准。

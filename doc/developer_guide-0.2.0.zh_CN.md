# rs-operation 0.2.0 中文开发者手册

[0.2 API 设计](design-0.2.0.zh_CN.md) · [中文 README](../README.zh_CN.md) · [历史版 0.1 中文用户指南](user_guide.zh_CN.md)

本文面向有经验的 Rust 开发者，介绍如何组织操作、共享资源和处理收尾，重点说明 API 的输入输出、所有权和失败边界。

**版本说明：**本文介绍 qubit-operation 0.2 API。当前请在应用的 `Cargo.toml` 中通过 `path` 依赖指向本地 `rs-operation` checkout；占位路径必须替换为你机器上该仓库的实际位置。待 0.2 发布到 crates.io 后，再改用版本依赖。旧版 API 不兼容，不能套用 0.1 的类型和方法。精确签名与状态契约见 [0.2 API 设计](design-0.2.0.zh_CN.md)。

## 1. 这个库解决什么问题

操作结束和资源可以释放，往往不是同一件事。上传请求可能已经取消，但 `file` 进程仍在读取输入；调用方可能不再等待模型结果，但 native 推理仍在使用缓冲区；服务可能已经停止接收请求，但后台清理尚未完成。

`qubit-operation` 将这些工作纳入统一管理：操作主体有自己的资源范围，后台使用者持有共享资源引用，管理器继续跟踪执行与清理。资源只在**所属范围已结束，并且所有使用引用均已归还**后，才进入清理。

它可以看作跨任务的 `try … finally …`。普通资源仍按 Rust 的规则析构，框架负责决定正确的析构时机；需要异步释放或处理清理失败时，再使用相应扩展。

本文以文件类型检测为主线。第 2 节给出完整代码并解释核心调用；第 3—5 节说明共享、后台执行和异步接口；第 6—8 节介绍清理、恢复和服务关闭；最后提供组件速查与排障说明。

## 2. 完整示例：检测输入文件并自动关闭

### 2.1 示例与依赖

服务读取输入文件，检查是否包含 PDF 文件头，并在操作结束后关闭文件。本例检查的是五个字节，不是完整 MIME 检测；后续接入 `file`、`ffprobe` 或模型时仍使用相同的资源持有规则。

目标依赖：

```toml
[dependencies]
qubit-operation = { path = "<path-to-rs-operation>" }
```

例如，将 `<path-to-rs-operation>` 替换为本机 `rs-operation` 仓库的路径。Cargo 会根据本地 crate 的 `Cargo.toml` 解析它自身的依赖。crate 发布 0.2 后，应用可改为 `qubit-operation = "^0.2"`；只有应用代码直接使用其他 crate 时，才需要把它们列为直接依赖。

下面是一份完整的目标程序，另存于 [detect_file.rs](examples-0.2.0/detect_file.rs)。InputFile 是应用定义的资源类型，框架类型均来自 qubit-operation。

```rust,ignore
use std::error::Error;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use qubit_operation::{
    ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode,
    ShutdownWait,
};

struct InputFile {
    file: Mutex<File>,
}

impl InputFile {
    fn open(path: PathBuf) -> io::Result<Self> {
        Ok(Self {
            file: Mutex::new(File::open(path)?),
        })
    }

    fn detect(&self) -> io::Result<Option<&'static str>> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("input lock poisoned"))?;
        let mut header = [0_u8; 5];
        file.read_exact(&mut header)?;
        Ok((header == *b"%PDF-").then_some("application/pdf"))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: detect_file <input-path>")?;
    let manager = OperationManager::new(ManagerOptions::default())?;

    let mut operation = manager.try_submit(OperationOptions::default(), move |scope| {
        let file = InputFile::open(path).map_err(|e| e.to_string())?;
        let input = scope
            .manage(file)
            .map_err(|error| format!("{:?}", error.kind()))?;
        input.detect().map_err(|e| e.to_string())
    })?;

    let outcome = operation.wait_outcome();
    let settlement = operation.wait_settlement();
    let closing = manager.request_shutdown(ShutdownMode::Drain);
    let shutdown = closing.wait_for(Duration::from_secs(1));

    // 三次观察均已进行，再处理各自的返回值。
    match outcome? {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection failed: {other:?}"),
    }

    let report = settlement?;
    if report.all_released() {
        println!("input cleanup completed");
    } else {
        eprintln!("cleanup requires attention: {:?}", report.status);
    }
    for failure in report.cleanup_failures.iter() {
        eprintln!("cleanup failure: {failure:?}");
    }

    match shutdown? {
        ShutdownWait::Completed(_) => println!("manager closed"),
        ShutdownWait::Pending(snapshot) => {
            eprintln!("shutdown pending: {snapshot:?}");
            // 本例继续等待；服务中可保存 closing，稍后再次观察。
            let _report = closing.wait()?;
            println!("manager closed");
        }
    }
    Ok(())
}
```

将代码放入应用的 `src/main.rs` 后，可用 `cargo run -- <input-path>` 传入文件。若输入以 `%PDF-` 开头，预期业务输出为 `MIME: application/pdf`，随后显示清理和管理器关闭结果。此输出尚待在完整 0.2 实现上执行确认。

文件不存在或不足五字节时，业务返回错误；文件已打开后发生的失败仍应完成收尾。文件内容至少五字节但没有匹配时，返回 `Succeeded(None)`，表示“没有识别到”，不等于执行失败。

### 2.2 提交的是什么，返回的是什么

`manager` 的类型是 OperationManager，负责接纳并执行同步操作。`try_submit` 的两个参数分别是本次操作配置和业务闭包，返回：

```rust,ignore
Result<OperationHandle<T, E>, SubmitError<F>>
```

F 是传入的闭包类型；本例的 T 是 `Option<&'static str>`，E 是 String。因此 operation 是 `OperationHandle<Option<&'static str>, String>`。

提交成功只表示任务已经交给管理器，不表示业务已执行完成。闭包由同步执行器调用，try_submit 本身不等待容量；无容量或管理器已关闭时，SubmitError 保留尚未执行的闭包。需要等待准入时使用同名参数形式的 submit。submit 有界等待相关 manager、父 scope 与 group 容量，不会因 group 已满而忙循环；丢弃仍在等待的提交会取消本次等待并归还有界等待者名额。等待不保证 FIFO，也不预留下一名额。

闭包自己的返回值是 `Result<T, E>`。它与提交结果处在不同阶段：文件读取失败属于业务 E，不会变成 SubmitError。不要只检查提交成功就响应“检测成功”。

### 2.3 scope 从哪里来

scope 是框架传给这次操作的 OperationScope，代表该操作的管理范围。每次提交建立新的 scope；这次闭包调用 scope.manage，就是把资源放到这次操作名下。

OperationScope 不实现 Clone，manage 等方法都借用 &self，因此可以在闭包内反复管理多个资源。普通辅助函数接收 `&OperationScope`；后台任务直接持有资源引用，需要受管的子操作则通过 scope 提交，由框架为它创建子 scope。

闭包返回后，其默认资源范围结束。框架的后台记录仍会保留，用来等待子任务和资源清理。保留一个 scope 对象不代表原业务主体仍在运行，也不能据此延长创建新资源的权限。

### 2.4 manage 接管什么，input 表示什么

本例先通过 InputFile::open 创建文件资源，再把它传给：

```rust,ignore
scope.manage(file)
// Result<ResourceRef<InputFile>, ManageResourceError<InputFile>>
```

manage 接管已经创建的 InputFile，不负责打开文件。成功返回的 input 是 `ResourceRef<InputFile>`，通过 Deref 可以调用 InputFile 的方法，同时它还持有一份使用引用。无需再取得一个独立租约。

InputFile 内的 Mutex 由应用设置，用于保护读取时会变化的文件游标；它不是框架自动提供的资源锁。InputFile 没有显式实现 Drop，仍会自动析构：Mutex 中的 File 最终随 InputFile 一起析构并关闭文件。若资源类型有额外同步释放动作，可以实现 Drop，但这不是调用 manage 的必需条件。

管理失败时，ManageResourceError 持有原文件。本例将错误转成字符串，错误对象随之自动析构，未被接管的文件也自动关闭。这里只适用于正常析构足以释放的资源；复杂创建失败需要第 6.3 节的预留和责任接管流程。

### 2.5 input 什么时候释放

闭包返回时，局部变量 input 自动归还使用引用，默认资源范围也结束。两项条件满足，管理器才将 InputFile 交给清理执行环境析构。

如果另一个任务还持有 input.clone()，它会继续保护同一个 InputFile，直到最后一份引用归还。这个规则与局部变量名或声明位置无关，取决于资源所属范围和全部实际使用者。

```text
闭包正在运行：所属范围开放，资源被保留
闭包返回：所属范围结束
    ├─没有其他引用 → 可以安排清理
    └─后台仍有引用 → 等最后一个使用者结束，再安排清理
```

“可以安排清理”不等于清理已经成功。文件关闭、异步释放和失败恢复可能需要继续执行，由收尾报告反映结果。

### 2.6 outcome 与 settlement：业务结果和收尾报告

**outcome 回答“检测到了什么”，settlement 回答“相关任务和资源收尾得怎么样”。** 它们描述同一次操作，但关注的事实不同，不能用其中一个替代另一个。

| 对比 | outcome | settlement |
| --- | --- | --- |
| 类型 | OperationOutcome<T, E> | SettlementReport |
| 本例中的内容 | MIME 类型、未匹配，或读取失败等 | 输入文件的清理结果，以及失败、转交或恢复信息 |
| 是否带业务值 | 包含 T 或 E | 不包含 T 或 E |
| 读取次数 | 只能取出一次 | 可反复观察同一份历史报告 |
| 等待方法 | wait_outcome | wait_settlement |

同步接口的准确返回类型是：

| 调用 | 返回类型 |
| --- | --- |
| wait_outcome | `Result<OperationOutcome<T, E>, OutcomeError>` |
| wait_settlement | `Result<Arc<SettlementReport>, ObservationError>` |

外层错误表示这次观察失败；业务错误仍放在 OperationOutcome::Failed(E) 中，清理错误仍放在报告里。异步 wait_outcome 保留同样的结果形状，而异步 wait_settlement 直接返回 Arc<SettlementReport>。

OperationOutcome 的 Succeeded 和 Failed 分别带成功值与业务错误；ChildFailed、Stopped、Panicked、ExecutionFailed 则表示必要子失败、操作停止、panic 或执行环境故障。无论属于哪一种，相关资源都仍需完成收尾。

下面几种组合都可能发生：

| outcome | settlement 或当前进展 | 应用如何处理 |
| --- | --- | --- |
| 检测成功 | Released | 使用 MIME 结果，并记录资源已按策略清理 |
| 检测成功 | RecoveryRequired | 保留成功结果，同时处理清理失败，不能忽略恢复对象 |
| 检测失败 | Released | 返回业务错误；清理成功不会使业务变为成功 |
| 已停止 | 还在等待 | 响应停止结果，继续等待原生调用、引用归还或清理结束 |
| 业务结果已确定 | Transferred | 资源责任已交给接收方；需要物理释放证据时继续跟踪接收方 |

例如，已经返回 MIME 类型，但删除暂存文件失败，属于“业务成功、清理待恢复”。模型取消时则可能先得到 Stopped，但 native 推理还持有输入，收尾报告要等它真正结束后才能发布。

**拿到 settlement 不等于所有资源都已物理释放。** 报告的稳定状态可以是 Released、Transferred 或 RecoveryRequired；还要查看 all_released、resource_reports、cleanup_failures 和 recoveries。普通 manage 的 DropCompleted 证据只说明析构完成，不能证明自定义 Drop 内被忽略的删除错误已经解决。

如果仍有使用者，或释放状态为 Unconfirmed，wait_settlement 会继续等待。需要限制等待时间时，使用 observer 的带期限接口，保存 Pending 快照后继续观察；Unconfirmed 不是一个可以当作成功返回的 SettlementStatus。

两者的共同点是都关联同一次操作，已经发布的事实不会被后续事件改写。历史 settlement 不会因为稍后重试成功而改写，恢复后的状态要另行查询。

读取 outcome 不会触发清理，两次等待之间也未必有时间间隔：调用方读取结果时，清理可能早已完成。一般可以分别等待；但若 T 包含当前资源的 ResourceRef，必须先取结果并结束使用，再等该资源清理，避免相互等待。收尾报告已经发布但仍需恢复时，manager 的整体关闭也可能继续等待。

## 3. 共享资源、再次获取与并发访问

### 3.1 把同一资源交给其他使用者

以下片段接续操作闭包内部，input 已是 ResourceRef<InputFile>：

```rust,ignore
let another_input = input.clone();
```

another_input 的类型仍是 ResourceRef<InputFile>，指向同一个 InputFile，不会复制文件内容。当前代码和后台任务各持一份时，使用计数为 2；将其中一份 move 到任务里，只改变持有者，不再增加计数。

使用者通常在函数或 future 结束时自动归还引用，不需要手动 drop。应用若复制原生文件句柄或路径交给外部程序，适配器还必须保留 ResourceRef 到实际读取结束，不能只保存路径就声称资源受到保护。

### 3.2 作用域还开放时，引用归零也不清理

下面是操作闭包内的完整局部片段，适用于需要独立资源范围的代码。资源是普通字节数组，不涉及额外清理策略：

```rust,ignore
let resources = scope.resource_scope(ResourceScopeOptions { capacity: 1 })?;
let key;
{
    let input = resources.manage(vec![1_u8, 2, 3])?;
    key = input.key();
    assert_eq!(input.len(), 3);
} // 使用计数变为 0，resources 仍开放，Vec 不释放。
{
    let input = resources.get_resource(key)?;
    assert_eq!(input.len(), 3);
}
// resources 随局部作用域结束，且没有引用时，Vec 才可清理。
```

resource_scope 返回 ResourceScope，表示一个显式的资源所属范围。key 是 `ResourceKey<Vec<u8>>`，只保存身份和类型，不延长资源存活；get_resource 则通过仍开放的范围建立新的使用引用。

ResourceScope 的 owner Drop 或 end 都会结束持有，但不会阻塞等待清理。需要提前等待该局部范围收尾时，先保存 observer，结束范围，并保证当前函数没有继续持有其中的资源引用，再等待 observer 的报告。

### 3.3 并发获取不表示独占

同一个 scope 可以重复获取同一个资源：

```rust,ignore
let state = scope.manage(Mutex::new(Vec::<u8>::new()))?;
let first = scope.get_resource(state.key())?;
let second = scope.get_resource(state.key())?;
first.lock().map_err(|e| e.to_string())?.push(1);
second.lock().map_err(|e| e.to_string())?.push(2);
```

Mutex 需由应用导入，片段放在返回兼容错误的操作闭包中。first、second 和 state 都指向同一个 Mutex<Vec<u8>>。资源引用负责存活，MutexGuard 负责互斥，两者的释放是不同动作。

框架只同步登记、引用计数和状态，不为资源内容加锁。可写共享状态由应用选择 Mutex、RwLock、原子或领域同步。异步代码还应选择适合运行时的锁，避免持有阻塞锁跨越不受控 await。

## 4. 子操作与后台使用

### 4.1 子操作有什么用，何时需要提交

普通的 `input.detect()` 是函数调用，执行仍属于当前主体。`scope.manager().try_submit(options, body)` 则接纳另一项受管工作，返回 `Result<OperationHandle<T, E>, SubmitError<F>>`；它拥有自己的 scope、结果和收尾记录，可由执行器推进，父操作继续跟踪它。

适用情况是拆出并行或后台步骤，例如文件检测同时生成摘要，或异步请求把同步推理交给领域执行器。父操作可以传播停止、按必要子失败策略处理结果，并在自己收尾时汇总子操作。直接向外部线程池提交但没有接入控制协议的工作，不会自动获得这些关系。

顺序执行的普通函数不必都变成 child，接收 &OperationScope 即可。try 表示立即接纳或返回拒绝，不是立即执行完毕；仅成功提交，也不能当作子工作已成功。

scope.manager() 的签名是 fn manager(&self) -> OperationManager。它返回与原 manager 相同类型的 owned 视图，并把当前操作固定为新任务的父级。它不是新的执行器，不新增容量，也不消耗 scope；可以多次调用，或把返回的 manager 借给辅助函数。同步主体用这个视图提交；异步主体可以用 scope.manager().as_async() 提交异步工作。

| 使用的入口 | 提交关系 |
| --- | --- |
| new 创建的 manager | 新根操作 |
| scope.manager() | 当前操作的直接子操作 |
| 子操作的 child_scope.manager() | 该子操作的直接子操作 |

try_submit/submit 的参数与返回类型在所有层级相同，区别由传入的 manager 决定。下面展示如何把输入交给后台子操作。

下面的函数可与第 2 节 InputFile 定义放在同一模块，演示后台检测。父结果是请求编号，子操作执行文件检测；此例不向调用方交付子 MIME 值，而由父级跟踪其结果摘要。

```rust,ignore
fn submit_background_detection(
    manager: &OperationManager,
    path: PathBuf,
) -> Result<OperationHandle<&'static str, String>, AdmissionError> {
    manager.try_submit(OperationOptions::default(), move |scope| {
        let file = InputFile::open(path).map_err(|e| e.to_string())?;
        let input = scope.manage(file).map_err(|error| format!("{:?}", error.kind()))?;
        let background_input = input.clone();
        let child = scope.manager().try_submit(OperationOptions::default(), move |_child_scope| {
            background_input.detect().map_err(|e| e.to_string())
        }).map_err(|e| e.to_string())?;
        let _observer = child.into_observer();
        Ok::<_, String>("document-42")
    }).map_err(|error| error.into_parts().0)
}
```

额外导入 OperationHandle、AdmissionError。函数在提交失败时只返回准入原因；闭包尚未执行，文件尚未打开，丢弃闭包只释放捕获的 PathBuf。这种错误转换不能照搬到捕获了特殊资源的任务。

scope.manager().try_submit 接收 OperationOptions 和同步闭包，返回子 OperationHandle。子闭包有自己的 scope，但 background_input 仍属于父资源范围。into_observer 放弃子 typed result，不取消子工作；观察者局部变量结束也不会取消。

| 事件 | 父资源范围 | 输入引用 |
| --- | --- | ---: |
| manage 成功 | 开放 | 1 |
| clone 给子操作 | 开放 | 2 |
| 父闭包先返回 | 结束 | 1 |
| 子操作真正结束 | 结束 | 0，开始满足清理条件 |

若子操作先返回，也只归还它的一份引用，父范围未结束前仍不能清理。默认 Required 子操作成功是父成功 outcome 的前提，因此“父闭包返回”和“父成功结果可用”也不应混为一谈。

### 4.2 必要与可选工作

OperationOptions.requirement 默认为 ChildRequirement::Required，表示子业务失败会影响父成功；根提交没有父级，该字段不参与判断。ChildFailurePolicy::StopSiblings 请求兄弟停止，ContinueSiblings 允许独立工作继续，但两者都不会让必要失败变成父成功。

例如 ffprobe 只用于补充媒体信息，且业务允许失败后返回基本类型时，应明确设为 Optional，由父主体决定降级。这不是把失败策略改成 ContinueSiblings 就能表达的。

例如在返回 String 错误的操作主体中，可以这样登记可选步骤；此处 task_body 是应用提供的同步闭包，签名为 FnOnce(OperationScope) -> Result<T, E>，需额外导入 ChildRequirement：

```rust,ignore
let mut options = OperationOptions::default();
options.requirement = ChildRequirement::Optional;
let child = scope.manager().try_submit(options, task_body)
    .map_err(|e| e.to_string())?;
let _observer = child.into_observer();
```

Optional 子操作也受收尾管理。需要其具体返回值时保留 handle，并采用合适的同步/异步等待；不要在父同步执行器 worker 上阻塞等待同一服务的子结果。异步父操作可以等待转换后的子 handle，下一节给出组合方式。

### 4.3 任意层级如何继续提交

可以。子闭包接收的是它自己的 OperationScope，拥有相同的子提交能力，可以继续创建孙操作。关系是树状的，而不是把所有工作都登记成独立根任务：

```text
根操作的 scope
└── 子操作的 scope
    └── 孙操作的 scope
```

下面是完整的目标函数，只验证嵌套提交关系。所有业务值均为 ()，并不执行实际检测。两个 into_observer 明确放弃子结果，但不停止子工作，避免子 handle 离开局部作用域时发出停止请求。

```rust,ignore
fn submit_nested(
    manager: &OperationManager,
) -> Result<OperationHandle<(), String>, AdmissionError> {
    manager.try_submit(OperationOptions::default(), |root_scope| {
        let child = root_scope.manager().try_submit(OperationOptions::default(), |child_scope| {
            let grandchild = child_scope.manager().try_submit(
                OperationOptions::default(),
                |_grandchild_scope| Ok::<(), String>(()),
            ).map_err(|error| error.to_string())?;
            let _observer = grandchild.into_observer();
            Ok::<(), String>(())
        }).map_err(|error| error.to_string())?;
        let _observer = child.into_observer();
        Ok::<(), String>(())
    }).map_err(|error| error.into_parts().0)
}
```

导入与 4.1 节相同。可以把这些规则理解为：

- 父函数创建子任务 B 后返回，不会自动取消 B。B 还在运行时，可以继续创建任务 C，这时用的是 B 自己的 scope。
- 一次请求如果只有 64 个子任务名额，B 和 C 都从这 64 个名额中分配。创建 B 不会额外得到另一组 64 个名额。
- 如果调用方取消整个请求，框架会要求 B 和 C 一起停止，并拒绝创建更多任务。无法中断的调用仍需等它实际返回。
- 子任务要使用父资源，应提前拿到 ResourceRef。父资源范围结束后，子任务不能再通过那个范围获取新引用，但仍可继续使用已经持有的引用。

同步任务不要在执行器的 worker 中阻塞等下一层结果。可以让框架按必要子操作关系汇总，或由异步主体等待子 handle 的异步视图。

### 4.4 父资源是否必须等全部子资源释放

不需要一律等待。这里要分开看“父操作的某个资源可以清理”和“父操作整体收尾完成”。

假设父操作创建输入文件 P，子操作使用 P，同时还管理自己的模型资源 C：

```text
父操作：拥有文件 P
└── 子操作：持有 P 的使用引用，拥有模型资源 C
```

父业务函数返回时，P 的所属资源范围结束，但只要子操作还持有 P，文件就不能清理。子操作停止使用 P 并归还引用后，如果没有其他使用者，P 就可以开始清理；此时子操作可能还在处理自己的 C，不必为了 C 而继续保留已经没人使用的 P。

| 事件 | P 是否可清理 | 父操作整体收尾 |
| --- | --- | --- |
| 父函数返回，子仍使用 P | 不可以 | 未完成 |
| 子归还 P，但 C 还在收尾 | 可以，前提是 P 没有其他引用或清理依赖 | 未完成 |
| P 已清理，子也产生了稳定收尾报告 | P 的清理已完成 | 可以汇总发布父报告 |

所以，单个资源仍遵循“自己的所属范围结束 + 自己的引用数为 0”。父子关系本身不会自动增加“所有子资源都释放”这一条件。如果 C 的清理过程也要访问 P，C 或它的清理对象必须继续持有 P 的引用，直到清理使用结束；P 就会按引用计数继续保留。业务确有其他先后要求时，需要明确的清理依赖，不能只靠操作嵌套来表达。

父 wait_settlement 等待的是自己的资源和子操作都已有收尾结果。子资源如果需要恢复或已经转交，也可以形成稳定报告，父报告必须如实保留这些状态；“父收尾报告可用”并不等于全部资源都物理释放。

### 4.5 子操作管理自己的资源，并把结果交给父操作

本目标展示父子操作共同完成文件摘要：异步父操作登记输入并提交同步子操作；子操作读取父级输入、登记自己的文件头缓冲区，最后返回不含资源引用的 FileSummary。容量场景也在同一 Cargo 目标中执行。

调用方提供已读入的 Vec<u8>，不在异步主体中读取文件。下面是完整 Cargo 目标源码，包含摘要函数、容量限制为 2 和 3 的两条运行路径，另存于 [family_and_capacity.rs](examples-0.2.0/family_and_capacity.rs)。

```rust,ignore
use qubit_operation::{
    AdmissionError, AsyncOperationHandle, AsyncOperationManager, ManagerOptions, OperationOptions,
    OperationOutcome,
};

#[derive(Debug)]
struct FileSummary {
    byte_count: usize,
    mime: Option<&'static str>,
}

fn submit_file_summary(
    manager: &AsyncOperationManager,
    bytes: Vec<u8>,
) -> Result<AsyncOperationHandle<FileSummary, String>, AdmissionError> {
    manager
        .try_submit(
            OperationOptions::default(),
            move |parent_scope| async move {
                let input = parent_scope
                    .manage(bytes)
                    .map_err(|error| format!("{:?}", error.kind()))?;
                let child = parent_scope
                    .manager()
                    .try_submit(OperationOptions::default(), move |child_scope| {
                        let header_bytes = input.iter().take(5).copied().collect::<Vec<u8>>();
                        let header = child_scope
                            .manage(header_bytes)
                            .map_err(|error| format!("{:?}", error.kind()))?;
                        Ok::<_, String>(FileSummary {
                            byte_count: input.len(),
                            mime: (header.as_slice() == b"%PDF-").then_some("application/pdf"),
                        })
                    })
                    .map_err(|e| e.to_string())?;
                let mut child = child.into_async();
                match child.wait_outcome().await.map_err(|e| e.to_string())? {
                    OperationOutcome::Succeeded(summary) => Ok(summary),
                    other => Err(format!("summary child failed: {other:?}")),
                }
            },
        )
        .map_err(|error| error.into_parts().0)
}

fn create_capacity_demo_manager(resource_limit: usize) -> Result<AsyncOperationManager, String> {
    let mut options = ManagerOptions::default();
    options.scope_limits.child_operations = 2;
    options.scope_limits.resources = resource_limit;
    AsyncOperationManager::new(options).map_err(|e| e.to_string())
}

fn submit_capacity_demo(
    manager: &AsyncOperationManager,
    resource_limit: usize,
) -> Result<AsyncOperationHandle<(), String>, AdmissionError> {
    let mut options = OperationOptions::default();
    options.limits.child_operations = 2;
    options.limits.resources = resource_limit;
    manager
        .try_submit(options, |parent_scope| async move {
            let input = parent_scope
                .manage(vec![1_u8, 2, 3])
                .map_err(|error| format!("{:?}", error.kind()))?;
            let _request_metadata = parent_scope
                .manage(String::from("document-42"))
                .map_err(|error| format!("{:?}", error.kind()))?;
            let child = parent_scope
                .manager()
                .try_submit(OperationOptions::default(), move |child_scope| {
                    let _header = child_scope
                        .manage(input.iter().take(2).copied().collect::<Vec<_>>())
                        .map_err(|error| format!("{:?}", error.kind()))?;
                    Ok::<(), String>(())
                })
                .map_err(|e| e.to_string())?;
            let mut child = child.into_async();
            match child.wait_outcome().await.map_err(|e| e.to_string())? {
                OperationOutcome::Succeeded(()) => Ok(()),
                other => Err(format!("capacity demo child failed: {other:?}")),
            }
        })
        .map_err(|error| error.into_parts().0)
}
```

逐项看输入和返回值：

| 调用或对象 | 输入 | 返回或含义 |
| --- | --- | --- |
| submit_file_summary | 异步 manager 的借用、输入 Vec 的所有权 | AsyncOperationHandle<FileSummary, String>，或未接纳的原因 |
| parent_scope.manage(bytes) | 输入缓冲区的所有权 | ResourceRef<Vec<u8>>，归属父资源范围 |
| scope.manager().try_submit | OperationOptions、接收子 scope 的同步闭包 | OperationHandle<FileSummary, String>，表示已接纳的子操作 |
| child_scope.manage(header_bytes) | 子操作新建的文件头缓冲区 | 另一份 ResourceRef<Vec<u8>>，归属子资源范围 |
| child.into_async() | 同步 handle 的所有权 | 同一子操作的异步 handle，不重新执行、不新占名额 |
| child.wait_outcome().await | 子 handle 的可变借用 | 子 OperationOutcome；取出成功值后，父主体才能组装自己的返回值 |

这里 input 被 move 进子闭包，未调用 clone，所以没有额外增加使用引用。子操作可以读取 input，但不能把它当作独占访问；若还有其他持有者，共享内容的同步仍由资源类型负责。header 则是子操作创建的新对象，单独登记、单独计数。

运行结束时，子闭包的 input 和 header 引用自动销毁，子默认资源范围也随子主体结束。header 满足两个释放条件后进入清理；父输入的使用引用虽然已归零，父主体尚在等待结果时，其所属范围仍未结束，因此输入继续保留。父主体拿到 FileSummary 后返回，父范围才结束，输入随后进入清理。代码不需要手动调用 drop，也不要求两次清理以特定顺序完成。

此函数的直接调用返回值只说明准入结果。调用方仍需对返回的父 handle 调用 wait_outcome().await 取得 FileSummary，再调用 wait_settlement().await 观察父子资源的整体收尾。FileSummary 不含 ResourceRef，所以保留它不会延长输入的生命周期。若业务返回值包含 ResourceRef，该引用也属于使用者，需要在等待其释放前归还。

本例由父代码把非成功的子 outcome 转换为 String 错误；默认 Required 还会保留必要子失败的语义，因此调用方不能依赖父失败必定以某一个枚举分支呈现，必须同时处理 Failed、ChildFailed、Stopped 等结果。

### 4.6 父返回、取消与整体收尾的区别

| 发生的事件 | 子操作会怎样 | 调用方应怎样理解 |
| --- | --- | --- |
| 父主体正常返回 | 已接纳子操作继续；子主体仍开放时可以继续提交后代 | 父主体返回不等于父成功结果可用，也不等于收尾完成 |
| Required 子失败 | 父成功受到影响；兄弟是否继续由失败策略决定 | ContinueSiblings 不能把必要失败变为成功 |
| Optional 子仍在执行 | 不阻止父业务成功，但仍参加父整体收尾 | Optional 表示业务上可选，不表示无人管理 |
| 请求停止父操作 | 停止向整棵子树传播，拒绝新子提交 | 停止请求不是线程已退出或资源已释放的证据 |
| 仅请求停止某个子操作 | 先影响该子树；必要子失败可能进一步触发父失败策略 | 不是无条件取消整棵操作树 |
| 父 settlement 可观察 | 父与所有子操作都已有稳定收尾报告 | 继续查看释放、转交、恢复状态，不能只检查报告是否存在 |

父子关系解决的是跟踪责任，而非隐式保存全部父资源。决定资源何时可释放的仍是其所属范围和使用引用。父 scope 结束后，仍存活的子 scope 是另一项操作的开放范围；子操作可以管理新资源，但不能通过已经结束的父范围重新获取父资源。需要继续使用的引用应在提交时传入。

### 4.7 同一个函数在根操作和任意子层级中复用

业务函数接受 &OperationManager 并使用统一的 try_submit 即可，不需要查询自己处于哪一层。本例用输入长度代表扫描结果，重点展示提交关系；两个完整目标函数另存于 [submission_context.rs](examples-0.2.0/submission_context.rs)。

```rust,ignore
use qubit_operation::{AdmissionError, OperationHandle, OperationManager, OperationOptions};

pub fn submit_scan(
    manager: &OperationManager,
    bytes: Vec<u8>,
) -> Result<OperationHandle<usize, String>, AdmissionError> {
    manager
        .try_submit(OperationOptions::default(), move |scope| {
            let input = scope
                .manage(bytes)
                .map_err(|error| format!("{:?}", error.kind()))?;
            Ok::<_, String>(input.len())
        })
        .map_err(|error| error.into_parts().0)
}

pub fn submit_scan_request(
    manager: &OperationManager,
    bytes: Vec<u8>,
) -> Result<OperationHandle<&'static str, String>, AdmissionError> {
    manager
        .try_submit(OperationOptions::default(), move |scope| {
            let child = submit_scan(&scope.manager(), bytes).map_err(|e| e.to_string())?;
            let _observer = child.into_observer();
            Ok::<_, String>("document-42")
        })
        .map_err(|error| error.into_parts().0)
}
```

submit_scan 接收 manager 的借用和输入 Vec 的所有权，返回 OperationHandle<usize, String>。它只创建自己的操作、通过 scope 管理输入，并返回输入长度；没有关于父子层级的分支。

在应用入口中调用 submit_scan(&manager, bytes)，传入 new 创建的 manager，就创建一个根扫描操作。调用 submit_scan_request(&manager, bytes) 则先创建请求操作，再把其 scope.manager() 传给同一个 submit_scan，扫描就成为请求的直接孩子。submit_scan_request 本身也可以接收某个 scope.manager()，此时请求成为子操作，扫描再成为孙操作。两层业务函数都无需修改。

submit_scan_request 通过 into_observer 放弃扫描的具体 usize 返回值，但扫描仍为默认 Required：其失败影响请求成功，资源也仍参加请求整体收尾。请求主体返回的字符串是业务标识，既不表示扫描已成功，也不表示输入已释放。调用方继续用返回的请求 handle 观察 outcome 与 settlement。

父级绑定在取得 manager 时确定，不会随着传参进入另一层而自动改变。例如把同一个父 manager 借给两个辅助函数，这两个函数创建的是兄弟操作；新操作要创建自己的孩子，应取得它自己的 scope.manager()。clone、as_async、as_sync 都保留原来的绑定。

同样，在任意主体中传入最初的根 manager，创建的仍是独立根操作，当前父级不会自动跟踪它。框架不从线程或调用栈推断父级；调用方通过传入哪一个 manager 明确选择归属。

保存 manager 视图不会延长 scope 开放期。父主体结束后，通过先前保存的视图提交返回 AdmissionError::ScopeEnded；父已接受停止时返回 Stopped，原闭包仍在错误中。不会自动改成根提交，也不会重新打开已结束范围。子操作仍在执行时，可以通过它自己的 scope.manager() 继续创建后代。

submit 同样适用于各层，只是等待准入：同步版本阻塞调用线程，异步版本在 scope.manager().as_async().submit(...).await 等待。等待者受全局容量、停止和期限约束。不要在同步 worker 中等待只有当前任务退出后才能腾出的容量；框架识别到此类依赖时返回 WouldDeadlock，异步 await 本身也不会解除逻辑上的容量依赖。

最后，绑定只改变提交位置。manager.snapshot()、close_admission() 和 request_shutdown() 仍面向共享核心，不表示只观察或停止绑定的父操作。应用入口负责全局关闭；操作内部的局部观察和停止使用 scope、handle 或 observer。调用 close_admission() 关闭的是新根准入，已接纳子树仍可按 Drain 规则完成工作。

## 5. 异步调用与不可中断推理

### 5.1 同步和异步管理器如何选择

OperationManager 提交同步闭包、同步等待准入与结果；AsyncOperationManager 提交返回 Future 的闭包，异步等待。两者的 scope 和资源接口相同。

as_async/as_sync 返回同一管理核心的另一外观，共享容量和关闭状态，并保留原来的提交位置；单独 new 才是独立管理器。handle 的 into_async/into_sync 则消费原句柄，转移同一次结果接收权。

下面是完整的应用函数，可与第 2 节 InputFile 放在同一模块。文件读取仍交给同步执行器，调用方通过异步 handle 等待，不在异步 poll 中执行阻塞文件 I/O：

```rust,ignore
async fn detect_from_async_service(
    manager: &AsyncOperationManager,
    path: PathBuf,
) -> Result<(OperationOutcome<Option<&'static str>, String>, Arc<SettlementReport>), Box<dyn Error>> {
    let handle = manager.as_sync().try_submit(OperationOptions::default(), move |scope| {
        let file = InputFile::open(path).map_err(|e| e.to_string())?;
        let input = scope.manage(file).map_err(|error| format!("{:?}", error.kind()))?;
        input.detect().map_err(|e| e.to_string())
    })?;
    let mut handle = handle.into_async();
    let outcome = handle.wait_outcome().await;
    let settlement = handle.wait_settlement().await;
    Ok((outcome?, settlement))
}
```

额外导入 AsyncOperationManager、SettlementReport 和 std::sync::Arc。业务输出仍是 Option MIME，报告仍单独返回。服务持有 manager，在整体关闭时统一处理，不在这个每请求函数中关闭共享管理器。

### 5.2 不可中断推理的完整接入函数

以下代码放在模型适配模块。ModelBackend 是应用接口，实际模型实现放在自己的线程安全后端中；这里不假定任何未实现的 rs-magika 方法。

```rust,ignore
use qubit_operation::{
    AdmissionError, AsyncOperationHandle, AsyncOperationManager, OperationOptions,
    OperationOutcome, ResourceScopeOptions,
};
use std::sync::Arc;

pub trait ModelBackend: Send + Sync + 'static {
    fn infer(&self, features: &[f32]) -> Result<Vec<f32>, String>;
}

pub fn submit_inference(
    manager: &AsyncOperationManager,
    model: Arc<dyn ModelBackend>,
    features: Vec<f32>,
) -> Result<AsyncOperationHandle<Vec<f32>, String>, AdmissionError> {
    manager
        .try_submit(OperationOptions::default(), move |scope| async move {
            let child = {
                let inputs = scope
                    .resource_scope(ResourceScopeOptions { capacity: 1 })
                    .map_err(|e| e.to_string())?;
                let input = inputs
                    .manage(features)
                    .map_err(|error| format!("{:?}", error.kind()))?;
                scope
                    .manager()
                    .try_submit(OperationOptions::default(), move |_child_scope| {
                        model.infer(&input)
                    })
                    .map_err(|e| e.to_string())?
            }; // inputs 结束；input 已移给同步子操作，仍然存活。

            let mut child = child.into_async();
            match child.wait_outcome().await.map_err(|e| e.to_string())? {
                OperationOutcome::Succeeded(prediction) => Ok(prediction),
                other => Err(format!("model outcome: {other:?}")),
            }
        })
        .map_err(|error| error.into_parts().0)
}
```

infer 运行于同步子操作的执行器，不在异步 worker 的 poll 中执行。它借用 input，子闭包自身拥有 ResourceRef<Vec<f32>>，所以原生调用返回前引用一直有效。inputs 的资源范围已结束，但计数仍为 1，不能清理 features。

调用方得到 AsyncOperationHandle<Vec<f32>, String>，可以由请求取消事件触发：

```rust,ignore
inference.request_stop(StopReason::UserCancelled);
```

这行代码的 inference 来自 submit_inference，需要导入 StopReason。它不会中断任意 native 调用，也不会使 model.infer 的局部引用消失。如果主体在等待子结果时被停止，子操作的执行状态仍持有 input，直至 infer 真正返回，之后才能归还引用并清理。

调用方分别等待 wait_outcome 和 wait_settlement；前者可能先返回 Stopped，后者仍会等待原生使用结束。停止若先于模型启动，可以取消尚未启动的工作；结果若已发布，则保留原结果。应用不应假定每次停止都得到同一种时序。

本函数只定义接入，不实现模型，也不保证模型占用或预测性能。线程绑定且不能 Send/Sync 的 session 应留在所属线程，由 ModelBackend 的安全端口访问。

### 5.3 Drop、放弃结果和观察者

丢弃未完成 handle 会请求停止并放弃结果，但不等待清理。into_observer 只放弃结果，不发停止请求，返回只读观察入口；observer 可以复制，不会获得另一份业务结果或停止权。

只丢弃一次 wait_outcome 的 Future，不等于丢弃 handle。只要 handle 仍持有结果权，之后可以再次等待；已经取走结果，再等待则返回 AlreadyTaken。

如果结果本身含 ResourceRef，先取出并完成资源使用，再等待原操作收尾。run_and_settle 不能用于仍持有当前资源引用的返回值，否则会等待自身输出释放。

## 6. 普通 Drop、异步清理与创建失败

### 6.1 什么时候只用 manage

普通 RAII 类型直接 manage。框架不要求显式 impl Drop：Vec 会释放缓冲区，File 会关闭句柄，struct 的字段也会自动析构。自定义 Drop 可以释放额外本地资源，但无法通过返回值告诉框架失败。

默认清理安排在有界的阻塞清理执行环境中，避免在最后一次引用 Drop 或框架状态锁内执行用户析构。引用归还和范围结束都不阻塞等待整个清理完成。

### 6.2 异步清理策略的形式

当释放需要 await，或应用必须知道清理是否失败时，用 manage_with(resource, cleanup)。资源 T 与访问方式不变，C 实现 AsyncCleanup<T>。

下面的完整类型定义属于应用的远程会话适配模块。SessionPort 是领域连接接口，其 close 返回 owned Future；实现由应用提供。

```rust,ignore
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use qubit_operation::{
    AsyncCleanup, CleanupContext, CleanupFailure, CleanupFuture, CleanupOutcome,
    CleanupStage, DiagnosticSummary,
};

trait SessionPort: Send + Sync {
    fn close(&self, id: u64)
        -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'static>>;
}

struct Session {
    id: u64,
    connection: Arc<dyn SessionPort>,
}
struct CloseSession;

impl AsyncCleanup<Session> for CloseSession {
    fn cleanup(self: Arc<Self>, session: Session, context: CleanupContext)
        -> CleanupFuture<Session>
    {
        Box::pin(async move {
            match session.connection.close(session.id).await {
                Ok(()) => CleanupOutcome::Released,
                Err(message) => CleanupOutcome::Failed {
                    failure: CleanupFailure {
                        resource_id: context.resource_id,
                        stage: CleanupStage::AsyncRelease,
                        summary: DiagnosticSummary::new(&message),
                        source: None,
                    },
                    resource: session,
                },
            }
        })
    }
}
```

清理开始时，框架把唯一的 Session 交给策略，已不存在仍能访问它的 ResourceRef。成功时会话已确认关闭；失败时把 Session 一起返回，框架保留它供恢复，而不是只留下字符串。

框架独立保留 Arc<CloseSession>，重试时仍能使用策略。Session 的 Drop 最终还会运行，因此它不应再无条件关闭同一远程会话；本例的 Drop 只析构 id 和连接引用。

manage_with 返回 `Result<ResourceRef<Session>, ManageWithError<Session, CloseSession>>`。错误保留两者；对于创建后不能直接丢弃的远程资源，应使用下一节的预留路径，而不是忽略登记错误。

### 6.3 创建前预留，提交失败仍有清理去向

以下函数与上一节定义放在同一模块：

```rust,ignore
fn commit_session(
    reservation: ResourceReservation,
    session: Session,
) -> Result<ResourceRef<Session>, ResourceManageErrorKind> {
    match reservation.commit_with(session, CloseSession) {
        Ok(session) => Ok(session),
        Err(error) => {
            let (reason, session, cleanup, reservation) = error.into_parts();
            reservation.retain_with_for_cleanup(session, cleanup);
            Err(reason)
        }
    }
}
```

额外导入 ResourceReservation、ResourceRef、ResourceManageErrorKind。调用方先执行 scope.reserve_resource，再通过领域服务创建 Session，最后调用 commit_session。创建本身失败时，部分资源也应交给领域清理对象；若尚未产生资源，reservation 可以直接自动释放。

commit_with 接管资源并建立首份引用。若停止或范围结束导致失败，错误归还资源、策略和原 reservation；retain_with_for_cleanup 利用预留容量接管清理，不再授权业务使用，也不因原任务已停止而再次拒绝。

这条路径解决的是“创建已产生资源，但业务登记失败”的责任问题。它不是正常使用时必须手动释放的步骤。

## 7. 收尾报告与恢复处理

### 7.1 如何判断收尾结果

SettlementReport 的 status 是 Released、Transferred 或 RecoveryRequired。all_released 不把转交和恢复算作已释放。业务代码还可以查看：

| 字段 | 含义与用途 |
| --- | --- |
| resource_reports | 各成功清理项的释放证据 |
| cleanup_failures | 失败阶段、资源 ID、摘要和可选原始错误 |
| children | 子操作业务与收尾信息 |
| transfers | 接收方签发的转交回执 |
| recoveries | 后续恢复句柄 |
| effects / effect_overflow | 应用声明的副作用及被截断数量 |

wait_settlement 返回报告，并不意味着所有外部资源都物理释放。普通 manage 只能确认析构完成；manage_with 的 Confirmed 来自策略确认。应用应按资源要求解释证据。

报告是历史快照，后续恢复不会改写它。列表可能为空，也有容量限制，处理时遍历即可，不应直接索引首项或把摘要当作完整审计日志。

### 7.2 显式重试恢复组

下面是接收一份报告的完整处理函数，类型均来自设计；重试一次，不做无界循环：

```rust,ignore
fn retry_report(report: &SettlementReport) -> Result<(), ObservationError> {
    for recovery in report.recoveries.iter() {
        match recovery.try_retry() {
            Ok(attempt) => {
                let result = attempt.wait()?;
                eprintln!("recovery state: {:?}", result.recovery.phase);
            }
            Err(reason) => eprintln!("retry not accepted: {reason:?}"),
        }
    }
    Ok(())
}
```

需导入 SettlementReport 和 ObservationError。try_retry 返回 RecoveryAttempt，表示同一恢复组的一次已接纳尝试；等待返回 RecoveryAttemptReport。AlreadyRunning、NoCapacity 或执行器不可用时，原资源继续保留，调用方按策略稍后处理。

恢复按依赖组进行，不能只取走一个前置资源就认为后继可以随意清理。需要转交处理方时，使用 RecoveryStore 的整组签收协议；收到回执后源 manager 才解除责任。

未确认还有实际使用者的资源不能通过“重试清理”强行删除。先确认使用已结束，再处理清理失败。manager 内的未解决恢复组会阻止整体关闭，这是责任尚未完成的信号。

操作主体在构造 Future 或轮询时发生的 panic，会在完成生命周期和容量记账后报告为 `Panicked`。恢复清理期间发生 panic 时报告为 `Unconfirmed`：框架保留恢复责任，不把资源当作已经释放。多个恢复观察者共享同一份最终报告；丢弃一个观察者只取消该等待，不会取消共享重试或释放资源。这些保证适用于 unwind panic，不涵盖 `panic = "abort"`。

`RecoveryStore::request_shutdown` 会关闭新组准入并返回可克隆的 `RecoveryStoreShutdownHandle`。各克隆以及重复调用 `wait`、`wait_async` 或 `wait_for` 都观察同一个最终结果。`is_complete()` 表示关闭收尾已结束，即使收尾结果是 `RecoveryStoreShutdownError` 也为真；应检查等待结果来区分成功与失败。丢弃仍在等待的 `wait_async` 只取消该观察者，不会重新开放 store、取消关闭或丢弃保留的恢复组。恢复组排空后，`wait_async` 仍会同步执行 executor 关闭和 worker join。

## 8. 容量、上下文与服务关闭

### 8.1 根据繁忙原因处理背压

#### 8.1.1 配置 manager 与单次请求

try_submit 返回 Busy 时，先检查 manager.snapshot() 中的活动、排队、等待准入和未取结果计数。结果未取会继续占结果槽，恢复对象也会占资源额度；提高队列长度不能代替消费结果和处理恢复。

默认活动 root 为 4，排队 root 为 64，结果槽为 64，每 root 子操作和资源分别为 64、128。这些都是可配置的初始值，64 不是固定上限。ManagerLimits 配置 manager 的容量，ManagerOptions.scope_limits 配置每个根操作的额度上限；OperationOptions.limits 可以进一步收紧单次请求的额度。父操作及其后代共享根操作的额度，创建子操作不会得到新的一套名额。

修改配置时可从默认值开始，例如：

```rust,ignore
let mut options = ManagerOptions::default();
options.limits.active_operations = 8;
options.limits.unconsumed_outcomes = 32;
let manager = OperationManager::new(options)?;
```

submit 会等待准入；同步方法阻塞当前调用线程，异步方法 await。等待者也有限，且受业务总期限约束，不能用无限重试绕过背压。

#### 8.1.2 父子操作怎样共同占用额度

根操作是直接通过 manager 提交的操作，子操作则通过某个 scope 提交。活动 root 上限为 4，只约束同时占用该名额的根操作；它不意味着所有后代加起来最多只有 4 个任务。后代受根的子操作额度和实际执行器容量约束，也不为自己再申请一个 root 名额。

| 额度 | 谁一起计数 | 什么时候归还 |
| --- | --- | --- |
| 活动 root | manager 下尚在执行或自动收尾的根请求树 | 根及全部后代形成稳定 settlement 后；仅根主体返回不归还 |
| 排队 root | manager 的根操作队列 | 出队或撤销排队时 |
| typed 结果槽 | manager 下的根操作和所有子操作 | 成功取出结果，或明确放弃结果时；结果尚未发布也可放弃交付权 |
| 准入等待者 | manager 下通过 submit 等待根或子准入的调用 | 接纳、拒绝或取消等待时 |
| 清理并发 | manager 下所有正在运行的清理步骤 | 一次清理实际结束时；清理失败保留资源，但不一直占 worker |
| 子操作名额 | 当前操作的所有后代，不包括它自己 | 子收尾报告稳定，且其 typed 结果已取出或放弃后 |
| 资源登记名额 | 当前操作及其所有后代的资源 | 该登记不再承担清理或恢复责任时 |
| ResourceScope 实例名额 | 当前操作及其所有后代创建并附着的局部 ResourceScope | ResourceScope settlement 稳定后；仅结束父操作或调用 end 不提前归还 |
| 局部 ResourceScope 容量 | 该局部范围的资源 | 同上；还必须满足根和沿途祖先的额度 |

不同限制需要同时满足。例如结果槽总额为 64，根 handle 已保留一个结果槽时，最多还能同时预留 63 个其他结果槽；把子名额设置为 64，并不保证 64 次子提交全部可以成功。消费结果或 into_observer 可以归还结果槽，但子操作仍须满足自己的收尾条件，才归还子名额。

例如根允许两个子名额：根创建 B 占一个，B 创建 C 再占一个，根池就满了。无论根还是 B 再创建任务，都不能绕过这两个名额的总限制。B 还可以有更低的局部上限，但把 B 的配置写得更大不会放宽根的限制；创建 B 时也不预留 B 配置中声明的全部名额。

资源登记也按整棵子树合并计数。根登记输入，B 登记临时缓冲区，各占一个资源名额；B 对输入 clone 一份 ResourceRef 不增加资源名额。`ScopeLimits.resources` 限制资源登记数，`ResourceScopeOptions.capacity` 再限制单个 ResourceScope 内的资源数。

创建一个附着到操作的 ResourceScope 还会单独消耗一个 `ScopeLimits.resource_scopes` 名额；它与资源登记额度分开计数。默认每个操作 scope 可同时拥有 64 个附属 ResourceScope，子操作创建的 scope 同时计入沿途祖先的额度。额度在该 ResourceScope 的 settlement 稳定后归还，因此只创建空 scope 也会受限，不能通过反复创建空 scope 绕过资源登记上限。服务级 `ServiceResourceScope` 使用独立配额，不计入操作 scope 的该额度。

OperationOptions.limits 沿父链收紧，实际按各字段与继承上限取较小值。因此默认值 64、128 不会覆盖父级更小的配置。额度限制尚未退还的占用，归还后可以再次使用；若要限制请求一生累计执行的任务数，应使用另一个 ResourceBudget，不要把并发容量当作累计消耗。

还可以用第 4.3 节的 submit_nested 验证子名额耗尽。下面是容量为 2 时的目标配置片段。四个目标的完整运行入口见相应 Cargo example；同步观察应在应用线程执行，不要阻塞 manager worker。

```rust,ignore
let mut options = ManagerOptions::default();
options.scope_limits.child_operations = 1;
let manager = AsyncOperationManager::new(options)?;
let mut parent = submit_nested(&manager.as_sync())?;
let outcome = parent.wait_outcome()?;
let settlement = parent.wait_settlement()?;
```

在没有额外取消或执行故障时，B 接纳后已占满根唯一的子名额。B 即使在父主体返回后继续运行，也仍占这个名额；它创建 C 时调用的 manager.try_submit 返回 Busy，C 的闭包不会执行。submit_nested 将拒绝转换为 B 的业务错误，而 B 是 Required，所以父 outcome 不会是成功。父 settlement 则仍需汇总 B 的收尾。例中的 manager 最终应按 8.3 节执行关闭。

#### 8.1.3 示例：子操作有名额，资源登记仍可能失败

下面说明 [family_and_capacity.rs](examples-0.2.0/family_and_capacity.rs) 的容量路径：入口先以资源额度 2 创建 manager 并运行 `run_case(2)`，再以额度 3 创建独立 manager 并运行 `run_case(3)`。这些极小数字只用于展示拒绝与成功边界，不应直接作为服务配置。

文件中的入口会观察父 outcome 与 settlement，并在每次运行后关闭对应 manager；完整 Cargo 目标源码见第 4.5 节。

在没有额外取消或执行故障的情况下，父操作的两个 manage 成功后，根的资源额度已用完。_request_metadata 虽然未被读取，仍然是一个存活的资源引用；即使把该引用放进更短的 Rust 代码块，父资源范围尚未结束，资源仍被范围持有，名额也不会立即归还。

子提交可以成功，因为子名额和资源名额分别计数。子操作得到自己的 scope 后，第三次 manage 返回 ManageResourceError<Vec<u8>>，其 kind() 为 ResourceManageErrorKind::CapacityExceeded。错误对象保留刚创建的 Vec；本例把错误转成 String，随后由普通 Rust 析构处理这段尚未登记的内存。特殊清理资源应采用第 6 章的预留及拒绝处理协议，不能照搬这里的转换。

父代码收到子失败后返回错误；默认 Required 也会阻止父成功。调用方观察到 Failed 或 ChildFailed 等非成功 outcome 后，仍需等待父 settlement，确认此前已登记的两个父资源完成收尾。失败的第三次登记没有新增资源记录，也没有消耗一个永远不归还的名额。

如果将 manager 与本次请求的 resources 都提高到 3，且其他条件满足，子登记就有容量。只提高子配置不能突破根额度；只增加 child_operations 也不能解决资源登记不足。实际服务应根据错误发生在“提交”还是“资源登记”，分别调整并发或资源策略。

父绑定 manager 的 try_submit 本身不等待，临时任务名额、结果槽或执行许可不足时返回 SubmitError<F>，reason() 为 AdmissionError::Busy；submit 则等待准入。资源 manage 的额度不足是上述 CapacityExceeded。提交被拒绝时闭包未执行，也没有建立子关系；若闭包捕获 ResourceRef，错误会连同闭包保留这些引用。需要重试时保留闭包，不再执行时明确放弃它，避免误以为拒绝会立即归还所有捕获引用。

不要在已耗尽执行器或额度的父任务中循环重试子提交。先限制扇出、把普通顺序步骤留在同一主体中，或交由外部调度安排重试。尤其不能让同步 worker 阻塞等待必须由同一批 worker 执行的后代；异步等待能避免占住线程，但不能凭空增加容量。

#### 8.1.4 用 rs-budget 约束实际资源

框架内部采用 rs-budget 的 ManagedResourcePool 管理可归还的容量。它的 try_acquire(amount) 返回 ManagedResourcePermit；permit 销毁时归还容量，复制 pool 句柄则仍共享原来的容量池。等待准入和唤醒由 rs-operation 负责。

应用也可以用独立的容量池限制模型内存或连接数：先申请 permit，再将 permit 与实际资源放入同一个 struct，并通过 scope.manage() 登记。复制 ResourceRef 不重复申请额度，操作主体返回也不立即归还额度；应当在资源所属范围结束且所有使用者归还引用后，先释放实际资源，再销毁 permit。资源 struct 的字段销毁顺序或清理实现需要保证这个顺序。异步清理失败时，恢复对象继续持有 permit；转交资源时也应一起转交，让接收方在实际释放后归还额度。

这类业务预算与框架的资源登记名额分别记账。需要限制累计处理量时使用 ResourceBudget，消耗后不归还；只校验单次输入大小时使用 ResourceLimit。ContextOptions.budget 的 BudgetPolicy::check() 仅用于附加策略检查，不替代容量申请。这些限制约束已登记的资源，不会自动限制进程的总内存。

### 8.2 期限与停止是两类等待约束

OperationOptions.context 接受 ContextOptions，其中可以配置 ClockSource、Deadline 和 BudgetPolicy。Deadline 是带时钟域的绝对时间，时钟由应用运行环境提供；子操作只能收紧继承期限。

准入前已经过期会拒绝未执行主体，执行期间到期则请求停止。观察者的带期限等待只限制本次观察，不修改任务期限，也不终止资源使用。

需要有界观察时，同步 observer 使用 wait_settlement_for(Duration)，异步 observer 使用 wait_settlement_until(Deadline)。返回 Pending 后保存观察者继续查询，不能把本次等待到期当作释放成功。

### 8.3 manager 和资源服务如何关闭

一个应用通常长期持有 manager，每次请求提交独立操作。OperationGroup/AsyncOperationGroup 划分某个领域服务的准入和关闭范围；跨请求资源由 ServiceResourceScope 管理，请求只持有资源引用。

关闭顺序是停止准入，再等待操作和资源。准入关闭后，等待中的根 submit 会及时返回拒绝；Drain 时仍符合条件的已接纳父操作可以继续提交后代。关闭 group 也会唤醒相关等待者。Drain 允许已接纳操作完成必要子工作；Cancel 进一步请求停止、禁止新子提交并中断准入等待。Drain 可以升级到 Cancel，不可重新开放准入。

`request_shutdown` 立即返回 `ShutdownHandle` 或 `AsyncShutdownHandle`。同步和异步 `wait()` 都返回 `Result<ShutdownReport, ObservationError>`；在目标自身操作中等待会得到 `WouldDeadlock`。同步 `wait_for(Duration::ZERO)` 只做快照检查，即使从目标操作内调用也不会阻塞；正时长等待若依赖当前操作结束则返回 `WouldDeadlock`。异步 `wait_until` 先验证时钟和时间域，再检查完成状态；已过期的 deadline 返回 `Pending`，不等待也不报自等待错误。

`wait_for` 的 deadline 若无法由平台 `Instant` 表示，会等待至关闭完成，而不会把超大时长当作零。`Pending` 时保留 handle，检查活动任务、未确认使用者、清理、恢复和服务资源计数，再次等待。Drain 完成后策略仍为 Drain；收到 Cancel 后策略单调升级为 Cancel，后续 Drain 不会降级它。

自等待检查覆盖框架执行上下文中直接等待自身 manager 或 group 的情况，并保守拒绝会阻塞当前执行器进展的同步等待。它无法发现任意脱离框架的跨任务依赖环；若外部任务仍需等待当前主体返回，应用应把最终关闭等待移到主体之外。

服务状态中保存的 ResourceRef 也算使用引用。关闭资源范围不会强行撤销它们，应用需要结束这些使用，才能让计数归零。runtime 和领域执行器应保持可用到其清理责任完成，再按各自协议关闭。

## 9. 核心组件和 API 速查

本节回顾正文中用到的类型；完整字段和方法签名见[设计文档第 8 章](design-0.2.0.zh_CN.md)。

| 组件 | 输入或创建方式 | 返回/代表的对象 |
| --- | --- | --- |
| OperationManager | ManagerOptions | 同步提交和观察入口 |
| AsyncOperationManager | ManagerOptions 或 as_async | 同一生命周期模型的异步入口 |
| scope.manager() | 当前 scope 的 &self | 绑定该操作为父级的 OperationManager；可按相同方式提交 |
| OperationScope | 框架按每次提交创建 | 本次操作的资源、子操作和上下文入口，不可 Clone |
| manage(T) | 已创建的 T，转移所有权 | ResourceRef<T> 或保留 T 的错误 |
| manage_with(T, C) | T 与 AsyncCleanup<T> 策略 | 相同 ResourceRef<T>，但最终运行显式清理策略 |
| ResourceRef<T> | manage/get/clone | 可访问 T 的共享使用引用 |
| ResourceKey<T> | ResourceRef::key | 身份和类型凭据，不延长存活 |
| OperationHandle<T, E> | 同步提交成功 | 唯一结果权、停止及收尾观察 |
| AsyncOperationHandle<T, E> | 异步提交或 into_async | 同一结果权的异步等待形式 |
| OperationObserver | handle.observer / into_observer | 可复制的只读观察，无业务结果接收权 |
| ResourceScope | scope.resource_scope | 可显式结束的局部资源所属范围 |
| SettlementReport | wait_settlement | 稳定的自动收尾报告，不保证没有恢复责任 |
| RecoveryHandle | 报告 recoveries | 查询和显式重试恢复组 |
| ShutdownHandle | request_shutdown | 同一次关闭过程的等待和快照入口 |

### 9.1 最需要区分的三个返回值

```text
// 提交是否被接纳：失败时原 F 尚未执行。
Result<OperationHandle<T, E>, SubmitError<F>>

// 业务最后发生了什么：包括成功、领域失败或停止等。
Result<OperationOutcome<T, E>, OutcomeError>

// 收尾报告是否成功观察到：报告内部再区分释放、转交和恢复。
Result<Arc<SettlementReport>, ObservationError>
```

以上是类型速查，不是可执行语句。异步 wait_settlement 直接返回 Arc<SettlementReport>，同步版本另有阻塞观察错误，不能机械地给 async 版本加问号。

### 9.2 常见 Drop 行为

| 丢弃的对象 | 结果 |
| --- | --- |
| ResourceRef<T> | 归还这一份使用引用，不直接强制清理 |
| ResourceKey<T> | 不改变资源存活 |
| ResourceScope owner | 结束范围持有；等待剩余引用 |
| 未完成操作 handle | 请求停止，放弃结果，不阻塞收尾 |
| observer | 只放弃观察，不停止操作 |
| 一次等待 future | 结束本次等待，仍持有 handle 时可再次等待 |

## 10. 按症状排障

| 症状 | 检查顺序和后续动作 |
| --- | --- |
| 业务结果到了，收尾仍在等待 | 查后台引用、返回值中的 ResourceRef、子操作和清理依赖；不要拿着资源等自身释放 |
| 使用引用已归零，但资源没清理 | 查所属范围是否仍开放；引用为 0 本身不构成释放条件 |
| ScopeEnded | 资源获取时检查所属范围；通过父绑定 manager 提交时检查父主体是否已结束，旧视图不能重新开放父级 |
| Stopped（准入拒绝） | 父级已接受停止，不能继续提交；这不代表已有使用者已退出 |
| 取消后模型仍运行 | 查后端是否支持中断；保留输入和执行许可，等待实际退出 |
| Busy | 查活动/排队数、结果槽、等待者及 executor；读取旧结果、处理恢复或拒绝请求 |
| WouldDeadlock | 查是否在框架 worker 或异步驱动线程上做阻塞等待；改为异步等待或外部线程 |
| 资源同时被修改 | scope 不提供独占权；检查应用锁、原子或领域同步 |
| RecoveryRequired | 按资源 ID 和失败阶段定位；重试或转交整个恢复组 |
| Unconfirmed / driver 丢失 | 先恢复释放证据和执行环境，不能直接报告成功或强行清理 |
| 关闭持续 Pending | 分别查执行、使用引用、清理、恢复和服务资源；保持运行环境可用并继续观察 |

## 11. 适用边界与延伸阅读

资源引用依赖使用者正确持有，不追踪独立复制的裸句柄，也不回收引用环。操作停止不撤销外部副作用；是否重试或补偿由业务判断。清理与恢复仍需要可用执行环境，进程退出后的恢复需由应用持久化信息。

- [0.2 API 设计](design-0.2.0.zh_CN.md)：本手册唯一的目标接口依据，包含精确签名和状态契约。
- [同步完整示例](examples-0.2.0/detect_file.rs)：与第 2 节相同的目标代码。
- [统一提交与函数复用](examples-0.2.0/submission_context.rs)：同一业务函数接受根或父绑定 manager。
- [父子资源与容量示例](examples-0.2.0/family_and_capacity.rs)：第 4.5 节和第 8.1 节的目标函数。
- [不可中断推理接入](examples-0.2.0/inference.rs)：第 5.2 节的模型接口与提交函数。
- [中文 README](../README.zh_CN.md)：项目定位和场景。
- [0.2 适配器接入指南](adapter-guide.zh_CN.md)：围绕 ResourceRef、scope.manager() 和 settlement 组织领域资源接入。

英文对应版本见 [0.2.0 Developer Guide](developer_guide-0.2.0.md)。四个 Cargo 示例以本目录源码为单一依据；本指南解释其接入路径，实际输出以示例运行结果为准。

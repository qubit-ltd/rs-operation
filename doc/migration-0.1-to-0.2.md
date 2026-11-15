# Migrating from qubit-operation 0.1 to 0.2

[中文迁移指南](migration-0.1-to-0.2.zh_CN.md) · [0.2 Developer Guide](developer_guide-0.2.0.md) · [Historical 0.1 guide](user_guide.md)

Version 0.2 replaces the supervisor/request protocol with managers, operation scopes, and resource references. This is a source-breaking migration. Update call sites to the 0.2 public API; do not treat the retained 0.1 guide as current usage.

## API mapping

| 0.1 | 0.2 | Migration note |
| --- | --- | --- |
| `OperationSupervisor` | `OperationManager` or `AsyncOperationManager` | The application owns the manager and chooses a sync or async view. |
| `SupervisorLimits` | `ManagerOptions` | Configure manager capacity with current options. |
| `start` / `start_blocking` | `try_submit` / `submit` | Accepted work receives an operation scope; `try_submit` does not wait for capacity. |
| `OperationScope::register_resource` + `acquire_use` | `OperationScope::manage` | Returns a `ResourceRef<T>` that owns a use reference. |
| `OperationOutcome` | `OperationOutcome` | Business result remains separate from settlement. |
| `released_blocking` | `wait_settlement` | Observe the new settlement report and inspect release/recovery state. |
| `shutdown_blocking(mode, timeout)` | `request_shutdown(mode)` then `wait_for(timeout)` | Shutdown observation is a handle; retain it to continue after `Pending`. |
| `ServiceGroup`-style local boundary | `OperationGroup` / `AsyncOperationGroup` | A group provides local admission and shutdown while sharing the manager lifecycle. |
| async shutdown `wait().await -> ShutdownReport` | `wait().await -> Result<ShutdownReport, ObservationError>` | Propagate or handle `WouldDeadlock`. |

## Move ownership to the application manager

Create one manager at service startup and inject a clone or view into request handlers. Use a group when one subsystem needs a bounded admission and independent shutdown boundary. The group routes accepted operations; each operation's scope owns its own request resources.

```rust,ignore
use qubit_operation::{
    GroupOptions, ManagerOptions, OperationManager, OperationOptions,
    OperationOutcome, ShutdownMode,
};

fn setup() -> Result<(OperationManager, qubit_operation::OperationGroup), Box<dyn std::error::Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let inference = manager.group(GroupOptions { capacity: 4 })?;
    Ok((manager, inference))
}

fn submit_document(
    inference: &qubit_operation::OperationGroup,
    bytes: Vec<u8>,
) -> Result<OperationOutcome<Option<&'static str>, String>, Box<dyn std::error::Error>> {
    let mut operation = inference.try_submit(OperationOptions::default(), move |scope| {
        let input = scope.manage(bytes).map_err(|error| format!("{:?}", error.kind()))?;
        Ok::<_, String>(input.starts_with(b"%PDF-").then_some("application/pdf"))
    })?;
    Ok(operation.wait_outcome()?)
}

fn close_service(
    manager: &OperationManager,
    inference: &qubit_operation::OperationGroup,
) -> Result<(), Box<dyn std::error::Error>> {
    let group_shutdown = inference.request_shutdown(ShutdownMode::Drain);
    group_shutdown.wait()?;
    manager.request_shutdown(ShutdownMode::Drain).wait()?;
    Ok(())
}
```

`scope.manage(bytes)` transfers the resource into the request scope and returns a `ResourceRef`. For a file, provider request, or native buffer, pass a clone to each real user and retain it until that user has stopped accessing the value. Do not replace this with a copied path or raw handle if that would lose the lifecycle reference.

## Migrate cleanup and resource handoff

For ordinary Rust-owned values, `manage(value)` uses normal `Drop`. For asynchronous or fallible external cleanup, use the current `manage_with(value, cleanup)` API and inspect `SettlementReport`. Settlement may report `Released`, `Transferred`, or `RecoveryRequired`; it is not equivalent to the business outcome.

An adapter that hands input to a worker should transfer a `ResourceRef` to that worker and wait for the worker's actual completion signal. A cancellation request or dropped future alone does not prove a native provider has stopped using the input. Keep the worker's resource reference and any execution permit until confirmed exit. The public patterns are illustrated by [inference.rs](examples-0.2.0/inference.rs) and the [adapter integration guide](adapter-guide.zh_CN.md).

## Handle asynchronous shutdown errors

The async shutdown `wait` now reports an observation error. Add `?` when the enclosing function already returns a compatible `Result`, or match the error explicitly:

```rust,ignore
let shutdown = manager.request_shutdown(qubit_operation::ShutdownMode::Drain);
match shutdown.as_async().wait().await {
    Ok(report) => println!("closed: {:?}", report.snapshot),
    Err(qubit_operation::ObservationError::WouldDeadlock) => {
        eprintln!("move shutdown waiting outside the managed operation");
    }
    Err(error) => return Err(error.into()),
}
```

The sync `wait()` also returns `Result<ShutdownReport, ObservationError>`. `wait_for(Duration::ZERO)` remains a nonblocking snapshot and can be used inside the target operation. Positive blocking waits can return `WouldDeadlock` when the current operation is part of the target. `wait_until` checks clock availability and domain; an expired deadline returns `Pending` before self-wait detection.

Direct self-wait detection covers managed execution context and matching manager/group targets. It does not discover arbitrary dependency cycles among detached application tasks. Place final manager shutdown at the service entry point, outside operations that must finish for shutdown to complete.

## Update call sites and validate

1. Replace 0.1 imports and constructors, then migrate operation submission and resource registration.
2. Separate admission errors, operation outcomes, and settlement reports in each handler.
3. Give every background/native user its own `ResourceRef` until actual access ends.
4. Replace one-shot shutdown calls with a retained shutdown handle and inspect `Completed` versus `Pending`.
5. Propagate or match async `wait()` errors, then review all `wait().await` sites.
6. Compile all targets and run the current examples. See the [0.2 developer guide](developer_guide-0.2.0.md) for the full lifecycle path.

The 0.1 documents remain available as historical references: [English](user_guide.md) and [中文](user_guide.zh_CN.md).

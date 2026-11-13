# User guide (0.1.0)

[中文用户指南](user_guide.zh_CN.md) · [README](../README.md) · [API reference](https://docs.rs/qubit-operation) · [Adapter guide (Chinese)](adapter-guide.zh_CN.md)

This guide is for Rust developers managing owned operations and their cleanup. In a download, transform, and publish pipeline, publishing the artifact and releasing temporary resources are separate events. The runnable MIME lifecycle example below demonstrates the complete public API path; later sections cover admission, child work, recovery, and service shutdown.

## Start a pipeline

Use Rust 1.94 or later. Add the published crate to your application’s `Cargo.toml`:

```toml
[dependencies]
qubit-operation = "^0.1"
```

The crate has no default features or runtime dependencies. Keep an `OperationSupervisor` for the service lifetime and shut it down explicitly. From the crate directory, run:

```sh
cargo run --example sync_pipeline --locked
```

The following complete example uses the public API with an in-memory input: define an application cleanup owner, register it, acquire a use lease, inspect the input, read the business result, inspect settlement, and drain the supervisor. The same lifecycle is available as [`mime_lifecycle.rs`](../examples/mime_lifecycle.rs); run it with `cargo run --example mime_lifecycle --locked`. It deliberately uses a buffer and a PDF signature check; it is an ownership exercise, not a MIME library.

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
use qubit_operation::report::{CleanupOutcome, OperationOutcome, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};

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

    let outcome = detection.result_blocking()?;
    let settlement = detection.released_blocking()?;
    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));

    match outcome {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection outcome: {other:?}"),
    }
    if settlement.all_released {
        println!("input released");
    } else {
        eprintln!("input settlement: {:?}; failures: {:?}",
            settlement.resource_phase, settlement.cleanup_failures);
    }
    if !shutdown.is_complete() {
        eprintln!("shutdown pending: {shutdown:?}");
    }
    Ok(())
}
```

A real file adapter stages and deletes files, while a command adapter starts `file` or `ffprobe`, tracks process exit and reaping, and holds the input lease until all readers stop. rs-operation coordinates those owners; it does not implement either adapter.

A non-success outcome requires an application decision to report, retry, or compensate. Check `settlement.all_released`, `resource_phase`, and cleanup failures before treating resources as released. If shutdown reports pending requests, retain a shutdown observer and check again; a timeout limits that wait and does not stop work.

### Admission and rejected work

`OperationSupervisor::start(options, body)` admits an owned `FnOnce(OperationScope) -> Future`. A successful return means the operation was accepted; it may still be queued. Capacity or deadline rejection returns `StartError<F>`, which retains the body that has not run. Inspect `reason()` and, if the application will retry or route the request elsewhere, recover the body with `into_parts()`:

```rust
let supervisor = OperationSupervisor::new(SupervisorLimits::default());
match supervisor.start(ScopeOptions::default(), body) {
    Ok(handle) => { /* observe the accepted operation */ }
    Err(error) => {
        let (reason, unstarted_body) = error.into_parts();
        // Reject, queue elsewhere, or retry under an application policy.
        // The body has not run; its captured inputs remain application-owned.
        let _ = (reason, unstarted_body);
    }
}
```

Do not retry admission in an unbounded loop. A rejected body may capture files, buffers, or permits that still need an owner.

## Result and settlement

`result_blocking` and `result().await` consume the typed result once; `released_blocking` and `released().await` allow repeated observation of final settlement. Outcomes include `Succeeded(T)`, `Failed(E)`, `ChildFailed`, `Stopped`, and `Panicked`. The business result may arrive while children or cleanup are still running. Use asynchronous observation on runtime workers rather than blocking them. `run_to_completion` waits for both result and settlement when the result does not depend on the current scope remaining alive.

If `T` contains access to a resource owned by the same scope, do not hold its use lease while waiting for that scope's release: cleanup waits for the lease. Release it inside the body or transfer the resource and its dependency group with `transfer_resource_to` to another admitted scope before waiting. If transfer fails, the source scope retains ownership.

`OperationHandle::observer()` creates a repeatable observer without consuming the handle. The handle's typed result is one-shot; the release report is repeatable. If observing the result fails, still follow the application's cleanup policy and retain an observer for settlement. `run_to_completion` is useful when the returned value does not itself hold a lease that prevents the same scope from settling.

## Cancellation and children

`request_cancel(StopReason::UserCancelled)` requests cooperative stop. `Stopped` means the business result could not be determined; a request does not prove an uninterruptible call has returned. Dropping an unfinished `OperationHandle` requests stop and abandons its typed result, without waiting or forcibly terminating work. Dropping an observer does not cancel. `detach` permits later observation while the supervisor remains responsible for shutdown and cleanup.

Within an `OperationScope`, create child scopes or register managed children with `reserve_child`. Reserve, attach, and commit before starting child side effects. The scope observes committed children and their release. Default `FailFast` requests that siblings stop when a required child fails; it does not terminate them immediately. `Collect` lets independent work continue, but required child failure still prevents parent success. Register fallback work as optional and handle its result in the parent body.

For an external process or domain task, first obtain its control object and the input lease, then bind and commit the reservation. Start the side effect only after commit succeeds:

```rust
let mut reservation = scope.reserve_child(ChildRequirement::Required)?;
reservation.attach_with_uses(control, vec![input_use])?;
reservation.commit()?;
// Only now start the process or other external side effect.
```

If reserve, attach, or commit fails, do not start the work. Drop the uncommitted reservation and handle any inputs or permits that remain with the adapter. The control object must tolerate a stop request arriving after commit but before the executor starts it. `FailFast` and `Collect` describe behavior for required children; neither makes a required failure optional.

## Capacity and deadlines

Default `SupervisorLimits` allow 4 active roots, 64 queued roots, 64 admission waiters, 4 concurrent cleanups, and 64 unconsumed results; each root defaults to 64 child and 128 resource registrations. These bound registrations and result slots, not total process memory. The application controls the sizes of `T`, `E`, and captured values.

An unread result occupies its slot until consumed or explicitly abandoned; exhaustion can cause `Busy`. On registration failure retain the owner or input returned in the error and decide whether to reject or degrade the request. Avoid unbounded admission or cleanup retries.

`OperationContext` and `ContextOptions` provide deadlines. Expiry during admission rejects and returns the unstarted body; expiry during execution requests stop but cannot interrupt a blocking system call or native inference. An observation deadline bounds the wait, not the operation. Retain an observer and check settlement later.

Pass a `ContextOptions` value through `ScopeOptions` when creating the root operation. A deadline uses a monotonic clock domain; child contexts may tighten inherited limits but must not reset the overall time budget. Applications provide clocks and any byte or memory budget implementation. The library does not infer domain-specific resource usage.

```rust
let options = ScopeOptions {
    context: ContextOptions {
        clock: Some(clock),
        deadline: Some(deadline),
        ..ContextOptions::default()
    },
    ..ScopeOptions::default()
};
let handle = supervisor.start(options, body)?;
```

## Cleanup and recovery

Register a resource before creating or exposing its handle. Users acquire a non-cloneable `ResourceUse` through `acquire_use` and release it when finished. Cleanup becomes eligible after future use is closed and leases reach zero. Declare dependencies with `cleanup_after(resource, prerequisite)`; dependent cleanup cannot be considered complete while the prerequisite is unconfirmed.

Register each underlying resource once, then share its `ResourceId` with wrappers and adapters. Close future use before cleanup; existing leases remain valid until their users return them. If registration fails, the returned `RegisterError` retains the cleanup owner so the application can dispose of it, save it, or transfer it to another owner. A cleanup owner should keep everything needed for cleanup inside itself. Its `begin` method receives a separate `CleanupContext`, so cooperative cleanup can run after the business context has been cancelled.

An `OwnedCleanup` implementation consumes its owner and returns `CleanupOutcome::Cleaned`, `Transferred(receipt)`, or `FailedWithRecovery(failure, owner)`. A recovery owner continues to occupy registration capacity. A recovery summary does not mean recovery succeeded: use the typed key to retry, retrieve the owner, or explicitly retain responsibility. The framework does not retry or compensate business side effects and does not treat Unknown or Unconfirmed as successful release. Consult the [adapter guide](adapter-guide.zh_CN.md) and API reference for implementation contracts.

Use `cleanup_after(later_resource, prerequisite)` to express cleanup order after both resources have been registered. A dependency error must be handled before starting work that relies on the order. `transfer_resource_to` moves a resource and its dependency group to a different admitted scope; use the returned destination ID. On transfer failure the source scope continues to own the resource. A successful transfer moves responsibility, but does not itself prove that the resource has been released.

For recovery outside the original cleanup callback, retain the typed key returned by a `RecoveryRegistry` or the key available from the original scope. Use that same key and registry to retrieve the owner. A key is tied to its registry, resource generation, and owner type. Capacity failure returns the value to the caller. Settlement reports are snapshots; a later retry does not rewrite an earlier report.

```rust
let recovery = RecoveryRegistry::new(16);
let key = recovery.register(resource_id, staged_path)?;
// Save `key` with the recovery record. Later, use this same registry and key.
let staged_path = recovery.take_recovery(key)?;
retry_delete(staged_path)?;
```

Cleanup code can panic after its unique owner has moved into a future. If recovery after panic is required, an adapter must retain the required recovery state independently, for example in a cleanup capsule or registry. Do not reconstruct an owner from a path or summary unless the domain itself can verify that this is safe.

## Drivers and service lifetime

`PortableDriver` runs portable owned `Send + 'static` futures on its worker threads. It does not provide a runtime reactor, so runtime-bound I/O must be driven by its owning runtime through an application `ExternalDriver` and `DriverLease`. Synchronous work belongs on a `BlockingExecutor`; `ThreadExecutor` provides fixed workers and a bounded queue. Saturation rejects work and preserves an unstarted closure. Keep the corresponding runtime or executor alive until accepted work has settled.

For a long-lived service, retain the supervisor and use a `ServiceGroup` for request admission. Shared model sessions or service handles can live in a `ServiceResourceScope`; per-request files and buffers should remain in request scopes. This keeps a service resource's lifetime separate from each request's temporary input lifetime. A request permit remains held until that operation settles; do not separately count the same permit through both manual admission and `service.start`.

`close()` stops new admission but existing work may remain. `shutdown_blocking(ShutdownMode::Drain, timeout)` waits for existing work; `Cancel` also requests cooperative stop. After a timed-out shutdown, inspect pending requests, recovery, and resources, and keep a `ShutdownObserver` to continue waiting. The timeout does not make an unconfirmed resource `Released`.

```rust
let observer = supervisor.shutdown_observer();
let report = supervisor.shutdown_blocking(ShutdownMode::Cancel, timeout);
if !report.is_complete() {
    // Keep `observer` and continue with wait_blocking or its async API.
}
```

## Errors and troubleshooting

| Symptom or state | What to check and do |
| --- | --- |
| `Busy` / admission rejection | Inspect active and queued roots, unconsumed result slots, waiters, and executor capacity. Consume old results, reduce submissions, or reject requests. |
| `RegisterError` | Recover the returned cleanup owner and arrange cleanup or transfer; do not drop a file or native owner whose release is still required. |
| `ScopeError`, dependency, or transfer error | Do not start dependent side effects. Keep ownership in the source scope and fix the registration, capacity, or resource ID issue. |
| Business failure with cleanup failure | Handle the typed business error separately from `CleanupFailure`; retain its recovery owner and key. |
| `RecoveryRequired` | Retrieve or retry through the correct registry and typed key, or explicitly retain responsibility. |
| `Unconfirmed` / `DriverLost` | Keep dependent resources and capacity; investigate whether the driver can still confirm actual release. |
| `WouldDeadlock` | Avoid blocking submission or observation on a library worker; use async observation or an external thread. |
| Shutdown still pending | Check tasks, service resources, and recovery separately; keep the observer and ensure the relevant runtime can still make progress. |

`SettlementReport` provides operation and resource identifiers, resource phase, cleanup failures, transfer and side-effect summaries, and bounded diagnostic data. Preserve IDs in application logs so an adapter's process or file logs can be correlated with the report. Bounded summaries are not a complete audit log, and `record_effect` records facts supplied by the application; it does not reverse side effects.

`Released`, `RecoveryRequired`, and `Unconfirmed` lead to different application decisions: confirmed release permits normal disposal; recovery required means an owner or retry path must remain available; unconfirmed means the application cannot yet claim release. A successful business outcome alone never changes these resource facts.

## Drivers and limits

`start` uses portable owned `Send + 'static` futures; `PortableDriver` does not provide a runtime reactor. For synchronous work, `start_blocking` uses an application-provided `BlockingExecutor`; `ThreadExecutor` provides fixed workers and a bounded queue and rejects saturation. Runtime-bound work needs an `ExternalDriver` and `DriverLease` driven by the application on the bound runtime. This crate does not include a formal Tokio adapter.

Operation and cleanup futures must be owned and `Send + 'static`. Polling, user-value destructors, and cleanup implementations must respect the non-blocking contract. The coordinator cannot preempt a blocking poll or forcibly terminate native work. The library does not provide distributed recovery, business retry, or compensation.

## Troubleshooting

- **`Busy` or registration failure:** inspect active and queued work, result slots, and scope registrations; consume old results and address recovery owners.
- **Still not released after cancellation:** check whether uninterruptible work returned, all `ResourceUse` leases were returned, and cleanup prerequisites were confirmed.
- **Recovery required:** inspect the settlement failure summary and resource ID; use the corresponding scope's recovery key to retrieve or retry the owner. Business success alone does not discharge cleanup responsibility.
- **`WouldDeadlock`:** a blocking observation or wait was attempted on a library worker. Use async observation or wait on an external thread.
- **Pending shutdown:** the timeout bounds this wait. Retain the shutdown observer and continue checking requests, resources, and recovery counts.

Continue with the [API reference](https://docs.rs/qubit-operation), [adapter guide](adapter-guide.zh_CN.md), [MIME lifecycle example](../examples/mime_lifecycle.rs), or [README](../README.md).

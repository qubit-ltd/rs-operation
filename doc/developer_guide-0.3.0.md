# rs-operation 0.3.0 Developer Guide

[中文](developer_guide-0.3.0.zh_CN.md) · [Design](design-0.3.0.md) · [README](../README.md) · [API reference](https://docs.rs/qubit-operation)

## Contents

- [1. What you should be able to build](#1-what-you-should-be-able-to-build)
- [2. Installation and application lifetime](#2-installation-and-application-lifetime)
- [3. From uploaded bytes to an inspection result](#3-from-uploaded-bytes-to-an-inspection-result)
- [4. Choose a submission mode and handle rejection](#4-choose-a-submission-mode-and-handle-rejection)
- [5. Observe outcomes, stops, and settlement correctly](#5-observe-outcomes-stops-and-settlement-correctly)
- [6. Compose inspections as parent and child operations](#6-compose-inspections-as-parent-and-child-operations)
- [7. Register, reserve, and clean up resources](#7-register-reserve-and-clean-up-resources)
- [8. Service groups and resources shared across requests](#8-service-groups-and-resources-shared-across-requests)
- [9. Configure capacity and backpressure](#9-configure-capacity-and-backpressure)
- [10. Deadlines, budgets, and effects](#10-deadlines-budgets-and-effects)
- [11. Interpret settlement and handle cleanup failure](#11-interpret-settlement-and-handle-cleanup-failure)
- [12. Integrate an external provider](#12-integrate-an-external-provider)
- [13. Shut down the service and recovery service](#13-shut-down-the-service-and-recovery-service)
- [14. Troubleshooting and development checks](#14-troubleshooting-and-development-checks)

## 1. What you should be able to build

This guide is for developers integrating `qubit-operation` into Rust services: operation callers, cleanup-policy authors, and external-provider adapter authors. It assumes familiarity with Rust ownership, `Arc`, and futures, but not the framework's internals.

The running scenario is document inspection after upload. A service receives bytes, runs inspection, and returns a decision. Requests may be cancelled, and inspection may run in another executor. A complete integration does more than receive a result: it handles the business outcome, tracks every resource user, assigns responsibility for failed cleanup, and can explain why shutdown completed or remains pending.

Start with sections 2–5, then consult the resource, budget, external-task, and recovery topics as needed. Read the [design document](design-0.3.0.md) when changing the underlying mechanisms. Throughout this guide, settlement means a disposition of lifecycle obligations: release, transfer, or retained recovery responsibility. It does not mean business success.

## 2. Installation and application lifetime

Use Rust 1.94 or later; Rust 2024 is recommended for the application:

```toml
[dependencies]
qubit-operation = "^0.3"
```

Create the manager in the application's startup entry point and inject it into the upload service. Request handling submits work and handles the outcome, with a defined path for observing settlement. Stop accepting requests before initiating manager shutdown. Avoid creating and destroying a complete executor set for every request.

Manager clones share the coordinator and capacities rather than creating new thread pools. `as_async()` / `as_sync()` also change only the view. To create children, obtain the submission view from the current `scope.manager()`; another root manager does not establish parentage.

## 3. From uploaded bytes to an inspection result

The following complete application-boundary example is also available as [upload_inspection.rs](../examples/upload_inspection.rs). Run `cargo run --example upload_inspection --locked`. `InspectionService` and `Inspection` are application-owned types, not crate APIs. HTTP routing, storage, and a real detector can attach outside this boundary.

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

The first input returns `Some("application/pdf")`, the second returns `None`, and both should report `released: true`. No match is a completed inspection, not an execution fault. The application can then accept, reject, or try another detector.

The manager is created at service startup. Uploaded bytes move into the body and are registered with its scope; the local `ResourceRef` drops when the body exits. The returned value owns no resource. The caller consumes the outcome, waits for settlement, and finally drains the manager. On registration failure, the example recovers and drops the `Vec`, which needs only memory release. File leases and remote handles need the retained cleanup path described in section 7.

This is still a simplified service example: it has no HTTP, persistence, full MIME detection, or production retry policy. On request failure, production code should also route settlement to logging, recovery, or background monitoring rather than merely formatting it into response text.

## 4. Choose a submission mode and handle rejection

### 4.1 Immediate admission or a bounded wait

`try_submit` does not wait for capacity; `submit` can wait for admission. The sync version blocks its caller, while the async version returns a future to await. On rejection, `SubmitError<F>` retains the uncalled closure; `into_parts()` recovers the reason and closure. External submissions retain `Arc<P>`. The application decides whether and when to retry or reconstruct inputs.

| Rejection | Meaning and next action |
| --- | --- |
| `Busy` | Immediate admission needs occupied capacity; retry later or apply upstream backpressure. |
| `CapacityExceeded` | A queue, waiter, or related quota is exhausted; inspect consumption and configuration. |
| `Closed` / `ScopeEnded` | The manager or target has closed; unbounded retries cannot repair this request. |
| `Stopped` / `DeadlineExpired` | Stop or time constraints prevent admission; end the request. |
| `ExecutorUnavailable` | The executor cannot accept work; inspect its lifecycle and capacity. |
| `AlreadyBound` | A prepared external adapter was already claimed; prepare a new instance. |
| `WouldDeadlock` | Blocking admission is unsuitable on this worker; use an immediate attempt or restructure composition. |

Admission failure differs from `OperationOutcome::Failed(E)`: the former did not accept the work; the latter is an accepted operation's application failure. Distinguish the stage before rerunning business work that may already have produced effects.

### 4.2 An async request entry point

This standalone integration function can live in the request-service module. Await it from your async runtime and pass the view returned by `manager.as_async()`. The manager's async executor drives the operation future. Inputs and result decisions match the sync example.

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

Async waiting does not make blocking I/O non-blocking. Put blocking work in an ordinary sync operation or an application-owned provider execution path, not in an async body or adapter poll. Likewise, do not call synchronous waits from async request tasks as a substitute for awaiting.

## 5. Observe outcomes, stops, and settlement correctly

### 5.1 A typed outcome is consumed once

The handle is the unique typed receiver. `wait_outcome` or `try_take_outcome` removes its `OperationOutcome<T,E>`; another take returns `OutcomeError::AlreadyTaken`. Handle reception errors separately from the enclosed execution outcome:

| Outcome | Application action |
| --- | --- |
| `Succeeded(T)` | Use the value and still inspect settlement. |
| `Failed(E)` | Handle the business error and arrange settlement observation. |
| `ChildFailed` | Inspect the required-child summary and parent policy. |
| `Stopped` | End the business flow according to the reason and keep tracking release. |
| `Panicked` | Record the panic summary without assuming cleanup completed. |
| `ExecutionFailed` | Investigate the executor or external-adapter protocol failure. |

`observer()` creates another read-only monitor without taking the result. `into_async()` / `into_sync()` transfer the unique receiver without cancelling. `into_observer()` discards the typed result without requesting stop. Dropping an unfinished operation's handle requests user cancellation and discards the result. Retain the appropriate handle or observer explicitly for ongoing background observation.

### 5.2 A stop request is not release evidence

Handle and scope `request_stop` return `Accepted`, `AlreadyRequested`, or `AlreadyFinished`: newly recorded, previously recorded, or execution already ended. Even the last case does not establish settlement. The first accepted stop reason is retained, and parent stops propagate downward.

Operation code should call `scope.context().check()` at suitable work boundaries; async code can await `stop_requested()`. A provider forwards stop into its own runtime and continues reporting actual input-use completion. The framework cannot forcibly interrupt arbitrary sync functions, system calls, or SDK work.

### 5.3 Release references carried by results first

If `T`, `E`, or anything they indirectly hold includes a `ResourceRef` needed by this settlement, take the outcome, process it, and drop those references before waiting. This includes clones derived from the result. Otherwise the wait depends on an object the caller still owns.

`run_and_settle` waits before returning the result, so use it only when success and error values cannot retain such references. The [resource-valued result example](examples-0.3.0/inference.rs) handles references in both success and error values.

From an external calling thread, `wait_settlement_for(timeout)` returns `Ready(report)` or `Pending(snapshot)`. Timeout ends only that observation; it does not cancel work or release resources. Synchronous worker settlement waits return `ObservationError::WouldDeadlock`. Observe from the caller rather than blocking the pool that must make progress.

## 6. Compose inspections as parent and child operations

An upload inspection might split into format detection, content scanning, and optional metadata extraction. Submit children through `scope.manager()` so they share the coordinator and join the current parent's lifecycle. Submitting on a root manager inside a parent body does not establish parentage automatically. See the runnable [family_and_capacity.rs](../examples/family_and_capacity.rs) and [submission_context.rs](../examples/submission_context.rs).

The child's `OperationOptions::requirement` selects whether it is required; the default is `Required`. An optional child's failure is reported but does not itself fail the parent. The parent's own `child_failure_policy` controls sibling behavior after required-child failure: default `StopSiblings`, or `ContinueSiblings`. Continuing siblings does not turn a required failure into success.

The parent outcome is published after its body ends and every Required child outcome is ready. Optional children do not block this outcome gate but still participate in parent settlement.

A returned parent body does not imply immediate parent settlement. The framework still tracks the child outcomes, input release, and settlement required by its lifecycle. Consume or explicitly discard child outcomes to avoid retaining result and path capacity. Do not drop unfinished child handles merely to “let the framework take over”: that requests cancellation.

A scope is not a cloneable, permanent submission permit. Its manager view retains the parent target without keeping that scope open. Admit children before leaving the parent body; later submissions through the retained view will be rejected.

This independently compilable async composition function can replace section 4's single-operation entry point. It configures the parent policy and child requirement explicitly, consumes the child result, and leaves parent-settlement waiting to the external caller. It does not block a synchronous worker. With only one child, ContinueSiblings becomes observable only when siblings are added.

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

Optional is appropriate for auxiliary work whose failure should not determine parent success. If format detection is required to accept a document, keep it Required. Optional changes outcome dependency, not resource or stop obligations.

## 7. Register, reserve, and clean up resources

### 7.1 Existing values and user references

`scope.manage(value)` registers an existing value; errors retain it. Resources require `Send + Sync + 'static`. A returned `ResourceRef<T>` dereferences to shared `&T`; each clone is an independent user that delays cleanup. Applications synchronize mutable contents using locks or other appropriate primitives.

`ResourceKey<T>` carries scope and generation identity but does not retain the resource. `get_resource` validates scope, type, and generation. Existing references remain readable after scope closure, but an old key cannot reopen lookup or resurrect a resource. A reference keeps the value alive, not the scope open for more work.

### 7.2 Secure capacity before creation

Call `reserve_resource()` before creating the value, then `commit` after creation succeeds. This independent operation helper can replace direct `manage` in the section 3 body; memory allocation stands in for application resource creation:

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

A reservation holds resource capacity and a cleanup dispatch permit. Dropping an unused reservation returns both. If creation fails before a resource exists, release the reservation. Stop can race with creation and commit; recover the resource and reservation from the commit error and use their cleanup path or another reliable application path. Custom cleanup uses `commit_with` and `retain_with_for_cleanup`; do not lose the cleanup strategy.

### 7.3 Custom async cleanup

For async release, create an attached scope with `scope.resource_scope(options)` and use its `manage_with`, or use `commit_with` on a reservation. `OperationScope` itself has no `manage_with` method. The following independently compilable policy and registration function releases bytes; a real remote-release implementation belongs to the application.

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

The attached scope's report participates in operation settlement. `end()` closes registration and begins cleanup coordination; cleanup becomes eligible only after the last user reference is released. Ordinary `manage` uses default drop, while `manage_with` invokes `AsyncCleanup<T>` and its `CleanupFuture<T>`:

- `Released` confirms release. Sending a release request alone is insufficient.
- `Failed { failure, resource }` returns diagnostics and the still-owned value for recovery.
- `Transferred(receipt)` proves receiver acceptance; it is not a marker for a planned transfer.

There is currently no downstream public API for configuring arbitrary cleanup dependency graphs or transferring resource groups. Internal machinery and report types do not let applications manufacture valid receipts. The public recovery handoff path is `RecoveryHandle::transfer_to` in section 11.

Cleanup context is independent of the business stop tree so cancellation does not directly cancel necessary cleanup. Policies must still report their own failures and uncertainty truthfully. Panic or an interrupted future is not confirmed release.

## 8. Service groups and resources shared across requests

Use `manager.group(GroupOptions { capacity })` when manager-wide limits cannot express a tenant or service boundary. Group capacity constrains admitted operations; it does not create another executor. `with_group(&group)` adds that constraint to an existing root/parent view while preserving parentage.

`ManagerOptions::max_groups_per_operation` defaults to 16. Rebinding the same group does not consume another slot. Exceeding the limit returns `GroupBindingError::CapacityExceeded`; other errors identify a different manager, closed group, or ended parent. Group `close_admission()` rejects direct submissions while already accepted parents can still submit descendants through scope views. Manager Drain rejects new roots but permits descendants from accepted open scopes; Cancel also rejects new ordinary child submissions.

Use `group.resource_scope(ServiceResourceOptions { capacity })` for a model or connection data shared across requests. Its resource-entry capacity is independent of each request's resources. Each user holds its own `ResourceRef`. On shutdown, prevent new use, `close()` the service resource scope, and release existing references. `close()` neither waits for the last user nor invalidates live references. See `tests/v02_service_resource_tests.rs` for complete behavior.

The following standalone sync function models acquiring, using, and closing a shared rule resource. Call it from an application thread with the root manager. The String stands in for a model; it loads no real model. It normally returns true and closes resources and the group only after request references are released. Production error paths must return cleanup responsibility to the service lifecycle rather than force exit while background users remain.

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

## 9. Configure capacity and backpressure

There is no single “concurrency number.” Different permits are returned at different stages:

| Configuration or permit | What to account for |
| --- | --- |
| `limits.active_operations` | Current production implementation's root lifecycle quota, returned at settlement; not simply CPU worker count. |
| `limits.queued_operations` / `admission_waiters` | Queued work and callers waiting for admission; waiting is bounded too. |
| `limits.unconsumed_outcomes` | Held until consumption or discard; avoid accumulating handles. |
| Parent `child_operations` | Charged along ancestor paths, returned after both settlement and outcome consumption/discard. |
| Group capacity | Also depends on settlement and outcome consumption/discard; ready outcomes may still occupy it. |
| `resources` / `resource_scopes` / `effect_records` | Resource entries, attached scopes, and facts in the scope hierarchy. |
| Executor settings | Separate sync, async, and cleanup workers, queues, or task capacity. |

Manager defaults are: active 4, queued 64, waiters 64, unconsumed outcomes 64, cleanup concurrency 4, diagnostic events 256. Scope defaults are children 64, resources 128, resource scopes 64, effects 64. The async executor defaults to 4 workers, task capacity 68, and 16 polls per turn. Sync and cleanup executors each default to 4 workers and queue capacity 64. Treat these as starting points and size for holding time, peak input, and cleanup latency.

A complete `OperationOptions::limits` equal to `ScopeLimits::default()` selects the manager's whole `scope_limits`. If any field differs, the whole supplied structure replaces that default; fields are not merged individually. Child capacities remain constrained by every ancestor's effective limits. Consequently, an explicit request for exactly the complete default structure cannot be distinguished from inheritance. Construct the complete intended configuration before changing one field.

For example, construct a complete upload-service configuration rather than infer inheritance from unrelated defaults:

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

This operation explicitly requests resources=20 and effects=4. Pass the returned manager configuration to `OperationManager::new` and the operation configuration to submission. Descendant requests still cannot bypass smaller parent capacities.

Resource-entry limits do not bound the size of a `Vec`, error source, or provider allocation. On Busy, first inspect unconsumed results, long-lived references, slow cleanup, and group occupancy. More workers cannot release those permits. External drivers from sync managers consume async executor capacity, so changing only the sync pool is insufficient.

## 10. Deadlines, budgets, and effects

`ContextOptions` can supply `clock`, `deadline`, and `budget`. Deadlines require a compatible `ClockSource`; ticks from different clock domains cannot be compared directly. A child's effective deadline is no later than its parent's. Child budget policies extend the inherited chain rather than replace it.

`context.check()` checks stop, expiry, and all inherited policies; expiry records `StopReason::Timeout`. Checking does not reserve task or resource capacity. Long operations should check at interruptible boundaries. `stop_requested()` exposes a stop signal; `deadline_signal()` provides a timer signal when clock and deadline exist. Configuring a deadline cannot interrupt arbitrary blocking code.

`scope.record_effect(EffectRecord { operation_id: scope.id(), resource_id, fact, description })` records application-reported effects. `EffectFact::Known` means the application confirmed the effect; `Unknown` means evidence is insufficient. For example, a provider may accept a write before the connection drops. Record uncertainty for later investigation rather than interpreting timeout as “nothing was written.”

Call this independent operation helper while the scope is open and the application has the corresponding remote evidence. Route any error into business diagnostics. It records uncertainty without querying the provider or compensating.

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

Effects use an inherited bounded budget. Exhaustion preserves existing records, returns an error, and increments overflow. Stop or scope end closes recording. Handle errors instead of treating a missing record as absence of an effect. The framework does not compensate automatically, and effects are not a durable transaction log.

## 11. Interpret settlement and handle cleanup failure

### 11.1 Know the report layers

Counts and arrays in a `SettlementReport` describe its local scope. `external_settlement` retains that adapter's original domain report. The outer `status` combines local and external disposition in order `RecoveryRequired`, `Transferred`, `Released`; `all_released()` checks this layer and its external report.

**It does not traverse `children`.** Child reports live at `children[i].settlement`; their counts and status are not flattened into the parent. To inspect the retained tree, recurse and account for truncated child details. This independent helper returns true only when that retained tree is complete and confirms release:

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

This is not a complete audit proof: other details may be truncated, and external evidence depends on truthful adapters. Maintain recovery owners/handles continuously when a complete recovery index is required; do not rely only on diagnostic arrays retained by a parent. A zero outer `recovery_count` does not exclude child or external responsibility. Deduplicate resources by identity before cleanup when reports overlap.

For `children`, `cleanup_failures`, `resource_reports`, `transfers`, and `recoveries`, inspect details together with total and truncated fields. Check overflow for effects as well. An empty array need not mean no events occurred. Published reports are stable, not live recovery views; use new handle/store snapshots for later progress.

### 11.2 Retry or hand off responsibility

Keep the `RecoveryHandle` supplied by the report. Clones share one recovery group rather than create independent cleanup. `try_retry()` admits an attempt; retain it and drive its wait method. These independent helpers demonstrate one direct retry and transfer followed by a store retry:

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

Create a `RecoveryStore` at recovery-service startup with `RecoveryStore::new(RecoveryStoreOptions::default())`. Only accepted transfers produce receipts and give the store the retry route. A failed transfer leaves the original handle usable; do not discard the only route to responsibility on error.

| RecoveryPhase | Application decision |
| --- | --- |
| `Pending` | Cleanup remains unresolved; retry according to application policy. |
| `Running` | An attempt already exists; do not clean the same resource concurrently. |
| `Resolved` | Release is confirmed; no retry is needed. |
| `Transferred` | The store has accepted ownership; use its retry route. |
| `Unconfirmed` | State is unresolved with no automatic action; retain responsibility and escalate. |

`RetryError` distinguishes `AlreadyRunning`, `AlreadyResolved`, `NoCapacity`, and `ExecutorUnavailable`. Ordinary failures can return to Pending. Panic or cancellation of a directly driven future can become Unconfirmed; not every failure can be retried automatically.

In direct mode, the first waiter drives cleanup. Dropping that pending driving future retains the unfinished action and records Unconfirmed. Once a store worker owns execution, cancelling observation does not stop it. Do not start a second cleanup merely because observation timed out; determine the existing attempt's and resource's state first.

### 11.3 History and persistence limits

Each snapshot keeps at most the 64 most recent failures, oldest to newest. `failures_total` counts failures since the last confirmed success with saturation; `failures_truncated` marks eviction of older entries. Confirmed success clears history and resets the fields. An attempt report's failures are the retained history at completion, not an event list for that attempt alone.

Truncated diagnostics do not automatically discard the recovery owner. This still provides neither crash recovery nor disk persistence. Restart recovery, remote-effect verification, retry intervals, and human escalation belong to the application. Emit events when they happen if complete history matters; snapshots cannot reconstruct evicted events.

## 12. Integrate an external provider

### 12.1 When an adapter is needed

An ordinary operation body needs no adapter. Implement `PreparedExternalOperation<T,E>` only when another SDK, executor, or provider owns execution and the manager must track it. The adapter implements `OperationControl + 'static`, including the control port's `Send + Sync` requirements. Submission uses a concrete `Sized` type; `T/E` require `Send + 'static`.

A prepared instance binds at most once, including across managers. Correlate summaries and reports with the manager-assigned OperationId. Submit via either manager's `try_submit_external(options, Arc<P>)`; rejection retains the Arc. Do not start irreversible provider work before admission and expect rejection to undo it.

### 12.2 Implement both complementary ports

| Method | Responsibility |
| --- | --- |
| `bind(&self, id)` | Claim identity once; AlreadyBound rejects, while Failed is accepted setup failure. |
| `activate(self: Arc<Self>, scope)` | Receive the scope and start work or hand off inputs. |
| `abandon(self: Arc<Self>)` | Release claimed but unactivated preparation state. |
| `poll_take_outcome` | Transfer the unique typed result. |
| `request_stop` | Forward cooperative stop to the provider. |
| `snapshot` | Return current identity and lifecycle facts. |
| `poll_outcome` | Publish matching identity and outcome-kind summary. |
| `poll_release` | Independently report that input use ended. |
| `poll_settlement` | Independently supply the domain settlement report. |

Every pending poll registers the latest waker; changes to its fact must wake it. Synchronize state checks and waker registration with the same lock or an equivalent protocol so an event cannot arrive between “not ready” and registration and be lost. Prefer waking outside locks, and keep callbacks short and non-blocking.

Publish a typed result once; summary, release, and settlement can become ready independently. The driver checks identity and outcome kind; mismatch becomes an execution fault. Failure of release or domain polling cannot manufacture successful cleanup and may leave settlement pending indefinitely. Panic is not a cancellation protocol.

After a bounded settlement wait returns `Pending`, inspect `handle.external_protocol_faults()` or the same method on an observer. The returned snapshot holds at most one first fault per port, ordered Release then Settlement. `Panicked` identifies a panic while polling; `IdentityMismatch` includes the provider's observed operation ID. These diagnostics do not change the business outcome or prove release. Release-port faults stop release polling and keep settlement pending; settlement-port faults stop settlement polling and do not publish domain evidence.

### 12.3 Verify the entire timeline

[provider_read.rs](examples-0.3.0/provider_read.rs) contains a complete simulated provider compiled as a library target; `tests/v03_external_tests.rs` executes its lifecycle demonstration. It calls no real SDK but covers every phase:

1. Bind stores manager identity; activation retains the scope and registers an input reference.
2. The application requests stop through the handle; the provider records it.
3. The provider publishes a Stopped summary and typed outcome while retaining input.
4. The caller receives the result while input drop count remains zero and settlement is pending.
5. Simulated use ends; the provider releases its reference and publishes ChildRelease.
6. The provider independently publishes its domain report, retained as external_settlement.
7. Local cleanup and domain evidence become complete, settlement is observed, then the manager shuts down.

Every actual user on a worker, SDK callback, or device queue must retain its ResourceRef until access ends. A stop notification, cancelled response, or typed outcome cannot replace that fact.

The domain report cannot wait for the manager's final report, which itself depends on the domain report. Ordinary sync tasks stay on the sync pool; external drivers from sync managers use `async_executor`, avoiding occupation of a synchronous worker they may depend on. This does not eliminate arbitrary application wait cycles.

## 13. Shut down the service and recovery service

Shutdown order should follow resource dependencies: stop new application requests, close direct admission, finish or stop accepted work, prevent new shared-resource use, close service scopes, release references, and advance cleanup and recovery.

`request_shutdown(Drain)` closes new root admission but lets accepted open parent scopes submit descendants and finish. `Cancel` also requests stop for unfinished operations. Retain the shutdown handle and call its wait or timed-wait methods. Shutdown cannot forcibly stop providers or resolve recovery on the application's behalf.

`ShutdownSnapshot` exposes active, queued, unconfirmed users, cleaning resources, recovery groups, and service resources as diagnostics. In the current implementation, service_resources counts service resource scopes and unconfirmed_users comes from Unconfirmed recovery records, not all ResourceRef instances. Combine it with scope and recovery snapshots. Both shutdown modes end service scopes owned by the manager. If requests have returned but shutdown is pending, inspect resource users and recovery rather than only business queues.

After a successful handoff to an independent store, the application must keep owning that store. Its `request_shutdown()` closes admission; waiting on the returned handle advances executor shutdown and join. It does not automatically retry every unresolved group. Advance or explicitly disposition unresolved recovery before completing recovery-service shutdown. Process exit is not release evidence.

## 14. Troubleshooting and development checks

| Symptom | Investigation and action |
| --- | --- |
| Outcome arrived, settlement waits | Inspect references in T/E, clones, and providers; end access and release them. |
| Parent all_released is true, but a child needs recovery | Inspect nested children and truncation; the method does not traverse them. |
| Business finished, new submissions remain Busy | Check unsettled root quotas, unconsumed results, group and child-path permits. |
| Provider stopped, manager still waits | Check outcome, release, domain settlement, and wakers independently; never invent release evidence. |
| External settlement times out | Query `external_protocol_faults()` on the handle or observer; inspect the failed port and observed identity. |
| External submissions are full while sync workers are idle | Inspect async_executor task capacity and driver callbacks. |
| Reservation succeeded but commit failed | Check stop/end races; recover resource and reservation and route cleanup. |
| Effects or report details are missing | Inspect total, truncated, and overflow fields alongside application logs. |
| Retry returns ExecutorUnavailable | Check not only workers but also Unconfirmed state without a safe retry action. |
| Shutdown stays pending | Use snapshots to identify resource users, cleanup, or recovery owners. |

From the crate root, run:

```bash
cargo run --example upload_inspection --locked
cargo run --example family_and_capacity --locked
cargo check --examples --locked
cargo test --locked
```

Use `tests/v03_external_tests.rs` for adapter contracts, `tests/v03_scope_tests.rs` for scope/group/effect behavior, and `tests/v03_lifecycle_tests.rs` for lifecycle separation. Exact signatures come from the current crate-root API. The private `src/v02/` directory name is not an older module to import.

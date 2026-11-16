# rs-operation 0.3.0 Developer Guide

[中文开发者手册](developer_guide-0.3.0.zh_CN.md) · [README](../README.md) · [Migration from 0.2](migration-0.2-to-0.3.md)

This guide describes the 0.3 API for coordinating managed operations with application-owned executors and providers. The manager owns admission, operation scope, stop propagation, outcome delivery, and settlement. Your adapter owns the actual external task and must report when it has stopped using its inputs.

## 1. External operation adapter

Implement `PreparedExternalOperation<T, E>` for a prepared task. `T` and `E` must be `Send + 'static`; the adapter type must be `Sized`, `OperationControl + 'static`. The trait has four lifecycle methods:

- `bind(&self, id)` claims this prepared operation exactly once. Return `ExternalBindError::AlreadyBound` if another submission claimed it. A `Failed(ExecutionFailure)` is an accepted operation whose adapter setup failed.
- `activate(self: Arc<Self>, scope)` starts external work with its manager-owned `OperationScope`.
- `poll_take_outcome` transfers the unique typed `OperationOutcome<T, E>` to the manager when ready. It must not clone or publish the result twice.
- `abandon(self: Arc<Self>)` releases a binding that was claimed but never activated.

Submit through `OperationManager::try_submit_external` or `AsyncOperationManager::try_submit_external`. Both return a normal typed handle on admission, or `SubmitError<Arc<P>>` with the original prepared adapter on rejection. `AdmissionError::AlreadyBound` is a new exhaustive-match case: handle it explicitly wherever you match admission errors.

The framework keeps the operation record alive after the business result is available. A provider reporting `Stopped` does not prove that its worker has released input. Keep each `ResourceRef` until the external user has actually stopped accessing it, and report release independently from outcome. See [provider_read.rs](examples-0.3.0/provider_read.rs).

### Synchronous manager capacity for external work

`OperationManager::try_submit_external` runs the adapter driver on the manager's `async_executor` capacity and worker threads. This keeps provider polling and child work from forming a same-pool blocking cycle. Keep adapter callbacks (`bind`, `activate`, `request_stop`, and poll methods) short and non-blocking; use them to publish facts and wake the driver, while the provider does its work independently. Ordinary synchronous `try_submit` jobs continue to use the synchronous worker pool. The guide's [complete simulated provider](examples-0.3.0/provider_read.rs) shows the separation between stop, typed outcome, input release, and domain settlement.

The example walks through the full sequence: activation stores the manager-provided scope and an input `ResourceRef`; the application requests stop through the handle; the simulated provider publishes a `Stopped` summary and typed outcome while retaining the input; the caller consumes that outcome and observes settlement still pending; only after simulated input use ends does the provider drop its reference and publish `ChildRelease` plus a domain report. The manager's final report retains that domain report, after which the demo shuts down the manager. The input is a marker value, so the example demonstrates lifecycle ownership rather than real I/O.

## 2. Outcome, stop, and settlement

`OperationHandle::request_stop` and its async counterpart return `StopRequestStatus`, not `bool`. Match all three states: `Accepted` means this request newly initiated stopping; `AlreadyRequested` means stopping was already recorded; `AlreadyFinished` means the operation had ended. `OperationScope::request_stop` returns the same enum. Update exhaustive matches accordingly.

Outcome and settlement remain separate observations. `wait_outcome` consumes the unique typed result. `wait_settlement` observes the resource and child-work report. If either `T` or `E`, including an object indirectly held by either, contains a `ResourceRef` that settlement is waiting to release, first take and drop that result and any references derived from it, then wait for settlement. Waiting for settlement while holding the returned resource can wait on your own reference. The synchronous and asynchronous `run_and_settle` convenience methods have the same restriction and can wait indefinitely if their returned `T` or `E` retains such a reference. See the [resource-valued result example](examples-0.3.0/inference.rs).

`SettlementReport` counts and detail arrays describe the manager's local scope. For manager-driven external work, `external_settlement` retains the adapter's domain report, including recovery handles and effects; the outer `status` combines both sources (`RecoveryRequired` before `Transferred` before `Released`), and `all_released()` includes the domain report. Check status and `all_released()` before deciding whether recovery is needed, then inspect `external_settlement`. Do not interpret a zero outer `recovery_count` as no recovery responsibility or treat the same resource recorded in both reports as two cleanup tasks. Adding a hand-written `SettlementReport` literal requires `external_settlement: None` for a leaf report.

## 3. Scope resources and groups

Use `OperationScope::reserve_resource` when capacity and cleanup responsibility must be secured before creating a resource. A failed reservation does not create the resource. Commit it only after creation succeeds, and retain the returned `ResourceRef` for every user. Service resource scopes and ordinary operation scopes share the manager's bounded cleanup and capacity rules.

`OperationManager::with_group` creates a manager view constrained by a service group while retaining its root or parent target. Distinct groups bound to one operation are limited by `ManagerOptions::max_groups_per_operation`, which defaults to 16. Configure the limit explicitly when the application has a different known topology. Exceeding it returns `GroupBindingError::CapacityExceeded`; other variants identify wrong manager, closed group, or ended parent scope. Duplicate binding of the same group does not consume another slot.

## 4. Effects, recovery, and shutdown

`OperationScope::record_effect` records an application-reported fact; it does not prove rollback, cleanup, or remote release. The inherited effect budget is bounded. When full, recording returns `EffectRecordError`, preserves existing records, and increases the report's overflow count. Stop and completion also reject later effect records.

Settlement may report resources requiring recovery. Keep the recovery owner reachable and follow the report; a diagnostic capacity limit can truncate summaries but does not discard the resource owner held by the recovery store. Manager shutdown can remain pending while external workers retain resources or cleanup/recovery remains outstanding. Preserve the shutdown handle and observe it again when the worker or recovery path makes progress.

Each recovery snapshot retains at most the 64 most recent cleanup failures, ordered from oldest to newest. `failures_total` counts all failure events since the last confirmed cleanup (saturating at `usize::MAX`), and `failures_truncated` tells whether older entries were evicted. Successful cleanup clears the history and resets both fields. Attempt reports expose the retained history at completion, not a separate per-attempt event list. Applications that need a complete log should emit each failure to their own logging system; the retained history bounds entry count, not the size of an individual error source.

## 5. Limits and examples

The crate coordinates lifecycles; it does not implement provider protocols, file or command adapters, business retries, compensation, or distributed recovery. Applications remain responsible for synchronizing shared resource contents and for making their adapter's stop/release facts truthful. The complete provider lifecycle is in [provider_read.rs](examples-0.3.0/provider_read.rs); [leased_temp.rs](examples-0.3.0/leased_temp.rs) is only a short stop/settlement helper excerpt. Both examples use a simulated provider boundary, not a real file or inference adapter. The resource-valued result example is in [inference.rs](examples-0.3.0/inference.rs). Cargo compiles these as library targets.

See [Migration from 0.2](migration-0.2-to-0.3.md) for source changes.

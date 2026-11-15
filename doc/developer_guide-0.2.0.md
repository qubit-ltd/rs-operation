# rs-operation 0.2.0 Developer Guide

[0.2 API design (Chinese)](design-0.2.0.zh_CN.md) · [中文开发者手册](developer_guide-0.2.0.zh_CN.md) · [README](../README.md) · [Historical 0.1 guide](user_guide.md)

This guide is for Rust developers building services that must keep resources alive while child work or native calls still use them. It covers the `qubit-operation` 0.2 API. Until the crate is published, use the checked-out `rs-operation` source with a Cargo path dependency. In your application's `Cargo.toml`, replace the placeholder with the path to that local checkout:

```toml
[dependencies]
qubit-operation = { path = "<path-to-rs-operation>" }
```

Cargo resolves `qubit-operation`'s dependencies from the checked-out crate manifest. After `qubit-operation` 0.2 is published, applications can instead use `qubit-operation = "^0.2"` (and any direct dependencies they use) from crates.io.

The 0.2 API is incompatible with 0.1. See the [migration guide](migration-0.1-to-0.2.md) before upgrading. The Chinese guide follows the same 11 sections and carries the same behavior and limits.

## 1. What problem does this crate solve?

An operation can finish before its resources are safe to release. An upload request may be cancelled while `file` still reads its staged input; a caller may stop waiting while native inference still uses a buffer; a service may stop accepting work while cleanup is still running.

`qubit-operation` tracks operation scopes, child work, resource references, outcomes, and settlement. A managed resource becomes eligible for cleanup only after its owning scope ends and every `ResourceRef` has been returned. The application still implements domain work, external adapters, and recovery decisions.

The running example is file type detection. The sections below move from a complete synchronous path to sharing, child work, async/native work, cleanup, recovery, capacity, and service shutdown.

## 2. Complete example: detect a file and close the manager

### 2.1 Example and dependency

The service opens an input file, reads its first five bytes, and checks for the PDF signature. This is a small file-header check, not full MIME detection. The same ownership rules apply when the application later calls `file`, `ffprobe`, or a model.

The complete target is [detect_file.rs](examples-0.2.0/detect_file.rs). `InputFile` is an application type; the manager and operation APIs come from `qubit-operation`. Use the path dependency above until the crate is available from crates.io.

```rust,ignore
use std::error::Error;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;
use qubit_operation::{ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode, ShutdownWait};

struct InputFile { file: Mutex<File> }
impl InputFile {
    fn open(path: PathBuf) -> io::Result<Self> { Ok(Self { file: Mutex::new(File::open(path)?) }) }
    fn detect(&self) -> io::Result<Option<&'static str>> {
        let mut file = self.file.lock().map_err(|_| io::Error::other("input lock poisoned"))?;
        let mut header = [0_u8; 5];
        file.read_exact(&mut header)?;
        Ok((header == *b"%PDF-").then_some("application/pdf"))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args_os().nth(1).map(PathBuf::from).ok_or("usage: detect_file <input-path>")?;
    let manager = OperationManager::new(ManagerOptions::default())?;
    let mut operation = manager.try_submit(OperationOptions::default(), move |scope| {
        let file = InputFile::open(path).map_err(|e| e.to_string())?;
        let input = scope.manage(file).map_err(|error| format!("{:?}", error.kind()))?;
        input.detect().map_err(|e| e.to_string())
    })?;
    let outcome = operation.wait_outcome();
    let settlement = operation.wait_settlement();
    let closing = manager.request_shutdown(ShutdownMode::Drain);
    let shutdown = closing.wait_for(Duration::from_secs(1));
    match outcome? {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection failed: {other:?}"),
    }
    let report = settlement?;
    if report.all_released() { println!("input cleanup completed"); }
    else { eprintln!("cleanup requires attention: {:?}", report.status); }
    for failure in report.cleanup_failures.iter() { eprintln!("cleanup failure: {failure:?}"); }
    match shutdown? {
        ShutdownWait::Completed(_) => println!("manager closed"),
        ShutdownWait::Pending(snapshot) => {
            eprintln!("shutdown pending: {snapshot:?}");
            let _report = closing.wait()?;
            println!("manager closed");
        }
    }
    Ok(())
}
```

Save it as `src/main.rs` and run `cargo run -- <input-path>`. A file beginning with `%PDF-` produces `MIME: application/pdf`. A short or missing file produces an operation error; a nonmatching header produces `Succeeded(None)`. Settlement and manager shutdown are observed separately from the business result.

### 2.2 Submission, result, and scope

`try_submit(options, body)` tries admission without waiting for capacity. On success it returns an `OperationHandle<T, E>`; on rejection, `SubmitError<F>` retains the unexecuted closure. The closure's `Result<T, E>` is the business result and is distinct from the admission result. Use `submit` when waiting for admission is appropriate.

The manager creates an `OperationScope` for each accepted operation and passes it to the closure. `scope.manage(file)` transfers the already-open `InputFile` into that scope and returns `ResourceRef<InputFile>`. The reference dereferences to the value and also represents one active use. The application's `Mutex` protects the mutable file cursor; the framework does not provide exclusive access.

When the closure returns, its default resource scope ends and its local reference is dropped. Cleanup can begin when all references are gone. A cloned reference held by a child keeps the value alive. If `manage` fails, its error retains the original value, which is dropped here when converted to a string; this is suitable only when ordinary `Drop` is sufficient.

### 2.3 Outcome and settlement are separate facts

`wait_outcome()` reports success, domain failure, stop, panic, or execution failure. `wait_settlement()` reports resource cleanup and recovery state. A successful MIME result does not prove cleanup succeeded, and successful cleanup does not turn a failed detection into success. A settlement may be Released, Transferred, or RecoveryRequired; inspect `all_released()`, reports, failures, and recoveries before deciding what the service should do.

If the operation's returned value itself contains a `ResourceRef`, take and finish using that value before waiting for its resource to settle, or the two waits can depend on each other.

## 3. Share resources and coordinate access

Clone a `ResourceRef` to give another task a use of the same value; it does not clone the underlying file. Each holder returns its reference when it stops accessing the resource. If a path or raw native handle is handed to an external process, the adapter must retain a `ResourceRef` until that process truly stops using the input.

A `ResourceScope` can keep a set of resources owned while references temporarily reach zero. Closing that scope ends ownership; remaining references still delay cleanup. Cloning references does not provide exclusive mutation. Use a mutex, atomics, or domain-specific synchronization where needed.

## 4. Child operations and background work

Submit child operations when independently managed work needs its own scope, result, or capacity accounting. Work can be Required or Optional according to `OperationOptions`; a required child failure can affect the parent's outcome, while optional work still participates in settlement. Child work remains part of the parent's lifecycle even if the parent body has returned.

Use the manager bound to the current scope to submit descendants at any depth. A group or manager view changes admission routing, not the ownership rule: an accepted child remains associated with its parent operation. A parent resource does not have to wait for every unrelated child resource if no child holds a reference to it.

The runnable [family and capacity example](examples-0.2.0/family_and_capacity.rs) shows parent/child resource transfer and admission limits. [submission_context.rs](examples-0.2.0/submission_context.rs) shows how one function can accept a root manager or a scope-bound manager. A returned business value can be sent back to the parent while settlement continues separately.

## 5. Async work and non-interruptible inference

### 5.1 Choose the manager view

Use `OperationManager` for synchronous bodies and `AsyncOperationManager` for async bodies. `as_async()` and `as_sync()` share the same lifecycle state; they are views, not independent managers. Async operation handles expose async observations.

### 5.2 Keep native inputs alive until actual use ends

The [inference example](examples-0.2.0/inference.rs) demonstrates the key integration pattern: the operation body hands a `ResourceRef` to the domain worker, awaits the worker's completion signal, and returns its business result. The worker keeps its own reference until native inference has stopped accessing the input. Cancellation can request that the work stop, but it cannot prove an uninterruptible native call has stopped.

Keep the domain executor, input, and execution permit available until the worker exits. If provider completion is unknown, retain the resource and recovery responsibility rather than treating a dropped future or missing result as proof of release. The application owns provider-specific cancellation and recovery.

### 5.3 Dropping a handle is not cancellation completion

Dropping an unfinished operation handle requests stopping and relinquishes result observation without blocking for cleanup. An observer can inspect progress without owning the result. Dropping one async wait future ends that observation attempt; the retained handle can be awaited again. These actions do not revoke resource references held by workers.

## 6. Synchronous Drop, async cleanup, and creation failures

Use `manage(value)` when ordinary Rust destruction is enough: a `Vec` frees memory and a `File` closes its handle. A custom `Drop` cannot report a failed external deletion. Use `manage_with(value, cleanup)` when cleanup needs async work or explicit success/failure reporting.

When creating an external resource can fail after partially allocating it, reserve a cleanup responsibility before starting creation, then attach and commit the resulting value through the documented resource protocol. This ensures rejected submission or partial creation still has an owner for cleanup. Follow the [adapter integration guide](adapter-guide.zh_CN.md) for the public resource handoff pattern; do not rely on crate-private adapter internals.

## 7. Settlement and recovery

Inspect a `SettlementReport` after the business result. `Released` means the framework received release evidence; `Transferred` means responsibility moved to another owner; `RecoveryRequired` means the application must act on the retained recovery object. A destructor completion signal does not prove an ignored error inside custom `Drop` was resolved.

Use recovery handles to inspect or retry a recovery group explicitly. Do not retry while an unknown user may still access the resource. An unresolved recovery group can keep manager shutdown pending. Preserve the resource and the execution environment until release is confirmed or responsibility is deliberately transferred.

## 8. Capacity, context, and service shutdown

### 8.1 Backpressure

`Busy` means an admission limit or result capacity prevented acceptance. Inspect the manager or group snapshot, consume old results, resolve recovery, or reject/defer the request. Waiting for capacity from a worker that must finish before capacity is freed creates a dependency; the framework may return `WouldDeadlock`.

### 8.2 Deadlines and stopping

Deadlines constrain waits; stop requests ask work to stop. They are separate controls. A timeout or expired deadline does not forcibly terminate a native call or revoke its resource references. The caller should retain the observer and choose whether to continue observing, return a pending response, or initiate domain recovery.

### 8.3 Close managers, groups, and service resources

An application usually owns a long-lived manager and submits one operation per request. `OperationGroup` and `AsyncOperationGroup` define local admission and shutdown boundaries for a service. `ServiceResourceScope` owns resources shared across requests; each request holds a `ResourceRef`.

Stop admission, then wait for accepted operations and resources. `ShutdownMode::Drain` lets accepted work finish required child work. `Cancel` additionally requests stopping and prevents new child submissions. A shutdown policy can upgrade from Drain to Cancel; it cannot downgrade or reopen admission. After shutdown, `mode()` continues to report the applied policy, even when a Drain has completed.

`request_shutdown` returns a `ShutdownHandle` or `AsyncShutdownHandle`. Both `wait()` methods have the signature `Result<ShutdownReport, ObservationError>`; waiting from an operation included in the target returns `ObservationError::WouldDeadlock`. Handle the error explicitly, for example:

```rust,ignore
let shutdown = manager.request_shutdown(ShutdownMode::Drain);
match shutdown.wait() {
    Ok(report) => println!("closed: {:?}", report.snapshot),
    Err(qubit_operation::ObservationError::WouldDeadlock) => {
        eprintln!("move final shutdown waiting outside this operation");
    }
    Err(error) => return Err(error.into()),
}
```

`wait_for(Duration::ZERO)` is a nonblocking snapshot check. If the target is not done it returns `Pending`; this remains safe inside a target operation. A positive duration that would wait for the current operation returns `WouldDeadlock`. An unrepresentably large `Instant` timeout waits until completion instead of overflowing or becoming zero.

Async `wait_until(deadline)` first checks clock availability and clock domain. If the deadline has already expired, it returns `Pending` immediately, including from a target operation. Otherwise it waits and can return `WouldDeadlock` if completion depends on the current operation. Retain the handle after `Pending`, inspect active operations, users, cleanup, recovery, and service resources, then wait again. Do not shut down the runtime or domain executor before their cleanup responsibilities finish.

The framework detects direct self-wait in its managed execution context and guards synchronous waits that would block required executor progress. It cannot discover every arbitrary dependency cycle between application tasks, especially tasks detached from its execution context. If another task needs the current body to return before shutdown can finish, move the final wait to an application entry point outside that body.

## 9. Core API quick reference

| Component | Role |
| --- | --- |
| `OperationManager` / `AsyncOperationManager` | Submit and observe managed operations |
| `OperationScope` | Per-operation resources, children, and context |
| `ResourceRef<T>` | Shared use reference that delays cleanup |
| `OperationHandle<T, E>` / `AsyncOperationHandle<T, E>` | Result ownership and lifecycle observation |
| `OperationObserver` | Read-only progress observation |
| `SettlementReport` | Stable cleanup and recovery report |
| `RecoveryHandle` | Inspect or retry a recovery group |
| `OperationGroup` / `AsyncOperationGroup` | Local admission and shutdown boundary |
| `ServiceResourceScope` | Shared service resource ownership |
| `ShutdownHandle` / `AsyncShutdownHandle` | Observe one shutdown process |

Keep three return stages distinct: admission (`Result<OperationHandle, SubmitError>`), business outcome (`Result<OperationOutcome, OutcomeError>`), and settlement observation (`Result<Arc<SettlementReport>, ObservationError>` for the synchronous handle). Async settlement observation returns its report directly.

Dropping a `ResourceRef` returns one use; dropping a `ResourceKey` does not affect lifetime. Dropping an unfinished operation handle requests stop and gives up the result. Dropping an observer gives up observation only. Dropping a wait future cancels that wait attempt, not the managed operation.

## 10. Troubleshoot by symptom

| Symptom | Check and next action |
| --- | --- |
| Outcome arrived but settlement is pending | Find child work, returned references, or cleanup dependencies; do not wait for a resource while holding its last use. |
| Reference count is zero but cleanup has not begun | Check whether the owning resource scope is still open. |
| `ScopeEnded` | Check whether the scope that owns the resource or parent operation has already ended. |
| Admission returns `Stopped` | A parent stop was accepted; this does not mean existing users have exited. |
| Model continues after cancellation | Check provider interrupt support; retain input and permits until actual exit. |
| `Busy` | Inspect active/queued work, result slots, waiters, and executor capacity; consume results or defer/reject. |
| `WouldDeadlock` | Move a blocking wait off a managed worker; for shutdown, move final waiting outside the target operation. |
| Concurrent mutation | Add application synchronization; scopes do not grant exclusive access. |
| `RecoveryRequired` | Use resource ID and failure phase to inspect and retry or transfer the complete recovery group. |
| `Unconfirmed` or lost driver | Restore release evidence and execution environment; do not claim success or force cleanup. |
| Shutdown remains `Pending` | Inspect active work, references, cleanup, recovery, and service resources; keep executors alive and observe again. |

## 11. Limits and further reading

The framework relies on users retaining references correctly. It does not track independently copied raw handles or collect reference cycles. Stopping an operation does not undo external side effects. Business retry, compensation, provider recovery, and persistence across process exit remain application responsibilities. Resource sharing is not exclusive access.

- [0.2 API design (Chinese)](design-0.2.0.zh_CN.md) for exact contracts.
- [Runnable file detection example](examples-0.2.0/detect_file.rs), matching Section 2.
- [Submission context example](examples-0.2.0/submission_context.rs).
- [Parent/child and capacity example](examples-0.2.0/family_and_capacity.rs).
- [Inference example](examples-0.2.0/inference.rs).
- [Adapter integration guide (Chinese)](adapter-guide.zh_CN.md).
- [Chinese README](../README.zh_CN.md).
- [0.1 to 0.2 migration](migration-0.1-to-0.2.md).

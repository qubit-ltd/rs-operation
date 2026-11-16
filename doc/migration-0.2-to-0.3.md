# Migrate from qubit-operation 0.2 to 0.3

Version 0.3 adds the public external-operation adapter protocol and bounded multi-group binding. Existing managed-operation code remains based on the same outcome, resource, and settlement model. Update exhaustive matches and any code that binds manager views to groups.

## External tasks

Implement `PreparedExternalOperation<T, E>` for work owned by an application executor or provider. Binding is one-shot: return `ExternalBindError::AlreadyBound` when the same prepared instance has already been claimed. Submit with `OperationManager::try_submit_external` or `AsyncOperationManager::try_submit_external`; rejected submissions return the original `Arc<P>` in `SubmitError`. Implement `activate` with the supplied scope, publish one typed outcome through `poll_take_outcome`, and implement `abandon` for a claimed task that was never activated. Keep resource references until the external worker actually stops using them; an early `Stopped` outcome is not proof of release.

`AdmissionError` gains `AlreadyBound`. Add it to exhaustive matches. The prepared object is still returned on admission failure, so the caller can inspect or discard it according to its own ownership policy.

## Stop request return value

`OperationHandle::request_stop`, `AsyncOperationHandle::request_stop`, and `OperationScope::request_stop` return `StopRequestStatus` rather than `bool`:

```rust,ignore
match handle.request_stop(StopReason::UserCancelled) {
    StopRequestStatus::Accepted => { /* this call newly requested stop */ }
    StopRequestStatus::AlreadyRequested => { /* stop was already recorded */ }
    StopRequestStatus::AlreadyFinished => { /* operation had ended */ }
}
```

Update exhaustive matches to handle all three variants. If the caller only needs a yes/no answer, use `matches!(status, StopRequestStatus::Accepted)` explicitly.

## Resource-valued outcomes

An outcome may contain a `ResourceRef<T>`. Take the unique result with `wait_outcome`, finish using and drop the returned references, and only then wait for settlement if settlement depends on those references being returned. Waiting for settlement while retaining a reference contained in the not-yet-consumed result can wait indefinitely on the caller's own reference. Outcome and settlement remain separate: an early business result or stop does not mean background work has released its inputs.

## Group limits and errors

`ManagerOptions` adds `max_groups_per_operation`, defaulting to 16 distinct service groups. Set it explicitly if a known application topology needs another bound. `OperationManager::with_group` now returns `Result<OperationManager, GroupBindingError>`; handle the added `CapacityExceeded` variant when binding would exceed the configured limit. Existing variants are `WrongManager`, `Closed`, and `ScopeEnded`. Binding an already-bound identical group remains idempotent and does not consume another slot.

The cap bounds per-operation group fan-out; it does not replace manager or scope resource/operation limits. The API and examples are documented in the [0.3 developer guide](developer_guide-0.3.0.md).

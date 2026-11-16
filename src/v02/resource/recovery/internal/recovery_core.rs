// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use super::State;
use crate::v02::report::CleanupFailure;
use crate::v02::report::CleanupStage;
use crate::v02::report::DiagnosticSummary;
use crate::v02::report::RecoveryId;
use crate::v02::report::RecoveryPhase;
use crate::v02::report::RecoverySnapshot;
use crate::v02::report::ResourceId;
use crate::v02::resource::recovery::AttemptObservation;
use crate::v02::resource::recovery::RecoveryAction;
use crate::v02::resource::recovery::RecoveryAttempt;
use crate::v02::resource::recovery::RecoveryPayload;
use crate::v02::resource::recovery::RecoveryRun;
use crate::v02::resource::recovery::lock;

/// Callback notified when a recovery enters or leaves a tracked store.
type TerminalCallback = Arc<dyn Fn(bool) + Send + Sync + 'static>;
/// Retry route used after ownership has transferred to a recovery store.
type RetryRoute = Arc<dyn Fn(RecoveryId) -> Result<RecoveryAttempt, crate::v02::report::RetryError> + Send + Sync>;

/// One dependency group with an owner capsule that outlives every public
/// handle and every retry attempt.
///
/// The core serializes phase changes and retains the payload/action until
/// cleanup is confirmed. If the last core handle is dropped while unresolved,
/// its destructor deliberately forgets those retained values rather than
/// dropping a resource whose cleanup was not confirmed.
pub(crate) struct RecoveryCore {
    /// Stable identity shared by recovery handles and reports.
    pub(in crate::v02::resource::recovery) id: RecoveryId,
    /// Resource identities managed by this recovery group.
    pub(in crate::v02::resource::recovery) resources: Arc<[ResourceId]>,
    /// Phase, retry count, retained owner payload, action, and failure history.
    pub(in crate::v02::resource::recovery) state: Mutex<State>,
    /// Observer notified when this recovery enters or leaves a tracked store.
    pub(in crate::v02::resource::recovery) terminal: Mutex<Option<TerminalCallback>>,
    /// Optional executor used to schedule asynchronous retry work.
    pub(in crate::v02::resource::recovery) executor: Mutex<Option<Arc<crate::v02::executor::CleanupExecutor>>>,
    /// Optional route for retries after ownership has transferred to a store.
    pub(in crate::v02::resource::recovery) retry_route: Mutex<Option<RetryRoute>>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::ready;
    use std::sync::Arc;
    use std::sync::Weak;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use super::RecoveryCore;
    use crate::v02::report::CleanupFailure;
    use crate::v02::report::CleanupStage;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ResourceId;
    use crate::v02::resource::recovery::RecoveryAction;
    use crate::v02::resource::recovery::RecoveryFuture;

    struct NoopAction;

    impl RecoveryAction for NoopAction {
        fn retry(&self, _payload: crate::v02::resource::recovery::RecoveryPayload) -> RecoveryFuture {
            Box::pin(ready(Ok(())))
        }
    }

    fn failure(resource_id: ResourceId, sequence: usize) -> CleanupFailure {
        CleanupFailure {
            resource_id,
            stage: CleanupStage::Drop,
            summary: DiagnosticSummary::new(&format!("failure-{sequence}")),
            source: None,
        }
    }

    #[test]
    fn test_retry_failure_history_retains_only_latest_sixty_four() {
        let resource_id = ResourceId::new(ManagerId::from_raw(1), 2, 3);
        let owner: crate::v02::resource::recovery::RecoveryPayload = Arc::new(());
        let core = Arc::new(RecoveryCore::new(
            vec![resource_id],
            Arc::clone(&owner),
            Arc::new(NoopAction),
        ));

        assert_eq!(core.snapshot().last_failures.len(), 0);
        assert_eq!(Arc::strong_count(&owner), 2);
        for sequence in 0..130 {
            let attempt = core.try_retry().expect("retry should be admitted");
            attempt.finish(Err(failure(resource_id, sequence)));
        }

        let snapshot = core.snapshot();
        assert_eq!(snapshot.last_failures.len(), 64);
        assert_eq!(snapshot.last_failures[0].summary.as_str(), "failure-66");
        assert_eq!(snapshot.last_failures[63].summary.as_str(), "failure-129");
        assert_eq!(snapshot.failures_total, 130);
        assert!(snapshot.failures_truncated);
        assert_eq!(snapshot.attempts, 130);
        assert_eq!(Arc::strong_count(&owner), 2, "eviction must not release the owner");
    }

    #[test]
    fn test_failure_history_truncates_at_sixty_fifth_failure() {
        let resource_id = ResourceId::new(ManagerId::from_raw(1), 2, 3);
        for count in [1, 64, 65] {
            let core = Arc::new(RecoveryCore::new(vec![resource_id], Arc::new(()), Arc::new(NoopAction)));
            for sequence in 0..count {
                let attempt = core.try_retry().expect("retry should be admitted");
                attempt.finish(Err(failure(resource_id, sequence)));
            }
            let snapshot = core.snapshot();
            assert_eq!(snapshot.last_failures.len(), count.min(64));
            assert_eq!(snapshot.failures_total, count);
            assert_eq!(snapshot.failures_truncated, count > 64);
        }
    }

    #[test]
    fn test_failure_history_metadata_saturates_clears_and_survives_transfer() {
        let resource_id = ResourceId::new(ManagerId::from_raw(1), 2, 3);
        let core = Arc::new(RecoveryCore::new(vec![resource_id], Arc::new(()), Arc::new(NoopAction)));
        {
            let mut state = super::lock(&core.state);
            state.failures_total = usize::MAX;
            state.failures_truncated = true;
            state.append_failure(failure(resource_id, 1));
        }
        let saturated = core.snapshot();
        assert_eq!(saturated.failures_total, usize::MAX);
        assert!(saturated.failures_truncated);
        assert_eq!(saturated.last_failures.len(), 1);

        assert!(core.mark_transferred());
        assert_eq!(core.snapshot().failures_total, usize::MAX);
        core.restore_from_store().expect("transfer should restore");
        assert_eq!(core.snapshot().last_failures.len(), 1);

        let attempt = core.try_retry().expect("retry should be admitted");
        attempt.finish(Ok(()));
        let resolved = core.snapshot();
        assert!(resolved.last_failures.is_empty());
        assert_eq!(resolved.failures_total, 0);
        assert!(!resolved.failures_truncated);
        assert_eq!(resolved.attempts, 1, "success must preserve the attempt count");
    }

    #[test]
    fn test_unconfirmed_failures_from_many_resources_are_bounded() {
        let resources = (0..130)
            .map(|slot| ResourceId::new(ManagerId::from_raw(1), slot, 3))
            .collect();
        let core = Arc::new(RecoveryCore::new(resources, Arc::new(()), Arc::new(NoopAction)));
        let attempt = core.try_retry().expect("retry should be admitted");
        let generation = super::lock(&core.state).generation;
        core.mark_cancelled(generation, None);

        let snapshot = core.snapshot();
        assert_eq!(snapshot.phase, crate::v02::report::RecoveryPhase::Unconfirmed);
        assert_eq!(snapshot.last_failures.len(), 64);
        assert_eq!(snapshot.failures_total, 130);
        assert!(snapshot.failures_truncated);
        assert_eq!(snapshot.attempts, 1);
        drop(attempt);
    }

    struct LockCheckingError {
        core: Weak<RecoveryCore>,
        lock_was_available_on_drop: Arc<AtomicBool>,
    }

    impl std::fmt::Debug for LockCheckingError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("LockCheckingError")
        }
    }

    impl std::fmt::Display for LockCheckingError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("lock checking failure")
        }
    }

    impl Error for LockCheckingError {}

    impl Drop for LockCheckingError {
        fn drop(&mut self) {
            let lock_was_available = self
                .core
                .upgrade()
                .is_some_and(|core| core.state.try_lock().is_ok());
            self.lock_was_available_on_drop.store(lock_was_available, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_evicted_error_is_dropped_after_state_unlock() {
        let resource_id = ResourceId::new(ManagerId::from_raw(1), 2, 3);
        let core = Arc::new(RecoveryCore::new(vec![resource_id], Arc::new(()), Arc::new(NoopAction)));
        let lock_was_available_on_drop = Arc::new(AtomicBool::new(false));
        {
            let mut state = super::lock(&core.state);
            state.append_failure(CleanupFailure {
                resource_id,
                stage: CleanupStage::Drop,
                summary: DiagnosticSummary::new("eviction probe"),
                source: Some(Arc::new(LockCheckingError {
                    core: Arc::downgrade(&core),
                    lock_was_available_on_drop: Arc::clone(&lock_was_available_on_drop),
                })),
            });
            for sequence in 1..64 {
                state.append_failure(failure(resource_id, sequence));
            }
        }
        let attempt = core.try_retry().expect("retry should be admitted");
        attempt.finish(Err(failure(resource_id, 65)));
        drop(attempt);
        assert!(
            lock_was_available_on_drop.load(Ordering::SeqCst),
            "evicted source destructor should be able to acquire the recovery state lock"
        );
    }
}

impl Drop for RecoveryCore {
    /// Retains unresolved cleanup ownership instead of dropping the resource.
    ///
    /// A resolved core has already confirmed cleanup, so its payload can be
    /// dropped normally. For unresolved state, forgetting the payload and
    /// action prevents an unconfirmed resource from being released implicitly.
    ///
    /// # Parameters
    ///
    /// * `self` — core whose retained recovery state is being destroyed.
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != RecoveryPhase::Resolved {
            if let Some(payload) = state.payload.take() {
                std::mem::forget(payload);
            }
            if let Some(action) = state.action.take() {
                std::mem::forget(action);
            }
            if let Some(future) = state.unconfirmed_future.take() {
                std::mem::forget(future);
            }
            if let Some(panic) = state.panic.take() {
                std::mem::forget(panic);
            }
        }
    }
}

impl RecoveryCore {
    /// Creates a pending recovery group with a retry action.
    ///
    /// # Parameters
    ///
    /// * `resources` — resource identities governed by this recovery.
    /// * `payload` — type-erased owner capsule retained across attempts.
    /// * `action` — cleanup strategy used to retry the retained owner.
    ///
    /// # Returns
    ///
    /// A core in the pending phase with no retry attempt in flight.
    pub(crate) fn new(resources: Vec<ResourceId>, payload: RecoveryPayload, action: Arc<dyn RecoveryAction>) -> Self {
        Self {
            id: RecoveryId::new_unique(),
            resources: resources.into(),
            state: Mutex::new(State {
                phase: RecoveryPhase::Pending,
                attempts: 0,
                generation: 0,
                payload: Some(payload),
                action: Some(action),
                failures: Vec::new(),
                failures_total: 0,
                failures_truncated: false,
                retry_disabled: false,
                panic: None,
                unconfirmed_future: None,
            }),
            terminal: Mutex::new(None),
            executor: Mutex::new(None),
            retry_route: Mutex::new(None),
        }
    }

    /// Creates an unconfirmed recovery group without an automatic retry action.
    ///
    /// This retains the owner payload for transfer or explicit observation but
    /// cannot execute cleanup until a recovery action is available.
    ///
    /// # Parameters
    ///
    /// * `resources` — resource identities governed by this recovery.
    /// * `payload` — type-erased owner capsule that must remain retained.
    ///
    /// # Returns
    ///
    /// A core in the unconfirmed phase.
    pub(crate) fn new_unconfirmed(resources: Vec<ResourceId>, payload: RecoveryPayload) -> Self {
        Self {
            id: RecoveryId::new_unique(),
            resources: resources.into(),
            state: Mutex::new(State {
                phase: RecoveryPhase::Unconfirmed,
                attempts: 0,
                generation: 0,
                payload: Some(payload),
                action: None,
                failures: Vec::new(),
                failures_total: 0,
                failures_truncated: false,
                retry_disabled: true,
                panic: None,
                unconfirmed_future: None,
            }),
            terminal: Mutex::new(None),
            executor: Mutex::new(None),
            retry_route: Mutex::new(None),
        }
    }

    /// Copies the current recovery phase and failure history into a snapshot.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery core whose state is observed.
    ///
    /// # Returns
    ///
    /// A point-in-time snapshot independent of later state changes.
    pub(crate) fn snapshot(&self) -> RecoverySnapshot {
        let state = lock(&self.state);
        RecoverySnapshot {
            id: self.id,
            phase: state.phase,
            resources: Arc::clone(&self.resources),
            attempts: state.attempts,
            last_failures: state.failures.clone().into(),
            failures_total: state.failures_total,
            failures_truncated: state.failures_truncated,
        }
    }

    /// Admits a retry or routes the request to the store that owns this group.
    ///
    /// At most one retry is active. A retry takes shared references to the
    /// owner payload and action; the `RecoveryRun` keeps them alive until its
    /// outcome is published.
    ///
    /// # Parameters
    ///
    /// * `self` — shared recovery group on which to admit a retry.
    ///
    /// # Returns
    ///
    /// An observer for the admitted retry.
    ///
    /// # Errors
    ///
    /// Returns an error if a retry is running, the group is resolved, required
    /// execution capacity is unavailable, or no retry action is available.
    pub(crate) fn try_retry(self: &Arc<Self>) -> Result<RecoveryAttempt, crate::v02::report::RetryError> {
        let route = {
            let state = lock(&self.state);
            if state.phase == RecoveryPhase::Transferred {
                lock(&self.retry_route).clone()
            } else {
                None
            }
        };
        if let Some(route) = route {
            return route(self.id);
        }
        let executor = lock(&self.executor).clone();
        let permit = match executor.as_ref() {
            Some(executor) => Some(
                executor
                    .reserve_async()
                    .map_err(|_| crate::v02::report::RetryError::NoCapacity)?,
            ),
            None => None,
        };
        let (generation, payload, action) = {
            let mut state = lock(&self.state);
            match state.phase {
                RecoveryPhase::Running => {
                    return Err(crate::v02::report::RetryError::AlreadyRunning);
                }
                RecoveryPhase::Resolved | RecoveryPhase::Transferred => {
                    return Err(crate::v02::report::RetryError::AlreadyResolved);
                }
                RecoveryPhase::Pending | RecoveryPhase::Unconfirmed => {
                    if state.retry_disabled {
                        return Err(crate::v02::report::RetryError::ExecutorUnavailable);
                    }
                }
            }
            let payload = state
                .payload
                .as_ref()
                .cloned()
                .ok_or(crate::v02::report::RetryError::AlreadyResolved)?;
            let action = state
                .action
                .as_ref()
                .cloned()
                .ok_or(crate::v02::report::RetryError::ExecutorUnavailable)?;
            state.phase = RecoveryPhase::Running;
            state.attempts += 1;
            state.generation = state.generation.wrapping_add(1);
            (state.generation, payload, action)
        };
        Ok(RecoveryAttempt {
            core: Arc::clone(self),
            run: Arc::new(Mutex::new(Some(RecoveryRun {
                core: Arc::clone(self),
                generation,
                payload: Some(payload),
                action,
                completed: false,
                permit,
            }))),
            observation: Arc::new(AttemptObservation::default()),
        })
    }

    /// Marks a panicked attempt unconfirmed while retaining owner and action.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery core whose active retry panicked.
    /// * `generation` — active retry token used to reject stale completion.
    /// * `panic` — panic payload retained for diagnosis.
    /// * `future` — panicked action future retained without dropping it.
    pub(in crate::v02::resource::recovery) fn mark_unconfirmed(
        &self,
        generation: u64,
        panic: Box<dyn std::any::Any + Send + 'static>,
        future: Option<crate::v02::resource::recovery::RecoveryFuture>,
    ) {
        let summary = panic
            .downcast_ref::<&str>()
            .map(|summary| (*summary).to_owned())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "recovery action panicked; cleanup outcome is unconfirmed".to_owned());
        self.mark_unconfirmed_with(generation, Some(panic), future, &summary);
    }

    /// Marks a cancelled attempt unconfirmed without inventing a panic cause.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery core whose direct retry was cancelled.
    /// * `generation` — active retry token used to reject stale completion.
    /// * `future` — unfinished cleanup future retained by the core.
    pub(in crate::v02::resource::recovery) fn mark_cancelled(
        &self,
        generation: u64,
        future: Option<crate::v02::resource::recovery::RecoveryFuture>,
    ) {
        self.mark_unconfirmed_with(
            generation,
            None,
            future,
            "recovery attempt execution cancelled; cleanup unconfirmed",
        );
    }

    /// Shared state transition for panic and cancellation outcomes.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery core whose retry outcome is being recorded.
    /// * `generation` — attempt token used to reject stale outcomes.
    /// * `panic` — optional panic payload retained when the action panicked.
    /// * `future` — unfinished action future retained when cleanup is unknown.
    /// * `summary` — bounded diagnostic applied to the affected resources.
    fn mark_unconfirmed_with(
        &self,
        generation: u64,
        panic: Option<Box<dyn std::any::Any + Send + 'static>>,
        future: Option<crate::v02::resource::recovery::RecoveryFuture>,
        summary: &str,
    ) {
        let failures = self
            .resources
            .iter()
            .copied()
            .map(|resource_id| CleanupFailure {
                resource_id,
                stage: CleanupStage::AsyncRelease,
                summary: DiagnosticSummary::new(summary),
                source: None,
            })
            .collect::<Vec<_>>();
        let mut retained_future = future;
        let mut evicted = Vec::new();
        let mut state = lock(&self.state);
        if state.phase == RecoveryPhase::Running && state.generation == generation {
            state.phase = RecoveryPhase::Unconfirmed;
            state.retry_disabled = true;
            state.panic = panic;
            state.unconfirmed_future = retained_future.take();
            for failure in failures {
                if let Some(failure) = state.append_failure(failure) {
                    evicted.push(failure);
                }
            }
        } else {
            // A stale future can still own a resource whose cleanup result is
            // unknown. Preserve it even when this generation no longer owns
            // the visible recovery state.
            if let Some(future) = retained_future {
                std::mem::forget(future);
            }
        }
        drop(state);
        drop(evicted);
    }

    /// Marks a pending or unconfirmed recovery group as transferred.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery group whose ownership is moving to a store.
    ///
    /// # Returns
    ///
    /// `true` when the phase changed to transferred; `false` if the group is
    /// already running, resolved, or transferred.
    pub(crate) fn mark_transferred(&self) -> bool {
        let mut state = lock(&self.state);
        if state.phase != RecoveryPhase::Pending && state.phase != RecoveryPhase::Unconfirmed {
            return false;
        }
        state.phase = RecoveryPhase::Transferred;
        drop(state);
        true
    }

    /// Registers the callback that observes store membership changes.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery group associated with the store entry.
    /// * `callback` — listener receiving `true` when tracked and `false` when
    ///   removed.
    pub(crate) fn set_terminal_callback(&self, callback: Arc<dyn Fn(bool) + Send + Sync + 'static>) {
        *lock(&self.terminal) = Some(callback);
    }

    /// Notifies the registered observer about the current store membership.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery group whose membership changed.
    /// * `present` — whether the group remains in its recovery store.
    pub(in crate::v02::resource::recovery) fn notify_terminal(&self, present: bool) {
        let callback = lock(&self.terminal).clone();
        if let Some(callback) = callback {
            callback(present);
        }
    }

    /// Restores a transferred group to its source after a failed store handoff.
    ///
    /// Pending and unconfirmed phases are already source-owned and remain
    /// unchanged.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery group whose previous ownership is being restored.
    ///
    /// # Returns
    ///
    /// `Ok(())` after restoring or confirming source ownership.
    ///
    /// # Errors
    ///
    /// Returns `AlreadyRunning` or `AlreadyResolved` if the group cannot be
    /// restored in its current phase.
    pub(in crate::v02::resource::recovery) fn restore_from_store(&self) -> Result<(), crate::v02::report::RetryError> {
        let mut state = lock(&self.state);
        match state.phase {
            RecoveryPhase::Transferred => {
                state.phase = RecoveryPhase::Pending;
                Ok(())
            }
            RecoveryPhase::Pending | RecoveryPhase::Unconfirmed => Ok(()),
            RecoveryPhase::Running => Err(crate::v02::report::RetryError::AlreadyRunning),
            RecoveryPhase::Resolved => Err(crate::v02::report::RetryError::AlreadyResolved),
        }
    }

    /// Returns the stable identity shared by handles and recovery reports.
    ///
    /// # Parameters
    ///
    /// * `self` — core whose identity is requested.
    ///
    /// # Returns
    ///
    /// This recovery group's unique identifier.
    pub(crate) fn id(&self) -> RecoveryId {
        self.id
    }

    /// Counts the resources governed by this recovery group.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery core whose resource identities are counted.
    ///
    /// # Returns
    ///
    /// The number of resource IDs retained in the group.
    pub(in crate::v02::resource::recovery) fn resource_count(&self) -> usize {
        self.resources.len()
    }
}

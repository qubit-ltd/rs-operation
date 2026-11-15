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
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("recovery action panicked; cleanup outcome is unconfirmed");
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
        let mut state = lock(&self.state);
        if state.phase == RecoveryPhase::Running && state.generation == generation {
            state.phase = RecoveryPhase::Unconfirmed;
            state.retry_disabled = true;
            state.panic = Some(panic);
            state.unconfirmed_future = future;
            state.failures.extend(failures);
        }
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

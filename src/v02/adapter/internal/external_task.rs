// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::mem::take;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use super::AdapterError;
use super::AdapterErrorKind;
use super::ExternalTaskControl;
use super::LOST_INPUTS;
use super::TaskState;
use super::lock;
use super::poll_fact;
use super::wake_all;
use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ChildRelease;
use crate::v02::report::ExecutionPhase;
use crate::v02::report::OperationId;
use crate::v02::report::SettlementReport;
use crate::v02::resource::ResourceRef;

/// External work together with the resource reference that keeps its input
/// alive.
///
/// # Type Parameters
///
/// * `T` is the retained input resource type. It must be `Send + Sync +
///   'static` so external work can share it across threads while the task keeps
///   the resource alive.
pub(crate) struct ExternalTask<T: Send + Sync + 'static> {
    /// Shared task facts and the resource reference retained for external work.
    state: Arc<Mutex<TaskState<T>>>,
}
impl<T: Send + Sync + 'static> ExternalTask<T> {
    /// Creates shared task state and a control port for an external executor.
    ///
    /// # Parameters
    ///
    /// * `operation_id` identifies the operation represented by this task.
    /// * `input` retains the input resource while external work may use it.
    ///
    /// # Returns
    ///
    /// A task handle and control port that share the retained input state.
    pub(crate) fn new(operation_id: OperationId, input: ResourceRef<T>) -> (Self, Arc<ExternalTaskControl<T>>) {
        let state = Arc::new(Mutex::new(TaskState {
            operation_id,
            execution: ExecutionPhase::Queued,
            stop: None,
            outcome: None,
            release: None,
            settlement: None,
            input: Some(input),
            runtime_lost: false,
            outcome_waiters: Vec::new(),
            release_waiters: Vec::new(),
            settlement_waiters: Vec::new(),
        }));
        (
            Self {
                state: Arc::clone(&state),
            },
            Arc::new(ExternalTaskControl { state }),
        )
    }

    /// Reports whether runtime loss still lacks evidence that input use ended.
    ///
    /// # Parameters
    ///
    /// * `self` is the task whose release state is checked.
    ///
    /// # Returns
    ///
    /// `true` if runtime was lost and input release remains unconfirmed.
    #[must_use]
    pub(crate) fn is_unconfirmed(&self) -> bool {
        let state = lock(&self.state);
        state.runtime_lost && state.release.is_none()
    }

    /// Polls the outcome and registers this task's waker while pending.
    ///
    /// # Parameters
    ///
    /// * `self` is the task whose outcome is polled.
    /// * `cx` supplies the waker to register when the outcome is not ready.
    ///
    /// # Returns
    ///
    /// `Ready` with the published outcome, or `Pending` while it is
    /// unavailable.
    pub(crate) fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.outcome_waiters,
            |state| state.outcome.clone(),
        )
    }

    /// Polls input release and registers this task's waker while pending.
    ///
    /// # Parameters
    ///
    /// * `self` is the task whose input release is polled.
    /// * `cx` supplies the waker to register when release is not confirmed.
    ///
    /// # Returns
    ///
    /// `Ready` with release evidence, or `Pending` until release is confirmed.
    pub(crate) fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.release_waiters,
            |state| state.release.clone(),
        )
    }

    /// Publishes the first outcome and marks execution as finished.
    ///
    /// # Parameters
    ///
    /// * `self` is the external task receiving the outcome.
    /// * `outcome` is the terminal outcome for this operation.
    ///
    /// # Panics
    ///
    /// Panics if `outcome` belongs to another operation.
    pub(crate) fn publish_outcome(&self, outcome: ChildOutcomeSummary) {
        let waiters = {
            let mut state = lock(&self.state);
            assert_eq!(outcome.operation_id, state.operation_id);
            if state.outcome.is_some() {
                return;
            }
            state.execution = ExecutionPhase::Finished;
            state.outcome = Some(outcome);
            take(&mut state.outcome_waiters)
        };
        wake_all(waiters);
    }

    /// Confirms input release and drops the retained reference outside the
    /// lock.
    ///
    /// # Parameters
    ///
    /// * `self` is the task holding the retained input reference.
    /// * `release` is the child's evidence that use of the input has ended.
    ///
    /// # Returns
    ///
    /// `Ok(())` if release evidence was recorded.
    ///
    /// # Errors
    ///
    /// Returns the release evidence if it belongs to another operation or
    /// release was already confirmed.
    pub(crate) fn confirm_input_release(&self, release: ChildRelease) -> Result<(), AdapterError<ChildRelease>> {
        let (input, waiters) = {
            let mut state = lock(&self.state);
            if release.operation_id != state.operation_id || state.release.is_some() {
                return Err(AdapterError {
                    reason: AdapterErrorKind::AlreadyCommitted,
                    payload: release,
                });
            }
            state.release = Some(release);
            (state.input.take(), take(&mut state.release_waiters))
        };
        drop(input);
        wake_all(waiters);
        Ok(())
    }

    /// Publishes the final settlement report once for this operation.
    ///
    /// # Parameters
    ///
    /// * `self` is the task receiving the final settlement.
    /// * `settlement` is the final resource settlement for this operation.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the settlement was recorded.
    ///
    /// # Errors
    ///
    /// Returns the settlement if it belongs to another operation or one was
    /// already published.
    pub(crate) fn publish_settlement(
        &self,
        settlement: SettlementReport,
    ) -> Result<(), AdapterError<Box<SettlementReport>>> {
        let waiters = {
            let mut state = lock(&self.state);
            if settlement.operation_id != state.operation_id || state.settlement.is_some() {
                return Err(AdapterError {
                    reason: AdapterErrorKind::AlreadyCommitted,
                    payload: Box::new(settlement),
                });
            }
            state.settlement = Some(Arc::new(settlement));
            take(&mut state.settlement_waiters)
        };
        wake_all(waiters);
        Ok(())
    }

    /// Records runtime loss while preserving input until release is confirmed.
    ///
    /// # Parameters
    ///
    /// * `self` is the task whose runtime was lost.
    pub(crate) fn mark_runtime_lost(&self) {
        let waiters = {
            let mut state = lock(&self.state);
            state.runtime_lost = true;
            take(&mut state.settlement_waiters)
        };
        wake_all(waiters);
    }
}
impl<T: Send + Sync + 'static> Clone for ExternalTask<T> {
    /// Shares the same task state and retained input reference.
    ///
    /// # Parameters
    ///
    /// * `self` is the task handle to clone.
    ///
    /// # Returns
    ///
    /// Another handle to the same task state.
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}
/// Releases a retained lost-runtime input after independent release evidence
/// arrives.
///
/// The reference is removed under the registry lock and dropped after unlocking
/// so normal resource cleanup can continue without running user destructors in
/// the lock.
///
/// # Parameters
///
/// * `operation_id` identifies the operation whose retained input may be
///   released.
///
/// # Returns
///
/// `true` if a retained input was found and released; otherwise `false`.
pub(crate) fn recover_lost_input(operation_id: OperationId) -> bool {
    let input = LOST_INPUTS.get().and_then(|inputs| lock(inputs).remove(&operation_id));
    let found = input.is_some();
    drop(input);
    found
}

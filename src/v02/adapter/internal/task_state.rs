// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::LOST_INPUTS;
use super::lock;
use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ChildRelease;
use crate::v02::report::OperationId;
use crate::v02::report::SettlementReport;
use crate::v02::report::StopReport;
use crate::v02::resource::ResourceRef;

/// Lifecycle facts retained until the task and every observation handle drop.
///
/// # Type Parameters
///
/// * `T` is the input resource type retained while external work may use it.
pub(in crate::v02::adapter) struct TaskState<T: Send + Sync + 'static> {
    /// Identity used to validate outcome, release and settlement reports.
    pub(super) operation_id: OperationId,
    /// Last execution phase observed for the external task.
    pub(super) execution: crate::v02::report::ExecutionPhase,
    /// First accepted stop request, if any.
    pub(super) stop: Option<StopReport>,
    /// Published outcome, independent of final settlement.
    pub(super) outcome: Option<ChildOutcomeSummary>,
    /// Evidence that the task no longer uses its input.
    pub(super) release: Option<ChildRelease>,
    /// Final settlement report, once published.
    pub(super) settlement: Option<Arc<SettlementReport>>,
    /// Resource reference kept alive until release is confirmed.
    pub(super) input: Option<ResourceRef<T>>,
    /// Whether the runtime disappeared before confirming input release.
    pub(super) runtime_lost: bool,
    /// Wakers waiting for the task outcome.
    pub(super) outcome_waiters: Vec<Waker>,
    /// Wakers waiting for input release.
    pub(super) release_waiters: Vec<Waker>,
    /// Wakers waiting for settlement.
    pub(super) settlement_waiters: Vec<Waker>,
}
impl<T: Send + Sync + 'static> Drop for TaskState<T> {
    /// Retains input if the runtime vanished before release was confirmed.
    ///
    /// # Parameters
    ///
    /// * `self` is the task state whose retained input may be registered.
    fn drop(&mut self) {
        if self.runtime_lost
            && let Some(input) = self.input.take()
        {
            lock(LOST_INPUTS.get_or_init(|| Mutex::new(HashMap::new()))).insert(self.operation_id, Box::new(input));
        }
    }
}
/// Polls one external-task fact and registers a waker without missed updates.
///
/// # Type Parameters
///
/// * `T` is the input resource type owned by the task state.
/// * `V` is the cloneable fact returned after publication.
///
/// # Parameters
///
/// * `state` stores the task facts and registered observer wakers.
/// * `cx` provides the waker to register while the fact is pending.
/// * `waiters` selects the waker list associated with the requested fact.
/// * `value` reads the fact when publication has completed.
///
/// # Returns
///
/// `Poll::Ready` with the published fact, or `Poll::Pending` after registering
/// the current task's waker.
pub(in crate::v02::adapter) fn poll_fact<T, V: Clone>(
    state: &Arc<Mutex<TaskState<T>>>,
    cx: &mut Context<'_>,
    waiters: impl for<'a> Fn(&'a mut TaskState<T>) -> &'a mut Vec<Waker>,
    value: impl Fn(&TaskState<T>) -> Option<V>,
) -> Poll<V>
where
    T: Send + Sync + 'static,
{
    if let Some(value) = value(&lock(state)) {
        return Poll::Ready(value);
    }
    let candidate = cx.waker().clone();
    let (result, retired) = {
        let mut state = lock(state);
        if let Some(value) = value(&state) {
            (Poll::Ready(value), None)
        } else {
            let waiters = waiters(&mut state);
            if let Some(index) = waiters.iter().position(|old| old.will_wake(&candidate)) {
                (Poll::Pending, Some(std::mem::replace(&mut waiters[index], candidate)))
            } else {
                waiters.push(candidate);
                (Poll::Pending, None)
            }
        }
    };
    drop(retired);
    result
}

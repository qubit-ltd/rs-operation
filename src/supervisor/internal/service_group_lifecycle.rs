// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shutdown and cleanup transitions for service admission groups.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::Inner;
use super::ServiceRequestEntry;
use super::SharedServiceGroupState;
use super::util::lock;
use super::util::wake_shutdown;
use crate::supervisor::OperationSupervisor;
use crate::supervisor::ShutdownMode;

/// Starts closing resource scopes after a service group has stopped accepting
/// requests.
///
/// Scope joins run as managed supervisor tasks. If a task cannot be scheduled,
/// its resource entry is made eligible for a later retry.
///
/// # Parameters
/// - `state` is the shared state of the group whose resource scopes are
///   closing.
/// - `supervisor` schedules joins and tracks their completion.
pub(in crate::supervisor) fn start_service_resource_cleanup(
    state: SharedServiceGroupState,
    supervisor: OperationSupervisor,
) {
    let ready = {
        let mut guard = lock(&state.0);
        if !guard.closed || !guard.requests.is_empty() {
            return;
        }
        guard
            .resource_scopes
            .iter_mut()
            .filter_map(|(id, (scope, closed, started))| {
                if *started {
                    return None;
                }
                *started = true;
                closed.store(true, Ordering::Release);
                scope.close_body_resources();
                Some((*id, scope.clone()))
            })
            .collect::<Vec<_>>()
    };
    if ready.is_empty() {
        remove_completed_service_group(&supervisor, &state);
    }
    for (id, scope) in ready {
        let task_state = Arc::clone(&state);
        let task_supervisor = supervisor.internal_handle();
        let result = supervisor.spawn_managed(async move {
            let _ = scope.join().await;
            let waiters = {
                let mut guard = lock(&task_state.0);
                guard.resource_scopes.remove(&id);
                std::mem::take(&mut guard.shutdown_waiters)
            };
            task_state.1.notify_all();
            wake_shutdown(waiters);
            notify_shutdown(&task_supervisor);
            remove_completed_service_group(&task_supervisor, &task_state);
            drop(task_supervisor);
        });
        if result.is_err() {
            let mut guard = lock(&state.0);
            if let Some((_, _, started)) = guard.resource_scopes.get_mut(&id) {
                *started = false;
            }
        }
    }
}

/// Wakes tasks waiting for supervisor shutdown state to change.
///
/// Waiters are removed while holding the state lock and woken after the lock is
/// released.
///
/// # Parameters
/// - `supervisor` owns the shutdown state and its blocking notification
///   primitive.
pub(in crate::supervisor) fn notify_shutdown(supervisor: &OperationSupervisor) {
    let waiters = {
        let mut state = lock(&supervisor.inner.state);
        std::mem::take(&mut state.shutdown_waiters)
    };
    supervisor.inner.changed.notify_all();
    wake_shutdown(waiters);
}

/// Retries closed service-group cleanup after supervisor capacity becomes
/// available.
///
/// The temporary handle is internal, so creating it does not change
/// public-handle lifetime accounting.
///
/// # Parameters
/// - `inner` is the shared supervisor state whose closed groups may be retried.
pub(in crate::supervisor) fn retry_service_cleanups_after_capacity_change(inner: &Arc<Inner>) {
    let supervisor = OperationSupervisor {
        inner: Arc::clone(inner),
        public: false,
    };
    retry_closed_service_group_cleanups(&supervisor);
}

/// Closes a service group, applies the requested shutdown mode, and starts
/// cleanup.
///
/// Cancellation requests stop for managed contexts; draining leaves those
/// contexts running while outstanding requests finish.
///
/// # Parameters
/// - `state` is the group being closed.
/// - `mode` selects whether managed contexts receive a stop request.
/// - `supervisor` coordinates cleanup and waiter notifications.
pub(in crate::supervisor) fn begin_service_group_state(
    state: SharedServiceGroupState,
    mode: ShutdownMode,
    supervisor: OperationSupervisor,
) {
    let (contexts, waiters) = {
        let mut guard = lock(&state.0);
        guard.closed = true;
        if mode == ShutdownMode::Cancel {
            guard.mode = ShutdownMode::Cancel;
        }
        let contexts = if guard.mode == ShutdownMode::Cancel {
            guard
                .requests
                .values()
                .filter_map(|entry| match entry {
                    ServiceRequestEntry::Managed(Some(context)) => Some(context.clone()),
                    _ => None,
                })
                .collect()
        } else {
            Vec::new()
        };
        (contexts, std::mem::take(&mut guard.shutdown_waiters))
    };
    for context in contexts {
        context.request_stop(crate::report::StopReason::ParentStopped);
    }
    wake_shutdown(waiters);
    start_service_resource_cleanup(state, supervisor);
}

/// Retries cleanup for every closed service group with no outstanding requests.
///
/// Groups are cloned from the supervisor registry before cleanup is attempted,
/// so cleanup can update the registry without holding its lock during the
/// iteration.
///
/// # Parameters
/// - `supervisor` provides the registry and cleanup scheduler.
pub(in crate::supervisor) fn retry_closed_service_group_cleanups(supervisor: &OperationSupervisor) {
    let groups = lock(&supervisor.inner.service_groups).clone();
    for group in groups {
        let ready = {
            let state = lock(&group.0);
            state.closed && state.requests.is_empty()
        };
        if ready {
            start_service_resource_cleanup(Arc::clone(&group), supervisor.internal_handle());
            remove_completed_service_group(supervisor, &group);
        }
    }
}

/// Removes a service group from the supervisor registry after all cleanup
/// completes.
///
/// A group remains registered until it is closed and has neither requests nor
/// resource scopes outstanding.
///
/// # Parameters
/// - `supervisor` owns the registry from which a completed group is removed.
/// - `group` is the candidate group whose completion state is checked.
pub(in crate::supervisor) fn remove_completed_service_group(
    supervisor: &OperationSupervisor,
    group: &SharedServiceGroupState,
) {
    let complete = {
        let state = lock(&group.0);
        state.closed && state.requests.is_empty() && state.resource_scopes.is_empty()
    };
    if complete {
        lock(&supervisor.inner.service_groups).retain(|candidate| !Arc::ptr_eq(candidate, group));
    }
}

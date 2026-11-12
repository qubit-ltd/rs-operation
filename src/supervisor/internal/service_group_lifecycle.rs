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

use crate::supervisor::OperationSupervisor;

use super::Inner;
use super::SharedServiceGroupState;
use super::ServiceRequestEntry;
use super::util::{lock, wake_shutdown};
use crate::supervisor::ShutdownMode;
use super::ServiceGroupState;

pub(in crate::supervisor) fn start_service_resource_cleanup(state: SharedServiceGroupState, supervisor: OperationSupervisor) {
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

pub(in crate::supervisor) fn notify_shutdown(supervisor: &OperationSupervisor) {
    let waiters = {
        let mut state = lock(&supervisor.inner.state);
        std::mem::take(&mut state.shutdown_waiters)
    };
    supervisor.inner.changed.notify_all();
    wake_shutdown(waiters);
}

pub(in crate::supervisor) fn retry_service_cleanups_after_capacity_change(inner: &Arc<Inner>) {
    let supervisor = OperationSupervisor {
        inner: Arc::clone(inner),
        public: false,
    };
    retry_closed_service_group_cleanups(&supervisor);
}

pub(in crate::supervisor) fn begin_service_group_state(state: SharedServiceGroupState, mode: ShutdownMode, supervisor: OperationSupervisor) {
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

pub(in crate::supervisor) fn remove_completed_service_group(supervisor: &OperationSupervisor, group: &SharedServiceGroupState) {
    let complete = {
        let state = lock(&group.0);
        state.closed && state.requests.is_empty() && state.resource_scopes.is_empty()
    };
    if complete {
        lock(&supervisor.inner.service_groups).retain(|candidate| !Arc::ptr_eq(candidate, group));
    }
}


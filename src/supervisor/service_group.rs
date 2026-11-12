// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded admission group for long-lived service work.

use std::sync::Arc;
use crate::context::ContextError;
use crate::context::Deadline;
use crate::protocol::ClockSource;
use crate::report::AdmissionError;
use crate::report::OperationId;
use crate::report::ScopeLimits;
use crate::report::StartError;

use super::OperationSupervisor;
use super::ServiceOperationHandle;
use super::ServiceRequest;
use super::ServiceResourceScope;
use super::ShutdownMode;
use super::ShutdownObservation;
use super::ShutdownObserver;
use super::ShutdownReport;
use super::internal::SharedServiceGroupState;
use super::internal::ServiceRequestEntry;
use super::internal::service_group_lifecycle::begin_service_group_state;
use super::internal::util::lock;

/// Bounded admission group for long-lived services. It consumes no root slot.
pub struct ServiceGroup {
    pub(super) state: SharedServiceGroupState,
    pub(super) supervisor: OperationSupervisor,
}

impl ServiceGroup {
    /// Create a resource owner scope with a finite quota independent from root
    /// admission.
    pub fn resource_scope(&self, capacity: usize) -> Result<ServiceResourceScope, AdmissionError> {
        if capacity == 0 || capacity > self.supervisor.inner.limits.scope.resources {
            return Err(AdmissionError::ReservationLimit);
        }
        let mut state = lock(&self.state.0);
        if state.closed {
            return Err(AdmissionError::Closed);
        }
        if state.resource_scopes.len() >= state.limit {
            return Err(AdmissionError::Busy);
        }
        let id = state.next_resource_id;
        state.next_resource_id = id.checked_add(1).ok_or(AdmissionError::Busy)?;
        let operation = OperationId::new((1_u128 << 120) | id as u128);
        let scope = crate::scope::OperationScope::new(
            operation,
            ScopeLimits {
                child_operations: 0,
                resources: capacity,
                effect_records: 0,
            },
            crate::scope::ScopePolicy::FailFast,
            self.supervisor.cleanup_scheduler(),
            crate::context::OperationContext::new(crate::context::ContextOptions::default()),
        );
        self.supervisor.track_scope_recovery(&scope);
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        state
            .resource_scopes
            .insert(id, (scope.clone(), Arc::clone(&closed), false));
        Ok(ServiceResourceScope { scope, closed })
    }

    /// Atomically reserve one request slot unless this group is closed or full.
    pub fn try_admit(&self) -> Result<ServiceRequest, AdmissionError> {
        self.reserve_request(false)
    }

    fn reserve_request(&self, managed: bool) -> Result<ServiceRequest, AdmissionError> {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(AdmissionError::Closed);
        }
        if state.requests.len() >= state.limit {
            return Err(AdmissionError::Busy);
        }
        let id = state.next_id;
        state.next_id = id.checked_add(1).ok_or(AdmissionError::Busy)?;
        state.requests.insert(
            id,
            if managed {
                ServiceRequestEntry::Managed(None)
            } else {
                ServiceRequestEntry::Standalone
            },
        );
        Ok(ServiceRequest {
            state: Arc::clone(&self.state),
            id,
            supervisor: self.supervisor.internal_handle(),
            released: false,
        })
    }

    /// Admit and supervise one real root operation under this service group.
    pub fn start<F, Fut, T, E>(
        &self,
        options: crate::scope::ScopeOptions,
        body: F,
    ) -> Result<ServiceOperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(crate::scope::OperationScope) -> Fut + Send + 'static,
        Fut: core::future::Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let request = match self.reserve_request(true) {
            Ok(request) => request,
            Err(reason) => return Err(StartError::new(reason, body)),
        };
        let handle = match self.supervisor.start(options, body) {
            Ok(handle) => handle,
            Err(error) => {
                drop(request);
                return Err(error);
            }
        };
        let context = handle.cancellation_context();
        {
            let mut state = lock(&self.state.0);
            if let Some(ServiceRequestEntry::Managed(slot)) = state.requests.get_mut(&request.id) {
                *slot = Some(context.clone());
            }
            if state.mode == ShutdownMode::Cancel {
                context.request_stop(crate::report::StopReason::ParentStopped);
            }
        }
        handle.hold_until_released(Box::new(request));
        Ok(ServiceOperationHandle { inner: handle })
    }

    /// Stop group admission and wait up to `timeout` for request leases to
    /// return.
    pub fn shutdown_blocking(&self, mode: ShutdownMode, timeout: std::time::Duration) -> ShutdownReport {
        self.begin_shutdown(mode);
        let (mutex, changed) = &*self.state;
        let state = mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = changed
            .wait_timeout_while(state, timeout, |state| {
                !state.requests.is_empty() || !state.resource_scopes.is_empty()
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ShutdownReport {
            pending_requests: state.requests.len(),
            pending_recovery: self.supervisor.pending_recovery(),
            pending_resources: state.resource_scopes.len(),
        }
    }

    /// Current number of live request leases.
    pub fn active_requests(&self) -> usize {
        self.state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requests
            .len()
    }

    /// Stop this group and observe its shutdown asynchronously.
    pub async fn shutdown_async(
        &self,
        mode: ShutdownMode,
        deadline: Deadline,
        clock: Arc<dyn ClockSource>,
    ) -> Result<ShutdownObservation, ContextError> {
        self.begin_shutdown(mode);
        ShutdownObserver(ShutdownSource::Service(
            Arc::clone(&self.state),
            self.supervisor.internal_handle(),
        ))
        .wait_async(deadline, clock)
        .await
    }

    fn begin_shutdown(&self, mode: ShutdownMode) {
        begin_service_group_state(Arc::clone(&self.state), mode, self.supervisor.internal_handle());
    }
}

impl Drop for ServiceGroup {
    fn drop(&mut self) {
        begin_service_group_state(
            Arc::clone(&self.state),
            ShutdownMode::Cancel,
            self.supervisor.internal_handle(),
        );
    }
}


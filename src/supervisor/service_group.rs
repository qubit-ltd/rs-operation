// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded admission group for long-lived service work.

use std::sync::Arc;

use super::OperationSupervisor;
use super::ServiceOperationHandle;
use super::ServiceRequest;
use super::ServiceResourceScope;
use super::ShutdownMode;
use super::ShutdownObservation;
use super::ShutdownObserver;
use super::ShutdownReport;
use super::internal::ServiceRequestEntry;
use super::internal::SharedServiceGroupState;
use super::internal::ShutdownSource;
use super::internal::service_group_lifecycle::begin_service_group_state;
use super::internal::util::lock;
use crate::context::ContextError;
use crate::context::Deadline;
use crate::protocol::ClockSource;
use crate::report::AdmissionError;
use crate::report::OperationId;
use crate::report::ScopeLimits;
use crate::report::StartError;

/// Bounded admission group for long-lived services. It consumes no root slot.
///
/// Create a group through [`OperationSupervisor::service_group`]. Request
/// leases occupy its finite capacity until released, and shutdown stops new
/// admission before waiting for existing leases and resource scopes.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::SupervisorLimits;
/// use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};
/// use std::time::Duration;
///
/// let supervisor = OperationSupervisor::new(SupervisorLimits::default());
/// let group = supervisor.service_group(2)?;
/// let request = group.try_admit()?;
/// drop(request);
/// let report = group.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
/// assert_eq!(report.pending_requests, 0);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ServiceGroup {
    /// Shared request and resource-scope admission state for this group.
    pub(super) state: SharedServiceGroupState,
    /// Internal supervisor handle used to start and settle managed work.
    pub(super) supervisor: OperationSupervisor,
}

impl ServiceGroup {
    /// Create a resource owner scope with a finite quota independent from root
    /// admission.
    ///
    /// # Parameters
    ///
    /// - `capacity`: Maximum number of resources the returned scope may own.
    ///
    /// # Returns
    ///
    /// Returns the resource scope when capacity is within the supervisor's
    /// configured limit.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionError::ReservationLimit`] for an invalid capacity,
    /// [`AdmissionError::Closed`] after shutdown starts, or
    /// [`AdmissionError::Busy`] when the group has no free scope slot or its ID
    /// space is exhausted.
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
    ///
    /// # Returns
    ///
    /// Returns a lease that occupies one request slot until it is released.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionError::Closed`] after shutdown starts or
    /// [`AdmissionError::Busy`] when the group is full or its request ID space
    /// is exhausted.
    pub fn try_admit(&self) -> Result<ServiceRequest, AdmissionError> {
        self.reserve_request(false)
    }

    /// Reserve one request entry, marking whether a managed operation owns it.
    ///
    /// The state lock makes the closed check, capacity check, and insertion
    /// atomic with respect to shutdown and competing admissions.
    ///
    /// # Parameters
    ///
    /// - `managed`: Whether the admitted request is owned by a managed
    ///   operation.
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
    ///
    /// # Type Parameters
    ///
    /// - `F`: A one-shot operation body that can move between worker threads.
    /// - `Fut`: The body's sendable, owned future.
    /// - `T`: The successful operation result, which must be sendable and
    ///   `'static`.
    /// - `E`: The operation error, which must be sendable and `'static`.
    ///
    /// # Parameters
    ///
    /// - `options`: Scope limits and policy for the operation.
    /// - `body`: The closure that receives the operation scope and produces its
    ///   result future.
    ///
    /// # Returns
    ///
    /// Returns a handle for observing the operation and its settlement.
    ///
    /// # Errors
    ///
    /// Returns the original body with a reason if group admission fails, or
    /// the supervisor's start error if operation startup fails.
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

    /// Current number of live request leases.
    ///
    /// # Returns
    ///
    /// Returns the number of request leases currently occupying group capacity.
    #[must_use]
    pub fn active_requests(&self) -> usize {
        self.state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requests
            .len()
    }

    /// Stop group admission and wait up to `timeout` for request leases to
    /// return.
    ///
    /// # Parameters
    ///
    /// - `mode`: Whether admitted work may drain or should receive a stop
    ///   request.
    /// - `timeout`: Maximum duration to wait for request leases and resource
    ///   scopes to settle.
    ///
    /// # Returns
    ///
    /// Returns counts of requests, recovery entries, and resources still
    /// pending when the wait completes or times out.
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

    /// Stop this group and observe its shutdown asynchronously.
    ///
    /// # Parameters
    ///
    /// - `mode`: Whether admitted work may drain or should receive a stop
    ///   request.
    /// - `deadline`: The latest time at which shutdown observation should
    ///   complete.
    /// - `clock`: The clock used to evaluate the deadline.
    ///
    /// # Returns
    ///
    /// Returns an observation of the shutdown state once the group settles.
    ///
    /// # Errors
    ///
    /// Returns a context error if deadline observation fails.
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

    /// Close admission and start the selected shutdown policy without waiting.
    ///
    /// # Parameters
    ///
    /// - `mode`: Whether admitted work may drain or should receive a stop
    ///   request.
    fn begin_shutdown(&self, mode: ShutdownMode) {
        begin_service_group_state(Arc::clone(&self.state), mode, self.supervisor.internal_handle());
    }
}

impl Drop for ServiceGroup {
    /// Requests cancellation and closes admission when the final group owner is
    /// dropped; it does not block for outstanding leases to settle.
    fn drop(&mut self) {
        begin_service_group_state(
            Arc::clone(&self.state),
            ShutdownMode::Cancel,
            self.supervisor.internal_handle(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::OperationSupervisor;
    use super::ShutdownMode;
    use crate::report::AdmissionError;
    use crate::report::SupervisorLimits;

    #[test]
    fn test_drain_closes_admission_and_waits_for_existing_request_lease() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let group = supervisor.service_group(1).unwrap();
        let request = group.try_admit().unwrap();

        let pending = group.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::ZERO);
        assert_eq!(pending.pending_requests, 1);
        assert!(matches!(group.try_admit(), Err(AdmissionError::Closed)));

        drop(request);
        let settled = group.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::ZERO);
        assert_eq!(settled.pending_requests, 0);
        assert_eq!(group.active_requests(), 0);
    }

    #[test]
    fn test_dropping_last_group_owner_closes_admission_but_lease_can_release() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let group = supervisor.service_group(1).unwrap();
        let request = group.try_admit().unwrap();
        drop(group);

        drop(request);
        let observation = supervisor.shutdown_observer().wait_blocking(std::time::Duration::ZERO);
        assert_eq!(observation.report.pending_requests, 0);
        assert_eq!(observation.report.pending_resources, 0);
    }
}

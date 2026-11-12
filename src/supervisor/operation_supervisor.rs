// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded root operation admission and supervisor lifecycle.

use core::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

use crate::context::ContextError;
use crate::context::Deadline;
use crate::context::OperationContext;
use crate::driver::PortableDriver;
use crate::driver::PortableDriverConfig;
use crate::driver::PortableDriverError;
use crate::driver::TaskHandle;
use crate::operation::OperationHandle;
use crate::protocol::ClockSource;
use crate::report::AdmissionError;
use crate::report::StartError;
use crate::report::SupervisorLimits;
use crate::resource::CleanupResponsibility;
use crate::resource::CleanupScheduler;
use crate::resource::RecoveryRegistry;

use super::AdmissionWait;
use super::CleanupPermit;
use super::RootAdmission;
use super::ServiceGroup;
use super::ShutdownMode;
use super::ShutdownObservation;
use super::ShutdownObserver;
use super::ShutdownReport;
use super::admission_wait::context_admission_error;
use super::internal::Inner;
use super::internal::LeaseState;
use super::internal::ServiceGroupState;
use super::internal::ServiceRequestEntry;
use super::internal::SharedServiceGroupState;
use super::internal::State;
use super::internal::service_group_lifecycle::{begin_service_group_state, remove_completed_service_group};
use super::internal::util::{lock, take_waiter_wakers, wake, wake_shutdown};

/// Application-owned bounded root operation admission service.
pub struct OperationSupervisor {
    pub(super) inner: Arc<Inner>,
    public: bool,
}

impl Clone for OperationSupervisor {
    fn clone(&self) -> Self {
        self.inner.public_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
            public: true,
        }
    }
}

impl Drop for OperationSupervisor {
    fn drop(&mut self) {
        if self.public && self.inner.public_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            let handle = Self {
                inner: Arc::clone(&self.inner),
                public: false,
            };
            handle.begin_shutdown(ShutdownMode::Cancel);
        }
    }
}

impl OperationSupervisor {
    pub(crate) fn internal_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            public: false,
        }
    }

    /// Create a service admission group with its own finite request capacity.
    pub fn service_group(&self, capacity: usize) -> Result<ServiceGroup, AdmissionError> {
        if capacity == 0 {
            return Err(AdmissionError::ReservationLimit);
        }
        let state = Arc::new((
            Mutex::new(ServiceGroupState {
                closed: false,
                limit: capacity,
                mode: ShutdownMode::Drain,
                next_id: 1,
                requests: std::collections::BTreeMap::new(),
                shutdown_waiters: Vec::new(),
                next_shutdown_waiter: 1,
                resource_scopes: std::collections::BTreeMap::new(),
                next_resource_id: 1,
            }),
            std::sync::Condvar::new(),
        ));
        lock(&self.inner.service_groups).push(Arc::clone(&state));
        Ok(ServiceGroup {
            state,
            supervisor: self.internal_handle(),
        })
    }
}

impl OperationSupervisor {
    /// Create a supervisor with explicit finite capacities.
    pub fn new(limits: SupervisorLimits) -> Self {
        let cleanup_scheduler = CleanupScheduler::new(limits.cleanup_concurrency);
        let cleanup_responsibility = cleanup_scheduler.responsibility_owner();
        Self {
            inner: Arc::new(Inner {
                limits,
                state: Mutex::new(State {
                    closed: false,
                    active: 0,
                    queued: VecDeque::new(),
                    results: 0,
                    cleanup_active: 0,
                    waiters: Vec::new(),
                    next_waiter: 1,
                    cancel: false,
                    contexts: Vec::new(),
                    shutdown_waiters: Vec::new(),
                    next_shutdown_waiter: 1,
                }),
                driver: Mutex::new(None),
                cleanup_scheduler,
                cleanup_responsibility,
                changed: std::sync::Condvar::new(),
                recovery_registries: Mutex::new(Vec::new()),
                public_handles: AtomicUsize::new(1),
                service_groups: Mutex::new(Vec::new()),
            }),
            public: true,
        }
    }

    pub(crate) fn cleanup_scheduler(&self) -> CleanupScheduler {
        self.inner.cleanup_scheduler.clone()
    }

    pub(crate) fn track_scope_recovery(&self, scope: &crate::scope::OperationScope) {
        let registry = scope.recovery_registry();
        let mut registries = lock(&self.inner.recovery_registries);
        registries.retain(|entry| !entry.is_empty());
        registries.push(registry);
    }

    fn pending_recovery(&self) -> usize {
        lock(&self.inner.recovery_registries)
            .iter()
            .map(RecoveryRegistry::len)
            .sum()
    }

    fn register_recovery_wakers(&self, waker: &Waker) {
        let registries = lock(&self.inner.recovery_registries).clone();
        for registry in registries {
            registry.register_waker_if_nonempty(waker);
        }
    }

    fn service_totals(&self) -> (usize, usize) {
        let groups = { lock(&self.inner.service_groups).clone() };
        groups.into_iter().fold((0, 0), |(requests, resources), group| {
            let state = lock(&group.0);
            (
                requests.saturating_add(
                    state
                        .requests
                        .values()
                        .filter(|entry| matches!(entry, ServiceRequestEntry::Standalone))
                        .count(),
                ),
                resources.saturating_add(state.resource_scopes.len()),
            )
        })
    }

    /// Number of cleanup permits retained by this shared supervisor.
    pub fn cleanup_tasks_active(&self) -> usize {
        self.inner.cleanup_responsibility.active()
    }

    /// Submit a managed future to the supervisor's shared bounded portable
    /// driver. This is crate-private so public operation APIs remain the
    /// only submission path.
    pub(crate) fn spawn_managed<F>(&self, future: F) -> Result<TaskHandle, PortableDriverError>
    where
        F: core::future::Future<Output = ()> + Send + 'static,
    {
        let mut driver = lock(&self.inner.driver);
        if driver.is_none() {
            let Some(task_capacity) = self
                .inner
                .limits
                .active_roots
                .checked_add(self.inner.limits.queued_roots)
            else {
                return Err(PortableDriverError::InvalidConfiguration);
            };
            let task_capacity = task_capacity.max(1);
            let driver_handle = PortableDriver::new(PortableDriverConfig {
                worker_count: self.inner.limits.active_roots.max(1),
                task_capacity,
                polls_per_turn: 16,
            })
            .map_err(|_| PortableDriverError::WorkerSpawn)?;
            *driver = Some(Arc::new(driver_handle));
        }
        driver.as_ref().expect("driver initialized").spawn(future)
    }

    /// Reserve a root execution position and its typed-result slot.
    pub fn try_admit(&self) -> Result<RootAdmission<()>, AdmissionError> {
        self.try_admit_body(()).map_err(|error| error.reason())
    }

    /// Reserve capacity while retaining the unstarted body with the admission.
    /// The body is never invoked by this method.
    pub fn try_admit_body<F>(&self, body: F) -> Result<RootAdmission<F>, StartError<F>> {
        let lease = Arc::new(Mutex::new(LeaseState::default()));
        let mut state = lock(&self.inner.state);
        if state.closed {
            return Err(StartError::new(AdmissionError::Closed, body));
        }
        if state.results >= self.inner.limits.unobserved_results {
            return Err(StartError::new(AdmissionError::Busy, body));
        }
        let queued = state.active >= self.inner.limits.active_roots;
        if queued && state.queued.len() >= self.inner.limits.queued_roots {
            return Err(StartError::new(AdmissionError::Busy, body));
        }
        state.results += 1;
        {
            let mut lease_state = lock(&lease);
            lease_state.result_reserved = true;
            lease_state.queued = queued;
            lease_state.active = !queued;
        }
        if queued {
            state.queued.push_back(Arc::downgrade(&lease));
        } else {
            state.active += 1;
        }
        Ok(RootAdmission {
            inner: Arc::clone(&self.inner),
            lease,
            body: Some(body),
            transferred: false,
        })
    }

    /// Validate inherited cancellation, deadline, and budget before reserving
    /// root capacity.
    pub fn try_admit_with_context<F>(
        &self,
        body: F,
        context: &OperationContext,
    ) -> Result<RootAdmission<F>, StartError<F>> {
        if let Err(error) = context.admission_check() {
            return Err(StartError::new(context_admission_error(context, error), body));
        }
        let admission = self.try_admit_body(body)?;
        let cancel = {
            let mut state = lock(&self.inner.state);
            state.contexts.retain(|(lease, _)| lease.strong_count() > 0);
            state.contexts.push((Arc::downgrade(&admission.lease), context.clone()));
            state.cancel
        };
        if cancel {
            context.request_stop(crate::report::StopReason::ParentStopped);
        }
        Ok(admission)
    }

    /// Create a bounded asynchronous admission waiter.
    pub fn start_wait_body<F>(&self, body: F) -> Result<AdmissionWait<F>, StartError<F>> {
        self.start_wait_body_with_context(body, None)
    }

    /// Create a bounded asynchronous admission waiter that also observes its
    /// context deadline.
    pub fn start_wait_body_with_context<F>(
        &self,
        body: F,
        context: Option<OperationContext>,
    ) -> Result<AdmissionWait<F>, StartError<F>> {
        if let Some(context) = &context
            && let Err(error) = context.admission_check()
        {
            return Err(StartError::new(context_admission_error(context, error), body));
        }
        let mut state = lock(&self.inner.state);
        let error = if state.closed {
            Some(AdmissionError::Closed)
        } else if state.waiters.len() >= self.inner.limits.admission_waiters {
            Some(AdmissionError::Busy)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(StartError::new(error, body));
        }
        let id = state.next_waiter;
        let Some(next_waiter) = id.checked_add(1) else {
            return Err(StartError::new(AdmissionError::Busy, body));
        };
        state.next_waiter = next_waiter;
        state.waiters.push((id, None));
        drop(state);
        let deadline_signal = context.as_ref().and_then(OperationContext::deadline_signal);
        let stop_signal = context.as_ref().map(OperationContext::stop_signal);
        Ok(AdmissionWait {
            supervisor: self.internal_handle(),
            waiter_id: id,
            body: Some(body),
            context,
            deadline_signal,
            stop_signal,
            done: false,
        })
    }

    /// Stop accepting new root operations and wake all registered waiters.
    pub fn close(&self) {
        let waiters = {
            let mut state = lock(&self.inner.state);
            state.closed = true;
            take_waiter_wakers(&mut state)
        };
        wake(waiters);
        self.inner.changed.notify_all();
    }

    /// Stop root admission and wait up to the caller-owned timeout for accepted
    /// work. Drain can only be upgraded to Cancel; every call observes the
    /// shared lifecycle.
    pub fn shutdown_blocking(&self, mode: ShutdownMode, timeout: std::time::Duration) -> ShutdownReport {
        self.begin_shutdown(mode);
        let state = lock(&self.inner.state);
        let (state, _) = self
            .inner
            .changed
            .wait_timeout_while(state, timeout, |state| {
                state.active > 0
                    || !state.queued.is_empty()
                    || self.service_totals().1 > 0
                    || self.service_totals().0 > 0
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ShutdownReport {
            pending_requests: state.active + state.queued.len() + self.service_totals().0,
            pending_recovery: self.pending_recovery(),
            pending_resources: self.service_totals().1,
        }
    }

    /// Begin shutdown and return a reusable observer after `timeout` expires or
    /// work settles.
    pub async fn shutdown_async(
        &self,
        mode: ShutdownMode,
        deadline: Deadline,
        clock: Arc<dyn ClockSource>,
    ) -> Result<ShutdownObservation, ContextError> {
        self.begin_shutdown(mode);
        ShutdownObserver(ShutdownSource::Supervisor(self.internal_handle()))
            .wait_async(deadline, clock)
            .await
    }

    /// Create a reusable observer for an already requested or future shutdown.
    pub fn shutdown_observer(&self) -> ShutdownObserver {
        ShutdownObserver(ShutdownSource::Supervisor(self.internal_handle()))
    }

    fn begin_shutdown(&self, mode: ShutdownMode) {
        let (contexts, waiters, shutdown_waiters) = {
            let mut state = lock(&self.inner.state);
            state.closed = true;
            if mode == ShutdownMode::Cancel {
                state.cancel = true;
            }
            state.contexts.retain(|(lease, _)| lease.strong_count() > 0);
            let contexts = if state.cancel {
                state.contexts.iter().map(|(_, context)| context.clone()).collect()
            } else {
                Vec::new()
            };
            (
                contexts,
                take_waiter_wakers(&mut state),
                std::mem::take(&mut state.shutdown_waiters),
            )
        };
        for context in contexts {
            context.request_stop(crate::report::StopReason::ParentStopped);
        }
        let groups = lock(&self.inner.service_groups).clone();
        for group in groups {
            begin_service_group_state(Arc::clone(&group), mode, self.internal_handle());
            remove_completed_service_group(self, &group);
        }
        wake(waiters);
        wake_shutdown(shutdown_waiters);
        self.inner.changed.notify_all();
    }

    /// Number of root bodies currently holding execution capacity.
    pub fn active_roots(&self) -> usize {
        lock(&self.inner.state).active
    }

    /// Number of accepted roots waiting for execution capacity.
    pub fn queued_roots(&self) -> usize {
        lock(&self.inner.state).queued.len()
    }

    /// Number of reserved typed-result slots, including completed results.
    pub fn reserved_results(&self) -> usize {
        lock(&self.inner.state).results
    }

    /// Reserve a cleanup slot that does not consume business admission
    /// capacity.
    pub fn try_reserve_cleanup(&self) -> Result<CleanupPermit, AdmissionError> {
        let mut state = lock(&self.inner.state);
        if state.cleanup_active >= self.inner.limits.cleanup_concurrency {
            return Err(AdmissionError::Busy);
        }
        state.cleanup_active += 1;
        Ok(CleanupPermit {
            inner: Arc::clone(&self.inner),
            released: false,
        })
    }

    /// Number of active cleanup reservations.
    pub fn cleanup_active(&self) -> usize {
        lock(&self.inner.state).cleanup_active
    }
}


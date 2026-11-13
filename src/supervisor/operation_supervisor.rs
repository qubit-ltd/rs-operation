// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded root operation admission and supervisor lifecycle.

use core::future::Future;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::mem::take;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

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
use super::internal::ShutdownSource;
use super::internal::State;
use super::internal::service_group_lifecycle::begin_service_group_state;
use super::internal::service_group_lifecycle::remove_completed_service_group;
use super::internal::util::lock;
use super::internal::util::take_waiter_wakers;
use super::internal::util::wake;
use super::internal::util::wake_shutdown;
use crate::context::ContextError;
use crate::context::Deadline;
use crate::context::OperationContext;
use crate::driver::BlockingExecutor;
use crate::driver::ExecutorError;
use crate::driver::PortableDriver;
use crate::driver::PortableDriverConfig;
use crate::driver::PortableDriverError;
use crate::driver::TaskHandle;
use crate::driver::is_portable_worker_thread;
use crate::driver::is_thread_executor_worker;
use crate::operation::OperationHandle;
use crate::operation::block_on_start_wait;
use crate::operation::internal::BlockingBodyResult;
use crate::operation::internal::BlockingCompletion;
use crate::operation::internal::Cancellable;
use crate::operation::internal::CatchUnwind;
use crate::operation::internal::Shared;
use crate::operation::internal::State as OperationState;
use crate::operation::internal::cleanup_scope_tree;
use crate::operation::lock as operation_lock;
use crate::operation::publish_released;
use crate::operation::publish_result;
use crate::protocol::ClockSource;
use crate::report::AdmissionError;
use crate::report::ChildFailureSummary;
use crate::report::DiagnosticSummary;
use crate::report::OperationId;
use crate::report::OperationOutcome;
use crate::report::PanicSummary;
use crate::report::ResourcePhase;
use crate::report::SettlementReport;
use crate::report::StartError;
use crate::report::StopReason;
use crate::report::StopReport;
use crate::report::SupervisorLimits;
use crate::resource::CleanupScheduler;
use crate::resource::RecoveryRegistry;
use crate::scope::JoinFacts;
use crate::scope::OperationScope;
use crate::scope::ScopeOptions;
use crate::scope::ScopePolicy;
use crate::scope::poll_scope_business;
use crate::scope::poll_scope_releases;
use crate::supervisor::ResultLease;
use crate::supervisor::TaskLease;

/// Application-owned bounded root operation admission service.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::SupervisorLimits;
/// use qubit_operation::supervisor::OperationSupervisor;
///
/// let supervisor = OperationSupervisor::new(SupervisorLimits::default());
/// let group = supervisor.service_group(4).expect("nonzero capacity");
/// assert_eq!(supervisor.active_roots(), 0);
/// drop(group);
/// ```
#[must_use]
pub struct OperationSupervisor {
    /// Shared admission, task, and cleanup lifecycle state.
    pub(super) inner: Arc<Inner>,
    /// Whether dropping this handle contributes to public-handle shutdown.
    pub(super) public: bool,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

impl Clone for OperationSupervisor {
    /// Clones a handle participating in the same supervisor lifecycle.
    ///
    /// # Returns
    ///
    /// A new public handle sharing the same supervisor state.
    #[inline]
    fn clone(&self) -> Self {
        self.inner.public_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
            public: true,
        }
    }
}

impl Drop for OperationSupervisor {
    /// Requests cancellation when the final public handle is dropped.
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
    /// Creates a supervisor with explicit finite capacities.
    ///
    /// # Parameters
    ///
    /// * `limits` - Queue, worker, cleanup, and result capacities shared by
    ///   this supervisor.
    ///
    /// # Returns
    ///
    /// A new supervisor with empty admission state and the supplied capacity
    /// limits.
    ///
    /// # Examples
    ///
    /// ```
    /// use qubit_operation::report::SupervisorLimits;
    /// use qubit_operation::supervisor::OperationSupervisor;
    ///
    /// let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    /// assert_eq!(supervisor.active_roots(), 0);
    /// ```
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
                changed: Condvar::new(),
                recovery_registries: Mutex::new(Vec::new()),
                public_handles: AtomicUsize::new(1),
                service_groups: Mutex::new(Vec::new()),
            }),
            public: true,
        }
    }

    /// Creates a service admission group with its own finite request capacity.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Maximum number of requests the group may admit; zero is
    ///   rejected.
    ///
    /// # Returns
    ///
    /// A service group sharing this supervisor's lifecycle and bounded by
    /// `capacity`.
    ///
    /// # Errors
    ///
    /// Returns `ReservationLimit` when `capacity` is zero.
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
                requests: BTreeMap::new(),
                shutdown_waiters: Vec::new(),
                next_shutdown_waiter: 1,
                resource_scopes: BTreeMap::new(),
                next_resource_id: 1,
            }),
            Condvar::new(),
        ));
        lock(&self.inner.service_groups).push(Arc::clone(&state));
        Ok(ServiceGroup {
            state,
            supervisor: self.internal_handle(),
        })
    }
}

impl OperationSupervisor {
    /// Creates a reusable observer for an already requested or future shutdown.
    ///
    /// # Returns
    ///
    /// An observer connected to this supervisor's shared shutdown lifecycle.
    #[must_use]
    #[inline]
    pub fn shutdown_observer(&self) -> ShutdownObserver {
        ShutdownObserver(ShutdownSource::Supervisor(self.internal_handle()))
    }

    /// Number of cleanup permits retained by this shared supervisor.
    ///
    /// # Returns
    ///
    /// The number of permits still held by cleanup work.
    #[must_use]
    #[inline]
    pub fn cleanup_tasks_active(&self) -> usize {
        self.inner.cleanup_responsibility.active()
    }

    /// Reserves a root execution position and its typed-result slot.
    ///
    /// # Returns
    ///
    /// A root admission retaining its execution and result capacity.
    ///
    /// # Errors
    ///
    /// Returns an admission error if the supervisor is closed or lacks
    /// capacity.
    #[inline]
    pub fn try_admit(&self) -> Result<RootAdmission<()>, AdmissionError> {
        self.try_admit_body(()).map_err(|error| error.reason())
    }

    /// Reserve capacity while retaining the unstarted body with the admission.
    /// The body is never invoked by this method.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The body value retained for later transfer to the operation
    ///   layer.
    ///
    /// # Parameters
    ///
    /// * `body` - An unstarted operation body.
    ///
    /// # Returns
    ///
    /// A reservation retaining `body` and its execution/result capacity.
    ///
    /// # Errors
    ///
    /// Returns the original body with the admission error if the supervisor is
    /// closed or has no root/result capacity.
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
    ///
    /// # Type Parameters
    ///
    /// * `F` - The body value retained for later transfer to the operation
    ///   layer.
    ///
    /// # Parameters
    ///
    /// * `body` - An unstarted operation body.
    /// * `context` - Inherited cancellation, deadline, and budget constraints.
    ///
    /// # Returns
    ///
    /// A root reservation retaining the body and associated context tracking.
    ///
    /// # Errors
    ///
    /// Returns the body with an error if context validation or root admission
    /// fails.
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
            context.request_stop(StopReason::ParentStopped);
        }
        Ok(admission)
    }

    /// Creates a bounded asynchronous admission waiter.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The body value retained until capacity becomes available.
    ///
    /// # Parameters
    ///
    /// * `body` - An unstarted operation body.
    ///
    /// # Returns
    ///
    /// A waiter retaining the body until admission can be attempted.
    ///
    /// # Errors
    ///
    /// Returns the body with an error if the supervisor is closed or its waiter
    /// capacity is exhausted.
    #[inline]
    pub fn start_wait_body<F>(&self, body: F) -> Result<AdmissionWait<F>, StartError<F>> {
        self.start_wait_body_with_context(body, None)
    }

    /// Create a bounded asynchronous admission waiter that also observes its
    /// context deadline.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The body value retained until capacity becomes available.
    ///
    /// # Parameters
    ///
    /// * `body` - An unstarted operation body.
    /// * `context` - Optional inherited cancellation and deadline constraints.
    ///
    /// # Returns
    ///
    /// A waiter retaining the body and the optional context signals.
    ///
    /// # Errors
    ///
    /// Returns the body with an error if context validation fails, the
    /// supervisor is closed, or waiter capacity is exhausted.
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

    /// Admit and immediately submit an owned asynchronous body.
    ///
    /// # Type Parameters
    ///
    /// `F` is the body, `Fut` its future, `T` the success value, and `E` the
    /// application error. The body and future must be `Send + 'static`.
    ///
    /// # Parameters
    ///
    /// `options` configures the operation scope; `body` runs after admission.
    ///
    /// # Errors
    ///
    /// Returns the original body with a `StartError` if admission or submission
    /// fails.
    ///
    /// # Returns
    ///
    /// The accepted operation handle, or the original body in `StartError`.
    pub fn start<F, Fut, T, E>(&self, options: ScopeOptions, body: F) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let context = OperationContext::new(options.context.clone());
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let retained = Arc::clone(&body_cell);
        let mut admission = match self.try_admit_with_context(
            move || operation_lock(&retained).take().expect("body invoked once"),
            &context,
        ) {
            Ok(admission) => admission,
            Err(error) => {
                return Err(StartError::new(
                    error.reason(),
                    operation_lock(&body_cell).take().expect("unstarted body retained"),
                ));
            }
        };
        let admitted = admission.transfer().expect("fresh admission transfers once");
        let (_, task_lease, result_lease) = admitted.into_parts();
        self.start_admitted(options, context, body_cell, task_lease, result_lease)
    }

    /// Wait for bounded root admission before submitting the body.
    ///
    /// # Type Parameters
    ///
    /// `F` is the body, `Fut` its future, `T` the success value, and `E` the
    /// application error. The body and future must be `Send + 'static`.
    ///
    /// # Parameters
    ///
    /// `options` configures the operation scope; `body` runs after admission.
    ///
    /// # Errors
    ///
    /// Returns the original body with a `StartError` if admission fails.
    ///
    /// # Returns
    ///
    /// The operation handle once root capacity is available and submission
    /// succeeds.
    pub async fn start_wait<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let context = OperationContext::new(options.context.clone());
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let retained = Arc::clone(&body_cell);
        let admission_body = move || operation_lock(&retained).take().expect("body invoked once");
        let waiter = self
            .start_wait_body_with_context(admission_body, Some(context.clone()))
            .map_err(|error| {
                let reason = error.reason();
                StartError::new(reason, error.into_body()())
            })?;
        let mut admission = waiter.await.map_err(|error| {
            let reason = error.reason();
            StartError::new(reason, error.into_body()())
        })?;
        let admitted = admission.transfer().expect("fresh admission transfers once");
        let (_, task_lease, result_lease) = admitted.into_parts();
        self.start_admitted(options, context, body_cell, task_lease, result_lease)
    }

    /// Block the calling thread until bounded root admission is available.
    ///
    /// Calls from a managed worker are rejected with `WouldDeadlock`.
    ///
    /// # Type Parameters
    ///
    /// `F` is the body, `Fut` its future, `T` the success value, and `E` the
    /// application error. The body and future must be `Send + 'static`.
    ///
    /// # Parameters
    ///
    /// `options` configures the operation scope; `body` runs after admission.
    ///
    /// # Errors
    ///
    /// Returns the original body with a `StartError` if admission fails.
    ///
    /// # Returns
    ///
    /// The operation handle after admission and submission complete.
    pub fn start_wait_blocking<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        if is_portable_worker_thread() || is_thread_executor_worker() {
            return Err(StartError::new(AdmissionError::WouldDeadlock, body));
        }
        block_on_start_wait(self.start_wait(options, body))
    }

    /// Immediately admit a synchronous body and run it on the caller-supplied
    /// bounded executor.
    ///
    /// If root admission would queue, this rolls the reservation back and
    /// returns `Busy` with the original body. Use [`Self::start_wait`] or
    /// [`Self::start_wait_blocking`] to wait.
    ///
    /// # Type Parameters
    ///
    /// `X` is the bounded executor, `F` the synchronous body, `T` its success
    /// value, and `E` its application error. The body must be `Send + 'static`.
    ///
    /// # Parameters
    ///
    /// `executor` runs the body; `options` configures its scope; `body` is
    /// submitted after immediate admission.
    ///
    /// # Errors
    ///
    /// Returns the original body with a `StartError` if admission or execution
    /// submission fails.
    ///
    /// # Returns
    ///
    /// The operation handle if admission and executor submission succeed.
    pub fn start_blocking<X, F, T, E>(
        &self,
        executor: &X,
        options: ScopeOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        X: BlockingExecutor,
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        if is_portable_worker_thread() || executor.is_current_thread_worker() {
            return Err(StartError::new(AdmissionError::WouldDeadlock, body));
        }
        let context = OperationContext::new(options.context.clone());
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let retained = Arc::clone(&body_cell);
        let mut admission = match self.try_admit_with_context(
            move || operation_lock(&retained).take().expect("blocking body invoked once"),
            &context,
        ) {
            Ok(admission) => admission,
            Err(error) => {
                return Err(StartError::new(
                    error.reason(),
                    operation_lock(&body_cell).take().expect("rejected body retained"),
                ));
            }
        };
        if admission.is_queued() {
            drop(admission);
            return Err(StartError::new(
                AdmissionError::Busy,
                operation_lock(&body_cell)
                    .take()
                    .expect("queued blocking body retained"),
            ));
        }
        let admitted = admission.transfer().expect("fresh admission transfers once");
        let (_, task_lease, result_lease) = admitted.into_parts();
        let id = OperationId::new(NEXT_ID.fetch_add(1, Ordering::Relaxed) as u128);
        let shared = Arc::new(Shared {
            state: Mutex::new(OperationState {
                result: None,
                result_taken: false,
                result_discarded: false,
                released: None,
                result_waiters: Vec::new(),
                release_waiters: Vec::new(),
                result_lease: Some(result_lease),
                completion_guards: Vec::new(),
            }),
            changed: Condvar::new(),
            context: context.clone(),
        });
        let scope = OperationScope::new(
            id,
            options.limits,
            options.policy,
            self.cleanup_scheduler(),
            context.clone(),
        );
        let scope_for_job = scope.clone();
        let completion = Arc::new(Mutex::new(BlockingCompletion {
            result: None,
            aborted: false,
            waker: None,
        }));
        let manager_completion = Arc::clone(&completion);
        let manager_scope = scope.clone();
        let manager_shared = Arc::clone(&shared);
        let manager_supervisor = self.internal_handle();
        let manager_context = context.clone();
        let manager_stop = manager_context.stop_signal();
        let manager_deadline = manager_context.deadline_signal();
        let lease = Arc::new(Mutex::new(Some(task_lease)));
        let worker_lease = Arc::clone(&lease);
        let manager = async move {
            manager_supervisor.track_scope_recovery(&manager_scope);
            let result = Cancellable {
                future: Box::pin(poll_fn(|cx| {
                    let mut state = operation_lock(&manager_completion);
                    if let Some(result) = state.result.take() {
                        Poll::Ready(Some(result))
                    } else if state.aborted {
                        Poll::Ready(None)
                    } else {
                        state.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })),
                stop: manager_stop,
                deadline: manager_deadline,
                scope: manager_scope.clone(),
                stopped: false,
                stop_reason: None,
            }
            .await;
            let (Some(result), stopped) = result else {
                return;
            };
            if matches!(&result, BlockingBodyResult::Returned(Err(_)))
                && manager_scope.policy() == ScopePolicy::FailFast
            {
                manager_scope.request_cancel(StopReason::ParentStopped);
            }
            manager_scope.close_body();
            manager_scope.close_body_resources();
            if matches!(result, BlockingBodyResult::Panicked) {
                manager_scope.request_cancel(StopReason::ParentStopped);
            }
            let mut facts = JoinFacts::default();
            poll_fn(|cx| poll_scope_business(&manager_scope, cx, &mut facts)).await;
            let result = match result {
                BlockingBodyResult::Returned(result) => Ok(result),
                BlockingBodyResult::Panicked => Err(()),
            };
            let outcome = Self::body_outcome(result, stopped, &facts, id, "blocking body");
            publish_result(&manager_shared, outcome);
            poll_fn(|cx| poll_scope_releases(&manager_scope, cx, &mut facts)).await;
            let settlement = cleanup_scope_tree(&manager_scope).await;
            let resource_phase = settlement.phase;
            let unconfirmed_count = settlement.unconfirmed;
            let (effects, effect_overflow) = manager_scope.effect_snapshot();
            publish_released(
                &manager_shared,
                SettlementReport {
                    operation_id: id,
                    resource_phase,
                    all_released: resource_phase == ResourcePhase::Released
                        || (resource_phase == ResourcePhase::RecoveryRequired && unconfirmed_count == 0),
                    children: facts.settlements.into(),
                    cleanup_failures: settlement.failures.into(),
                    transfers: settlement.transfers.into(),
                    unconfirmed_count,
                    effects,
                    effect_overflow,
                },
            );
        };
        if let Err(error) = self.spawn_managed(async move {
            manager.await;
            if let Some(lease) = operation_lock(&worker_lease).take() {
                lease.complete();
            }
        }) {
            let reason = match error {
                PortableDriverError::Capacity => AdmissionError::Busy,
                _ => AdmissionError::DriverUnavailable,
            };
            drop(operation_lock(&lease).take());
            return Err(StartError::new(
                reason,
                operation_lock(&body_cell).take().expect("unstarted body retained"),
            ));
        }
        let job_completion = Arc::clone(&completion);
        let job_body = Arc::clone(&body_cell);
        if let Err(rejected) = executor.execute(move || {
            let body = operation_lock(&job_body)
                .take()
                .expect("accepted blocking body invoked once");
            let result = catch_unwind(AssertUnwindSafe(|| body(scope_for_job)))
                .map_or(BlockingBodyResult::Panicked, BlockingBodyResult::Returned);
            let waker = {
                let mut state = operation_lock(&job_completion);
                state.result = Some(result);
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }) {
            let reason = if rejected.error == ExecutorError::Capacity {
                AdmissionError::Busy
            } else {
                AdmissionError::DriverUnavailable
            };
            drop(rejected);
            let waker = {
                let mut state = operation_lock(&completion);
                state.aborted = true;
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
            return Err(StartError::new(
                reason,
                operation_lock(&body_cell)
                    .take()
                    .expect("rejected blocking body retained"),
            ));
        }
        Ok(OperationHandle {
            shared,
            context,
            detached: false,
        })
    }

    /// Lazily starts an operation and returns its business outcome.
    ///
    /// # Type Parameters
    ///
    /// `F` is the body, `Fut` its future, `T` the success value, and `E` its
    /// application error. Both the body and future must be `Send + 'static`.
    ///
    /// # Parameters
    ///
    /// `options` configures the operation scope; `body` is run on first poll.
    ///
    /// # Errors
    ///
    /// Returns the original body with a `StartError` if admission fails.
    ///
    /// # Returns
    ///
    /// The typed business outcome after the operation finishes.
    pub async fn run_async<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        body: F,
    ) -> Result<OperationOutcome<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let mut handle = self.start(options, body)?;
        Ok(handle
            .result()
            .await
            .expect("private run handle result is observed once"))
    }

    /// Runs a body through result delivery and final resource settlement.
    /// Resource-bearing outputs must be transferred or have their use leases
    /// returned before the body completes, otherwise this future remains
    /// pending.
    ///
    /// # Type Parameters
    ///
    /// `F` is the body, `Fut` its future, `T` the success value, and `E` the
    /// application error. The body and future must be `Send + 'static`.
    ///
    /// # Parameters
    ///
    /// `options` configures the operation scope; `body` executes
    /// asynchronously.
    ///
    /// # Errors
    ///
    /// Returns the original body with a `StartError` if admission fails.
    ///
    /// # Returns
    ///
    /// The typed business outcome and final resource settlement report.
    pub async fn run_to_completion<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        body: F,
    ) -> Result<(OperationOutcome<T, E>, SettlementReport), StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let mut handle = self.start(options, body)?;
        let result = handle
            .result()
            .await
            .expect("private run handle result is observed once");
        let released = handle.released().await;
        Ok((result, released))
    }

    /// Stops accepting new root operations and wakes every registered waiter.
    pub fn close(&self) {
        let waiters = {
            let mut state = lock(&self.inner.state);
            state.closed = true;
            take_waiter_wakers(&mut state)
        };
        wake(waiters);
        self.inner.changed.notify_all();
    }

    /// Stops root admission and waits up to the caller-owned timeout for
    /// accepted work. Drain can only be upgraded to Cancel; every call
    /// observes the shared lifecycle.
    ///
    /// # Parameters
    ///
    /// * `mode` - Drain accepted work or request cancellation.
    /// * `timeout` - Maximum duration to wait for accepted requests and
    ///   resources.
    ///
    /// # Returns
    ///
    /// A snapshot of pending requests, recovery work, and resources after
    /// waiting.
    #[must_use]
    pub fn shutdown_blocking(&self, mode: ShutdownMode, timeout: Duration) -> ShutdownReport {
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
            .unwrap_or_else(PoisonError::into_inner);
        ShutdownReport {
            pending_requests: state.active + state.queued.len() + self.service_totals().0,
            pending_recovery: self.pending_recovery(),
            pending_resources: self.service_totals().1,
        }
    }

    /// Begins shutdown and returns an observation after the deadline or
    /// work settles.
    ///
    /// # Parameters
    ///
    /// * `mode` - Drain accepted work or request cancellation.
    /// * `deadline` - Caller-owned deadline for observing shutdown progress.
    /// * `clock` - Clock used to evaluate the deadline.
    ///
    /// # Returns
    ///
    /// An observation of shutdown progress once work settles or the deadline
    /// expires.
    ///
    /// # Errors
    ///
    /// Returns a context error if deadline observation cannot proceed.
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

    /// Number of root bodies currently holding execution capacity.
    ///
    /// # Returns
    ///
    /// The number of currently active root bodies.
    #[must_use]
    #[inline]
    pub fn active_roots(&self) -> usize {
        lock(&self.inner.state).active
    }

    /// Number of accepted roots waiting for execution capacity.
    ///
    /// # Returns
    ///
    /// The number of queued root bodies.
    #[must_use]
    #[inline]
    pub fn queued_roots(&self) -> usize {
        lock(&self.inner.state).queued.len()
    }

    /// Number of reserved typed-result slots, including completed results.
    ///
    /// # Returns
    ///
    /// The number of result slots still reserved.
    #[must_use]
    #[inline]
    pub fn reserved_results(&self) -> usize {
        lock(&self.inner.state).results
    }

    /// Reserve a cleanup slot that does not consume business admission
    /// capacity.
    ///
    /// # Errors
    ///
    /// Returns `Busy` when all cleanup slots are reserved.
    ///
    /// # Returns
    ///
    /// A permit representing one reserved cleanup slot.
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
    ///
    /// # Returns
    ///
    /// The number of cleanup slots currently reserved.
    #[must_use]
    #[inline]
    pub fn cleanup_active(&self) -> usize {
        lock(&self.inner.state).cleanup_active
    }

    /// Creates an internal handle that does not participate in public drop
    /// counting.
    ///
    /// # Returns
    ///
    /// A handle sharing this supervisor without affecting public-handle
    /// shutdown.
    #[inline]
    pub(crate) fn internal_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            public: false,
        }
    }

    /// Returns the shared cleanup scheduler used by supervised scopes.
    ///
    /// # Returns
    ///
    /// A clone of the scheduler shared by this supervisor's scopes.
    #[inline]
    pub(crate) fn cleanup_scheduler(&self) -> CleanupScheduler {
        self.inner.cleanup_scheduler.clone()
    }

    /// Tracks a scope's recovery registry so shutdown observes outstanding
    /// recovery work.
    ///
    /// # Parameters
    ///
    /// * `scope` - Scope whose recovery obligations must be observed during
    ///   shutdown.
    pub(crate) fn track_scope_recovery(&self, scope: &OperationScope) {
        let registry = scope.recovery_registry();
        let mut registries = lock(&self.inner.recovery_registries);
        registries.retain(|entry| !entry.is_empty());
        registries.push(registry);
    }

    /// Submits a managed future to the supervisor's shared bounded portable
    /// driver. This is crate-private so public operation APIs remain the
    /// only submission path.
    ///
    /// # Type Parameters
    ///
    /// * `F` - A sendable, static future whose output is unit.
    ///
    /// # Parameters
    ///
    /// * `future` - Work to submit to the lazily initialized shared driver.
    ///
    /// # Errors
    ///
    /// Returns a driver error when its configuration is invalid, a worker
    /// cannot be started, or the bounded driver cannot accept the task.
    ///
    /// # Returns
    ///
    /// A handle for the accepted task.
    pub(crate) fn spawn_managed<F>(&self, future: F) -> Result<TaskHandle, PortableDriverError>
    where
        F: Future<Output = ()> + Send + 'static,
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

    /// Counts recovery obligations retained by tracked scopes.
    ///
    /// # Returns
    ///
    /// The number of outstanding recovery obligations.
    pub(super) fn pending_recovery(&self) -> usize {
        lock(&self.inner.recovery_registries)
            .iter()
            .map(RecoveryRegistry::len)
            .sum()
    }

    /// Returns the aggregate standalone-request and resource counts across
    /// groups.
    ///
    /// # Returns
    ///
    /// A pair containing the standalone request count and resource count.
    pub(super) fn service_totals(&self) -> (usize, usize) {
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

    /// Registers a shutdown observer with every nonempty recovery registry.
    ///
    /// # Parameters
    ///
    /// * `waker` - Task to wake when a tracked recovery registry changes.
    pub(super) fn register_recovery_wakers(&self, waker: &Waker) {
        let registries = lock(&self.inner.recovery_registries).clone();
        for registry in registries {
            let _ = registry.register_waker_if_nonempty(waker);
        }
    }

    /// Closes admission, applies the strongest requested mode, and wakes
    /// observers.
    ///
    /// # Parameters
    ///
    /// * `mode` - Shutdown policy to apply; cancellation can upgrade a prior
    ///   drain.
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
                take(&mut state.shutdown_waiters),
            )
        };
        for context in contexts {
            context.request_stop(StopReason::ParentStopped);
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

    /// Submits a body after admission has transferred its execution leases.
    ///
    /// # Type Parameters
    ///
    /// * `F` - The owned operation body retained until the worker starts it.
    /// * `Fut` - The sendable future produced by the body.
    /// * `T` - The sendable success value returned by the future.
    /// * `E` - The sendable application error returned by the future.
    ///
    /// # Parameters
    ///
    /// * `options` - Scope limits and failure policy for the admitted
    ///   operation.
    /// * `context` - Cancellation, deadline, and budget state inherited by the
    ///   operation.
    /// * `body_cell` - Shared slot retaining the body until the managed worker
    ///   invokes it.
    /// * `task_lease` - Transferred execution-capacity reservation for the
    ///   operation.
    /// * `result_lease` - Transferred result-capacity reservation for its
    ///   outcome.
    ///
    /// # Returns
    ///
    /// The operation handle after its task and result leases have been
    /// accepted.
    ///
    /// # Errors
    ///
    /// Returns the original body in `StartError` if the managed driver rejects
    /// submission.
    fn start_admitted<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        context: OperationContext,
        body_cell: Arc<Mutex<Option<F>>>,
        task_lease: TaskLease,
        result_lease: ResultLease,
    ) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let id = OperationId::new(NEXT_ID.fetch_add(1, Ordering::Relaxed) as u128);
        let shared = Arc::new(Shared {
            state: Mutex::new(OperationState {
                result: None,
                result_taken: false,
                result_discarded: false,
                released: None,
                result_waiters: Vec::new(),
                release_waiters: Vec::new(),
                result_lease: Some(result_lease),
                completion_guards: Vec::new(),
            }),
            changed: Condvar::new(),
            context: context.clone(),
        });
        let worker_shared = Arc::clone(&shared);
        let gate_shared = Arc::clone(&shared);
        let worker_context = context.clone();
        let body_context = worker_context.clone();
        let scope_context = worker_context.clone();
        let mut admission_stop = worker_context.stop_signal();
        let worker_body = Arc::clone(&body_cell);
        let cleanup_scheduler = self.cleanup_scheduler();
        let manager_supervisor = self.internal_handle();
        let future = async move {
            let scope = OperationScope::new(id, options.limits, options.policy, cleanup_scheduler, scope_context);
            manager_supervisor.track_scope_recovery(&scope);
            let f = operation_lock(&worker_body).take().expect("body invoked once");
            let body_scope = scope.clone();
            let body_future = async move {
                let body_future = f(body_scope);
                body_future.await
            };
            let (result, stopped) = Cancellable {
                future: Box::pin(CatchUnwind::new(body_future)),
                stop: body_context.stop_signal(),
                deadline: body_context.deadline_signal(),
                scope: scope.clone(),
                stopped: false,
                stop_reason: None,
            }
            .await;
            if stopped.is_none()
                && (result.is_err() || (matches!(&result, Ok(Err(_))) && scope.policy() == ScopePolicy::FailFast))
            {
                scope.request_cancel(StopReason::ParentStopped);
            }
            scope.close_body();
            scope.close_body_resources();
            let mut facts = JoinFacts::default();
            poll_fn(|cx| poll_scope_business(&scope, cx, &mut facts)).await;
            let outcome = Self::body_outcome(result, stopped, &facts, id, "async body");
            publish_result(&worker_shared, outcome);
            poll_fn(|cx| poll_scope_releases(&scope, cx, &mut facts)).await;
            let settlement = cleanup_scope_tree(&scope).await;
            let resource_phase = settlement.phase;
            let failures = settlement.failures;
            let transfers = settlement.transfers;
            let unconfirmed_count = settlement.unconfirmed;
            let (effects, effect_overflow) = scope.effect_snapshot();
            let report = SettlementReport {
                operation_id: id,
                resource_phase,
                all_released: resource_phase == ResourcePhase::Released
                    || (resource_phase == ResourcePhase::RecoveryRequired && unconfirmed_count == 0),
                children: facts.settlements.into(),
                cleanup_failures: failures.into(),
                transfers: transfers.into(),
                unconfirmed_count,
                effects,
                effect_overflow,
            };
            publish_released(&worker_shared, report);
        };
        let lease = Arc::new(Mutex::new(Some(task_lease)));
        let worker_lease = Arc::clone(&lease);
        match self.spawn_managed(async move {
            let ready = poll_fn(|cx| {
                if let Poll::Ready(active) = operation_lock(&worker_lease)
                    .as_ref()
                    .expect("task lease")
                    .poll_active(cx)
                {
                    return Poll::Ready(active);
                }
                if let Poll::Ready(Ok(_reason)) = Pin::new(&mut admission_stop).poll(cx) {
                    return Poll::Ready(false);
                }
                Poll::Pending
            })
            .await;
            if ready && worker_context.admission_check().is_ok() {
                future.await;
            } else {
                let reason = worker_context.stop_reason().unwrap_or(StopReason::UserCancelled);
                publish_result(
                    &gate_shared,
                    OperationOutcome::Stopped(StopReport {
                        reason,
                        accepted_at: None,
                    }),
                );
                publish_released(
                    &gate_shared,
                    SettlementReport {
                        operation_id: id,
                        resource_phase: ResourcePhase::Released,
                        all_released: true,
                        children: Arc::from([]),
                        cleanup_failures: Arc::from([]),
                        transfers: Arc::from([]),
                        unconfirmed_count: 0,
                        effects: Arc::from([]),
                        effect_overflow: 0,
                    },
                );
            }
            if let Some(lease) = operation_lock(&worker_lease).take() {
                lease.complete();
            }
        }) {
            Ok(_) => Ok(OperationHandle {
                shared,
                context,
                detached: false,
            }),
            Err(error) => {
                let reason = match error {
                    PortableDriverError::Capacity => AdmissionError::Busy,
                    _ => AdmissionError::DriverUnavailable,
                };
                drop(operation_lock(&lease).take());
                let body = operation_lock(&body_cell).take().expect("unstarted body retained");
                Err(StartError::new(reason, body))
            }
        }
    }

    /// Converts a completed body result into the operation's public outcome
    /// variants.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Successful body value.
    /// * `E` - Application error returned by the body.
    ///
    /// # Parameters
    ///
    /// * `result` - Normalized panic or business result from an async or
    ///   blocking body.
    /// * `stopped` - Stop reason, if cancellation or deadline won the race.
    /// * `facts` - Settled child-operation facts used to apply fail-fast
    ///   policy.
    /// * `operation_id` - Identifier attached to panic outcomes.
    /// * `phase` - Body kind recorded in a panic summary.
    ///
    /// # Returns
    ///
    /// The stopped, panicked, child-failed, succeeded, or failed operation
    /// outcome.
    fn body_outcome<T, E>(
        result: Result<Result<T, E>, ()>,
        stopped: Option<StopReason>,
        facts: &JoinFacts,
        operation_id: OperationId,
        phase: &'static str,
    ) -> OperationOutcome<T, E> {
        if let Some(reason) = stopped {
            drop(result);
            return OperationOutcome::Stopped(StopReport {
                reason,
                accepted_at: None,
            });
        }
        match result {
            Err(()) => OperationOutcome::Panicked(PanicSummary {
                operation_id,
                phase: DiagnosticSummary::new(phase),
            }),
            Ok(Ok(value)) if facts.failed_required.is_some() => {
                drop(value);
                let child = facts.failed_required.as_ref().expect("checked");
                OperationOutcome::ChildFailed(ChildFailureSummary {
                    operation_id: child.operation_id,
                    summary: child.summary.clone(),
                })
            }
            Ok(Ok(value)) => OperationOutcome::Succeeded(value),
            Ok(Err(error)) => OperationOutcome::Failed(error),
        }
    }
}

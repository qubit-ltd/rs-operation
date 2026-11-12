//! Bounded root admission for managed operations.
//!
//! This module owns admission bookkeeping only. Operation execution and result
//! observation are attached by the operation handle layer.

use crate::context::{ContextError, DeadlineSignal, OperationContext, StopSignal};
use crate::driver::{PortableDriver, PortableDriverConfig, PortableDriverError, TaskHandle};
use crate::report::{AdmissionError, StartError, SupervisorLimits};
use crate::resource::{CleanupResponsibility, CleanupScheduler};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

struct Inner {
    limits: SupervisorLimits,
    state: Mutex<State>,
    driver: Mutex<Option<Arc<PortableDriver>>>,
    cleanup_scheduler: CleanupScheduler,
    cleanup_responsibility: CleanupResponsibility,
}

struct State {
    closed: bool,
    active: usize,
    queued: VecDeque<Weak<Mutex<LeaseState>>>,
    results: usize,
    cleanup_active: usize,
    waiters: Vec<(u64, Option<Waker>)>,
    next_waiter: u64,
}

#[derive(Default)]
struct LeaseState {
    queued: bool,
    active: bool,
    execution_released: bool,
    result_reserved: bool,
    active_waiters: Vec<Waker>,
}

/// Application-owned bounded root operation admission service.
#[derive(Clone)]
pub struct OperationSupervisor {
    inner: Arc<Inner>,
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
                }),
                driver: Mutex::new(None),
                cleanup_scheduler,
                cleanup_responsibility,
            }),
        }
    }

    pub(crate) fn cleanup_scheduler(&self) -> CleanupScheduler {
        self.inner.cleanup_scheduler.clone()
    }

    /// Number of cleanup permits retained by this shared supervisor.
    pub fn cleanup_tasks_active(&self) -> usize {
        self.inner.cleanup_responsibility.active()
    }

    /// Submit a managed future to the supervisor's shared bounded portable driver.
    /// This is crate-private so public operation APIs remain the only submission path.
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

    /// Validate inherited cancellation, deadline, and budget before reserving root capacity.
    pub fn try_admit_with_context<F>(
        &self,
        body: F,
        context: &OperationContext,
    ) -> Result<RootAdmission<F>, StartError<F>> {
        if let Err(error) = context.admission_check() {
            return Err(StartError::new(
                context_admission_error(context, error),
                body,
            ));
        }
        self.try_admit_body(body)
    }

    /// Create a bounded asynchronous admission waiter.
    pub fn start_wait_body<F>(&self, body: F) -> Result<AdmissionWait<F>, StartError<F>> {
        self.start_wait_body_with_context(body, None)
    }

    /// Create a bounded asynchronous admission waiter that also observes its context deadline.
    pub fn start_wait_body_with_context<F>(
        &self,
        body: F,
        context: Option<OperationContext>,
    ) -> Result<AdmissionWait<F>, StartError<F>> {
        if let Some(context) = &context
            && let Err(error) = context.admission_check()
        {
            return Err(StartError::new(
                context_admission_error(context, error),
                body,
            ));
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
            supervisor: self.clone(),
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

    /// Reserve a cleanup slot that does not consume business admission capacity.
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

/// Independent cleanup capacity reservation.
pub struct CleanupPermit {
    inner: Arc<Inner>,
    released: bool,
}

impl Drop for CleanupPermit {
    fn drop(&mut self) {
        if !self.released {
            let mut state = lock(&self.inner.state);
            state.cleanup_active = state.cleanup_active.saturating_sub(1);
            self.released = true;
        }
    }
}

/// A rollback-safe root capacity reservation.
pub struct RootAdmission<F> {
    inner: Arc<Inner>,
    lease: Arc<Mutex<LeaseState>>,
    body: Option<F>,
    transferred: bool,
}

impl<F> RootAdmission<F> {
    /// Whether this accepted operation is waiting for an active-root slot.
    pub fn is_queued(&self) -> bool {
        lock(&self.lease).queued
    }

    /// Transfer an accepted body and its independent execution/result leases to the coordinator.
    ///
    /// This is crate-private until the operation handle layer can bind both leases to
    /// the actual task and result registry. Dropping this reservation after transfer
    /// cannot release either lease.
    pub(crate) fn transfer(&mut self) -> Option<AdmittedTask<F>> {
        if self.transferred {
            return None;
        }
        let body = self.body.take()?;
        self.transferred = true;
        Some(AdmittedTask {
            body,
            execution: Some(TaskLease {
                inner: Arc::clone(&self.inner),
                lease: Arc::clone(&self.lease),
            }),
            result: Some(ResultLease {
                inner: Arc::clone(&self.inner),
                lease: Arc::clone(&self.lease),
            }),
        })
    }
}

impl<F> fmt::Debug for RootAdmission<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootAdmission")
            .field("queued", &self.is_queued())
            .field("transferred", &self.transferred)
            .finish_non_exhaustive()
    }
}

impl<F> Drop for RootAdmission<F> {
    fn drop(&mut self) {
        if self.transferred {
            return;
        }
        release_all(&self.inner, &self.lease);
    }
}

/// Body transferred to a driver with separately owned lifecycle leases.
pub(crate) struct AdmittedTask<F> {
    body: F,
    execution: Option<TaskLease>,
    result: Option<ResultLease>,
}

impl<F> AdmittedTask<F> {
    pub(crate) fn into_parts(mut self) -> (F, TaskLease, ResultLease) {
        (
            self.body,
            self.execution.take().expect("execution lease exists"),
            self.result.take().expect("result lease exists"),
        )
    }
}

/// Capacity token retained by the running task until it finishes or is dropped.
pub(crate) struct TaskLease {
    inner: Arc<Inner>,
    lease: Arc<Mutex<LeaseState>>,
}

impl TaskLease {
    /// Mark task completion and release execution capacity.
    pub(crate) fn complete(self) {}

    /// Poll until this reservation owns an active root execution slot.
    pub(crate) fn poll_active(&self, cx: &mut Context<'_>) -> Poll<bool> {
        let mut lease = lock(&self.lease);
        if lease.active {
            return Poll::Ready(true);
        }
        if lease.execution_released {
            return Poll::Ready(false);
        }
        if !lease
            .active_waiters
            .iter()
            .any(|waker| waker.will_wake(cx.waker()))
        {
            lease.active_waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for TaskLease {
    fn drop(&mut self) {
        release_execution(&self.inner, &self.lease);
    }
}

/// Capacity token retained with a result until it is observed or discarded.
pub(crate) struct ResultLease {
    inner: Arc<Inner>,
    lease: Arc<Mutex<LeaseState>>,
}

impl ResultLease {
    /// Release this slot after the result is taken or explicitly discarded.
    pub(crate) fn observe(self) {}
}

impl Drop for ResultLease {
    fn drop(&mut self) {
        release_result(&self.inner, &self.lease);
    }
}

fn release_execution(inner: &Arc<Inner>, lease: &Arc<Mutex<LeaseState>>) {
    let waiters = {
        let mut state = lock(&inner.state);
        let mut lease_state = lock(lease);
        if lease_state.execution_released {
            return;
        }
        lease_state.execution_released = true;
        let mut promoted = Vec::new();
        if lease_state.active {
            lease_state.active = false;
            state.active = state.active.saturating_sub(1);
            promoted.extend(promote_one(&mut state, inner.limits.active_roots));
        }
        if lease_state.queued {
            lease_state.queued = false;
            state.queued.retain(|entry| {
                entry
                    .upgrade()
                    .is_some_and(|candidate| !Arc::ptr_eq(&candidate, lease))
            });
        }
        promoted.extend(take_waiter_wakers(&mut state));
        promoted
    };
    wake(waiters);
}

fn release_result(inner: &Arc<Inner>, lease: &Arc<Mutex<LeaseState>>) {
    let waiters = {
        let mut state = lock(&inner.state);
        let mut lease_state = lock(lease);
        if !lease_state.result_reserved {
            return;
        }
        lease_state.result_reserved = false;
        state.results = state.results.saturating_sub(1);
        take_waiter_wakers(&mut state)
    };
    wake(waiters);
}

fn release_all(inner: &Arc<Inner>, lease: &Arc<Mutex<LeaseState>>) {
    let waiters = {
        let mut state = lock(&inner.state);
        let mut lease_state = lock(lease);
        let mut promoted = Vec::new();
        if !lease_state.execution_released {
            lease_state.execution_released = true;
            if lease_state.active {
                lease_state.active = false;
                state.active = state.active.saturating_sub(1);
                promoted.extend(promote_one(&mut state, inner.limits.active_roots));
            }
            if lease_state.queued {
                lease_state.queued = false;
                state.queued.retain(|entry| {
                    entry
                        .upgrade()
                        .is_some_and(|candidate| !Arc::ptr_eq(&candidate, lease))
                });
            }
        }
        if lease_state.result_reserved {
            lease_state.result_reserved = false;
            state.results = state.results.saturating_sub(1);
        }
        promoted.extend(take_waiter_wakers(&mut state));
        promoted
    };
    wake(waiters);
}

/// A bounded waiter for root admission. Dropping it removes its registration;
/// after admission, dropping the returned reservation follows the ordinary root path.
pub struct AdmissionWait<F> {
    supervisor: OperationSupervisor,
    waiter_id: u64,
    body: Option<F>,
    context: Option<OperationContext>,
    deadline_signal: Option<DeadlineSignal>,
    stop_signal: Option<StopSignal>,
    done: bool,
}

impl<F> fmt::Debug for AdmissionWait<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdmissionWait")
            .field("waiter_id", &self.waiter_id)
            .finish_non_exhaustive()
    }
}

impl<F> Unpin for AdmissionWait<F> {}

impl<F> Future for AdmissionWait<F> {
    type Output = Result<RootAdmission<F>, StartError<F>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.done {
            panic!("admission waiter polled after completion");
        }
        if let Some(signal) = &mut this.stop_signal {
            match Pin::new(signal).poll(cx) {
                Poll::Ready(Ok(reason)) => {
                    let error = if reason == crate::report::StopReason::Timeout {
                        AdmissionError::DeadlineExpired
                    } else {
                        AdmissionError::Stopped
                    };
                    let error = StartError::new(error, this.body.take().expect("body retained"));
                    this.unregister();
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Err(ContextError::WaiterCapacityExceeded)) => {
                    let error = StartError::new(
                        AdmissionError::Busy,
                        this.body.take().expect("body retained"),
                    );
                    this.unregister();
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Err(error)) => {
                    let error = StartError::new(
                        context_admission_error(
                            this.context.as_ref().expect("signal has context"),
                            error,
                        ),
                        this.body.take().expect("body retained"),
                    );
                    this.unregister();
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => {}
            }
        }
        if let Some(context) = &this.context {
            if let Some(signal) = &mut this.deadline_signal
                && Pin::new(signal).poll(cx).is_ready()
            {
                let error =
                    StartError::new(AdmissionError::DeadlineExpired, this.body.take().unwrap());
                this.unregister();
                return Poll::Ready(Err(error));
            }
            if let Err(error) = context.admission_check() {
                let error = StartError::new(
                    context_admission_error(context, error),
                    this.body.take().unwrap(),
                );
                this.unregister();
                return Poll::Ready(Err(error));
            }
        }
        match this
            .supervisor
            .try_admit_body(this.body.take().expect("body retained"))
        {
            Ok(admission) => {
                this.unregister();
                Poll::Ready(Ok(admission))
            }
            Err(error) if error.reason() == AdmissionError::Busy => {
                this.body = Some(error.into_body());
                this.register_waker(cx.waker());
                match this
                    .supervisor
                    .try_admit_body(this.body.take().expect("body retained"))
                {
                    Ok(admission) => {
                        this.unregister();
                        Poll::Ready(Ok(admission))
                    }
                    Err(error) if error.reason() == AdmissionError::Busy => {
                        this.body = Some(error.into_body());
                        Poll::Pending
                    }
                    Err(error) => {
                        this.unregister();
                        Poll::Ready(Err(error))
                    }
                }
            }
            Err(error) => {
                this.unregister();
                Poll::Ready(Err(error))
            }
        }
    }
}

impl<F> AdmissionWait<F> {
    fn register_waker(&self, waker: &Waker) {
        let mut replacement = Some(waker.clone());
        let replaced = {
            let mut state = lock(&self.supervisor.inner.state);
            state
                .waiters
                .iter_mut()
                .find(|(id, _)| *id == self.waiter_id)
                .and_then(|(_, registered)| {
                    if registered.as_ref().is_some_and(|old| {
                        old.will_wake(replacement.as_ref().expect("waker cloned"))
                    }) {
                        None
                    } else {
                        registered.replace(replacement.take().expect("waker cloned"))
                    }
                })
        };
        drop(replaced);
        drop(replacement);
    }

    fn unregister(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        let removed = {
            let mut state = lock(&self.supervisor.inner.state);
            state
                .waiters
                .iter()
                .position(|(id, _)| *id == self.waiter_id)
                .map(|index| state.waiters.swap_remove(index).1)
        };
        drop(removed);
        self.deadline_signal.take();
        self.stop_signal.take();
    }
}

impl<F> Drop for AdmissionWait<F> {
    fn drop(&mut self) {
        self.unregister();
    }
}

fn context_admission_error(context: &OperationContext, error: ContextError) -> AdmissionError {
    match error {
        ContextError::DeadlineExpired => AdmissionError::DeadlineExpired,
        ContextError::Stopped
            if context.stop_reason() == Some(crate::report::StopReason::Timeout) =>
        {
            AdmissionError::DeadlineExpired
        }
        ContextError::Stopped => AdmissionError::Stopped,
        ContextError::BudgetUnavailable => AdmissionError::ReservationLimit,
        ContextError::WaiterCapacityExceeded => AdmissionError::Busy,
        ContextError::ClockUnavailable | ContextError::ClockDomainMismatch => {
            AdmissionError::DriverUnavailable
        }
    }
}

fn take_waiter_wakers(state: &mut State) -> Vec<Waker> {
    state
        .waiters
        .iter_mut()
        .filter_map(|(_, waker)| waker.take())
        .collect()
}

fn wake(waiters: Vec<Waker>) {
    for waker in waiters {
        waker.wake();
    }
}

fn promote_one(state: &mut State, active_limit: usize) -> Vec<Waker> {
    while state.active < active_limit {
        let Some(next) = state.queued.pop_front() else {
            return Vec::new();
        };
        let Some(next) = next.upgrade() else {
            continue;
        };
        let mut lease = lock(&next);
        if lease.execution_released || !lease.queued {
            continue;
        }
        lease.queued = false;
        lease.active = true;
        state.active += 1;
        return std::mem::take(&mut lease.active_waiters);
    }
    Vec::new()
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn limits(
        active_roots: usize,
        queued_roots: usize,
        unobserved_results: usize,
    ) -> SupervisorLimits {
        SupervisorLimits {
            active_roots,
            queued_roots,
            unobserved_results,
            admission_waiters: 1,
            ..SupervisorLimits::default()
        }
    }

    #[test]
    fn root_and_result_capacity_are_reserved_and_released_separately() {
        let supervisor = OperationSupervisor::new(limits(1, 1, 2));
        let active = supervisor.try_admit().unwrap();
        let queued = supervisor.try_admit().unwrap();
        assert!(queued.is_queued());
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        drop(active);
        assert_eq!(supervisor.active_roots(), 1);
        assert_eq!(supervisor.queued_roots(), 0);
        let replacement = supervisor.try_admit().unwrap();
        assert!(replacement.is_queued());
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        drop(queued);
        assert_eq!(supervisor.active_roots(), 1);
        drop(replacement);
        assert_eq!(supervisor.active_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 0);
        assert!(supervisor.try_admit().is_ok());
    }

    #[test]
    fn dropping_queued_admission_rolls_back_without_calling_its_body() {
        let supervisor = OperationSupervisor::new(limits(0, 1, 1));
        let count = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&count);
        let admission = supervisor
            .try_admit_body(move || captured.fetch_add(1, Ordering::SeqCst))
            .unwrap();
        assert!(admission.is_queued());
        drop(admission);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(supervisor.queued_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 0);
    }

    #[test]
    fn waiters_are_bounded_cancel_safely_and_recover_rejected_input() {
        use std::task::{Wake, Waker};
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }

        let supervisor = OperationSupervisor::new(limits(0, 1, 2));
        let accepted = supervisor.try_admit().unwrap();
        let mut waiting = Box::pin(supervisor.start_wait_body(String::from("kept")).unwrap());
        let waker = Waker::from(Arc::new(Noop));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        let rejected = supervisor
            .start_wait_body(String::from("rejected"))
            .unwrap_err();
        assert_eq!(rejected.reason(), AdmissionError::Busy);
        assert_eq!(rejected.into_body(), "rejected");
        drop(waiting);
        drop(accepted);
        assert_eq!(lock(&supervisor.inner.state).waiters.len(), 0);
    }

    #[test]
    fn close_wakes_waiters_and_they_recover_their_input() {
        use std::task::{Wake, Waker};
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }
        let supervisor = OperationSupervisor::new(limits(0, 1, 2));
        let _accepted = supervisor.try_admit().unwrap();
        let mut waiting = Box::pin(supervisor.start_wait_body(String::from("body")).unwrap());
        let waker = Waker::from(Arc::new(Noop));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        supervisor.close();
        match waiting.as_mut().poll(&mut context) {
            Poll::Ready(Err(error)) => {
                assert_eq!(error.reason(), AdmissionError::Closed);
                assert_eq!(error.into_body(), "body");
            }
            other => panic!("expected closed error, got {other:?}"),
        }
    }

    #[test]
    fn result_slot_limit_is_one_shot_and_released_separately() {
        let supervisor = OperationSupervisor::new(limits(1, 0, 1));
        let mut first = supervisor.try_admit_body(String::from("body")).unwrap();
        let work = first.transfer().unwrap();
        drop(first);
        let (body, task_lease, result_lease) = work.into_parts();
        assert_eq!(body, "body");
        assert_eq!(supervisor.active_roots(), 1);
        assert_eq!(supervisor.reserved_results(), 1);
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        task_lease.complete();
        assert_eq!(supervisor.active_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 1);
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        result_lease.observe();
        assert_eq!(supervisor.reserved_results(), 0);
        assert!(supervisor.try_admit().is_ok());
    }

    #[test]
    fn cleanup_capacity_remains_available_when_business_admission_is_full() {
        let supervisor = OperationSupervisor::new(limits(1, 0, 1));
        let _root = supervisor.try_admit().unwrap();
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
        let _cleanup = supervisor.try_reserve_cleanup().unwrap();
        assert_eq!(supervisor.cleanup_active(), 1);
    }

    #[test]
    fn expired_context_rejects_and_returns_the_original_body() {
        use crate::context::{ContextOptions, Deadline};
        use crate::protocol::{ClockSource, TimerRegistration};
        use crate::report::{ClockDomainId, MonotonicTime};
        use std::task::Waker;

        struct FixedClock;
        struct NoTimer;
        impl TimerRegistration for NoTimer {
            fn cancel(self: Box<Self>) {}
        }
        impl ClockSource for FixedClock {
            fn now(&self) -> MonotonicTime {
                MonotonicTime::new(ClockDomainId(9), 2)
            }
            fn register_deadline(&self, _: MonotonicTime, _: Waker) -> Box<dyn TimerRegistration> {
                Box::new(NoTimer)
            }
        }

        let supervisor = OperationSupervisor::new(limits(1, 1, 1));
        let context = OperationContext::new(ContextOptions {
            clock: Some(Arc::new(FixedClock)),
            deadline: Some(Deadline::at(MonotonicTime::new(ClockDomainId(9), 1))),
            ..ContextOptions::default()
        });
        let error = supervisor
            .try_admit_with_context(String::from("body"), &context)
            .unwrap_err();
        assert_eq!(error.reason(), AdmissionError::DeadlineExpired);
        assert_eq!(error.into_body(), "body");
        assert_eq!(supervisor.active_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 0);
    }

    #[test]
    fn close_rejects_new_work() {
        let supervisor = OperationSupervisor::new(limits(0, 1, 1));
        supervisor.close();
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Closed);
    }
}

#[cfg(test)]
mod review_regressions {
    use super::*;
    use crate::report::SupervisorLimits;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn dropping_admission_handle_does_not_release_transferred_running_body_quota() {
        let supervisor = OperationSupervisor::new(SupervisorLimits {
            active_roots: 1,
            queued_roots: 0,
            unobserved_results: 1,
            ..SupervisorLimits::default()
        });
        let runs = Arc::new(AtomicUsize::new(0));
        let body_runs = Arc::clone(&runs);
        let mut admission = supervisor
            .try_admit_body(move || body_runs.fetch_add(1, Ordering::SeqCst))
            .unwrap();
        let work = admission.transfer().unwrap();
        let (body, execution, result) = work.into_parts();
        drop(admission);

        assert_eq!(body(), 0);
        assert_eq!(supervisor.active_roots(), 1);
        assert_eq!(supervisor.reserved_results(), 1);
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        execution.complete();
        assert_eq!(supervisor.active_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 1);
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
        result.observe();
        assert_eq!(supervisor.reserved_results(), 0);
    }
}

#[cfg(test)]
mod reentrant_waker_regression {
    use super::*;
    use core::task::{Context, RawWaker, RawWakerVTable, Waker};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    struct Probe {
        supervisor: OperationSupervisor,
        lock_timeouts: Arc<AtomicUsize>,
    }

    fn check_reentry(probe: &Probe) {
        let supervisor = probe.supervisor.clone();
        let (send, receive) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = supervisor.active_roots();
            let _ = send.send(());
        });
        if receive.recv_timeout(Duration::from_millis(100)).is_err() {
            probe.lock_timeouts.fetch_add(1, Ordering::SeqCst);
        }
    }

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        let probe = unsafe { Arc::<Probe>::from_raw(data.cast()) };
        check_reentry(&probe);
        let clone = Arc::clone(&probe);
        std::mem::forget(probe);
        RawWaker::new(Arc::into_raw(clone).cast(), &VTABLE)
    }

    unsafe fn wake_waker(data: *const ()) {
        unsafe { drop_waker(data) };
    }

    unsafe fn wake_by_ref(data: *const ()) {
        let probe = unsafe { &*data.cast::<Probe>() };
        check_reentry(probe);
    }

    unsafe fn drop_waker(data: *const ()) {
        let probe = unsafe { Arc::<Probe>::from_raw(data.cast()) };
        check_reentry(&probe);
        drop(probe);
    }

    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref, drop_waker);

    fn reentrant_waker(
        supervisor: &OperationSupervisor,
        lock_timeouts: &Arc<AtomicUsize>,
    ) -> Waker {
        let probe = Arc::new(Probe {
            supervisor: supervisor.clone(),
            lock_timeouts: Arc::clone(lock_timeouts),
        });
        let raw = RawWaker::new(Arc::into_raw(probe).cast(), &VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    #[test]
    fn raw_waker_clone_replacement_and_removal_can_reenter_without_lock() {
        let supervisor = OperationSupervisor::new(SupervisorLimits {
            active_roots: 0,
            queued_roots: 0,
            unobserved_results: 1,
            ..SupervisorLimits::default()
        });
        let lock_timeouts = Arc::new(AtomicUsize::new(0));
        let first_waker = reentrant_waker(&supervisor, &lock_timeouts);
        let second_waker = reentrant_waker(&supervisor, &lock_timeouts);
        let mut first_cx = Context::from_waker(&first_waker);
        let mut second_cx = Context::from_waker(&second_waker);
        let mut waiter = Box::pin(supervisor.start_wait_body(()).unwrap());
        assert!(matches!(waiter.as_mut().poll(&mut first_cx), Poll::Pending));
        // Different RawWaker data forces a replacement and destruction of the old waker.
        assert!(matches!(
            waiter.as_mut().poll(&mut second_cx),
            Poll::Pending
        ));
        drop(waiter);
        assert_eq!(lock_timeouts.load(Ordering::SeqCst), 0);
    }
}

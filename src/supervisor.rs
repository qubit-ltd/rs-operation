//! Bounded root admission for managed operations.
//!
//! This module owns admission bookkeeping only. Operation execution and result
//! observation are attached by the operation handle layer.

use crate::context::{ContextError, DeadlineSignal, OperationContext};
use crate::report::{AdmissionError, StartError, SupervisorLimits};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

struct Inner {
    limits: SupervisorLimits,
    state: Mutex<State>,
}

struct State {
    closed: bool,
    active: usize,
    queued: VecDeque<Weak<Mutex<LeaseState>>>,
    results: usize,
    cleanup_active: usize,
    stop_requests: usize,
    waiters: Vec<(u64, Option<Waker>)>,
    next_waiter: u64,
}

#[derive(Default)]
struct LeaseState {
    queued: bool,
    active: bool,
    result_reserved: bool,
    released: bool,
}

/// Application-owned bounded root operation admission service.
#[derive(Clone)]
pub struct OperationSupervisor {
    inner: Arc<Inner>,
}

impl OperationSupervisor {
    /// Create a supervisor with explicit finite capacities.
    pub fn new(limits: SupervisorLimits) -> Self {
        Self {
            inner: Arc::new(Inner {
                limits,
                state: Mutex::new(State {
                    closed: false,
                    active: 0,
                    queued: VecDeque::new(),
                    results: 0,
                    cleanup_active: 0,
                    stop_requests: 0,
                    waiters: Vec::new(),
                    next_waiter: 1,
                }),
            }),
        }
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
        if let Some(context) = &context {
            if let Err(error) = context.admission_check() {
                return Err(StartError::new(
                    context_admission_error(context, error),
                    body,
                ));
            }
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
        Ok(AdmissionWait {
            supervisor: self.clone(),
            waiter_id: id,
            body: Some(body),
            context,
            deadline_signal,
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

    /// Record a nonblocking stop control independently of business queue capacity.
    pub fn request_stop(&self) {
        let mut state = lock(&self.inner.state);
        state.stop_requests = state.stop_requests.saturating_add(1);
    }

    /// Number of stop controls accepted by the independent control path.
    pub fn stop_requests(&self) -> usize {
        lock(&self.inner.state).stop_requests
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
}

impl<F> RootAdmission<F> {
    /// Whether this accepted operation is waiting for an active-root slot.
    pub fn is_queued(&self) -> bool {
        lock(&self.lease).queued
    }

    /// Complete execution capacity while keeping its result slot reserved.
    pub fn complete(&mut self) {
        let waiters = {
            let mut state = lock(&self.inner.state);
            let mut lease = lock(&self.lease);
            if lease.released || !lease.active {
                return;
            }
            lease.active = false;
            state.active = state.active.saturating_sub(1);
            promote_one(&mut state, self.inner.limits.active_roots);
            take_waiter_wakers(&mut state)
        };
        wake(waiters);
    }

    /// Release the result slot after its result was taken or explicitly discarded.
    pub fn take_result_slot(&mut self) {
        let waiters = {
            let mut state = lock(&self.inner.state);
            let mut lease = lock(&self.lease);
            if lease.result_reserved {
                lease.result_reserved = false;
                state.results = state.results.saturating_sub(1);
            }
            take_waiter_wakers(&mut state)
        };
        wake(waiters);
    }

    /// Move the still-unstarted body to its driver while retaining admission leases.
    pub fn take_body(&mut self) -> Option<F> {
        self.body.take()
    }

    /// Return the still-unstarted body and release this admission reservation.
    pub fn into_body(mut self) -> F {
        self.body.take().expect("body is present until consumed")
    }
}

impl<F> fmt::Debug for RootAdmission<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootAdmission")
            .field("queued", &self.is_queued())
            .finish_non_exhaustive()
    }
}

impl<F> Drop for RootAdmission<F> {
    fn drop(&mut self) {
        let waiters = {
            let mut state = lock(&self.inner.state);
            let mut lease = lock(&self.lease);
            if lease.released {
                return;
            }
            lease.released = true;
            if lease.active {
                lease.active = false;
                state.active = state.active.saturating_sub(1);
                promote_one(&mut state, self.inner.limits.active_roots);
            }
            if lease.queued {
                lease.queued = false;
                state.queued.retain(|entry| {
                    entry
                        .upgrade()
                        .is_some_and(|candidate| !Arc::ptr_eq(&candidate, &self.lease))
                });
            }
            if lease.result_reserved {
                lease.result_reserved = false;
                state.results = state.results.saturating_sub(1);
            }
            take_waiter_wakers(&mut state)
        };
        wake(waiters);
    }
}

/// A bounded waiter for root admission. Dropping it removes its registration;
/// after admission, dropping the returned reservation follows the ordinary root path.
pub struct AdmissionWait<F> {
    supervisor: OperationSupervisor,
    waiter_id: u64,
    body: Option<F>,
    context: Option<OperationContext>,
    deadline_signal: Option<DeadlineSignal>,
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
        if let Some(context) = &this.context {
            if let Some(signal) = &mut this.deadline_signal {
                if Pin::new(signal).poll(cx).is_ready() {
                    let error =
                        StartError::new(AdmissionError::DeadlineExpired, this.body.take().unwrap());
                    this.unregister();
                    return Poll::Ready(Err(error));
                }
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
        let mut state = lock(&self.supervisor.inner.state);
        if let Some((_, registered)) = state
            .waiters
            .iter_mut()
            .find(|(id, _)| *id == self.waiter_id)
        {
            if registered.as_ref().is_none_or(|old| !old.will_wake(waker)) {
                *registered = Some(waker.clone());
            }
        }
    }

    fn unregister(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        lock(&self.supervisor.inner.state)
            .waiters
            .retain(|(id, _)| *id != self.waiter_id);
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
        ContextError::Stopped => AdmissionError::Closed,
        ContextError::BudgetUnavailable => AdmissionError::ReservationLimit,
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

fn promote_one(state: &mut State, active_limit: usize) {
    while state.active < active_limit {
        let Some(next) = state.queued.pop_front() else {
            return;
        };
        let Some(next) = next.upgrade() else {
            continue;
        };
        let mut lease = lock(&next);
        if lease.released || !lease.queued {
            continue;
        }
        lease.queued = false;
        lease.active = true;
        state.active += 1;
        return;
    }
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
        let mut active = supervisor.try_admit().unwrap();
        let mut queued = supervisor.try_admit().unwrap();
        assert!(queued.is_queued());
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        active.complete();
        assert_eq!(supervisor.active_roots(), 1);
        assert_eq!(supervisor.queued_roots(), 0);
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        queued.take_result_slot();
        assert_eq!(supervisor.reserved_results(), 1);
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
        let mut first = supervisor.try_admit().unwrap();
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
        first.take_result_slot();
        assert_eq!(supervisor.reserved_results(), 0);
        first.complete();
        assert!(supervisor.try_admit().is_ok());
    }

    #[test]
    fn stop_and_cleanup_paths_remain_available_when_business_admission_is_full() {
        let supervisor = OperationSupervisor::new(limits(1, 0, 1));
        let _root = supervisor.try_admit().unwrap();
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
        supervisor.request_stop();
        let _cleanup = supervisor.try_reserve_cleanup().unwrap();
        assert_eq!(supervisor.stop_requests(), 1);
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

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded asynchronous root-admission waiter.

use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::Waker;
use std::fmt;

use crate::context::ContextError;
use crate::context::DeadlineSignal;
use crate::context::OperationContext;
use crate::context::StopSignal;
use crate::report::AdmissionError;
use crate::report::StartError;

use super::OperationSupervisor;
use super::RootAdmission;
use super::internal::util::lock;

/// A bounded waiter for root admission. Dropping it removes its registration;
/// after admission, dropping the returned reservation follows the ordinary root
/// path.
///
/// # Type Parameters
///
/// - `F`: The caller's body value retained until admission succeeds or fails.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationSupervisor;
///
/// async fn reserve(supervisor: &OperationSupervisor) {
///     let waiter = supervisor.start_wait_body("request").expect("waiter capacity");
///     let admission = waiter.await.expect("admission becomes available");
///     drop(admission);
/// }
/// ```
pub struct AdmissionWait<F> {
    /// Internal supervisor handle used to retry admission and remove the waiter.
    supervisor: OperationSupervisor,
    /// Registration key identifying this waiter's slot in the supervisor.
    waiter_id: u64,
    /// Body returned to the caller if admission fails before it is reserved.
    body: Option<F>,
    /// Optional context whose stop state is checked while waiting.
    context: Option<OperationContext>,
    /// Optional deadline notification registered for the context.
    deadline_signal: Option<DeadlineSignal>,
    /// Optional stop notification registered for the context.
    stop_signal: Option<StopSignal>,
    /// Prevents reuse and makes cleanup idempotent after completion.
    done: bool,
}

impl<F> fmt::Debug for AdmissionWait<F> {
    /// Formats the registration identity without exposing the stored body or context.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter receiving the debug representation.
    ///
    /// # Returns
    ///
    /// Returns the formatter's result so formatting failures propagate to the caller.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdmissionWait")
            .field("waiter_id", &self.waiter_id)
            .finish_non_exhaustive()
    }
}

impl<F> Unpin for AdmissionWait<F> {}

impl<F> Future for AdmissionWait<F> {
    /// Returns the body reservation once capacity is available or a start error.
    type Output = Result<RootAdmission<F>, StartError<F>>;

    /// Rechecks stop/deadline state, attempts admission, and parks on the bounded waiter slot.
    ///
    /// # Parameters
    ///
    /// - `self`: Pinned waiter whose body and registration are being advanced.
    /// - `cx`: Executor context used to register the current task's waker.
    ///
    /// # Returns
    ///
    /// Returns `Pending` while capacity is unavailable, or `Ready` with an admission
    /// reservation or a `StartError` that preserves the original body.
    ///
    /// # Panics
    ///
    /// Panics if polled after this waiter has already completed.
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
                    let error = StartError::new(AdmissionError::Busy, this.body.take().expect("body retained"));
                    this.unregister();
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Err(error)) => {
                    let error = StartError::new(
                        context_admission_error(this.context.as_ref().expect("signal has context"), error),
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
                let error = StartError::new(AdmissionError::DeadlineExpired, this.body.take().unwrap());
                this.unregister();
                return Poll::Ready(Err(error));
            }
            if let Err(error) = context.admission_check() {
                let error = StartError::new(context_admission_error(context, error), this.body.take().unwrap());
                this.unregister();
                return Poll::Ready(Err(error));
            }
        }
        match this.supervisor.try_admit_body(this.body.take().expect("body retained")) {
            Ok(admission) => {
                this.unregister();
                Poll::Ready(Ok(admission))
            }
            Err(error) if error.reason() == AdmissionError::Busy => {
                this.body = Some(error.into_body());
                this.register_waker(cx.waker());
                match this.supervisor.try_admit_body(this.body.take().expect("body retained")) {
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
    /// Replaces this waiter's registered waker when the executor changes it.
    ///
    /// # Parameters
    ///
    /// - `waker`: Executor waker to store for the next capacity notification.
    fn register_waker(&self, waker: &Waker) {
        let mut replacement = Some(waker.clone());
        let replaced = {
            let mut state = lock(&self.supervisor.inner.state);
            state
                .waiters
                .iter_mut()
                .find(|(id, _)| *id == self.waiter_id)
                .and_then(|(_, registered)| {
                    if registered
                        .as_ref()
                        .is_some_and(|old| old.will_wake(replacement.as_ref().expect("waker cloned")))
                    {
                        None
                    } else {
                        registered.replace(replacement.take().expect("waker cloned"))
                    }
                })
        };
        drop(replaced);
        drop(replacement);
    }

    /// Removes the waiter slot and drops signal registrations exactly once.
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
    /// Releases the bounded waiter slot when the future is abandoned.
    fn drop(&mut self) {
        self.unregister();
    }
}

/// Converts a context failure into the admission error exposed by supervisor APIs.
///
/// Timeout stops remain deadline errors, budget exhaustion maps to reservation
/// capacity, and unavailable clocks prevent safe admission.
///
/// # Parameters
///
/// - `context`: Context whose stop reason disambiguates timeout from cancellation.
/// - `error`: Context failure to translate.
///
/// # Returns
///
/// Returns the corresponding public admission error category.
pub(super) fn context_admission_error(context: &OperationContext, error: ContextError) -> AdmissionError {
    match error {
        ContextError::DeadlineExpired => AdmissionError::DeadlineExpired,
        ContextError::Stopped if context.stop_reason() == Some(crate::report::StopReason::Timeout) => {
            AdmissionError::DeadlineExpired
        }
        ContextError::Stopped => AdmissionError::Stopped,
        ContextError::BudgetUnavailable => AdmissionError::ReservationLimit,
        ContextError::WaiterCapacityExceeded => AdmissionError::Busy,
        ContextError::ClockUnavailable | ContextError::ClockDomainMismatch => AdmissionError::DriverUnavailable,
    }
}


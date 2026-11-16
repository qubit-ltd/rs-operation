// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::operation_core::OperationCore;
use crate::v02::internal::notify::Notified;
use crate::v02::protocol::Deadline;
use crate::v02::protocol::TimerRegistration;
use crate::v02::report::ObservationError;
use crate::v02::report::SettlementWait;

/// Waits for operation settlement until a deadline expires.
pub(in crate::v02::operation) struct SettlementUntil<'a> {
    /// Operation state whose settlement is observed.
    pub(in crate::v02::operation::internal) core: &'a OperationCore,
    /// Deadline that bounds the wait.
    pub(in crate::v02::operation::internal) deadline: Deadline,
    /// Registered state-change notification used to wake polling.
    pub(in crate::v02::operation::internal) notification: Option<Notified>,
    /// Clock registration used to wake polling when the deadline is reached.
    pub(in crate::v02::operation::internal) timer: Option<Box<dyn TimerRegistration>>,
    /// Waker currently associated with the clock registration.
    pub(in crate::v02::operation::internal) registered_waker: Option<Waker>,
}

impl Future for SettlementUntil<'_> {
    /// A ready settlement report, a pending snapshot at timeout, or a clock
    /// observation error.
    type Output = Result<SettlementWait, ObservationError>;

    /// Checks for settlement or timeout and registers wakeups while pending.
    ///
    /// # Parameters
    ///
    /// - `self`: The future being polled.
    /// - `context`: Task context used to register the current waker.
    ///
    /// # Returns
    ///
    /// `Ready` with the settlement result or clock error, or `Pending` until a
    ///   state change or deadline wakeup.
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if let Some(report) = self.core.settlement() {
                self.cancel_timer();
                return Poll::Ready(Ok(SettlementWait::Ready(report)));
            }
            let Some(clock) = self.core.clock.as_ref() else {
                self.cancel_timer();
                return Poll::Ready(Err(ObservationError::ClockUnavailable));
            };
            let now = clock.now();
            if now.domain() != self.deadline.time_point().domain() {
                self.cancel_timer();
                return Poll::Ready(Err(ObservationError::ClockDomainMismatch));
            }
            if now.ticks() >= self.deadline.time_point().ticks() {
                self.cancel_timer();
                return Poll::Ready(Ok(SettlementWait::Pending(self.core.snapshot())));
            }
            if self.notification.is_none() {
                let generation = self.core.changed.generation();
                if let Some(report) = self.core.settlement() {
                    self.cancel_timer();
                    return Poll::Ready(Ok(SettlementWait::Ready(report)));
                }
                self.notification = Some(self.core.changed.notified_since(generation));
            }
            let waker_changed = self
                .registered_waker
                .as_ref()
                .is_none_or(|registered| !registered.will_wake(context.waker()));
            if waker_changed {
                self.cancel_timer();
                self.registered_waker = Some(context.waker().clone());
                self.timer = Some(clock.register_deadline(self.deadline.time_point(), context.waker().clone()));
                // Registration may race the clock reaching its deadline.
                let now = clock.now();
                if now.domain() != self.deadline.time_point().domain() {
                    self.cancel_timer();
                    return Poll::Ready(Err(ObservationError::ClockDomainMismatch));
                }
                if now.ticks() >= self.deadline.time_point().ticks() {
                    self.cancel_timer();
                    return Poll::Ready(Ok(SettlementWait::Pending(self.core.snapshot())));
                }
            }

            match Pin::new(self.notification.as_mut().expect("notification was set")).poll(context) {
                Poll::Ready(()) => {
                    self.notification = None;
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl SettlementUntil<'_> {
    /// Cancels and removes the registered deadline timer, if present.
    ///
    /// # Parameters
    ///
    /// - `self`: The wait future whose timer is cancelled.
    fn cancel_timer(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.cancel();
        }
        self.registered_waker = None;
    }
}

impl Drop for SettlementUntil<'_> {
    /// Cancels the deadline timer when the wait future is dropped.
    ///
    /// # Parameters
    ///
    /// - `self`: The wait future being dropped.
    fn drop(&mut self) {
        self.cancel_timer();
    }
}

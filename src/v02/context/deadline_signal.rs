// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::StopNode;
use super::deadline_reached;
use super::fire_timeout;
use crate::v02::protocol::ClockSource;
use crate::v02::protocol::Deadline;
use crate::v02::protocol::TimerRegistration;

/// Future which requests timeout once its registered deadline is reached.
pub struct DeadlineSignal {
    /// Weak reference to the operation state that receives the timeout.
    pub(in crate::v02::context) node: Weak<StopNode>,
    /// Clock used to determine whether the deadline has been reached.
    pub(in crate::v02::context) clock: Arc<dyn ClockSource>,
    /// Deadline whose expiry requests a timeout stop.
    pub(in crate::v02::context) deadline: Deadline,
    /// Active timer registration, canceled when replaced or dropped.
    pub(in crate::v02::context) registration: Option<Box<dyn TimerRegistration>>,
    /// Waker currently associated with the timer registration.
    pub(in crate::v02::context) registered_waker: Option<Waker>,
    /// Whether expiry has already completed this signal.
    pub(in crate::v02::context) fired: bool,
}

impl Future for DeadlineSignal {
    /// The signal completes once the deadline is reached.
    type Output = ();

    /// Checks expiry and registers a timer waker while the deadline is pending.
    ///
    /// # Parameters
    ///
    /// * `cx` provides the waker registered with the clock source.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.fired {
            return Poll::Ready(());
        }
        if deadline_reached(&*self.clock, self.deadline) {
            fire_timeout(&mut self);
            return Poll::Ready(());
        }
        let changed = self
            .registered_waker
            .as_ref()
            .is_none_or(|old| !old.will_wake(cx.waker()));
        if changed {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            self.registered_waker = Some(cx.waker().clone());
            self.registration = Some(
                self.clock
                    .register_deadline(self.deadline.time_point(), cx.waker().clone()),
            );
        }
        // Close the check/register race even for a clock that advances before
        // its registration handle becomes visible to this future.
        if deadline_reached(&*self.clock, self.deadline) {
            fire_timeout(&mut self);
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

impl Drop for DeadlineSignal {
    /// Cancels the active timer registration when the signal is discarded.
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            registration.cancel();
        }
    }
}

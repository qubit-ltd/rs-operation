// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use std::sync::Arc;
use std::sync::Weak;

use super::Deadline;
use crate::protocol::ClockSource;
use crate::protocol::StopReason;
use crate::protocol::TimerRegistration;

/// Future that requests timeout when its registered deadline is reached.
///
/// An [`OperationContext::deadline_signal`](super::super::OperationContext::deadline_signal)
/// supplies this future when the context has a clock and deadline.
///
/// # Examples
///
/// An [`OperationContext`](super::super::OperationContext) supplies this
/// signal when its deadline and clock are available; await it to observe the
/// deadline.
///
/// ```
/// use qubit_operation::context::DeadlineSignal;
///
/// async fn wait_for_deadline(signal: DeadlineSignal) {
///     signal.await;
/// }
/// ```
#[must_use]
pub struct DeadlineSignal {
    /// Absolute time point whose expiry resolves this future.
    deadline: Deadline,
    /// Clock used to observe the deadline and register its wake-up.
    clock: Arc<dyn ClockSource>,
    /// Weak cancellation target; the signal does not keep the context alive.
    node: Weak<super::super::internal::Node>,
    /// Active clock registration, canceled on wake or drop.
    registration: Option<Box<dyn TimerRegistration>>,
    /// Waker used for the current registration so changed wakers can replace
    /// it.
    registered_waker: Option<core::task::Waker>,
    /// Whether the deadline has fired and the future is complete.
    fired: bool,
}

impl DeadlineSignal {
    /// Create an unregistered future for a deadline and its cancellation
    /// target.
    ///
    /// # Parameters
    ///
    /// - `deadline`: Absolute time point at which the future completes.
    /// - `clock`: Source used to read time and register a wake-up.
    /// - `node`: Weak reference to the context node that receives the timeout.
    pub(in crate::context) fn new(
        deadline: Deadline,
        clock: Arc<dyn ClockSource>,
        node: Weak<super::super::internal::Node>,
    ) -> Self {
        Self {
            deadline,
            clock,
            node,
            registration: None,
            registered_waker: None,
            fired: false,
        }
    }
}

impl Future for DeadlineSignal {
    /// The signal completes without producing a value.
    type Output = ();

    /// Poll the clock and register the current waker until the deadline fires.
    ///
    /// # Parameters
    ///
    /// - `self`: Pinned mutable access to this signal's timer state.
    /// - `cx`: Poll context whose waker is registered with the clock.
    ///
    /// # Returns
    ///
    /// Returns `Ready(())` after the deadline fires or its clock domain no
    /// longer matches. Otherwise, returns `Pending` and registers `cx`'s waker.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.fired {
            return Poll::Ready(());
        }
        let now = self.clock.now();
        if now.domain != self.deadline.at.domain || now.ticks >= self.deadline.at.ticks {
            if let Some(node) = self.node.upgrade() {
                node.request_stop(StopReason::Timeout);
            }
            self.fired = true;
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            self.registered_waker = None;
            return Poll::Ready(());
        }
        let waker_changed = self
            .registered_waker
            .as_ref()
            .is_none_or(|registered| !registered.will_wake(cx.waker()));
        if waker_changed {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            self.registered_waker = Some(cx.waker().clone());
            self.registration = Some(self.clock.register_deadline(self.deadline.at, cx.waker().clone()));
        }
        Poll::Pending
    }
}

impl Drop for DeadlineSignal {
    /// Cancel any outstanding timer registration when the future is dropped.
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            registration.cancel();
        }
    }
}

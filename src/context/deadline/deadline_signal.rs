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

use crate::protocol::ClockSource;
use crate::protocol::StopReason;
use crate::protocol::TimerRegistration;

use super::Deadline;

/// Future that requests timeout when its registered deadline is reached.
///
/// An [`OperationContext::deadline_signal`](super::super::OperationContext::deadline_signal)
/// supplies this future when the context has a clock and deadline.
///
/// # Examples
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
    /// Waker used for the current registration so changed wakers can replace it.
    registered_waker: Option<core::task::Waker>,
    /// Whether the deadline has fired and the future is complete.
    fired: bool,
}

impl DeadlineSignal {
    /// Create an unregistered future for a deadline and its cancellation target.
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
    type Output = ();

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
            self.registration = Some(
                self.clock
                    .register_deadline(self.deadline.at, cx.waker().clone()),
            );
        }
        Poll::Pending
    }
}

impl Drop for DeadlineSignal {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            registration.cancel();
        }
    }
}

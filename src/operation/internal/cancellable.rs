// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Polling wrapper for operation cancellation and deadline handling.

use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;

use crate::context::DeadlineSignal;
use crate::context::StopSignal;
use crate::report::StopReason;
use crate::scope::OperationScope;

/// Future wrapper that observes cancellation and deadline signals while
/// polling.
///
/// # Type Parameters
///
/// `F` is the operation body future. The `Future` implementation requires `F`
/// to implement `Future`.
pub(crate) struct Cancellable<F> {
    /// Pinned operation body future.
    pub(crate) future: Pin<Box<F>>,
    /// Cooperative stop signal for the operation.
    pub(crate) stop: StopSignal,
    /// Optional deadline signal for the operation.
    pub(crate) deadline: Option<DeadlineSignal>,
    /// Scope whose deadlines and child operations are supervised.
    pub(crate) scope: OperationScope,
    /// Whether cancellation has already been observed.
    pub(crate) stopped: bool,
    /// Reason recorded when cancellation or timeout is observed.
    pub(crate) stop_reason: Option<StopReason>,
}

impl<F: Future> Future for Cancellable<F> {
    /// Returns the inner output and any observed stop reason.
    type Output = (F::Output, Option<StopReason>);

    /// Polls deadlines and cancellation before and after polling the body.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the task waker used by the body and supervised signals
    ///   when they need to be polled again.
    ///
    /// # Returns
    ///
    /// `Poll::Pending` while the body is pending; otherwise,
    /// `Poll::Ready((output, reason))`. `reason` is `Some(reason)` if
    /// cancellation or a deadline was observed, and `None` otherwise.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.scope.poll_deadlines(cx);
        if !self.stopped
            && let Poll::Ready(Ok(reason)) = Pin::new(&mut self.stop).poll(cx)
        {
            self.stopped = true;
            self.stop_reason = Some(reason);
            self.scope.request_cancel(reason);
        }
        if !self.stopped
            && let Some(deadline) = self.deadline.as_mut()
            && Pin::new(deadline).poll(cx).is_ready()
        {
            self.stopped = true;
            self.stop_reason = Some(StopReason::Timeout);
            self.scope.request_cancel(StopReason::Timeout);
        }
        match self.future.as_mut().poll(cx) {
            Poll::Ready(output) => Poll::Ready((output, self.stop_reason)),
            Poll::Pending => {
                self.scope.poll_deadlines(cx);
                Poll::Pending
            }
        }
    }
}

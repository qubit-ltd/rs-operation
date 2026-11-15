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

use super::StateCell;
use crate::v02::report::OperationOutcome;
use crate::v02::report::OutcomeError;

/// Async receiver for the single typed outcome.
///
/// The future keeps a borrow of the state cell and returns its outcome when
/// that value has been published.
///
/// # Type Parameters
///
/// - `T`: success value stored in the operation outcome.
/// - `E`: failure value stored in the operation outcome.
pub(crate) struct ResultFuture<'a, T, E> {
    /// State cell from which this future takes the unique outcome.
    pub(in crate::v02::internal::state) cell: &'a StateCell<T, E>,
    /// Generation-aware notification used to register the current task waker.
    pub(in crate::v02::internal::state) notification: Option<crate::v02::internal::notify::Notified>,
}

impl<T, E> Future for ResultFuture<'_, T, E> {
    /// Outcome value returned when the notification completes.
    type Output = Result<OperationOutcome<T, E>, OutcomeError>;

    /// Returns the outcome when ready or registers for a state change.
    ///
    /// A ready outcome is consumed from the cell. If no outcome is available,
    /// the notification tracks the observed generation so a concurrent update
    /// cannot be lost before the task is woken.
    ///
    /// # Parameters
    ///
    /// - `self`: The result future being polled.
    /// - `context`: task context used to register the waker while pending.
    ///
    /// # Returns
    ///
    /// `Poll::Ready` with the outcome or terminal error, or `Poll::Pending`
    /// until the cell changes.
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let generation = self.cell.changed.generation();
            match self.cell.take_if_ready() {
                Ok(Some(result)) => return Poll::Ready(Ok(result)),
                Err(error) => return Poll::Ready(Err(error)),
                Ok(None) => {}
            }
            if self.notification.is_none() {
                self.notification = Some(self.cell.changed.notified_since(generation));
            }
            match Pin::new(self.notification.as_mut().expect("notification was set")).poll(context) {
                Poll::Ready(()) => self.notification = None,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

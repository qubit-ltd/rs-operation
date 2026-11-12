// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The future that consumes a result when the state cell becomes ready.

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use super::notify::Notified;
use super::StateCell;
use crate::report::ObservationError;
use crate::report::OperationOutcome;

/// Future that takes the result on readiness and unregisters on drop.
pub(crate) struct ResultFuture<'a, T, E> {
    /// Shared cell whose result this future may consume once.
    cell: &'a StateCell<T, E>,
    /// Current generation waiter, dropped automatically if observation is cancelled.
    notification: Option<Notified>,
}

impl<T, E> Future for ResultFuture<'_, T, E> {
    type Output = Result<OperationOutcome<T, E>, ObservationError>;

    /// Takes a ready result or registers this future for the next state change.
    ///
    /// # Parameters
    ///
    /// - `context`: The task context used to register this future's waker.
    ///
    /// # Returns
    ///
    /// Returns `Pending` while the result is unavailable, `Ready(Ok(result))`
    /// when this future takes it, or `Ready(Err(error))` after a terminal
    /// observation failure.
    ///
    /// # Errors
    ///
    /// Returns `AlreadyTaken` or `ResultDiscarded` when the result slot cannot
    /// be consumed, or propagates an error from the notification future.
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
            let notification = self.notification.as_mut().expect("notification was set");
            match Pin::new(notification).poll(context) {
                Poll::Ready(Ok(())) => self.notification = None,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

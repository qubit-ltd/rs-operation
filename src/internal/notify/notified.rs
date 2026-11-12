// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The future returned by a notification source.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;

use super::Inner;
use super::ASYNC_WAITER_CAPACITY;
use super::lock;
use crate::report::ObservationError;

/// Future that completes after the notification generation changes.
pub(crate) struct Notified {
    /// Weak ownership avoids keeping its state cell alive.
    inner: Weak<Inner>,
    /// Generation this future waits to advance beyond.
    generation: u64,
    /// Unique registration removed when this future is dropped or completes.
    waiter_id: Option<u64>,
}

impl Future for Notified {
    type Output = Result<(), ObservationError>;

    /// Registers this future or completes when its notification generation advances.
    ///
    /// # Parameters
    ///
    /// - `context`: The task context containing the waker to register.
    ///
    /// # Returns
    ///
    /// Returns `Pending` while waiting, `Ready(Ok(()))` after a generation
    /// change, or `Ready(Err(error))` if the waiter limit has been reached.
    ///
    /// # Errors
    ///
    /// Returns `ObservationError::WaiterCapacityExceeded` when no waiter slot is
    /// available for this future.
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(inner) = self.inner.upgrade() else {
            self.waiter_id = None;
            return Poll::Ready(Ok(()));
        };
        let id = *self
            .waiter_id
            .get_or_insert_with(|| inner.next_waiter.fetch_add(1, Ordering::Relaxed));
        let mut replaced_waker = None;
        let registration = {
            let mut state = lock(&inner.state);
            if state.generation != self.generation {
                if let Some(index) = state.waiters.iter().position(|(waiter_id, _)| *waiter_id == id) {
                    replaced_waker = Some(state.waiters.swap_remove(index).1);
                }
                Ok(true)
            } else if let Some((_, existing)) = state.waiters.iter_mut().find(|(waiter_id, _)| *waiter_id == id) {
                if !existing.will_wake(context.waker()) {
                    replaced_waker = Some(std::mem::replace(existing, context.waker().clone()));
                }
                Ok(false)
            } else {
                if state.waiters.len() >= ASYNC_WAITER_CAPACITY {
                    Err(ObservationError::WaiterCapacityExceeded)
                } else {
                    state.waiters.push((id, context.waker().clone()));
                    Ok(false)
                }
            }
        };
        drop(replaced_waker);
        match registration {
            Ok(true) => {
                self.waiter_id = None;
                Poll::Ready(Ok(()))
            }
            Ok(false) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl Drop for Notified {
    fn drop(&mut self) {
        let Some(id) = self.waiter_id.take() else {
            return;
        };
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let removed = {
            let mut state = lock(&inner.state);
            state
                .waiters
                .iter()
                .position(|(waiter_id, _)| *waiter_id == id)
                .map(|index| state.waiters.swap_remove(index).1)
        };
        drop(removed);
    }
}

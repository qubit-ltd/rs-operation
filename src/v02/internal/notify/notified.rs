// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use super::Inner;
use super::lock;

/// Future that completes after the notification generation changes.
pub(crate) struct Notified {
    /// Notification state observed without keeping its owner alive.
    pub(in crate::v02::internal::notify) inner: Weak<Inner>,
    /// Generation captured when this future was created.
    pub(in crate::v02::internal::notify) generation: u64,
    /// Registration identifier used to remove this task's waker on completion.
    pub(in crate::v02::internal::notify) waiter_id: Option<u64>,
}

impl Future for Notified {
    /// Notification carries no payload; generation changes only signal
    /// readiness.
    type Output = ();

    /// Registers the current waker until the notification generation changes.
    ///
    /// Waker callbacks are cloned and dropped outside the waiter-list lock.
    ///
    /// # Parameters
    ///
    /// * `context` provides the current task's waker for notification.
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(inner) = self.inner.upgrade() else {
            self.waiter_id = None;
            return Poll::Ready(());
        };
        let id = *self
            .waiter_id
            .get_or_insert_with(|| inner.next_waiter.fetch_add(1, Ordering::Relaxed));
        // Waker clone can invoke a user-defined RawWaker callback, so it must
        // not run while the waiter-list lock is held.
        let mut new_waker = Some(context.waker().clone());
        let mut removed_waker = None;
        let changed = {
            let mut state = lock(&inner.state);
            if state.generation != self.generation {
                if let Some(index) = state.waiters.iter().position(|(waiter_id, _)| *waiter_id == id) {
                    removed_waker = Some(state.waiters.swap_remove(index).1);
                }
                true
            } else if let Some((_, existing)) = state.waiters.iter_mut().find(|(waiter_id, _)| *waiter_id == id) {
                if !existing.will_wake(new_waker.as_ref().expect("new waker is present")) {
                    removed_waker = Some(std::mem::replace(
                        existing,
                        new_waker.take().expect("new waker is present"),
                    ));
                }
                false
            } else {
                state
                    .waiters
                    .push((id, new_waker.take().expect("new waker is present")));
                false
            }
        };
        // Waker drop can also run user-defined code.
        drop(removed_waker);
        drop(new_waker);
        if changed {
            self.waiter_id = None;
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Notified {
    /// Removes a pending waiter registration and drops its waker outside the
    /// lock.
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

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Execution-capacity token retained by one running operation task.

use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use super::Inner;
use super::LeaseState;
use super::leases::release_execution;
use super::util::lock;

/// Capacity token retained by the running task until it finishes or is dropped.
pub(crate) struct TaskLease {
    /// Shared supervisor state used to release capacity and notify waiters.
    pub(in crate::supervisor) inner: Arc<Inner>,
    /// Shared reservation state whose active execution slot this lease
    /// controls.
    pub(in crate::supervisor) lease: Arc<Mutex<LeaseState>>,
}

impl TaskLease {
    /// Marks task completion and releases its execution capacity.
    ///
    /// Consuming the lease runs its drop handler, which performs the idempotent
    /// release and wakes tasks waiting for capacity.
    pub(crate) fn complete(self) {}

    /// Poll until this reservation owns an active root execution slot.
    ///
    /// # Parameters
    ///
    /// - `cx`: task context used to register the current waker while capacity
    ///   is unavailable.
    ///
    /// # Returns
    ///
    /// Returns `Ready(true)` when the reservation is active, `Ready(false)` if
    /// its execution capacity has been released, or `Pending` while it waits.
    pub(crate) fn poll_active(&self, cx: &mut Context<'_>) -> Poll<bool> {
        let mut lease = lock(&self.lease);
        if lease.active {
            return Poll::Ready(true);
        }
        if lease.execution_released {
            return Poll::Ready(false);
        }
        if !lease.active_waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
            lease.active_waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for TaskLease {
    /// Releases this reservation's execution capacity when its owner is
    /// dropped.
    ///
    /// The release operation is idempotent, so explicit completion and
    /// cancellation can safely converge on this cleanup path.
    fn drop(&mut self) {
        release_execution(&self.inner, &self.lease);
    }
}

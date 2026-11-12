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
    pub(in crate::supervisor) inner: Arc<Inner>,
    pub(in crate::supervisor) lease: Arc<Mutex<LeaseState>>,
}

impl TaskLease {
    /// Mark task completion and release execution capacity.
    pub(crate) fn complete(self) {}

    /// Poll until this reservation owns an active root execution slot.
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
    fn drop(&mut self) {
        release_execution(&self.inner, &self.lease);
    }
}


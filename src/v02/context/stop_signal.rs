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
use std::task::Context;
use std::task::Poll;

use super::StopNode;
use super::lock;
use crate::v02::report::StopReason;

/// Future resolving when the context accepts its first stop request.
///
/// # Examples
///
/// ```
/// use qubit_operation::{OperationContext, StopReason};
///
/// async fn wait_for_stop(context: &OperationContext) -> StopReason {
///     context.stop_requested().await
/// }
/// ```
pub struct StopSignal {
    /// Context stop node whose first reason resolves this future.
    pub(super) node: Arc<StopNode>,
    /// Identifier of the waker registered in the node, if polling is pending.
    pub(super) waiter: Option<u64>,
    /// Whether this future has already resolved.
    pub(super) completed: bool,
}

impl Future for StopSignal {
    /// The first accepted stop reason for the context tree.
    type Output = StopReason;

    /// Resolves with the stop reason or registers the current task's waker.
    ///
    /// # Parameters
    ///
    /// * `cx` provides the waker registered while the stop reason is pending.
    ///
    /// # Panics
    ///
    /// Panics if this signal is polled after it has returned `Poll::Ready`.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        assert!(!this.completed, "stop signal polled after completion");
        let node = Arc::clone(&this.node);
        let mut replacement = Some(cx.waker().clone());
        let mut removed = None;
        let mut result = None;
        {
            let mut state = lock(&node.state);
            if let Some(reason) = state.reason {
                if let Some(id) = this.waiter.take()
                    && let Some(index) = state.waiters.iter().position(|(candidate, _)| *candidate == id)
                {
                    let (_, waker) = state.waiters.swap_remove(index);
                    removed = Some(waker);
                }
                result = Some(reason);
            } else if let Some(id) = this.waiter {
                if let Some((_, registered)) = state.waiters.iter_mut().find(|(candidate, _)| *candidate == id) {
                    if !registered.will_wake(replacement.as_ref().expect("replacement is present")) {
                        removed = Some(std::mem::replace(
                            registered,
                            replacement.take().expect("replacement is present"),
                        ));
                    }
                } else {
                    result = Some(StopReason::ParentStopped);
                }
            } else {
                let id = state.next_waiter;
                state.next_waiter = state.next_waiter.wrapping_add(1).max(1);
                state
                    .waiters
                    .push((id, replacement.take().expect("replacement is present")));
                this.waiter = Some(id);
            }
        }
        // A user-provided RawWaker can run arbitrary code in clone/drop. Keep
        // both callbacks outside the context state lock.
        drop(replacement);
        drop(removed);
        if let Some(reason) = result {
            this.completed = true;
            Poll::Ready(reason)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for StopSignal {
    /// Removes this future's waker from the stop node if it is still pending.
    fn drop(&mut self) {
        let removed = if let Some(id) = self.waiter.take() {
            let mut state = lock(&self.node.state);
            if let Some(index) = state.waiters.iter().position(|(candidate, _)| *candidate == id) {
                let (_, waker) = state.waiters.swap_remove(index);
                Some(waker)
            } else {
                None
            }
        } else {
            None
        };
        drop(removed);
    }
}

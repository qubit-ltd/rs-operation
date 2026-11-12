// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::future::Future;
use core::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::protocol::StopReason;

use super::ContextError;
use super::internal::Node;

/// Maximum number of stop observers registered on one context node.
const STOP_WAITER_CAPACITY: usize = 64;

/// Future that resolves with this context's first stop reason.
///
/// An [`OperationContext::stop_signal`](super::OperationContext::stop_signal)
/// supplies this future to observe a context's cancellation request.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::{ContextError, StopSignal};
/// use qubit_operation::protocol::StopReason;
///
/// async fn wait_for_stop(signal: StopSignal) -> Result<StopReason, ContextError> {
///     signal.await
/// }
/// ```
///
/// # Panics
///
/// Polling the future again after it has returned a ready result panics.
#[must_use]
pub struct StopSignal {
    /// Cancellation node whose stop cause this future observes.
    node: Arc<Node>,
    /// Registry identifier assigned while this future is awaiting a stop.
    waiter_id: Option<u64>,
    /// Whether the future has returned a terminal result and must not be polled again.
    completed: bool,
}

impl StopSignal {
    /// Create a signal tied to the operation context's cancellation node.
    #[inline]
    pub(super) fn new(node: Arc<Node>) -> Self {
        Self {
            node,
            waiter_id: None,
            completed: false,
        }
    }
}

impl Unpin for StopSignal {}

impl Future for StopSignal {
    type Output = Result<StopReason, ContextError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.completed {
            panic!("stop signal polled after completion");
        }
        let replacement = cx.waker().clone();
        let (result, old_waker, unused_waker) = {
            let mut state = lock(&this.node.state);
            if let Some(reason) = state.stop_reason {
                let old_waker = this
                    .waiter_id
                    .take()
                    .and_then(|id| {
                        state
                            .stop_waiters
                            .iter()
                            .position(|(candidate, _)| *candidate == id)
                    })
                    .map(|index| state.stop_waiters.swap_remove(index).1);
                (Some(Ok(reason)), old_waker, Some(replacement))
            } else if let Some(id) = this.waiter_id {
                let Some((_, stored)) = state
                    .stop_waiters
                    .iter_mut()
                    .find(|(candidate, _)| *candidate == id)
                else {
                    this.completed = true;
                    return Poll::Ready(Err(ContextError::Stopped));
                };
                if stored.will_wake(&replacement) {
                    (None, None, Some(replacement))
                } else {
                    (None, Some(std::mem::replace(stored, replacement)), None)
                }
            } else if state.stop_waiters.len() >= STOP_WAITER_CAPACITY {
                this.completed = true;
                (
                    Some(Err(ContextError::WaiterCapacityExceeded)),
                    None,
                    Some(replacement),
                )
            } else {
                let id = state.next_waiter;
                let Some(next) = id.checked_add(1) else {
                    this.completed = true;
                    return Poll::Ready(Err(ContextError::WaiterCapacityExceeded));
                };
                state.next_waiter = next;
                state.stop_waiters.push((id, replacement));
                this.waiter_id = Some(id);
                (None, None, None)
            }
        };
        drop(old_waker);
        drop(unused_waker);
        if let Some(result) = result {
            this.completed = true;
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for StopSignal {
    fn drop(&mut self) {
        let removed = self.waiter_id.take().and_then(|id| {
            let mut state = lock(&self.node.state);
            state
                .stop_waiters
                .iter()
                .position(|(candidate, _)| *candidate == id)
                .map(|index| state.stop_waiters.swap_remove(index).1)
        });
        drop(removed);
    }
}

/// Lock a waiter table, recovering its data if another thread poisoned the mutex.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

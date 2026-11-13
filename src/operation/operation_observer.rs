// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Non-consuming access to operation state and settlement.

use core::pin::Pin;
use core::task::Poll;
use std::sync::Arc;

use super::internal::Shared;
use super::lock;
use super::observe_until::ObserveUntil;
use super::poll_released;
use super::remove_release_waiter;
use crate::context::ContextError;
use crate::context::Deadline;
use crate::report::ObservationError;
use crate::report::ResourcePhase;
use crate::report::SettlementReport;

/// Cloneable observation access that never consumes the typed result.
///
/// # Type Parameters
///
/// `T` and `E` describe the operation's business result and error, though this
/// observer does not consume either value.
///
/// The handle's [`crate::operation::OperationHandle::observer`] method provides
/// this view.
///
/// # Examples
///
/// ```
/// use qubit_operation::operation::{OperationHandle, OperationObserver};
/// async fn wait_for_settlement<T, E>(handle: &OperationHandle<T, E>) {
///     let observer: OperationObserver<T, E> = handle.observer();
///     let report = observer.released().await;
///     drop(report);
/// }
/// ```
pub struct OperationObserver<T, E> {
    /// Shared operation state observed without taking its result.
    pub(super) shared: Arc<Shared<T, E>>,
}
impl<T, E> Clone for OperationObserver<T, E> {
    /// Clones access to the same non-consuming observation state.
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}
impl<T, E> OperationObserver<T, E> {
    /// Return the latest resource state without waiting.
    ///
    /// # Returns
    ///
    /// The current phase, or `Active` until a settlement report is published.
    #[must_use]
    #[inline]
    pub fn resource_phase(&self) -> ResourcePhase {
        lock(&self.shared.state)
            .released
            .as_ref()
            .map_or(ResourcePhase::Active, |r| r.resource_phase)
    }

    /// Observe a stable settlement report once available.
    ///
    /// # Returns
    ///
    /// The final settlement report after operation cleanup completes.
    #[must_use]
    pub async fn released(&self) -> SettlementReport {
        std::future::poll_fn(|cx| poll_released(&self.shared, cx)).await
    }
    /// Wait for a stable settlement snapshot using a blocking wait.
    ///
    /// # Errors
    ///
    /// Returns `WouldDeadlock` when called on a managed worker.
    ///
    /// # Returns
    ///
    /// The final settlement report after cleanup completes.
    pub fn released_blocking(&self) -> Result<SettlementReport, ObservationError> {
        if crate::driver::is_portable_worker_thread() || crate::driver::is_thread_executor_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        let mut state = lock(&self.shared.state);
        loop {
            if let Some(report) = &state.released {
                return Ok(report.clone());
            }
            state = self.shared.changed.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }
    /// Observe the current state until a caller-owned deadline without stopping
    /// the operation.
    ///
    /// # Parameters
    ///
    /// `deadline` bounds this observation only; it does not cancel the
    /// operation.
    ///
    /// # Returns
    ///
    /// The latest snapshot and a live observer, even if the deadline expires.
    ///
    /// # Errors
    ///
    /// Returns an observation error if the deadline uses an unavailable or
    /// incompatible clock domain.
    pub async fn observe_until(&self, deadline: Deadline) -> Result<ObserveUntil<T, E>, ObservationError> {
        let mut timer = self.shared.context.signal_at(deadline).map_err(|error| match error {
            ContextError::ClockUnavailable => ObservationError::ClockUnavailable,
            ContextError::ClockDomainMismatch => ObservationError::ClockDomainMismatch,
            _ => ObservationError::InvalidObserver,
        })?;
        let snapshot = std::future::poll_fn(|cx| {
            let timed_out = Pin::new(&mut timer).poll(cx).is_ready();
            let mut candidate = Some(cx.waker().clone());
            let (snapshot, remove_waiter) = {
                let mut state = lock(&self.shared.state);
                if let Some(report) = &state.released {
                    (Some(super::observer_snapshot(&state, Some(report.clone()))), false)
                } else {
                    if !state.release_waiters.iter().any(|w| w.will_wake(cx.waker())) {
                        state
                            .release_waiters
                            .push(candidate.take().expect("candidate waker is present"));
                    }
                    (timed_out.then(|| super::observer_snapshot(&state, None)), timed_out)
                }
            };
            drop(candidate);
            if remove_waiter {
                remove_release_waiter(&self.shared, cx.waker());
            }
            match snapshot {
                Some(snapshot) => Poll::Ready(snapshot),
                None => Poll::Pending,
            }
        })
        .await;
        Ok(ObserveUntil {
            snapshot,
            observer: self.clone(),
        })
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Unique typed result ownership for managed operations.

use std::sync::Arc;

use super::OperationObserver;
use super::internal::Shared;
use super::lock;
use super::poll_released;
use super::poll_result;
use crate::context::OperationContext;
use crate::report::ObservationError;
use crate::report::OperationOutcome;
use crate::report::SettlementReport;
use crate::report::StopReason;

/// Unique typed result consumer for one accepted operation.
///
/// # Type Parameters
///
/// `T` is the successful business result and `E` is its application error.
/// The supervisor returns this handle after accepting an operation.
///
/// # Examples
///
/// ```
/// use qubit_operation::operation::OperationHandle;
/// use qubit_operation::report::OperationOutcome;
/// use qubit_operation::scope::ScopeOptions;
/// use qubit_operation::supervisor::OperationSupervisor;
/// async fn observe(supervisor: &OperationSupervisor, options: ScopeOptions) {
///     let mut handle: OperationHandle<i32, ()> =
///         match supervisor.start(options, |_scope| async { Ok::<_, ()>(42) }) {
///             Ok(handle) => handle,
///             Err(_) => return,
///         };
///     assert!(matches!(
///         handle.result().await,
///         Ok(OperationOutcome::Succeeded(42))
///     ));
/// }
/// ```
///
/// ```compile_fail
/// use qubit_operation::operation::OperationHandle;
/// fn assert_clone<T: Clone>() {}
/// assert_clone::<OperationHandle<(), ()>>();
/// ```
pub struct OperationHandle<T, E> {
    /// Shared state used by the handle and observer clones.
    pub(crate) shared: Arc<Shared<T, E>>,
    /// Cancellation context owned by this handle.
    pub(crate) context: OperationContext,
    /// Whether dropping this handle must cancel the operation.
    pub(crate) detached: bool,
}

impl<T, E> OperationHandle<T, E> {
    /// Create a cloneable observer that cannot consume the typed result.
    ///
    /// # Returns
    ///
    /// An observer sharing this operation's state without result ownership.
    #[must_use]
    #[inline]
    pub fn observer(&self) -> OperationObserver<T, E> {
        OperationObserver {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Request cooperative cancellation. This never runs domain code inline.
    ///
    /// # Parameters
    ///
    /// `reason` is recorded as the operation's requested stop reason.
    #[inline]
    pub fn request_cancel(&self, reason: StopReason) {
        self.context.request_stop(reason);
    }

    /// Consume the typed outcome exactly once.
    ///
    /// # Errors
    ///
    /// Returns `AlreadyTaken` when consumed already or `ResultDiscarded` after
    /// detachment or handle drop.
    ///
    /// # Returns
    ///
    /// The operation's typed outcome when this handle still owns delivery.
    pub async fn result(&mut self) -> Result<OperationOutcome<T, E>, ObservationError> {
        std::future::poll_fn(|cx| poll_result(&self.shared, cx)).await
    }

    /// Consume the typed outcome using a blocking wait.
    ///
    /// # Errors
    ///
    /// Returns `WouldDeadlock` on a managed worker, or reports an already-taken
    /// or discarded result.
    ///
    /// # Returns
    ///
    /// The typed outcome after it is published.
    pub fn result_blocking(&mut self) -> Result<OperationOutcome<T, E>, ObservationError> {
        if crate::driver::is_portable_worker_thread() || crate::driver::is_thread_executor_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        let mut state = lock(&self.shared.state);
        loop {
            if state.result_taken {
                return Err(ObservationError::AlreadyTaken);
            }
            if state.result_discarded {
                return Err(ObservationError::ResultDiscarded);
            }
            if state.result.is_some() {
                state.result_taken = true;
                let result = state.result.take().expect("result checked");
                let lease = state.result_lease.take();
                drop(state);
                if let Some(lease) = lease {
                    lease.observe();
                }
                return Ok(result);
            }
            state = self.shared.changed.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Wait repeatedly for an immutable resource settlement snapshot.
    ///
    /// # Returns
    ///
    /// The final report once cleanup has completed.
    #[must_use]
    pub async fn released(&self) -> SettlementReport {
        std::future::poll_fn(|cx| poll_released(&self.shared, cx)).await
    }

    /// Wait for settlement using a blocking wait.
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

    /// Drop typed result delivery while keeping operation supervision active.
    ///
    /// # Returns
    ///
    /// An observer that remains usable after this handle is dropped.
    #[must_use]
    pub fn detach(mut self) -> OperationObserver<T, E> {
        self.detached = true;
        self.discard_result();
        self.observer()
    }

    /// Clones the context used to propagate this operation's cancellation.
    #[must_use]
    #[inline]
    pub(crate) fn cancellation_context(&self) -> OperationContext {
        self.context.clone()
    }

    /// Retains a value until resource settlement finishes.
    ///
    /// # Parameters
    ///
    /// `guard` is held until settlement is published, then dropped outside the
    /// state lock.
    pub(crate) fn hold_until_released(&self, guard: Box<dyn Send>) {
        let mut state = lock(&self.shared.state);
        if state.released.is_some() {
            drop(state);
            drop(guard);
        } else {
            state.completion_guards.push(guard);
        }
    }

    /// Releases any unobserved business result while supervision continues.
    fn discard_result(&mut self) {
        let (value, lease) = {
            let mut state = lock(&self.shared.state);
            state.result_discarded = true;
            (state.result.take(), state.result_lease.take())
        };
        drop(value);
        drop(lease);
        self.shared.changed.notify_all();
    }
}

impl<T, E> Drop for OperationHandle<T, E> {
    /// Requests cancellation and discards an unobserved result unless detached.
    fn drop(&mut self) {
        if !self.detached {
            self.context.request_stop(StopReason::UserCancelled);
            self.discard_result();
        }
    }
}

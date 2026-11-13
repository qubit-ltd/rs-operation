// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Operation result and settlement observer tied to a service request lease.

use crate::operation::OperationHandle;

/// A managed operation whose service-group lease lasts through resource
/// settlement.
///
/// # Type Parameters
///
/// - `T` is the successful operation result delivered by
///   [`result`](Self::result).
/// - `E` is the operation error delivered by [`result`](Self::result).
///
/// # Examples
///
/// A [`ServiceGroup`](crate::supervisor::ServiceGroup) produces this handle
/// from its `start` method. Once received, the outcome can be observed:
///
/// ```
/// use qubit_operation::supervisor::ServiceOperationHandle;
///
/// async fn observe_result(mut handle: ServiceOperationHandle<u8, std::io::Error>) {
///     assert!(handle.result().await.is_ok());
/// }
/// ```
pub struct ServiceOperationHandle<T, E> {
    /// Owns the operation result and keeps its service request lease until
    /// resource settlement completes.
    pub(super) inner: OperationHandle<T, E>,
}

impl<T, E> ServiceOperationHandle<T, E> {
    /// Creates an observer that can inspect settlement without consuming the
    /// operation's typed result.
    ///
    /// # Returns
    ///
    /// An observer that shares settlement state without consuming the result.
    #[must_use]
    #[inline]
    pub fn observer(&self) -> crate::operation::OperationObserver<T, E> {
        self.inner.observer()
    }

    /// Requests cooperative cancellation of the managed operation.
    ///
    /// # Parameters
    ///
    /// - `reason` describes why cancellation is requested.
    #[inline]
    pub fn request_cancel(&self, reason: crate::report::StopReason) {
        self.inner.request_cancel(reason);
    }

    /// Waits asynchronously for and consumes the operation outcome.
    ///
    /// # Returns
    ///
    /// The outcome on success, or an observation error if the result cannot be
    /// delivered.
    ///
    /// # Errors
    ///
    /// Returns an error if the result was already taken or discarded.
    #[inline]
    pub async fn result(&mut self) -> Result<crate::report::OperationOutcome<T, E>, crate::report::ObservationError> {
        self.inner.result().await
    }

    /// Blocks until the operation outcome is available and consumes it.
    ///
    /// # Returns
    ///
    /// The outcome on success, or an observation error if the result cannot be
    /// delivered.
    ///
    /// # Errors
    ///
    /// Returns an error if the result was already taken or discarded, or if
    /// blocking here could deadlock an executor worker.
    #[inline]
    pub fn result_blocking(
        &mut self,
    ) -> Result<crate::report::OperationOutcome<T, E>, crate::report::ObservationError> {
        self.inner.result_blocking()
    }

    /// Waits asynchronously for a stable report of resource settlement.
    ///
    /// # Returns
    ///
    /// The settlement report once all resources have settled.
    #[must_use]
    #[inline]
    pub async fn released(&self) -> crate::report::SettlementReport {
        self.inner.released().await
    }

    /// Blocks until a stable report of resource settlement is available.
    ///
    /// # Returns
    ///
    /// The settlement report on success.
    ///
    /// # Errors
    ///
    /// Returns an error if blocking here could deadlock an executor worker.
    #[inline]
    pub fn released_blocking(&self) -> Result<crate::report::SettlementReport, crate::report::ObservationError> {
        self.inner.released_blocking()
    }
}

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
pub struct ServiceOperationHandle<T, E> {
    inner: OperationHandle<T, E>,
}

impl<T, E> ServiceOperationHandle<T, E> {
    pub fn request_cancel(&self, reason: crate::report::StopReason) {
        self.inner.request_cancel(reason);
    }
    pub fn observer(&self) -> crate::operation::OperationObserver<T, E> {
        self.inner.observer()
    }
    pub async fn result(&mut self) -> Result<crate::report::OperationOutcome<T, E>, crate::report::ObservationError> {
        self.inner.result().await
    }
    pub fn result_blocking(
        &mut self,
    ) -> Result<crate::report::OperationOutcome<T, E>, crate::report::ObservationError> {
        self.inner.result_blocking()
    }
    pub async fn released(&self) -> crate::report::SettlementReport {
        self.inner.released().await
    }
    pub fn released_blocking(&self) -> Result<crate::report::SettlementReport, crate::report::ObservationError> {
        self.inner.released_blocking()
    }
}


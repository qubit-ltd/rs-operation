// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Independent finite resource owner scope associated with a service group.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::report::AdmissionError;
use crate::report::ResourceId;

use super::ServiceResourceError;

/// Independent finite resource owner scope associated with a service group.
pub struct ServiceResourceScope {
    scope: crate::scope::OperationScope,
    closed: Arc<std::sync::atomic::AtomicBool>,
}

impl ServiceResourceScope {
    /// Register one resource while the service group remains open.
    pub fn register_resource<C: crate::protocol::OwnedCleanup>(
        &self,
        owner: C,
    ) -> Result<ResourceId, ServiceResourceError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ServiceResourceError {
                reason: AdmissionError::Closed,
                owner: Box::new(owner),
            });
        }
        self.scope
            .register_resource(owner)
            .map_err(|error| ServiceResourceError {
                reason: AdmissionError::ReservationLimit,
                owner: error.into_owner(),
            })
    }

    /// Acquire one lease for a registered service resource.
    pub fn acquire_use(&self, id: ResourceId) -> Result<crate::resource::ResourceUse, crate::resource::ResourceError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::resource::ResourceError::ClosedForUse);
        }
        self.scope.acquire_use(id)
    }

    /// Close new resource use. Existing leases remain until returned.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.scope.close_body_resources();
        }
    }

    /// Take the one-use recovery key for a cleanup owner retained by this
    /// scope.
    pub fn take_recovery_key(&self) -> Option<crate::report::RecoveryKey<crate::report::RecoveryOwner>> {
        self.scope.take_recovery_key()
    }

    /// Recover a typed owner using the key returned by
    /// [`Self::take_recovery_key`].
    pub fn take_recovery<R: Send + 'static>(
        &self,
        key: crate::report::RecoveryKey<R>,
    ) -> Result<R, crate::report::RecoveryKey<R>> {
        self.scope.take_recovery(key)
    }
}

impl Drop for ServiceResourceScope {
    fn drop(&mut self) {
        self.close();
    }
}


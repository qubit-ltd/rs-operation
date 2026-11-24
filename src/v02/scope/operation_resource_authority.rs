// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Weak;

use super::ScopeCore;

/// Weak resource-registration authority that does not keep an operation alive.
///
/// Clone this value when a resource may be needed later, such as after passing
/// through a layer that might not perform any I/O. A reservation is acquired
/// only when [`reserve_resource`](Self::reserve_resource) is called.
#[derive(Clone)]
pub struct OperationResourceAuthority {
    /// Weak reference to the operation scope that owns the resource capacity.
    core: Weak<ScopeCore>,
}

impl OperationResourceAuthority {
    /// Builds a weak resource authority for the specified operation scope.
    ///
    /// # Parameters
    ///
    /// * `core` — the operation scope whose resource capacity may be reserved.
    ///
    /// # Returns
    ///
    /// A weak authority that does not keep the operation scope alive.
    pub(super) fn new(core: &Arc<ScopeCore>) -> Self {
        Self {
            core: Arc::downgrade(core),
        }
    }

    /// Reserves capacity and cleanup storage while the operation is active.
    ///
    /// # Errors
    ///
    /// Returns `ScopeEnded` if the operation is no longer available,
    /// `Stopped` if its lifecycle gate rejects registration, or
    /// `CapacityExceeded` if no resource slot is available.
    pub fn reserve_resource(
        &self,
    ) -> Result<crate::v02::resource::ResourceReservation, crate::v02::resource::ResourceCapacityError> {
        let Some(core) = self.core.upgrade() else {
            return Err(crate::v02::resource::ResourceCapacityError::ScopeEnded);
        };
        reserve_resource_for_core(core)
    }
}

/// Reserves scope capacity and binds the reservation to its operation.
///
/// # Parameters
///
/// * `core` — the active operation scope that owns the resource capacity.
///
/// # Returns
///
/// The reservation bound to the operation scope.
///
/// # Errors
///
/// Returns `ScopeEnded` or `Stopped` when the lifecycle gate rejects the
/// reservation, or `CapacityExceeded` when the resource scope has no free slot.
pub(super) fn reserve_resource_for_core(
    core: Arc<ScopeCore>,
) -> Result<crate::v02::resource::ResourceReservation, crate::v02::resource::ResourceCapacityError> {
    let result = core.with_resource_lifecycle(|| core.resource_scope().reserve());
    match result {
        Ok(Ok(reservation)) => Ok(reservation.bind_operation_scope(core)),
        Ok(Err(error)) => Err(error),
        Err(crate::v02::report::ResourceManageErrorKind::ScopeEnded) => {
            Err(crate::v02::resource::ResourceCapacityError::ScopeEnded)
        }
        Err(crate::v02::report::ResourceManageErrorKind::Stopped) => {
            Err(crate::v02::resource::ResourceCapacityError::Stopped)
        }
        Err(crate::v02::report::ResourceManageErrorKind::CapacityExceeded) => {
            unreachable!("lifecycle gate only rejects stop or scope end")
        }
    }
}

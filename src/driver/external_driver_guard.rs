// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::DriverLease;
use super::DriverStatus;
use super::ExternalDriverError;

/// Handle that marks its lease lost if dropped before explicit settlement.
///
/// # Examples
///
/// ```
/// use qubit_operation::driver::{DriverStatus, ExternalDriverGuard};
///
/// let guard = ExternalDriverGuard::new();
/// assert_eq!(guard.lease().status(), DriverStatus::Active);
/// guard.release().expect("the external task is still active");
/// ```
pub struct ExternalDriverGuard {
    /// Shared lifecycle state observed by the task retaining the lease.
    lease: DriverLease,
}

impl ExternalDriverGuard {
    /// Create a guard whose drop means the external driver disappeared.
    ///
    /// # Returns
    ///
    /// A guard with an active lease. Dropping it without calling
    /// [`Self::release`] marks the external driver as lost.
    #[must_use]
    pub fn new() -> Self {
        Self {
            lease: DriverLease::new(),
        }
    }

    /// Clone the observation lease retained by a managed task.
    ///
    /// # Returns
    ///
    /// A lease sharing this guard's lifecycle state.
    #[must_use]
    pub fn lease(&self) -> DriverLease {
        self.lease.clone()
    }

    /// Confirm the task was released by the runtime.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the active lease is marked released.
    ///
    /// # Errors
    ///
    /// Returns [`ExternalDriverError::AlreadySettled`] if the lease was already
    /// marked lost or released.
    pub fn release(self) -> Result<(), ExternalDriverError> {
        self.lease.mark_released()
    }
}

impl Drop for ExternalDriverGuard {
    /// Marks an unsettled lease as lost when its guard leaves scope.
    fn drop(&mut self) {
        if self.lease.status() == DriverStatus::Active {
            let _ = self.lease.mark_lost();
        }
    }
}

impl Default for ExternalDriverGuard {
    /// Creates a guard with an active external-driver lease.
    fn default() -> Self {
        Self::new()
    }
}

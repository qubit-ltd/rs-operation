// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use super::DriverStatus;
use super::ExternalDriverError;

/// Tracks whether an external driver still owns a managed operation.
///
/// The lease is shared with the adapter so lifecycle reports update the same
/// atomic state observed by the operation.
///
/// # Examples
///
/// ```
/// use qubit_operation::driver::{DriverLease, DriverStatus};
///
/// let lease = DriverLease::new();
/// assert_eq!(lease.status(), DriverStatus::Active);
/// lease.mark_released().unwrap();
/// assert!(lease.is_released());
/// ```
#[derive(Clone)]
pub struct DriverLease {
    /// Stores the shared lifecycle state using the values mapped by `status`.
    status: Arc<AtomicU8>,
}

impl DriverLease {
    /// Create an active lease for an external driver adapter.
    pub fn new() -> Self {
        Self {
            status: Arc::new(AtomicU8::new(0)),
        }
    }

    /// Read the current driver status.
    ///
    /// # Returns
    ///
    /// `Active` while the adapter owns the lease, `DriverLost` after an
    /// unconfirmed loss, or `Released` after explicit release confirmation.
    #[must_use]
    #[inline]
    pub fn status(&self) -> DriverStatus {
        match self.status.load(Ordering::Acquire) {
            1 => DriverStatus::DriverLost,
            2 => DriverStatus::Released,
            _ => DriverStatus::Active,
        }
    }

    /// Record that external execution disappeared without confirming release.
    ///
    /// # Errors
    ///
    /// Returns [`ExternalDriverError::AlreadySettled`] if the lease was
    /// previously marked lost or released.
    pub fn mark_lost(&self) -> Result<(), ExternalDriverError> {
        self.status
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| ExternalDriverError::AlreadySettled)
    }

    /// Confirm task completion and resource release from the owning runtime.
    ///
    /// # Errors
    ///
    /// Returns [`ExternalDriverError::AlreadySettled`] if the lease was
    /// previously marked lost or released.
    pub fn mark_released(&self) -> Result<(), ExternalDriverError> {
        self.status
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| ExternalDriverError::AlreadySettled)
    }

    /// Return true only after the adapter explicitly confirms release.
    #[must_use]
    #[inline]
    pub fn is_released(&self) -> bool {
        self.status() == DriverStatus::Released
    }

    /// Return true when driver loss makes completion and release unconfirmed.
    #[must_use]
    #[inline]
    pub fn is_unconfirmed(&self) -> bool {
        self.status() == DriverStatus::DriverLost
    }
}

impl Default for DriverLease {
    /// Creates a lease in the active state, matching [`DriverLease::new`].
    fn default() -> Self {
        Self::new()
    }
}

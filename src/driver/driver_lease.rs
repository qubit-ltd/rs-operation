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

/// Driver ownership observation retained by managed operations.
#[derive(Clone)]
pub struct DriverLease {
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
    pub fn status(&self) -> DriverStatus {
        match self.status.load(Ordering::Acquire) {
            1 => DriverStatus::DriverLost,
            2 => DriverStatus::Released,
            _ => DriverStatus::Active,
        }
    }

    /// Record that external execution disappeared without confirming release.
    pub fn mark_lost(&self) -> Result<(), ExternalDriverError> {
        self.status
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| ExternalDriverError::AlreadySettled)
    }

    /// Confirm task completion and resource release from the owning runtime.
    pub fn mark_released(&self) -> Result<(), ExternalDriverError> {
        self.status
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| ExternalDriverError::AlreadySettled)
    }

    /// Return true only after the adapter explicitly confirms release.
    pub fn is_released(&self) -> bool {
        self.status() == DriverStatus::Released
    }

    /// Return true when driver loss makes completion and release unconfirmed.
    pub fn is_unconfirmed(&self) -> bool {
        self.status() == DriverStatus::DriverLost
    }
}

impl Default for DriverLease {
    fn default() -> Self {
        Self::new()
    }
}

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
pub struct ExternalDriverGuard {
    lease: DriverLease,
}

impl ExternalDriverGuard {
    /// Create a guard whose drop means the external driver disappeared.
    pub fn new() -> Self {
        Self {
            lease: DriverLease::new(),
        }
    }

    /// Clone the observation lease retained by a managed task.
    pub fn lease(&self) -> DriverLease {
        self.lease.clone()
    }

    /// Confirm the task was released by the runtime.
    pub fn release(self) -> Result<(), ExternalDriverError> {
        self.lease.mark_released()
    }
}

impl Drop for ExternalDriverGuard {
    fn drop(&mut self) {
        if self.lease.status() == DriverStatus::Active {
            let _ = self.lease.mark_lost();
        }
    }
}

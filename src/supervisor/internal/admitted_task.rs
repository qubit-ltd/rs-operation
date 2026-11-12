// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Body and lifecycle leases transferred to the operation coordinator.

use super::ResultLease;
use super::TaskLease;

/// Body transferred to a driver with separately owned lifecycle leases.
pub(crate) struct AdmittedTask<F> {
    /// Work item that the driver will execute after admission succeeds.
    pub(in crate::supervisor) body: F,
    /// Lease keeping the root execution slot occupied while the body runs.
    pub(in crate::supervisor) execution: Option<TaskLease>,
    /// Lease reserving capacity for the eventual operation result.
    pub(in crate::supervisor) result: Option<ResultLease>,
}

impl<F> AdmittedTask<F> {
    /// Moves the body and both reservations to the operation coordinator.
    ///
    /// # Returns
    ///
    /// Returns the body, execution lease, and result lease as one ownership handoff.
    ///
    /// # Panics
    ///
    /// Panics if either reservation was already taken from this admitted task.
    pub(crate) fn into_parts(mut self) -> (F, TaskLease, ResultLease) {
        (
            self.body,
            self.execution.take().expect("execution lease exists"),
            self.result.take().expect("result lease exists"),
        )
    }
}


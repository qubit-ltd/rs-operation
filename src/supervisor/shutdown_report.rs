// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pending work snapshot returned by shutdown observers.

/// Counts work that still prevents a supervisor from completing shutdown.
///
/// The report is a point-in-time snapshot. A zero count in every category means
/// that the observed shutdown work has finished.
///
/// # Examples
///
/// ```
/// use qubit_operation::supervisor::ShutdownReport;
///
/// let report = ShutdownReport {
///     pending_requests: 0,
///     pending_recovery: 0,
///     pending_resources: 0,
/// };
/// assert!(report.is_complete());
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Number of group requests still holding an admission lease.
    pub pending_requests: usize,
    /// Recovery owners still retained by accepted operations.
    pub pending_recovery: usize,
    /// Service resource scopes still awaiting cleanup confirmation.
    pub pending_resources: usize,
}

impl ShutdownReport {
    /// Reports whether the snapshot contains any pending shutdown work.
    ///
    /// # Returns
    ///
    /// `true` when all request, recovery, and resource counts are zero.
    #[must_use]
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.pending_requests == 0 && self.pending_recovery == 0 && self.pending_resources == 0
    }
}

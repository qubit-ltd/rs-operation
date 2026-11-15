// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::ShutdownSnapshot;

/// Final manager shutdown snapshot.
///
/// # Examples
///
/// ```
/// use qubit_operation::{ShutdownReport, ShutdownSnapshot};
///
/// fn snapshot(report: ShutdownReport) -> ShutdownSnapshot {
///     report.snapshot
/// }
/// ```
#[must_use = "the shutdown snapshot should be inspected"]
#[derive(Clone, Debug)]
pub struct ShutdownReport {
    /// Outstanding counts observed when shutdown completed.
    pub snapshot: ShutdownSnapshot,
}

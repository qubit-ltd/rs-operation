// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::ShutdownReport;
use crate::v02::report::ShutdownSnapshot;

/// Result of waiting for manager shutdown.
///
/// # Examples
///
/// ```
/// use qubit_operation::ShutdownWait;
///
/// fn describe(wait: &ShutdownWait) -> &'static str {
///     match wait {
///         ShutdownWait::Completed(_) => "shutdown completed",
///         ShutdownWait::Pending(_) => "shutdown is pending",
///     }
/// }
/// ```
#[derive(Clone, Debug)]
pub enum ShutdownWait {
    /// Shutdown completed and produced a final report.
    Completed(#[doc = "Final report produced when all shutdown work settled."] ShutdownReport),
    /// Shutdown is still waiting on these outstanding items.
    Pending(#[doc = "Latest shutdown state while work remains outstanding."] ShutdownSnapshot),
}

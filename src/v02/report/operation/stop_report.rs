// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::protocol::MonotonicTime;
use crate::v02::report::StopReason;

/// Stop cause and time accepted by the operation protocol.
///
/// # Examples
///
/// Inspect the cause and optional acceptance time from a stop report:
///
/// ```
/// use qubit_operation::MonotonicTime;
/// use qubit_operation::{StopReason, StopReport};
///
/// fn stop_details(report: &StopReport) -> (StopReason, Option<MonotonicTime>) {
///     (report.reason, report.accepted_at)
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StopReport {
    /// Why execution was asked to stop.
    pub reason: StopReason,
    /// Acceptance time as `Some` when the clock provides it, or `None`
    /// otherwise.
    pub accepted_at: Option<MonotonicTime>,
}

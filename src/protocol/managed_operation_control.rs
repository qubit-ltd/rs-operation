// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::task::Context;
use core::task::Poll;

use crate::report::ChildBusinessSummary;
use crate::report::ChildSettlement;
use crate::report::ChildSnapshot;
use crate::report::StopReason;

/// Nonblocking, repeatably observable control for a domain operation.
///
/// # Examples
///
/// A control is supplied by the operation driver. A poller can observe its
/// business result and confirmed release using the task context supplied by
/// its executor:
///
/// ```
/// use core::task::Context;
/// use core::task::Poll;
/// use qubit_operation::protocol::ManagedOperationControl;
///
/// fn observe(control: &dyn ManagedOperationControl, context: &mut Context<'_>) {
///     if let Poll::Ready(summary) = control.poll_business(context) {
///         let _ = summary;
///         let _ = control.poll_released(context);
///     }
/// }
/// ```
pub trait ManagedOperationControl: Send + Sync {
    /// Request stop. Implementations must be nonblocking and idempotent.
    ///
    /// # Parameters
    ///
    /// * `reason` — the reason to record for the stop request.
    fn request_stop(&self, reason: StopReason);
    /// Return a bounded snapshot without performing I/O.
    ///
    /// # Returns
    ///
    /// The current bounded snapshot of the operation.
    #[must_use]
    fn snapshot(&self) -> ChildSnapshot;
    /// Observe the stable business fact without consuming a typed domain
    /// result.
    ///
    /// # Parameters
    ///
    /// * `cx` — the task context used to register for a later state change.
    ///
    /// # Returns
    ///
    /// `Ready(summary)` when the business result is stable, or `Pending` after
    /// registering interest in a later change.
    fn poll_business(&self, cx: &mut Context<'_>) -> Poll<ChildBusinessSummary>;
    /// Observe domain-confirmed release; `Ready` is never inferred from a stop
    /// request.
    ///
    /// # Parameters
    ///
    /// * `cx` — the task context used to register for confirmed release.
    ///
    /// # Returns
    ///
    /// `Ready(settlement)` after domain-confirmed release, or `Pending` while
    /// release is still outstanding.
    fn poll_released(&self, cx: &mut Context<'_>) -> Poll<ChildSettlement>;
}

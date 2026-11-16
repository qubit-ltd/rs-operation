// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use super::super::report::ChildOutcomeSummary;
use super::super::report::ChildRelease;
use super::super::report::ChildSnapshot;
use super::super::report::SettlementReport;
use super::super::report::StopReason;

/// Control port for reading and requesting independent child lifecycle facts.
///
/// Operation managers provide this port to child adapters. It exposes outcome,
/// release, and settlement as separate facts rather than retaining or returning
/// a typed result.
/// The outcome summary must describe the same operation identity and outcome
/// kind as the typed result from `PreparedExternalOperation`; its diagnostic
/// text may contain domain-specific information and is not part of that
/// consistency check. A `Pending` poll must register the supplied waker.
/// Outcome, input release, and settlement are independent facts: publishing
/// one does not imply either of the others.
///
/// # Examples
///
/// Adapter code can poll the settlement fact without blocking:
///
/// ```
/// use qubit_operation::OperationControl;
/// use qubit_operation::SettlementReport;
/// use std::sync::Arc;
/// use std::task::Context;
/// use std::task::Poll;
///
/// fn poll_child_settlement(
///     control: &dyn OperationControl,
///     context: &mut Context<'_>,
/// ) -> Poll<Arc<SettlementReport>> {
///     control.poll_settlement(context)
/// }
/// ```
pub trait OperationControl: Send + Sync {
    /// Requests that the child stop for the specified reason.
    ///
    /// # Parameters
    ///
    /// - `self`: Child control port receiving the request.
    /// - `reason`: reason to attach to the stop request.
    fn request_stop(&self, reason: StopReason);

    /// Returns the latest untyped snapshot of the child's lifecycle state.
    ///
    /// # Parameters
    ///
    /// - `self`: Child control port whose state is observed.
    ///
    /// # Returns
    ///
    /// A copy of the latest execution phase, stop, outcome, input-release, and
    /// settlement facts published for the child.
    #[must_use]
    fn snapshot(&self) -> ChildSnapshot;

    /// Polls for the child's outcome summary without taking its typed outcome.
    ///
    /// # Parameters
    ///
    /// - `self`: Child control port whose outcome is observed.
    /// - `cx`: task context used to register for changes while the summary is
    ///   unavailable.
    ///
    /// # Returns
    ///
    /// `Poll::Pending` until the summary is available, then `Poll::Ready` with
    /// the summary.
    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary>;

    /// Polls for the child's resource-release fact independently of its
    /// settlement.
    ///
    /// # Parameters
    ///
    /// - `self`: Child control port whose resource release is observed.
    /// - `cx`: task context used to register for changes while release is
    ///   pending.
    ///
    /// # Returns
    ///
    /// `Poll::Pending` until release is recorded, then `Poll::Ready` with its
    /// summary.
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease>;

    /// Polls for the child's final settlement report.
    ///
    /// # Parameters
    ///
    /// - `self`: Child control port whose settlement is observed.
    /// - `cx`: task context used to register for changes while settlement is
    ///   pending.
    ///
    /// # Returns
    ///
    /// `Poll::Pending` until settlement is published, then `Poll::Ready` with a
    /// shared reference-counted report.
    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>>;
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::protocol::MonotonicTime;
use crate::v02::report::OperationId;

/// Confirmation that a child released the input held by its parent.
///
/// Operation adapters publish this fact independently from the child's outcome
/// and settlement. The manager exposes it through
/// `OperationControl::poll_release`.
///
/// # Examples
///
/// Poll the release fact and preserve the optional timestamp:
///
/// ```
/// use qubit_operation::OperationControl;
/// use qubit_operation::ChildRelease;
/// use std::task::Context;
/// use std::task::Poll;
///
/// fn poll_child_release(
///     control: &dyn OperationControl,
///     context: &mut Context<'_>,
/// ) -> Poll<ChildRelease> {
///     control.poll_release(context)
/// }
///
/// fn confirmed_at(release: &ChildRelease) -> Option<qubit_operation::MonotonicTime> {
///     release.confirmed_at
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ChildRelease {
    /// Identity of the child operation whose input was released.
    pub operation_id: OperationId,
    /// Confirmation time in the configured clock domain, if available.
    pub confirmed_at: Option<MonotonicTime>,
}

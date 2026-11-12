// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::report::ClockDomainId;

/// Two monotonic instants cannot be ordered because they use different clocks.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::ClockDomainMismatch;
/// use qubit_operation::report::ClockDomainId;
///
/// let mismatch = ClockDomainMismatch {
///     left: ClockDomainId(1),
///     right: ClockDomainId(2),
/// };
/// assert_ne!(mismatch.left, mismatch.right);
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClockDomainMismatch {
    /// Domain of the left-hand instant.
    pub left: ClockDomainId,
    /// Domain of the right-hand instant.
    pub right: ClockDomainId,
}

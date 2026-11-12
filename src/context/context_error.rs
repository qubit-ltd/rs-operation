// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::fmt;

/// Context creation or admission failed.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::ContextError;
///
/// let error = ContextError::Stopped;
/// assert_eq!(error.to_string(), "operation context has been stopped");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextError {
    /// Parent and child deadlines belong to different clock domains.
    ClockDomainMismatch,
    /// A deadline requires a clock source from its domain.
    ClockUnavailable,
    /// The context has already received a stop request.
    Stopped,
    /// Its absolute deadline has elapsed.
    DeadlineExpired,
    /// The caller-owned budget lease rejected work.
    BudgetUnavailable,
    /// The finite cancellation waiter table is full.
    WaiterCapacityExceeded,
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ClockDomainMismatch => "operation deadlines use different clock domains",
            Self::ClockUnavailable => "operation deadline clock is unavailable",
            Self::Stopped => "operation context has been stopped",
            Self::DeadlineExpired => "operation deadline has expired",
            Self::BudgetUnavailable => "operation budget lease is unavailable",
            Self::WaiterCapacityExceeded => "operation context waiter capacity is full",
        })
    }
}

impl std::error::Error for ContextError {}

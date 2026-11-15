// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

use crate::v02::protocol::BudgetError;

/// Failure to obtain or validate execution context for an operation.
///
/// # Examples
///
/// ```
/// use qubit_operation::ContextError;
///
/// fn describe(error: &ContextError) -> &'static str {
///     match error {
///         ContextError::ClockUnavailable => "clock unavailable",
///         ContextError::ClockDomainMismatch => "clock domain mismatch",
///         ContextError::Stopped => "stopped during setup",
///         ContextError::BudgetRejected(_) => "budget rejected",
///     }
/// }
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub enum ContextError {
    /// No clock was configured for a time-sensitive operation.
    ClockUnavailable,
    /// A supplied deadline uses another clock's domain.
    ClockDomainMismatch,
    /// The operation was stopped before context setup completed.
    Stopped,
    /// The configured budget policy rejected the operation.
    BudgetRejected(
        /// The budget policy's reason for rejecting the operation.
        BudgetError,
    ),
}

impl fmt::Display for ContextError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for ContextError {}

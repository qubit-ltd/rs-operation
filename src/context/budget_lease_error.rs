// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::error::Error;
use core::fmt;

/// The caller-owned budget lease cannot authorize further work.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::BudgetLeaseError;
///
/// let error = BudgetLeaseError;
/// assert_eq!(error.to_string(), "operation budget lease is unavailable");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetLeaseError;

impl fmt::Display for BudgetLeaseError {
    /// Writes the stable diagnostic used when a budget lease cannot authorize
    /// work.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter that receives the diagnostic text.
    ///
    /// # Returns
    ///
    /// Returns the formatter's result, including any formatting failure.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operation budget lease is unavailable")
    }
}

impl Error for BudgetLeaseError {}

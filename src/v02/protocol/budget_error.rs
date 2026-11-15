// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

use super::super::report::DiagnosticSummary;

/// Error returned when a budget policy rejects a context.
///
/// The policy supplies a diagnostic summary that explains the rejection.
/// The value is returned by
/// [`BudgetPolicy::check`](crate::v02::protocol::BudgetPolicy::check).
///
/// # Examples
///
/// ```
/// use qubit_operation::{BudgetError, BudgetPolicy};
///
/// fn display_rejection(policy: &dyn BudgetPolicy) -> Result<(), String> {
///     policy.check().map_err(|error: BudgetError| error.to_string())
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BudgetError {
    /// Diagnostic details explaining why the budget policy rejected the
    /// context.
    pub summary: DiagnosticSummary,
}

impl fmt::Display for BudgetError {
    /// Formats the diagnostic summary supplied by the rejecting policy.
    ///
    /// # Parameters
    ///
    /// - `self`: Budget rejection whose summary is formatted.
    /// - `f`: Formatter receiving the diagnostic text.
    ///
    /// # Returns
    ///
    /// The formatting result from writing the summary.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.summary.fmt(f)
    }
}
impl std::error::Error for BudgetError {}

#[cfg(test)]
mod tests {
    use super::BudgetError;
    use crate::v02::report::DiagnosticSummary;

    #[test]
    fn test_budget_error_displays_its_bounded_summary() {
        let error = BudgetError {
            summary: DiagnosticSummary::new("budget exhausted"),
        };

        assert_eq!(error.to_string(), "budget exhausted");
        assert!(std::error::Error::source(&error).is_none());
    }
}

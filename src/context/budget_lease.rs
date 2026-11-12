// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::BudgetLeaseError;

/// A borrowed policy reference for a caller-owned finite budget.
///
/// This interface intentionally does not define what the budget measures.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::{BudgetLease, BudgetLeaseError};
///
/// struct AvailableBudget;
///
/// impl BudgetLease for AvailableBudget {
///     fn check(&self) -> Result<(), BudgetLeaseError> {
///         Ok(())
///     }
/// }
///
/// let budget = AvailableBudget;
/// assert!(budget.check().is_ok());
/// ```
pub trait BudgetLease: Send + Sync {
    /// Verify that the caller-owned lease still permits this operation.
    fn check(&self) -> Result<(), BudgetLeaseError>;
}

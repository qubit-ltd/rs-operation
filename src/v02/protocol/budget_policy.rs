// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::budget_error::BudgetError;

/// Application-supplied policy check; this does not reserve capacity.
///
/// # Examples
///
/// ```
/// use qubit_operation::{BudgetError, BudgetPolicy};
///
/// struct AcceptPolicy;
///
/// impl BudgetPolicy for AcceptPolicy {
///     fn check(&self) -> Result<(), BudgetError> {
///         Ok(())
///     }
/// }
///
/// assert!(AcceptPolicy.check().is_ok());
/// ```
pub trait BudgetPolicy: Send + Sync {
    /// Checks whether the application's budget policy accepts this operation
    /// context.
    ///
    /// This check does not reserve capacity or guarantee that a later operation
    /// can be admitted.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the policy accepts the check, or a [`BudgetError`] with
    /// diagnostic details when it rejects the context.
    fn check(&self) -> Result<(), BudgetError>;
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::super::BudgetLease;
use super::super::BudgetLeaseError;

/// Requires both inherited caller-owned budget policies to permit work.
pub(in crate::context) struct CombinedBudgetLease {
    /// Parent policy that was inherited by the child operation.
    pub(in crate::context) parent: Arc<dyn BudgetLease>,
    /// Additional policy supplied by the child operation.
    pub(in crate::context) child: Arc<dyn BudgetLease>,
}

impl BudgetLease for CombinedBudgetLease {
    /// Checks the inherited parent policy before the child policy.
    ///
    /// The child policy is checked only when the parent still permits work.
    ///
    /// # Errors
    ///
    /// Returns the first [`BudgetLeaseError`] reported by either policy.
    fn check(&self) -> Result<(), BudgetLeaseError> {
        self.parent.check()?;
        self.child.check()
    }
}

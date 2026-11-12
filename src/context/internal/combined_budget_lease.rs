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
pub(super) struct CombinedBudgetLease {
    /// Parent policy that was inherited by the child operation.
    pub(in crate::context) parent: Arc<dyn BudgetLease>,
    /// Additional policy supplied by the child operation.
    pub(in crate::context) child: Arc<dyn BudgetLease>,
}

impl BudgetLease for CombinedBudgetLease {
    fn check(&self) -> Result<(), BudgetLeaseError> {
        self.parent.check()?;
        self.child.check()
    }
}

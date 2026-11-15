// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::OutcomeDelivery;

/// Capacity permits whose release points follow operation lifecycle facts.
pub(crate) trait SubmissionLease: Send + Sync + 'static {
    /// Releases execution capacity after the body stops using its inputs.
    fn execution_finished(&self);
    /// Releases settlement capacity after the operation tree and resources
    /// settle.
    fn settlement_finished(&self);
    /// Releases outcome capacity according to how the result was observed.
    ///
    /// # Parameters
    ///
    /// - `delivery`: Describes how the operation's outcome was consumed.
    fn outcome_delivered(&self, delivery: OutcomeDelivery);
}

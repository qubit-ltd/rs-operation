// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Weak;

use super::ScopeCore;
use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ChildRequirement;
use crate::v02::report::SettlementReport;

/// Tracks a child operation until its outcome, settlement, and input release
/// arrive.
pub(super) struct ChildRecord {
    /// Child scope retained weakly for stop propagation.
    pub(super) scope: Weak<ScopeCore>,
    /// Whether a child failure affects the parent's outcome.
    pub(super) requirement: ChildRequirement,
    /// First observed child outcome summary.
    pub(super) outcome: Option<ChildOutcomeSummary>,
    /// Child resource settlement, independent from its outcome.
    pub(super) settlement: Option<Arc<SettlementReport>>,
    /// Whether the child confirmed release of parent-held inputs.
    pub(super) input_released: bool,
}

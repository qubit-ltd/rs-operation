// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Intermediate child facts accumulated while a scope is being joined.

use crate::report::ChildBusinessSummary;
use crate::report::ChildSettlement;

/// Business and release facts collected from registered children.
#[derive(Default)]
pub(crate) struct JoinFacts {
    /// First observed failed required-child summary, if any.
    pub(crate) failed_required: Option<ChildBusinessSummary>,
    /// Business summaries already observed during polling.
    pub(crate) business: Vec<ChildBusinessSummary>,
    /// Release settlements already observed during polling.
    pub(crate) settlements: Vec<ChildSettlement>,
}

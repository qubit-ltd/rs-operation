// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Business outcomes and settlement facts collected when a scope joins.

use crate::report::ChildBusinessSummary;
use crate::report::ChildSettlement;

/// Aggregate of committed child operations after business and release observation.
#[derive(Clone, Debug, Default)]
pub struct ScopeJoinReport {
    /// Settlement records for children whose resources have been released.
    pub settlements: Vec<ChildSettlement>,
    /// Business summaries for required children that failed.
    pub required_failures: Vec<ChildBusinessSummary>,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::BudgetLease;
use super::Deadline;
use crate::protocol::ClockSource;

/// Optional inherited inputs when creating a root or child operation context.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::ContextOptions;
///
/// let options = ContextOptions::default();
/// assert!(options.clock.is_none());
/// ```
#[derive(Clone, Default)]
pub struct ContextOptions {
    /// Clock used to evaluate and signal deadlines.
    pub clock: Option<Arc<dyn ClockSource>>,
    /// Absolute deadline; it must use the selected clock's domain.
    pub deadline: Option<Deadline>,
    /// Optional caller-owned budget policy; no measurement semantics are
    /// imposed.
    pub budget: Option<Arc<dyn BudgetLease>>,
}

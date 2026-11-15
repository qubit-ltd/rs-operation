// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::protocol::BudgetPolicy;
use crate::v02::protocol::ClockSource;
use crate::v02::protocol::Deadline;

/// Optional clock, deadline, and budget policy supplied to an operation.
///
/// # Examples
///
/// ```
/// use qubit_operation::ContextOptions;
///
/// let context = ContextOptions::default();
/// assert!(context.deadline.is_none());
/// ```
#[derive(Clone, Default)]
pub struct ContextOptions {
    /// Clock used to interpret deadlines and timestamps.
    pub clock: Option<Arc<dyn ClockSource>>,
    /// Latest accepted completion time, interpreted using `clock`.
    pub deadline: Option<Deadline>,
    /// Policy consulted before the operation begins work.
    pub budget: Option<Arc<dyn BudgetPolicy>>,
}

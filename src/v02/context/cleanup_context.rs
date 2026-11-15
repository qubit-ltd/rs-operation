// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::validate_clock;
use crate::v02::protocol::BudgetPolicy;
use crate::v02::protocol::ClockSource;
use crate::v02::protocol::Deadline;
use crate::v02::report::ContextError;
use crate::v02::report::ContextOptions;

/// Independent cleanup authority with its own optional deadline and policy.
///
/// # Examples
///
/// ```
/// use qubit_operation::{CleanupContext, ContextOptions};
///
/// let cleanup = CleanupContext::new(ContextOptions::default()).unwrap();
/// cleanup.check().unwrap();
/// ```
pub struct CleanupContext {
    /// Clock used to check the cleanup deadline.
    clock: Option<Arc<dyn ClockSource>>,
    /// Latest time at which cleanup work is allowed to proceed.
    deadline: Option<Deadline>,
    /// Cleanup-specific budget policies checked by this context.
    policies: Vec<Arc<dyn BudgetPolicy>>,
}

impl CleanupContext {
    /// Constructs a cleanup context without inheriting business context state.
    ///
    /// # Parameters
    ///
    /// * `options` provides the cleanup clock, deadline, and budget policy.
    pub fn new(options: ContextOptions) -> Result<Self, ContextError> {
        validate_clock(options.clock.as_deref(), options.deadline)?;
        Ok(Self {
            clock: options.clock,
            deadline: options.deadline,
            policies: options.budget.into_iter().collect(),
        })
    }

    /// Checks only the cleanup context's independent deadline and policies.
    ///
    /// # Parameters
    ///
    /// * `self` is the cleanup context whose deadline and policies are checked.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the cleanup deadline and policies permit work to proceed.
    ///
    /// # Errors
    ///
    /// Returns `ContextError::Stopped` if the deadline has elapsed or its clock
    /// domain differs, or `ContextError::BudgetRejected` if a policy rejects
    /// the work.
    pub fn check(&self) -> Result<(), ContextError> {
        if let (Some(clock), Some(deadline)) = (self.clock.as_deref(), self.deadline) {
            let now = clock.now();
            if now.domain() != deadline.time_point().domain() || now.ticks() >= deadline.time_point().ticks() {
                return Err(ContextError::Stopped);
            }
        }
        let mut first_error = None;
        for policy in &self.policies {
            if let Err(error) = policy.check()
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), |error| Err(ContextError::BudgetRejected(error)))
    }
}

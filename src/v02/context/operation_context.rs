// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::CleanupContext;
use super::DeadlineSignal;
use super::StopNode;
use super::StopSignal;
use super::lock;
use super::validate_clock;
use crate::v02::protocol::BudgetPolicy;
use crate::v02::protocol::ClockSource;
use crate::v02::protocol::Deadline;
use crate::v02::report::ContextError;
use crate::v02::report::ContextOptions;
use crate::v02::report::StopReason;

/// Shared stop tree and inherited time and budget policy for one operation.
///
/// # Examples
///
/// ```
/// use qubit_operation::{ContextOptions, OperationContext, StopReason};
///
/// let context = OperationContext::new(ContextOptions::default())
///     .expect("default context options are valid");
/// assert!(context.request_stop(StopReason::UserCancelled));
/// assert_eq!(context.stop_reason(), Some(StopReason::UserCancelled));
/// ```
pub struct OperationContext {
    /// Node in the shared cancellation tree for this operation.
    pub(in crate::v02::context) node: Arc<StopNode>,
    /// Clock source inherited by descendants unless explicitly overridden.
    clock: Option<Arc<dyn ClockSource>>,
    /// Effective deadline, bounded by every ancestor's deadline.
    deadline: Option<Deadline>,
    /// Budget policies inherited from ancestors and this context.
    policies: Vec<Arc<dyn BudgetPolicy>>,
}

impl Clone for OperationContext {
    /// Shares the same cancellation node and clones the time/budget settings.
    ///
    /// # Parameters
    ///
    /// * `self` is the context whose shared settings are cloned.
    ///
    /// # Returns
    ///
    /// A context sharing the same stop tree.
    fn clone(&self) -> Self {
        Self {
            node: Arc::clone(&self.node),
            clock: self.clock.clone(),
            deadline: self.deadline,
            policies: self.policies.clone(),
        }
    }
}

impl OperationContext {
    /// Constructs a root context after validating its clock and deadline.
    ///
    /// # Parameters
    ///
    /// * `options` configures the root clock, deadline, and budget policy.
    ///
    /// # Returns
    ///
    /// A root operation context.
    ///
    /// # Errors
    ///
    /// Returns a context error if the clock and deadline are incompatible.
    pub fn new(options: ContextOptions) -> Result<Self, ContextError> {
        validate_clock(options.clock.as_deref(), options.deadline)?;
        Ok(Self {
            node: StopNode::new(),
            clock: options.clock,
            deadline: options.deadline,
            policies: options.budget.into_iter().collect(),
        })
    }

    /// Constructs a descendant with a deadline no later than its parent's.
    ///
    /// # Parameters
    ///
    /// * `options` optionally overrides the clock, adds a deadline, and adds a
    ///   budget policy for the child.
    /// * `self` is the parent context whose settings and stop tree are
    ///   inherited.
    ///
    /// # Returns
    ///
    /// A child context bounded by its parent's deadline and policies.
    ///
    /// # Errors
    ///
    /// Returns a context error if the child clock or deadline is incompatible
    /// with the parent.
    pub fn child(&self, options: ContextOptions) -> Result<Self, ContextError> {
        let clock = options.clock.or_else(|| self.clock.clone());
        if let (Some(parent_clock), Some(child_clock)) = (self.clock.as_deref(), clock.as_deref())
            && parent_clock.now().domain() != child_clock.now().domain()
        {
            return Err(ContextError::ClockDomainMismatch);
        }
        let deadline = match (self.deadline, options.deadline) {
            (Some(parent), Some(child)) => Some(parent.earliest(child).map_err(|_| ContextError::ClockDomainMismatch)?),
            (Some(parent), None) => Some(parent),
            (None, child) => child,
        };
        validate_clock(clock.as_deref(), deadline)?;
        let node = StopNode::new();
        let parent_stopped = {
            let mut state = lock(&self.node.state);
            if state.reason.is_some() {
                true
            } else {
                state.children.retain(|child| child.strong_count() != 0);
                state.children.push(Arc::downgrade(&node));
                false
            }
        };
        if parent_stopped {
            node.request_stop(StopReason::ParentStopped);
        }
        let mut policies = self.policies.clone();
        policies.extend(options.budget);
        Ok(Self {
            node,
            clock,
            deadline,
            policies,
        })
    }

    /// Creates an independent cleanup context from explicit cleanup options.
    ///
    /// # Parameters
    ///
    /// * `options` configures cleanup's own clock, deadline, and budget policy.
    /// * `self` is the operation context from which cleanup is created.
    ///
    /// # Returns
    ///
    /// An independent cleanup context.
    ///
    /// # Errors
    ///
    /// Returns a context error if cleanup options have an incompatible clock
    /// and deadline.
    pub fn cleanup_context(&self, options: ContextOptions) -> Result<CleanupContext, ContextError> {
        CleanupContext::new(options)
    }

    /// Returns the first accepted stop reason.
    ///
    /// # Parameters
    ///
    /// * `self` is the context whose stop reason is queried.
    ///
    /// # Returns
    ///
    /// The first accepted reason, or `None` if no stop has been requested.
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.node.reason()
    }

    /// Requests stop and propagates it to descendants exactly once.
    ///
    /// # Parameters
    ///
    /// * `reason` is the first stop reason recorded for this context tree.
    /// * `self` is the context receiving the stop request.
    ///
    /// # Returns
    ///
    /// `true` if this request was the first accepted stop request.
    pub fn request_stop(&self, reason: StopReason) -> bool {
        self.node.request_stop(reason)
    }

    /// Returns the effective deadline inherited from this context's ancestry.
    ///
    /// # Parameters
    ///
    /// * `self` is the context whose effective deadline is returned.
    ///
    /// # Returns
    ///
    /// The effective deadline, or `None` if no ancestor configured one.
    pub const fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }

    /// Clones the configured clock source for internal deadline handling.
    ///
    /// # Parameters
    ///
    /// * `self` is the context whose clock source is cloned.
    ///
    /// # Returns
    ///
    /// The configured clock handle, or `None` if no clock is configured.
    pub(crate) fn clock_source(&self) -> Option<Arc<dyn ClockSource>> {
        self.clock.clone()
    }

    /// Returns true when the configured clock has reached this context's
    /// deadline.
    ///
    /// # Parameters
    ///
    /// * `self` is the context whose deadline is checked.
    pub fn is_expired(&self) -> bool {
        match (self.clock.as_deref(), self.deadline) {
            (Some(clock), Some(deadline)) => {
                let now = clock.now();
                now.domain() == deadline.time_point().domain() && now.ticks() >= deadline.time_point().ticks()
            }
            _ => false,
        }
    }

    /// Checks stop, deadline, and every inherited policy without reserving
    /// capacity.
    ///
    /// # Parameters
    ///
    /// * `self` is the context whose stop, deadline, and policies are checked.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the context permits work to proceed.
    ///
    /// # Errors
    ///
    /// Returns `ContextError::Stopped` after stop or deadline expiry, or
    /// `ContextError::BudgetRejected` if an inherited policy rejects work.
    pub fn check(&self) -> Result<(), ContextError> {
        if self.node.reason().is_some() {
            return Err(ContextError::Stopped);
        }
        if self.is_expired() {
            self.request_stop(StopReason::Timeout);
            return Err(ContextError::Stopped);
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

    /// Returns a future which resolves with the first accepted stop reason.
    ///
    /// # Parameters
    ///
    /// * `self` is the context whose stop signal is observed.
    ///
    /// # Returns
    ///
    /// A future resolving to the first accepted stop reason.
    pub fn stop_requested(&self) -> StopSignal {
        StopSignal {
            node: Arc::clone(&self.node),
            waiter: None,
            completed: false,
        }
    }

    /// Creates a signal that registers and cancels its deadline timer.
    ///
    /// # Parameters
    ///
    /// * `self` is the context whose deadline is observed.
    ///
    /// # Returns
    ///
    /// A deadline signal if both clock and deadline are configured; otherwise
    /// `None`.
    pub fn deadline_signal(&self) -> Option<DeadlineSignal> {
        Some(DeadlineSignal {
            node: Arc::downgrade(&self.node),
            clock: Arc::clone(self.clock.as_ref()?),
            deadline: self.deadline?,
            registration: None,
            registered_waker: None,
            fired: false,
        })
    }
}

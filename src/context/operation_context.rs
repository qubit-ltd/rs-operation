// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::BudgetLease;
use super::ContextError;
use super::ContextOptions;
use super::Deadline;
use super::DeadlineSignal;
use super::StopSignal;
use super::internal::CombinedBudgetLease;
use super::internal::Node;
use crate::protocol::ClockSource;
use crate::protocol::StopReason;
use crate::report::MonotonicTime;

/// Combines inherited budget checks so both parent and child policies must
/// allow work.
///
/// When both leases exist, the resulting lease checks the parent before the
/// child. If only one exists, it is passed through; if neither exists, the
/// result is `None`.
///
/// # Parameters
///
/// - `parent`: Lease inherited from the parent context, if present.
/// - `child`: Lease supplied for the child context, if present.
///
/// # Returns
///
/// `Some` containing both leases when both are present, the present lease when
/// only one exists, or `None` when neither lease exists.
fn inherit_budget(
    parent: Option<Arc<dyn BudgetLease>>,
    child: Option<Arc<dyn BudgetLease>>,
) -> Option<Arc<dyn BudgetLease>> {
    match (parent, child) {
        (Some(parent), Some(child)) => Some(Arc::new(CombinedBudgetLease { parent, child })),
        (Some(parent), None) => Some(parent),
        (None, Some(child)) => Some(child),
        (None, None) => None,
    }
}

/// Shared operation context with a parent-to-descendant cancellation tree.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::{ContextOptions, OperationContext};
/// use qubit_operation::protocol::StopReason;
///
/// let context = OperationContext::new(ContextOptions::default());
/// assert!(context.request_stop(StopReason::UserCancelled));
/// assert_eq!(context.stop_reason(), Some(StopReason::UserCancelled));
/// ```
#[derive(Clone)]
pub struct OperationContext {
    /// Shared cancellation node used to observe and propagate stop requests.
    node: Arc<Node>,
    /// Clock domain source used to evaluate the effective deadline.
    clock: Option<Arc<dyn ClockSource>>,
    /// Effective absolute deadline inherited from this context's ancestry.
    deadline: Option<Deadline>,
    /// Caller-owned policy lease inherited alongside child contexts.
    budget: Option<Arc<dyn BudgetLease>>,
}

impl OperationContext {
    /// Create a root context.
    ///
    /// A provided deadline is validated by [`Self::admission_check`] before
    /// work begins. Child creation validates inherited and supplied deadlines.
    ///
    /// # Parameters
    ///
    /// - `options`: Optional clock, absolute deadline, and caller-owned budget.
    ///
    /// # Returns
    ///
    /// A new root context with independent cancellation state.
    #[must_use]
    pub fn new(options: ContextOptions) -> Self {
        Self {
            node: Node::new(),
            clock: options.clock,
            deadline: options.deadline,
            budget: options.budget,
        }
    }

    /// Create a descendant, inheriting the earlier of both deadlines.
    ///
    /// # Parameters
    ///
    /// - `options`: Child clock, deadline, and budget overrides.
    ///
    /// # Returns
    ///
    /// A child context whose cancellation follows this context and whose
    /// deadline is the earlier compatible parent or child deadline.
    ///
    /// # Errors
    ///
    /// Returns `ClockDomainMismatch` for incompatible clocks or deadlines,
    /// `ClockUnavailable` when a deadline has no clock, or `Stopped` when the
    /// parent state mutex cannot be locked.
    pub fn child(&self, options: ContextOptions) -> Result<Self, ContextError> {
        let clock = options.clock.or_else(|| self.clock.clone());
        let deadline = match (self.deadline, options.deadline) {
            (Some(parent), Some(child)) => Some(parent.earliest(child).map_err(|_| ContextError::ClockDomainMismatch)?),
            (Some(parent), None) => Some(parent),
            (None, child) => child,
        };
        validate_clock(clock.as_deref(), deadline)?;
        let node = Node::new();
        {
            let mut state = self.node.state.lock().map_err(|_| ContextError::Stopped)?;
            if state.stop_reason.is_some() {
                node.request_stop(StopReason::ParentStopped);
            } else {
                state.children.retain(|child| child.strong_count() > 0);
                state.children.push(Arc::downgrade(&node));
            }
        }
        Ok(Self {
            node,
            clock,
            deadline,
            budget: inherit_budget(self.budget.clone(), options.budget),
        })
    }

    /// Create an independent context for cleanup work.
    ///
    /// It does not inherit this context's cancellation, deadline, or budget.
    /// Cleanup policy must explicitly supply any independent deadline or lease.
    ///
    /// # Parameters
    ///
    /// - `options`: Clock, deadline, and budget policy for the independent
    ///   cleanup context.
    ///
    /// # Returns
    ///
    /// A root context with no cancellation or policy inheritance from `self`.
    #[must_use]
    #[inline]
    pub fn cleanup_context(options: ContextOptions) -> Self {
        Self::new(options)
    }

    /// Return the first accepted stop cause, if any.
    ///
    /// # Returns
    ///
    /// The stored first cause, or `None` if no cause is available.
    #[inline]
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.node.stop_reason()
    }

    /// Return the effective absolute deadline inherited by this context.
    ///
    /// # Returns
    ///
    /// The earliest deadline inherited from this context's ancestry, if any.
    #[inline]
    pub const fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }

    /// Whether the clock reports that this context's deadline has elapsed.
    ///
    /// # Returns
    ///
    /// `true` only when both a clock and deadline exist in the same domain and
    /// the clock has reached or passed the deadline.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        match (self.deadline, self.clock.as_ref()) {
            (Some(deadline), Some(clock)) => {
                let now = clock.now();
                now.domain == deadline.time_point().domain && now.ticks >= deadline.time_point().ticks
            }
            _ => false,
        }
    }

    /// Request stop; the first accepted cause is stable and propagates
    /// downward.
    ///
    /// # Parameters
    ///
    /// - `reason`: Cause stored if no earlier stop has been accepted.
    ///
    /// # Returns
    ///
    /// `true` when this request records the first cause; otherwise `false`.
    pub fn request_stop(&self, reason: StopReason) -> bool {
        self.node.request_stop(reason)
    }

    /// Observe the first stop request asynchronously. The node retains at most
    /// 64 waiters.
    ///
    /// # Returns
    ///
    /// A future that resolves to the first stop reason or an error if its
    /// waiter registration is no longer available or the table is full.
    #[inline]
    pub fn stop_signal(&self) -> StopSignal {
        StopSignal::new(Arc::clone(&self.node))
    }

    /// Check stop, deadline, and the optional caller-owned lease before work
    /// begins.
    ///
    /// # Returns
    ///
    /// `Ok(())` when admission is allowed.
    ///
    /// # Errors
    ///
    /// Returns `Stopped`, `ClockUnavailable`, `ClockDomainMismatch`,
    /// `DeadlineExpired`, or `BudgetUnavailable` when the corresponding
    /// admission condition fails.
    pub fn admission_check(&self) -> Result<(), ContextError> {
        if self.node.stop_reason().is_some() {
            return Err(ContextError::Stopped);
        }
        validate_clock(self.clock.as_deref(), self.deadline)?;
        if self.is_expired() {
            self.node.request_stop(StopReason::Timeout);
            return Err(ContextError::DeadlineExpired);
        }
        if let Some(budget) = &self.budget {
            budget.check().map_err(|_| ContextError::BudgetUnavailable)?;
        }
        Ok(())
    }

    /// Create a future that requests timeout when the deadline timer wakes it.
    ///
    /// # Returns
    ///
    /// A deadline signal when a deadline and matching clock are available;
    /// otherwise `None`.
    pub fn deadline_signal(&self) -> Option<DeadlineSignal> {
        let deadline = self.deadline?;
        let clock = self.clock.clone()?;
        if clock.now().domain != deadline.time_point().domain {
            return None;
        }
        Some(DeadlineSignal::new(deadline, clock, Arc::downgrade(&self.node)))
    }

    /// Register an observer-only timer in this context's clock domain.
    ///
    /// # Parameters
    ///
    /// - `deadline`: Time point to register in the context clock's domain.
    ///
    /// # Returns
    ///
    /// A signal that observes the timer without requesting stop on this
    /// context.
    ///
    /// # Errors
    ///
    /// Returns `ClockUnavailable` when no clock is configured, or
    /// `ClockDomainMismatch` when the time point uses another clock domain.
    pub(crate) fn signal_at(&self, deadline: Deadline) -> Result<DeadlineSignal, ContextError> {
        let clock = self.clock.clone().ok_or(ContextError::ClockUnavailable)?;
        if clock.now().domain != deadline.time_point().domain {
            return Err(ContextError::ClockDomainMismatch);
        }
        let observer_node = Arc::new(Node::new());
        Ok(DeadlineSignal::new(deadline, clock, Arc::downgrade(&observer_node)))
    }
}

/// Ensure a deadline has a clock and that both use the same domain.
///
/// # Parameters
///
/// - `clock`: Clock to validate when a deadline is configured.
/// - `deadline`: Optional deadline whose clock domain must match `clock`.
///
/// # Errors
///
/// Returns `ClockUnavailable` when a deadline is supplied without a clock, or
/// `ClockDomainMismatch` when their domains differ.
fn validate_clock(clock: Option<&dyn ClockSource>, deadline: Option<Deadline>) -> Result<(), ContextError> {
    if let Some(deadline) = deadline {
        let Some(clock) = clock else {
            return Err(ContextError::ClockUnavailable);
        };
        if clock.now().domain != deadline.time_point().domain {
            return Err(ContextError::ClockDomainMismatch);
        }
    }
    Ok(())
}

impl From<MonotonicTime> for Deadline {
    /// Convert a monotonic time point into an absolute deadline.
    ///
    /// The time point's clock domain is preserved unchanged.
    ///
    /// # Parameters
    ///
    /// - `at`: Monotonic time point that defines the deadline.
    ///
    /// # Returns
    ///
    /// A deadline at `at`, preserving its clock domain and tick value.
    #[inline]
    fn from(at: MonotonicTime) -> Self {
        Self::at(at)
    }
}

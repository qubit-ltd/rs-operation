//! Cancellation, deadline, and budget references shared by related operations.

#[path = "context/deadline.rs"]
mod deadline;

pub use deadline::{ClockDomainMismatch, Deadline, DeadlineSignal};

use crate::protocol::{ClockSource, StopReason};
use crate::report::MonotonicTime;
use core::fmt;
use std::sync::{Arc, Mutex, Weak};

/// A borrowed policy reference for a caller-owned finite budget.
///
/// This interface intentionally does not define what the budget measures.
pub trait BudgetLease: Send + Sync {
    /// Verify that the caller-owned lease still permits this operation.
    fn check(&self) -> Result<(), BudgetLeaseError>;
}

/// The caller-owned budget lease cannot authorize further work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetLeaseError;

impl fmt::Display for BudgetLeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("operation budget lease is unavailable")
    }
}

impl std::error::Error for BudgetLeaseError {}

/// Optional inherited inputs when creating a root or child operation context.
#[derive(Clone, Default)]
pub struct ContextOptions {
    /// Clock used to evaluate and signal deadlines.
    pub clock: Option<Arc<dyn ClockSource>>,
    /// Absolute deadline; it must use the selected clock's domain.
    pub deadline: Option<Deadline>,
    /// Optional caller-owned budget policy; no measurement semantics are imposed.
    pub budget: Option<Arc<dyn BudgetLease>>,
}

/// Context creation or admission failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextError {
    /// Parent and child deadlines belong to different clock domains.
    ClockDomainMismatch,
    /// A deadline requires a clock source from its domain.
    ClockUnavailable,
    /// The context has already received a stop request.
    Stopped,
    /// Its absolute deadline has elapsed.
    DeadlineExpired,
    /// The caller-owned budget lease rejected work.
    BudgetUnavailable,
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ClockDomainMismatch => "operation deadlines use different clock domains",
            Self::ClockUnavailable => "operation deadline clock is unavailable",
            Self::Stopped => "operation context has been stopped",
            Self::DeadlineExpired => "operation deadline has expired",
            Self::BudgetUnavailable => "operation budget lease is unavailable",
        })
    }
}

impl std::error::Error for ContextError {}

struct NodeState {
    stop_reason: Option<StopReason>,
    children: Vec<Weak<Node>>,
}

struct Node {
    state: Mutex<NodeState>,
}

impl Node {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(NodeState {
                stop_reason: None,
                children: Vec::new(),
            }),
        })
    }

    fn request_stop(&self, reason: StopReason) -> bool {
        let children = {
            let Ok(mut state) = self.state.lock() else {
                return false;
            };
            if state.stop_reason.is_some() {
                return false;
            }
            state.stop_reason = Some(reason);
            state.children.retain(|child| child.strong_count() > 0);
            state
                .children
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        for child in children {
            child.request_stop(StopReason::ParentStopped);
        }
        true
    }

    fn stop_reason(&self) -> Option<StopReason> {
        self.state.lock().ok().and_then(|state| state.stop_reason)
    }
}

/// Shared operation context with a parent-to-descendant cancellation tree.
#[derive(Clone)]
pub struct OperationContext {
    node: Arc<Node>,
    clock: Option<Arc<dyn ClockSource>>,
    deadline: Option<Deadline>,
    budget: Option<Arc<dyn BudgetLease>>,
}

impl OperationContext {
    /// Create a root context.
    ///
    /// A provided deadline is validated by [`Self::admission_check`] before
    /// work begins. Child creation validates inherited and supplied deadlines.
    pub fn new(options: ContextOptions) -> Self {
        Self {
            node: Node::new(),
            clock: options.clock,
            deadline: options.deadline,
            budget: options.budget,
        }
    }

    /// Create a descendant, inheriting the earlier of both deadlines.
    pub fn child(&self, options: ContextOptions) -> Result<Self, ContextError> {
        let clock = options.clock.or_else(|| self.clock.clone());
        let deadline = match (self.deadline, options.deadline) {
            (Some(parent), Some(child)) => Some(
                parent
                    .earliest(child)
                    .map_err(|_| ContextError::ClockDomainMismatch)?,
            ),
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
            budget: options.budget.or_else(|| self.budget.clone()),
        })
    }

    /// Create an independent context for cleanup work.
    ///
    /// It does not inherit this context's cancellation, deadline, or budget.
    /// Cleanup policy must explicitly supply any independent deadline or lease.
    pub fn cleanup_context(options: ContextOptions) -> Self {
        Self::new(options)
    }

    /// Request stop; the first accepted cause is stable and propagates downward.
    pub fn request_stop(&self, reason: StopReason) -> bool {
        self.node.request_stop(reason)
    }

    /// Return the first accepted stop cause, if any.
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.node.stop_reason()
    }

    /// Return the effective absolute deadline inherited by this context.
    pub const fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }

    /// Check stop, deadline, and the optional caller-owned lease before work begins.
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
            budget
                .check()
                .map_err(|_| ContextError::BudgetUnavailable)?;
        }
        Ok(())
    }

    /// Whether the clock reports that this context's deadline has elapsed.
    pub fn is_expired(&self) -> bool {
        match (self.deadline, self.clock.as_ref()) {
            (Some(deadline), Some(clock)) => {
                let now = clock.now();
                now.domain == deadline.time_point().domain
                    && now.ticks >= deadline.time_point().ticks
            }
            _ => false,
        }
    }

    /// Create a future that requests timeout when the deadline timer wakes it.
    pub fn deadline_signal(&self) -> Option<DeadlineSignal> {
        let deadline = self.deadline?;
        let clock = self.clock.clone()?;
        if clock.now().domain != deadline.time_point().domain {
            return None;
        }
        Some(DeadlineSignal::new(
            deadline,
            clock,
            Arc::downgrade(&self.node),
        ))
    }
}

fn validate_clock(
    clock: Option<&dyn ClockSource>,
    deadline: Option<Deadline>,
) -> Result<(), ContextError> {
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
    fn from(at: MonotonicTime) -> Self {
        Self::at(at)
    }
}

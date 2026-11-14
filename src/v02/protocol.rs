//! Shared time and budget protocols for operation contexts.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use super::report::{
    ChildOutcomeSummary, ChildRelease, ChildSnapshot, DiagnosticSummary, SettlementReport,
    StopReason,
};

static NEXT_CLOCK_DOMAIN: AtomicU64 = AtomicU64::new(1);

/// Opaque identity for one monotonic clock domain.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClockDomainId(u128);

impl ClockDomainId {
    /// Creates a process-unique clock-domain identity for an external clock.
    pub fn new_unique() -> Self {
        Self(u128::from(
            NEXT_CLOCK_DOMAIN.fetch_add(1, Ordering::Relaxed),
        ))
    }

    #[allow(dead_code)]
    pub(crate) const fn raw(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClockDomainMismatch {
    pub left: ClockDomainId,
    pub right: ClockDomainId,
}

impl fmt::Display for ClockDomainMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "clock domains differ: {:?} and {:?}",
            self.left, self.right
        )
    }
}
impl std::error::Error for ClockDomainMismatch {}

/// A monotonic tick value interpreted only within its clock domain.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MonotonicTime {
    domain: ClockDomainId,
    ticks: u64,
}

impl MonotonicTime {
    pub const fn new(domain: ClockDomainId, ticks: u64) -> Self {
        Self { domain, ticks }
    }
    pub const fn domain(self) -> ClockDomainId {
        self.domain
    }
    pub const fn ticks(self) -> u64 {
        self.ticks
    }
}

/// A deadline on a monotonic clock.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Deadline {
    at: MonotonicTime,
}

impl Deadline {
    pub const fn at(at: MonotonicTime) -> Self {
        Self { at }
    }
    pub const fn time_point(self) -> MonotonicTime {
        self.at
    }

    pub fn earliest(self, other: Self) -> Result<Self, ClockDomainMismatch> {
        if self.at.domain != other.at.domain {
            return Err(ClockDomainMismatch {
                left: self.at.domain,
                right: other.at.domain,
            });
        }
        Ok(if self.at.ticks <= other.at.ticks {
            self
        } else {
            other
        })
    }
}

/// A timer registration is an owning cancellation token.
pub trait TimerRegistration: Send {
    fn cancel(self: Box<Self>);
}

/// Error returned when a budget policy rejects a context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BudgetError {
    pub summary: DiagnosticSummary,
}

impl fmt::Display for BudgetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.summary.fmt(f)
    }
}
impl std::error::Error for BudgetError {}

/// Application-supplied policy check; this does not reserve capacity.
pub trait BudgetPolicy: Send + Sync {
    fn check(&self) -> Result<(), BudgetError>;
}

/// Monotonic clock interface used by deadlines and asynchronous waits.
pub trait ClockSource: Send + Sync {
    fn now(&self) -> MonotonicTime;
    fn register_deadline(&self, at: MonotonicTime, wake: Waker) -> Box<dyn TimerRegistration>;
}

/// Internal control port that exposes independent child lifecycle facts.
pub trait OperationControl: Send + Sync {
    fn request_stop(&self, reason: StopReason);
    fn snapshot(&self) -> ChildSnapshot;
    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary>;
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease>;
    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>>;
}

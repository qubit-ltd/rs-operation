use core::cmp::Ordering;
use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use std::sync::Arc;
use std::sync::Weak;

use crate::protocol::ClockSource;
use crate::protocol::TimerRegistration;
use crate::report::ClockDomainId;
use crate::report::MonotonicTime;
use crate::report::StopReason;

/// Two monotonic instants cannot be ordered because they use different clocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClockDomainMismatch {
    /// Domain of the left-hand instant.
    pub left: ClockDomainId,
    /// Domain of the right-hand instant.
    pub right: ClockDomainId,
}

/// Absolute deadline in one monotonic clock domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Deadline {
    at: MonotonicTime,
}

impl Deadline {
    /// Construct a deadline at a domain-local monotonic instant.
    pub const fn at(at: MonotonicTime) -> Self {
        Self { at }
    }

    /// Return the absolute time point and its clock domain.
    pub const fn time_point(self) -> MonotonicTime {
        self.at
    }

    /// Compare deadlines; cross-domain comparisons are rejected.
    pub fn compare(self, other: Self) -> Result<Ordering, ClockDomainMismatch> {
        if self.at.domain != other.at.domain {
            return Err(ClockDomainMismatch {
                left: self.at.domain,
                right: other.at.domain,
            });
        }
        Ok(self.at.ticks.cmp(&other.at.ticks))
    }

    /// Return the earlier of two deadlines in the same clock domain.
    pub fn earliest(self, other: Self) -> Result<Self, ClockDomainMismatch> {
        if self.compare(other)? == Ordering::Greater {
            Ok(other)
        } else {
            Ok(self)
        }
    }
}

/// Future that requests timeout when its registered deadline is reached.
pub struct DeadlineSignal {
    deadline: Deadline,
    clock: Arc<dyn ClockSource>,
    node: Weak<super::Node>,
    registration: Option<Box<dyn TimerRegistration>>,
    registered_waker: Option<core::task::Waker>,
    fired: bool,
}

impl DeadlineSignal {
    pub(super) fn new(deadline: Deadline, clock: Arc<dyn ClockSource>, node: Weak<super::Node>) -> Self {
        Self {
            deadline,
            clock,
            node,
            registration: None,
            registered_waker: None,
            fired: false,
        }
    }
}

impl Future for DeadlineSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.fired {
            return Poll::Ready(());
        }
        let now = self.clock.now();
        if now.domain != self.deadline.at.domain || now.ticks >= self.deadline.at.ticks {
            if let Some(node) = self.node.upgrade() {
                node.request_stop(StopReason::Timeout);
            }
            self.fired = true;
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            self.registered_waker = None;
            return Poll::Ready(());
        }
        let waker_changed = self
            .registered_waker
            .as_ref()
            .is_none_or(|registered| !registered.will_wake(cx.waker()));
        if waker_changed {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            self.registered_waker = Some(cx.waker().clone());
            self.registration = Some(self.clock.register_deadline(self.deadline.at, cx.waker().clone()));
        }
        Poll::Pending
    }
}

impl Drop for DeadlineSignal {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            registration.cancel();
        }
    }
}

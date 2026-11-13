// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

use qubit_operation::protocol::ClockSource;
use qubit_operation::protocol::TimerRegistration;
use qubit_operation::report::ClockDomainId;
use qubit_operation::report::MonotonicTime;

/// Failure to advance a manual clock to a point in its domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClockAdvanceError {
    /// The requested time belongs to another clock domain.
    WrongDomain {
        /// Domain owned by this clock.
        expected: ClockDomainId,
        /// Domain supplied by the caller.
        actual: ClockDomainId,
    },
    /// The requested tick precedes the current tick.
    TimeWentBackwards {
        /// Current tick before the attempted advance.
        current: u64,
        /// Requested target tick.
        requested: u64,
    },
}

struct ClockState {
    ticks: u64,
    registrations: BTreeMap<u64, (u64, Waker)>,
}

struct ClockInner {
    domain: ClockDomainId,
    next_registration: AtomicU64,
    state: Mutex<ClockState>,
}

/// Deterministic clock that wakes registered timers when their deadline passes.
#[derive(Clone)]
pub struct ManualClock {
    inner: Arc<ClockInner>,
}

impl ManualClock {
    /// Create a clock at tick zero in the supplied, caller-owned domain.
    #[must_use]
    pub fn new(domain: ClockDomainId) -> Self {
        Self {
            inner: Arc::new(ClockInner {
                domain,
                next_registration: AtomicU64::new(1),
                state: Mutex::new(ClockState {
                    ticks: 0,
                    registrations: BTreeMap::new(),
                }),
            }),
        }
    }

    /// Return the number of timer registrations that have not fired or been
    /// cancelled.
    #[must_use]
    #[inline]
    pub fn pending_timers(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("clock mutex poisoned")
            .registrations
            .len()
    }

    /// Advance by a number of ticks and wake every timer crossed by the
    /// advance.
    pub fn advance_by(&self, ticks: u64) -> MonotonicTime {
        let current = self.now();
        let target = current.ticks.checked_add(ticks).expect("manual clock tick overflow");
        self.advance_to(MonotonicTime::new(self.inner.domain, target))
            .expect("same-domain manual clock advance must succeed")
    }

    /// Advance to a point in this clock's domain, rejecting other domains or
    /// reversal.
    pub fn advance_to(&self, target: MonotonicTime) -> Result<MonotonicTime, ClockAdvanceError> {
        if target.domain != self.inner.domain {
            return Err(ClockAdvanceError::WrongDomain {
                expected: self.inner.domain,
                actual: target.domain,
            });
        }
        let due = {
            let mut state = self.inner.state.lock().expect("clock mutex poisoned");
            if target.ticks < state.ticks {
                return Err(ClockAdvanceError::TimeWentBackwards {
                    current: state.ticks,
                    requested: target.ticks,
                });
            }
            state.ticks = target.ticks;
            let ids = state
                .registrations
                .iter()
                .filter_map(|(id, (deadline, _))| (*deadline <= target.ticks).then_some(*id))
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| state.registrations.remove(&id).map(|(_, waker)| waker))
                .collect::<Vec<_>>()
        };
        for waker in due {
            waker.wake();
        }
        Ok(target)
    }
}

impl ClockSource for ManualClock {
    fn now(&self) -> MonotonicTime {
        let ticks = self.inner.state.lock().expect("clock mutex poisoned").ticks;
        MonotonicTime::new(self.inner.domain, ticks)
    }

    fn register_deadline(&self, at: MonotonicTime, wake: Waker) -> Box<dyn TimerRegistration> {
        assert_eq!(
            at.domain, self.inner.domain,
            "manual clock cannot register a deadline from another domain"
        );
        let id = self.inner.next_registration.fetch_add(1, Ordering::Relaxed);
        let already_due = {
            let mut state = self.inner.state.lock().expect("clock mutex poisoned");
            if at.ticks <= state.ticks {
                true
            } else {
                state.registrations.insert(id, (at.ticks, wake.clone()));
                false
            }
        };
        if already_due {
            wake.wake();
        }
        Box::new(ManualTimer {
            inner: Arc::clone(&self.inner),
            id,
        })
    }
}

struct ManualTimer {
    inner: Arc<ClockInner>,
    id: u64,
}

impl TimerRegistration for ManualTimer {
    fn cancel(self: Box<Self>) {
        self.inner
            .state
            .lock()
            .expect("clock mutex poisoned")
            .registrations
            .remove(&self.id);
    }
}

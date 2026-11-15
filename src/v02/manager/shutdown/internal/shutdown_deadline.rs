// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use crate::v02::protocol::ClockSource;
use crate::v02::protocol::Deadline;
use crate::v02::protocol::TimerRegistration;

/// Future that wakes when a shutdown deadline is reached.
#[must_use = "poll the deadline future while waiting for shutdown"]
pub(in crate::v02::manager::shutdown) struct ShutdownDeadline {
    /// Clock used to observe time and register the deadline wakeup.
    clock: Arc<dyn ClockSource>,
    /// Absolute deadline being observed.
    deadline: Deadline,
    /// Active timer registration, cancelled when completed or dropped.
    registration: Option<Box<dyn TimerRegistration>>,
    /// Last task waker registered with the clock.
    waker: Option<Waker>,
}

impl ShutdownDeadline {
    /// Creates a future that resolves when `deadline` is reached on `clock`.
    ///
    /// # Parameters
    ///
    /// - `clock`: Clock source used to compare time and register wakeups.
    /// - `deadline`: Absolute deadline to observe.
    ///
    /// # Returns
    ///
    /// A future that registers and cancels its timer as it is polled and
    /// dropped.
    #[inline]
    pub(in crate::v02::manager::shutdown) fn new(clock: Arc<dyn ClockSource>, deadline: Deadline) -> Self {
        Self {
            clock,
            deadline,
            registration: None,
            waker: None,
        }
    }
}

impl Future for ShutdownDeadline {
    /// Completes with unit when the deadline is reached.
    type Output = ();

    /// Polls the clock and registers this task for deadline notification.
    ///
    /// # Parameters
    ///
    /// * `cx` provides the task waker to register with the clock.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = self.clock.now();
        if now.domain() != self.deadline.time_point().domain() || now.ticks() >= self.deadline.time_point().ticks() {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            return Poll::Ready(());
        }
        if self.waker.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            self.waker = Some(cx.waker().clone());
            self.registration = Some(
                self.clock
                    .register_deadline(self.deadline.time_point(), cx.waker().clone()),
            );
        }
        let now = self.clock.now();
        if now.domain() != self.deadline.time_point().domain() || now.ticks() >= self.deadline.time_point().ticks() {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ShutdownDeadline {
    /// Cancels any outstanding clock registration.
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            registration.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use super::ShutdownDeadline;
    use crate::v02::protocol::ClockDomainId;
    use crate::v02::protocol::ClockSource;
    use crate::v02::protocol::Deadline;
    use crate::v02::protocol::MonotonicTime;
    use crate::v02::protocol::TimerRegistration;

    struct TestClock {
        now: Mutex<MonotonicTime>,
        registrations: AtomicUsize,
        cancellations: Arc<AtomicUsize>,
        advance_on_register: bool,
    }

    impl ClockSource for TestClock {
        fn now(&self) -> MonotonicTime {
            *self.now.lock().unwrap()
        }

        fn register_deadline(&self, at: MonotonicTime, _wake: Waker) -> Box<dyn TimerRegistration> {
            self.registrations.fetch_add(1, Ordering::SeqCst);
            if self.advance_on_register {
                *self.now.lock().unwrap() = at;
            }
            Box::new(TestRegistration(Arc::clone(&self.cancellations)))
        }
    }

    struct TestRegistration(Arc<AtomicUsize>);

    impl TimerRegistration for TestRegistration {
        fn cancel(self: Box<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct TestWaker(Arc<AtomicUsize>);

    impl Wake for TestWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn clock(now: MonotonicTime, advance_on_register: bool) -> Arc<TestClock> {
        Arc::new(TestClock {
            now: Mutex::new(now),
            registrations: AtomicUsize::new(0),
            cancellations: Arc::new(AtomicUsize::new(0)),
            advance_on_register,
        })
    }

    fn poll(future: Pin<&mut ShutdownDeadline>, waker: &Waker) -> Poll<()> {
        let mut context = Context::from_waker(waker);
        future.poll(&mut context)
    }

    #[test]
    fn test_deadline_cancels_each_timer_registration_on_drop() {
        let domain = ClockDomainId::new_unique();
        let clock = clock(MonotonicTime::new(domain, 1), false);
        let waker = Waker::from(Arc::new(TestWaker(Arc::new(AtomicUsize::new(0)))));
        let mut future = Box::pin(ShutdownDeadline::new(
            clock.clone(),
            Deadline::at(MonotonicTime::new(domain, 5)),
        ));

        assert!(poll(future.as_mut(), &waker).is_pending());
        assert!(poll(future.as_mut(), &waker).is_pending());
        let registrations = clock.registrations.load(Ordering::SeqCst);
        assert!(registrations >= 1);
        assert_eq!(clock.cancellations.load(Ordering::SeqCst), registrations - 1);
        drop(future);
        assert_eq!(clock.cancellations.load(Ordering::SeqCst), registrations);
    }

    #[test]
    fn test_deadline_replaces_registration_for_new_waker() {
        let domain = ClockDomainId::new_unique();
        let clock = clock(MonotonicTime::new(domain, 1), false);
        let first = Waker::from(Arc::new(TestWaker(Arc::new(AtomicUsize::new(0)))));
        let second = Waker::from(Arc::new(TestWaker(Arc::new(AtomicUsize::new(0)))));
        let mut future = Box::pin(ShutdownDeadline::new(
            clock.clone(),
            Deadline::at(MonotonicTime::new(domain, 5)),
        ));

        assert!(poll(future.as_mut(), &first).is_pending());
        assert!(poll(future.as_mut(), &second).is_pending());
        assert_eq!(clock.registrations.load(Ordering::SeqCst), 2);
        assert_eq!(clock.cancellations.load(Ordering::SeqCst), 1);
        drop(future);
        assert_eq!(clock.cancellations.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_deadline_completes_and_cancels_registration_when_time_advances() {
        let domain = ClockDomainId::new_unique();
        let clock = clock(MonotonicTime::new(domain, 1), true);
        let waker = Waker::from(Arc::new(TestWaker(Arc::new(AtomicUsize::new(0)))));
        let mut future = Box::pin(ShutdownDeadline::new(
            clock.clone(),
            Deadline::at(MonotonicTime::new(domain, 5)),
        ));

        assert!(poll(future.as_mut(), &waker).is_ready());
        assert_eq!(clock.registrations.load(Ordering::SeqCst), 1);
        assert_eq!(clock.cancellations.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_deadline_completes_immediately_for_elapsed_or_other_domain() {
        let domain = ClockDomainId::new_unique();
        let other_domain = ClockDomainId::new_unique();
        let waker = Waker::from(Arc::new(TestWaker(Arc::new(AtomicUsize::new(0)))));
        let elapsed_clock = clock(MonotonicTime::new(domain, 5), false);
        let mut elapsed = Box::pin(ShutdownDeadline::new(
            elapsed_clock.clone(),
            Deadline::at(MonotonicTime::new(domain, 5)),
        ));
        assert!(poll(elapsed.as_mut(), &waker).is_ready());
        assert_eq!(elapsed_clock.registrations.load(Ordering::SeqCst), 0);

        let mismatched_clock = clock(MonotonicTime::new(other_domain, 1), false);
        let mut mismatched = Box::pin(ShutdownDeadline::new(
            mismatched_clock.clone(),
            Deadline::at(MonotonicTime::new(domain, 5)),
        ));
        assert!(poll(mismatched.as_mut(), &waker).is_ready());
        assert_eq!(mismatched_clock.registrations.load(Ordering::SeqCst), 0);
    }
}

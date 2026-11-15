// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Context cancellation, deadlines, clock-domain validation, and budget checks.

mod cleanup_context;
mod deadline_signal;
mod internal;
mod operation_context;
mod stop_signal;

pub use cleanup_context::CleanupContext;
pub use deadline_signal::DeadlineSignal;
pub(in crate::v02::context) use internal::StopNode;
pub(in crate::v02::context) use internal::deadline_reached;
pub(in crate::v02::context) use internal::fire_timeout;
pub(in crate::v02::context) use internal::lock;
pub(in crate::v02::context) use internal::validate_clock;
pub use operation_context::OperationContext;
pub use stop_signal::StopSignal;

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::task::Waker;

    use super::OperationContext;
    use crate::v02::protocol::BudgetError;
    use crate::v02::protocol::BudgetPolicy;
    use crate::v02::protocol::ClockDomainId;
    use crate::v02::protocol::ClockSource;
    use crate::v02::protocol::Deadline;
    use crate::v02::protocol::MonotonicTime;
    use crate::v02::protocol::TimerRegistration;
    use crate::v02::report::ContextError;
    use crate::v02::report::ContextOptions;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::StopReason;

    struct ManualClock {
        domain: ClockDomainId,
        ticks: AtomicUsize,
        registration_wakes: AtomicUsize,
        registration_cancels: Arc<AtomicUsize>,
    }

    impl ManualClock {
        fn new(domain: ClockDomainId, ticks: usize) -> Arc<Self> {
            Arc::new(Self {
                domain,
                ticks: AtomicUsize::new(ticks),
                registration_wakes: AtomicUsize::new(0),
                registration_cancels: Arc::new(AtomicUsize::new(0)),
            })
        }
    }

    struct Registration(Arc<AtomicUsize>);

    impl TimerRegistration for Registration {
        fn cancel(self: Box<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl ClockSource for ManualClock {
        fn now(&self) -> MonotonicTime {
            MonotonicTime::new(self.domain, self.ticks.load(Ordering::SeqCst) as u64)
        }

        fn register_deadline(&self, at: MonotonicTime, wake: Waker) -> Box<dyn TimerRegistration> {
            self.registration_wakes.fetch_add(1, Ordering::SeqCst);
            // Advance between the caller's initial check and timer install.
            self.ticks.store(at.ticks() as usize, Ordering::SeqCst);
            wake.wake();
            Box::new(Registration(Arc::clone(&self.registration_cancels)))
        }
    }

    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Policy {
        calls: AtomicUsize,
        reject: bool,
    }

    struct ReentrantWakerProbe {
        context: Arc<OperationContext>,
        clone_lock_violations: AtomicUsize,
        drop_lock_violations: AtomicUsize,
        clone_callbacks: AtomicUsize,
        other_callbacks: AtomicUsize,
    }

    impl ReentrantWakerProbe {
        fn callback(&self, is_clone: bool) {
            let callbacks = if is_clone {
                &self.clone_callbacks
            } else {
                &self.other_callbacks
            };
            callbacks.fetch_add(1, Ordering::SeqCst);
            if self.context.node.state.try_lock().is_err() {
                let counter = if is_clone {
                    &self.clone_lock_violations
                } else {
                    &self.drop_lock_violations
                };
                counter.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    unsafe fn clone_reentrant_waker(data: *const ()) -> RawWaker {
        let probe = unsafe { Arc::from_raw(data.cast::<ReentrantWakerProbe>()) };
        unsafe { Arc::increment_strong_count(Arc::as_ptr(&probe)) };
        probe.callback(true);
        RawWaker::new(Arc::into_raw(probe).cast(), &REENTRANT_WAKER_VTABLE)
    }

    unsafe fn drop_reentrant_waker(data: *const ()) {
        let probe = unsafe { Arc::from_raw(data.cast::<ReentrantWakerProbe>()) };
        probe.callback(false);
    }

    unsafe fn wake_reentrant_waker(data: *const ()) {
        unsafe { drop_reentrant_waker(data) };
    }

    unsafe fn wake_reentrant_waker_by_ref(data: *const ()) {
        let probe = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<ReentrantWakerProbe>()) });
        probe.callback(false);
    }

    static REENTRANT_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_reentrant_waker,
        wake_reentrant_waker,
        wake_reentrant_waker_by_ref,
        drop_reentrant_waker,
    );

    fn reentrant_waker(probe: Arc<ReentrantWakerProbe>) -> Waker {
        let raw = RawWaker::new(Arc::into_raw(probe).cast(), &REENTRANT_WAKER_VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    impl BudgetPolicy for Policy {
        fn check(&self) -> Result<(), BudgetError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.reject {
                Err(BudgetError {
                    summary: DiagnosticSummary::new("budget rejected"),
                })
            } else {
                Ok(())
            }
        }
    }

    fn options(clock: Arc<dyn ClockSource>, domain: ClockDomainId, deadline: u64) -> ContextOptions {
        ContextOptions {
            clock: Some(clock),
            deadline: Some(Deadline::at(MonotonicTime::new(domain, deadline))),
            budget: None,
        }
    }

    #[test]
    fn test_stop_signal_resolves_after_first_stop_request() {
        use std::future::Future;
        use std::task::Poll;

        let context = OperationContext::new(ContextOptions::default()).unwrap();
        let mut signal = Box::pin(context.stop_requested());
        let waker = Waker::noop();
        let mut task = std::task::Context::from_waker(waker);

        assert!(matches!(signal.as_mut().poll(&mut task), Poll::Pending));
        assert!(context.request_stop(StopReason::ManagerShutdown));
        assert!(matches!(
            signal.as_mut().poll(&mut task),
            Poll::Ready(StopReason::ManagerShutdown)
        ));
    }

    #[test]
    fn test_cleanup_context_rejects_its_own_budget() {
        let policy = Arc::new(Policy {
            calls: AtomicUsize::new(0),
            reject: true,
        });
        let cleanup = OperationContext::new(ContextOptions::default())
            .unwrap()
            .cleanup_context(ContextOptions {
                clock: None,
                deadline: None,
                budget: Some(policy.clone()),
            })
            .unwrap();

        assert!(matches!(cleanup.check(), Err(ContextError::BudgetRejected(_))));
        assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_parent_stop_reaches_descendants() {
        let root = OperationContext::new(ContextOptions::default()).unwrap();
        let child = root.child(ContextOptions::default()).unwrap();
        let grandchild = child.child(ContextOptions::default()).unwrap();

        assert!(root.request_stop(StopReason::UserCancelled));
        assert_eq!(child.stop_reason(), Some(StopReason::ParentStopped));
        assert_eq!(grandchild.stop_reason(), Some(StopReason::ParentStopped));
    }

    #[test]
    fn test_child_stop_does_not_unconditionally_stop_parent() {
        let root = OperationContext::new(ContextOptions::default()).unwrap();
        let child = root.child(ContextOptions::default()).unwrap();

        assert!(child.request_stop(StopReason::UserCancelled));
        assert_eq!(root.stop_reason(), None);
    }

    #[test]
    fn test_context_clone_shares_stop_state_and_clock() {
        let domain = ClockDomainId::new_unique();
        let clock = ManualClock::new(domain, 3);
        let clock_source: Arc<dyn ClockSource> = clock.clone();
        let context = OperationContext::new(options(clock_source, domain, 9)).unwrap();
        let cloned = context.clone();

        assert!(cloned.clock_source().is_some());
        assert_eq!(domain.raw(), clock.domain.raw());
        assert!(context.request_stop(StopReason::UserCancelled));
        assert_eq!(cloned.stop_reason(), Some(StopReason::UserCancelled));
    }

    #[test]
    fn test_first_stop_cause_wins() {
        let context = OperationContext::new(ContextOptions::default()).unwrap();

        assert!(context.request_stop(StopReason::UserCancelled));
        assert!(!context.request_stop(StopReason::ManagerShutdown));
        assert_eq!(context.stop_reason(), Some(StopReason::UserCancelled));
    }

    #[test]
    fn test_stop_signal_waker_callbacks_run_outside_context_lock() {
        let context = Arc::new(OperationContext::new(ContextOptions::default()).unwrap());
        let probe = Arc::new(ReentrantWakerProbe {
            context: Arc::clone(&context),
            clone_lock_violations: AtomicUsize::new(0),
            drop_lock_violations: AtomicUsize::new(0),
            clone_callbacks: AtomicUsize::new(0),
            other_callbacks: AtomicUsize::new(0),
        });
        let waker = reentrant_waker(Arc::clone(&probe));
        let mut task = std::task::Context::from_waker(&waker);
        let mut signal = Box::pin(context.stop_requested());
        use std::future::Future;
        use std::task::Poll;

        assert!(matches!(Pin::as_mut(&mut signal).poll(&mut task), Poll::Pending));
        waker.wake_by_ref();
        #[allow(clippy::waker_clone_wake)]
        {
            // Exercise the consuming RawWaker callback separately from wake_by_ref.
            waker.clone().wake();
        }
        drop(signal);
        drop(waker);
        assert!(probe.clone_callbacks.load(Ordering::SeqCst) >= 1);
        assert!(probe.other_callbacks.load(Ordering::SeqCst) >= 1);
        assert_eq!(probe.clone_lock_violations.load(Ordering::SeqCst), 0);
        assert_eq!(probe.drop_lock_violations.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_deadline_registration_races_expiry() {
        let domain = ClockDomainId::new_unique();
        let clock = ManualClock::new(domain, 0);
        let context = OperationContext::new(options(clock.clone(), domain, 4)).unwrap();
        let wake = Arc::new(CountWake(AtomicUsize::new(0)));

        let mut signal = Box::pin(context.deadline_signal().unwrap());
        let waker = Waker::from(wake.clone());
        let mut task = std::task::Context::from_waker(&waker);
        use std::future::Future;
        use std::task::Poll;
        assert!(matches!(Pin::as_mut(&mut signal).poll(&mut task), Poll::Ready(())));

        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(clock.registration_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(clock.registration_cancels.load(Ordering::SeqCst), 1);
        assert_eq!(context.stop_reason(), Some(StopReason::Timeout));
    }

    #[test]
    fn test_child_deadline_cannot_extend_parent() {
        let domain = ClockDomainId::new_unique();
        let clock: Arc<dyn ClockSource> = ManualClock::new(domain, 0);
        let root = OperationContext::new(options(clock.clone(), domain, 10)).unwrap();
        let child = root
            .child(ContextOptions {
                clock: None,
                deadline: Some(Deadline::at(MonotonicTime::new(domain, 20))),
                budget: None,
            })
            .unwrap();

        assert_eq!(child.deadline(), root.deadline());
    }

    #[test]
    fn test_wrong_clock_domain_is_rejected() {
        let first = ClockDomainId::new_unique();
        let second = ClockDomainId::new_unique();
        let context = OperationContext::new(options(ManualClock::new(first, 0), second, 1));

        assert!(matches!(context, Err(ContextError::ClockDomainMismatch)));
    }

    #[test]
    fn test_parent_and_child_policies_are_both_checked() {
        let parent_policy = Arc::new(Policy {
            calls: AtomicUsize::new(0),
            reject: true,
        });
        let child_policy = Arc::new(Policy {
            calls: AtomicUsize::new(0),
            reject: false,
        });
        let root = OperationContext::new(ContextOptions {
            clock: None,
            deadline: None,
            budget: Some(parent_policy.clone()),
        })
        .unwrap();
        let child = root
            .child(ContextOptions {
                clock: None,
                deadline: None,
                budget: Some(child_policy.clone()),
            })
            .unwrap();

        assert!(matches!(child.check(), Err(ContextError::BudgetRejected(_))));
        assert_eq!(parent_policy.calls.load(Ordering::SeqCst), 1);
        assert_eq!(child_policy.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_cleanup_context_does_not_inherit_expired_business_deadline() {
        let domain = ClockDomainId::new_unique();
        let clock: Arc<dyn ClockSource> = ManualClock::new(domain, 10);
        let business = OperationContext::new(options(clock, domain, 10)).unwrap();

        let cleanup = business.cleanup_context(ContextOptions::default()).unwrap();

        assert!(cleanup.check().is_ok());
        assert_eq!(business.check().unwrap_err().to_string(), "Stopped");
    }
}

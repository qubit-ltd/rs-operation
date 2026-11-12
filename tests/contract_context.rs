#[path = "../src/context.rs"]
mod context;

mod protocol {
    pub(crate) use qubit_operation::protocol::{ClockSource, StopReason, TimerRegistration};
}
mod report {
    pub(crate) use qubit_operation::report::{ClockDomainId, MonotonicTime, StopReason};
}

use context::{ClockDomainMismatch, ContextOptions, Deadline, OperationContext};
use qubit_operation::protocol::{ClockSource, TimerRegistration};
use qubit_operation::report::{ClockDomainId, MonotonicTime, StopReason};
use std::cmp::Ordering;
use std::future::Future;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
};
use std::task::{Context, Poll, Wake, Waker};

#[derive(Clone)]
struct ManualClock {
    domain: ClockDomainId,
    ticks: Arc<Mutex<u64>>,
    cancellations: Arc<AtomicUsize>,
}

impl ManualClock {
    fn new(domain: u64) -> Self {
        Self {
            domain: ClockDomainId(domain),
            ticks: Arc::new(Mutex::new(0)),
            cancellations: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn advance(&self, ticks: u64) {
        *self.ticks.lock().expect("clock lock") += ticks;
    }
}

struct Registration(Arc<AtomicUsize>);
impl TimerRegistration for Registration {
    fn cancel(self: Box<Self>) {
        self.0.fetch_add(1, AtomicOrdering::SeqCst);
    }
}

impl ClockSource for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::new(self.domain, *self.ticks.lock().expect("clock lock"))
    }
    fn register_deadline(&self, _at: MonotonicTime, _wake: Waker) -> Box<dyn TimerRegistration> {
        Box::new(Registration(self.cancellations.clone()))
    }
}

#[test]
fn parent_stop_reaches_all_descendants_without_stopping_siblings_from_child_stop() {
    let root = OperationContext::new(ContextOptions::default());
    let left = root.child(ContextOptions::default()).expect("left");
    let right = root.child(ContextOptions::default()).expect("right");
    let grandchild = left.child(ContextOptions::default()).expect("grandchild");

    left.request_stop(StopReason::UserCancelled);
    assert_eq!(right.stop_reason(), None);
    assert_eq!(grandchild.stop_reason(), Some(StopReason::ParentStopped));
    root.request_stop(StopReason::SupervisorShutdown);
    assert_eq!(right.stop_reason(), Some(StopReason::ParentStopped));
}

#[test]
fn first_stop_reason_wins_and_repeated_requests_are_idempotent() {
    let context = OperationContext::new(ContextOptions::default());
    context.request_stop(StopReason::UserCancelled);
    context.request_stop(StopReason::Timeout);
    assert_eq!(context.stop_reason(), Some(StopReason::UserCancelled));
}

#[test]
fn deadline_compares_only_within_its_clock_domain_and_child_takes_earliest() {
    let clock = Arc::new(ManualClock::new(7));
    let first = Deadline::at(MonotonicTime::new(ClockDomainId(7), 10));
    let earlier = Deadline::at(MonotonicTime::new(ClockDomainId(7), 8));
    assert_eq!(
        first.compare(earlier).expect("same clock domain"),
        Ordering::Greater
    );
    assert_eq!(
        first.compare(Deadline::at(MonotonicTime::new(ClockDomainId(8), 8))),
        Err(ClockDomainMismatch {
            left: ClockDomainId(7),
            right: ClockDomainId(8)
        })
    );

    let parent = OperationContext::new(ContextOptions {
        clock: Some(clock.clone()),
        deadline: Some(first),
        ..ContextOptions::default()
    });
    let child = parent
        .child(ContextOptions {
            clock: Some(clock.clone()),
            deadline: Some(earlier),
            ..ContextOptions::default()
        })
        .expect("child");
    assert_eq!(child.deadline(), Some(earlier));
    clock.advance(8);
    assert!(child.is_expired());
    assert_eq!(
        child.admission_check(),
        Err(context::ContextError::DeadlineExpired)
    );
}

#[test]
fn child_deadline_cannot_extend_parent_deadline() {
    let clock = Arc::new(ManualClock::new(9));
    let parent_deadline = Deadline::at(MonotonicTime::new(ClockDomainId(9), 5));
    let parent = OperationContext::new(ContextOptions {
        clock: Some(clock.clone()),
        deadline: Some(parent_deadline),
        ..ContextOptions::default()
    });
    let child = parent
        .child(ContextOptions {
            clock: Some(clock),
            deadline: Some(Deadline::at(MonotonicTime::new(ClockDomainId(9), 50))),
            ..ContextOptions::default()
        })
        .expect("child");
    assert_eq!(child.deadline(), Some(parent_deadline));
}

#[test]
fn deadline_signal_requests_timeout_and_cancels_its_timer_registration() {
    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let clock = Arc::new(ManualClock::new(10));
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(10), 3));
    let context = OperationContext::new(ContextOptions {
        clock: Some(clock.clone()),
        deadline: Some(deadline),
        ..ContextOptions::default()
    });
    let mut signal = Box::pin(context.deadline_signal().expect("deadline signal"));
    let waker = Waker::from(Arc::new(NoopWake));
    let mut task_context = Context::from_waker(&waker);
    assert!(matches!(
        signal.as_mut().poll(&mut task_context),
        Poll::Pending
    ));
    clock.advance(3);
    assert!(matches!(
        signal.as_mut().poll(&mut task_context),
        Poll::Ready(())
    ));
    assert_eq!(context.stop_reason(), Some(StopReason::Timeout));
    assert_eq!(clock.cancellations.load(AtomicOrdering::SeqCst), 1);
}

#[test]
fn cleanup_context_does_not_inherit_business_cancellation_or_deadline() {
    let clock = Arc::new(ManualClock::new(11));
    let business = OperationContext::new(ContextOptions {
        clock: Some(clock),
        deadline: Some(Deadline::at(MonotonicTime::new(ClockDomainId(11), 0))),
        ..ContextOptions::default()
    });
    business.request_stop(StopReason::UserCancelled);
    let cleanup = OperationContext::cleanup_context(ContextOptions::default());
    assert_eq!(cleanup.stop_reason(), None);
    assert_eq!(cleanup.deadline(), None);
    assert_eq!(cleanup.admission_check(), Ok(()));
}

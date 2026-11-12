use qubit_operation::context::{
    BudgetLease, BudgetLeaseError, ClockDomainMismatch, ContextError, ContextOptions, Deadline,
    OperationContext,
};
use qubit_operation::protocol::{ClockSource, TimerRegistration};
use qubit_operation::report::{ClockDomainId, MonotonicTime, StopReason};
use std::cmp::Ordering;
use std::future::Future;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
};
use std::task::{Context, Poll, Wake, Waker};

struct TestBudget(AtomicBool);

impl BudgetLease for TestBudget {
    fn check(&self) -> Result<(), BudgetLeaseError> {
        self.0
            .load(AtomicOrdering::SeqCst)
            .then_some(())
            .ok_or(BudgetLeaseError)
    }
}

#[derive(Clone)]
struct TimerEntry {
    id: u64,
    deadline: u64,
    waker: Waker,
    active: bool,
}

struct ClockState {
    ticks: u64,
    next_id: u64,
    registrations: Vec<TimerEntry>,
}

struct ManualClock {
    domain: ClockDomainId,
    state: Arc<Mutex<ClockState>>,
    cancellations: Arc<AtomicUsize>,
}

impl ManualClock {
    fn new(domain: u64) -> Self {
        Self {
            domain: ClockDomainId(domain),
            state: Arc::new(Mutex::new(ClockState {
                ticks: 0,
                next_id: 0,
                registrations: Vec::new(),
            })),
            cancellations: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn advance(&self, ticks: u64) {
        let due = {
            let mut state = self.state.lock().expect("clock lock");
            state.ticks += ticks;
            let now = state.ticks;
            let mut due = Vec::new();
            state.registrations.retain(|entry| {
                if entry.active && entry.deadline <= now {
                    due.push(entry.waker.clone());
                    false
                } else {
                    true
                }
            });
            due
        };
        for waker in due {
            waker.wake();
        }
    }
}

struct Registration {
    id: u64,
    state: Arc<Mutex<ClockState>>,
    cancellations: Arc<AtomicUsize>,
}

impl TimerRegistration for Registration {
    fn cancel(self: Box<Self>) {
        if let Ok(mut state) = self.state.lock() {
            state.registrations.retain(|entry| entry.id != self.id);
        }
        self.cancellations.fetch_add(1, AtomicOrdering::SeqCst);
    }
}

impl ClockSource for ManualClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::new(self.domain, self.state.lock().expect("clock lock").ticks)
    }
    fn register_deadline(&self, at: MonotonicTime, wake: Waker) -> Box<dyn TimerRegistration> {
        let (id, already_elapsed) = {
            let mut state = self.state.lock().expect("clock lock");
            let id = state.next_id;
            state.next_id += 1;
            let already_elapsed = at.ticks <= state.ticks;
            if !already_elapsed {
                state.registrations.push(TimerEntry {
                    id,
                    deadline: at.ticks,
                    waker: wake.clone(),
                    active: true,
                });
            }
            (id, already_elapsed)
        };
        if already_elapsed {
            wake.wake();
        }
        Box::new(Registration {
            id,
            state: self.state.clone(),
            cancellations: self.cancellations.clone(),
        })
    }
}

struct CountingWake(Arc<AtomicUsize>);

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, AtomicOrdering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, AtomicOrdering::SeqCst);
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
    assert_eq!(child.admission_check(), Err(ContextError::DeadlineExpired));
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
fn child_budget_must_satisfy_both_parent_and_child_leases() {
    let denied_parent = Arc::new(TestBudget(AtomicBool::new(false)));
    let permissive_child = Arc::new(TestBudget(AtomicBool::new(true)));
    let parent = OperationContext::new(ContextOptions {
        budget: Some(denied_parent),
        ..ContextOptions::default()
    });
    let child = parent
        .child(ContextOptions {
            budget: Some(permissive_child),
            ..ContextOptions::default()
        })
        .expect("child context");
    assert_eq!(
        child.admission_check(),
        Err(ContextError::BudgetUnavailable)
    );

    let permissive_parent = Arc::new(TestBudget(AtomicBool::new(true)));
    let denied_child = Arc::new(TestBudget(AtomicBool::new(false)));
    let parent = OperationContext::new(ContextOptions {
        budget: Some(permissive_parent),
        ..ContextOptions::default()
    });
    let child = parent
        .child(ContextOptions {
            budget: Some(denied_child),
            ..ContextOptions::default()
        })
        .expect("child context");
    assert_eq!(
        child.admission_check(),
        Err(ContextError::BudgetUnavailable)
    );
}

#[test]
fn deadline_signal_requests_timeout_and_cancels_its_timer_registration() {
    let clock = Arc::new(ManualClock::new(10));
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(10), 3));
    let context = OperationContext::new(ContextOptions {
        clock: Some(clock.clone()),
        deadline: Some(deadline),
        ..ContextOptions::default()
    });
    let mut signal = Box::pin(context.deadline_signal().expect("deadline signal"));
    let first_wake_count = Arc::new(AtomicUsize::new(0));
    let first_waker = Waker::from(Arc::new(CountingWake(first_wake_count.clone())));
    let mut first_task_context = Context::from_waker(&first_waker);
    assert!(matches!(
        signal.as_mut().poll(&mut first_task_context),
        Poll::Pending
    ));
    let current_wake_count = Arc::new(AtomicUsize::new(0));
    let current_wake = Arc::new(CountingWake(current_wake_count.clone()));
    let current_wake_weak = Arc::downgrade(&current_wake);
    let current_waker = Waker::from(current_wake.clone());
    drop(current_wake);
    {
        let mut current_task_context = Context::from_waker(&current_waker);
        assert!(matches!(
            signal.as_mut().poll(&mut current_task_context),
            Poll::Pending
        ));
    }
    assert_eq!(clock.cancellations.load(AtomicOrdering::SeqCst), 1);
    clock.advance(3);
    assert_eq!(first_wake_count.load(AtomicOrdering::SeqCst), 0);
    assert_eq!(current_wake_count.load(AtomicOrdering::SeqCst), 1);
    {
        let mut current_task_context = Context::from_waker(&current_waker);
        assert!(matches!(
            signal.as_mut().poll(&mut current_task_context),
            Poll::Ready(())
        ));
    }
    assert_eq!(context.stop_reason(), Some(StopReason::Timeout));
    assert_eq!(clock.cancellations.load(AtomicOrdering::SeqCst), 2);
    drop(current_waker);
    assert!(current_wake_weak.upgrade().is_none());
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

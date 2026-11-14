//! Context cancellation, deadlines, clock-domain validation, and budget checks.

use crate::v02::protocol::{BudgetPolicy, ClockSource, Deadline, TimerRegistration};
use crate::v02::report::{ContextError, ContextOptions, StopReason};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

/// A cancellation node shared with its descendants.
struct StopNode {
    state: Mutex<StopState>,
}

struct StopState {
    reason: Option<StopReason>,
    children: Vec<Weak<StopNode>>,
    waiters: Vec<(u64, Waker)>,
    next_waiter: u64,
}

impl StopNode {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(StopState {
                reason: None,
                children: Vec::new(),
                waiters: Vec::new(),
                next_waiter: 1,
            }),
        })
    }

    fn reason(&self) -> Option<StopReason> {
        lock(&self.state).reason
    }

    fn request_stop(&self, reason: StopReason) -> bool {
        let (children, waiters) = {
            let mut state = lock(&self.state);
            if state.reason.is_some() {
                return false;
            }
            state.reason = Some(reason);
            state.children.retain(|child| child.strong_count() != 0);
            let children = state
                .children
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            let waiters = std::mem::take(&mut state.waiters);
            (children, waiters)
        };
        for (_, waiter) in waiters {
            waiter.wake();
        }
        for child in children {
            child.request_stop(StopReason::ParentStopped);
        }
        true
    }
}

/// Shared stop tree and inherited time and budget policy for one operation.
pub struct OperationContext {
    node: Arc<StopNode>,
    clock: Option<Arc<dyn ClockSource>>,
    deadline: Option<Deadline>,
    policies: Vec<Arc<dyn BudgetPolicy>>,
}

impl OperationContext {
    /// Constructs a root context after validating its clock and deadline.
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
    pub fn child(&self, options: ContextOptions) -> Result<Self, ContextError> {
        let clock = options.clock.or_else(|| self.clock.clone());
        if let (Some(parent_clock), Some(child_clock)) = (self.clock.as_deref(), clock.as_deref()) {
            if parent_clock.now().domain() != child_clock.now().domain() {
                return Err(ContextError::ClockDomainMismatch);
            }
        }
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
    pub fn cleanup_context(&self, options: ContextOptions) -> Result<CleanupContext, ContextError> {
        CleanupContext::new(options)
    }

    /// Returns the first accepted stop reason.
    pub fn stop_reason(&self) -> Option<StopReason> {
        self.node.reason()
    }

    /// Requests stop and propagates it to descendants exactly once.
    pub fn request_stop(&self, reason: StopReason) -> bool {
        self.node.request_stop(reason)
    }

    /// Returns the effective deadline inherited from this context's ancestry.
    pub const fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }

    /// Returns true when the configured clock has reached this context's deadline.
    pub fn is_expired(&self) -> bool {
        match (self.clock.as_deref(), self.deadline) {
            (Some(clock), Some(deadline)) => {
                let now = clock.now();
                now.domain() == deadline.time_point().domain()
                    && now.ticks() >= deadline.time_point().ticks()
            }
            _ => false,
        }
    }

    /// Checks stop, deadline, and every inherited policy without reserving capacity.
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
            if let Err(error) = policy.check() {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), |error| Err(ContextError::BudgetRejected(error)))
    }

    /// Returns a future which resolves with the first accepted stop reason.
    pub fn stop_requested(&self) -> StopSignal {
        StopSignal {
            node: Arc::clone(&self.node),
            waiter: None,
            completed: false,
        }
    }

    /// Creates a signal that registers and cancels its deadline timer.
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

/// Independent cleanup authority with its own optional deadline and policy.
pub struct CleanupContext {
    clock: Option<Arc<dyn ClockSource>>,
    deadline: Option<Deadline>,
    policies: Vec<Arc<dyn BudgetPolicy>>,
}

impl CleanupContext {
    /// Constructs a cleanup context without inheriting business context state.
    pub fn new(options: ContextOptions) -> Result<Self, ContextError> {
        validate_clock(options.clock.as_deref(), options.deadline)?;
        Ok(Self {
            clock: options.clock,
            deadline: options.deadline,
            policies: options.budget.into_iter().collect(),
        })
    }

    /// Checks only the cleanup context's independent deadline and policies.
    pub fn check(&self) -> Result<(), ContextError> {
        if let (Some(clock), Some(deadline)) = (self.clock.as_deref(), self.deadline) {
            let now = clock.now();
            if now.domain() != deadline.time_point().domain()
                || now.ticks() >= deadline.time_point().ticks()
            {
                return Err(ContextError::Stopped);
            }
        }
        let mut first_error = None;
        for policy in &self.policies {
            if let Err(error) = policy.check() {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), |error| Err(ContextError::BudgetRejected(error)))
    }
}

/// Future resolving when the context accepts its first stop request.
pub struct StopSignal {
    node: Arc<StopNode>,
    waiter: Option<u64>,
    completed: bool,
}

impl Future for StopSignal {
    type Output = StopReason;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        assert!(!this.completed, "stop signal polled after completion");
        let node = Arc::clone(&this.node);
        let mut state = lock(&node.state);
        if let Some(reason) = state.reason {
            if let Some(id) = this.waiter.take() {
                if let Some(index) = state
                    .waiters
                    .iter()
                    .position(|(candidate, _)| *candidate == id)
                {
                    state.waiters.swap_remove(index);
                }
            }
            drop(state);
            this.completed = true;
            return Poll::Ready(reason);
        }
        if let Some(id) = this.waiter {
            if let Some((_, waker)) = state
                .waiters
                .iter_mut()
                .find(|(candidate, _)| *candidate == id)
            {
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
            } else {
                drop(state);
                this.completed = true;
                return Poll::Ready(StopReason::ParentStopped);
            }
        } else {
            let id = state.next_waiter;
            state.next_waiter = state.next_waiter.wrapping_add(1).max(1);
            state.waiters.push((id, cx.waker().clone()));
            this.waiter = Some(id);
        }
        Poll::Pending
    }
}

impl Drop for StopSignal {
    fn drop(&mut self) {
        if let Some(id) = self.waiter.take() {
            let mut state = lock(&self.node.state);
            if let Some(index) = state
                .waiters
                .iter()
                .position(|(candidate, _)| *candidate == id)
            {
                state.waiters.swap_remove(index);
            }
        }
    }
}

/// Future which requests timeout once its registered deadline is reached.
pub struct DeadlineSignal {
    node: Weak<StopNode>,
    clock: Arc<dyn ClockSource>,
    deadline: Deadline,
    registration: Option<Box<dyn TimerRegistration>>,
    registered_waker: Option<Waker>,
    fired: bool,
}

impl Future for DeadlineSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.fired {
            return Poll::Ready(());
        }
        if deadline_reached(&*self.clock, self.deadline) {
            fire_timeout(&mut self);
            return Poll::Ready(());
        }
        let changed = self
            .registered_waker
            .as_ref()
            .is_none_or(|old| !old.will_wake(cx.waker()));
        if changed {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            self.registered_waker = Some(cx.waker().clone());
            self.registration = Some(
                self.clock
                    .register_deadline(self.deadline.time_point(), cx.waker().clone()),
            );
        }
        // Close the check/register race even for a clock that advances before
        // its registration handle becomes visible to this future.
        if deadline_reached(&*self.clock, self.deadline) {
            fire_timeout(&mut self);
            return Poll::Ready(());
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

fn fire_timeout(signal: &mut DeadlineSignal) {
    if let Some(node) = signal.node.upgrade() {
        node.request_stop(StopReason::Timeout);
    }
    signal.fired = true;
    if let Some(registration) = signal.registration.take() {
        registration.cancel();
    }
    signal.registered_waker = None;
}

fn deadline_reached(clock: &dyn ClockSource, deadline: Deadline) -> bool {
    let now = clock.now();
    now.domain() != deadline.time_point().domain() || now.ticks() >= deadline.time_point().ticks()
}

fn validate_clock(
    clock: Option<&dyn ClockSource>,
    deadline: Option<Deadline>,
) -> Result<(), ContextError> {
    if let Some(deadline) = deadline {
        let Some(clock) = clock else {
            return Err(ContextError::ClockUnavailable);
        };
        if clock.now().domain() != deadline.time_point().domain() {
            return Err(ContextError::ClockDomainMismatch);
        }
    }
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::OperationContext;
    use crate::v02::protocol::{
        BudgetError, BudgetPolicy, ClockDomainId, ClockSource, Deadline, MonotonicTime,
        TimerRegistration,
    };
    use crate::v02::report::{ContextError, ContextOptions, DiagnosticSummary, StopReason};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

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

    fn options(
        clock: Arc<dyn ClockSource>,
        domain: ClockDomainId,
        deadline: u64,
    ) -> ContextOptions {
        ContextOptions {
            clock: Some(clock),
            deadline: Some(Deadline::at(MonotonicTime::new(domain, deadline))),
            budget: None,
        }
    }

    #[test]
    fn parent_stop_reaches_descendants() {
        let root = OperationContext::new(ContextOptions::default()).unwrap();
        let child = root.child(ContextOptions::default()).unwrap();
        let grandchild = child.child(ContextOptions::default()).unwrap();

        assert!(root.request_stop(StopReason::UserCancelled));
        assert_eq!(child.stop_reason(), Some(StopReason::ParentStopped));
        assert_eq!(grandchild.stop_reason(), Some(StopReason::ParentStopped));
    }

    #[test]
    fn child_stop_does_not_unconditionally_stop_parent() {
        let root = OperationContext::new(ContextOptions::default()).unwrap();
        let child = root.child(ContextOptions::default()).unwrap();

        assert!(child.request_stop(StopReason::UserCancelled));
        assert_eq!(root.stop_reason(), None);
    }

    #[test]
    fn first_stop_cause_wins() {
        let context = OperationContext::new(ContextOptions::default()).unwrap();

        assert!(context.request_stop(StopReason::UserCancelled));
        assert!(!context.request_stop(StopReason::ManagerShutdown));
        assert_eq!(context.stop_reason(), Some(StopReason::UserCancelled));
    }

    #[test]
    fn deadline_registration_races_expiry() {
        let domain = ClockDomainId::new_unique();
        let clock = ManualClock::new(domain, 0);
        let context = OperationContext::new(options(clock.clone(), domain, 4)).unwrap();
        let wake = Arc::new(CountWake(AtomicUsize::new(0)));

        let mut signal = Box::pin(context.deadline_signal().unwrap());
        let waker = Waker::from(wake.clone());
        let mut task = std::task::Context::from_waker(&waker);
        use std::future::Future;
        use std::task::Poll;
        assert!(matches!(
            Pin::as_mut(&mut signal).poll(&mut task),
            Poll::Ready(())
        ));

        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(clock.registration_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(clock.registration_cancels.load(Ordering::SeqCst), 1);
        assert_eq!(context.stop_reason(), Some(StopReason::Timeout));
    }

    #[test]
    fn child_deadline_cannot_extend_parent() {
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
    fn wrong_clock_domain_is_rejected() {
        let first = ClockDomainId::new_unique();
        let second = ClockDomainId::new_unique();
        let context = OperationContext::new(options(ManualClock::new(first, 0), second, 1));

        assert!(matches!(context, Err(ContextError::ClockDomainMismatch)));
    }

    #[test]
    fn parent_and_child_policies_both_checked() {
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

        assert!(matches!(
            child.check(),
            Err(ContextError::BudgetRejected(_))
        ));
        assert_eq!(parent_policy.calls.load(Ordering::SeqCst), 1);
        assert_eq!(child_policy.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cleanup_context_does_not_inherit_expired_business_deadline() {
        let domain = ClockDomainId::new_unique();
        let clock: Arc<dyn ClockSource> = ManualClock::new(domain, 10);
        let business = OperationContext::new(options(clock, domain, 10)).unwrap();

        let cleanup = business.cleanup_context(ContextOptions::default()).unwrap();

        assert!(cleanup.check().is_ok());
        assert_eq!(business.check().unwrap_err().to_string(), "Stopped");
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::internal::OperationCore;
use super::operation_observer::OperationObserver;
use crate::v02::protocol::Deadline;
use crate::v02::report::ObservationError;
use crate::v02::report::OperationId;
use crate::v02::report::OperationSnapshot;
use crate::v02::report::SettlementReport;
use crate::v02::report::SettlementWait;

/// Asynchronous, untyped read-only lifecycle observer.
///
/// The observer can inspect execution and settlement state without owning or
/// taking the operation's typed outcome.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationManager;
/// use qubit_operation::{ManagerOptions, OperationOptions};
///
/// let manager = OperationManager::new(ManagerOptions::default()).unwrap();
/// let operation = manager
///     .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
///     .unwrap();
/// let observer = operation.observer().as_async();
/// assert_eq!(observer.id(), operation.id());
/// ```
#[must_use = "retain the observer to inspect operation lifecycle state"]
#[derive(Clone)]
pub struct AsyncOperationObserver {
    /// Shared lifecycle core retained for read-only operation observation.
    pub(in crate::v02::operation) core: Arc<OperationCore>,
}

impl AsyncOperationObserver {
    /// Returns this operation's stable ID.
    ///
    /// # Returns
    ///
    /// The ID shared by the operation and all its observer views.
    #[must_use = "use the operation ID to correlate this observer with its source"]
    #[inline]
    pub fn id(&self) -> OperationId {
        self.core.id()
    }

    /// Returns the bounded diagnostics reported by external protocol ports.
    /// Faults are ordered Release then Settlement and do not alter the
    /// operation outcome.
    pub fn external_protocol_faults(&self) -> Arc<[crate::v02::report::ExternalProtocolFault]> {
        self.core.external_protocol_faults()
    }

    /// Returns the latest untyped lifecycle snapshot.
    ///
    /// # Returns
    ///
    /// A snapshot of the operation's current execution and settlement state.
    #[must_use = "use the snapshot to inspect the latest operation state"]
    #[inline]
    pub fn snapshot(&self) -> OperationSnapshot {
        self.core.snapshot()
    }

    /// Asynchronously waits for the immutable settlement report.
    ///
    /// The observer does not own the typed outcome, but another handle or owner
    /// can retain a [`crate::v02::resource::ResourceRef`] needed for
    /// settlement. Release references held by success and error values
    /// before awaiting.
    /// See [`crate::v02::operation::OperationHandle::wait_settlement`] for an
    /// example of the shared ownership rule.
    ///
    /// # Returns
    ///
    /// A shared settlement report after settlement completes.
    #[must_use = "await the report to observe completed operation settlement"]
    pub async fn wait_settlement(&self) -> Arc<SettlementReport> {
        self.core.wait_settlement_async().await
    }

    /// Waits until the deadline, returning a pending snapshot if settlement is
    /// not ready.
    ///
    /// Other owners, including references inside an unconsumed outcome, can
    /// keep settlement pending. The deadline only bounds observation; it does
    /// not prove release.
    ///
    /// The deadline must use the same clock domain as the operation context.
    ///
    /// # Parameters
    ///
    /// - `deadline`: Absolute deadline for waiting on settlement publication.
    ///
    /// # Returns
    ///
    /// A ready settlement report or the latest pending snapshot.
    ///
    /// # Errors
    ///
    /// Returns `ClockUnavailable` if the operation has no clock, or
    /// `ClockDomainMismatch` if the deadline uses another clock domain.
    pub async fn wait_settlement_until(&self, deadline: Deadline) -> Result<SettlementWait, ObservationError> {
        self.core.wait_settlement_until(deadline).await
    }

    /// Creates the sync view of this read-only observer.
    ///
    /// # Returns
    ///
    /// A synchronous observer retaining the same operation lifecycle core.
    #[must_use = "retain the synchronous observer to inspect operation lifecycle state"]
    #[inline]
    pub fn as_sync(&self) -> OperationObserver {
        OperationObserver {
            core: Arc::clone(&self.core),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::task::Waker;

    use super::AsyncOperationObserver;
    use crate::v02::operation::OperationCore;
    use crate::v02::protocol::ClockDomainId;
    use crate::v02::protocol::ClockSource;
    use crate::v02::protocol::Deadline;
    use crate::v02::protocol::MonotonicTime;
    use crate::v02::protocol::TimerRegistration;
    use crate::v02::report::ExecutionPhase;
    use crate::v02::report::ObservationError;
    use crate::v02::report::OperationId;
    use crate::v02::report::SettlementReport;
    use crate::v02::report::SettlementStatus;
    use crate::v02::report::SettlementWait;

    struct FixedClock(MonotonicTime);

    impl ClockSource for FixedClock {
        fn now(&self) -> MonotonicTime {
            self.0
        }

        fn register_deadline(&self, _at: MonotonicTime, _wake: Waker) -> Box<dyn TimerRegistration> {
            Box::new(NoopTimer)
        }
    }

    struct NoopTimer;

    impl TimerRegistration for NoopTimer {
        fn cancel(self: Box<Self>) {}
    }

    struct ClockState {
        now: MonotonicTime,
        registrations: Vec<(bool, Waker)>,
        cancelled: usize,
    }

    struct AdvancingClock {
        state: Arc<Mutex<ClockState>>,
        advance_on_register: bool,
    }

    impl AdvancingClock {
        fn new(now: MonotonicTime) -> Self {
            Self {
                state: Arc::new(Mutex::new(ClockState {
                    now,
                    registrations: Vec::new(),
                    cancelled: 0,
                })),
                advance_on_register: false,
            }
        }

        fn advancing_on_register(now: MonotonicTime) -> Self {
            Self {
                advance_on_register: true,
                ..Self::new(now)
            }
        }

        fn counts(&self) -> (usize, usize) {
            let state = self.state.lock().unwrap();
            (state.registrations.len(), state.cancelled)
        }

        fn advance_to(&self, now: MonotonicTime) {
            let wakers = {
                let mut state = self.state.lock().unwrap();
                state.now = now;
                state
                    .registrations
                    .iter()
                    .filter(|(active, _)| *active)
                    .map(|(_, waker)| waker.clone())
                    .collect::<Vec<_>>()
            };
            for waker in wakers {
                waker.wake();
            }
        }
    }

    impl ClockSource for AdvancingClock {
        fn now(&self) -> MonotonicTime {
            self.state.lock().unwrap().now
        }

        fn register_deadline(&self, at: MonotonicTime, wake: Waker) -> Box<dyn TimerRegistration> {
            let index = {
                let mut state = self.state.lock().unwrap();
                if self.advance_on_register {
                    state.now = at;
                }
                let index = state.registrations.len();
                state.registrations.push((true, wake));
                index
            };
            Box::new(ClockTimer {
                state: Arc::clone(&self.state),
                index,
            })
        }
    }

    struct ClockTimer {
        state: Arc<Mutex<ClockState>>,
        index: usize,
    }

    impl TimerRegistration for ClockTimer {
        fn cancel(self: Box<Self>) {
            let mut state = self.state.lock().unwrap();
            if state.registrations[self.index].0 {
                state.registrations[self.index].0 = false;
                state.cancelled += 1;
            }
        }
    }

    #[derive(Default)]
    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    unsafe fn clone_noop_waker(data: *const ()) -> RawWaker {
        RawWaker::new(data, &NOOP_WAKER_VTABLE)
    }

    unsafe fn wake_noop_waker(_: *const ()) {}

    unsafe fn wake_noop_waker_by_ref(_: *const ()) {}

    unsafe fn drop_noop_waker(_: *const ()) {}

    static NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_noop_waker,
        wake_noop_waker,
        wake_noop_waker_by_ref,
        drop_noop_waker,
    );

    fn stable_noop_waker() -> Waker {
        // SAFETY: this vtable never dereferences its null data pointer and owns
        // no resources, so cloning and dropping the waker are always valid.
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)) }
    }

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    fn report(id: OperationId) -> SettlementReport {
        SettlementReport {
            operation_id: id,
            status: SettlementStatus::Released,
            released_count: 0,
            transferred_count: 0,
            recovery_count: 0,
            children: Arc::from([]),
            children_total: 0,
            children_truncated: false,
            cleanup_failures: Arc::from([]),
            cleanup_failures_total: 0,
            cleanup_failures_truncated: false,
            resource_reports: Arc::from([]),
            resource_reports_total: 0,
            resource_reports_truncated: false,
            transfers: Arc::from([]),
            transfers_total: 0,
            transfers_truncated: false,
            recoveries: Arc::from([]),
            recoveries_total: 0,
            recoveries_truncated: false,
            effects: Arc::from([]),
            effects_total: 0,
            effect_overflow: 0,
            external_settlement: None,
        }
    }

    fn make_observer(clock: Option<Arc<dyn ClockSource>>) -> (AsyncOperationObserver, OperationId) {
        let id = OperationId::new_unique();
        let core = OperationCore::new_with_observers(id, clock, Arc::new(|| false));
        (AsyncOperationObserver { core }, id)
    }

    #[test]
    fn test_async_observer_reads_identity_snapshot_and_sync_view() {
        let (observer, id) = make_observer(None);
        assert_eq!(observer.id(), id);
        assert_eq!(observer.snapshot().execution, ExecutionPhase::Queued);
        assert_eq!(observer.as_sync().id(), id);
    }

    #[test]
    fn test_async_observer_waits_for_published_settlement() {
        let (observer, id) = make_observer(None);
        let expected = observer.core.publish_settlement(report(id)).unwrap();
        let mut wait = Box::pin(observer.wait_settlement());
        match poll_once(wait.as_mut()) {
            Poll::Ready(actual) => assert!(Arc::ptr_eq(&actual, &expected)),
            Poll::Pending => panic!("published settlement should be immediately observable"),
        }
    }

    #[test]
    fn test_async_observer_deadline_errors_and_returns_pending_or_ready_state() {
        let (without_clock, id) = make_observer(None);
        let arbitrary = Deadline::at(MonotonicTime::new(ClockDomainId::new_unique(), 1));
        let mut unavailable = Box::pin(without_clock.wait_settlement_until(arbitrary));
        assert!(matches!(
            poll_once(unavailable.as_mut()),
            Poll::Ready(Err(ObservationError::ClockUnavailable))
        ));

        let domain = ClockDomainId::new_unique();
        let now = MonotonicTime::new(domain, 5);
        let (observed, observed_id) = make_observer(Some(Arc::new(FixedClock(now))));
        let mismatch = Deadline::at(MonotonicTime::new(ClockDomainId::new_unique(), 10));
        let mut wrong_domain = Box::pin(observed.wait_settlement_until(mismatch));
        assert!(matches!(
            poll_once(wrong_domain.as_mut()),
            Poll::Ready(Err(ObservationError::ClockDomainMismatch))
        ));

        let mut elapsed = Box::pin(observed.wait_settlement_until(Deadline::at(now)));
        assert!(matches!(
            poll_once(elapsed.as_mut()),
            Poll::Ready(Ok(SettlementWait::Pending(snapshot))) if snapshot.operation_id == observed_id
        ));

        observed.core.publish_settlement(report(observed_id)).unwrap();
        let mut ready = Box::pin(observed.wait_settlement_until(Deadline::at(now)));
        assert!(matches!(
            poll_once(ready.as_mut()),
            Poll::Ready(Ok(SettlementWait::Ready(_)))
        ));
        assert_ne!(id, observed_id);
    }

    #[test]
    fn test_settlement_deadline_tracks_latest_waker_and_cancels_registration() {
        let domain = ClockDomainId::new_unique();
        let start = MonotonicTime::new(domain, 5);
        let deadline = MonotonicTime::new(domain, 10);
        let clock = Arc::new(AdvancingClock::new(start));
        let (observer, id) = make_observer(Some(clock.clone()));
        let mut wait = Box::pin(observer.core.wait_settlement_until(Deadline::at(deadline)));
        let wake_a = Arc::new(CountWake::default());
        let wake_b = Arc::new(CountWake::default());
        let waker_a = Waker::from(Arc::clone(&wake_a));
        let waker_b = Waker::from(Arc::clone(&wake_b));
        let mut context_a = Context::from_waker(&waker_a);
        let mut context_b = Context::from_waker(&waker_b);

        let same_waker = stable_noop_waker();
        assert!(matches!(poll_once_with(wait.as_mut(), &same_waker), Poll::Pending));
        assert_eq!(clock.counts(), (1, 0));
        assert!(matches!(poll_once_with(wait.as_mut(), &same_waker), Poll::Pending));
        assert_eq!(
            clock.counts(),
            (1, 0),
            "re-polling with the same waker must reuse its timer"
        );
        assert!(matches!(wait.as_mut().poll(&mut context_a), Poll::Pending));
        assert_eq!(clock.counts(), (2, 1));
        assert!(matches!(wait.as_mut().poll(&mut context_b), Poll::Pending));
        assert_eq!(
            clock.counts(),
            (3, 2),
            "replacing a waker must cancel and replace its timer"
        );

        clock.advance_to(deadline);
        assert_eq!(wake_a.0.load(Ordering::SeqCst), 0);
        assert_eq!(
            wake_b.0.load(Ordering::SeqCst),
            1,
            "deadline must wake the latest task waker"
        );
        assert!(
            matches!(wait.as_mut().poll(&mut context_b), Poll::Ready(Ok(SettlementWait::Pending(snapshot))) if snapshot.operation_id == id)
        );
        assert_eq!(clock.counts(), (3, 3), "expiry completion must cancel the active timer");
    }

    #[test]
    fn test_settlement_deadline_cancels_timer_on_settlement_and_drop() {
        let domain = ClockDomainId::new_unique();
        let start = MonotonicTime::new(domain, 5);
        let deadline = MonotonicTime::new(domain, 10);
        let clock = Arc::new(AdvancingClock::new(start));
        let (observer, id) = make_observer(Some(clock.clone()));
        let mut wait = Box::pin(observer.wait_settlement_until(Deadline::at(deadline)));
        let waker = Waker::noop();
        assert!(matches!(poll_once_with(wait.as_mut(), waker), Poll::Pending));
        observer.core.publish_settlement(report(id)).unwrap();
        assert!(matches!(
            poll_once_with(wait.as_mut(), waker),
            Poll::Ready(Ok(SettlementWait::Ready(_)))
        ));
        assert_eq!(clock.counts(), (1, 1));

        let drop_clock = Arc::new(AdvancingClock::new(start));
        let (drop_observer, _) = make_observer(Some(drop_clock.clone()));
        let mut dropped = Box::pin(drop_observer.wait_settlement_until(Deadline::at(deadline)));
        assert!(matches!(poll_once_with(dropped.as_mut(), waker), Poll::Pending));
        drop(dropped);
        assert_eq!(drop_clock.counts(), (1, 1), "dropping the future must cancel its timer");

        let wrong_clock = Arc::new(AdvancingClock::new(start));
        let (wrong_observer, _) = make_observer(Some(wrong_clock.clone()));
        let mut wrong_wait = Box::pin(wrong_observer.wait_settlement_until(Deadline::at(deadline)));
        assert!(matches!(poll_once_with(wrong_wait.as_mut(), waker), Poll::Pending));
        wrong_clock.advance_to(MonotonicTime::new(ClockDomainId::new_unique(), 6));
        assert!(matches!(
            poll_once_with(wrong_wait.as_mut(), waker),
            Poll::Ready(Err(ObservationError::ClockDomainMismatch))
        ));
        assert_eq!(
            wrong_clock.counts(),
            (1, 1),
            "clock-domain error must cancel the active timer"
        );
    }

    #[test]
    fn test_settlement_deadline_rechecks_clock_after_registration() {
        let domain = ClockDomainId::new_unique();
        let start = MonotonicTime::new(domain, 5);
        let deadline = MonotonicTime::new(domain, 10);
        let clock = Arc::new(AdvancingClock::advancing_on_register(start));
        let (observer, id) = make_observer(Some(clock.clone()));
        let mut wait = Box::pin(observer.wait_settlement_until(Deadline::at(deadline)));
        assert!(
            matches!(poll_once(wait.as_mut()), Poll::Ready(Ok(SettlementWait::Pending(snapshot))) if snapshot.operation_id == id)
        );
        assert_eq!(
            clock.counts(),
            (1, 1),
            "deadline reached during registration must be observed and cancelled"
        );
    }

    fn poll_once_with<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }
}

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
    use std::task::Context;
    use std::task::Poll;
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
        }
    }

    fn observer(clock: Option<Arc<dyn ClockSource>>) -> (AsyncOperationObserver, OperationId) {
        let id = OperationId::new_unique();
        let core = OperationCore::new_with_observers(id, clock, Arc::new(|| false));
        (AsyncOperationObserver { core }, id)
    }

    #[test]
    fn test_async_observer_reads_identity_snapshot_and_sync_view() {
        let (observer, id) = observer(None);
        assert_eq!(observer.id(), id);
        assert_eq!(observer.snapshot().execution, ExecutionPhase::Queued);
        assert_eq!(observer.as_sync().id(), id);
    }

    #[test]
    fn test_async_observer_waits_for_published_settlement() {
        let (observer, id) = observer(None);
        let expected = observer.core.publish_settlement(report(id)).unwrap();
        let mut wait = Box::pin(observer.wait_settlement());
        match poll_once(wait.as_mut()) {
            Poll::Ready(actual) => assert!(Arc::ptr_eq(&actual, &expected)),
            Poll::Pending => panic!("published settlement should be immediately observable"),
        }
    }

    #[test]
    fn test_async_observer_deadline_errors_and_returns_pending_or_ready_state() {
        let (without_clock, id) = observer(None);
        let arbitrary = Deadline::at(MonotonicTime::new(ClockDomainId::new_unique(), 1));
        let mut unavailable = Box::pin(without_clock.wait_settlement_until(arbitrary));
        assert!(matches!(
            poll_once(unavailable.as_mut()),
            Poll::Ready(Err(ObservationError::ClockUnavailable))
        ));

        let domain = ClockDomainId::new_unique();
        let now = MonotonicTime::new(domain, 5);
        let (observed, observed_id) = observer(Some(Arc::new(FixedClock(now))));
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
}

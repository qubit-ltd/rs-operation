// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use crate::v02::internal::notify::Notify;
use crate::v02::report::RecoverySnapshot;
use crate::v02::resource::recovery::lock;

/// Report and waiter state shared by synchronous and asynchronous observers.
///
/// The report is published once a retry completes. An async waiter registers
/// its notification generation while checking the report so completion cannot
/// be missed.
pub(in crate::v02::resource::recovery) struct AttemptObservation {
    /// Completed attempt report made available to every observer.
    report: Mutex<Option<crate::v02::report::RecoveryAttemptReport>>,
    /// Change notification shared by every async waiter.
    changed: Notify,
}

/// Creates an empty report slot and notification set for one retry.
impl Default for AttemptObservation {
    /// Returns an observation with no report and no registered waiters.
    fn default() -> Self {
        Self {
            report: Mutex::new(None),
            changed: Notify::new(),
        }
    }
}

/// Publishes the first completed retry report and wakes all async waiters.
///
/// The recovery snapshot also provides the failure list copied into the
/// observer-facing report.
///
/// # Parameters
///
/// * `observation` — shared report and waiter state for the retry.
/// * `snapshot` — final recovery snapshot produced by the attempt.
pub(in crate::v02::resource::recovery) fn publish_attempt(
    observation: &AttemptObservation,
    snapshot: RecoverySnapshot,
) {
    let mut report = lock(&observation.report);
    if report.is_some() {
        return;
    }
    *report = Some(crate::v02::report::RecoveryAttemptReport {
        failures: Arc::clone(&snapshot.last_failures),
        recovery: snapshot,
    });
    drop(report);
    observation.changed.notify_waiters();
}

/// Waits for the shared retry report without blocking the executor thread.
///
/// The future records a notification generation before checking the report,
/// then waits for that generation to change. This ordering prevents a
/// concurrent publication from being missed, and dropping the notification
/// future unregisters its waker.
///
/// # Parameters
///
/// * `observation` — shared report and waiter state to observe.
///
/// # Returns
///
/// The completed report published for the retry.
pub(in crate::v02::resource::recovery) async fn wait_attempt_async(
    observation: &AttemptObservation,
) -> crate::v02::report::RecoveryAttemptReport {
    loop {
        let generation = observation.changed.generation();
        if let Some(report) = lock(&observation.report).clone() {
            return report;
        }
        observation.changed.notified_since(generation).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use super::AttemptObservation;
    use super::publish_attempt;
    use super::wait_attempt_async;
    use crate::v02::report::RecoveryId;
    use crate::v02::report::RecoveryPhase;
    use crate::v02::report::RecoverySnapshot;
    use crate::v02::report::ResourceId;

    struct CountWake(Arc<std::sync::atomic::AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    struct CallbackWake(Box<dyn Fn() + Send + Sync>);

    impl Wake for CallbackWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            (self.0)();
        }
    }

    fn snapshot(phase: RecoveryPhase, attempts: usize) -> RecoverySnapshot {
        RecoverySnapshot {
            id: RecoveryId::from_raw(7),
            phase,
            resources: Arc::from([ResourceId::new(crate::v02::report::ManagerId::from_raw(1), 2, 3)]),
            attempts,
            last_failures: Arc::from([]),
            failures_total: 0,
            failures_truncated: false,
        }
    }

    #[test]
    fn test_all_observers_wake_and_receive_the_first_published_report() {
        let observation = AttemptObservation::default();
        let first_wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let second_wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let first_waker = Waker::from(Arc::new(CountWake(Arc::clone(&first_wakes))));
        let second_waker = Waker::from(Arc::new(CountWake(Arc::clone(&second_wakes))));
        let mut first = Box::pin(wait_attempt_async(&observation));
        let mut second = Box::pin(wait_attempt_async(&observation));
        assert!(matches!(
            first.as_mut().poll(&mut Context::from_waker(&first_waker)),
            Poll::Pending
        ));
        assert!(matches!(
            second.as_mut().poll(&mut Context::from_waker(&second_waker)),
            Poll::Pending
        ));

        publish_attempt(&observation, snapshot(RecoveryPhase::Resolved, 1));

        assert_eq!(first_wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(second_wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
        let first_report = match first.as_mut().poll(&mut Context::from_waker(&first_waker)) {
            Poll::Ready(report) => report,
            Poll::Pending => panic!("first observer was not ready after notification"),
        };
        let second_report = match second.as_mut().poll(&mut Context::from_waker(&second_waker)) {
            Poll::Ready(report) => report,
            Poll::Pending => panic!("second observer was not ready after notification"),
        };
        assert_eq!(first_report.recovery.id, second_report.recovery.id);
        assert_eq!(first_report.recovery.phase, RecoveryPhase::Resolved);
        assert_eq!(second_report.recovery.phase, RecoveryPhase::Resolved);

        publish_attempt(&observation, snapshot(RecoveryPhase::Pending, 2));
        assert_eq!(
            observation.report.lock().unwrap().as_ref().unwrap().recovery.attempts,
            1
        );
        let mut late = Box::pin(wait_attempt_async(&observation));
        assert!(matches!(
            late.as_mut().poll(&mut Context::from_waker(&first_waker)),
            Poll::Ready(_)
        ));
    }

    #[test]
    fn test_cancelled_waiter_unregisters_and_repoll_replaces_its_waker() {
        let observation = AttemptObservation::default();
        let old_wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let new_wakes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let old_waker = Waker::from(Arc::new(CountWake(Arc::clone(&old_wakes))));
        let new_waker = Waker::from(Arc::new(CountWake(Arc::clone(&new_wakes))));
        let mut waiter = Box::pin(wait_attempt_async(&observation));
        assert!(matches!(
            waiter.as_mut().poll(&mut Context::from_waker(&old_waker)),
            Poll::Pending
        ));
        assert!(matches!(
            waiter.as_mut().poll(&mut Context::from_waker(&new_waker)),
            Poll::Pending
        ));
        drop(waiter);
        assert_eq!(observation.changed.waiter_count(), 0);

        let mut replacement = Box::pin(wait_attempt_async(&observation));
        assert!(matches!(
            replacement.as_mut().poll(&mut Context::from_waker(&old_waker)),
            Poll::Pending
        ));
        assert!(matches!(
            replacement.as_mut().poll(&mut Context::from_waker(&new_waker)),
            Poll::Pending
        ));
        publish_attempt(&observation, snapshot(RecoveryPhase::Resolved, 1));
        assert_eq!(old_wakes.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(new_wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn test_notification_wakes_after_releasing_the_report_lock() {
        let observation = Arc::new(AttemptObservation::default());
        let callback_observation = Arc::clone(&observation);
        let waker = Waker::from(Arc::new(CallbackWake(Box::new(move || {
            assert!(callback_observation.report.try_lock().is_ok());
        }))));
        let mut waiter = Box::pin(wait_attempt_async(&observation));
        assert!(matches!(
            waiter.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        publish_attempt(&observation, snapshot(RecoveryPhase::Resolved, 1));
    }
}

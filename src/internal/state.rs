// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::notify::Notified;
use super::notify::Notify;
use crate::report::ObservationError;
use crate::report::OperationOutcome;
use crate::report::SettlementReport;
use crate::report::StopReason;
use crate::report::StopReport;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

/// Ownership state of the single typed business result.
enum ResultSlot<T, E> {
    /// No result or accepted stop has been published.
    Pending,
    /// A stable outcome is available to one consumer.
    Ready(OperationOutcome<T, E>),
    /// The consumer has already taken the outcome.
    Taken,
    /// The supervisor discarded the typed outcome but retains lifecycle state.
    Discarded,
}

/// Facts serialized with the typed result and terminal settlement snapshot.
struct State<T, E> {
    /// The one-shot typed result slot.
    result: ResultSlot<T, E>,
    /// First accepted stop reason, independent of the business result.
    stop: Option<StopReport>,
    /// Whether a successful outcome was published before any stop request.
    success_published: bool,
    /// Immutable settlement report shared by all later observers.
    settlement: Option<Arc<SettlementReport>>,
}

/// Shared, single-consumer business result and repeatable settlement snapshot.
pub(crate) struct StateCell<T, E> {
    /// Serializes all business, stop, and settlement facts.
    state: Mutex<State<T, E>>,
    /// Notifies both blocking and async result observers after publication.
    changed: Notify,
}

impl<T, E> StateCell<T, E> {
    /// Create a pending result cell with no stop or settlement facts.
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(State {
                result: ResultSlot::Pending,
                stop: None,
                success_published: false,
                settlement: None,
            }),
            changed: Notify::new(),
        }
    }

    /// Publish the first business outcome. A previously accepted stop wins.
    pub(crate) fn publish(
        &self,
        outcome: OperationOutcome<T, E>,
    ) -> Result<(), OperationOutcome<T, E>> {
        let mut state = lock(&self.state);
        if !matches!(state.result, ResultSlot::Pending) || state.stop.is_some() {
            drop(state);
            return Err(outcome);
        }
        if matches!(&outcome, OperationOutcome::Succeeded(_)) {
            state.success_published = true;
        }
        if let OperationOutcome::Stopped(report) = &outcome {
            state.stop = Some(report.clone());
        }
        state.result = ResultSlot::Ready(outcome);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    /// Accept the first stop reason if no business outcome has been published.
    pub(crate) fn request_stop(
        &self,
        reason: StopReason,
        accepted_at: Option<crate::report::MonotonicTime>,
    ) -> bool {
        let accepted = {
            let mut state = lock(&self.state);
            if state.success_published {
                false
            } else if state.stop.is_some() {
                false
            } else {
                let report = StopReport {
                    reason,
                    accepted_at,
                };
                state.stop = Some(report.clone());
                if matches!(state.result, ResultSlot::Pending) {
                    state.result = ResultSlot::Ready(OperationOutcome::Stopped(report));
                }
                true
            }
        };
        if accepted {
            self.changed.notify_waiters();
        }
        accepted
    }

    /// Take the typed result once; subsequent consumers receive `AlreadyTaken`.
    pub(crate) fn take_result_blocking(&self) -> Result<OperationOutcome<T, E>, ObservationError> {
        loop {
            let generation = self.changed.generation();
            if let Some(result) = self.take_if_ready()? {
                return Ok(result);
            }
            self.changed.wait_blocking_since(generation);
        }
    }

    /// Observe and consume the typed result asynchronously.
    pub(crate) fn result(&self) -> ResultFuture<'_, T, E> {
        ResultFuture {
            cell: self,
            notification: None,
        }
    }

    /// Discard an available result without dropping its user value under the state lock.
    pub(crate) fn discard_result(&self) -> bool {
        let discarded = {
            let mut state = lock(&self.state);
            match std::mem::replace(&mut state.result, ResultSlot::Discarded) {
                ResultSlot::Ready(result) => Some(result),
                ResultSlot::Pending => {
                    state.result = ResultSlot::Pending;
                    None
                }
                ResultSlot::Taken => {
                    state.result = ResultSlot::Taken;
                    None
                }
                ResultSlot::Discarded => None,
            }
        };
        if let Some(result) = discarded {
            drop(result);
            self.changed.notify_waiters();
            true
        } else {
            false
        }
    }

    /// Publish one immutable terminal snapshot for repeatable observation.
    pub(crate) fn publish_settlement(
        &self,
        report: SettlementReport,
    ) -> Result<Arc<SettlementReport>, SettlementReport> {
        let mut state = lock(&self.state);
        if state.settlement.is_some() {
            drop(state);
            return Err(report);
        }
        let report = Arc::new(report);
        state.settlement = Some(Arc::clone(&report));
        drop(state);
        self.changed.notify_waiters();
        Ok(report)
    }

    /// Return the stable settlement snapshot for repeated observation.
    pub(crate) fn settlement(&self) -> Option<Arc<SettlementReport>> {
        lock(&self.state).settlement.as_ref().map(Arc::clone)
    }

    /// Return the first accepted stop fact without consuming the business result.
    pub(crate) fn stop_report(&self) -> Option<StopReport> {
        lock(&self.state).stop.clone()
    }

    fn take_if_ready(&self) -> Result<Option<OperationOutcome<T, E>>, ObservationError> {
        let mut state = lock(&self.state);
        match std::mem::replace(&mut state.result, ResultSlot::Taken) {
            ResultSlot::Pending => {
                state.result = ResultSlot::Pending;
                Ok(None)
            }
            ResultSlot::Ready(result) => Ok(Some(result)),
            ResultSlot::Taken => {
                state.result = ResultSlot::Taken;
                Err(ObservationError::AlreadyTaken)
            }
            ResultSlot::Discarded => {
                state.result = ResultSlot::Discarded;
                Err(ObservationError::ResultDiscarded)
            }
        }
    }
}

/// Future that takes the result on readiness and unregisters on drop.
pub(crate) struct ResultFuture<'a, T, E> {
    /// Shared cell whose result this future may consume once.
    cell: &'a StateCell<T, E>,
    /// Current generation waiter, dropped automatically if observation is cancelled.
    notification: Option<Notified>,
}

impl<T, E> Future for ResultFuture<'_, T, E> {
    type Output = Result<OperationOutcome<T, E>, ObservationError>;

    /// Take a ready result or register this future for the next state change.
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let generation = self.cell.changed.generation();
            match self.cell.take_if_ready() {
                Ok(Some(result)) => return Poll::Ready(Ok(result)),
                Err(error) => return Poll::Ready(Err(error)),
                Ok(None) => {}
            }

            if self.notification.is_none() {
                self.notification = Some(self.cell.changed.notified_since(generation));
            }
            let notification = self.notification.as_mut().expect("notification was set");
            match Pin::new(notification).poll(context) {
                Poll::Ready(()) => self.notification = None,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Recover a poisoned internal synchronization lock without exposing poison.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::StateCell;
    use crate::report::ObservationError;
    use crate::report::OperationId;
    use crate::report::OperationOutcome;
    use crate::report::ResourcePhase;
    use crate::report::SettlementReport;
    use crate::report::StopReason;
    use crate::report::StopReport;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread;
    use std::time::Duration;

    #[derive(Debug, Eq, PartialEq)]
    struct NonCloneError(&'static str);

    struct CountingWake(Arc<Mutex<usize>>);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            *self.0.lock().unwrap() += 1;
        }

        fn wake_by_ref(self: &Arc<Self>) {
            *self.0.lock().unwrap() += 1;
        }
    }

    fn counting_waker() -> (Waker, Arc<Mutex<usize>>) {
        let count = Arc::new(Mutex::new(0));
        (
            Waker::from(Arc::new(CountingWake(Arc::clone(&count)))),
            count,
        )
    }

    #[test]
    fn test_completed_result_can_be_taken_once_without_clone() {
        let cell = StateCell::<usize, NonCloneError>::new();
        cell.publish(OperationOutcome::Failed(NonCloneError("original")))
            .unwrap();

        assert_eq!(
            cell.take_result_blocking().unwrap(),
            OperationOutcome::Failed(NonCloneError("original"))
        );
        assert_eq!(
            cell.take_result_blocking(),
            Err(ObservationError::AlreadyTaken)
        );
    }

    #[test]
    fn test_stop_accepted_before_success_prevents_result_from_flipping() {
        let cell = StateCell::<usize, NonCloneError>::new();
        assert!(cell.request_stop(StopReason::UserCancelled, None));
        assert!(!cell.request_stop(StopReason::Timeout, None));

        assert_eq!(
            cell.take_result_blocking().unwrap(),
            OperationOutcome::Stopped(StopReport {
                reason: StopReason::UserCancelled,
                accepted_at: None,
            })
        );
        assert!(cell.publish(OperationOutcome::Succeeded(7)).is_err());
        assert_eq!(
            cell.take_result_blocking(),
            Err(ObservationError::AlreadyTaken)
        );
    }

    #[test]
    fn test_published_success_wins_over_later_stop_request() {
        let cell = StateCell::<usize, NonCloneError>::new();
        cell.publish(OperationOutcome::Succeeded(11)).unwrap();

        assert!(!cell.request_stop(StopReason::UserCancelled, None));
        assert_eq!(
            cell.take_result_blocking().unwrap(),
            OperationOutcome::Succeeded(11)
        );
    }

    #[test]
    fn test_later_stop_is_recorded_without_overwriting_a_failure() {
        let cell = StateCell::<usize, NonCloneError>::new();
        cell.publish(OperationOutcome::Failed(NonCloneError("failed")))
            .unwrap();

        assert!(cell.request_stop(StopReason::UserCancelled, None));
        assert_eq!(
            cell.take_result_blocking().unwrap(),
            OperationOutcome::Failed(NonCloneError("failed"))
        );
        assert_eq!(
            cell.stop_report(),
            Some(StopReport {
                reason: StopReason::UserCancelled,
                accepted_at: None,
            })
        );
    }

    #[test]
    fn test_waiting_future_drop_preserves_result_for_next_waiter() {
        let cell = StateCell::<usize, NonCloneError>::new();
        let (first_waker, _) = counting_waker();
        let mut first_context = Context::from_waker(&first_waker);
        let mut first = Box::pin(cell.result());
        assert!(matches!(
            first.as_mut().poll(&mut first_context),
            Poll::Pending
        ));
        drop(first);

        cell.publish(OperationOutcome::Succeeded(23)).unwrap();
        let (second_waker, _) = counting_waker();
        let mut second_context = Context::from_waker(&second_waker);
        let mut second = Box::pin(cell.result());
        assert!(matches!(
            second.as_mut().poll(&mut second_context),
            Poll::Ready(Ok(OperationOutcome::Succeeded(23)))
        ));
    }

    #[test]
    fn test_registered_result_waiter_is_woken_by_publication() {
        let cell = StateCell::<usize, NonCloneError>::new();
        let (waker, wake_count) = counting_waker();
        let mut context = Context::from_waker(&waker);
        let mut result = Box::pin(cell.result());
        assert!(matches!(result.as_mut().poll(&mut context), Poll::Pending));

        cell.publish(OperationOutcome::Succeeded(29)).unwrap();

        assert_eq!(*wake_count.lock().unwrap(), 1);
        assert!(matches!(
            result.as_mut().poll(&mut context),
            Poll::Ready(Ok(OperationOutcome::Succeeded(29)))
        ));
    }

    #[test]
    fn test_condvar_waiter_observes_published_snapshot() {
        let cell = Arc::new(StateCell::<usize, NonCloneError>::new());
        let rendezvous = Arc::new((Mutex::new(false), Condvar::new()));
        let (sender, receiver) = mpsc::channel();
        let child_cell = Arc::clone(&cell);
        let child_rendezvous = Arc::clone(&rendezvous);
        let waiter = thread::spawn(move || {
            let (lock, condition) = &*child_rendezvous;
            let mut registered = lock.lock().unwrap();
            *registered = true;
            condition.notify_one();
            drop(registered);
            let result = child_cell.take_result_blocking().unwrap();
            sender.send(result).unwrap();
        });

        let (lock, condition) = &*rendezvous;
        let mut registered = lock.lock().unwrap();
        while !*registered {
            let (next, timeout) = condition
                .wait_timeout(registered, Duration::from_secs(2))
                .unwrap();
            registered = next;
            assert!(!timeout.timed_out(), "waiter did not reach its gate");
        }
        drop(registered);
        cell.publish(OperationOutcome::Succeeded(31)).unwrap();

        assert_eq!(
            receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("result waiter did not receive the published snapshot"),
            OperationOutcome::Succeeded(31)
        );
        waiter.join().unwrap();
    }

    #[test]
    fn test_async_and_blocking_observers_share_one_result_slot() {
        let cell = StateCell::<usize, NonCloneError>::new();
        cell.publish(OperationOutcome::Succeeded(41)).unwrap();

        let (waker, _) = counting_waker();
        let mut context = Context::from_waker(&waker);
        let mut result = Box::pin(cell.result());
        assert!(matches!(
            Pin::as_mut(&mut result).poll(&mut context),
            Poll::Ready(Ok(OperationOutcome::Succeeded(41)))
        ));
        assert_eq!(
            cell.take_result_blocking(),
            Err(ObservationError::AlreadyTaken)
        );
    }

    #[test]
    fn test_settlement_snapshot_is_stable_and_repeatable() {
        let cell = StateCell::<usize, NonCloneError>::new();
        let report = SettlementReport {
            operation_id: OperationId::new(9),
            resource_phase: ResourcePhase::Released,
            all_released: true,
            children: Arc::from([]),
            cleanup_failures: Arc::from([]),
            transfers: Arc::from([]),
            unconfirmed_count: 0,
        };
        let published = cell.publish_settlement(report).unwrap();
        let first_observation = cell.settlement().unwrap();
        let second_observation = cell.settlement().unwrap();

        assert!(Arc::ptr_eq(&published, &first_observation));
        assert!(Arc::ptr_eq(&first_observation, &second_observation));
        assert!(cell.publish_settlement((*published).clone()).is_err());
    }

    #[test]
    fn test_discarded_result_stays_discarded_for_every_observer() {
        let cell = StateCell::<usize, NonCloneError>::new();
        cell.publish(OperationOutcome::Succeeded(53)).unwrap();

        assert!(cell.discard_result());
        assert!(!cell.discard_result());
        assert_eq!(
            cell.take_result_blocking(),
            Err(ObservationError::ResultDiscarded)
        );
        assert_eq!(
            cell.take_result_blocking(),
            Err(ObservationError::ResultDiscarded)
        );
    }
}

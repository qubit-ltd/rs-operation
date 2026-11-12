// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the crate-internal state cell contract.

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

use crate::internal::state::StateCell;
use crate::report::ObservationError;
use crate::report::OperationId;
use crate::report::OperationOutcome;
use crate::report::ResourcePhase;
use crate::report::SettlementReport;
use crate::report::StopReason;
use crate::report::StopReport;

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
    (Waker::from(Arc::new(CountingWake(Arc::clone(&count)))), count)
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
    assert_eq!(cell.take_result_blocking(), Err(ObservationError::AlreadyTaken));
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
    assert_eq!(cell.take_result_blocking(), Err(ObservationError::AlreadyTaken));
}

#[test]
fn test_published_success_wins_over_later_stop_request() {
    let cell = StateCell::<usize, NonCloneError>::new();
    cell.publish(OperationOutcome::Succeeded(11)).unwrap();

    assert!(!cell.request_stop(StopReason::UserCancelled, None));
    assert_eq!(cell.take_result_blocking().unwrap(), OperationOutcome::Succeeded(11));
}

#[test]
fn test_later_stop_is_recorded_without_overwriting_a_failure() {
    let cell = StateCell::<usize, NonCloneError>::new();
    cell.publish(OperationOutcome::Failed(NonCloneError("failed"))).unwrap();

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
    assert!(matches!(first.as_mut().poll(&mut first_context), Poll::Pending));
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
fn test_result_waiter_capacity_error_is_propagated() {
    let cell = StateCell::<usize, NonCloneError>::new();
    let (waker, _) = counting_waker();
    let mut context = Context::from_waker(&waker);
    let mut waiters = (0..crate::internal::notify::ASYNC_WAITER_CAPACITY)
        .map(|_| Box::pin(cell.result()))
        .collect::<Vec<_>>();

    for waiter in &mut waiters {
        assert!(matches!(waiter.as_mut().poll(&mut context), Poll::Pending));
    }

    let mut overflow = Box::pin(cell.result());
    assert!(matches!(
        overflow.as_mut().poll(&mut context),
        Poll::Ready(Err(ObservationError::WaiterCapacityExceeded))
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
        let (next, timeout) = condition.wait_timeout(registered, Duration::from_secs(2)).unwrap();
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
    assert_eq!(cell.take_result_blocking(), Err(ObservationError::AlreadyTaken));
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
        effects: Arc::from([]),
        effect_overflow: 0,
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
    assert_eq!(cell.take_result_blocking(), Err(ObservationError::ResultDiscarded));
    assert_eq!(cell.take_result_blocking(), Err(ObservationError::ResultDiscarded));
}

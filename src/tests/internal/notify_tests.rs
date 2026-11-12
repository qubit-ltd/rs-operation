// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the crate-internal notification contract.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use crate::internal::notify::ASYNC_WAITER_CAPACITY;
use crate::internal::notify::Notify;
use crate::report::ObservationError;

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
fn test_notification_before_wait_is_latched() {
    let notify = Notify::new();
    let observed = notify.generation();
    notify.notify_waiters();
    let mut notified = Box::pin(notify.notified_since(observed));
    let (waker, _) = counting_waker();
    let mut context = Context::from_waker(&waker);

    assert!(matches!(
        Pin::as_mut(&mut notified).poll(&mut context),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn test_notification_after_wait_registration_wakes_once() {
    let notify = Notify::new();
    let mut notified = Box::pin(notify.notified());
    let (waker, wake_count) = counting_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(Pin::as_mut(&mut notified).poll(&mut context), Poll::Pending));

    notify.notify_waiters();

    assert_eq!(*wake_count.lock().unwrap(), 1);
    assert!(matches!(
        Pin::as_mut(&mut notified).poll(&mut context),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn test_observers_keep_independent_wakers_and_repeated_poll_is_stable() {
    let notify = Notify::new();
    let mut first = Box::pin(notify.notified());
    let mut second = Box::pin(notify.notified());
    let (first_waker, first_count) = counting_waker();
    let (second_waker, second_count) = counting_waker();
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);

    assert!(matches!(
        Pin::as_mut(&mut first).poll(&mut first_context),
        Poll::Pending
    ));
    assert!(matches!(
        Pin::as_mut(&mut first).poll(&mut first_context),
        Poll::Pending
    ));
    assert!(matches!(
        Pin::as_mut(&mut second).poll(&mut second_context),
        Poll::Pending
    ));

    notify.notify_waiters();

    assert_eq!(*first_count.lock().unwrap(), 1);
    assert_eq!(*second_count.lock().unwrap(), 1);
}

#[test]
fn test_dropping_one_waiter_does_not_remove_another_waiter() {
    let notify = Notify::new();
    let mut first = Box::pin(notify.notified());
    let mut second = Box::pin(notify.notified());
    let (first_waker, first_count) = counting_waker();
    let (second_waker, second_count) = counting_waker();
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);
    assert!(matches!(
        Pin::as_mut(&mut first).poll(&mut first_context),
        Poll::Pending
    ));
    assert!(matches!(
        Pin::as_mut(&mut second).poll(&mut second_context),
        Poll::Pending
    ));
    drop(first);

    notify.notify_waiters();

    assert_eq!(*first_count.lock().unwrap(), 0);
    assert_eq!(*second_count.lock().unwrap(), 1);
}

#[test]
fn test_async_waiter_capacity_is_enforced_and_reclaimed_on_drop() {
    let notify = Notify::new();
    let (waker, _) = counting_waker();
    let mut context = Context::from_waker(&waker);
    let mut waiters = (0..ASYNC_WAITER_CAPACITY)
        .map(|_| Box::pin(notify.notified()))
        .collect::<Vec<_>>();

    for waiter in &mut waiters {
        assert!(matches!(waiter.as_mut().poll(&mut context), Poll::Pending));
    }

    let mut overflow = Box::pin(notify.notified());
    assert!(matches!(
        overflow.as_mut().poll(&mut context),
        Poll::Ready(Err(ObservationError::WaiterCapacityExceeded))
    ));
    drop(overflow);

    drop(waiters.pop());
    let mut replacement = Box::pin(notify.notified());
    assert!(matches!(replacement.as_mut().poll(&mut context), Poll::Pending));
}

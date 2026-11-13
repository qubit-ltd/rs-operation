// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavior tests for context stop observation and operation driver
//! integration.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use crate::context::ContextError;
use crate::context::ContextOptions;
use crate::context::OperationContext;
use crate::protocol::StopReason;

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
fn test_stop_signal_wakes_with_first_stop_reason() {
    let context = OperationContext::new(ContextOptions::default());
    let mut signal = Box::pin(context.stop_signal());
    let (waker, wake_count) = counting_waker();
    let mut task_context = Context::from_waker(&waker);

    assert!(matches!(
        Pin::as_mut(&mut signal).poll(&mut task_context),
        Poll::Pending
    ));
    assert!(context.request_stop(StopReason::UserCancelled));
    assert!(!context.request_stop(StopReason::Timeout));
    assert_eq!(*wake_count.lock().unwrap(), 1);
    assert!(matches!(
        Pin::as_mut(&mut signal).poll(&mut task_context),
        Poll::Ready(Ok(StopReason::UserCancelled))
    ));
}

#[test]
fn test_stop_signal_capacity_recovers_when_waiter_is_dropped() {
    let context = OperationContext::new(ContextOptions::default());
    let (waker, _) = counting_waker();
    let mut task_context = Context::from_waker(&waker);
    let mut signals = (0..64).map(|_| Box::pin(context.stop_signal())).collect::<Vec<_>>();
    for signal in &mut signals {
        assert!(matches!(signal.as_mut().poll(&mut task_context), Poll::Pending));
    }

    let mut overflow = Box::pin(context.stop_signal());
    assert!(matches!(
        overflow.as_mut().poll(&mut task_context),
        Poll::Ready(Err(ContextError::WaiterCapacityExceeded))
    ));
    drop(overflow);
    drop(signals.pop());

    let mut replacement = Box::pin(context.stop_signal());
    assert!(matches!(replacement.as_mut().poll(&mut task_context), Poll::Pending));
}

#[test]
fn test_notified_future_completes_when_source_is_dropped() {
    use crate::internal::notify::Notify;

    let source = Notify::new();
    let mut notified = Box::pin(source.notified());
    drop(source);
    let (waker, _) = counting_waker();
    let mut task_context = Context::from_waker(&waker);

    assert!(matches!(
        Pin::as_mut(&mut notified).poll(&mut task_context),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn test_current_thread_waker_unparks_the_waiting_thread() {
    use crate::operation::internal::CurrentThreadWaker;

    let waker = Waker::from(Arc::new(CurrentThreadWaker(std::thread::current())));
    waker.wake_by_ref();
    std::thread::park();
}

#[test]
fn test_thread_executor_continues_after_a_job_panics() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use crate::driver::BlockingExecutor;
    use crate::driver::ThreadExecutor;

    let executor = ThreadExecutor::new(1, 2).expect("worker starts");
    let completed = Arc::new(AtomicBool::new(false));
    assert!(executor.execute(|| panic!("isolated job panic")).is_ok());
    let task_completed = Arc::clone(&completed);
    assert!(
        executor
            .execute(move || task_completed.store(true, Ordering::SeqCst))
            .is_ok()
    );
    drop(executor);

    assert!(completed.load(Ordering::SeqCst));
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use qubit_operation::report::AdmissionError;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::supervisor::OperationSupervisor;

fn limits(active_roots: usize, queued_roots: usize, unobserved_results: usize) -> SupervisorLimits {
    SupervisorLimits {
        active_roots,
        queued_roots,
        unobserved_results,
        ..SupervisorLimits::default()
    }
}

#[test]
fn test_root_capacity_and_result_reservations_are_released_independently() {
    let supervisor = OperationSupervisor::new(limits(1, 1, 2));
    let active = supervisor.try_admit().unwrap();
    let queued = supervisor.try_admit().unwrap();
    assert!(queued.is_queued());
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

    drop(active);
    assert_eq!(supervisor.active_roots(), 1);
    assert_eq!(supervisor.queued_roots(), 0);
    let replacement = supervisor.try_admit().unwrap();
    assert!(replacement.is_queued());
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

    drop(queued);
    assert_eq!(supervisor.active_roots(), 1);
    drop(replacement);
    assert_eq!(supervisor.active_roots(), 0);
    assert_eq!(supervisor.reserved_results(), 0);
    assert!(supervisor.try_admit().is_ok());
}

#[test]
fn test_dropping_a_queued_admission_rolls_back_without_running_body() {
    let supervisor = OperationSupervisor::new(limits(0, 1, 1));
    let marker = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let body_marker = marker.clone();
    let queued = supervisor
        .try_admit_body(move || {
            body_marker.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        })
        .unwrap();
    assert!(queued.is_queued());
    drop(queued);
    assert_eq!(marker.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(supervisor.queued_roots(), 0);
    assert_eq!(supervisor.reserved_results(), 0);
}

#[test]
fn test_closure_and_expired_admission_have_stable_errors() {
    let supervisor = OperationSupervisor::new(limits(0, 1, 1));
    supervisor.close();
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Closed);
}

#[test]
fn test_rejected_body_is_returned_unchanged_and_reservations_roll_back() {
    let supervisor = OperationSupervisor::new(limits(0, 0, 1));
    let original = String::from("unstarted body");
    let error = supervisor.try_admit_body(original).unwrap_err();
    assert_eq!(error.reason(), AdmissionError::Busy);
    assert_eq!(error.into_body(), "unstarted body");
    assert_eq!(supervisor.active_roots(), 0);
    assert_eq!(supervisor.queued_roots(), 0);
    assert_eq!(supervisor.reserved_results(), 0);
}

#[test]
fn test_admission_waiter_drop_unregisters_and_close_returns_its_body() {
    use core::future::Future;
    use core::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    let supervisor = OperationSupervisor::new(SupervisorLimits {
        active_roots: 0,
        queued_roots: 1,
        unobserved_results: 2,
        admission_waiters: 1,
        ..SupervisorLimits::default()
    });
    let blocker = supervisor.try_admit().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut dropped = Box::pin(supervisor.start_wait_body(String::from("dropped")).unwrap());
    assert!(matches!(Pin::as_mut(&mut dropped).poll(&mut cx), Poll::Pending));
    assert_eq!(
        supervisor
            .start_wait_body(String::from("over limit"))
            .unwrap_err()
            .into_body(),
        "over limit"
    );
    drop(dropped);
    drop(blocker);

    let blocker = supervisor.try_admit().unwrap();
    let mut waiting = Box::pin(supervisor.start_wait_body(String::from("kept")).unwrap());
    assert!(matches!(Pin::as_mut(&mut waiting).poll(&mut cx), Poll::Pending));
    supervisor.close();
    match Pin::as_mut(&mut waiting).poll(&mut cx) {
        Poll::Ready(Err(error)) => {
            assert_eq!(error.reason(), AdmissionError::Closed);
            assert_eq!(error.into_body(), "kept");
        }
        other => panic!("expected closed admission error, got {other:?}"),
    }
    drop(blocker);
}

#[test]
fn test_business_capacity_does_not_consume_cleanup_capacity() {
    let supervisor = OperationSupervisor::new(SupervisorLimits {
        active_roots: 1,
        queued_roots: 0,
        unobserved_results: 1,
        cleanup_concurrency: 1,
        ..SupervisorLimits::default()
    });
    let _root = supervisor.try_admit().unwrap();
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
    let _cleanup = supervisor.try_reserve_cleanup().unwrap();
    assert_eq!(supervisor.cleanup_active(), 1);
}

#[test]
fn test_released_capacity_wakes_waiter_and_admits_without_running_body() {
    use core::future::Future;
    use core::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    struct CountWake(Arc<AtomicUsize>);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let supervisor = OperationSupervisor::new(SupervisorLimits {
        active_roots: 0,
        queued_roots: 1,
        unobserved_results: 2,
        ..SupervisorLimits::default()
    });
    let blocker = supervisor.try_admit().unwrap();
    let body_runs = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&body_runs);
    let mut waiter = Box::pin(
        supervisor
            .start_wait_body(move || captured.fetch_add(1, Ordering::SeqCst))
            .unwrap(),
    );
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(Pin::as_mut(&mut waiter).poll(&mut cx), Poll::Pending));
    drop(blocker);
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    let admitted = match Pin::as_mut(&mut waiter).poll(&mut cx) {
        Poll::Ready(Ok(admitted)) => admitted,
        other => panic!("expected admission after capacity release, got {other:?}"),
    };
    assert!(admitted.is_queued());
    assert_eq!(body_runs.load(Ordering::SeqCst), 0);
}

#[test]
fn test_admission_wait_deadline_returns_expired_error_and_unstarted_body() {
    use core::future::Future;
    use core::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use qubit_operation::context::ContextOptions;
    use qubit_operation::context::Deadline;
    use qubit_operation::context::OperationContext;
    use qubit_operation::protocol::ClockSource;
    use qubit_operation::protocol::TimerRegistration;
    use qubit_operation::report::ClockDomainId;
    use qubit_operation::report::MonotonicTime;
    struct TestClock {
        ticks: AtomicU64,
        waker: Mutex<Option<Waker>>,
    }
    struct Registration;
    impl TimerRegistration for Registration {
        fn cancel(self: Box<Self>) {}
    }
    impl ClockSource for TestClock {
        fn now(&self) -> MonotonicTime {
            MonotonicTime::new(ClockDomainId(17), self.ticks.load(Ordering::SeqCst))
        }
        fn register_deadline(&self, _: MonotonicTime, waker: Waker) -> Box<dyn TimerRegistration> {
            *self.waker.lock().unwrap() = Some(waker);
            Box::new(Registration)
        }
    }

    let supervisor = OperationSupervisor::new(SupervisorLimits {
        active_roots: 0,
        queued_roots: 0,
        unobserved_results: 1,
        ..SupervisorLimits::default()
    });
    let clock = Arc::new(TestClock {
        ticks: AtomicU64::new(1),
        waker: Mutex::new(None),
    });
    let context = OperationContext::new(ContextOptions {
        clock: Some(clock.clone()),
        deadline: Some(Deadline::at(MonotonicTime::new(ClockDomainId(17), 2))),
        ..ContextOptions::default()
    });
    let mut waiter = Box::pin(
        supervisor
            .start_wait_body_with_context(String::from("body"), Some(context))
            .unwrap(),
    );
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::as_mut(&mut waiter).poll(&mut cx), Poll::Pending));
    clock.ticks.store(2, Ordering::SeqCst);
    clock.waker.lock().unwrap().take().unwrap().wake();
    match Pin::as_mut(&mut waiter).poll(&mut cx) {
        Poll::Ready(Err(error)) => {
            assert_eq!(error.reason(), AdmissionError::DeadlineExpired);
            assert_eq!(error.into_body(), "body");
        }
        other => panic!("expected deadline error, got {other:?}"),
    }
    assert_eq!(supervisor.queued_roots(), 0);
    assert_eq!(supervisor.reserved_results(), 0);
}

#[test]
fn test_context_stop_wakes_pending_admission_waiter_and_returns_body() {
    use core::future::Future;
    use core::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use qubit_operation::context::ContextOptions;
    use qubit_operation::context::OperationContext;
    use qubit_operation::report::StopReason;
    struct CountWake(Arc<AtomicUsize>);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let supervisor = OperationSupervisor::new(SupervisorLimits {
        active_roots: 0,
        queued_roots: 0,
        unobserved_results: 1,
        admission_waiters: 1,
        ..SupervisorLimits::default()
    });
    let operation_context = OperationContext::new(ContextOptions::default());
    let mut waiter = Box::pin(
        supervisor
            .start_wait_body_with_context(String::from("body"), Some(operation_context.clone()))
            .unwrap(),
    );
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wakes))));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(Pin::as_mut(&mut waiter).poll(&mut cx), Poll::Pending));
    assert!(operation_context.request_stop(StopReason::UserCancelled));
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    match Pin::as_mut(&mut waiter).poll(&mut cx) {
        Poll::Ready(Err(error)) => {
            assert_eq!(error.reason(), AdmissionError::Stopped);
            assert_eq!(error.into_body(), "body");
        }
        other => panic!("expected stop rejection, got {other:?}"),
    }
    assert_eq!(supervisor.reserved_results(), 0);
    let second_waiter = supervisor.start_wait_body("capacity returned").unwrap();
    drop(second_waiter);
}

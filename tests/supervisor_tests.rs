// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public root admission and shutdown contracts.

use core::task::Context;
use core::task::Poll;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_operation::context::OperationContext;
use qubit_operation::report::AdmissionError;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::supervisor::OperationSupervisor;

fn limits(active_roots: usize, queued_roots: usize, unobserved_results: usize) -> SupervisorLimits {
    SupervisorLimits {
        active_roots,
        queued_roots,
        unobserved_results,
        admission_waiters: 1,
        ..SupervisorLimits::default()
    }
}

#[test]
fn test_root_and_result_capacity_are_reserved_and_released_separately() {
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
fn test_dropping_queued_admission_rolls_back_without_calling_its_body() {
    let supervisor = OperationSupervisor::new(limits(0, 1, 1));
    let count = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&count);
    let admission = supervisor
        .try_admit_body(move || captured.fetch_add(1, Ordering::SeqCst))
        .unwrap();
    assert!(admission.is_queued());
    drop(admission);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(supervisor.queued_roots(), 0);
    assert_eq!(supervisor.reserved_results(), 0);
}

#[test]
fn test_close_wakes_waiters_and_they_recover_their_input() {
    use std::task::Waker;
    let supervisor = OperationSupervisor::new(limits(0, 1, 2));
    let _accepted = supervisor.try_admit().unwrap();
    let mut waiting = Box::pin(supervisor.start_wait_body(String::from("body")).unwrap());
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    supervisor.close();
    match waiting.as_mut().poll(&mut context) {
        Poll::Ready(Err(error)) => {
            assert_eq!(error.reason(), AdmissionError::Closed);
            assert_eq!(error.into_body(), "body");
        }
        other => panic!("expected closed error, got {other:?}"),
    }
}

#[test]
fn test_cleanup_capacity_remains_available_when_business_admission_is_full() {
    let supervisor = OperationSupervisor::new(limits(1, 0, 1));
    let _root = supervisor.try_admit().unwrap();
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
    let _cleanup = supervisor.try_reserve_cleanup().unwrap();
    assert_eq!(supervisor.cleanup_active(), 1);
}

#[test]
fn test_expired_context_rejects_and_returns_the_original_body() {
    use std::task::Waker;

    use qubit_operation::context::ContextOptions;
    use qubit_operation::context::Deadline;
    use qubit_operation::protocol::ClockSource;
    use qubit_operation::protocol::TimerRegistration;
    use qubit_operation::report::ClockDomainId;
    use qubit_operation::report::MonotonicTime;

    struct FixedClock;
    struct NoTimer;
    impl TimerRegistration for NoTimer {
        fn cancel(self: Box<Self>) {}
    }
    impl ClockSource for FixedClock {
        fn now(&self) -> MonotonicTime {
            MonotonicTime::new(ClockDomainId(9), 2)
        }
        fn register_deadline(&self, _: MonotonicTime, _: Waker) -> Box<dyn TimerRegistration> {
            Box::new(NoTimer)
        }
    }

    let supervisor = OperationSupervisor::new(limits(1, 1, 1));
    let context = OperationContext::new(ContextOptions {
        clock: Some(Arc::new(FixedClock)),
        deadline: Some(Deadline::at(MonotonicTime::new(ClockDomainId(9), 1))),
        ..ContextOptions::default()
    });
    let error = supervisor
        .try_admit_with_context(String::from("body"), &context)
        .unwrap_err();
    assert_eq!(error.reason(), AdmissionError::DeadlineExpired);
    assert_eq!(error.into_body(), "body");
    assert_eq!(supervisor.active_roots(), 0);
    assert_eq!(supervisor.reserved_results(), 0);
}

#[test]
fn test_close_rejects_new_work() {
    let supervisor = OperationSupervisor::new(limits(0, 1, 1));
    supervisor.close();
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Closed);
}
#[cfg(test)]
mod reentrant_waker_regression {
    use core::task::Context;
    use core::task::RawWaker;
    use core::task::RawWakerVTable;
    use core::task::Waker;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::Arc;
    use super::AtomicUsize;
    use super::Future;
    use super::OperationSupervisor;
    use super::Ordering;
    use super::Poll;
    use super::SupervisorLimits;

    struct Probe {
        supervisor: OperationSupervisor,
        lock_timeouts: Arc<AtomicUsize>,
    }

    fn check_reentry(probe: &Probe) {
        let supervisor = probe.supervisor.clone();
        let (send, receive) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = supervisor.active_roots();
            let _ = send.send(());
        });
        if receive.recv_timeout(Duration::from_millis(100)).is_err() {
            probe.lock_timeouts.fetch_add(1, Ordering::SeqCst);
        }
    }

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        let probe = unsafe { Arc::<Probe>::from_raw(data.cast()) };
        check_reentry(&probe);
        let clone = Arc::clone(&probe);
        std::mem::forget(probe);
        RawWaker::new(Arc::into_raw(clone).cast(), &VTABLE)
    }

    unsafe fn wake_waker(data: *const ()) {
        unsafe { drop_waker(data) };
    }

    unsafe fn wake_by_ref(data: *const ()) {
        let probe = unsafe { &*data.cast::<Probe>() };
        check_reentry(probe);
    }

    unsafe fn drop_waker(data: *const ()) {
        let probe = unsafe { Arc::<Probe>::from_raw(data.cast()) };
        check_reentry(&probe);
        drop(probe);
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref, drop_waker);

    fn reentrant_waker(supervisor: &OperationSupervisor, lock_timeouts: &Arc<AtomicUsize>) -> Waker {
        let probe = Arc::new(Probe {
            supervisor: supervisor.clone(),
            lock_timeouts: Arc::clone(lock_timeouts),
        });
        let raw = RawWaker::new(Arc::into_raw(probe).cast(), &VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    #[test]
    fn test_raw_waker_clone_replacement_and_removal_can_reenter_without_lock() {
        let supervisor = OperationSupervisor::new(SupervisorLimits {
            active_roots: 0,
            queued_roots: 0,
            unobserved_results: 1,
            ..SupervisorLimits::default()
        });
        let lock_timeouts = Arc::new(AtomicUsize::new(0));
        let first_waker = reentrant_waker(&supervisor, &lock_timeouts);
        let second_waker = reentrant_waker(&supervisor, &lock_timeouts);
        let mut first_cx = Context::from_waker(&first_waker);
        let mut second_cx = Context::from_waker(&second_waker);
        let mut waiter = Box::pin(supervisor.start_wait_body(()).unwrap());
        assert!(matches!(waiter.as_mut().poll(&mut first_cx), Poll::Pending));
        // Different RawWaker data forces a replacement and destruction of the old
        // waker.
        assert!(matches!(waiter.as_mut().poll(&mut second_cx), Poll::Pending));
        drop(waiter);
        assert_eq!(lock_timeouts.load(Ordering::SeqCst), 0);
    }
}

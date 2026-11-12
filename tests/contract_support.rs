// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod support;

use std::task::Context;

use qubit_operation::protocol::ClockSource;
use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::report::ClockDomainId;
use qubit_operation::report::MonotonicTime;
use qubit_operation::report::StopReason;
use support::CountingWaker;
use support::FakeControl;
use support::Gate;
use support::ManualClock;
use support::ManualDriver;

#[test]
fn test_gate_reports_start_before_release_and_drop_probe_tracks_real_drop() {
    let gate = Gate::new();
    let (probe, probe_counter): (support::DropProbe, support::DropProbeCounter) = support::DropProbe::new();
    let driver = ManualDriver::new();
    let gate_future = gate.wait();
    let probe_future = async move {
        let _probe = probe;
        gate_future.await;
    };
    let task = driver.spawn("gate", probe_future);

    assert_eq!(driver.poll_once(task), std::task::Poll::Pending);
    assert!(gate.started());
    gate.wait_started();
    assert!(!gate.is_released());
    assert_eq!(probe_counter.drop_count(), 0);

    gate.release();
    gate.wait_released();
    assert!(gate.is_released());
    assert_eq!(driver.poll_once(task), std::task::Poll::Ready(()));
    assert_eq!(probe_counter.drop_count(), 1);
}

#[test]
fn test_manual_clock_advances_only_within_its_domain() {
    let clock = ManualClock::new(ClockDomainId(7));

    assert_eq!(clock.advance_by(5), MonotonicTime::new(ClockDomainId(7), 5));
    assert_eq!(
        clock.advance_to(MonotonicTime::new(ClockDomainId(8), 9)),
        Err(support::ClockAdvanceError::WrongDomain {
            expected: ClockDomainId(7),
            actual: ClockDomainId(8),
        })
    );
    assert_eq!(clock.now(), MonotonicTime::new(ClockDomainId(7), 5));
}

#[test]
fn test_manual_clock_wakes_only_deadlines_crossed_by_advance() {
    let clock = ManualClock::new(ClockDomainId(9));
    let waker = CountingWaker::new();
    let before = clock.register_deadline(MonotonicTime::new(ClockDomainId(9), 4), waker.waker());
    let after = clock.register_deadline(MonotonicTime::new(ClockDomainId(9), 8), waker.waker());

    clock.advance_by(5);

    assert_eq!(waker.wake_count(), 1);
    assert_eq!(clock.pending_timers(), 1);
    after.cancel();
    before.cancel();
    assert_eq!(clock.pending_timers(), 0);
}

#[test]
fn test_fake_control_records_real_stop_calls_and_separates_business_from_release() {
    let control = FakeControl::new();
    let waker = CountingWaker::new();
    let task_waker = waker.waker();
    let mut context = Context::from_waker(&task_waker);

    assert_eq!(control.poll_business(&mut context), std::task::Poll::Pending);
    control.request_stop(StopReason::UserCancelled);
    control.request_stop(StopReason::Timeout);
    assert_eq!(control.stop_requests(), 2);
    assert_eq!(control.first_stop_reason(), Some(StopReason::UserCancelled));
    assert_eq!(waker.wake_count(), 0);
    control.complete_business(support::BusinessResult::Succeeded);
    assert_eq!(waker.wake_count(), 1);
    assert!(matches!(control.poll_business(&mut context), std::task::Poll::Ready(_)));
    assert_eq!(control.poll_released(&mut context), std::task::Poll::Pending);
    control.confirm_release();
    assert_eq!(waker.wake_count(), 2);
    assert!(matches!(control.poll_released(&mut context), std::task::Poll::Ready(_)));
    assert_eq!(control.snapshot().operation_id, control.operation_id());

    let failed_control = FakeControl::new();
    failed_control.complete_business(support::BusinessResult::Failed);
    assert!(matches!(
        failed_control.poll_business(&mut context),
        std::task::Poll::Ready(summary)
            if summary.outcome == qubit_operation::report::ChildBusinessOutcome::Failed
    ));
}

#[test]
fn test_manual_driver_records_actual_scripted_interleaving_and_wakeups() {
    let driver = ManualDriver::new();
    let first: support::TaskId = driver.spawn("first", support::recording_step());
    let second: support::TaskId = driver.spawn("second", support::recording_step());

    assert_eq!(driver.poll_once(first), std::task::Poll::Pending);
    assert_eq!(driver.poll_once(second), std::task::Poll::Pending);
    driver.wake(first);
    assert_eq!(driver.poll_once(first), std::task::Poll::Ready(()));
    driver.finish(second);

    assert_eq!(
        driver.trace(),
        [
            "poll:first",
            "poll:second",
            "wake:first",
            "poll:first",
            "drop:first",
            "drop:second"
        ]
    );
    assert_eq!(driver.wake_count(), 1);
}

#[test]
fn test_fake_cleanup_keeps_pending_branch_pending_until_gate_release() {
    let gate = Gate::new();
    let cleanup = support::FakeCleanup::pending(gate.clone());
    let driver = ManualDriver::new();
    let cleanup_future: support::FakeCleanupFuture = cleanup.into_future();
    let task = driver.spawn("cleanup", async move {
        let _ = cleanup_future.await;
    });

    assert_eq!(driver.poll_once(task), std::task::Poll::Pending);
    assert!(gate.started());
    gate.release();
    assert!(matches!(driver.poll_once(task), std::task::Poll::Ready(_)));
}

#[test]
fn test_fake_cleanup_success_and_panic_follow_the_selected_branch() {
    let success_driver = ManualDriver::new();
    let success = support::FakeCleanup::success().into_future();
    let success_task = success_driver.spawn("success", async move {
        assert!(matches!(
            success.await,
            qubit_operation::report::CleanupOutcome::Cleaned
        ));
    });
    assert_eq!(success_driver.poll_once(success_task), std::task::Poll::Ready(()));

    let panic_waker = CountingWaker::new();
    let panic_task_waker = panic_waker.waker();
    let mut context = Context::from_waker(&panic_task_waker);
    let mut panicking = Box::pin(support::FakeCleanup::panicking().into_future());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        std::future::Future::poll(panicking.as_mut(), &mut context)
    }));

    assert!(panic.is_err(), "the scripted panic branch must panic when polled");
}

#[test]
fn test_counting_waker_counts_only_real_wake_calls() {
    let waker = CountingWaker::new();
    let cloned = waker.waker().clone();

    assert_eq!(waker.wake_count(), 0);
    cloned.wake_by_ref();
    assert_eq!(waker.wake_count(), 1);
    waker.waker().wake_by_ref();
    assert_eq!(waker.wake_count(), 2);
}

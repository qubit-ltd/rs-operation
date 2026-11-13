// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests that inspect the supervisor's private waiter registration state.

use core::future::Future;
use core::task::Context;
use core::task::Poll;

use super::OperationSupervisor;
use super::internal::util::lock;
use crate::report::AdmissionError;
use crate::report::SupervisorLimits;

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
fn test_waiters_are_bounded_cancel_safely_and_recover_rejected_input() {
    let supervisor = OperationSupervisor::new(limits(0, 1, 2));
    let accepted = supervisor.try_admit().unwrap();
    let mut waiting = Box::pin(supervisor.start_wait_body(String::from("kept")).unwrap());
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    let rejected = supervisor.start_wait_body(String::from("rejected")).unwrap_err();
    assert_eq!(rejected.reason(), AdmissionError::Busy);
    assert_eq!(rejected.into_body(), "rejected");
    drop(waiting);
    drop(accepted);
    assert_eq!(lock(&supervisor.inner.state).waiters.len(), 0);
}

#[test]
fn test_transferred_admission_keeps_capacity_until_both_leases_release() {
    let supervisor = OperationSupervisor::new(limits(1, 0, 1));
    let mut admission = supervisor.try_admit_body("body").unwrap();
    let admitted = admission.transfer().expect("first transfer succeeds");
    drop(admission);

    {
        let state = lock(&supervisor.inner.state);
        assert_eq!(state.active, 1);
        assert_eq!(state.results, 1);
    }

    let (_body, execution, result) = admitted.into_parts();
    drop(execution);
    {
        let state = lock(&supervisor.inner.state);
        assert_eq!(state.active, 0);
        assert_eq!(state.results, 1);
    }
    drop(result);
    let state = lock(&supervisor.inner.state);
    assert_eq!(state.results, 0);
}

#[test]
fn test_queued_task_lease_wakes_and_becomes_active_after_capacity_release() {
    let supervisor = OperationSupervisor::new(limits(1, 1, 2));
    let mut active = supervisor.try_admit_body("active").unwrap();
    let active = active.transfer().unwrap();
    let mut queued = supervisor.try_admit_body("queued").unwrap();
    assert!(queued.is_queued());
    let queued = queued.transfer().unwrap();
    let (_active_body, active_lease, _active_result) = active.into_parts();
    let (_queued_body, queued_lease, _queued_result) = queued.into_parts();
    let mut task_context = Context::from_waker(std::task::Waker::noop());

    assert!(matches!(queued_lease.poll_active(&mut task_context), Poll::Pending));
    drop(active_lease);
    assert!(matches!(queued_lease.poll_active(&mut task_context), Poll::Ready(true)));
}

#[test]
fn test_context_admission_errors_map_to_public_admission_categories() {
    use crate::context::ContextError;
    use crate::context::ContextOptions;
    use crate::context::OperationContext;
    use crate::protocol::StopReason;
    use crate::supervisor::admission_wait::context_admission_error;

    let context = OperationContext::new(ContextOptions::default());
    let cases = [
        (ContextError::DeadlineExpired, AdmissionError::DeadlineExpired),
        (ContextError::BudgetUnavailable, AdmissionError::ReservationLimit),
        (ContextError::WaiterCapacityExceeded, AdmissionError::Busy),
        (ContextError::ClockUnavailable, AdmissionError::DriverUnavailable),
        (ContextError::ClockDomainMismatch, AdmissionError::DriverUnavailable),
        (ContextError::Stopped, AdmissionError::Stopped),
    ];
    for (context_error, expected) in cases {
        assert_eq!(context_admission_error(&context, context_error), expected);
    }
    context.request_stop(StopReason::Timeout);
    assert_eq!(
        context_admission_error(&context, ContextError::Stopped),
        AdmissionError::DeadlineExpired
    );
}

#[test]
fn test_cloned_shutdown_observer_reports_current_supervisor_state() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let observer = supervisor.shutdown_observer();
    let cloned = observer.clone();

    let report = cloned.wait_blocking(std::time::Duration::ZERO).report;
    assert_eq!(report.pending_requests, 0);
    assert_eq!(report.pending_resources, 0);
}

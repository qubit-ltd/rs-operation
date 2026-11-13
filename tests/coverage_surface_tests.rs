// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use qubit_operation::context::BudgetLease;
use qubit_operation::context::BudgetLeaseError;
use qubit_operation::context::ContextError;
use qubit_operation::context::ContextOptions;
use qubit_operation::context::Deadline;
use qubit_operation::context::OperationContext;
use qubit_operation::driver::DriverLease;
use qubit_operation::driver::DriverStatus;
use qubit_operation::driver::ExternalDriverGuard;
use qubit_operation::protocol::ClockSource;
use qubit_operation::protocol::TimerRegistration;
use qubit_operation::report::AdmissionError;
use qubit_operation::report::ClockDomainId;
use qubit_operation::report::DiagnosticSummary;
use qubit_operation::report::MonotonicTime;
use qubit_operation::report::OperationId;
use qubit_operation::report::OperationOutcome;
use qubit_operation::report::StartError;
use qubit_operation::report::StopReason;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::resource::CleanupScheduler;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

#[test]
fn test_external_driver_settlement_observations() {
    let lease = DriverLease::default();
    assert_eq!(lease.status(), DriverStatus::Active);
    lease.mark_released().expect("lease is active");
    assert!(lease.is_released());
    assert!(!lease.is_unconfirmed());

    let guard = ExternalDriverGuard::default();
    let observed = guard.lease();
    guard.release().expect("guard lease is active");
    assert_eq!(observed.status(), DriverStatus::Released);
}

#[test]
fn test_cleanup_capacity_reservation_and_rejection() {
    let scheduler = CleanupScheduler::new(2);
    let responsibility = scheduler.responsibility_owner();
    assert_eq!(scheduler.limit(), 2);
    assert_eq!(responsibility.limit(), 2);
    assert_eq!(scheduler.active(), 0);

    let first = scheduler.try_acquire().expect("first slot is available");
    let second = scheduler.try_acquire().expect("second slot is available");
    assert_eq!(scheduler.active(), 2);
    assert!(scheduler.try_acquire().is_none());
    drop(first);
    assert_eq!(scheduler.active(), 1);
    drop(second);
    assert_eq!(scheduler.active(), 0);
}

#[test]
fn test_rejected_start_preserves_reason_and_unstarted_body() {
    let rejection = StartError::new(AdmissionError::Busy, String::from("unstarted"));
    assert_eq!(rejection.reason(), AdmissionError::Busy);
    assert_eq!(rejection.to_string(), AdmissionError::Busy.to_string());
    assert!(format!("{rejection:?}").contains("Busy"));

    let (reason, body) = rejection.into_parts();
    assert_eq!(reason, AdmissionError::Busy);
    assert_eq!(body, "unstarted");
}

#[test]
fn test_service_handle_can_cancel_and_repeat_settlement_observation() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(2).expect("service capacity is valid");
    let mut handle = service
        .start(ScopeOptions::default(), |_scope| async { Ok::<_, &'static str>(7_u8) })
        .expect("service accepts the operation");
    let observer = handle.observer();
    handle.request_cancel(StopReason::UserCancelled);

    let outcome = handle.result_blocking().expect("result can be observed once");
    assert!(matches!(
        outcome,
        OperationOutcome::Succeeded(7) | OperationOutcome::Stopped(_)
    ));
    assert!(handle.released_blocking().expect("release settles").all_released);
    assert!(
        observer
            .released_blocking()
            .expect("observer can repeat settlement")
            .all_released
    );

    let service_report = service.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    assert!(service_report.is_complete());
    assert!(
        supervisor
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1))
            .is_complete()
    );
}

#[test]
fn test_service_handle_async_result_and_release_observation() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).expect("service capacity is valid");
    let mut handle = service
        .start(ScopeOptions::default(), |_scope| async { Ok::<_, &'static str>(11_u8) })
        .expect("service accepts the operation");

    assert!(matches!(
        block_on(handle.result()).expect("result is available"),
        OperationOutcome::Succeeded(11)
    ));
    assert!(block_on(handle.released()).all_released);
    assert!(
        service
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1))
            .is_complete()
    );
    assert!(
        supervisor
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1))
            .is_complete()
    );
}

#[test]
fn test_small_public_value_accessors_report_stable_values() {
    let operation = OperationId::new(42);
    assert_eq!(operation.get(), 42);
    assert_eq!(
        MonotonicTime::new(qubit_operation::report::ClockDomainId(3), 10)
            .checked_duration_since(MonotonicTime::new(qubit_operation::report::ClockDomainId(3), 4)),
        Some(6)
    );
    assert_eq!(
        MonotonicTime::new(qubit_operation::report::ClockDomainId(3), 4)
            .checked_duration_since(MonotonicTime::new(qubit_operation::report::ClockDomainId(3), 10)),
        None
    );
    assert_eq!(
        MonotonicTime::new(qubit_operation::report::ClockDomainId(4), 10)
            .checked_duration_since(MonotonicTime::new(qubit_operation::report::ClockDomainId(3), 4)),
        None
    );

    let description = DiagnosticSummary::new("persisted checkpoint");
    assert_eq!(AsRef::<str>::as_ref(&description), "persisted checkpoint");
    assert_eq!(description.to_string(), "persisted checkpoint");
}

#[test]
fn test_operation_context_inherits_budget_deadline_and_stop() {
    let now = MonotonicTime::new(ClockDomainId(19), 4);
    let clock: Arc<dyn ClockSource> = Arc::new(StaticClock(now));
    let parent_deadline = Deadline::from(MonotonicTime::new(ClockDomainId(19), 12));
    let parent = OperationContext::new(ContextOptions {
        clock: Some(Arc::clone(&clock)),
        deadline: Some(parent_deadline),
        budget: Some(Arc::new(AvailableBudget)),
    });
    assert_eq!(parent.deadline(), Some(parent_deadline));
    assert!(!parent.is_expired());
    parent.admission_check().expect("parent deadline and budget allow work");

    let child_deadline = Deadline::at(MonotonicTime::new(ClockDomainId(19), 9));
    let child = parent
        .child(ContextOptions {
            deadline: Some(child_deadline),
            budget: Some(Arc::new(AvailableBudget)),
            ..ContextOptions::default()
        })
        .expect("child inherits compatible clock policy");
    assert_eq!(child.deadline(), Some(child_deadline));
    child.admission_check().expect("both inherited budgets allow work");

    let denied = child
        .child(ContextOptions {
            budget: Some(Arc::new(DeniedBudget)),
            ..ContextOptions::default()
        })
        .expect("child can add its own budget policy");
    assert_eq!(denied.admission_check(), Err(ContextError::BudgetUnavailable));

    let mismatch = parent.child(ContextOptions {
        deadline: Some(Deadline::at(MonotonicTime::new(ClockDomainId(20), 5))),
        ..ContextOptions::default()
    });
    assert!(matches!(mismatch, Err(ContextError::ClockDomainMismatch)));

    assert!(parent.request_stop(StopReason::UserCancelled));
    assert!(!parent.request_stop(StopReason::Timeout));
    assert_eq!(parent.stop_reason(), Some(StopReason::UserCancelled));
    assert_eq!(child.stop_reason(), Some(StopReason::ParentStopped));

    let cleanup = OperationContext::cleanup_context(ContextOptions::default());
    assert_eq!(cleanup.stop_reason(), None);
    assert_eq!(cleanup.deadline(), None);
}

#[test]
fn test_expired_context_requests_timeout_and_deadline_signal_requires_matching_clock() {
    let clock: Arc<dyn ClockSource> = Arc::new(StaticClock(MonotonicTime::new(ClockDomainId(21), 10)));
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(21), 8));
    let expired = OperationContext::new(ContextOptions {
        clock: Some(Arc::clone(&clock)),
        deadline: Some(deadline),
        ..ContextOptions::default()
    });
    assert!(expired.is_expired());
    assert!(expired.deadline_signal().is_some());
    assert_eq!(expired.admission_check(), Err(ContextError::DeadlineExpired));
    assert_eq!(expired.stop_reason(), Some(StopReason::Timeout));

    let wrong_clock: Arc<dyn ClockSource> = Arc::new(StaticClock(MonotonicTime::new(ClockDomainId(22), 10)));
    let invalid = OperationContext::new(ContextOptions {
        clock: Some(wrong_clock),
        deadline: Some(deadline),
        ..ContextOptions::default()
    });
    assert!(!invalid.is_expired());
    assert!(!invalid.deadline_signal().is_some());
    assert_eq!(invalid.admission_check(), Err(ContextError::ClockDomainMismatch));
}

struct StaticClock(MonotonicTime);

impl ClockSource for StaticClock {
    fn now(&self) -> MonotonicTime {
        self.0
    }

    fn register_deadline(&self, _: MonotonicTime, _: Waker) -> Box<dyn TimerRegistration> {
        Box::new(NoopTimer)
    }
}

struct NoopTimer;

impl TimerRegistration for NoopTimer {
    fn cancel(self: Box<Self>) {}
}

struct AvailableBudget;

impl BudgetLease for AvailableBudget {
    fn check(&self) -> Result<(), BudgetLeaseError> {
        Ok(())
    }
}

struct DeniedBudget;

impl BudgetLease for DeniedBudget {
    fn check(&self) -> Result<(), BudgetLeaseError> {
        Err(BudgetLeaseError)
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

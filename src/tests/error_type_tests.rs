// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for public error and value types with stable diagnostics or accessors.

use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;

use crate::context::BudgetLeaseError;
use crate::context::ContextError;
use crate::driver::BlockingExecutor;
use crate::driver::ExecutorError;
use crate::driver::ExternalDriverError;
use crate::driver::PortableDriver;
use crate::driver::PortableDriverConfig;
use crate::driver::PortableDriverError;
use crate::driver::RejectedJob;
use crate::report::AdmissionError;
use crate::report::CleanupFailure;
use crate::report::CleanupStage;
use crate::report::DiagnosticSummary;
use crate::report::ObservationError;
use crate::report::OperationId;
use crate::report::ResourceId;
use crate::resource::DependencyError;
use crate::resource::RegistryError;
use crate::resource::ResourceError;
use crate::resource::TransferError;

fn assert_error<E: Error>() {}

fn assert_display<T: Display>(value: T, expected: &str) {
    assert_eq!(value.to_string(), expected);
}

#[test]
fn test_budget_lease_error_has_stable_display_and_error_trait() {
    assert_display(BudgetLeaseError, "operation budget lease is unavailable");
    assert_error::<BudgetLeaseError>();
    assert!(Error::source(&BudgetLeaseError).is_none());
}

#[test]
fn test_context_error_displays_each_context_failure() {
    let cases = [
        (
            ContextError::ClockDomainMismatch,
            "operation deadlines use different clock domains",
        ),
        (
            ContextError::ClockUnavailable,
            "operation deadline clock is unavailable",
        ),
        (ContextError::Stopped, "operation context has been stopped"),
        (ContextError::DeadlineExpired, "operation deadline has expired"),
        (ContextError::BudgetUnavailable, "operation budget lease is unavailable"),
        (
            ContextError::WaiterCapacityExceeded,
            "operation context waiter capacity is full",
        ),
    ];
    for (error, expected) in cases {
        assert_display(error, expected);
    }
}

#[test]
fn test_external_driver_error_reports_settled_lease() {
    assert_display(
        ExternalDriverError::AlreadySettled,
        "external driver lease has already been settled",
    );
    assert_error::<ExternalDriverError>();
}

#[test]
fn test_portable_driver_error_displays_each_failure() {
    let cases = [
        (
            PortableDriverError::InvalidConfiguration,
            "portable driver limits must be positive",
        ),
        (PortableDriverError::Capacity, "portable driver task capacity is full"),
        (PortableDriverError::Closed, "portable driver is closed"),
        (
            PortableDriverError::WorkerSpawn,
            "portable driver worker could not be started",
        ),
    ];
    for (error, expected) in cases {
        assert_display(error, expected);
    }
    assert_error::<PortableDriverError>();
}

#[test]
fn test_task_handle_exposes_stable_nonzero_driver_identity() {
    let driver = PortableDriver::new(PortableDriverConfig {
        worker_count: 1,
        task_capacity: 2,
        polls_per_turn: 1,
    })
    .expect("valid driver configuration starts");
    let task = driver.spawn(async {}).expect("driver accepts task");
    let cloned = task;
    assert_ne!(task.as_u64(), 0);
    assert_eq!(task.as_u64(), cloned.as_u64());
}

struct LocalBlockingExecutor;

impl BlockingExecutor for LocalBlockingExecutor {
    fn execute<F>(&self, _: F) -> Result<(), RejectedJob<F>>
    where
        F: FnOnce() + Send + 'static,
    {
        Ok(())
    }
}

#[test]
fn test_blocking_executor_default_worker_check_is_false() {
    assert!(!LocalBlockingExecutor.is_current_thread_worker());
}

#[test]
fn test_executor_error_displays_each_failure() {
    let cases = [
        (ExecutorError::InvalidConfiguration, "executor limits must be positive"),
        (ExecutorError::Capacity, "executor queue capacity is full"),
        (ExecutorError::Closed, "executor is closed"),
        (ExecutorError::WorkerSpawn, "executor worker could not be started"),
    ];
    for (error, expected) in cases {
        assert_display(error, expected);
    }
    assert_error::<ExecutorError>();
}

#[test]
fn test_admission_error_displays_each_rejection_reason() {
    let cases = [
        (AdmissionError::Busy, "operation capacity is busy"),
        (AdmissionError::Closed, "operation supervisor is closed"),
        (AdmissionError::Stopped, "operation context has been stopped"),
        (AdmissionError::DeadlineExpired, "operation deadline has expired"),
        (AdmissionError::ReservationLimit, "operation reservation limit reached"),
        (AdmissionError::DriverUnavailable, "operation driver is unavailable"),
        (AdmissionError::WouldDeadlock, "blocking admission wait would deadlock"),
    ];
    for (error, expected) in cases {
        assert_display(error, expected);
    }
    assert_error::<AdmissionError>();
}

#[test]
fn test_observation_error_displays_each_observation_failure() {
    let cases = [
        (ObservationError::AlreadyTaken, "operation result was already taken"),
        (ObservationError::ResultDiscarded, "operation result was discarded"),
        (ObservationError::WouldDeadlock, "blocking observation would deadlock"),
        (ObservationError::InvalidObserver, "operation observer is invalid"),
        (
            ObservationError::ClockUnavailable,
            "operation observer has no clock source",
        ),
        (
            ObservationError::ClockDomainMismatch,
            "operation observer deadline uses another clock domain",
        ),
        (
            ObservationError::WaiterCapacityExceeded,
            "operation has reached its asynchronous waiter capacity",
        ),
    ];
    for (error, expected) in cases {
        assert_display(error, expected);
    }
    assert_error::<ObservationError>();
}

#[derive(Debug)]
struct TypedCleanupSource;

impl Display for TypedCleanupSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("private source detail")
    }
}

impl Error for TypedCleanupSource {}

#[test]
fn test_cleanup_failure_preserves_facts_and_hides_source_from_debug() {
    let id = ResourceId::new(OperationId::new(880), 3, 5);
    let source: Arc<dyn Error + Send + Sync> = Arc::new(TypedCleanupSource);
    let failure = CleanupFailure {
        resource_id: id,
        stage: CleanupStage::Releasing,
        summary: DiagnosticSummary::new("release was not confirmed"),
        source: Some(Arc::clone(&source)),
    };
    assert_eq!(failure.resource_id, id);
    assert_eq!(failure.stage, CleanupStage::Releasing);
    assert_eq!(failure.summary.as_str(), "release was not confirmed");
    assert!(
        failure
            .source
            .as_ref()
            .is_some_and(|source| source.is::<TypedCleanupSource>())
    );
    let debug = format!("{failure:?}");
    assert!(debug.contains("has_source: true"));
    assert!(!debug.contains("private source detail"));
    assert!(source.downcast_ref::<TypedCleanupSource>().is_some());
}

#[test]
fn test_dependency_error_displays_each_rejected_edge() {
    let cases = [
        (
            DependencyError::DifferentScope,
            "cleanup dependency rejected: DifferentScope",
        ),
        (
            DependencyError::SelfDependency,
            "cleanup dependency rejected: SelfDependency",
        ),
        (DependencyError::Cycle, "cleanup dependency rejected: Cycle"),
        (
            DependencyError::NotRegistered,
            "cleanup dependency rejected: NotRegistered",
        ),
        (
            DependencyError::RegistryPoisoned,
            "cleanup dependency rejected: RegistryPoisoned",
        ),
    ];
    for (error, expected) in cases {
        assert_display(error, expected);
    }
    assert_error::<DependencyError>();
}

#[test]
fn test_registry_error_displays_each_failed_confirmation() {
    let cases = [
        (
            RegistryError::NotRegistered,
            "resource registry operation failed: NotRegistered",
        ),
        (
            RegistryError::NotCleaning,
            "resource registry operation failed: NotCleaning",
        ),
        (
            RegistryError::InvalidReceipt,
            "resource registry operation failed: InvalidReceipt",
        ),
        (
            RegistryError::OwnerUnavailable,
            "resource registry operation failed: OwnerUnavailable",
        ),
        (
            RegistryError::RegistryPoisoned,
            "resource registry operation failed: RegistryPoisoned",
        ),
    ];
    for (error, expected) in cases {
        assert_display(error, expected);
    }
    assert_error::<RegistryError>();
}

#[test]
fn test_resource_error_displays_each_rejected_resource_operation() {
    let cases = [
        (ResourceError::ClosedForUse, "resource operation failed: ClosedForUse"),
        (
            ResourceError::AlreadyCleaning,
            "resource operation failed: AlreadyCleaning",
        ),
        (ResourceError::NotRegistered, "resource operation failed: NotRegistered"),
        (
            ResourceError::RegistryDropped,
            "resource operation failed: RegistryDropped",
        ),
        (
            ResourceError::RegistryPoisoned,
            "resource operation failed: RegistryPoisoned",
        ),
        (
            ResourceError::LeaseCountOverflow,
            "resource operation failed: LeaseCountOverflow",
        ),
    ];
    for (error, expected) in cases {
        assert_display(error, expected);
    }
    assert_error::<ResourceError>();
}

#[test]
fn test_transfer_error_displays_each_rejected_transfer() {
    let cases = [
        (TransferError::SameRegistry, "resource transfer rejected: SameRegistry"),
        (
            TransferError::NotRegistered,
            "resource transfer rejected: NotRegistered",
        ),
        (
            TransferError::CapacityExceeded,
            "resource transfer rejected: CapacityExceeded",
        ),
        (
            TransferError::RegistryPoisoned,
            "resource transfer rejected: RegistryPoisoned",
        ),
    ];
    for (error, expected) in cases {
        assert_display(error, expected);
    }
    assert_error::<TransferError>();
}

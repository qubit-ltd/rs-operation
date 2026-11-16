// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Production assembly for manager capacity, scopes, and executors.

mod internal;
use std::sync::Arc;
use std::sync::Mutex;

use internal::ProductionSubmissionPort;

use super::capacity::CapacityPermit;
use super::capacity::PermitBundle;
use super::core::AsyncOperationManager;
use super::core::ManagerCore;
use super::core::OperationJob;
use super::core::OperationManager;
use super::core::SubmissionLease;
use super::core::SubmissionPermit;
use crate::v02::context::OperationContext;
use crate::v02::executor::ExecutorCreateError;
use crate::v02::executor::ExecutorError;
use crate::v02::report::AdmissionError;
use crate::v02::report::ManagerCreateError;
use crate::v02::report::ManagerOptions;

/// Constructs the production manager assembly and returns its synchronous root
/// view.
///
/// # Parameters
///
/// - `options`: Manager limits, executor settings, and root context
///   configuration.
///
/// # Returns
///
/// The synchronous manager handle backed by the production assembly.
///
/// # Errors
///
/// Returns an error when options are invalid or an executor/cleanup worker
/// cannot start.
pub(crate) fn new_sync(options: ManagerOptions) -> Result<OperationManager, ManagerCreateError> {
    validate_options(&options)?;
    let root_context = OperationContext::new(crate::v02::report::ContextOptions {
        clock: options.clock.clone(),
        deadline: None,
        budget: None,
    })
    .map_err(|_| ManagerCreateError::InvalidOptions { field: "clock" })?;
    let port = ProductionSubmissionPort::new(&options)?;
    let core = ManagerCore::new_with_diagnostic_limit(
        Arc::new(port),
        root_context,
        options.scope_limits,
        options.limits.diagnostic_events,
        options.max_groups_per_operation,
    );
    Ok(core.root_view())
}

/// Constructs the shared production assembly and returns its asynchronous root
/// view.
///
/// # Parameters
///
/// - `options`: Manager limits, executor settings, and root context
///   configuration.
///
/// # Returns
///
/// The asynchronous manager handle backed by the production assembly.
///
/// # Errors
///
/// Returns the same validation or worker-startup errors as [`new_sync`].
pub(crate) fn new_async(options: ManagerOptions) -> Result<AsyncOperationManager, ManagerCreateError> {
    new_sync(options).map(|manager| manager.as_async())
}

/// Rejects zero capacities before starting threads or allocating executors.
///
/// # Parameters
///
/// - `options`: Manager configuration whose capacities are validated.
///
/// # Errors
///
/// Returns [`ManagerCreateError::InvalidOptions`] when a configured capacity is
/// zero.
fn validate_options(options: &ManagerOptions) -> Result<(), ManagerCreateError> {
    let limits = options.limits;
    for (field, value) in [
        ("limits.active_operations", limits.active_operations),
        ("limits.queued_operations", limits.queued_operations),
        ("limits.admission_waiters", limits.admission_waiters),
        ("limits.unconsumed_outcomes", limits.unconsumed_outcomes),
        ("limits.cleanup_concurrency", limits.cleanup_concurrency),
        ("limits.diagnostic_events", limits.diagnostic_events),
        ("max_groups_per_operation", options.max_groups_per_operation),
        ("scope_limits.child_operations", options.scope_limits.child_operations),
        ("scope_limits.resources", options.scope_limits.resources),
        ("scope_limits.resource_scopes", options.scope_limits.resource_scopes),
        ("scope_limits.effect_records", options.scope_limits.effect_records),
        ("async_executor.worker_count", options.async_executor.worker_count),
        ("async_executor.task_capacity", options.async_executor.task_capacity),
        ("async_executor.polls_per_turn", options.async_executor.polls_per_turn),
        ("sync_executor.worker_count", options.sync_executor.worker_count),
        ("sync_executor.queue_capacity", options.sync_executor.queue_capacity),
        ("cleanup_executor.worker_count", options.cleanup_executor.worker_count),
        (
            "cleanup_executor.queue_capacity",
            options.cleanup_executor.queue_capacity,
        ),
    ] {
        if value == 0 {
            return Err(ManagerCreateError::InvalidOptions { field });
        }
    }
    Ok(())
}

/// Maps executor saturation and closure into manager admission errors.
///
/// # Parameters
///
/// - `error`: The executor failure to translate.
fn map_executor_admission(error: ExecutorError) -> AdmissionError {
    match error {
        ExecutorError::Full => AdmissionError::Busy,
        ExecutorError::Closed => AdmissionError::ExecutorUnavailable,
    }
}

/// Maps executor startup failures into manager construction errors.
///
/// # Parameters
///
/// - `error`: The executor startup failure to translate.
fn map_executor_create_error(error: ExecutorCreateError) -> ManagerCreateError {
    match error {
        ExecutorCreateError::InvalidOptions => ManagerCreateError::InvalidOptions {
            field: "executor options",
        },
        ExecutorCreateError::ThreadSpawn => ManagerCreateError::ThreadSpawn {
            source: Arc::new(std::io::Error::other("worker thread spawn failed")),
        },
    }
}

/// Recovers a poisoned assembly mutex because its state remains inspectable.
///
/// # Type Parameters
///
/// - `T`: The mutex-protected state type.
///
/// # Parameters
///
/// - `mutex`: The assembly mutex to lock.
///
/// # Returns
///
/// The mutex guard, recovering the inner state if another thread poisoned it.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

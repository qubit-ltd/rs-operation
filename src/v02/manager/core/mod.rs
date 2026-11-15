// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Shared admission and operation-tree coordination for manager views.

mod async_operation_manager;
mod internal;
mod operation_manager;

use std::future::Future;
#[cfg(test)]
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

pub use async_operation_manager::AsyncOperationManager;
use internal::CatchUnwindFuture;
pub(crate) use internal::ExecutionKind;
pub(crate) use internal::ManagerCore;
pub(crate) use internal::ManagerPhase;
pub(crate) use internal::OperationJob;
use internal::RunnerParts;
pub(crate) use internal::SubmissionLease;
pub(crate) use internal::SubmissionPermit;
pub(crate) use internal::SubmissionPort;
pub(crate) use internal::SubmissionReservation;
pub(crate) use internal::SubmissionTarget;
use internal::ThreadUnparker;
pub use operation_manager::OperationManager;

use crate::v02::adapter::ManagedOperationControl;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::operation::AsyncOperationHandle;
use crate::v02::operation::OperationCore;
use crate::v02::operation::OperationHandle;
use crate::v02::operation::OutcomeDelivery;
use crate::v02::operation::OutcomeSlot;
use crate::v02::operation::StopControl;
use crate::v02::report::AdmissionError;
use crate::v02::report::ChildOutcomeKind;
use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ChildRelease;
use crate::v02::report::ContextError;
use crate::v02::report::DiagnosticSummary;
use crate::v02::report::ExecutionPhase;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOptions;
use crate::v02::report::OperationOutcome;
use crate::v02::report::PanicSummary;
use crate::v02::report::SubmitError;
#[cfg(test)]
use crate::v02::resource::ResourceScope;
use crate::v02::scope::ExecutionFrameGuard;
use crate::v02::scope::OperationScope;
use crate::v02::scope::ScopeCore;

/// Converts an operation outcome into the compact form stored by its parent.
///
/// # Type Parameters
///
/// - `T`: The operation's successful output type.
/// - `E`: The operation's error type.
///
/// # Parameters
///
/// - `operation_id`: Identifies the child operation.
/// - `outcome`: Supplies the child's terminal outcome.
fn child_outcome_summary<T, E>(operation_id: OperationId, outcome: &OperationOutcome<T, E>) -> ChildOutcomeSummary {
    let (kind, summary) = match outcome {
        OperationOutcome::Succeeded(_) => (ChildOutcomeKind::Succeeded, "child succeeded"),
        OperationOutcome::Failed(_) | OperationOutcome::ChildFailed(_) => (ChildOutcomeKind::Failed, "child failed"),
        OperationOutcome::Stopped(_) => (ChildOutcomeKind::Stopped, "child stopped"),
        OperationOutcome::Panicked(panic) => (ChildOutcomeKind::Panicked, panic.summary.as_str()),
        OperationOutcome::ExecutionFailed(failure) => (ChildOutcomeKind::ExecutionFailed, failure.summary.as_str()),
    };
    ChildOutcomeSummary {
        operation_id,
        kind,
        summary: DiagnosticSummary::new(summary),
    }
}

/// Upgrades and validates the target parent before waiting for retry capacity.
///
/// # Parameters
///
/// - `target`: Specifies the parent target to validate.
///
/// # Errors
///
/// Returns an admission error if the parent scope ended, stopped, or its
/// context is invalid.
fn resolve_wait_parent(target: &SubmissionTarget) -> Result<Option<Arc<ScopeCore>>, AdmissionError> {
    let SubmissionTarget::Parent { scope, .. } = target else {
        return Ok(None);
    };
    let scope = scope.upgrade().ok_or(AdmissionError::ScopeEnded)?;
    if !scope.is_open() {
        return Err(AdmissionError::ScopeEnded);
    }
    if let Err(error) = scope.context().check() {
        return Err(map_context_error(error));
    }
    Ok(Some(scope))
}

/// Produces bounded human-readable text for common panic payload types.
///
/// # Parameters
///
/// - `payload`: Supplies the panic payload to summarize.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        format!("operation panicked: {message}")
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        format!("operation panicked: {message}")
    } else {
        "operation panicked with a non-string payload".to_owned()
    }
}

/// Drives a future on the current thread until it completes.
///
/// # Type Parameters
///
/// - `F`: The future type to drive.
///
/// # Parameters
///
/// - `future`: The future to poll until completion.
///
/// # Returns
///
/// The output produced by the completed future.
fn block_on<F: Future>(future: F) -> F::Output {
    let unparker = Arc::new(ThreadUnparker(std::thread::current()));
    let waker = std::task::Waker::from(unparker);
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

/// Maps inherited context failures into manager admission reasons.
///
/// # Parameters
///
/// - `error`: The inherited context error to map.
fn map_context_error(error: ContextError) -> AdmissionError {
    match error {
        ContextError::Stopped => AdmissionError::Stopped,
        ContextError::ClockUnavailable | ContextError::ClockDomainMismatch => AdmissionError::DeadlineExpired,
        ContextError::BudgetRejected(_) => AdmissionError::CapacityExceeded,
    }
}

/// Recovers a poisoned mutex guard because coordinator state remains
/// inspectable.
///
/// # Type Parameters
///
/// - `T`: The value protected by the mutex.
///
/// # Parameters
///
/// - `mutex`: The mutex to lock.
///
/// # Returns
///
/// The mutex guard, including when the mutex was poisoned.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
struct TestPort;
#[cfg(test)]
struct TestPermit;
#[cfg(test)]
struct TestLease;
#[cfg(test)]
impl SubmissionPort for TestPort {
    fn reserve(
        &self,
        scope_id: OperationId,
        _execution: ExecutionKind,
        parent: Option<&ScopeCore>,
        options: &OperationOptions,
    ) -> Result<SubmissionReservation, AdmissionError> {
        Ok(test_reservation(scope_id, parent, options))
    }

    fn wait_for_capacity<'a>(
        &'a self,
        _parent: Option<Arc<ScopeCore>>,
        _options: OperationOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdmissionError>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

/// Creates a reservation backed by test permits and resources.
///
/// # Parameters
///
/// - `scope_id`: Identifies the operation scope.
/// - `parent`: Supplies a parent scope, if present.
/// - `options`: Provides the operation options.
///
/// # Returns
///
/// The test reservation.
#[cfg(test)]
pub(crate) fn test_reservation(
    scope_id: OperationId,
    parent: Option<&ScopeCore>,
    options: &OperationOptions,
) -> SubmissionReservation {
    let (capacity, resource_scope) = test_resources(scope_id, parent, options);
    SubmissionReservation {
        execution: Box::new(TestPermit),
        lifecycle: Arc::new(TestLease),
        capacity,
        resource_scope,
    }
}

/// Creates test capacity and a resource registry for an operation.
///
/// # Parameters
///
/// - `scope_id`: Identifies the operation scope.
/// - `parent`: Supplies a parent scope, if present.
/// - `options`: Provides the operation options.
///
/// # Returns
///
/// The operation capacity and its test resource registry.
#[cfg(test)]
pub(crate) fn test_resources(
    scope_id: OperationId,
    parent: Option<&ScopeCore>,
    options: &OperationOptions,
) -> (Arc<crate::v02::manager::capacity::ScopeCapacity>, ResourceScope) {
    use crate::v02::manager::capacity::ManagerCapacityPools;
    use crate::v02::manager::capacity::ScopeCapacity;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ManagerLimits;
    use crate::v02::report::ResourceScopeId;
    use crate::v02::resource::cleanup_port::CleanupDispatch;
    use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
    use crate::v02::resource::cleanup_port::CleanupQueueError;

    struct NoCleanup;
    impl CleanupDispatch for NoCleanup {
        fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
            Err(CleanupQueueError::Closed)
        }
    }

    let manager = ManagerCapacityPools::new(ManagerLimits::default());
    let capacity = Arc::new(match parent {
        Some(parent) => parent.capacity().child_for_operation(scope_id, options.limits),
        None => ScopeCapacity::root_for_operation(&manager, scope_id, options.limits),
    });
    let resource_scope = ResourceScope::new(
        ManagerId::new_unique(),
        ResourceScopeId::new_unique(),
        Arc::clone(&capacity),
        Arc::new(NoCleanup),
    );
    (capacity, resource_scope)
}
#[cfg(test)]
impl SubmissionLease for TestLease {
    fn execution_finished(&self) {}

    fn settlement_finished(&self) {}

    fn outcome_delivered(&self, _delivery: OutcomeDelivery) {}
}
#[cfg(test)]
impl SubmissionPermit for TestPermit {
    fn dispatch(self: Box<Self>, job: OperationJob) {
        match job {
            OperationJob::Sync(job) => job(),
            OperationJob::Async(future) => poll_immediately(future),
        }
    }
}

/// Polls a test future until it completes.
///
/// # Parameters
///
/// - `future`: The future to poll.
#[cfg(test)]
fn poll_immediately(mut future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    loop {
        if future.as_mut().poll(&mut context).is_ready() {
            return;
        }
        std::thread::yield_now();
    }
}

#[cfg(test)]
mod tests;

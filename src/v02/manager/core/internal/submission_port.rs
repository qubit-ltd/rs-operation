// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::super::ScopeCapacity;
use super::ExecutionKind;
use super::ManagerPhase;
use super::SubmissionReservation;
use crate::v02::report::AdmissionError;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOptions;
use crate::v02::resource::ResourceScope;
use crate::v02::scope::ScopeCore;

/// Capacity and execution boundary supplied by the production manager assembly.
pub(crate) trait SubmissionPort: Send + Sync + 'static {
    /// Reserves executor, lifecycle, capacity, and resource state atomically.
    ///
    /// # Parameters
    ///
    /// - `scope_id`: Identifies the operation scope being submitted.
    /// - `execution`: Selects the synchronous or asynchronous execution path.
    /// - `parent`: Supplies the parent scope, if this is a child operation.
    /// - `options`: Provides admission and capacity options.
    ///
    /// # Returns
    ///
    /// The reservation containing the operation's required permits and scope.
    ///
    /// # Errors
    ///
    /// Returns an admission error when the operation cannot be reserved.
    fn reserve(
        &self,
        scope_id: OperationId,
        execution: ExecutionKind,
        parent: Option<&ScopeCore>,
        options: &OperationOptions,
    ) -> Result<SubmissionReservation, AdmissionError>;

    /// Waits for a retryable capacity miss. Implementations must reserve a
    /// bounded waiter slot and wake for capacity, manager close, or parent
    /// scope end/stop. Dropping the future must unregister and release it.
    ///
    /// # Parameters
    ///
    /// - `parent`: Supplies the parent scope, if this is a child operation.
    /// - `options`: Provides the operation's admission and capacity options.
    ///
    /// # Returns
    ///
    /// A future that resolves when capacity is available or reports an
    /// admission error.
    ///
    /// # Errors
    ///
    /// The future returns an admission error if the manager closes or the
    /// parent scope ends or stops before capacity becomes available.
    fn wait_for_capacity<'a>(
        &'a self,
        parent: Option<Arc<ScopeCore>>,
        execution: ExecutionKind,
        options: OperationOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdmissionError>> + Send + 'a>>;

    /// Updates executor admission when the shared manager changes phase.
    ///
    /// # Parameters
    ///
    /// - `_phase`: The manager phase to propagate to executor admission.
    fn phase_changed(&self, _phase: ManagerPhase) {}

    /// Wakes a retained async task for an internal lifecycle test.
    #[cfg(test)]
    fn test_active_async_task_waker(&self) -> Option<std::task::Waker> {
        None
    }

    /// Stops executor workers after all manager-owned state has drained.
    ///
    /// # Parameters
    ///
    /// - `self`: Port whose executor workers should stop.
    fn finish_shutdown(&self) {}
    /// Reports queued executor jobs for manager diagnostics.
    ///
    /// # Returns
    ///
    /// The number of queued executor jobs, or zero when the port does not
    /// provide a count.
    #[must_use]
    fn queued_operation_count(&self) -> usize {
        0
    }
    /// Creates a manager-lifetime resource registry with the requested
    /// capacity.
    ///
    /// # Parameters
    ///
    /// - `_capacity`: The maximum number of registered resources.
    ///
    /// # Returns
    ///
    /// The manager-lifetime resource scope.
    ///
    /// # Errors
    ///
    /// Returns a resource-scope error if the registry cannot be created.
    fn service_resource_scope(
        &self,
        _capacity: usize,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        Err(crate::v02::report::ResourceScopeError::InvalidOptions)
    }
    /// Creates an operation resource registry backed by the supplied capacity.
    ///
    /// # Parameters
    ///
    /// - `_id`: Identifies the operation resource scope.
    /// - `_capacity`: Supplies the operation's hierarchical resource limits.
    ///
    /// # Returns
    ///
    /// The operation-owned resource scope.
    ///
    /// # Errors
    ///
    /// Returns a resource-scope error if the registry cannot be created.
    fn operation_resource_scope(
        &self,
        _id: crate::v02::report::ResourceScopeId,
        _capacity: Arc<ScopeCapacity>,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        Err(crate::v02::report::ResourceScopeError::InvalidOptions)
    }
}

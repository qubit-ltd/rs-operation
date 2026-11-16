// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Contracts for operations prepared and driven by an external runtime.

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use super::external_bind_error::ExternalBindError;
use crate::v02::protocol::OperationControl;
use crate::v02::report::ExecutionFailure;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOutcome;
use crate::v02::scope::OperationScope;

/// A prepared external task that the manager can bind and drive.
///
/// Implementations must keep binding one-shot across manager instances.
///
/// # Type Parameters
///
/// - `T`: Successful value produced by the external task.
/// - `E`: Error value produced by the external task.
pub trait PreparedExternalOperation<T, E>: OperationControl + 'static
where
    T: Send + 'static,
    E: Send + 'static,
{
    /// Claims this prepared task for the supplied operation identity.
    ///
    /// # Parameters
    ///
    /// - `id`: Manager-assigned identity that the task must use for its result.
    ///
    /// # Errors
    ///
    /// Returns [`ExternalBindError::AlreadyBound`] if this task was previously
    /// claimed, or [`ExternalBindError::Failed`] if binding could not complete.
    fn bind(&self, id: OperationId) -> Result<(), ExternalBindError>;

    /// Starts the task with its manager-owned operation scope.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared prepared task being activated.
    /// - `scope`: Scope assigned to this operation by its manager.
    ///
    /// # Errors
    ///
    /// Returns the execution failure reported by the task when activation
    /// cannot start.
    fn activate(self: Arc<Self>, scope: OperationScope) -> Result<(), ExecutionFailure>;

    /// Releases a claimed task that was not activated.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared prepared task whose claim is being released.
    fn abandon(self: Arc<Self>);

    /// Takes the unique typed result when it becomes available.
    ///
    /// # Parameters
    ///
    /// - `self`: Prepared task whose result is polled.
    /// - `cx`: Current task context used to register for result readiness.
    ///
    /// # Returns
    ///
    /// `Pending` while no result is available, or the task's outcome once
    /// ready.
    fn poll_take_outcome(&self, cx: &mut Context<'_>) -> Poll<OperationOutcome<T, E>>;
}

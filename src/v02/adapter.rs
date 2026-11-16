// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Admission and lifecycle ports for externally executed child operations.

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::v02::protocol::OperationControl;
use crate::v02::report::ExecutionFailure;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOutcome;
use crate::v02::scope::OperationScope;

/// Error returned when an external operation cannot claim its binding.
#[derive(Clone, Debug)]
pub enum ExternalBindError {
    /// This prepared operation has already been bound to another submission.
    AlreadyBound,
    /// Binding failed after the adapter claimed this operation.
    Failed(ExecutionFailure),
}

/// A prepared external task that the manager can bind and drive.
///
/// Implementations must keep binding one-shot across manager instances.
pub trait PreparedExternalOperation<T, E>: OperationControl + 'static
where
    T: Send + 'static,
    E: Send + 'static,
{
    /// Claims this prepared task for the supplied operation identity.
    fn bind(&self, id: OperationId) -> Result<(), ExternalBindError>;
    /// Starts the task with its manager-owned operation scope.
    fn activate(self: Arc<Self>, scope: OperationScope) -> Result<(), ExecutionFailure>;
    /// Releases a claimed task that was not activated.
    fn abandon(self: Arc<Self>);
    /// Takes the unique typed result when it becomes available.
    fn poll_take_outcome(&self, cx: &mut Context<'_>) -> Poll<OperationOutcome<T, E>>;
}

mod internal;

#[cfg(test)]
mod tests;

pub(crate) use internal::AdapterErrorKind;
pub(crate) use internal::AdapterReservation;
#[cfg(test)]
pub(crate) use internal::ExternalTask;
pub(crate) use internal::ManagedOperationControl;
#[cfg(test)]
pub(crate) use internal::recover_lost_input;

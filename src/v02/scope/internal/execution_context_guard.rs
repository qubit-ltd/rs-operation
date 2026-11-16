// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::execution_context::EXECUTION_CONTEXTS;
use super::execution_context::ExecutionContext;
use crate::v02::manager::core::ManagerCore;
use crate::v02::manager::group::GroupCore;
use crate::v02::report::OperationId;

/// Installs the current operation context for a synchronous call or one poll.
#[must_use = "drop the guard to restore the previous execution context"]
pub(crate) struct ExecutionContextGuard;

impl ExecutionContextGuard {
    /// Pushes an operation identity onto the current thread's context stack.
    ///
    /// # Parameters
    ///
    /// - `manager`: Manager that owns the operation.
    /// - `operation`: Operation currently being executed.
    /// - `groups`: Groups that contain the operation.
    ///
    /// # Returns
    ///
    /// A guard that pops this context when dropped.
    pub(crate) fn enter(manager: &Arc<ManagerCore>, operation: OperationId, groups: &[Arc<GroupCore>]) -> Self {
        EXECUTION_CONTEXTS.with(|contexts| {
            contexts.borrow_mut().push(ExecutionContext {
                manager: Arc::as_ptr(manager) as usize,
                operation,
                groups: groups.iter().map(|group| Arc::as_ptr(group) as usize).collect(),
            });
        });
        Self
    }
}

impl Drop for ExecutionContextGuard {
    /// Removes the most recently installed operation context.
    fn drop(&mut self) {
        EXECUTION_CONTEXTS.with(|contexts| {
            contexts.borrow_mut().pop();
        });
    }
}

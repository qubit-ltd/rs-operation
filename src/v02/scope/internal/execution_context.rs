// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::cell::RefCell;
use std::sync::Arc;

use crate::v02::manager::core::ManagerCore;
use crate::v02::manager::group::GroupCore;
use crate::v02::report::OperationId;

thread_local! {
    /// Nested operation contexts active during a body call or one future poll.
    pub(super) static EXECUTION_CONTEXTS: RefCell<Vec<ExecutionContext>> = const { RefCell::new(Vec::new()) };
}

/// Identifies one managed operation while its body is being executed.
#[derive(Clone, Copy)]
pub(super) struct ExecutionContext {
    /// Address identity of the manager coordinator.
    pub(super) manager: usize,
    /// Operation whose completion depends on the current body.
    pub(super) operation: OperationId,
    /// Address identity of the operation group, if it belongs to a group.
    pub(super) group: Option<usize>,
}

/// Reports whether the active operation belongs to the given manager.
///
/// # Parameters
///
/// - `manager`: Manager identity to compare with the current execution stack.
///
/// # Returns
///
/// `true` if a live operation context for this manager is on the stack.
pub(crate) fn is_waiting_on_manager(manager: &Arc<ManagerCore>) -> bool {
    let manager_ptr = Arc::as_ptr(manager) as usize;
    EXECUTION_CONTEXTS.with(|contexts| {
        contexts
            .borrow()
            .iter()
            .any(|context| context.manager == manager_ptr && manager.has_active_scope(context.operation))
    })
}

/// Reports whether the active operation belongs to the given group.
///
/// # Parameters
///
/// - `manager`: Manager that owns the target group.
/// - `group`: Group identity to compare with the current execution stack.
///
/// # Returns
///
/// `true` if a live operation context for this manager and group is on the
/// stack.
pub(crate) fn is_waiting_on_group(manager: &Arc<ManagerCore>, group: &Arc<GroupCore>) -> bool {
    let manager_ptr = Arc::as_ptr(manager) as usize;
    let group_ptr = Arc::as_ptr(group) as usize;
    EXECUTION_CONTEXTS.with(|contexts| {
        contexts.borrow().iter().any(|context| {
            context.manager == manager_ptr
                && context.group == Some(group_ptr)
                && manager.has_active_scope(context.operation)
        })
    })
}

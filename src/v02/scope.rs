// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Non-cloneable operation scope views and execution-frame lifetime.

mod internal;
mod operation_resource_authority;
mod operation_scope;

pub(crate) use internal::ExecutionContextFuture;
pub(crate) use internal::ExecutionContextGuard;
pub(crate) use internal::ExecutionFrameGuard;
pub(crate) use internal::ScopeCore;
pub(crate) use internal::ScopeCoreInit;
pub(crate) use internal::is_waiting_on_group;
pub(crate) use internal::is_waiting_on_manager;
pub use operation_resource_authority::OperationResourceAuthority;
pub use operation_scope::OperationScope;

#[cfg(test)]
mod tests;

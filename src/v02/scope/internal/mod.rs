// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Internal operation-scope state and lifecycle types.

mod child_record;
mod execution_context;
mod execution_context_future;
mod execution_context_guard;
mod execution_frame_guard;
mod scope_core;
mod scope_core_init;
mod scope_resource_owner;
mod scope_state;
mod start_state;
mod sync;

pub(crate) use execution_context::is_waiting_on_group;
pub(crate) use execution_context::is_waiting_on_manager;
pub(crate) use execution_context_future::ExecutionContextFuture;
pub(crate) use execution_context_guard::ExecutionContextGuard;
pub(crate) use execution_frame_guard::ExecutionFrameGuard;
pub(crate) use scope_core::ScopeCore;
pub(crate) use scope_core_init::ScopeCoreInit;
pub(crate) use scope_resource_owner::ScopeResourceOwner;
pub(crate) use sync::lock;

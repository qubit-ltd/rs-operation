// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Weak;

use super::scope_core::ScopeCore;
use crate::v02::context::OperationContext;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::manager::core::ManagerCore;
use crate::v02::manager::group::GroupCore;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOptions;
use crate::v02::resource::ResourceScope;

/// Inputs used to create a scope core from its manager-owned lifecycle state.
pub(crate) struct ScopeCoreInit {
    /// Stable identity for this operation.
    pub(crate) id: OperationId,
    /// Weak link to the owning manager.
    pub(crate) manager: Weak<ManagerCore>,
    /// Optional weak link to the parent scope.
    pub(crate) parent: Option<Weak<ScopeCore>>,
    /// Inherited cancellation, clock, deadline, and budget state.
    pub(crate) context: OperationContext,
    /// Child requirements and failure policy.
    pub(crate) options: OperationOptions,
    /// Maximum retained diagnostic entries.
    pub(crate) diagnostic_limit: usize,
    /// Hierarchical capacity owned by this scope.
    pub(crate) capacity: Arc<ScopeCapacity>,
    /// Resource registry attached to this scope.
    pub(crate) resource_scope: ResourceScope,
    /// Optional operation group tracking execution and resources.
    pub(crate) groups: Vec<Arc<GroupCore>>,
}

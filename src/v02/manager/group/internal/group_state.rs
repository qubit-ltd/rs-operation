// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Weak;

use crate::v02::report::OperationId;
use crate::v02::report::RecoveryId;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ShutdownMode;
use crate::v02::resource::RecoveryHandle;
use crate::v02::resource::ResourceScope;
use crate::v02::scope::ScopeCore;

/// Mutable admission and lifecycle state shared by a group's core.
#[must_use = "retain the state while the group core is active"]
pub(super) struct GroupState {
    /// Whether new root operations may be admitted.
    pub(super) accepting: bool,
    /// Number of operations currently holding group capacity.
    pub(super) active: usize,
    /// Shutdown policy applied to current and future operations.
    pub(super) mode: ShutdownMode,
    /// Weak references to registered operation scopes used for cancellation.
    pub(super) scopes: HashMap<OperationId, Weak<ScopeCore>>,
    /// Operation identifiers that have been admitted but have not started.
    pub(super) queued: HashSet<OperationId>,
    /// Service resource scopes retained until their lifecycle ends.
    pub(super) services: HashMap<ResourceScopeId, ResourceScope>,
    /// Recovery handles counted until their recovery state is resolved.
    pub(super) recoveries: HashMap<RecoveryId, RecoveryHandle>,
}

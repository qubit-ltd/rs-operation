// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::sync::Weak;

use super::ManagerPhase;
use crate::v02::report::OperationId;
use crate::v02::report::RecoveryId;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ShutdownMode;
use crate::v02::resource::RecoveryHandle;
use crate::v02::resource::ResourceScope;
use crate::v02::scope::ScopeCore;

/// Manager-owned lifecycle phase and registries for active scopes/resources.
pub(in crate::v02::manager::core) struct ManagerState {
    /// Current admission and shutdown phase shared by all manager views.
    pub(in crate::v02::manager::core) phase: ManagerPhase,
    /// Strongest shutdown policy requested so far.
    pub(in crate::v02::manager::core) shutdown_mode: ShutdownMode,
    /// Active operation scopes retained weakly until settlement.
    pub(super) active_scopes: HashMap<OperationId, Weak<ScopeCore>>,
    /// Service resource registries retained until their settlement callbacks.
    pub(super) service_scopes: HashMap<ResourceScopeId, ResourceScope>,
    /// Recovery handles that keep shutdown open until explicitly confirmed.
    pub(super) recoveries: HashMap<RecoveryId, RecoveryHandle>,
    #[cfg(test)]
    pub(super) relationships: Vec<(OperationId, Option<OperationId>)>,
    #[cfg(test)]
    pub(super) values: Vec<usize>,
}

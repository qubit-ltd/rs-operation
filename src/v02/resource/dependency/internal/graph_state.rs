// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::collections::HashSet;

use super::DependencyEdge;
use crate::v02::report::ResourceId;

/// Mutable graph facts guarded by the dependency graph mutex.
#[derive(Default)]
pub(in crate::v02::resource::dependency) struct GraphState {
    /// Active edges from each dependent resource to its prerequisites.
    pub(in crate::v02::resource::dependency) edges: HashMap<ResourceId, Vec<DependencyEdge>>,
    /// Resources whose cleanup has started and has not yet been recorded.
    pub(in crate::v02::resource::dependency) started: HashSet<ResourceId>,
    /// Resources whose cleanup succeeded and released their dependency leases.
    pub(in crate::v02::resource::dependency) released: HashSet<ResourceId>,
    /// Resources whose latest cleanup attempt failed and can enter recovery.
    pub(in crate::v02::resource::dependency) failed: HashSet<ResourceId>,
}

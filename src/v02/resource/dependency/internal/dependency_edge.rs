// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::dependency_graph::DependencyLease;
use crate::v02::report::ResourceId;

/// One prerequisite edge and the strong lease that keeps it alive.
pub(in crate::v02::resource::dependency) struct DependencyEdge {
    /// Resource that must be released before the dependent can be cleaned up.
    pub(in crate::v02::resource::dependency) prerequisite: ResourceId,
    /// Keeps the prerequisite owner registered until dependent cleanup
    /// succeeds.
    pub(in crate::v02::resource::dependency) _lease: DependencyLease,
}

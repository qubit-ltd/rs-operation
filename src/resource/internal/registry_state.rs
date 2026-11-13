// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::Slot;
use crate::report::ResourceId;

/// Stores the resource slots and dependency edges used by the resource
/// registry.
///
/// The registry uses this state to resolve resources and release dependents
/// only after their prerequisites have been released.
pub(in crate::resource) struct RegistryState {
    /// Holds resource entries at their reusable slot indices.
    pub(in crate::resource) slots: Vec<Slot>,
    /// Edges point from prerequisite B to dependent A: B must release before A.
    pub(in crate::resource) dependencies: BTreeMap<ResourceId, BTreeSet<ResourceId>>,
}

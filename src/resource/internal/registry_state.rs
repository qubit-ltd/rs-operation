// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::Slot;
use crate::report::ResourceId;

pub(in crate::resource) struct RegistryState {
    pub(in crate::resource) slots: Vec<Slot>,
    /// Edges point from prerequisite B to dependent A: B must release before A.
    pub(in crate::resource) dependencies: BTreeMap<ResourceId, BTreeSet<ResourceId>>,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::PermitBundle;

/// A single registration shared by clones of a resource reference.
#[derive(Clone)]
pub(crate) struct ResourceRegistration {
    /// Holds one set of hierarchical resource permits across reference clones.
    pub(in crate::v02::manager::capacity) permits: Arc<PermitBundle>,
}

impl ResourceRegistration {
    /// Counts hierarchical permits held for this one shared registration.
    ///
    /// # Returns
    ///
    /// The number of permits held by this registration.
    #[must_use]
    #[inline]
    pub(crate) fn permit_count(&self) -> usize {
        self.permits.len()
    }
}

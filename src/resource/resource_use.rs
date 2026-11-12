// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use super::internal::Entry;
use super::internal::RegistryInner;

use std::sync::Arc;

/// One outstanding authorization for a resource to be used.
///
/// Dropping the lease only returns this lease's count; it cannot prove that
/// copied native handles or external users have stopped accessing the resource.
pub struct ResourceUse {
    pub(super) entry: Arc<Entry>,
    pub(super) registry: Arc<RegistryInner>,
}

impl Drop for ResourceUse {
    fn drop(&mut self) {
        if let Ok(mut state) = self.entry.state.lock() {
            state.active_uses = state.active_uses.saturating_sub(1);
        }
        self.registry.notify_change();
    }
}

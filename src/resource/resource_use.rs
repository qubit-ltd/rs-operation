// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::internal::Entry;
use super::internal::RegistryInner;

/// One outstanding authorization for a resource to be used.
///
/// Dropping the lease only returns this lease's count; it cannot prove that
/// copied native handles or external users have stopped accessing the resource.
///
/// # Examples
///
/// Acquire a lease from a registered handle and release it when the protected
/// work is complete:
///
/// ```
/// use qubit_operation::resource::ResourceError;
/// use qubit_operation::resource::ResourceHandle;
///
/// fn use_resource(handle: &ResourceHandle) -> Result<(), ResourceError> {
///     let lease = handle.acquire_use()?;
///     // Perform work that requires the resource to remain registered.
///     drop(lease);
///     Ok(())
/// }
/// ```
pub struct ResourceUse {
    /// Entry whose active-use count this lease keeps elevated until it drops.
    pub(super) entry: Arc<Entry>,
    /// Registry notified when this lease releases its active-use count.
    pub(super) registry: Arc<RegistryInner>,
}

impl Drop for ResourceUse {
    /// Return this lease's active-use count and wake registry observers.
    fn drop(&mut self) {
        if let Ok(mut state) = self.entry.state.lock() {
            state.active_uses = state.active_uses.saturating_sub(1);
        }
        self.registry.notify_change();
    }
}

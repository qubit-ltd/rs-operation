// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::sync::Arc;

use super::super::Owner;

/// Type-erased lifecycle operations stored in a resource scope's table.
pub(in crate::v02::resource) trait ErasedOwner: Any + Send + Sync {
    /// Closes admission for new resource users and starts cleanup when ready.
    ///
    /// Existing users retain access until their references are released; the
    /// resource is dispatched for cleanup only after the user count reaches
    /// zero.
    ///
    /// # Parameters
    ///
    /// - `self`: Erased owner whose scope is ending.
    fn end(&self);

    /// Reports whether resource references are still held by users.
    ///
    /// # Parameters
    ///
    /// - `self`: Owner whose active user count is inspected.
    ///
    /// # Returns
    ///
    /// `true` while at least one resource user remains.
    fn has_users(&self) -> bool;

    /// Exposes the concrete owner for internal type inspection.
    ///
    /// # Parameters
    ///
    /// - `self`: Erased owner borrowed for downcasting.
    ///
    /// # Returns
    ///
    /// A borrow of the concrete owner wrapped in [`Any`]; it does not transfer
    /// ownership or extend the borrow beyond `self`.
    fn as_any(&self) -> &dyn Any;
}

impl<T: Send + Sync + 'static> ErasedOwner for Arc<Owner<T>> {
    /// Delegates scope-end handling to the typed resource owner.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared typed owner whose scope is ending.
    fn end(&self) {
        Owner::end(self);
    }

    /// Delegates the active-user check to the typed resource owner.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared typed owner whose active users are checked.
    ///
    /// # Returns
    ///
    /// `true` while the typed owner has active resource users.
    fn has_users(&self) -> bool {
        Owner::has_users(self)
    }

    /// Returns this `Arc<Owner<T>>` as `Any` for internal downcasting.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared typed owner borrowed for downcasting.
    ///
    /// # Returns
    ///
    /// A borrow of this shared owner as `Any`.
    fn as_any(&self) -> &dyn Any {
        self
    }
}

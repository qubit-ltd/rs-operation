// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ErasedOwner;

/// Stores one resource owner behind the scope table's type-erased interface.
///
/// The owner retains the typed resource lifecycle while exposing only the
/// operations needed by the table, such as ending the owner and checking
/// whether external users remain.
pub(in crate::v02::resource) struct Entry {
    /// Shared typed owner erased for storage alongside other resource types.
    pub(in crate::v02::resource) owner: Box<dyn ErasedOwner>,
}

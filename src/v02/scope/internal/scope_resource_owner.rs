// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::resource::ResourceScope;

/// An owner that receives the scope-end event independently of child
/// settlement.
pub(crate) trait ScopeResourceOwner: Send + Sync + 'static {
    /// Ends resource admission while existing references finish their
    /// lifecycle.
    fn end(&self);
}
impl ScopeResourceOwner for ResourceScope {
    /// Ends resource admission while outstanding references drain.
    fn end(&self) {
        ResourceScope::end(self);
    }
}

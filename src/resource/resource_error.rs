// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::fmt;

/// Reason a resource operation could not be completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceError {
    /// New use leases are no longer accepted.
    ClosedForUse,
    /// Cleanup has started or release was confirmed.
    AlreadyCleaning,
    /// The handle no longer names an active registration.
    NotRegistered,
    /// The registry that owned the handle has been dropped.
    RegistryDropped,
    /// Registry state was poisoned by a prior panic.
    RegistryPoisoned,
    /// The number of active leases cannot be represented.
    LeaseCountOverflow,
}

impl fmt::Display for ResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource operation failed: {self:?}")
    }
}

impl std::error::Error for ResourceError {}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::fmt;

/// Invalid registry confirmation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryError {
    /// The resource generation is not registered in this registry.
    NotRegistered,
    /// Cleanup has not been handed out for this resource.
    NotCleaning,
    /// The receipt does not identify this resource generation.
    InvalidReceipt,
    /// No cleanup owner is currently available to restore.
    OwnerUnavailable,
    /// Registry state was poisoned by a prior panic.
    RegistryPoisoned,
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource registry operation failed: {self:?}")
    }
}

impl std::error::Error for RegistryError {}

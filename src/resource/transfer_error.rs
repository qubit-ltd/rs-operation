// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::fmt;

/// Transfer did not move any registration or cleanup responsibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferError {
    /// The source and destination are the same registry.
    SameRegistry,
    /// The handle is no longer registered in the stated source registry.
    NotRegistered,
    /// The destination cannot reserve the full dependency group.
    CapacityExceeded,
    /// A registry state was poisoned by a prior panic.
    RegistryPoisoned,
}

impl fmt::Display for TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource transfer rejected: {self:?}")
    }
}

impl std::error::Error for TransferError {}

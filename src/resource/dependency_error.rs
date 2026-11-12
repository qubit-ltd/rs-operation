// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::fmt;

/// Invalid cleanup dependency registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyError {
    /// Resources belong to different scope registries.
    DifferentScope,
    /// A resource cannot depend on itself.
    SelfDependency,
    /// The edge would create a directed dependency cycle.
    Cycle,
    /// A resource handle no longer names an active registration.
    NotRegistered,
    /// Registry state was poisoned by a prior panic.
    RegistryPoisoned,
}

impl fmt::Display for DependencyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cleanup dependency rejected: {self:?}")
    }
}

impl std::error::Error for DependencyError {}

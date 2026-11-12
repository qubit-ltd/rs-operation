// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

/// Classification of a resource registration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegisterErrorKind {
    /// No reusable resource slot is available.
    CapacityExceeded,
    /// Registry state was poisoned by a prior panic.
    RegistryPoisoned,
}

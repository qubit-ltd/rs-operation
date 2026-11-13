// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::task::Waker;

use crate::report::RecoveryOwner;
use crate::report::ResourceId;

/// Shared identity, capacity, owners, and waiters for recovery registry clones.
pub(in crate::resource) struct RecoveryInner {
    /// Distinguishes this registry from every other registry instance.
    pub(in crate::resource) registry_id: u64,
    /// Maximum number of recovery owners the registry retains.
    pub(in crate::resource) capacity: usize,
    /// Issues unique nonces for keys created by this registry.
    pub(in crate::resource) next_nonce: AtomicU64,
    /// Maps each issued key identity to its uniquely owned recovery payload.
    pub(in crate::resource) owners: Mutex<BTreeMap<(u64, ResourceId, u64), RecoveryOwner>>,
    /// Stores tasks waiting for an owner removal to free registry capacity.
    pub(in crate::resource) waiters: Mutex<Vec<Waker>>,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;

use super::Entry;

/// Mutable admission and resource-table state for one resource scope.
///
/// The scope core guards this value with a mutex. Sealing closes admission,
/// invalidates outstanding key generations, and uses the retained and
/// in-progress reservation counts to determine how many cleanups to await.
pub(in crate::v02::resource) struct State {
    /// Whether the scope may still accept resource registrations.
    pub(in crate::v02::resource) open: bool,
    /// Current key generation; incremented when the scope closes to invalidate
    /// old keys.
    pub(in crate::v02::resource) generation: u64,
    /// Resource reservations admitted but not yet converted into retained
    /// owners.
    pub(in crate::v02::resource) reservations: usize,
    /// Number of resource owners retained by the scope and included in
    /// settlement.
    pub(in crate::v02::resource) retained: usize,
    /// Registered owners indexed by their slot until the scope begins ending
    /// them.
    pub(in crate::v02::resource) slots: HashMap<u64, Entry>,
}

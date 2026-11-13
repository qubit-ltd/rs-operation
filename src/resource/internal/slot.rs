// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::Entry;

/// Stores the generation and current entry state for one reusable registry
/// slot.
///
/// A vacant slot can be reused only while its generation counter can advance.
/// Retaining the generation prevents stale resource IDs from matching a later
/// entry.
pub(in crate::resource) struct Slot {
    /// Identifies this use of the slot and advances when the slot is released,
    /// unless the counter is already at its maximum value.
    pub(in crate::resource) generation: u32,
    /// Marks the slot unavailable after its generation counter can no longer
    /// advance.
    pub(in crate::resource) exhausted: bool,
    /// Holds the registered entry, or `None` while the slot is vacant.
    pub(in crate::resource) entry: Option<Arc<Entry>>,
}

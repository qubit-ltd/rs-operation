// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::Waiter;

/// Tracks capacity-change generations and registered task wakeups.
pub(in crate::v02::manager::capacity) struct CoordinatorState {
    /// Monotonic change counter used to close register-after-release races.
    pub(in crate::v02::manager::capacity) generation: u64,
    /// Bounded slots for task wake registrations.
    pub(in crate::v02::manager::capacity) waiters: Vec<Option<Waiter>>,
}

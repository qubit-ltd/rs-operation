// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::sync::Weak;

use super::CleanupDispatcherInner;

/// Mutable scheduler flags and dispatcher registrations guarded by one mutex.
pub(in crate::v02::resource::cleanup) struct CleanupPumpState {
    /// Set when the worker must exit after waking.
    pub(in crate::v02::resource::cleanup) stopping: bool,
    /// Coalesced wake flag indicating registered queues should be retried.
    pub(in crate::v02::resource::cleanup) signaled: bool,
    /// Next registration identifier; zero is skipped when the counter wraps.
    pub(in crate::v02::resource::cleanup) next_id: u64,
    /// Weak dispatcher entries so the pump does not extend dispatcher
    /// lifetimes.
    pub(in crate::v02::resource::cleanup) dispatchers: HashMap<u64, Weak<CleanupDispatcherInner>>,
}

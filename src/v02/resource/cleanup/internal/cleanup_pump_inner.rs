// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Condvar;
use std::sync::Mutex;

use super::CleanupPumpState;

/// Shared signal and weak dispatcher index for the manager cleanup pump.
pub(in crate::v02::resource::cleanup) struct CleanupPumpInner {
    /// Stop/wake flags and weak dispatcher registrations inspected by the
    /// worker.
    pub(in crate::v02::resource::cleanup) state: Mutex<CleanupPumpState>,
    /// Notifies the worker when queue state changes or shutdown begins.
    pub(in crate::v02::resource::cleanup) changed: Condvar,
}

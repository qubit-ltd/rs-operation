// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::task::Waker;

/// Identifies one registered task and stores the waker used to notify it.
pub(in crate::v02::manager::capacity) struct Waiter {
    /// Unique id used to replace or remove this registration.
    pub(in crate::v02::manager::capacity) id: u64,
    /// Task notified when capacity changes or a generation race is detected.
    pub(in crate::v02::manager::capacity) waker: Waker,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Policy used to stop a supervisor or service group.

/// Shutdown policy for an admission service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownMode {
    /// Stop new roots while allowing already admitted work to finish.
    Drain,
    /// Stop admission and request cancellation of managed work.
    Cancel,
}

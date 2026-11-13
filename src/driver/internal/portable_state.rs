// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use super::PortableTask;

/// Task registry and ready queue protected by the portable scheduler mutex.
#[derive(Default)]
pub struct PortableState {
    /// Maps each registered task identifier to its shared task state.
    pub tasks: HashMap<u64, Arc<PortableTask>>,
    /// Holds task identifiers awaiting a worker poll, in scheduling order.
    pub ready: VecDeque<u64>,
    /// Stores the most recently allocated identifier for wraparound-safe
    /// allocation.
    pub next_id: u64,
    /// Prevents new scheduling and tells workers that shutdown has begun.
    pub closed: bool,
}

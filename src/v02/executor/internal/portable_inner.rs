// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Shared task queue and polling state for the portable executor.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;

use super::PortableState;

/// Shared bounded task and worker state used by portable executor handles.
pub(in crate::v02::executor) struct PortableInner {
    /// Maximum number of accepted tasks waiting for polling.
    pub(in crate::v02::executor) capacity: usize,
    /// Maximum number of workers allowed to poll concurrently.
    pub(in crate::v02::executor) max_pollers: usize,
    /// Maximum polls a worker may perform before yielding its turn.
    pub(in crate::v02::executor) polls_per_turn: usize,
    /// Number of workers currently polling a task.
    pub(in crate::v02::executor) pollers: Mutex<usize>,
    /// Queue, lifecycle flags, and task scheduling state protected together.
    pub(in crate::v02::executor) state: Mutex<PortableState>,
    /// Wakes workers when a task becomes ready or shutdown begins.
    pub(in crate::v02::executor) ready_available: Condvar,
    /// Optional observer notified when task capacity becomes available.
    pub(in crate::v02::executor) capacity_callback: Mutex<Option<Arc<dyn Fn() + Send + Sync + 'static>>>,
}

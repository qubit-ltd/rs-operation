// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::task::Waker;

use crate::report::ResourceId;
use crate::resource::cleanup_scheduler::internal::CleanupTaskState;

/// Shared counters, waiters, and task ownership for a cleanup pool.
pub(in crate::resource::cleanup_scheduler) struct SchedulerState {
    /// Maximum number of cleanup tasks holding permits simultaneously.
    pub(in crate::resource::cleanup_scheduler) limit: usize,
    /// Number of permits currently held by callers or tasks.
    pub(in crate::resource::cleanup_scheduler) active: Arc<AtomicUsize>,
    /// Tasks waiting for a permit to become available.
    pub(in crate::resource::cleanup_scheduler) capacity_waiters: Arc<Mutex<Vec<Waker>>>,
    /// Active cleanup tasks indexed by their resource identifiers.
    pub(in crate::resource::cleanup_scheduler) tasks: Mutex<BTreeMap<ResourceId, Arc<CleanupTaskState>>>,
}

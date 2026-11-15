// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Shared state and synchronization for a bounded blocking worker pool.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::thread::JoinHandle;

use super::PoolState;

/// Shared worker-pool resources used by submitters, workers, and shutdown
/// observers.
pub(in crate::v02::executor) struct PoolInner {
    /// Unique identity used to detect calls from this pool's worker threads.
    pub(in crate::v02::executor) id: u64,
    /// Number of worker threads configured for this pool.
    pub(in crate::v02::executor) worker_count: usize,
    /// Maximum number of queued jobs waiting for a worker.
    pub(in crate::v02::executor) capacity: usize,
    /// Admission, queue, and shutdown state protected as one snapshot.
    pub(in crate::v02::executor) state: Mutex<PoolState>,
    /// Wakes workers when queued work or shutdown becomes available.
    pub(in crate::v02::executor) work_available: Condvar,
    /// Wakes observers when pool lifecycle state changes.
    pub(in crate::v02::executor) changed: Condvar,
    /// Serializes ownership and joining of worker handles.
    pub(in crate::v02::executor) join_lock: Mutex<()>,
    /// Handles for the worker threads owned by this pool.
    pub(in crate::v02::executor) workers: Mutex<Vec<JoinHandle<()>>>,
    /// Optional observer notified when bounded queue capacity is released.
    pub(in crate::v02::executor) capacity_callback: Mutex<Option<Arc<dyn Fn() + Send + Sync + 'static>>>,
}

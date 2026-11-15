// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::PoolInner;
use super::lock;
use super::notify_capacity;

/// A task slot reserved before an operation starts.
pub(crate) struct ThreadPoolPermit {
    /// Shared pool state whose outstanding reservation is owned by this permit.
    pub(in crate::v02::executor) inner: Arc<PoolInner>,
    /// Whether dropping this permit must return its reservation to the pool.
    pub(in crate::v02::executor) active: bool,
}

impl ThreadPoolPermit {
    /// Enqueues a job using the reservation represented by this permit.
    ///
    /// The permit is consumed so its reservation remains held until the worker
    /// finishes the job. A reservation acquired before shutdown remains valid
    /// and is drained as part of shutdown.
    ///
    /// # Type Parameters
    ///
    /// - `F`: closure executed by a worker after it is dequeued.
    ///
    /// # Parameters
    ///
    /// - `job`: owned, thread-safe closure to enqueue.
    pub(crate) fn submit<F>(mut self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let mut state = lock(&self.inner.state);
        // Admission was reserved before shutdown. The reserved task remains
        // valid and is drained as part of shutdown.
        state.queue.push_back(Box::new(job));
        self.active = false;
        drop(state);
        self.inner.work_available.notify_one();
    }
}

impl Drop for ThreadPoolPermit {
    /// Returns an unused reservation and wakes workers and capacity waiters.
    fn drop(&mut self) {
        if self.active {
            let mut state = lock(&self.inner.state);
            state.outstanding -= 1;
            drop(state);
            self.inner.changed.notify_all();
            self.inner.work_available.notify_all();
            notify_capacity(&self.inner.capacity_callback);
        }
    }
}

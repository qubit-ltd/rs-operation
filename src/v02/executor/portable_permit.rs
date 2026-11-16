// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use super::PortableInner;
use super::PortableTask;
use super::lock;
use super::notify_capacity;

/// A pre-reserved portable task slot.
pub(crate) struct PortablePermit {
    /// Executor whose bounded task capacity this permit reserves.
    pub(in crate::v02::executor) inner: Arc<PortableInner>,
    /// Whether dropping this permit must return its reserved slot.
    pub(in crate::v02::executor) active: bool,
}

impl PortablePermit {
    /// Submits a future using the capacity held by this permit.
    ///
    /// A reservation remains valid if admission closes after it was acquired.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Future submitted to the portable executor.
    ///
    /// # Parameters
    ///
    /// - `self`: Permit holding a reserved portable task slot.
    /// - `future` is polled by the portable executor until it resolves.
    pub(crate) fn spawn<F>(mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = lock(&self.inner.state);
        // A reservation made before close remains valid and must be honored.
        let task = Arc::new(PortableTask {
            executor: Arc::downgrade(&self.inner),
            future: Mutex::new(Some(Box::pin(future))),
            queued: AtomicBool::new(true),
            completed: AtomicBool::new(false),
        });
        state.active.push(Arc::clone(&task));
        state.ready.push_back(Arc::clone(&task));
        self.active = false;
        drop(state);
        self.inner.ready_available.notify_one();
    }
}

impl Drop for PortablePermit {
    /// Returns the reserved slot if the permit was dropped without submission.
    fn drop(&mut self) {
        if self.active {
            let mut state = lock(&self.inner.state);
            state.tasks -= 1;
            drop(state);
            self.inner.ready_available.notify_all();
            notify_capacity(&self.inner.capacity_callback);
        }
    }
}

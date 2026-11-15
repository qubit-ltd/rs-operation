// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::cell::Cell;
use std::sync::Arc;

use super::CURRENT_POOL_ID;
use super::PoolInner;
use super::ShutdownError;
use super::lock;

/// Explicit shutdown observer. Waiting from one of its own workers is rejected.
#[derive(Clone)]
pub(crate) struct ShutdownHandle {
    /// Shared pool lifecycle and worker handles observed during shutdown.
    pub(in crate::v02::executor) inner: Arc<PoolInner>,
}

impl ShutdownHandle {
    /// Reports whether all accepted jobs and pool workers have finished.
    #[cfg(test)]
    pub(crate) fn is_finished(&self) -> bool {
        let state = lock(&self.inner.state);
        state.outstanding == 0 && state.exited_workers == self.inner.worker_count
    }

    /// Blocks until all accepted jobs finish and every worker is joined.
    ///
    /// # Errors
    ///
    /// Returns `WouldDeadlock` when called from a worker belonging to this
    /// pool, or `WorkerPanicked` if a job or worker panicked during
    /// shutdown.
    pub(crate) fn wait(&self) -> Result<(), ShutdownError> {
        if CURRENT_POOL_ID.with(Cell::get) == self.inner.id {
            return Err(ShutdownError::WouldDeadlock);
        }
        let _join_guard = lock(&self.inner.join_lock);
        let mut state = lock(&self.inner.state);
        while state.outstanding != 0 || state.exited_workers != self.inner.worker_count {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        let mut panicked = state.job_panicked;
        drop(state);
        let workers = std::mem::take(&mut *lock(&self.inner.workers));
        for worker in workers {
            panicked |= worker.join().is_err();
        }
        if panicked {
            Err(ShutdownError::WorkerPanicked)
        } else {
            Ok(())
        }
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::JoinHandle;

use super::ExecutorCreateError;
use super::ExecutorError;
use super::NEXT_POOL_ID;
use super::PoolInner;
use super::PoolState;
use super::ShutdownHandle;
#[cfg(test)]
use super::SubmitFailure;
use super::ThreadPoolPermit;
use super::close_pool;
use super::lock;
use super::pool_worker;
use crate::v02::report::ThreadPoolExecutorOptions;

/// Bounded blocking worker pool. Queue and running work share a reservation
/// budget of `worker_count + queue_capacity` slots.
pub(crate) struct ThreadPoolExecutor {
    /// Shared worker, queue, and shutdown state retained by active permits.
    pub(in crate::v02::executor) inner: Arc<PoolInner>,
}

impl ThreadPoolExecutor {
    /// Creates a worker pool from the supplied worker and queue limits.
    ///
    /// Returns an error when either limit is zero or a worker thread cannot be
    /// started. If starting a worker fails, already-started workers are stopped
    /// and joined before the error is returned.
    ///
    /// # Parameters
    ///
    /// * `options` sets the worker count and queued-job capacity.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorCreateError::InvalidOptions`] when either limit is
    /// zero, or [`ExecutorCreateError::ThreadSpawn`] when a worker cannot
    /// start.
    pub(crate) fn new(options: ThreadPoolExecutorOptions) -> Result<Self, ExecutorCreateError> {
        Self::new_with_spawn(options, |id, index, worker_inner| {
            let name = format!("qubit-operation-{id}-{index}");
            thread::Builder::new()
                .name(name)
                .spawn(move || pool_worker(worker_inner))
        })
    }

    /// Creates a worker pool using the supplied thread-spawn operation.
    ///
    /// The spawn callback receives the pool identifier, worker index, and
    /// shared pool state. This injection point supports deterministic tests of
    /// partial thread-start failure; successfully started workers are joined
    /// if a later callback returns an I/O error.
    ///
    /// # Parameters
    ///
    /// * `options` sets the worker count and queued-job capacity.
    /// * `spawn` creates a worker thread for each pool identifier and index.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorCreateError::InvalidOptions`] when either limit is
    /// zero, or [`ExecutorCreateError::ThreadSpawn`] when the callback fails.
    pub(in crate::v02::executor) fn new_with_spawn(
        options: ThreadPoolExecutorOptions,
        mut spawn: impl FnMut(u64, usize, Arc<PoolInner>) -> std::io::Result<JoinHandle<()>>,
    ) -> Result<Self, ExecutorCreateError> {
        if options.worker_count == 0 || options.queue_capacity == 0 {
            return Err(ExecutorCreateError::InvalidOptions);
        }
        let id = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::new(PoolInner {
            id,
            worker_count: options.worker_count,
            capacity: options.worker_count.saturating_add(options.queue_capacity),
            state: Mutex::new(PoolState {
                accepting: true,
                stopping: false,
                outstanding: 0,
                exited_workers: 0,
                job_panicked: false,
                queue: VecDeque::new(),
            }),
            work_available: Condvar::new(),
            changed: Condvar::new(),
            join_lock: Mutex::new(()),
            workers: Mutex::new(Vec::with_capacity(options.worker_count)),
            capacity_callback: Mutex::new(None),
        });
        for index in 0..options.worker_count {
            match spawn(id, index, Arc::clone(&inner)) {
                Ok(worker) => lock(&inner.workers).push(worker),
                Err(_) => {
                    close_pool(&inner);
                    let workers = std::mem::take(&mut *lock(&inner.workers));
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(ExecutorCreateError::ThreadSpawn);
                }
            }
        }
        Ok(Self { inner })
    }

    /// Reserves one slot shared by queued and currently running jobs.
    ///
    /// The returned permit releases its reservation when dropped or after its
    /// job completes. Returns [`ExecutorError::Closed`] after shutdown begins
    /// and [`ExecutorError::Full`] when all reservations are occupied.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Closed`] if shutdown has begun, or
    /// [`ExecutorError::Full`] if the shared reservation budget is exhausted.
    pub(crate) fn reserve(&self) -> Result<ThreadPoolPermit, ExecutorError> {
        let mut state = lock(&self.inner.state);
        if !state.accepting {
            return Err(ExecutorError::Closed);
        }
        if state.outstanding >= self.inner.capacity {
            return Err(ExecutorError::Full);
        }
        state.outstanding += 1;
        Ok(ThreadPoolPermit {
            inner: Arc::clone(&self.inner),
            active: true,
        })
    }

    /// Submits a job immediately, returning ownership of it when admission
    /// fails.
    ///
    /// # Type Parameters
    ///
    /// * `F` is the job closure submitted to the pool.
    ///
    /// # Parameters
    ///
    /// * `job` is run by a worker if the executor accepts the submission.
    #[cfg(test)]
    pub(crate) fn try_submit<F>(&self, job: F) -> Result<(), SubmitFailure<F>>
    where
        F: FnOnce() + Send + 'static,
    {
        match self.reserve() {
            Ok(permit) => {
                permit.submit(job);
                Ok(())
            }
            Err(error) => Err(SubmitFailure { error, job }),
        }
    }

    /// Closes admission, wakes workers, and returns a handle for a real join.
    pub(crate) fn shutdown(&self) -> ShutdownHandle {
        let handle = ShutdownHandle {
            inner: Arc::clone(&self.inner),
        };
        close_pool(&self.inner);
        handle
    }
}

impl Drop for ThreadPoolExecutor {
    /// Stops further admission and wakes workers when the last executor owner
    /// drops.
    fn drop(&mut self) {
        close_pool(&self.inner);
    }
}

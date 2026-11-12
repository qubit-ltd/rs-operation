// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;

use super::super::internal::ThreadShared;
use super::super::internal::ThreadState;
use super::super::internal::close_thread_executor;
use super::super::internal::current_thread_executor_id;
use super::super::internal::next_executor_id;
use super::super::internal::thread_worker_loop;
use super::BlockingExecutor;
use super::ExecutorError;
use super::RejectedJob;

/// Fixed-size worker pool with a bounded pending queue.
///
/// Dropping the executor closes the queue and joins every worker. It can block
/// until running closures return, so do not drop it from its own worker or from
/// code needed to release a running job.
pub struct ThreadExecutor {
    shared: Arc<ThreadShared>,
    workers: Vec<JoinHandle<()>>,
}

/// Return true on a worker thread currently servicing a [`ThreadExecutor`].
pub fn is_thread_executor_worker() -> bool {
    current_thread_executor_id() != 0
}

impl ThreadExecutor {
    /// Start a fixed worker pool and cap pending jobs.
    ///
    /// Returns [`ExecutorError::InvalidConfiguration`] for a zero limit and
    /// [`ExecutorError::WorkerSpawn`] if the operating system rejects a thread.
    pub fn new(worker_count: usize, queue_capacity: usize) -> Result<Self, ExecutorError> {
        if worker_count == 0 || queue_capacity == 0 {
            return Err(ExecutorError::InvalidConfiguration);
        }
        let shared = Arc::new(ThreadShared {
            state: Mutex::new(ThreadState {
                queue: std::collections::VecDeque::new(),
                closed: false,
            }),
            ready: Condvar::new(),
            queue_capacity,
            executor_id: next_executor_id(),
        });
        let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let worker_shared = Arc::clone(&shared);
            match thread::Builder::new()
                .name(format!("qubit-operation-blocking-{index}"))
                .spawn(move || thread_worker_loop(worker_shared))
            {
                Ok(worker) => workers.push(worker),
                Err(_) => {
                    close_thread_executor(&shared);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(ExecutorError::WorkerSpawn);
                }
            }
        }
        Ok(Self { shared, workers })
    }

    /// Return the fixed number of worker threads.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Check the current thread against this executor's worker identity.
    pub fn is_worker_thread(&self) -> bool {
        current_thread_executor_id() == self.shared.executor_id
    }
}

impl BlockingExecutor for ThreadExecutor {
    fn execute<F>(&self, job: F) -> Result<(), RejectedJob<F>>
    where
        F: FnOnce() + Send + 'static,
    {
        let mut state = lock(&self.shared.state);
        let error = if state.closed {
            Some(ExecutorError::Closed)
        } else if state.queue.len() >= self.shared.queue_capacity {
            Some(ExecutorError::Capacity)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(RejectedJob { error, job });
        }
        state.queue.push_back(Box::new(job));
        self.shared.ready.notify_one();
        Ok(())
    }

    fn is_current_thread_worker(&self) -> bool {
        self.is_worker_thread()
    }
}

impl Drop for ThreadExecutor {
    fn drop(&mut self) {
        close_thread_executor(&self.shared);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

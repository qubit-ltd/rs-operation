// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::thread;
use std::thread::JoinHandle;

use crate::driver::internal::ThreadShared;
use crate::driver::internal::ThreadState;
use crate::driver::internal::close_thread_executor;
use crate::driver::internal::current_thread_executor_id;
use crate::driver::internal::next_executor_id;
use crate::driver::internal::thread_worker_loop;
use crate::driver::thread_executor::BlockingExecutor;
use crate::driver::thread_executor::ExecutorError;
use crate::driver::thread_executor::RejectedJob;

/// Fixed-size worker pool with a bounded pending queue.
///
/// Dropping the executor closes the queue and joins every worker. It can block
/// until running closures return, so do not drop it from its own worker or from
/// code needed to release a running job.
///
/// # Examples
///
/// ```
/// use qubit_operation::driver::BlockingExecutor;
/// use qubit_operation::driver::ThreadExecutor;
///
/// let executor = ThreadExecutor::new(2, 8).expect("executor starts");
/// assert!(executor.execute(|| {}).is_ok());
/// assert_eq!(executor.worker_count(), 2);
/// ```
pub struct ThreadExecutor {
    /// Shared queue and shutdown state used by all workers.
    shared: Arc<ThreadShared>,
    /// Worker handles joined when the executor is dropped.
    workers: Vec<JoinHandle<()>>,
}

/// Check whether the current thread is servicing any [`ThreadExecutor`].
///
/// # Returns
///
/// `true` while called from a thread executor worker, regardless of which
/// executor owns that worker.
#[must_use]
#[inline]
pub fn is_thread_executor_worker() -> bool {
    current_thread_executor_id() != 0
}

impl ThreadExecutor {
    /// Start a fixed worker pool and cap pending jobs.
    ///
    /// # Parameters
    ///
    /// * `worker_count` - Number of worker threads; must be positive.
    /// * `queue_capacity` - Maximum pending jobs; must be positive.
    ///
    /// # Returns
    ///
    /// The configured executor.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::InvalidConfiguration`] for a zero limit and
    /// [`ExecutorError::WorkerSpawn`] if the operating system rejects a thread.
    ///
    /// # Panics
    ///
    /// Panics if `worker_count` exceeds the capacity supported by the worker
    /// handle vector.
    pub fn new(worker_count: usize, queue_capacity: usize) -> Result<Self, ExecutorError> {
        if worker_count == 0 || queue_capacity == 0 {
            return Err(ExecutorError::InvalidConfiguration);
        }
        let shared = Arc::new(ThreadShared {
            state: Mutex::new(ThreadState {
                queue: VecDeque::new(),
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
    ///
    /// # Returns
    ///
    /// The number of worker threads configured at construction.
    #[must_use]
    #[inline]
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Check the current thread against this executor's worker identity.
    ///
    /// # Returns
    ///
    /// `true` only when the current thread is servicing this executor.
    #[must_use]
    #[inline]
    pub fn is_worker_thread(&self) -> bool {
        current_thread_executor_id() == self.shared.executor_id
    }
}

impl BlockingExecutor for ThreadExecutor {
    /// Enqueue the closure if the executor is open and has queue capacity.
    ///
    /// # Type Parameters
    ///
    /// * `F` - A one-shot, sendable closure that owns all values it needs.
    ///
    /// # Parameters
    ///
    /// * `job` - The closure to enqueue.
    ///
    /// # Returns
    ///
    /// `Ok(())` when queued; otherwise returns the uncalled closure.
    ///
    /// # Errors
    ///
    /// Returns [`RejectedJob`] when the executor is closed or its pending queue
    /// has reached capacity.
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

    /// Report whether the caller is one of this executor's worker threads.
    ///
    /// # Returns
    ///
    /// `true` when the caller is servicing this executor.
    #[inline]
    fn is_current_thread_worker(&self) -> bool {
        self.is_worker_thread()
    }
}

impl Drop for ThreadExecutor {
    /// Close submissions and join workers after queued jobs finish.
    fn drop(&mut self) {
        close_thread_executor(&self.shared);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

/// Acquire a mutex, recovering its protected value if it was poisoned.
///
/// # Type Parameters
///
/// * `T` - The protected value type.
///
/// # Parameters
///
/// * `mutex` - The mutex to acquire.
///
/// # Returns
///
/// The guard that holds the mutex lock.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::ThreadExecutor;
    use crate::driver::BlockingExecutor;
    use crate::driver::ExecutorError;
    use crate::driver::thread_executor::is_thread_executor_worker;

    #[test]
    fn test_executor_rejects_full_queue_without_running_rejected_job() {
        let executor = ThreadExecutor::new(1, 1).expect("executor starts");
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let (queued_sender, queued_receiver) = mpsc::sync_channel(1);

        executor
            .execute(move || {
                started_sender.send(()).expect("test is waiting");
                release_receiver.recv().expect("test releases worker");
            })
            .unwrap_or_else(|_| panic!("first job is accepted"));
        started_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("worker started first job");
        executor
            .execute(move || queued_sender.send(()).expect("test receives queued job"))
            .unwrap_or_else(|_| panic!("one job fits the pending queue"));

        let (rejected_sender, rejected_receiver) = mpsc::sync_channel(1);
        let rejected = executor
            .execute(move || rejected_sender.send(()).expect("rejected job can be recovered"))
            .expect_err("pending queue is full");
        assert_eq!(rejected.error, ExecutorError::Capacity);
        assert!(rejected_receiver.try_recv().is_err());

        release_sender.send(()).expect("release first job");
        queued_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("queued job runs after first job returns");
        (rejected.job)();
        rejected_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("rejected job remains callable by its owner");
    }

    #[test]
    fn test_worker_identity_is_specific_to_each_executor() {
        let first = ThreadExecutor::new(1, 1).expect("first executor starts");
        let second = ThreadExecutor::new(1, 1).expect("second executor starts");
        let (sender, receiver) = mpsc::sync_channel(1);
        let first_id = first.shared.executor_id;
        let second_id = second.shared.executor_id;

        first
            .execute(move || {
                sender
                    .send((
                        is_thread_executor_worker(),
                        crate::driver::internal::current_thread_executor_id() == first_id,
                        crate::driver::internal::current_thread_executor_id() == second_id,
                    ))
                    .expect("test receives worker identity");
            })
            .unwrap_or_else(|_| panic!("job is accepted"));
        assert_eq!(
            receiver.recv_timeout(Duration::from_secs(5)).expect("worker runs job"),
            (true, true, false)
        );
    }
}

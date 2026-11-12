// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::task::Waker;
use std::thread;
use std::thread::JoinHandle;

use super::super::internal::PortableShared;
use super::super::internal::PortableState;
use super::super::internal::PortableTask;
use super::super::internal::PortableTaskWake;
use super::super::internal::close_portable;
use super::super::internal::portable_worker_loop;
use super::PortableDriverConfig;
use super::PortableDriverError;
use super::TaskHandle;

/// Future driver that owns a fixed worker set and a bounded ready queue.
///
/// Dropping the driver stops polling, joins the workers, and drops unfinished
/// futures on the dropping thread. Poll implementations and future destructors
/// must therefore be non-blocking; the owner must retain the driver until its
/// tasks no longer carry resource-release responsibility.
pub struct PortableDriver {
    shared: Arc<PortableShared>,
    workers: Vec<JoinHandle<()>>,
}

impl PortableDriver {
    /// Start a fixed worker set with bounded task storage.
    ///
    /// Returns [`PortableDriverError::InvalidConfiguration`] if any configured
    /// limit is zero, or [`PortableDriverError::WorkerSpawn`] if a thread
    /// cannot be created.
    pub fn new(config: PortableDriverConfig) -> Result<Self, PortableDriverError> {
        if config.worker_count == 0 || config.task_capacity == 0 || config.polls_per_turn == 0 {
            return Err(PortableDriverError::InvalidConfiguration);
        }
        let shared = Arc::new(PortableShared {
            state: Mutex::new(PortableState::default()),
            ready: Condvar::new(),
            task_capacity: config.task_capacity,
            polls_per_turn: config.polls_per_turn,
            #[cfg(test)]
            active_workers: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(config.worker_count);
        for index in 0..config.worker_count {
            let worker_shared = Arc::clone(&shared);
            match thread::Builder::new()
                .name(format!("qubit-operation-driver-{index}"))
                .spawn(move || portable_worker_loop(worker_shared))
            {
                Ok(worker) => workers.push(worker),
                Err(_) => {
                    close_portable(&shared);
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(PortableDriverError::WorkerSpawn);
                }
            }
        }
        Ok(Self { shared, workers })
    }

    /// Accept a Send future, or reject it without polling it if capacity is
    /// full.
    ///
    /// Returns [`PortableDriverError::Capacity`] when the task limit is full
    /// and [`PortableDriverError::Closed`] after shutdown begins.
    ///
    /// The driver does not inspect or replace runtime-bound futures. Only
    /// submit futures that can progress on its worker threads; use an
    /// adapter attached to the owning runtime for reactor-bound futures.
    pub fn spawn<F>(&self, future: F) -> Result<TaskHandle, PortableDriverError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut state = lock(&self.shared.state);
        if state.closed {
            return Err(PortableDriverError::Closed);
        }
        if state.tasks.len() >= self.shared.task_capacity {
            return Err(PortableDriverError::Capacity);
        }
        state.next_id = state.next_id.wrapping_add(1).max(1);
        let id = state.next_id;
        let wake = Waker::from(Arc::new(PortableTaskWake {
            shared: Arc::downgrade(&self.shared),
            id,
        }));
        let task = Arc::new(PortableTask {
            future: Mutex::new(Some(Box::pin(future))),
            queued: std::sync::atomic::AtomicBool::new(true),
            wake,
        });
        state.tasks.insert(id, task);
        state.ready.push_back(id);
        self.shared.ready.notify_one();
        Ok(TaskHandle(id))
    }

    /// Return the configured worker count.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Return the number of accepted tasks that have not completed.
    pub fn task_count(&self) -> usize {
        lock(&self.shared.state).tasks.len()
    }
}

impl Drop for PortableDriver {
    fn drop(&mut self) {
        close_portable(&self.shared);
        let current_thread = thread::current().id();
        for worker in self.workers.drain(..) {
            if worker.thread().id() == current_thread {
                // The last admission lease can release the shared supervisor from
                // inside a managed task. Detach this already-closing worker rather
                // than asking it to join itself; it exits on the closed flag.
                drop(worker);
            } else {
                let _ = worker.join();
            }
        }
        let tasks: Vec<Arc<PortableTask>> = {
            let mut state = lock(&self.shared.state);
            state.ready.clear();
            state.tasks.drain().map(|(_, task)| task).collect()
        };
        for task in tasks {
            let future = lock(&task.future).take();
            drop(future);
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use super::PortableDriver;
    use super::PortableDriverConfig;

    struct DropNotice {
        sender: mpsc::SyncSender<bool>,
        completed: Arc<AtomicBool>,
    }

    impl Drop for DropNotice {
        fn drop(&mut self) {
            let _ = self.sender.send(self.completed.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn dropping_driver_on_its_own_worker_detaches_self_and_drains_workers() {
        let driver = PortableDriver::new(PortableDriverConfig {
            worker_count: 2,
            task_capacity: 1,
            polls_per_turn: 1,
        })
        .expect("driver starts");
        let shared = Arc::clone(&driver.shared);
        let owner = Arc::new(Mutex::new(Some(driver)));
        let (sender, receiver) = mpsc::sync_channel(1);
        let completed = Arc::new(AtomicBool::new(false));
        let task_owner = Arc::clone(&owner);
        let task_completed = Arc::clone(&completed);
        let _task = owner
            .lock()
            .unwrap()
            .as_ref()
            .expect("driver owner exists")
            .spawn(async move {
                let _notice = DropNotice {
                    sender,
                    completed: Arc::clone(&task_completed),
                };
                let last_owner = task_owner.lock().unwrap().take().expect("last driver owner");
                drop(last_owner);
                task_completed.store(true, Ordering::SeqCst);
            })
            .expect("managed task accepted");
        drop(owner);

        assert!(
            receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("managed task drops its observable notice")
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while shared.active_workers.load(Ordering::Acquire) != 0 {
            assert!(Instant::now() < deadline, "closing workers exit within watchdog");
            thread::yield_now();
        }
    }
}

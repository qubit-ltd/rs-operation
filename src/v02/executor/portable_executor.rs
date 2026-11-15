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
use std::task::Context;
use std::task::Waker;
use std::thread;
use std::thread::JoinHandle;

use super::ExecutorCreateError;
use super::ExecutorError;
use super::PollSlot;
use super::PortableInner;
use super::PortablePermit;
use super::PortableState;
use super::lock;
use super::notify_capacity;
use crate::v02::report::PortableExecutorOptions;

/// Cooperative, runtime-independent executor for `Send` futures.
///
/// The owner of this executor calls `poll_once` from its own scheduling loop;
/// no hidden thread is created. A task is only polled again after its waker is
/// invoked. Reservations account for queued and currently-polled tasks.
#[derive(Clone)]
pub(crate) struct PortableExecutor {
    /// Shared queue and polling state held across executor clones.
    pub(in crate::v02::executor) inner: Arc<PortableInner>,
}

impl PortableExecutor {
    /// Creates a bounded executor without starting worker threads.
    ///
    /// # Parameters
    ///
    /// * `options` sets worker concurrency, queued task capacity, and polls per
    ///   turn.
    ///
    /// # Errors
    ///
    /// Returns `InvalidOptions` if any configured limit is zero.
    pub(crate) fn new(options: PortableExecutorOptions) -> Result<Self, ExecutorCreateError> {
        if options.worker_count == 0 || options.task_capacity == 0 || options.polls_per_turn == 0 {
            return Err(ExecutorCreateError::InvalidOptions);
        }
        Ok(Self {
            inner: Arc::new(PortableInner {
                capacity: options.task_capacity,
                max_pollers: options.worker_count,
                polls_per_turn: options.polls_per_turn,
                pollers: Mutex::new(0),
                state: Mutex::new(PortableState {
                    accepting: true,
                    tasks: 0,
                    ready: VecDeque::new(),
                }),
                ready_available: Condvar::new(),
                capacity_callback: Mutex::new(None),
            }),
        })
    }

    /// Starts `count` worker threads that poll tasks when they are woken.
    ///
    /// # Parameters
    ///
    /// * `count` is the number of worker threads to start.
    /// * `name` is the thread-name prefix.
    ///
    /// # Errors
    ///
    /// Returns `ThreadSpawn` if a worker cannot be created; previously started
    /// workers are shut down and joined before returning.
    pub(crate) fn start_workers(
        &self,
        count: usize,
        name: &'static str,
    ) -> Result<Vec<JoinHandle<()>>, ExecutorCreateError> {
        self.start_workers_with(count, |index, executor| {
            let worker_name = format!("{name}-{index}");
            thread::Builder::new()
                .name(worker_name)
                .spawn(move || executor.worker_loop())
        })
    }

    /// Starts workers through a caller-provided spawn operation.
    ///
    /// This variant allows internal tests to control thread creation and keeps
    /// partial startup cleanup in the executor.
    ///
    /// # Parameters
    ///
    /// * `count` is the number of workers to start.
    /// * `spawn` creates each worker handle for its index and executor clone.
    ///
    /// # Errors
    ///
    /// Returns `ThreadSpawn` if any spawn operation fails, after joining
    /// workers that were already started.
    pub(in crate::v02::executor) fn start_workers_with(
        &self,
        count: usize,
        mut spawn: impl FnMut(usize, PortableExecutor) -> std::io::Result<JoinHandle<()>>,
    ) -> Result<Vec<JoinHandle<()>>, ExecutorCreateError> {
        let mut workers = Vec::with_capacity(count);
        for index in 0..count {
            let executor = self.clone();
            match spawn(index, executor) {
                Ok(worker) => workers.push(worker),
                Err(_) => {
                    self.close_admission();
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(ExecutorCreateError::ThreadSpawn);
                }
            }
        }
        Ok(workers)
    }

    /// Runs the worker wait/poll loop until admission is closed and tasks
    /// drain.
    pub(in crate::v02::executor) fn worker_loop(&self) {
        loop {
            let mut state = lock(&self.inner.state);
            while state.ready.is_empty() && (state.accepting || state.tasks != 0) {
                state = self
                    .inner
                    .ready_available
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            if state.ready.is_empty() && !state.accepting && state.tasks == 0 {
                return;
            }
            drop(state);
            self.poll_turn();
        }
    }

    /// Reserves one bounded task slot before a future is submitted.
    ///
    /// # Errors
    ///
    /// Returns `Closed` after admission closes or `Full` when all task slots
    /// are already reserved.
    pub(crate) fn reserve(&self) -> Result<PortablePermit, ExecutorError> {
        let mut state = lock(&self.inner.state);
        if !state.accepting {
            return Err(ExecutorError::Closed);
        }
        if state.tasks == self.inner.capacity {
            return Err(ExecutorError::Full);
        }
        state.tasks += 1;
        Ok(PortablePermit {
            inner: Arc::clone(&self.inner),
            active: true,
        })
    }

    /// Polls one correctly-woken task, returning whether a task was polled.
    pub(crate) fn poll_once(&self) -> bool {
        {
            let mut pollers = lock(&self.inner.pollers);
            if *pollers >= self.inner.max_pollers {
                return false;
            }
            *pollers += 1;
        }
        let _poll_slot = PollSlot(&self.inner.pollers);
        loop {
            let task = lock(&self.inner.state).ready.pop_front();
            let Some(task) = task else { return false };
            task.queued.store(false, Ordering::Release);
            if task.completed.load(Ordering::Acquire) {
                continue;
            }
            let waker = Waker::from(Arc::clone(&task));
            let mut context = Context::from_waker(&waker);
            let poll = {
                let mut future = lock(&task.future);
                if task.completed.load(Ordering::Acquire) {
                    continue;
                }
                let Some(future) = future.as_mut() else {
                    continue;
                };
                let poll = future.as_mut().poll(&mut context);
                if poll.is_ready() {
                    task.completed.store(true, Ordering::Release);
                }
                poll
            };
            if poll.is_ready() {
                let future = lock(&task.future).take();
                drop(future);
                if let Some(inner) = task.executor.upgrade() {
                    let mut state = lock(&inner.state);
                    state.ready.retain(|queued| !Arc::ptr_eq(queued, &task));
                    state.tasks -= 1;
                    drop(state);
                    inner.ready_available.notify_all();
                    notify_capacity(&inner.capacity_callback);
                }
            }
            return true;
        }
    }

    /// Polls at most the configured number of tasks for one scheduling turn.
    pub(crate) fn poll_turn(&self) -> usize {
        (0..self.inner.polls_per_turn).take_while(|_| self.poll_once()).count()
    }

    /// Rejects future reservations and wakes workers to observe shutdown.
    pub(crate) fn close_admission(&self) {
        lock(&self.inner.state).accepting = false;
        self.inner.ready_available.notify_all();
    }

    /// Installs an observer notified when task capacity is released.
    ///
    /// # Parameters
    ///
    /// * `callback` is invoked after a task releases its reserved capacity.
    pub(in crate::v02::executor) fn set_capacity_callback(&self, callback: Arc<dyn Fn() + Send + Sync + 'static>) {
        *lock(&self.inner.capacity_callback) = Some(callback);
    }
}

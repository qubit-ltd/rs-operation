// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

/// Stable identifier for a task held by one manual driver.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TaskId(usize);

#[derive(Default)]
struct DriverState {
    next_task: usize,
    tasks: BTreeMap<TaskId, Arc<Mutex<DriverTask>>>,
    trace: Vec<String>,
}

struct DriverInner {
    state: Mutex<DriverState>,
    wakes: AtomicUsize,
}

struct DriverTask {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    waker: Waker,
}

struct DriverWake {
    inner: Weak<DriverInner>,
    label: String,
}

/// Executor that polls only when a test explicitly asks it to do so.
#[derive(Clone)]
pub struct ManualDriver {
    inner: Arc<DriverInner>,
}

impl ManualDriver {
    /// Create a driver with an empty script and zero observed calls.
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a future under a label and return its stable task identifier.
    pub fn spawn<F>(&self, label: &str, future: F) -> TaskId
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task_id = {
            let mut state = self.inner.state.lock().expect("driver mutex poisoned");
            state.next_task += 1;
            TaskId(state.next_task)
        };
        let waker = Waker::from(Arc::new(DriverWake {
            inner: Arc::downgrade(&self.inner),
            label: label.to_owned(),
        }));
        let task = Arc::new(Mutex::new(DriverTask {
            future: Box::pin(TrackedFuture {
                future: Box::pin(future),
                inner: Arc::clone(&self.inner),
                label: label.to_owned(),
            }),
            waker,
        }));
        self.inner
            .state
            .lock()
            .expect("driver mutex poisoned")
            .tasks
            .insert(task_id, task);
        task_id
    }

    /// Poll one task exactly once and return the future's actual poll result.
    pub fn poll_once(&self, task_id: TaskId) -> Poll<()> {
        let task = self
            .inner
            .state
            .lock()
            .expect("driver mutex poisoned")
            .tasks
            .get(&task_id)
            .cloned()
            .expect("task must be registered before polling");
        let mut task = task.lock().expect("task mutex poisoned");
        let waker = task.waker.clone();
        let mut context = Context::from_waker(&waker);
        let poll = task.future.as_mut().poll(&mut context);
        let completed = poll.is_ready();
        drop(task);
        if completed {
            let removed = {
                self.inner
                    .state
                    .lock()
                    .expect("driver mutex poisoned")
                    .tasks
                    .remove(&task_id)
            };
            drop(removed);
        }
        poll
    }

    /// Invoke the task waker as a real scheduling event without polling the
    /// task.
    pub fn wake(&self, task_id: TaskId) {
        let task = self
            .inner
            .state
            .lock()
            .expect("driver mutex poisoned")
            .tasks
            .get(&task_id)
            .cloned()
            .expect("task must be registered before waking");
        let waker = task.lock().expect("task mutex poisoned").waker.clone();
        waker.wake_by_ref();
    }

    /// Drop a task that has not completed and record its actual destruction.
    pub fn finish(&self, task_id: TaskId) {
        let removed = {
            self.inner
                .state
                .lock()
                .expect("driver mutex poisoned")
                .tasks
                .remove(&task_id)
        };
        drop(removed.expect("task must be registered before finishing"));
    }

    /// Return the actual event sequence emitted by polls, wakes, and task
    /// drops.
    pub fn trace(&self) -> Vec<String> {
        self.inner.state.lock().expect("driver mutex poisoned").trace.clone()
    }

    /// Return the number of actual calls through task wakers.
    pub fn wake_count(&self) -> usize {
        self.inner.wakes.load(Ordering::SeqCst)
    }
}

impl Default for ManualDriver {
    fn default() -> Self {
        Self {
            inner: Arc::new(DriverInner {
                state: Mutex::default(),
                wakes: AtomicUsize::new(0),
            }),
        }
    }
}

struct TrackedFuture<F> {
    future: Pin<Box<F>>,
    inner: Arc<DriverInner>,
    label: String,
}

impl<F> Future for TrackedFuture<F>
where
    F: Future<Output = ()>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner
            .state
            .lock()
            .expect("driver mutex poisoned")
            .trace
            .push(format!("poll:{}", self.label));
        self.future.as_mut().poll(context)
    }
}

impl<F> Drop for TrackedFuture<F> {
    fn drop(&mut self) {
        self.inner
            .state
            .lock()
            .expect("driver mutex poisoned")
            .trace
            .push(format!("drop:{}", self.label));
    }
}

impl Wake for DriverWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        inner.wakes.fetch_add(1, Ordering::SeqCst);
        inner
            .state
            .lock()
            .expect("driver mutex poisoned")
            .trace
            .push(format!("wake:{}", self.label));
    }
}

struct RecordingStep {
    pending_once: bool,
}

impl Future for RecordingStep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.pending_once {
            self.pending_once = false;
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

/// Build a future whose completion is controlled only by successive manual
/// polls.
pub fn recording_step() -> impl Future<Output = ()> + Send + 'static {
    RecordingStep { pending_once: true }
}

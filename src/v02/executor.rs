//! Bounded execution engines for the staged 0.2 API.

use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::{Context, Wake, Waker};
use std::thread::{self, JoinHandle};

use super::report::{PortableExecutorOptions, ThreadPoolExecutorOptions};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_POOL_ID: Cell<u64> = const { Cell::new(0) };
}

/// Executor admission failure. A rejected submit preserves and returns its job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutorError {
    Full,
    Closed,
}

/// Invalid executor configuration or failure to create a worker thread.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutorCreateError {
    InvalidOptions,
    ThreadSpawn,
}

/// Failure to submit a job, retaining ownership for retry or caller cleanup.
pub(crate) struct SubmitFailure<F> {
    error: ExecutorError,
    job: F,
}

impl<F> std::fmt::Debug for SubmitFailure<F> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubmitFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<F> SubmitFailure<F> {
    pub(crate) fn error(&self) -> ExecutorError {
        self.error
    }

    pub(crate) fn into_job(self) -> F {
        self.job
    }
}

/// Cooperative, runtime-independent executor for `Send` futures.
///
/// The owner of this executor calls `poll_once` from its own scheduling loop;
/// no hidden thread is created. A task is only polled again after its waker is
/// invoked. Reservations account for queued and currently-polled tasks.
#[derive(Clone)]
pub(crate) struct PortableExecutor {
    inner: Arc<PortableInner>,
}

struct PortableInner {
    capacity: usize,
    max_pollers: usize,
    polls_per_turn: usize,
    pollers: Mutex<usize>,
    state: Mutex<PortableState>,
    ready_available: Condvar,
    capacity_callback: Mutex<Option<Arc<dyn Fn() + Send + Sync + 'static>>>,
}

struct PortableState {
    accepting: bool,
    tasks: usize,
    ready: VecDeque<Arc<PortableTask>>,
}

struct PortableTask {
    executor: Weak<PortableInner>,
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>,
    queued: AtomicBool,
    completed: AtomicBool,
}

/// A pre-reserved portable task slot.
pub(crate) struct PortablePermit {
    inner: Arc<PortableInner>,
    active: bool,
}

impl PortableExecutor {
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

    fn start_workers_with(
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

    fn worker_loop(&self) {
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
            self.poll_once();
        }
    }

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
        (0..self.inner.polls_per_turn)
            .take_while(|_| self.poll_once())
            .count()
    }

    pub(crate) fn close_admission(&self) {
        lock(&self.inner.state).accepting = false;
        self.inner.ready_available.notify_all();
    }

    fn set_capacity_callback(&self, callback: Arc<dyn Fn() + Send + Sync + 'static>) {
        *lock(&self.inner.capacity_callback) = Some(callback);
    }
}

impl PortablePermit {
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
        state.ready.push_back(Arc::clone(&task));
        self.active = false;
        drop(state);
        self.inner.ready_available.notify_one();
    }
}

impl Drop for PortablePermit {
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

impl Wake for PortableTask {
    fn wake(self: Arc<Self>) {
        self.schedule();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.schedule();
    }
}

impl PortableTask {
    fn schedule(self: &Arc<Self>) {
        if self.completed.load(Ordering::Acquire) || self.queued.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(inner) = self.executor.upgrade() else {
            self.queued.store(false, Ordering::Release);
            return;
        };
        let mut state = lock(&inner.state);
        if self.completed.load(Ordering::Acquire) {
            self.queued.store(false, Ordering::Release);
        } else {
            state.ready.push_back(Arc::clone(self));
            drop(state);
            inner.ready_available.notify_one();
        }
    }
}

struct PollSlot<'a>(&'a Mutex<usize>);

impl Drop for PollSlot<'_> {
    fn drop(&mut self) {
        *lock(self.0) -= 1;
    }
}

/// Bounded blocking worker pool. Queue and running work share a reservation
/// budget of `worker_count + queue_capacity` slots.
pub(crate) struct ThreadPoolExecutor {
    inner: Arc<PoolInner>,
}

/// Independent bounded dispatch endpoint reserved for resource cleanup.
///
/// Construct a separate instance from the ordinary business pool so cleanup
/// cannot be starved by user work. Values passed to `CleanupPermit::drop_value`
/// are destroyed only by this endpoint's blocking worker.
pub(crate) struct CleanupExecutor {
    blocking: ThreadPoolExecutor,
    asynchronous: PortableExecutor,
}

/// Pre-reserved capacity for one blocking resource destructor.
pub(crate) struct CleanupPermit {
    permit: ThreadPoolPermit,
}

struct PoolInner {
    id: u64,
    worker_count: usize,
    capacity: usize,
    state: Mutex<PoolState>,
    work_available: Condvar,
    changed: Condvar,
    join_lock: Mutex<()>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    capacity_callback: Mutex<Option<Arc<dyn Fn() + Send + Sync + 'static>>>,
}

struct PoolState {
    accepting: bool,
    stopping: bool,
    outstanding: usize,
    exited_workers: usize,
    job_panicked: bool,
    queue: VecDeque<Box<dyn FnOnce() + Send + 'static>>,
}

/// A task slot reserved before an operation starts.
pub(crate) struct ThreadPoolPermit {
    inner: Arc<PoolInner>,
    active: bool,
}

/// Explicit shutdown observer. Waiting from one of its own workers is rejected.
#[derive(Clone)]
pub(crate) struct ShutdownHandle {
    inner: Arc<PoolInner>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShutdownError {
    WouldDeadlock,
    WorkerPanicked,
}

impl ThreadPoolExecutor {
    pub(crate) fn new(options: ThreadPoolExecutorOptions) -> Result<Self, ExecutorCreateError> {
        Self::new_with_spawn(options, |id, index, worker_inner| {
            let name = format!("qubit-operation-{id}-{index}");
            thread::Builder::new()
                .name(name)
                .spawn(move || pool_worker(worker_inner))
        })
    }

    fn new_with_spawn(
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

impl CleanupExecutor {
    pub(crate) fn new(
        blocking_options: ThreadPoolExecutorOptions,
        async_options: PortableExecutorOptions,
    ) -> Result<Self, ExecutorCreateError> {
        Ok(Self {
            blocking: ThreadPoolExecutor::new(blocking_options)?,
            asynchronous: PortableExecutor::new(async_options)?,
        })
    }

    /// Reserves bounded capacity before removing a resource owner.
    pub(crate) fn reserve_drop(&self) -> Result<CleanupPermit, ExecutorError> {
        self.blocking
            .reserve()
            .map(|permit| CleanupPermit { permit })
    }

    /// Reserves a slot for asynchronous cleanup without polling user code.
    pub(crate) fn reserve_async(&self) -> Result<PortablePermit, ExecutorError> {
        self.asynchronous.reserve()
    }

    pub(crate) fn start_async_workers(
        &self,
        count: usize,
        name: &'static str,
    ) -> Result<Vec<JoinHandle<()>>, ExecutorCreateError> {
        self.asynchronous.start_workers(count, name)
    }

    pub(crate) fn set_capacity_callback(&self, callback: Arc<dyn Fn() + Send + Sync + 'static>) {
        *lock(&self.blocking.inner.capacity_callback) = Some(Arc::clone(&callback));
        self.asynchronous.set_capacity_callback(callback);
    }

    /// Advances one scheduled asynchronous cleanup future.
    pub(crate) fn poll_async_once(&self) -> bool {
        self.asynchronous.poll_once()
    }

    pub(crate) fn poll_async_turn(&self) -> usize {
        self.asynchronous.poll_turn()
    }

    pub(crate) fn close_admission(&self) {
        self.asynchronous.close_admission();
        close_pool(&self.blocking.inner);
    }

    /// Closes both cleanup paths and returns an observer for blocking workers.
    pub(crate) fn shutdown(&self) -> ShutdownHandle {
        self.close_admission();
        self.blocking.shutdown()
    }
}

impl Drop for ThreadPoolExecutor {
    fn drop(&mut self) {
        close_pool(&self.inner);
    }
}

impl ThreadPoolPermit {
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

impl CleanupPermit {
    /// Transfers `value` to a cleanup worker, where its destructor runs.
    pub(crate) fn drop_value<T: Send + 'static>(self, value: T) {
        self.permit.submit(move || drop(value));
    }
}

impl Drop for ThreadPoolPermit {
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

impl ShutdownHandle {
    pub(crate) fn is_finished(&self) -> bool {
        let state = lock(&self.inner.state);
        state.outstanding == 0 && state.exited_workers == self.inner.worker_count
    }

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

fn close_pool(inner: &PoolInner) {
    let mut state = lock(&inner.state);
    state.accepting = false;
    state.stopping = true;
    drop(state);
    inner.work_available.notify_all();
    inner.changed.notify_all();
}

fn pool_worker(inner: Arc<PoolInner>) {
    CURRENT_POOL_ID.with(|current| current.set(inner.id));
    loop {
        let job = {
            let mut state = lock(&inner.state);
            loop {
                if let Some(job) = state.queue.pop_front() {
                    break Some(job);
                }
                if state.stopping && state.outstanding == 0 {
                    state.exited_workers += 1;
                    inner.changed.notify_all();
                    break None;
                }
                state = inner
                    .work_available
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        };
        let Some(job) = job else { break };
        if panic::catch_unwind(AssertUnwindSafe(job)).is_err() {
            lock(&inner.state).job_panicked = true;
        }
        let mut state = lock(&inner.state);
        state.outstanding -= 1;
        drop(state);
        inner.changed.notify_all();
        inner.work_available.notify_all();
        notify_capacity(&inner.capacity_callback);
    }
    CURRENT_POOL_ID.with(|current| current.set(0));
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn notify_capacity(callback: &Mutex<Option<Arc<dyn Fn() + Send + Sync + 'static>>>) {
    let callback = lock(callback).as_ref().map(Arc::clone);
    if let Some(callback) = callback {
        callback();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v02::report::{PortableExecutorOptions, ThreadPoolExecutorOptions};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::{Duration, Instant};

    struct Gate {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }

    impl Gate {
        fn new() -> Self {
            Self {
                state: Mutex::new((false, false)),
                changed: Condvar::new(),
            }
        }

        fn enter_and_wait(&self) {
            let mut state = self.state.lock().unwrap();
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self.changed.wait(state).unwrap();
            }
        }

        fn wait_started(&self) {
            let mut state = self.state.lock().unwrap();
            while !state.0 {
                state = self.changed.wait(state).unwrap();
            }
        }

        fn release(&self) {
            let mut state = self.state.lock().unwrap();
            state.1 = true;
            self.changed.notify_all();
        }
    }

    fn options(workers: usize, queue: usize) -> ThreadPoolExecutorOptions {
        ThreadPoolExecutorOptions {
            worker_count: workers,
            queue_capacity: queue,
        }
    }

    #[derive(Default)]
    struct SavedWaker(Mutex<Option<Waker>>);

    struct PendingUntilWoken {
        saved: Arc<SavedWaker>,
        polls: Arc<AtomicUsize>,
    }

    impl Future for PendingUntilWoken {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let poll = self.polls.fetch_add(1, Ordering::SeqCst);
            if poll == 0 {
                *self.saved.0.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    #[test]
    fn pending_future_requires_real_wake() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 1,
            task_capacity: 2,
            polls_per_turn: 8,
        })
        .unwrap();
        let saved = Arc::new(SavedWaker::default());
        let polls = Arc::new(AtomicUsize::new(0));
        executor.reserve().unwrap().spawn(PendingUntilWoken {
            saved: Arc::clone(&saved),
            polls: Arc::clone(&polls),
        });

        assert_eq!(executor.poll_turn(), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(
            !executor.poll_once(),
            "a pending future must not be busy-polled"
        );
        saved.0.lock().unwrap().take().unwrap().wake();
        assert!(executor.poll_once());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(!executor.poll_once());
    }

    struct WakeWhilePolling {
        polls: Arc<AtomicUsize>,
    }

    impl Future for WakeWhilePolling {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    #[test]
    fn wake_during_poll_is_not_lost() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 1,
            task_capacity: 1,
            polls_per_turn: 8,
        })
        .unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        executor.reserve().unwrap().spawn(WakeWhilePolling {
            polls: Arc::clone(&polls),
        });

        assert!(executor.poll_once());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(executor.poll_once());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(!executor.poll_once());
    }

    #[test]
    fn queue_full_preserves_job() {
        let pool = ThreadPoolExecutor::new(options(1, 1)).unwrap();
        let gate = Arc::new(Gate::new());
        let worker_gate = Arc::clone(&gate);
        pool.try_submit(move || worker_gate.enter_and_wait())
            .unwrap();
        gate.wait_started();

        let queued = Arc::new(AtomicBool::new(false));
        let queued_job = Arc::clone(&queued);
        pool.try_submit(move || queued_job.store(true, Ordering::SeqCst))
            .unwrap();
        let rejected = Arc::new(AtomicBool::new(false));
        let rejected_job = Arc::clone(&rejected);
        let failure = pool
            .try_submit(move || rejected_job.store(true, Ordering::SeqCst))
            .expect_err("worker plus bounded queue must reject excess work");
        assert_eq!(failure.error(), ExecutorError::Full);
        (failure.into_job())();
        assert!(rejected.load(Ordering::SeqCst));

        gate.release();
        pool.shutdown().wait().unwrap();
        assert!(queued.load(Ordering::SeqCst));
    }

    #[test]
    fn dropped_public_handle_does_not_join() {
        let pool = ThreadPoolExecutor::new(options(1, 1)).unwrap();
        let gate = Arc::new(Gate::new());
        let worker_gate = Arc::clone(&gate);
        pool.try_submit(move || worker_gate.enter_and_wait())
            .unwrap();
        gate.wait_started();
        let shutdown = pool.shutdown();

        let started = Instant::now();
        drop(pool);
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(!shutdown.is_finished());
        gate.release();
        shutdown.wait().unwrap();
        assert!(shutdown.is_finished());
    }

    #[test]
    fn explicit_shutdown_waits_workers() {
        let pool = ThreadPoolExecutor::new(options(1, 1)).unwrap();
        let gate = Arc::new(Gate::new());
        let worker_gate = Arc::clone(&gate);
        pool.try_submit(move || worker_gate.enter_and_wait())
            .unwrap();
        gate.wait_started();
        let shutdown = pool.shutdown();
        let waiter = shutdown.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let waiter_finished = Arc::clone(&finished);
        let join = std::thread::spawn(move || {
            waiter.wait().unwrap();
            waiter_finished.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(20));
        assert!(!finished.load(Ordering::SeqCst));
        gate.release();
        join.join().unwrap();
        assert!(finished.load(Ordering::SeqCst));
    }

    #[test]
    fn blocking_worker_detects_self_wait() {
        let pool = ThreadPoolExecutor::new(options(1, 1)).unwrap();
        let permit = pool.reserve().unwrap();
        let shutdown = pool.shutdown();
        let worker_shutdown = shutdown.clone();
        let (send, receive) = std::sync::mpsc::channel();
        permit.submit(move || send.send(worker_shutdown.wait()).unwrap());
        assert_eq!(
            receive.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err(ShutdownError::WouldDeadlock)
        );
        shutdown.wait().unwrap();
    }

    #[test]
    fn portable_worker_spawn_failure_joins_started_workers() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 2,
            task_capacity: 2,
            polls_per_turn: 1,
        })
        .unwrap();
        let exited = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&exited);
        let result = executor.start_workers_with(2, move |index, executor| {
            if index == 1 {
                return Err(std::io::Error::other("injected spawn failure"));
            }
            let exited = Arc::clone(&observed);
            Ok(thread::spawn(move || {
                executor.worker_loop();
                exited.fetch_add(1, Ordering::SeqCst);
            }))
        });
        assert_eq!(result.unwrap_err(), ExecutorCreateError::ThreadSpawn);
        assert_eq!(exited.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn thread_pool_worker_spawn_failure_joins_started_workers() {
        let exited = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&exited);
        let result = ThreadPoolExecutor::new_with_spawn(options(2, 1), move |_, index, inner| {
            if index == 1 {
                return Err(std::io::Error::other("injected spawn failure"));
            }
            let exited = Arc::clone(&observed);
            Ok(thread::spawn(move || {
                pool_worker(inner);
                exited.fetch_add(1, Ordering::SeqCst);
            }))
        });
        assert!(matches!(result, Err(ExecutorCreateError::ThreadSpawn)));
        assert_eq!(exited.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cleanup_destructor_runs_on_reserved_cleanup_worker() {
        let executor = CleanupExecutor::new(
            options(1, 1),
            PortableExecutorOptions {
                worker_count: 1,
                task_capacity: 1,
                polls_per_turn: 1,
            },
        )
        .unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let submitter = thread::current().id();
        struct Probe(std::sync::mpsc::Sender<(thread::ThreadId, Option<String>)>);
        impl Drop for Probe {
            fn drop(&mut self) {
                let current = thread::current();
                let _ = self
                    .0
                    .send((current.id(), current.name().map(str::to_owned)));
            }
        }

        executor.reserve_drop().unwrap().drop_value(Probe(send));
        let (dropper, name) = receive.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_ne!(dropper, submitter);
        assert!(name.unwrap().starts_with("qubit-operation-"));
        executor.shutdown().wait().unwrap();
    }

    #[test]
    fn async_cleanup_progresses_only_after_its_wake() {
        let executor = CleanupExecutor::new(
            options(1, 1),
            PortableExecutorOptions {
                worker_count: 1,
                task_capacity: 1,
                polls_per_turn: 8,
            },
        )
        .unwrap();
        let saved = Arc::new(SavedWaker::default());
        let polls = Arc::new(AtomicUsize::new(0));
        executor.reserve_async().unwrap().spawn(PendingUntilWoken {
            saved: Arc::clone(&saved),
            polls: Arc::clone(&polls),
        });
        assert_eq!(executor.poll_async_turn(), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(!executor.poll_async_once());
        saved.0.lock().unwrap().take().unwrap().wake();
        assert!(executor.poll_async_once());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        executor.shutdown().wait().unwrap();
    }
}

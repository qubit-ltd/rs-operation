//! Bounded, per-scope handoff queue for resource cleanup.
#![allow(dead_code)] // The manager and scope assembly is completed by T11.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use super::cleanup_port::{CleanupDispatch, CleanupDispatchPermit, CleanupJob, CleanupQueueError};
use crate::v02::executor::CleanupExecutor;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupDispatcherError {
    ZeroCapacity,
}

struct QueueState {
    accepting: bool,
    /// Reservations include uncommitted resources and jobs still pending in
    /// this queue. Each registered resource owns at most one reservation.
    reserved: usize,
    pending: VecDeque<Box<dyn CleanupJob>>,
}

/// Per-resource-scope pending queue backed by the manager's T05 cleanup
/// executor. The queue is bounded by that scope's effective resource limit;
/// worker concurrency remains independently bounded by `CleanupExecutor`.
pub(crate) struct CleanupDispatcher {
    inner: Arc<CleanupDispatcherInner>,
}

struct CleanupDispatcherInner {
    capacity: usize,
    executor: Arc<CleanupExecutor>,
    state: Mutex<QueueState>,
    driver: Mutex<()>,
}

impl CleanupDispatcher {
    pub(crate) fn new(
        scope_resource_capacity: usize,
        executor: Arc<CleanupExecutor>,
    ) -> Result<Self, CleanupDispatcherError> {
        if scope_resource_capacity == 0 {
            return Err(CleanupDispatcherError::ZeroCapacity);
        }
        Ok(Self {
            inner: Arc::new(CleanupDispatcherInner {
                capacity: scope_resource_capacity,
                executor,
                state: Mutex::new(QueueState {
                    accepting: true,
                    reserved: 0,
                    pending: VecDeque::with_capacity(scope_resource_capacity),
                }),
                driver: Mutex::new(()),
            }),
        })
    }

    /// Schedules up to `limit` pending jobs. If the T05 executor has no slot,
    /// the first rejected job remains at the front of the queue with its
    /// resource owner intact. T11 drives this method from manager progress and
    /// shutdown polling; the method never blocks or invokes user cleanup inline.
    pub(crate) fn drive(&self, limit: usize) -> usize {
        let _driver = lock(&self.inner.driver);
        let mut scheduled = 0;
        while scheduled < limit {
            let Some(job) = lock(&self.inner.state).pending.pop_front() else {
                break;
            };
            match job.schedule(Arc::clone(&self.inner.executor)) {
                Ok(()) => {
                    let mut state = lock(&self.inner.state);
                    debug_assert!(state.reserved > 0);
                    state.reserved -= 1;
                    scheduled += 1;
                }
                Err(job) => {
                    lock(&self.inner.state).pending.push_front(job);
                    break;
                }
            }
        }
        scheduled
    }

    /// Stops new scope registrations. Previously reserved permits remain
    /// valid and can enqueue after close, and `drive` continues to drain them.
    pub(crate) fn close_admission(&self) {
        lock(&self.inner.state).accepting = false;
    }

    pub(crate) fn pending_count(&self) -> usize {
        lock(&self.inner.state).pending.len()
    }

    pub(crate) fn reserved_count(&self) -> usize {
        lock(&self.inner.state).reserved
    }

    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

impl CleanupDispatch for CleanupDispatcher {
    fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
        let mut state = lock(&self.inner.state);
        if !state.accepting {
            return Err(CleanupQueueError::Closed);
        }
        if state.reserved == self.inner.capacity {
            return Err(CleanupQueueError::Full);
        }
        state.reserved += 1;
        Ok(Box::new(DispatchPermit {
            inner: Arc::clone(&self.inner),
            active: true,
        }))
    }
}

struct DispatchPermit {
    inner: Arc<CleanupDispatcherInner>,
    active: bool,
}

impl CleanupDispatchPermit for DispatchPermit {
    fn enqueue(mut self: Box<Self>, job: Box<dyn CleanupJob>) {
        let mut state = lock(&self.inner.state);
        debug_assert!(self.active);
        debug_assert!(state.pending.len() < self.inner.capacity);
        state.pending.push_back(job);
        self.active = false;
    }
}

impl Drop for DispatchPermit {
    fn drop(&mut self) {
        if self.active {
            let mut state = lock(&self.inner.state);
            state.reserved -= 1;
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{CleanupDispatch, CleanupDispatcher, CleanupJob};
    use crate::v02::executor::CleanupExecutor;
    use crate::v02::report::{PortableExecutorOptions, ThreadPoolExecutorOptions};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    fn executor() -> Arc<CleanupExecutor> {
        Arc::new(
            CleanupExecutor::new(
                ThreadPoolExecutorOptions {
                    worker_count: 1,
                    queue_capacity: 1,
                },
                PortableExecutorOptions {
                    worker_count: 1,
                    task_capacity: 2,
                    polls_per_turn: 2,
                },
            )
            .unwrap(),
        )
    }

    struct DropOnSchedule(mpsc::Sender<String>);
    impl Drop for DropOnSchedule {
        fn drop(&mut self) {
            self.0
                .send(std::thread::current().name().unwrap_or("unnamed").into())
                .ok();
        }
    }
    impl CleanupJob for DropOnSchedule {
        fn schedule(
            self: Box<Self>,
            executor: Arc<CleanupExecutor>,
        ) -> Result<(), Box<dyn CleanupJob>> {
            match executor.reserve_drop() {
                Ok(permit) => {
                    permit.drop_value(self);
                    Ok(())
                }
                Err(_) => Err(self),
            }
        }
    }

    #[test]
    fn default_drop_runs_on_cleanup_worker() {
        let dispatcher = CleanupDispatcher::new(1, executor()).unwrap();
        let (send, receive) = mpsc::channel();
        dispatcher
            .reserve()
            .unwrap()
            .enqueue(Box::new(DropOnSchedule(send)));
        assert_eq!(dispatcher.drive(1), 1);
        let thread_name = receive.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(thread_name.starts_with("qubit-operation-"));
    }

    #[test]
    fn closed_admission_does_not_discard_reserved_job() {
        let dispatcher = CleanupDispatcher::new(1, executor()).unwrap();
        let permit = dispatcher.reserve().unwrap();
        dispatcher.close_admission();
        let (send, receive) = mpsc::channel();
        permit.enqueue(Box::new(DropOnSchedule(send)));
        assert_eq!(dispatcher.drive(1), 1);
        assert!(receive.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn executor_saturation_keeps_owner_in_pending_queue() {
        struct HoldWorker(mpsc::Receiver<()>);
        impl Drop for HoldWorker {
            fn drop(&mut self) {
                let _ = self.0.recv();
            }
        }
        let executor = executor();
        let (started_send, started_receive) = mpsc::channel();
        let (release_send, release_receive) = mpsc::channel();
        let (release_second, release_second_receive) = mpsc::channel();
        struct SignalThenHold(mpsc::Sender<()>, mpsc::Receiver<()>);
        impl Drop for SignalThenHold {
            fn drop(&mut self) {
                let _ = self.0.send(());
                let _ = self.1.recv();
            }
        }
        executor
            .reserve_drop()
            .unwrap()
            .drop_value(SignalThenHold(started_send, release_receive));
        started_receive
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        executor
            .reserve_drop()
            .unwrap()
            .drop_value(HoldWorker(release_second_receive));

        let dispatcher = CleanupDispatcher::new(1, Arc::clone(&executor)).unwrap();
        let (job_send, job_receive) = mpsc::channel();
        dispatcher
            .reserve()
            .unwrap()
            .enqueue(Box::new(DropOnSchedule(job_send)));
        assert_eq!(dispatcher.drive(1), 0);
        assert_eq!(dispatcher.pending_count(), 1);
        assert_eq!(dispatcher.reserved_count(), 1);
        assert!(job_receive.try_recv().is_err());

        release_send.send(()).unwrap();
        release_second.send(()).unwrap();
        for _ in 0..1_000 {
            if dispatcher.drive(1) == 1 {
                break;
            }
            std::thread::yield_now();
        }
        assert!(job_receive.recv_timeout(Duration::from_secs(1)).is_ok());
        assert_eq!(dispatcher.pending_count(), 0);
        assert_eq!(dispatcher.reserved_count(), 0);
    }
}

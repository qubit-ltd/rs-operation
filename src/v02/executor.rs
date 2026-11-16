// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Bounded execution engines for the staged 0.2 API.

mod cleanup_executor;
mod cleanup_permit;
mod executor_create_error;
mod executor_error;
mod internal;
mod portable_executor;
mod portable_permit;
mod shutdown_error;
mod shutdown_handle;
#[cfg(test)]
mod submit_failure;
mod thread_pool_executor;
mod thread_pool_permit;

pub(crate) use cleanup_executor::CleanupExecutor;
pub(crate) use cleanup_permit::CleanupPermit;
pub(crate) use executor_create_error::ExecutorCreateError;
pub(crate) use executor_error::ExecutorError;
pub(in crate::v02::executor) use internal::CURRENT_POOL_ID;
pub(in crate::v02::executor) use internal::NEXT_POOL_ID;
pub(in crate::v02::executor) use internal::PollSlot;
pub(in crate::v02::executor) use internal::PoolInner;
pub(in crate::v02::executor) use internal::PoolState;
pub(in crate::v02::executor) use internal::PortableInner;
pub(in crate::v02::executor) use internal::PortableState;
pub(in crate::v02::executor) use internal::PortableTask;
pub(in crate::v02::executor) use internal::close_pool;
pub(in crate::v02::executor) use internal::lock;
pub(in crate::v02::executor) use internal::notify_capacity;
pub(in crate::v02::executor) use internal::pool_worker;
pub(crate) use portable_executor::PortableExecutor;
pub(crate) use portable_permit::PortablePermit;
pub(crate) use shutdown_error::ShutdownError;
pub(crate) use shutdown_handle::ShutdownHandle;
#[cfg(test)]
pub(crate) use submit_failure::SubmitFailure;
pub(crate) use thread_pool_executor::ThreadPoolExecutor;
pub(crate) use thread_pool_permit::ThreadPoolPermit;

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;

    use super::CleanupExecutor;
    use super::ExecutorCreateError;
    use super::ExecutorError;
    use super::lock;
    use super::PortableExecutor;
    use super::ShutdownError;
    use super::ThreadPoolExecutor;
    use super::pool_worker;
    use crate::v02::report::PortableExecutorOptions;
    use crate::v02::report::ThreadPoolExecutorOptions;

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
    fn test_pending_future_requires_real_wake() {
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
        let late_waker = saved.0.lock().unwrap().take().unwrap();
        late_waker.clone().wake();
        assert!(executor.poll_once());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(lock(&executor.inner.state).tasks, 0);
        late_waker.wake();
        assert!(!executor.poll_once());
        assert_eq!(lock(&executor.inner.state).tasks, 0);
        assert!(executor.reserve().is_ok());
    }

    struct PendingWithoutWaker {
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for PendingWithoutWaker {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    impl Drop for PendingWithoutWaker {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct PendingWithSavedWaker {
        saved: Arc<SavedWaker>,
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for PendingWithSavedWaker {
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

    impl Drop for PendingWithSavedWaker {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_pending_task_without_waker_is_retained() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 1,
            task_capacity: 1,
            polls_per_turn: 8,
        })
        .unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        executor.reserve().unwrap().spawn(PendingWithoutWaker {
            polls: Arc::clone(&polls),
            drops: Arc::clone(&drops),
        });

        assert!(executor.poll_once());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let state = lock(&executor.inner.state);
        assert_eq!(state.tasks, 1);
        assert_eq!(state.active.len(), 1);
        assert!(state.ready.is_empty());
        drop(state);
        assert!(
            !executor.poll_once(),
            "pending task must not be busy-polled"
        );
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }

    fn active_task_waker(executor: &PortableExecutor) -> Waker {
        let state = lock(&executor.inner.state);
        Waker::from(Arc::clone(state.active.first().expect("active task should exist")))
    }

    #[test]
    fn test_pending_without_waker_completes_after_internal_wake() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 1,
            task_capacity: 1,
            polls_per_turn: 8,
        })
        .unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        executor.reserve().unwrap().spawn(PendingWithoutWaker {
            polls: Arc::clone(&polls),
            drops: Arc::clone(&drops),
        });

        assert!(executor.poll_once());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        active_task_waker(&executor).wake();
        assert!(executor.poll_once());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let state = lock(&executor.inner.state);
        assert_eq!(state.tasks, 0);
        assert!(state.active.is_empty());
    }

    struct ReadyFuturePanickingOnDrop;

    impl Future for ReadyFuturePanickingOnDrop {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
    }

    impl Drop for ReadyFuturePanickingOnDrop {
        fn drop(&mut self) {
            panic!("future drop probe");
        }
    }

    #[test]
    fn test_unwind_completion_releases_capacity_and_notifies() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 1,
            task_capacity: 1,
            polls_per_turn: 8,
        })
        .unwrap();
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed_notifications = Arc::clone(&notifications);
        executor.set_capacity_callback(Arc::new(move || {
            if observed_notifications.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("capacity callback probe");
            }
        }));
        executor
            .reserve()
            .unwrap()
            .spawn(ReadyFuturePanickingOnDrop);

        let panic = catch_unwind(AssertUnwindSafe(|| executor.poll_once()))
            .expect_err("Future destructor should propagate its panic");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"future drop probe"));

        assert_eq!(notifications.load(Ordering::SeqCst), 1);
        let state = lock(&executor.inner.state);
        assert_eq!(state.tasks, 0);
        assert!(state.active.is_empty());
        assert!(state.ready.is_empty());
        drop(state);
        assert!(executor.reserve().is_ok());
    }

    #[test]
    fn test_pending_task_survives_dropped_external_waker() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 1,
            task_capacity: 1,
            polls_per_turn: 8,
        })
        .unwrap();
        let saved = Arc::new(SavedWaker::default());
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        executor.reserve().unwrap().spawn(PendingWithSavedWaker {
            saved: Arc::clone(&saved),
            polls: Arc::clone(&polls),
            drops: Arc::clone(&drops),
        });

        assert!(executor.poll_once());
        drop(saved.0.lock().unwrap().take().unwrap());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let state = lock(&executor.inner.state);
        assert_eq!(state.tasks, 1);
        assert_eq!(state.active.len(), 1);
        assert!(state.ready.is_empty());
        drop(state);
        assert!(!executor.poll_once());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
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
    fn test_wake_during_poll_is_not_lost() {
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
        let state = lock(&executor.inner.state);
        assert_eq!(state.tasks, 0);
        assert!(state.active.is_empty());
        assert!(state.ready.is_empty());
        drop(state);
        assert!(!executor.poll_once());
    }

    struct WakeTwiceAndFinish {
        polls: Arc<AtomicUsize>,
    }

    impl Future for WakeTwiceAndFinish {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            cx.waker().wake_by_ref();
            cx.waker().wake_by_ref();
            Poll::Ready(())
        }
    }

    #[test]
    fn test_ready_task_releases_capacity_once_despite_wakes() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 2,
            task_capacity: 1,
            polls_per_turn: 8,
        })
        .unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        executor.reserve().unwrap().spawn(WakeTwiceAndFinish {
            polls: Arc::clone(&polls),
        });

        assert!(executor.poll_once());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let state = lock(&executor.inner.state);
        assert_eq!(state.tasks, 0);
        assert!(state.active.is_empty());
        assert!(state.ready.is_empty());
        drop(state);
        assert!(!executor.poll_once());
        assert!(executor.reserve().is_ok());
    }

    #[test]
    fn test_dropped_portable_permit_releases_capacity() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 1,
            task_capacity: 1,
            polls_per_turn: 1,
        })
        .unwrap();
        let permit = executor.reserve().unwrap();
        assert_eq!(lock(&executor.inner.state).tasks, 1);

        drop(permit);

        let state = lock(&executor.inner.state);
        assert_eq!(state.tasks, 0);
        assert!(state.active.is_empty());
        assert!(state.ready.is_empty());
        drop(state);
        assert!(executor.reserve().is_ok());
    }

    struct CompetingPolls {
        gate: Arc<Gate>,
        saved: Arc<SavedWaker>,
        polls: Arc<AtomicUsize>,
        concurrent: Arc<AtomicUsize>,
        max_concurrent: Arc<AtomicUsize>,
    }

    impl Future for CompetingPolls {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let concurrent = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_concurrent.fetch_max(concurrent, Ordering::SeqCst);
            let poll = self.polls.fetch_add(1, Ordering::SeqCst);
            let result = if poll == 0 {
                *self.saved.0.lock().unwrap() = Some(cx.waker().clone());
                self.gate.enter_and_wait();
                Poll::Pending
            } else {
                Poll::Ready(())
            };
            self.concurrent.fetch_sub(1, Ordering::SeqCst);
            result
        }
    }

    #[test]
    fn test_competing_workers_serialize_polls_and_complete_once() {
        let executor = PortableExecutor::new(PortableExecutorOptions {
            worker_count: 2,
            task_capacity: 1,
            polls_per_turn: 8,
        })
        .unwrap();
        let gate = Arc::new(Gate::new());
        let saved = Arc::new(SavedWaker::default());
        let polls = Arc::new(AtomicUsize::new(0));
        let concurrent = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        executor.reserve().unwrap().spawn(CompetingPolls {
            gate: Arc::clone(&gate),
            saved: Arc::clone(&saved),
            polls: Arc::clone(&polls),
            concurrent: Arc::clone(&concurrent),
            max_concurrent: Arc::clone(&max_concurrent),
        });

        let first_executor = executor.clone();
        let first = thread::spawn(move || first_executor.poll_once());
        gate.wait_started();
        saved.0.lock().unwrap().take().unwrap().wake();

        let second_executor = executor.clone();
        let second = thread::spawn(move || second_executor.poll_once());
        loop {
            let pollers = *lock(&executor.inner.pollers);
            let ready_empty = lock(&executor.inner.state).ready.is_empty();
            if pollers == 2 && ready_empty {
                break;
            }
            thread::yield_now();
        }
        gate.release();

        assert!(first.join().unwrap());
        assert!(second.join().unwrap());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
        let state = lock(&executor.inner.state);
        assert_eq!(state.tasks, 0);
        assert!(state.active.is_empty());
        assert!(state.ready.is_empty());
    }

    #[test]
    fn test_queue_full_preserves_job() {
        let pool = ThreadPoolExecutor::new(options(1, 1)).unwrap();
        let gate = Arc::new(Gate::new());
        let worker_gate = Arc::clone(&gate);
        pool.try_submit(move || worker_gate.enter_and_wait()).unwrap();
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
    fn test_dropped_public_handle_does_not_join() {
        let pool = ThreadPoolExecutor::new(options(1, 1)).unwrap();
        let gate = Arc::new(Gate::new());
        let worker_gate = Arc::clone(&gate);
        pool.try_submit(move || worker_gate.enter_and_wait()).unwrap();
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
    fn test_explicit_shutdown_waits_workers() {
        let pool = ThreadPoolExecutor::new(options(1, 1)).unwrap();
        let gate = Arc::new(Gate::new());
        let worker_gate = Arc::clone(&gate);
        pool.try_submit(move || worker_gate.enter_and_wait()).unwrap();
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
    fn test_blocking_worker_detects_self_wait() {
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
    fn test_portable_worker_spawn_failure_joins_started_workers() {
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
    fn test_thread_pool_worker_spawn_failure_joins_started_workers() {
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
    fn test_cleanup_destructor_runs_on_reserved_cleanup_worker() {
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
                let _ = self.0.send((current.id(), current.name().map(str::to_owned)));
            }
        }

        executor.reserve_drop().unwrap().drop_value(Probe(send));
        let (dropper, name) = receive.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_ne!(dropper, submitter);
        assert!(name.unwrap().starts_with("qubit-operation-"));
        executor.shutdown().wait().unwrap();
    }

    #[test]
    fn test_async_cleanup_progresses_only_after_its_wake() {
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

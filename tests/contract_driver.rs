// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

#[allow(dead_code, unused_imports)]
#[path = "support/mod.rs"]
mod support;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_operation::driver::BlockingExecutor;
use qubit_operation::driver::DriverStatus;
use qubit_operation::driver::ExecutorError;
use qubit_operation::driver::ExternalDriver;
use qubit_operation::driver::ExternalDriverGuard;
use qubit_operation::driver::PortableDriver;
use qubit_operation::driver::PortableDriverConfig;
use qubit_operation::driver::ThreadExecutor;
use qubit_operation::driver::is_thread_executor_worker;
use support::Gate;

fn wait_for_task_count(driver: &PortableDriver, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while driver.task_count() != expected {
        assert!(Instant::now() < deadline, "driver task count did not settle");
        thread::yield_now();
    }
}

struct SelfWake {
    polls: Arc<AtomicUsize>,
}

impl Future for SelfWake {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.polls.fetch_add(1, Ordering::SeqCst) < 3 {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

struct NopWake;
impl Wake for NopWake {
    fn wake(self: Arc<Self>) {}
}

struct ExternalDriverProbe {
    wakes: AtomicUsize,
    alive: bool,
}

impl ExternalDriver for ExternalDriverProbe {
    fn wake(&self, _task_id: u64) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn is_alive(&self) -> bool {
        self.alive
    }
}

struct WakeUntil {
    stop: Arc<std::sync::atomic::AtomicBool>,
}

struct WakeBurst {
    polls: Arc<AtomicUsize>,
    done: Option<std::sync::mpsc::Sender<()>>,
}

impl Future for WakeBurst {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            for _ in 0..128 {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        } else {
            self.done.take().unwrap().send(()).unwrap();
            Poll::Ready(())
        }
    }
}

impl Future for WakeUntil {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.stop.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[test]
fn test_portable_driver_bounds_tasks_and_executes_self_waking_future() {
    let driver = PortableDriver::new(PortableDriverConfig {
        worker_count: 2,
        task_capacity: 64,
        polls_per_turn: 1,
    })
    .unwrap();
    let polls = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    driver
        .spawn(async move {
            SelfWake { polls }.await;
            done_tx.send(()).unwrap();
        })
        .unwrap();
    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(driver.worker_count(), 2);
    wait_for_task_count(&driver, 0);
    for _ in 0..64 {
        driver.spawn(std::future::pending::<()>()).unwrap();
    }
    assert_eq!(driver.task_count(), 64);
    assert_eq!(driver.worker_count(), 2);
}

#[test]
fn test_portable_driver_rejects_capacity_without_starting_future() {
    let driver = PortableDriver::new(PortableDriverConfig {
        worker_count: 1,
        task_capacity: 1,
        polls_per_turn: 1,
    })
    .unwrap();
    let _gate = driver.spawn(std::future::pending::<()>()).unwrap();
    let rejected = driver.spawn(async {});
    assert!(rejected.is_err());
}

#[test]
fn test_portable_driver_wakes_a_pending_test_gate() {
    let driver = PortableDriver::new(PortableDriverConfig {
        worker_count: 1,
        task_capacity: 1,
        polls_per_turn: 1,
    })
    .unwrap();
    let gate = Gate::new();
    let task_gate = gate.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    driver
        .spawn(async move {
            task_gate.wait().await;
            done_tx.send(()).unwrap();
        })
        .unwrap();
    gate.wait_started();
    gate.release();
    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    wait_for_task_count(&driver, 0);
}

#[test]
fn test_repeated_wakes_coalesce_into_one_ready_entry() {
    let driver = PortableDriver::new(PortableDriverConfig {
        worker_count: 1,
        task_capacity: 1,
        polls_per_turn: 1,
    })
    .unwrap();
    let polls = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    driver
        .spawn(WakeBurst {
            polls: Arc::clone(&polls),
            done: Some(done_tx),
        })
        .unwrap();
    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    wait_for_task_count(&driver, 0);
}

#[test]
fn test_continuously_self_waking_future_does_not_starve_other_task() {
    let driver = PortableDriver::new(PortableDriverConfig {
        worker_count: 1,
        task_capacity: 2,
        polls_per_turn: 1,
    })
    .unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    driver
        .spawn(WakeUntil {
            stop: Arc::clone(&stop),
        })
        .unwrap();
    let (progress_tx, progress_rx) = std::sync::mpsc::channel();
    driver
        .spawn(async move {
            progress_tx.send(()).unwrap();
        })
        .unwrap();
    progress_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    stop.store(true, Ordering::SeqCst);
}

#[test]
fn test_thread_executor_has_fixed_workers_and_returns_rejected_closure() {
    let executor = ThreadExecutor::new(1, 1).unwrap();
    let gate = Arc::new(std::sync::Barrier::new(2));
    let held = Arc::clone(&gate);
    let (worker_tx, worker_rx) = std::sync::mpsc::channel();
    executor
        .execute(move || {
            worker_tx.send(is_thread_executor_worker()).unwrap();
            held.wait();
        })
        .unwrap_or_else(|_| panic!("job should be accepted"));
    assert!(worker_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    gate.wait();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let held_rx = release_rx;
    executor
        .execute(move || {
            held_rx.recv().unwrap();
        })
        .unwrap_or_else(|_| panic!("job should be accepted"));
    let rejected_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let marker = Arc::clone(&rejected_called);
    let rejected = executor.execute(move || marker.store(true, Ordering::SeqCst));
    let rejected = rejected.expect_err("queue should be full");
    assert_eq!(rejected.error, ExecutorError::Capacity);
    (rejected.job)();
    assert!(rejected_called.load(Ordering::SeqCst));
    release_tx.send(()).unwrap();
    assert_eq!(executor.worker_count(), 1);
}

#[test]
fn test_external_driver_loss_stays_unconfirmed() {
    let external = ExternalDriverGuard::new();
    let lease = external.lease();
    assert_eq!(lease.status(), DriverStatus::Active);
    drop(external);
    assert_eq!(lease.status(), DriverStatus::DriverLost);
    assert!(lease.is_unconfirmed());
    assert!(!lease.is_released());
}

#[test]
fn test_external_driver_contract_wakes_without_rehosting_future() {
    let external = ExternalDriverProbe {
        wakes: AtomicUsize::new(0),
        alive: true,
    };
    external.wake(17);
    assert_eq!(external.wakes.load(Ordering::SeqCst), 1);
    assert!(external.is_alive());
}

#[test]
fn test_standard_future_can_be_polled_without_runtime_assumptions() {
    let waker = Waker::from(Arc::new(NopWake));
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(async { 7usize });
    assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(7));
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

use super::ExecutionKind;
use super::ManagerCore;
use super::OperationJob;
use super::OperationManager;
use super::ScopeCore;
use super::SubmissionLease;
use super::SubmissionPermit;
use super::SubmissionPort;
use super::SubmissionReservation;
use crate::v02::adapter::ManagedOperationControl;
use crate::v02::executor::CleanupExecutor;
use crate::v02::manager::capacity::ManagerCapacityPools;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::operation::OperationCore;
use crate::v02::operation::OperationHandle;
use crate::v02::operation::OutcomeDelivery;
use crate::v02::operation::OutcomeSlot;
use crate::v02::operation::StopControl;
use crate::v02::protocol::OperationControl;
use crate::v02::report::AdmissionError;
use crate::v02::report::ContextOptions;
use crate::v02::report::ManagerId;
use crate::v02::report::ManagerLimits;
use crate::v02::report::ManagerOptions;
use crate::v02::report::OperationOptions;
use crate::v02::report::PortableExecutorOptions;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ScopeLimits;
use crate::v02::report::ShutdownMode;
use crate::v02::report::StopReason;
use crate::v02::report::SubmitError;
use crate::v02::report::ThreadPoolExecutorOptions;
use crate::v02::resource::cleanup::CleanupTransferReceiver;
use crate::v02::resource::cleanup_port::CleanupDispatch;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
use crate::v02::resource::cleanup_port::CleanupJob;
use crate::v02::resource::cleanup_port::CleanupQueueError;
use crate::v02::scope::ScopeCoreInit;

struct PayloadDropFlag(Arc<std::sync::atomic::AtomicBool>);

impl Drop for PayloadDropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct WaitPort {
    ready: std::sync::atomic::AtomicBool,
    attempts: AtomicUsize,
    waiters: Arc<AtomicUsize>,
    changed: Arc<crate::v02::internal::notify::Notify>,
    wait_started: Mutex<Option<mpsc::Sender<()>>>,
}

impl WaitPort {
    fn new() -> Self {
        Self {
            ready: std::sync::atomic::AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
            waiters: Arc::new(AtomicUsize::new(0)),
            changed: Arc::new(crate::v02::internal::notify::Notify::new()),
            wait_started: Mutex::new(None),
        }
    }

    fn make_ready(&self) {
        self.ready.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}

struct WaiterCount(Arc<AtomicUsize>);
impl WaiterCount {
    /// Records a waiter and returns an owner that decrements the count on drop.
    ///
    /// # Parameters
    ///
    /// - `waiters`: The shared waiter counter.
    ///
    /// # Returns
    ///
    /// A guard that decrements the counter when dropped.
    fn new(waiters: Arc<AtomicUsize>) -> Self {
        waiters.fetch_add(1, Ordering::SeqCst);
        Self(waiters)
    }
}
impl Drop for WaiterCount {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct InlinePermit;
struct NoopLease;
impl SubmissionPermit for InlinePermit {
    fn dispatch(self: Box<Self>, job: OperationJob) {
        match job {
            OperationJob::Sync(job) => job(),
            OperationJob::Async(mut future) => {
                let waker = std::task::Waker::noop();
                let mut context = std::task::Context::from_waker(waker);
                while future.as_mut().poll(&mut context).is_pending() {
                    std::thread::yield_now();
                }
            }
        }
    }
}
impl SubmissionLease for NoopLease {
    fn execution_finished(&self) {}
    fn settlement_finished(&self) {}
    fn outcome_delivered(&self, _delivery: OutcomeDelivery) {}
}
impl SubmissionPort for WaitPort {
    fn reserve(
        &self,
        scope_id: crate::v02::report::OperationId,
        _execution: super::ExecutionKind,
        parent: Option<&super::ScopeCore>,
        options: &OperationOptions,
    ) -> Result<SubmissionReservation, AdmissionError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if !self.ready.load(Ordering::Acquire) {
            return Err(AdmissionError::Busy);
        }
        let (capacity, resource_scope) = super::test_resources(scope_id, parent, options);
        Ok(SubmissionReservation {
            execution: Box::new(InlinePermit),
            lifecycle: Arc::new(NoopLease),
            capacity,
            resource_scope,
        })
    }

    fn wait_for_capacity<'a>(
        &'a self,
        _parent: Option<Arc<super::ScopeCore>>,
        _execution: super::ExecutionKind,
        _options: OperationOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdmissionError>> + Send + 'a>> {
        Box::pin(async move {
            let _waiter = WaiterCount::new(Arc::clone(&self.waiters));
            if let Some(started) = self.wait_started.lock().unwrap().take() {
                let _ = started.send(());
            }
            loop {
                if self.ready.load(Ordering::Acquire) {
                    return Ok(());
                }
                let changed = self.changed.notified();
                if self.ready.load(Ordering::Acquire) {
                    return Ok(());
                }
                changed.await;
            }
        })
    }
}

/// Polls a future once using a no-op waker.
///
/// # Type Parameters
///
/// - `F`: The future type being polled.
///
/// # Parameters
///
/// - `future`: The pinned future to poll.
///
/// # Returns
///
/// The result of the single poll.
fn poll_once<F: Future>(future: Pin<&mut F>) -> std::task::Poll<F::Output> {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    future.poll(&mut context)
}

/// Builds a test manager backed by the supplied wait port.
///
/// # Parameters
///
/// - `port`: Supplies controlled admission and capacity waiting.
///
/// # Returns
///
/// The manager core and its root operation manager.
fn wait_test_manager(port: Arc<WaitPort>) -> (Arc<ManagerCore>, OperationManager) {
    let core = ManagerCore::new(
        port,
        crate::v02::context::OperationContext::new(ContextOptions::default()).unwrap(),
        ScopeLimits::default(),
    );
    let root = core.root_view();
    (core, root)
}

#[test]
fn test_async_submit_is_lazy_and_cancel_releases_waiter() {
    let port = Arc::new(WaitPort::new());
    let (_, root) = wait_test_manager(Arc::clone(&port));
    let async_manager = root.as_async();
    let body_calls = Arc::new(AtomicUsize::new(0));
    let body_counter = Arc::clone(&body_calls);
    let mut submit = Box::pin(async_manager.submit(
        OperationOptions::default(),
        move |_| async move {
            body_counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(())
        },
    ));
    assert_eq!(port.attempts.load(Ordering::SeqCst), 0);
    assert_eq!(port.waiters.load(Ordering::SeqCst), 0);
    assert!(poll_once(submit.as_mut()).is_pending());
    assert_eq!(port.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(port.waiters.load(Ordering::SeqCst), 1);
    assert_eq!(body_calls.load(Ordering::SeqCst), 0);
    drop(submit);
    assert_eq!(port.waiters.load(Ordering::SeqCst), 0);
    assert_eq!(body_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn test_async_submit_retries_after_capacity_wake_without_losing_notification() {
    let port = Arc::new(WaitPort::new());
    let (_, root) = wait_test_manager(Arc::clone(&port));
    let async_manager = root.as_async();
    let body_calls = Arc::new(AtomicUsize::new(0));
    let body_counter = Arc::clone(&body_calls);
    let mut submit = Box::pin(async_manager.submit(
        OperationOptions::default(),
        move |_| async move {
            body_counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(8usize)
        },
    ));
    assert!(poll_once(submit.as_mut()).is_pending());
    assert_eq!(port.waiters.load(Ordering::SeqCst), 1);
    port.make_ready();
    let mut handle = match poll_once(submit.as_mut()) {
        std::task::Poll::Ready(Ok(handle)) => handle.into_sync(),
        _ => panic!("capacity notification should resume submission"),
    };
    assert_eq!(body_calls.load(Ordering::SeqCst), 1);
    assert_eq!(port.waiters.load(Ordering::SeqCst), 0);
    assert!(matches!(
        handle.try_take_outcome().unwrap(),
        Some(crate::v02::report::OperationOutcome::Succeeded(8))
    ));
    assert!(handle.wait_settlement().unwrap().all_released());
}

#[test]
fn test_async_submit_parent_close_and_stop_interrupt_wait_and_preserve_body() {
    for stop in [false, true] {
        let port = Arc::new(WaitPort::new());
        let (core, _) = wait_test_manager(Arc::clone(&port));
        let id = crate::v02::report::OperationId::new_unique();
        let options = crate::v02::report::OperationOptions::default();
        let (capacity, resource_scope) = super::test_resources(id, None, &options);
        let parent = super::ScopeCore::new(ScopeCoreInit {
            id,
            manager: Arc::downgrade(&core),
            parent: None,
            context: crate::v02::context::OperationContext::new(ContextOptions::default()).unwrap(),
            options,
            diagnostic_limit: 8,
            capacity,
            resource_scope,
            groups: Vec::new(),
        });
        let child_manager = OperationManager::bound(Arc::clone(&core), &parent);
        let async_child_manager = child_manager.as_async();
        let body_calls = Arc::new(AtomicUsize::new(0));
        let body_counter = Arc::clone(&body_calls);
        let captured = DropCounter(Arc::clone(&body_calls));
        let mut submit = Box::pin(async_child_manager.submit(
            OperationOptions::default(),
            move |_| async move {
                let _captured = captured;
                body_counter.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(())
            },
        ));
        assert!(poll_once(submit.as_mut()).is_pending());
        assert_eq!(port.waiters.load(Ordering::SeqCst), 1);
        port.make_ready();
        if stop {
            parent.request_stop(crate::v02::report::StopReason::UserCancelled);
        } else {
            parent.end();
        }
        let error = match poll_once(submit.as_mut()) {
            std::task::Poll::Ready(Err(error)) => error,
            _ => panic!("parent close/stop must interrupt capacity wait"),
        };
        assert_eq!(
            error.reason(),
            if stop {
                AdmissionError::Stopped
            } else {
                AdmissionError::ScopeEnded
            }
        );
        let (_, body) = error.into_parts();
        assert_eq!(port.waiters.load(Ordering::SeqCst), 0);
        assert_eq!(body_calls.load(Ordering::SeqCst), 0);
        drop(body);
        assert_eq!(body_calls.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn test_sync_submit_waits_without_busy_retry_and_resumes_on_capacity() {
    let port = Arc::new(WaitPort::new());
    let (started, waiting) = mpsc::channel();
    *port.wait_started.lock().unwrap() = Some(started);
    let (_, root) = wait_test_manager(Arc::clone(&port));
    let body_calls = Arc::new(AtomicUsize::new(0));
    let body_counter = Arc::clone(&body_calls);
    let submitter = std::thread::spawn(move || {
        root.submit(OperationOptions::default(), move |_| {
            body_counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, ()>(5usize)
        })
    });
    waiting.recv().unwrap();
    assert_eq!(port.waiters.load(Ordering::SeqCst), 1);
    assert_eq!(port.attempts.load(Ordering::SeqCst), 1);
    port.make_ready();
    let mut handle = submitter.join().unwrap().unwrap();
    assert_eq!(body_calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        handle.try_take_outcome().unwrap(),
        Some(crate::v02::report::OperationOutcome::Succeeded(5))
    ));
}
#[test]
fn test_typed_sync_submission_publishes_outcome_and_settlement() {
    let (_, root) = ManagerCore::for_test();
    let mut handle = root
        .try_submit(crate::v02::report::OperationOptions::default(), |_| {
            Ok::<_, ()>(42usize)
        })
        .unwrap();
    assert!(matches!(
        handle.try_take_outcome().unwrap(),
        Some(crate::v02::report::OperationOutcome::Succeeded(42))
    ));
    assert!(handle.wait_settlement().unwrap().all_released());
}

struct ClosedCleanupDispatch;
impl CleanupDispatch for ClosedCleanupDispatch {
    fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
        Err(CleanupQueueError::Closed)
    }
}

type OneSlotCleanupState = Arc<Mutex<(bool, Option<Box<dyn CleanupJob>>)>>;

struct OneSlotCleanup {
    state: OneSlotCleanupState,
}

impl OneSlotCleanup {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(Mutex::new((false, None))),
        })
    }

    /// Dispatches the stored cleanup job to the executor.
    ///
    /// # Parameters
    ///
    /// - `executor`: The executor that should run the cleanup job.
    fn drive(&self, executor: Arc<CleanupExecutor>) {
        let job = self.state.lock().unwrap().1.take().unwrap();
        match job.schedule(executor) {
            Ok(()) => self.state.lock().unwrap().0 = false,
            Err(job) => self.state.lock().unwrap().1 = Some(job),
        }
    }
}

struct OneSlotPermit {
    state: OneSlotCleanupState,
}

impl CleanupDispatchPermit for OneSlotPermit {
    fn enqueue(self: Box<Self>, job: Box<dyn CleanupJob>) {
        let mut state = self.state.lock().unwrap();
        assert!(state.0 && state.1.is_none());
        state.1 = Some(job);
    }
}

impl Drop for OneSlotPermit {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        if state.1.is_none() {
            state.0 = false;
        }
    }
}

impl CleanupDispatch for OneSlotCleanup {
    fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
        let mut state = self.state.lock().unwrap();
        if state.0 {
            return Err(CleanupQueueError::Full);
        }
        state.0 = true;
        Ok(Box::new(OneSlotPermit {
            state: Arc::clone(&self.state),
        }))
    }
}

#[test]
fn test_recovery_required_overrides_real_transfer_in_operation_settlement() {
    struct PanicsOnDrop;
    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("cleanup failed intentionally");
        }
    }

    let (_, root) = ManagerCore::for_test();
    let handle = root
        .try_submit(OperationOptions::default(), |scope| {
            let manager_id = ManagerId::new_unique();
            let source_scope_id = ResourceScopeId::new_unique();
            let target_scope_id = ResourceScopeId::new_unique();
            let pools = ManagerCapacityPools::new(ManagerLimits::default());
            let cleanup = OneSlotCleanup::new();
            let resource_scope = crate::v02::resource::ResourceScope::new(
                manager_id,
                source_scope_id,
                Arc::new(ScopeCapacity::root(&pools, 91, ScopeLimits::default())),
                cleanup.clone(),
            );
            let reporting_scope = resource_scope.internal_clone();
            let callback_scope = reporting_scope.internal_clone();
            assert!(scope.core.attach_resource_scope(resource_scope).is_ok());

            let executor = Arc::new(
                CleanupExecutor::new(
                    ThreadPoolExecutorOptions {
                        worker_count: 1,
                        queue_capacity: 1,
                    },
                    PortableExecutorOptions::default(),
                )
                .unwrap(),
            );
            let receiver = CleanupTransferReceiver::new_with_receipt_callback(
                manager_id,
                target_scope_id,
                1,
                Arc::clone(&executor),
                move |receipt| {
                    assert!(callback_scope.record_transfer_receipt(receipt));
                },
            )
            .unwrap();
            let transferred_id = ResourceId::new(manager_id, 1, 1);
            let receipt =
                match receiver.accept(transferred_id, source_scope_id, Box::new(TransferOwner)) {
                    Ok(receipt) => receipt,
                    Err(_) => panic!("receiver should accept committed transfer"),
                };
            assert_eq!(receipt.resource(), transferred_id);

            let failing_resource = reporting_scope.manage(PanicsOnDrop).unwrap();
            drop(failing_resource);
            reporting_scope.end();
            cleanup.drive(Arc::clone(&executor));
            executor.shutdown().wait().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();

    let settlement = handle.wait_settlement().unwrap();
    assert_eq!(
        settlement.status,
        crate::v02::report::SettlementStatus::RecoveryRequired
    );
    assert_eq!(settlement.cleanup_failures_total, 1);
    assert_eq!(settlement.transfers_total, 1);
    assert_eq!(settlement.transferred_count, 1);
}

struct TransferOwner;
impl CleanupJob for TransferOwner {
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
fn test_accepted_cleanup_transfer_is_in_operation_settlement() {
    let (_, root) = ManagerCore::for_test();
    let mut handle = root
        .try_submit(OperationOptions::default(), |scope| {
            let manager_id = ManagerId::new_unique();
            let source_scope_id = ResourceScopeId::new_unique();
            let target_scope_id = ResourceScopeId::new_unique();
            let pools = ManagerCapacityPools::new(ManagerLimits::default());
            let resource_scope = crate::v02::resource::ResourceScope::new(
                manager_id,
                source_scope_id,
                Arc::new(ScopeCapacity::root(&pools, 90, ScopeLimits::default())),
                Arc::new(ClosedCleanupDispatch),
            );
            let reporting_scope = resource_scope.internal_clone();
            assert!(scope.core.attach_resource_scope(resource_scope).is_ok());

            let executor = Arc::new(
                CleanupExecutor::new(
                    ThreadPoolExecutorOptions {
                        worker_count: 1,
                        queue_capacity: 1,
                    },
                    PortableExecutorOptions::default(),
                )
                .unwrap(),
            );
            let receiver = CleanupTransferReceiver::new_with_receipt_callback(
                manager_id,
                target_scope_id,
                1,
                Arc::clone(&executor),
                move |receipt| {
                    assert!(reporting_scope.record_transfer_receipt(receipt));
                },
            )
            .unwrap();
            let resource_id = ResourceId::new(manager_id, 1, 1);
            let receipt =
                match receiver.accept(resource_id, source_scope_id, Box::new(TransferOwner)) {
                    Ok(receipt) => receipt,
                    Err(_) => panic!("receiver should accept committed transfer"),
                };
            assert_eq!(receipt.resource(), resource_id);
            assert_eq!(receiver.drive(1), 1);
            executor.shutdown().wait().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        handle.try_take_outcome().unwrap(),
        Some(crate::v02::report::OperationOutcome::Succeeded(()))
    ));
    let settlement = handle.wait_settlement().unwrap();
    assert_eq!(
        settlement.status,
        crate::v02::report::SettlementStatus::Transferred
    );
    assert_eq!(settlement.transferred_count, 1);
    assert_eq!(settlement.transfers_total, 1);
    assert_eq!(settlement.transfers.len(), 1);
}

#[test]
fn test_typed_async_submission_preserves_target_and_publishes_lifecycle() {
    let (_, root) = ManagerCore::for_test();
    let mut handle = root
        .as_async()
        .try_submit(OperationOptions::default(), |_| async {
            Ok::<_, ()>(24usize)
        })
        .unwrap()
        .into_sync();
    assert!(matches!(
        handle.try_take_outcome().unwrap(),
        Some(crate::v02::report::OperationOutcome::Succeeded(24))
    ));
    assert!(handle.wait_settlement().unwrap().all_released());
}

struct SingleWorkerPort(mpsc::Sender<OperationJob>, Arc<LeaseCounts>);
struct SingleWorkerPermit(mpsc::Sender<OperationJob>);
#[derive(Default)]
struct LeaseCounts {
    execution: AtomicUsize,
    settlement: AtomicUsize,
    delivered: AtomicUsize,
    discarded: AtomicUsize,
}
struct CountingLease(Arc<LeaseCounts>);

impl SubmissionPort for SingleWorkerPort {
    fn reserve(
        &self,
        scope_id: crate::v02::report::OperationId,
        _execution: super::ExecutionKind,
        parent: Option<&super::ScopeCore>,
        options: &OperationOptions,
    ) -> Result<SubmissionReservation, AdmissionError> {
        let (capacity, resource_scope) = super::test_resources(scope_id, parent, options);
        Ok(SubmissionReservation {
            execution: Box::new(SingleWorkerPermit(self.0.clone())),
            lifecycle: Arc::new(CountingLease(Arc::clone(&self.1))),
            capacity,
            resource_scope,
        })
    }

    fn wait_for_capacity<'a>(
        &'a self,
        _parent: Option<Arc<super::ScopeCore>>,
        _execution: super::ExecutionKind,
        _options: OperationOptions,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<(), AdmissionError>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

impl SubmissionLease for CountingLease {
    fn execution_finished(&self) {
        self.0.execution.fetch_add(1, Ordering::SeqCst);
    }
    fn settlement_finished(&self) {
        self.0.settlement.fetch_add(1, Ordering::SeqCst);
    }
    fn outcome_delivered(&self, delivery: OutcomeDelivery) {
        match delivery {
            OutcomeDelivery::Delivered => &self.0.delivered,
            OutcomeDelivery::Discarded => &self.0.discarded,
        }
        .fetch_add(1, Ordering::SeqCst);
    }
}

impl SubmissionPermit for SingleWorkerPermit {
    fn dispatch(self: Box<Self>, job: OperationJob) {
        self.0.send(job).expect("single worker remains alive");
    }
}

#[test]
fn test_typed_sync_parent_settles_after_child_on_one_worker() {
    let (sender, receiver) = mpsc::channel::<OperationJob>();
    let worker = std::thread::spawn(move || {
        while let Ok(job) = receiver.recv() {
            match job {
                OperationJob::Sync(job) => job(),
                OperationJob::Async(mut future) => {
                    let waker = std::task::Waker::noop();
                    let mut context = std::task::Context::from_waker(waker);
                    while future.as_mut().poll(&mut context).is_pending() {
                        std::thread::yield_now();
                    }
                }
            }
        }
    });
    let counts = Arc::new(LeaseCounts::default());
    let port = Arc::new(SingleWorkerPort(sender, Arc::clone(&counts)));
    let core = ManagerCore::new(
        port,
        crate::v02::context::OperationContext::new(ContextOptions::default()).unwrap(),
        ScopeLimits::default(),
    );
    let root = core.root_view();
    let mut parent = root
        .try_submit(OperationOptions::default(), |scope| {
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
                .unwrap();
            drop(child.into_observer());
            Ok::<_, ()>(17usize)
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        crate::v02::report::OperationOutcome::Succeeded(17)
    ));
    let report = parent.wait_settlement().unwrap();
    assert_eq!(report.children_total, 1);
    assert_eq!(counts.execution.load(Ordering::SeqCst), 2);
    assert_eq!(counts.settlement.load(Ordering::SeqCst), 2);
    assert_eq!(counts.delivered.load(Ordering::SeqCst), 1);
    assert_eq!(counts.discarded.load(Ordering::SeqCst), 1);
    drop(parent);
    drop(root);
    drop(core);
    worker.join().unwrap();
}

#[test]
fn test_typed_async_parent_settles_after_child_on_one_worker() {
    let (sender, receiver) = mpsc::channel::<OperationJob>();
    let worker = std::thread::spawn(move || {
        while let Ok(job) = receiver.recv() {
            match job {
                OperationJob::Sync(job) => job(),
                OperationJob::Async(mut future) => {
                    let waker = std::task::Waker::noop();
                    let mut context = std::task::Context::from_waker(waker);
                    while future.as_mut().poll(&mut context).is_pending() {
                        std::thread::yield_now();
                    }
                }
            }
        }
    });
    let port = Arc::new(SingleWorkerPort(sender, Arc::new(LeaseCounts::default())));
    let core = ManagerCore::new(
        port,
        crate::v02::context::OperationContext::new(ContextOptions::default()).unwrap(),
        ScopeLimits::default(),
    );
    let root = core.root_view();
    let mut parent = root
        .as_async()
        .try_submit(OperationOptions::default(), |scope| async move {
            let child = scope
                .manager()
                .as_async()
                .try_submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) })
                .unwrap();
            drop(child.into_observer());
            Ok::<_, ()>(31usize)
        })
        .unwrap()
        .into_sync();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        crate::v02::report::OperationOutcome::Succeeded(31)
    ));
    assert_eq!(parent.wait_settlement().unwrap().children_total, 1);
    drop(parent);
    drop(root);
    drop(core);
    worker.join().unwrap();
}

#[test]
fn test_required_child_failure_changes_parent_outcome_but_not_settlement_fact() {
    let (sender, receiver) = mpsc::channel::<OperationJob>();
    let worker = std::thread::spawn(move || {
        while let Ok(job) = receiver.recv() {
            match job {
                OperationJob::Sync(job) => job(),
                OperationJob::Async(mut future) => {
                    let waker = std::task::Waker::noop();
                    let mut context = std::task::Context::from_waker(waker);
                    while future.as_mut().poll(&mut context).is_pending() {
                        std::thread::yield_now();
                    }
                }
            }
        }
    });
    let port = Arc::new(SingleWorkerPort(sender, Arc::new(LeaseCounts::default())));
    let core = ManagerCore::new(
        port,
        crate::v02::context::OperationContext::new(ContextOptions::default()).unwrap(),
        ScopeLimits::default(),
    );
    let root = core.root_view();
    let mut parent = root
        .try_submit(OperationOptions::default(), |scope| {
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), |_| Err::<(), _>("failed"))
                .unwrap();
            drop(child.into_observer());
            Ok::<_, ()>(99usize)
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        crate::v02::report::OperationOutcome::ChildFailed(_)
    ));
    let settlement = parent.wait_settlement().unwrap();
    assert_eq!(
        settlement.status,
        crate::v02::report::SettlementStatus::Released
    );
    assert_eq!(settlement.children_total, 1);
    drop(parent);
    drop(root);
    drop(core);
    worker.join().unwrap();
}

struct DropCounter(Arc<AtomicUsize>);
impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_rejected_typed_submission_retains_original_body() {
    let (core, root) = ManagerCore::for_test();
    super::lock(&core.state).phase = super::ManagerPhase::Closed;
    let drops = Arc::new(AtomicUsize::new(0));
    let marker = DropCounter(Arc::clone(&drops));
    let error = match root.try_submit(OperationOptions::default(), move |_| {
        let _marker = marker;
        Ok::<_, ()>(())
    }) {
        Ok(_) => panic!("closed manager accepted submission"),
        Err(error) => error,
    };
    assert_eq!(error.reason(), AdmissionError::Closed);
    let (_, body) = error.into_parts();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(body);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// Submits nested test work through the same manager helper.
///
/// # Parameters
///
/// - `manager`: The manager used to submit the root operation.
/// - `value`: The value recorded by the grandchild operation.
///
/// # Errors
///
/// Returns an admission error if any operation in the hierarchy is rejected.
fn submit_work(manager: &OperationManager, value: usize) -> Result<(), AdmissionError> {
    manager
        .try_submit_body(move |scope| {
            scope
                .manager()
                .try_submit_body(move |grandchild| {
                    grandchild.record_test_value(value);
                })
                .unwrap();
        })
        .map(|_| ())
        .map_err(|error: SubmitError<_>| error.into_parts().0)
}

#[test]
fn test_same_helper_works_at_every_depth() {
    let (core, root) = ManagerCore::for_test();
    submit_work(&root, 7).unwrap();
    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 2);
    assert_eq!(relationships[0].1, None);
    assert_eq!(relationships[1].1, Some(relationships[0].0));
    assert_eq!(core.test_values(), vec![7]);
}

#[test]
fn test_manager_conversion_preserves_parent() {
    let (core, root) = ManagerCore::for_test();
    root.try_submit_body(|scope| {
        let parent = scope.id();
        scope
            .manager()
            .as_async()
            .as_sync()
            .try_submit_body(|_| {})
            .unwrap();
        assert_eq!(parent, scope.id());
    })
    .unwrap();
    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 2);
    assert_eq!(relationships[1].1, Some(relationships[0].0));
}

#[test]
fn test_root_manager_inside_operation_stays_root() {
    let (core, root) = ManagerCore::for_test();
    let root_clone = root.clone();
    root.try_submit_body(move |_| {
        root_clone.try_submit_body(|_| {}).unwrap();
    })
    .unwrap();
    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 2);
    assert_eq!(relationships[1].1, None);
}

#[test]
fn test_expired_parent_does_not_fall_back_to_root() {
    let saved = Arc::new(Mutex::new(None));
    let saved_in_body = Arc::clone(&saved);
    let (_, root) = ManagerCore::for_test();
    root.try_submit_body(move |scope| {
        *saved_in_body.lock().unwrap() = Some(scope.manager());
    })
    .unwrap();
    let manager = saved.lock().unwrap().take().unwrap();
    assert_eq!(
        manager.try_submit_body(|_| {}).unwrap_err().reason(),
        AdmissionError::ScopeEnded
    );
}

#[test]
fn test_frame_end_closes_scope_even_when_scope_moves_out() {
    let escaped = Arc::new(Mutex::new(None));
    let escaped_in_body = Arc::clone(&escaped);
    let (_, root) = ManagerCore::for_test();
    root.try_submit_body(move |scope| {
        *escaped_in_body.lock().unwrap() = Some(scope);
    })
    .unwrap();
    let scope = escaped.lock().unwrap().take().unwrap();
    assert!(!scope.is_open());
    assert_eq!(
        scope
            .manager()
            .try_submit_body(|_| {})
            .unwrap_err()
            .reason(),
        AdmissionError::ScopeEnded
    );
}

#[test]
fn test_async_manager_conversion_keeps_parent_and_runs_grandchildren() {
    let (core, root) = ManagerCore::for_test();
    root.try_submit_body(|scope| {
        let async_manager = scope.manager().as_async();
        async_manager
            .try_submit_body(|child_scope| async move {
                child_scope.manager().try_submit_body(|_| {}).unwrap();
            })
            .unwrap();
    })
    .unwrap();

    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 3);
    assert_eq!(relationships[0].1, None);
    assert_eq!(relationships[1].1, Some(relationships[0].0));
    assert_eq!(relationships[2].1, Some(relationships[1].0));
}

/// Reserves a root operation from a production manager for lifecycle tests.
///
/// # Parameters
///
/// - `manager`: The production manager used for reservation.
///
/// # Returns
///
/// The core, operation ID, scope, and complete reservation.
fn reserve_production_root(
    manager: &OperationManager,
) -> (
    Arc<ManagerCore>,
    crate::v02::report::OperationId,
    Arc<ScopeCore>,
    SubmissionReservation,
) {
    let core = manager.core_arc();
    let reserved = core.reserve_scope(
        &manager.target,
        OperationOptions::default(),
        ExecutionKind::Sync,
        || {},
    );
    let (_, id, scope, reservation) = match reserved {
        Ok(reserved) => reserved,
        Err(_) => panic!("default production manager should reserve a root"),
    };
    (core, id, scope, reservation)
}

#[test]
fn test_preflight_rejection_returns_body_before_scope_registration() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let core = manager.core_arc();
    let rejected = core.reserve_scope_with_preflight(
        &manager.target,
        OperationOptions::default(),
        ExecutionKind::Sync,
        17usize,
        |_| Err(AdmissionError::AlreadyBound),
        false,
    );
    let error = match rejected {
        Ok(_) => panic!("preflight rejection must roll back reservation"),
        Err(error) => error,
    };
    let (reason, body) = error.into_parts();
    assert_eq!(reason, AdmissionError::AlreadyBound);
    assert_eq!(body, 17);
    assert_eq!(core.active_scope_count(), 0);
}

#[test]
fn test_production_cancel_abandons_adapter_without_running_unstarted_body() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (core, id, scope, reservation) = reserve_production_root(&manager);
    let operation = OperationCore::new(id);
    let slot = Arc::new(OutcomeSlot::<(), ()>::new(Arc::clone(&operation)));
    let lease = Arc::clone(&reservation.lifecycle);
    slot.set_delivery_hook({
        let lease = Arc::clone(&lease);
        move |delivery| lease.outcome_delivered(delivery)
    });
    let control = ManagedOperationControl::new(Arc::clone(&operation), Arc::clone(&scope));
    core.register_settlement(
        Arc::clone(&scope),
        Arc::clone(&operation),
        Arc::clone(&lease),
        Arc::clone(&control),
    );
    let mut handle = OperationHandle::new(
        Arc::clone(&operation),
        Arc::clone(&slot),
        Arc::clone(&control) as Arc<dyn StopControl>,
    );

    assert_eq!(
        core.request_shutdown(ShutdownMode::Cancel),
        super::ManagerPhase::Cancelling
    );
    assert_eq!(
        scope.context().stop_reason(),
        Some(StopReason::ManagerShutdown)
    );
    let ran_body = Arc::new(AtomicUsize::new(0));
    let payload_dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let abandon_count = Arc::new(AtomicUsize::new(0));
    let ran_body_on_job = Arc::clone(&ran_body);
    let payload_dropped_on_job = Arc::clone(&payload_dropped);
    let drop_flag = PayloadDropFlag(Arc::clone(&payload_dropped_on_job));
    let abandon_count_on_callback = Arc::clone(&abandon_count);
    let abandon_scope = Arc::clone(&scope);
    let abandon_operation = Arc::clone(&operation);
    let abandon_slot = Arc::clone(&slot);
    let abandon_lease = Arc::clone(&lease);
    let abandon_control = Arc::clone(&control);
    let execution = reservation.execution;
    let result = core.dispatch_adapter_job(
        Arc::clone(&scope),
        Arc::clone(&control),
        execution,
        OperationJob::Sync(Box::new(move || {
            let _drop_flag = drop_flag;
            ran_body_on_job.fetch_add(1, Ordering::SeqCst);
        })),
        move || {
            assert!(payload_dropped.load(Ordering::SeqCst));
            abandon_count_on_callback.fetch_add(1, Ordering::SeqCst);
            ManagerCore::abandon_reserved_operation(
                abandon_scope,
                abandon_operation,
                abandon_slot,
                abandon_lease,
                None,
                abandon_control,
            );
        },
    );

    assert_eq!(result, Err(crate::v02::adapter::AdapterErrorKind::Stopped));
    assert_eq!(ran_body.load(Ordering::SeqCst), 0);
    assert_eq!(abandon_count.load(Ordering::SeqCst), 1);
    assert!(matches!(
        handle.try_take_outcome().unwrap(),
        Some(crate::v02::report::OperationOutcome::Stopped(_))
    ));
    assert_eq!(
        control.snapshot().execution,
        crate::v02::report::ExecutionPhase::Finished
    );
    assert!(control.snapshot().input_released);
    let settlement = handle.wait_settlement().unwrap();
    assert!(settlement.all_released());
    assert_eq!(core.active_scope_count(), 0);
    core.finish_shutdown();
}

#[test]
fn test_production_parent_stop_and_adapter_activation_have_one_gate_winner() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (core, _id, scope, reservation) = reserve_production_root(&manager);
    scope.commit_start().unwrap();

    let start = Arc::new(Barrier::new(3));
    let activate_scope = Arc::clone(&scope);
    let activate_start = Arc::clone(&start);
    let activate = std::thread::spawn(move || {
        activate_start.wait();
        activate_scope.activate_start()
    });
    let stop_scope = Arc::clone(&scope);
    let stop_start = Arc::clone(&start);
    let stop = std::thread::spawn(move || {
        stop_start.wait();
        stop_scope.request_stop(StopReason::UserCancelled)
    });
    start.wait();
    let activation_result = activate.join().unwrap();
    assert!(stop.join().unwrap());
    assert!(matches!(
        activation_result,
        Ok(()) | Err(crate::v02::adapter::AdapterErrorKind::Stopped)
    ));
    assert!(scope.context().stop_reason().is_some());

    scope.end();
    drop(reservation);
    core.finish_shutdown();
}

#[test]
fn test_production_scope_stop_and_adapter_commit_have_one_gate_winner() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (core, id, scope, reservation) = reserve_production_root(&manager);
    let operation = OperationCore::new(id);
    let control = ManagedOperationControl::new(Arc::clone(&operation), Arc::clone(&scope));
    let lease = Arc::clone(&reservation.lifecycle);
    core.register_settlement(
        Arc::clone(&scope),
        Arc::clone(&operation),
        Arc::clone(&lease),
        Arc::clone(&control),
    );

    let start = Arc::new(Barrier::new(3));
    let commit_scope = Arc::clone(&scope);
    let commit_start = Arc::clone(&start);
    let commit = std::thread::spawn(move || {
        commit_start.wait();
        commit_scope.commit_start()
    });
    let stop_scope = Arc::clone(&scope);
    let stop_start = Arc::clone(&start);
    let stop = std::thread::spawn(move || {
        stop_start.wait();
        stop_scope.request_stop(StopReason::UserCancelled)
    });
    start.wait();
    let commit_result = commit.join().unwrap();
    assert!(stop.join().unwrap());
    assert!(matches!(
        commit_result,
        Ok(()) | Err(crate::v02::adapter::AdapterErrorKind::Stopped)
    ));
    assert_eq!(
        scope.activate_start(),
        Err(crate::v02::adapter::AdapterErrorKind::Stopped)
    );

    lease.execution_finished();
    scope.end();
    drop(reservation);
    core.finish_shutdown();
}

#[test]
fn test_async_manager_run_and_run_and_settle_return_outcomes() {
    let (_, root) = ManagerCore::for_test();
    let async_manager = root.as_async();

    let mut run = Box::pin(async_manager.run(OperationOptions::default(), |_| async {
        Ok::<_, ()>(17usize)
    }));
    assert!(matches!(
        poll_once(run.as_mut()),
        std::task::Poll::Ready(Ok(crate::v02::report::OperationOutcome::Succeeded(17)))
    ));

    let mut run_and_settle = Box::pin(
        async_manager.run_and_settle(OperationOptions::default(), |_| async {
            Ok::<_, ()>(23usize)
        }),
    );
    match poll_once(run_and_settle.as_mut()) {
        std::task::Poll::Ready(Ok(completion)) => {
            assert!(matches!(
                completion.outcome,
                crate::v02::report::OperationOutcome::Succeeded(23)
            ));
            assert!(completion.settlement.all_released());
        }
        _ => panic!("inline async operation should complete with its settlement"),
    }
}

#[test]
fn test_async_shutdown_wait_completes_after_admission_is_closed() {
    let (_, root) = ManagerCore::for_test();
    let shutdown = root.as_async().request_shutdown(ShutdownMode::Drain);
    let mut wait = Box::pin(shutdown.wait());
    match poll_once(wait.as_mut()) {
        std::task::Poll::Ready(Ok(report)) => assert_eq!(report.snapshot.active_operations, 0),
        std::task::Poll::Ready(Err(error)) => panic!("idle shutdown observation failed: {error}"),
        std::task::Poll::Pending => panic!("an idle manager shutdown should complete immediately"),
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::v02::context::CleanupContext;
use crate::v02::executor::CleanupExecutor;
use crate::v02::manager::capacity::ManagerCapacityPools;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::report::ManagerId;
use crate::v02::report::ManagerLimits;
use crate::v02::report::PortableExecutorOptions;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceManageErrorKind;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ScopeLimits;
use crate::v02::report::ThreadPoolExecutorOptions;
use crate::v02::resource::AsyncCleanup;
use crate::v02::resource::CleanupFuture;
use crate::v02::resource::ResourceScope;
use crate::v02::resource::cleanup::CleanupTransferReceiver;
use crate::v02::resource::cleanup_port::CleanupDispatch;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
use crate::v02::resource::cleanup_port::CleanupJob;
use crate::v02::resource::cleanup_port::CleanupQueueError;

struct QueueSlot {
    reserved: bool,
    job: Option<Box<dyn CleanupJob>>,
}
struct ManualCleanup {
    slots: Arc<Mutex<Vec<QueueSlot>>>,
}
impl ManualCleanup {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Mutex::new(
                (0..limit)
                    .map(|_| QueueSlot {
                        reserved: false,
                        job: None,
                    })
                    .collect(),
            )),
        })
    }
    fn run_all(&self, executor: &Arc<CleanupExecutor>) {
        let slot_count = self.slots.lock().unwrap().len();
        for index in 0..slot_count {
            let job = { self.slots.lock().unwrap()[index].job.take() };
            let Some(job) = job else { continue };
            match job.schedule(Arc::clone(executor)) {
                Ok(()) => self.slots.lock().unwrap()[index].reserved = false,
                Err(job) => {
                    self.slots.lock().unwrap()[index].job = Some(job);
                    break;
                }
            }
        }
    }
}
impl CleanupDispatch for ManualCleanup {
    fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
        let mut slots = self.slots.lock().unwrap();
        let (index, slot) = slots
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| !slot.reserved)
            .ok_or(CleanupQueueError::Full)?;
        slot.reserved = true;
        Ok(Box::new(ManualPermit {
            slots: Arc::clone(&self.slots),
            index,
        }))
    }
}
struct ManualPermit {
    slots: Arc<Mutex<Vec<QueueSlot>>>,
    index: usize,
}
impl CleanupDispatchPermit for ManualPermit {
    fn enqueue(self: Box<Self>, job: Box<dyn CleanupJob>) {
        let mut slots = self.slots.lock().unwrap();
        let slot = &mut slots[self.index];
        assert!(slot.reserved && slot.job.is_none());
        slot.job = Some(job);
    }
}
impl Drop for ManualPermit {
    fn drop(&mut self) {
        let mut slots = self.slots.lock().unwrap();
        let slot = &mut slots[self.index];
        if slot.job.is_none() {
            slot.reserved = false;
        }
    }
}

fn scope() -> (ResourceScope, Arc<ManualCleanup>) {
    let (scope, cleanup, _) = scope_with_manager();
    (scope, cleanup)
}

fn scope_with_manager() -> (ResourceScope, Arc<ManualCleanup>, ManagerId) {
    let manager = ManagerCapacityPools::new(ManagerLimits::default());
    let cleanup = ManualCleanup::new(16);
    let manager_id = ManagerId::new_unique();
    let scope = ResourceScope::new(
        manager_id,
        ResourceScopeId::new_unique(),
        Arc::new(ScopeCapacity::root(&manager, 42, ScopeLimits::default())),
        cleanup.clone(),
    );
    (scope, cleanup, manager_id)
}
fn executor() -> Arc<CleanupExecutor> {
    Arc::new(
        CleanupExecutor::new(
            ThreadPoolExecutorOptions {
                worker_count: 1,
                queue_capacity: 16,
            },
            PortableExecutorOptions::default(),
        )
        .unwrap(),
    )
}

#[test]
fn test_resource_drop_waits_for_manual_cleanup_dispatch() {
    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (scope, cleanup) = scope();
    let executor = executor();
    let drops = Arc::new(AtomicUsize::new(0));
    let resource = scope.manage(DropCount(Arc::clone(&drops))).unwrap();
    drop(resource);
    scope.end();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    cleanup.run_all(&executor);
    executor.shutdown().wait().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn test_live_resource_reference_reports_waiting_then_cleanup_progress() {
    let (scope, cleanup) = scope();
    let progress = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&progress);
    scope.set_progress_callback(Arc::new(move |value| {
        observed.lock().unwrap().push(value);
    }));
    let resource = scope.manage(String::from("live")).unwrap();
    scope.end();
    assert_eq!(
        *progress.lock().unwrap(),
        vec![
            crate::v02::report::ResourceProgress::ScopeOpen,
            crate::v02::report::ResourceProgress::WaitingForUsers,
        ]
    );
    drop(resource);
    assert_eq!(
        *progress.lock().unwrap(),
        vec![
            crate::v02::report::ResourceProgress::ScopeOpen,
            crate::v02::report::ResourceProgress::WaitingForUsers,
            crate::v02::report::ResourceProgress::Cleaning,
        ]
    );
    let executor = executor();
    cleanup.run_all(&executor);
    executor.shutdown().wait().unwrap();
}

#[test]
fn test_ended_reservation_commit_returns_resource_and_bound_reservation() {
    let (scope, _) = scope();
    let reservation = scope.reserve().unwrap();
    scope.end();
    let error = match reservation.commit(String::from("owner")) {
        Ok(_) => panic!("ended scope accepted commit"),
        Err(error) => error,
    };
    let (reason, resource, reservation) = error.into_parts();
    assert_eq!(reason, ResourceManageErrorKind::ScopeEnded);
    assert_eq!(resource, "owner");
    drop(reservation);
}

#[test]
fn test_retained_cleanup_uses_reservation_after_scope_end() {
    struct CleanupMarker(Arc<AtomicBool>);
    impl AsyncCleanup<String> for CleanupMarker {
        fn cleanup(self: Arc<Self>, resource: String, _context: CleanupContext) -> CleanupFuture<String> {
            Box::pin(async move {
                self.0.store(true, Ordering::SeqCst);
                drop(resource);
                crate::v02::resource::CleanupOutcome::Released
            })
        }
    }

    let (scope, cleanup, manager_id) = scope_with_manager();
    let executor = executor();
    let progress = Arc::new(Mutex::new(Vec::new()));
    let observed_progress = Arc::clone(&progress);
    scope.set_progress_callback(Arc::new(move |value| {
        observed_progress.lock().unwrap().push(value);
    }));
    let reports = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&reports);
    scope.set_settlement_callback(Box::new(move |report| {
        observed.lock().unwrap().push(report);
    }));
    let reservation = scope.reserve().unwrap();
    scope.end();
    let ran_cleanup = Arc::new(AtomicBool::new(false));
    let policy = CleanupMarker(Arc::clone(&ran_cleanup));
    let error = match reservation.commit_with(String::from("partial session"), policy) {
        Ok(_) => panic!("closed resource scope must reject commit"),
        Err(error) => error,
    };
    let (reason, resource, policy, reservation) = error.into_parts();
    assert_eq!(reason, ResourceManageErrorKind::ScopeEnded);
    assert_eq!(resource, "partial session");
    let resource_id = reservation.retain_with_for_cleanup(resource, policy);

    assert_eq!(
        *progress.lock().unwrap(),
        vec![
            crate::v02::report::ResourceProgress::ScopeOpen,
            crate::v02::report::ResourceProgress::WaitingForUsers,
            crate::v02::report::ResourceProgress::Cleaning,
        ]
    );

    assert_eq!(resource_id.manager(), manager_id);
    assert!(!ran_cleanup.load(Ordering::SeqCst));
    assert!(reports.lock().unwrap().is_empty());
    cleanup.run_all(&executor);
    while executor.poll_async_turn() > 0 {}
    executor.shutdown().wait().unwrap();

    assert!(ran_cleanup.load(Ordering::SeqCst));
    let reports = reports.lock().unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].resources.len(), 1);
    assert_eq!(reports[0].resources[0].resource_id, resource_id);
}

#[test]
fn test_settlement_callback_waits_for_real_cleanup_completion() {
    struct Probe(Arc<AtomicUsize>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let (scope, cleanup) = scope();
    let executor = executor();
    let resource_drops = Arc::new(AtomicUsize::new(0));
    let reports = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&reports);
    scope.set_settlement_callback(Box::new(move |report| {
        observed.lock().unwrap().push(report);
    }));
    let resource = scope.manage(Probe(Arc::clone(&resource_drops))).unwrap();
    scope.end();
    assert!(reports.lock().unwrap().is_empty());

    drop(resource);
    assert_eq!(resource_drops.load(Ordering::SeqCst), 0);
    assert!(reports.lock().unwrap().is_empty());
    cleanup.run_all(&executor);
    executor.shutdown().wait().unwrap();

    assert_eq!(resource_drops.load(Ordering::SeqCst), 1);
    let reports = reports.lock().unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].resources.len(), 1);
    assert_eq!(reports[0].failures.len(), 0);
}

struct AcceptedOwner(Arc<AtomicUsize>);
impl Drop for AcceptedOwner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl CleanupJob for AcceptedOwner {
    fn schedule(self: Box<Self>, executor: Arc<CleanupExecutor>) -> Result<(), Box<dyn CleanupJob>> {
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
fn test_accepted_cleanup_transfer_receipt_reaches_resource_scope_report() {
    let manager_id = ManagerId::new_unique();
    let source_id = ResourceScopeId::new_unique();
    let target_id = ResourceScopeId::new_unique();
    let pools = ManagerCapacityPools::new(ManagerLimits::default());
    let source = ResourceScope::new(
        manager_id,
        source_id,
        Arc::new(ScopeCapacity::root(&pools, 77, ScopeLimits::default())),
        ManualCleanup::new(1),
    );
    let report = Arc::new(Mutex::new(None));
    let report_slot = Arc::clone(&report);
    source.set_settlement_callback(Box::new(move |report| {
        *report_slot.lock().unwrap() = Some(report);
    }));
    let reporting_scope = source.internal_clone();
    let executor = executor();
    let receiver = CleanupTransferReceiver::new_with_receipt_callback(
        manager_id,
        target_id,
        1,
        Arc::clone(&executor),
        move |receipt| {
            assert!(reporting_scope.record_transfer_receipt(receipt));
        },
    )
    .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let resource_id = ResourceId::new(manager_id, 12, 1);
    let receipt = match receiver.accept(resource_id, source_id, Box::new(AcceptedOwner(Arc::clone(&drops)))) {
        Ok(receipt) => receipt,
        Err(_) => panic!("open receiver should accept transfer"),
    };
    assert_eq!(receipt.resource(), resource_id);
    assert_eq!(receiver.drive(1), 1);
    executor.shutdown().wait().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    source.end();
    let report = report.lock().unwrap().take().unwrap();
    assert_eq!(report.transfers_total, 1);
    assert!(!report.transfers_truncated);
    assert_eq!(report.transfers[0].resource(), resource_id);
    assert_eq!(report.transfers[0].source_scope(), source_id);
    assert_eq!(report.transfers[0].target_scope(), target_id);
}

struct PanicOnDrop;
impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("resource cleanup did not confirm release");
    }
}

#[test]
fn test_resource_drop_panic_has_no_released_evidence() {
    let (scope, cleanup) = scope();
    let report = Arc::new(Mutex::new(None));
    let report_slot = Arc::clone(&report);
    scope.set_settlement_callback(Box::new(move |report| {
        *report_slot.lock().unwrap() = Some(report);
    }));
    let resource = match scope.manage(PanicOnDrop) {
        Ok(resource) => resource,
        Err(_) => panic!("open resource scope should accept resource"),
    };
    scope.end();
    drop(resource);
    let executor = executor();
    cleanup.run_all(&executor);
    executor.shutdown().wait().unwrap();
    let report = report.lock().unwrap().take().unwrap();
    assert!(report.resources.is_empty());
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].stage, crate::v02::report::CleanupStage::Drop);
}

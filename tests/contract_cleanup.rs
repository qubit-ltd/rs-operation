use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use qubit_operation::protocol::CleanupContext;
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::report::CleanupFailure;
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::CleanupStage;
use qubit_operation::report::ClockDomainId;
use qubit_operation::report::DiagnosticSummary;
use qubit_operation::report::MonotonicTime;
use qubit_operation::report::OperationId;
use qubit_operation::report::RecoveryOwner;
use qubit_operation::report::ResourceId;
use qubit_operation::report::ResourcePhase;
use qubit_operation::report::SettlementReport;
use qubit_operation::resource::CleanupObservation;
use qubit_operation::resource::CleanupResponsibility;
use qubit_operation::resource::CleanupScheduler;
use qubit_operation::resource::RecoveryRegistry;
use qubit_operation::resource::ResourceRegistry;

struct NoopWake;
impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

struct ControlledPending {
    ready: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
}

impl Future for ControlledPending {
    type Output = CleanupOutcome;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.ready.load(Ordering::SeqCst) {
            Poll::Ready(CleanupOutcome::Cleaned)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ControlledPending {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct CountCleanup(Arc<AtomicUsize>);

impl OwnedCleanup for CountCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { CleanupOutcome::Cleaned })
    }
}

struct FailedCleanup {
    resource_id: ResourceId,
    owner: RecoveryOwner,
}
impl OwnedCleanup for FailedCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        let failure = CleanupFailure {
            resource_id: self.resource_id,
            stage: CleanupStage::Removing,
            summary: DiagnosticSummary::new("cleanup failed"),
            source: None,
        };
        Box::pin(std::future::ready(CleanupOutcome::FailedWithRecovery(
            failure, self.owner,
        )))
    }
}

#[test]
fn cleanup_permits_are_independent_of_business_queue_and_bounded() {
    let scheduler = CleanupScheduler::new(1);
    let permit = scheduler.try_acquire().expect("cleanup slot available");
    assert!(scheduler.try_acquire().is_none());
    drop(permit);
    assert!(scheduler.try_acquire().is_some());
}

#[test]
fn receiver_receipt_requires_acceptance_and_source_tombstone_waits_for_release() {
    let source = ResourceRegistry::new(OperationId::new(1), 1);
    let destination = ResourceRegistry::new(OperationId::new(2), 1);
    let calls = Arc::new(AtomicUsize::new(0));
    let handle = source.register(Box::new(CountCleanup(Arc::clone(&calls)))).unwrap();
    handle.close_for_use().unwrap();
    let ready = source.take_ready_cleanup().unwrap();
    let (resource_id, owner) = ready.into_parts();
    let (_, receipt) = destination.accept_cleanup_transfer(resource_id, owner).unwrap();
    source.confirm_transferred(resource_id, receipt).unwrap();
    assert!(source.contains(resource_id), "source tombstone survives transfer");
    let destination_cleanup = destination.take_ready_cleanup().unwrap();
    let (destination_id, owner) = destination_cleanup.into_parts();
    let _future = owner.begin(CleanupContext { deadline: None });
    assert!(
        destination.confirm_released(destination_id).is_err(),
        "transferred owner requires receiver release proof"
    );
    let release = destination
        .confirm_released_with_receipt(destination_id)
        .unwrap()
        .unwrap();
    assert!(
        source.contains(resource_id),
        "receiver cleanup alone cannot erase source history"
    );
    assert!(
        source.confirm_released(resource_id).is_err(),
        "source cannot self-confirm external release"
    );
    source.confirm_transferred_released(resource_id, release).unwrap();
    assert!(!source.contains(resource_id));
}

#[test]
fn recovery_registry_preserves_typed_owner_until_single_extraction() {
    let id = ResourceId::new(OperationId::new(4), 2, 9);
    let recovery = RecoveryRegistry::new(1);
    let key = recovery.register(id, String::from("reopen handle")).unwrap();
    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery.take_recovery(key).unwrap(), "reopen handle");
    assert_eq!(recovery.len(), 0);
}

#[test]
fn recovery_key_from_another_registry_cannot_take_owner_with_same_resource_id() {
    let id = ResourceId::new(OperationId::new(90), 1, 3);
    let registry_a = RecoveryRegistry::new(1);
    let registry_b = RecoveryRegistry::new(1);
    let key_a = registry_a.register(id, String::from("owner a")).unwrap();
    let key_b = registry_b.register(id, String::from("owner b")).unwrap();

    let key_a = match registry_b.take_recovery(key_a) {
        Ok(_) => panic!("a registry must reject another registry's key"),
        Err(key) => key,
    };
    assert_eq!(registry_b.take_recovery(key_b).unwrap(), "owner b");
    assert_eq!(registry_a.take_recovery(key_a).unwrap(), "owner a");
}

#[test]
fn responsibility_owner_retains_pending_task_after_scheduler_and_observer_drop() {
    let id = ResourceId::new(OperationId::new(91), 0, 1);
    let scheduler = CleanupScheduler::new(1);
    let responsibility: CleanupResponsibility = scheduler.responsibility_owner();
    let ready = Arc::new(AtomicBool::new(false));
    let drops = Arc::new(AtomicUsize::new(0));
    let future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send>> = Box::pin(ControlledPending {
        ready: Arc::clone(&ready),
        drops: Arc::clone(&drops),
    });
    let mut observer = scheduler
        .schedule_until(id, MonotonicTime::new(ClockDomainId(3), 5), future)
        .unwrap_or_else(|_| unreachable!());
    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        observer.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(3), 5)),
        Poll::Ready(CleanupObservation::Unconfirmed { resource_id }) if resource_id == id
    ));
    assert_eq!(scheduler.active(), 1);

    drop(observer);
    drop(scheduler);
    assert_eq!(responsibility.active(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    let mut observer = responsibility.task(id).expect("supervisor responsibility retains task");
    assert!(matches!(
        observer.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(3), 6)),
        Poll::Ready(CleanupObservation::Pending)
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    ready.store(true, Ordering::SeqCst);
    assert!(matches!(
        observer.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(3), 7)),
        Poll::Ready(CleanupObservation::Settled(Ok(CleanupOutcome::Cleaned)))
    ));
    assert_eq!(responsibility.active(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn cleanup_panic_is_reported_and_independent_capsule_remains_retrievable() {
    let id = ResourceId::new(OperationId::new(7), 1, 2);
    let recovery = RecoveryRegistry::new(1);
    let key = recovery.register(id, String::from("reopen resource")).unwrap();
    let scheduler = CleanupScheduler::new(1);
    let future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send>> = Box::pin(async {
        panic!("cleanup adapter panicked");
        #[allow(unreachable_code)]
        CleanupOutcome::Cleaned
    });
    let scheduled = match scheduler.schedule(id, future) {
        Ok(task) => task,
        Err(_) => panic!("cleanup slot available"),
    };
    let mut task = Box::pin(scheduled);
    assert_eq!(scheduler.active(), 1);
    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);
    let Poll::Ready(CleanupObservation::Settled(Err(panic))) = task
        .as_mut()
        .get_mut()
        .poll_at(&mut cx, MonotonicTime::new(ClockDomainId(1), 0))
    else {
        panic!("panic must settle the cleanup task")
    };
    assert_eq!(panic.resource_id(), id);
    assert_eq!(scheduler.active(), 0);
    assert_eq!(recovery.take_recovery(key).unwrap(), "reopen resource");
}

#[test]
fn expired_deadline_reports_unconfirmed_without_dropping_task_or_permit() {
    let id = ResourceId::new(OperationId::new(8), 1, 0);
    let scheduler = CleanupScheduler::new(1);
    let future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send>> = Box::pin(async { CleanupOutcome::Cleaned });
    let mut task_handle = Box::pin(
        scheduler
            .schedule_until(id, MonotonicTime::new(ClockDomainId(9), 5), future)
            .unwrap_or_else(|_| unreachable!()),
    );
    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);
    {
        let task = task_handle.as_mut().get_mut();
        assert!(matches!(
            task.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(8), 5)),
            Poll::Ready(CleanupObservation::ClockDomainMismatch { resource_id }) if resource_id == id
        ));
        assert_eq!(scheduler.active(), 1);
        assert!(matches!(
            task.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(9), 5)),
            Poll::Ready(CleanupObservation::Unconfirmed { resource_id }) if resource_id == id
        ));
    }
    assert_eq!(scheduler.active(), 1, "Unconfirmed keeps cleanup capacity reserved");
    drop(task_handle);
    let mut task = scheduler.task(id).expect("scheduler retains task after observer drop");
    assert!(matches!(
        task.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(9), 6)),
        Poll::Ready(CleanupObservation::Settled(Ok(CleanupOutcome::Cleaned)))
    ));
    assert_eq!(scheduler.active(), 0, "confirmed completion releases cleanup capacity");
}

#[test]
fn failed_prerequisite_blocks_dependent_even_after_capsule_is_taken() {
    let registry = ResourceRegistry::new(OperationId::new(10), 2);
    let calls = Arc::new(AtomicUsize::new(0));
    let dependent = registry.register(Box::new(CountCleanup(Arc::clone(&calls)))).unwrap();
    let prerequisite_id = ResourceId::new(OperationId::new(10), 1, 0);
    let recovery = RecoveryRegistry::new(1);
    let (key, failure_owner) = recovery
        .register_with_capsule(
            prerequisite_id,
            String::from("panic capsule"),
            String::from("retry capability"),
        )
        .unwrap();
    let prerequisite = registry
        .register(Box::new(FailedCleanup {
            resource_id: prerequisite_id,
            owner: failure_owner,
        }))
        .unwrap();
    dependent.cleanup_after(&prerequisite).unwrap();
    dependent.close_for_use().unwrap();
    prerequisite.close_for_use().unwrap();
    let ready = registry.take_ready_cleanup().unwrap();
    assert_eq!(ready.resource_id(), prerequisite.id());
    let (resource_id, owner) = ready.into_parts();
    let future = owner.begin(CleanupContext { deadline: None });
    let scheduler = CleanupScheduler::new(1);
    let mut task = Box::pin(
        scheduler
            .schedule(resource_id, future)
            .unwrap_or_else(|_| unreachable!()),
    );
    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);
    let Poll::Ready(CleanupObservation::Settled(Ok(CleanupOutcome::FailedWithRecovery(failure, _owner)))) = task
        .as_mut()
        .get_mut()
        .poll_at(&mut cx, MonotonicTime::new(ClockDomainId(1), 0))
    else {
        panic!("prerequisite must report its cleanup failure")
    };
    assert_eq!(failure.resource_id, prerequisite_id);
    assert!(
        registry.take_ready_cleanup().is_none(),
        "dependent remains blocked by failed prerequisite"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(recovery.take_recovery(key).unwrap(), "panic capsule");
    assert!(
        registry.take_ready_cleanup().is_none(),
        "taking one capsule does not release the dependent"
    );
}

#[test]
fn recovery_key_is_single_use_and_capacity_failure_returns_owner() {
    let id = ResourceId::new(OperationId::new(11), 0, 4);
    let old_generation = ResourceId::new(OperationId::new(11), 0, 3);
    let recovery = RecoveryRegistry::new(1);
    let key = recovery.register(id, String::from("current owner")).unwrap();
    let rejected = recovery
        .register(old_generation, String::from("old owner"))
        .unwrap_err();
    assert_eq!(rejected, "old owner");
    assert!(recovery.keep(&key));
    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery.take_recovery(key).unwrap(), "current owner");
    assert_eq!(recovery.len(), 0);
}

#[test]
fn settlement_report_remains_a_stable_historical_snapshot_after_recovery() {
    let id = ResourceId::new(OperationId::new(15), 2, 3);
    let recovery = RecoveryRegistry::new(1);
    let key = recovery.register(id, String::from("capsule")).unwrap();
    let report = SettlementReport {
        operation_id: OperationId::new(15),
        resource_phase: ResourcePhase::RecoveryRequired,
        all_released: false,
        children: Arc::from([]),
        cleanup_failures: Arc::from([]),
        transfers: Arc::from([]),
        unconfirmed_count: 0,
        effects: Arc::from([]),
        effect_overflow: 0,
    };
    let historical = report.clone();
    assert_eq!(recovery.take_recovery(key).unwrap(), "capsule");
    assert_eq!(report, historical);
    assert_eq!(report.resource_phase, ResourcePhase::RecoveryRequired);
}

#[test]
fn retry_keeps_capsule_and_dependency_group_transfer_is_atomic() {
    let id = ResourceId::new(OperationId::new(12), 0, 2);
    let recovery = RecoveryRegistry::new(1);
    let key = recovery.register(id, String::from("capsule")).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let retry = recovery
        .retry_cleanup(
            &key,
            Box::new(CountCleanup(Arc::clone(&calls))),
            CleanupContext { deadline: None },
        )
        .unwrap_or_else(|_| unreachable!());
    let scheduler = CleanupScheduler::new(1);
    let mut task = Box::pin(scheduler.schedule(id, retry).unwrap_or_else(|_| unreachable!()));
    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        task.as_mut()
            .get_mut()
            .poll_at(&mut cx, MonotonicTime::new(ClockDomainId(1), 0)),
        Poll::Ready(CleanupObservation::Settled(Ok(CleanupOutcome::Cleaned)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(recovery.keep(&key), "retry does not discard the retained capsule");
    assert_eq!(recovery.take_recovery(key).unwrap(), "capsule");

    let source = ResourceRegistry::new(OperationId::new(13), 2);
    let destination = ResourceRegistry::new(OperationId::new(14), 2);
    let prerequisite = source
        .register(Box::new(CountCleanup(Arc::new(AtomicUsize::new(0)))))
        .unwrap();
    let dependent = source
        .register(Box::new(CountCleanup(Arc::new(AtomicUsize::new(0)))))
        .unwrap();
    dependent.cleanup_after(&prerequisite).unwrap();
    let source_ids = [prerequisite.id(), dependent.id()];
    source.transfer_to(&prerequisite, &destination).unwrap();
    assert!(source_ids.iter().all(|resource| !source.contains(*resource)));
    assert!(destination.contains(prerequisite.id()) && destination.contains(dependent.id()));
}

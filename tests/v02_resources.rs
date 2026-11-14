use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_operation::AsyncCleanup;
use qubit_operation::CleanupContext;
use qubit_operation::CleanupFuture;
use qubit_operation::CleanupOutcome;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ResourceRef;
use qubit_operation::SettlementWait;
use qubit_operation::ShutdownMode;

struct Probe(Arc<AtomicUsize>);
impl Drop for Probe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ChildPayload {
    _parent_ref: ResourceRef<Probe>,
}
struct GatedCleanup(crate::support::Gate);

impl AsyncCleanup<ChildPayload> for GatedCleanup {
    fn cleanup(self: Arc<Self>, resource: ChildPayload, _context: CleanupContext) -> CleanupFuture<ChildPayload> {
        let gate = self.0.clone();
        Box::pin(async move {
            gate.wait().await;
            drop(resource);
            CleanupOutcome::Released
        })
    }
}

impl AsyncCleanup<usize> for GatedCleanup {
    fn cleanup(self: Arc<Self>, resource: usize, _context: CleanupContext) -> CleanupFuture<usize> {
        let gate = self.0.clone();
        Box::pin(async move {
            gate.wait().await;
            let _ = resource;
            CleanupOutcome::Released
        })
    }
}

#[allow(dead_code, unused_imports)]
#[path = "support/mod.rs"]
mod support;

#[test]
fn open_zero_can_get_again_then_settle_releases_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let body_drops = Arc::clone(&drops);
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let key = {
                let input = scope.manage(Probe(Arc::clone(&body_drops))).map_err(|_| "manage")?;
                input.key()
            };
            assert_eq!(body_drops.load(Ordering::SeqCst), 0);
            let again = scope.get_resource(key).map_err(|_| "lookup")?;
            let copied_key_lookup = scope.get_resource(key).map_err(|_| "copied key lookup")?;
            assert_eq!(body_drops.load(Ordering::SeqCst), 0);
            drop(again);
            drop(copied_key_lookup);
            Ok::<_, &'static str>(())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert!(operation.wait_settlement().unwrap().all_released());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
}

#[test]
fn resource_ref_shares_mutex_protected_value() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), |scope| {
            let value = scope.manage(std::sync::Mutex::new(1usize)).unwrap();
            let clone = value.clone();
            *clone.lock().unwrap() += 1;
            Ok::<_, ()>(*value.lock().unwrap())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(2)
    ));
    operation.wait_settlement().unwrap();
    manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
}

#[test]
fn moving_scope_out_of_body_does_not_extend_its_authority() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let saved = Arc::new(std::sync::Mutex::new(None));
    let saved_in_body = Arc::clone(&saved);
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |scope| {
            *saved_in_body.lock().unwrap() = Some(scope);
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    operation.wait_settlement().unwrap();
    let scope = saved.lock().unwrap().take().unwrap();
    let error = match scope.manage(7usize) {
        Ok(_) => panic!("ended scope unexpectedly accepted a resource"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), qubit_operation::ResourceManageErrorKind::ScopeEnded);
    let rejected = scope
        .manager()
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(
        rejected,
        Err(error) if error.reason() == qubit_operation::AdmissionError::ScopeEnded
    ));
    manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
}

#[test]
fn child_cleanup_keeps_parent_resource_until_its_last_parent_ref_is_released() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let drops = Arc::clone(&drop_count);
    let probe = Probe(Arc::clone(&drop_count));
    let gate = support::Gate::new();
    let cleanup = Arc::new(GatedCleanup(gate.clone()));
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let parent_ref = scope.manage(probe).unwrap();
            let child_ref = parent_ref.clone();
            drop(parent_ref);
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), move |child_scope| {
                    let child_resources = child_scope
                        .resource_scope(qubit_operation::ResourceScopeOptions { capacity: 1 })
                        .unwrap();
                    let child = child_resources
                        .manage_with(ChildPayload { _parent_ref: child_ref }, cleanup)
                        .unwrap();
                    drop(child);
                    child_resources.end();
                    Ok::<_, ()>(())
                })
                .unwrap();
            let _child_observer = child.into_observer();
            Ok::<_, ()>(())
        })
        .unwrap();

    gate.wait_started();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "child cleanup still owns the parent reference"
    );
    assert!(matches!(
        parent.wait_settlement_for(Duration::ZERO).unwrap(),
        SettlementWait::Pending(_)
    ));
    gate.release();
    assert!(parent.wait_settlement().unwrap().all_released());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
}

#[test]
fn unrelated_child_cleanup_does_not_delay_parent_resource_release() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let probe = Probe(Arc::clone(&drop_count));
    let gate = support::Gate::new();
    let cleanup = Arc::new(GatedCleanup(gate.clone()));
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let parent_ref = scope.manage(probe).unwrap();
            drop(parent_ref);
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), move |child_scope| {
                    let child_resources = child_scope
                        .resource_scope(qubit_operation::ResourceScopeOptions { capacity: 1 })
                        .unwrap();
                    let child = child_resources.manage_with(7usize, cleanup).unwrap();
                    drop(child);
                    child_resources.end();
                    Ok::<_, ()>(())
                })
                .unwrap();
            let _child_observer = child.into_observer();
            Ok::<_, ()>(())
        })
        .unwrap();

    gate.wait_started();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert_eq!(
        drop_count.load(Ordering::SeqCst),
        1,
        "parent scope ended and no child holds its resource"
    );
    assert!(matches!(
        parent.wait_settlement_for(Duration::ZERO).unwrap(),
        SettlementWait::Pending(_)
    ));
    gate.release();
    assert!(parent.wait_settlement().unwrap().all_released());
    manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
}

use qubit_operation::{
    ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn body_result_and_resource_settlement_are_separate_facts() {
    let drops = Arc::new(AtomicUsize::new(0));
    let probe_drops = Arc::clone(&drops);
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut handle = manager
        .try_submit(OperationOptions::default(), move |scope| {
            struct Count(Arc<AtomicUsize>);
            impl Drop for Count {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
            let resource = scope.manage(Count(probe_drops)).unwrap();
            drop(resource);
            Ok::<_, ()>(19)
        })
        .unwrap();
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(19)
    ));
    let settlement = handle.wait_settlement().unwrap();
    assert!(settlement.all_released());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

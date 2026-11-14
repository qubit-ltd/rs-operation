use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ShutdownMode;
use qubit_operation::StopReason;

struct Input(Arc<AtomicUsize>);
impl Drop for Input {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn stopping_sync_work_keeps_input_until_the_body_really_returns() {
    let drops = Arc::new(AtomicUsize::new(0));
    let body_drops = Arc::clone(&drops);
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let input = scope.manage(Input(body_drops)).unwrap();
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(input);
            Ok::<_, ()>(())
        })
        .unwrap();

    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        operation.request_stop(StopReason::UserCancelled),
        qubit_operation::StopRequestStatus::Accepted
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "stop is a request and cannot reclaim active input"
    );
    release_tx.send(()).unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    assert!(operation.wait_settlement().unwrap().all_released());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
}

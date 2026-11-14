use std::sync::mpsc;
use std::time::Duration;

use qubit_operation::AdmissionError;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ShutdownMode;
use qubit_operation::ShutdownWait;

#[test]
fn drain_closes_roots_and_completes_when_idle() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Drain);
    let result = manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(result, Err(error) if error.reason() == AdmissionError::Closed));
    shutdown.wait().unwrap();
}

#[test]
fn drain_rejects_new_roots_but_keeps_open_descendants_admissible() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (bound_tx, bound_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            bound_tx.send(scope.manager()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let bound = bound_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Drain);
    assert!(matches!(
        manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
        Err(error) if error.reason() == AdmissionError::Closed
    ));
    let mut child = bound
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
        .unwrap();
    assert!(matches!(child.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
    release_tx.send(()).unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    parent.wait_settlement().unwrap();
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(5)).unwrap(),
        ShutdownWait::Completed(_)
    ));
}

#[test]
fn cancel_waits_for_noninterruptible_body_to_return() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Cancel);
    assert!(matches!(
        shutdown.wait_for(Duration::ZERO).unwrap(),
        ShutdownWait::Pending(_)
    ));
    release_tx.send(()).unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    operation.wait_settlement().unwrap();
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(5)).unwrap(),
        ShutdownWait::Completed(_)
    ));
}

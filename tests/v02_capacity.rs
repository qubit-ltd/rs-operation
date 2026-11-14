use qubit_operation::{
    AdmissionError, ManagerOptions, OperationManager, OperationOptions, OperationOutcome,
    ScopeLimits, ShutdownMode,
};
use std::sync::mpsc;

#[test]
fn independent_new_managers_do_not_share_capacity_state() {
    let first = OperationManager::new(ManagerOptions::default()).unwrap();
    let second = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut a = first
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(1))
        .unwrap();
    let mut b = second
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(2))
        .unwrap();
    assert!(matches!(
        a.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(1)
    ));
    assert!(matches!(
        b.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(2)
    ));
    a.wait_settlement().unwrap();
    b.wait_settlement().unwrap();
    first.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    second.request_shutdown(ShutdownMode::Drain).wait().unwrap();
}

#[test]
fn a_parent_child_slot_stays_reserved_until_the_child_really_finishes() {
    let mut manager_options = ManagerOptions::default();
    manager_options.scope_limits = ScopeLimits {
        child_operations: 1,
        ..ScopeLimits::default()
    };
    let manager = OperationManager::new(manager_options).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (rejection_tx, rejection_rx) = mpsc::channel();
    let (observer_tx, observer_rx) = mpsc::channel();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), move |_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok::<_, ()>(())
                })
                .unwrap();
            observer_tx.send(child.observer()).unwrap();
            let rejected = scope
                .manager()
                .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
            rejection_tx.send(rejected.err().unwrap().reason()).unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();

    entered_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        rejection_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap(),
        AdmissionError::Busy
    );
    let child_observer = observer_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let before_release = child_observer.snapshot();
    assert!(
        before_release.stop.is_none(),
        "a Busy rejection must not stop the accepted child: {before_release:?}"
    );
    release_tx.send(()).unwrap();
    let parent_outcome = parent.wait_outcome().unwrap();
    assert!(
        matches!(parent_outcome, OperationOutcome::Succeeded(())),
        "{parent_outcome:?}"
    );
    let after_release = child_observer.snapshot();
    assert!(
        after_release.stop.is_none(),
        "the accepted child was stopped unexpectedly: {after_release:?}"
    );
    assert!(parent.wait_settlement().unwrap().all_released());
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

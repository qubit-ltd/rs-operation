use qubit_operation::{
    ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode,
};

#[test]
fn scope_manager_submits_a_direct_child_and_cannot_reopen_after_body() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let saved = std::sync::Arc::new(std::sync::Mutex::new(None));
    let saved_body = saved.clone();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            *saved_body.lock().unwrap() = Some(scope.manager());
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    parent.wait_settlement().unwrap();
    let bound = saved.lock().unwrap().take().unwrap();
    let rejected = bound.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
    assert!(
        matches!(rejected, Err(error) if error.reason() == qubit_operation::AdmissionError::ScopeEnded)
    );
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

#[test]
fn required_child_failure_is_not_hidden_by_parent_success() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut parent = manager
        .try_submit(OperationOptions::default(), |scope| {
            let mut child = scope
                .manager()
                .try_submit(OperationOptions::default(), |_| Err::<(), _>("failed"))
                .unwrap();
            let _ = child.wait_outcome();
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::ChildFailed(_)
    ));
    parent.wait_settlement().unwrap();
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

#[test]
fn manager_helper_creates_a_root_child_and_grandchild_at_the_callers_depth() {
    fn submit_value(
        manager: &OperationManager,
        value: usize,
    ) -> qubit_operation::OperationHandle<usize, ()> {
        manager
            .try_submit(OperationOptions::default(), move |_| Ok::<_, ()>(value))
            .unwrap()
    }
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut root = submit_value(&manager, 1);
    assert!(matches!(
        root.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(1)
    ));
    root.wait_settlement().unwrap();

    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let mut child = scope
                .manager()
                .try_submit(OperationOptions::default(), move |child_scope| {
                    let mut grandchild = submit_value(&child_scope.manager(), 3);
                    match grandchild.wait_outcome().unwrap() {
                        OperationOutcome::Succeeded(value) => {
                            grandchild.wait_settlement().unwrap();
                            Ok::<_, ()>(value)
                        }
                        _ => Err(()),
                    }
                })
                .unwrap();
            match child.wait_outcome().unwrap() {
                OperationOutcome::Succeeded(value) => {
                    child.wait_settlement().unwrap();
                    Ok::<_, ()>(value)
                }
                _ => Err(()),
            }
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(3)
    ));
    parent.wait_settlement().unwrap();
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

#[test]
fn serial_children_keep_cumulative_history_beyond_the_retained_report_limit() {
    let mut options = ManagerOptions::default();
    options.limits.diagnostic_events = 1;
    let manager = OperationManager::new(options).unwrap();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            for value in 0..8usize {
                let mut child = scope
                    .manager()
                    .try_submit(OperationOptions::default(), move |_| Ok::<_, ()>(value))
                    .unwrap();
                assert!(matches!(
                    child.wait_outcome().unwrap(),
                    OperationOutcome::Succeeded(_)
                ));
                child.wait_settlement().unwrap();
            }
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let report = parent.wait_settlement().unwrap();
    assert_eq!(report.children_total, 8);
    assert!(report.children_truncated);
    assert!(report.children.len() <= 1);
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

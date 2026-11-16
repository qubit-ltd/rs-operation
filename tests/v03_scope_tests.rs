use qubit_operation::{GroupBindingError, GroupOptions, ManagerCreateError, ManagerOptions, OperationManager};

#[test]
fn with_group_returns_same_constraint_view_and_deduplicates() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let bound = manager.with_group(&group).unwrap();
    let repeated = bound.with_group(&group).unwrap();
    assert_eq!(group.snapshot().active_operations, 0);
    assert!(bound.as_async().as_sync().with_group(&group).is_ok());
    drop(repeated);
    drop(bound);
    group.close_admission();
    assert!(matches!(
        manager.with_group(&group),
        Err(GroupBindingError::Closed)
    ));
}

#[test]
fn distinct_groups_both_track_a_bound_operation() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let bound = manager.with_group(&first).unwrap().with_group(&second).unwrap();
    let mut operation = bound.try_submit(Default::default(), |_| Ok::<_, ()>(())).unwrap();
    assert!(operation.wait_outcome().is_ok());
    operation.wait_settlement().unwrap();
    assert_eq!(first.snapshot().active_operations, 0);
    assert_eq!(second.snapshot().active_operations, 0);
}

#[test]
fn group_binding_limit_uses_manager_configuration() {
    let mut options = ManagerOptions::default();
    options.max_groups_per_operation = 1;
    let manager = OperationManager::new(options).unwrap();
    let first = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let bound = manager.with_group(&first).unwrap();
    assert!(matches!(bound.with_group(&second), Err(GroupBindingError::CapacityExceeded)));
}

#[test]
fn zero_group_binding_limit_is_rejected() {
    let mut options = ManagerOptions::default();
    options.max_groups_per_operation = 0;
    assert!(matches!(OperationManager::new(options), Err(ManagerCreateError::InvalidOptions { field: "max_groups_per_operation" })));
}

#[test]
fn closing_any_bound_group_rejects_new_submission() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let bound = manager.with_group(&first).unwrap().with_group(&second).unwrap();
    second.close_admission();
    let rejected = bound.try_submit(Default::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(rejected, Err(error) if error.reason() == qubit_operation::AdmissionError::Closed));
}

#[test]
fn parent_bound_view_inherits_and_adds_group_constraints() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 4 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 4 }).unwrap();
    let second_for_child = second.clone();
    let mut parent = first.try_submit(Default::default(), move |scope| {
        let bound = scope.manager().with_group(&second_for_child).unwrap();
        let mut child = bound.try_submit(Default::default(), |_| Ok::<_, ()>(9usize)).unwrap();
        assert!(matches!(child.wait_outcome().unwrap(), qubit_operation::OperationOutcome::Succeeded(9)));
        child.wait_settlement().unwrap();
        Ok::<_, ()>(())
    }).unwrap();
    assert!(parent.wait_outcome().is_ok());
    parent.wait_settlement().unwrap();
    assert_eq!(first.snapshot().active_operations, 0);
    assert_eq!(second.snapshot().active_operations, 0);
}

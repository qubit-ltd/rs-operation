use super::{ManagerCore, OperationManager};
use crate::v02::report::AdmissionError;
use std::sync::{Arc, Mutex};

fn submit_work(manager: &OperationManager, value: usize) -> Result<(), AdmissionError> {
    manager
        .try_submit(move |scope| {
            scope
                .manager()
                .try_submit(move |grandchild| {
                    grandchild.record_test_value(value);
                })
                .unwrap();
        })
        .map(|_| ())
        .map_err(|error| error.into_parts().0)
}

#[test]
fn same_helper_works_at_every_depth() {
    let (core, root) = ManagerCore::for_test();
    submit_work(&root, 7).unwrap();
    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 2);
    assert_eq!(relationships[0].1, None);
    assert_eq!(relationships[1].1, Some(relationships[0].0));
    assert_eq!(core.test_values(), vec![7]);
}

#[test]
fn manager_conversion_preserves_parent() {
    let (core, root) = ManagerCore::for_test();
    root.try_submit(|scope| {
        let parent = scope.id();
        scope
            .manager()
            .as_async()
            .as_sync()
            .try_submit(|_| {})
            .unwrap();
        assert_eq!(parent, scope.id());
    })
    .unwrap();
    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 2);
    assert_eq!(relationships[1].1, Some(relationships[0].0));
}

#[test]
fn root_manager_inside_operation_stays_root() {
    let (core, root) = ManagerCore::for_test();
    let root_clone = root.clone();
    root.try_submit(move |_| {
        root_clone.try_submit(|_| {}).unwrap();
    })
    .unwrap();
    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 2);
    assert_eq!(relationships[1].1, None);
}

#[test]
fn expired_parent_does_not_fall_back_to_root() {
    let saved = Arc::new(Mutex::new(None));
    let saved_in_body = Arc::clone(&saved);
    let (_, root) = ManagerCore::for_test();
    root.try_submit(move |scope| {
        *saved_in_body.lock().unwrap() = Some(scope.manager());
    })
    .unwrap();
    let manager = saved.lock().unwrap().take().unwrap();
    assert_eq!(
        manager.try_submit(|_| {}).unwrap_err().reason(),
        AdmissionError::ScopeEnded
    );
}

#[test]
fn frame_end_closes_scope_even_when_scope_moves_out() {
    let escaped = Arc::new(Mutex::new(None));
    let escaped_in_body = Arc::clone(&escaped);
    let (_, root) = ManagerCore::for_test();
    root.try_submit(move |scope| {
        *escaped_in_body.lock().unwrap() = Some(scope);
    })
    .unwrap();
    let scope = escaped.lock().unwrap().take().unwrap();
    assert!(!scope.is_open());
    assert_eq!(
        scope.manager().try_submit(|_| {}).unwrap_err().reason(),
        AdmissionError::ScopeEnded
    );
}

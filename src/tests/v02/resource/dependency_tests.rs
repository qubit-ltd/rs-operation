// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::v02::report::CleanupDependencyError;
use crate::v02::report::ManagerId;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;
use crate::v02::resource::dependency::DependencyGraph;

fn rid(manager: ManagerId, slot: u64) -> ResourceId {
    ResourceId::new(manager, slot, 1)
}

#[test]
fn test_dependency_graph_rejects_cycles() {
    let graph = DependencyGraph::default();
    let manager = ManagerId::new_unique();
    let scope = ResourceScopeId::new_unique();
    let a = rid(manager, 1);
    let b = rid(manager, 2);
    graph.add(a, scope, b, scope, Box::new(())).unwrap();
    let (error, _) = graph.add(b, scope, a, scope, Box::new(())).unwrap_err();
    assert_eq!(
        error,
        CleanupDependencyError::Cycle,
        "adding a reverse edge must report a dependency cycle"
    );
}

#[test]
fn test_dependent_holds_prerequisite_lease_until_its_cleanup() {
    let graph = DependencyGraph::default();
    let manager = ManagerId::new_unique();
    let scope = ResourceScopeId::new_unique();
    let prerequisite = rid(manager, 1);
    let dependent = rid(manager, 2);
    let drops = Arc::new(AtomicUsize::new(0));
    struct Lease(Arc<AtomicUsize>);
    impl Drop for Lease {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    graph
        .add(
            dependent,
            scope,
            prerequisite,
            scope,
            Box::new(Lease(Arc::clone(&drops))),
        )
        .unwrap();
    assert!(
        graph.try_start(prerequisite),
        "the prerequisite should start before its dependent"
    );
    assert!(
        graph.finish(prerequisite, true),
        "the prerequisite cleanup should finish successfully"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "the lease must remain held until dependent cleanup"
    );
    assert!(
        graph.try_start(dependent),
        "the dependent should start after prerequisite cleanup"
    );
    assert!(
        graph.finish(dependent, true),
        "the dependent cleanup should finish successfully"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "dependent cleanup should release the prerequisite lease exactly once"
    );
}

#[test]
fn test_failed_prerequisite_keeps_dependent_pending() {
    let graph = DependencyGraph::default();
    let manager = ManagerId::new_unique();
    let scope = ResourceScopeId::new_unique();
    let prerequisite = rid(manager, 1);
    let dependent = rid(manager, 2);
    graph.add(dependent, scope, prerequisite, scope, Box::new(())).unwrap();
    assert!(
        graph.try_start(prerequisite),
        "the prerequisite should start before the failed cleanup attempt"
    );
    assert!(
        graph.finish(prerequisite, false),
        "the graph should record the failed prerequisite cleanup"
    );
    assert!(
        !graph.try_start(dependent),
        "a dependent must stay pending while its prerequisite cleanup failed"
    );
    assert!(
        graph.retry(prerequisite),
        "a failed prerequisite cleanup should be retryable"
    );
    assert!(
        graph.try_start(prerequisite),
        "the retried prerequisite should become runnable"
    );
    assert!(
        graph.finish(prerequisite, true),
        "the retry should allow prerequisite cleanup to finish"
    );
    assert!(
        graph.try_start(dependent),
        "the dependent should become runnable after successful prerequisite cleanup"
    );
}

#[test]
fn test_released_resource_cannot_be_cleaned_twice() {
    let graph = DependencyGraph::default();
    let resource = rid(ManagerId::new_unique(), 1);
    assert!(graph.try_start(resource), "an unreleased resource should start cleanup");
    assert!(
        graph.finish(resource, true),
        "the resource cleanup should finish successfully"
    );
    assert!(
        !graph.try_start(resource),
        "a released resource must not start cleanup twice"
    );
}

#[test]
fn test_dependency_rejects_cross_scope_edges() {
    let graph = DependencyGraph::default();
    let manager = ManagerId::new_unique();
    let (error, _) = graph
        .add(
            rid(manager, 1),
            ResourceScopeId::new_unique(),
            rid(manager, 2),
            ResourceScopeId::new_unique(),
            Box::new(()),
        )
        .unwrap_err();
    assert_eq!(
        error,
        CleanupDependencyError::WrongScope,
        "dependency edges across resource scopes must be rejected"
    );
}

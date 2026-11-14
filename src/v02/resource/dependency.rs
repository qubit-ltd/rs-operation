//! Resource cleanup dependency graph.
#![allow(dead_code)] // Consumed by T11 resource-scope wiring.

use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::v02::report::{CleanupDependencyError, ResourceId, ResourceScopeId};

type DependencyLease = Box<dyn Any + Send + Sync + 'static>;

struct DependencyEdge {
    prerequisite: ResourceId,
    // Keeping this lease alive makes a prerequisite ResourceRef stay counted
    // until its dependent has actually completed cleanup.
    _lease: DependencyLease,
}

#[derive(Default)]
struct GraphState {
    edges: HashMap<ResourceId, Vec<DependencyEdge>>,
    started: HashSet<ResourceId>,
    released: HashSet<ResourceId>,
    failed: HashSet<ResourceId>,
}

/// Tracks cleanup ordering and the strong resource references that enforce it.
#[derive(Default)]
pub(crate) struct DependencyGraph {
    state: std::sync::Mutex<GraphState>,
}

impl DependencyGraph {
    /// Adds `dependent -> prerequisite`: the dependent must wait for the
    /// prerequisite, and its lease is retained until dependent cleanup succeeds.
    pub(crate) fn add(
        &self,
        dependent: ResourceId,
        dependent_scope: ResourceScopeId,
        prerequisite: ResourceId,
        prerequisite_scope: ResourceScopeId,
        lease: DependencyLease,
    ) -> Result<(), (CleanupDependencyError, DependencyLease)> {
        let err = |error, lease| Err((error, lease));
        if dependent.manager() != prerequisite.manager() {
            return err(CleanupDependencyError::WrongManager, lease);
        }
        if dependent_scope != prerequisite_scope {
            return err(CleanupDependencyError::WrongScope, lease);
        }
        if dependent == prerequisite {
            return err(CleanupDependencyError::SelfDependency, lease);
        }

        let mut state = lock(&self.state);
        if state.started.contains(&dependent) || state.started.contains(&prerequisite) {
            return err(CleanupDependencyError::CleanupStarted, lease);
        }
        if reaches(&state, prerequisite, dependent) {
            return err(CleanupDependencyError::Cycle, lease);
        }
        state
            .edges
            .entry(dependent)
            .or_default()
            .push(DependencyEdge {
                prerequisite,
                _lease: lease,
            });
        Ok(())
    }

    /// Atomically marks cleanup started only when all prerequisites were
    /// released. A failed or still-pending prerequisite keeps this resource
    /// unavailable for cleanup and leaves its dependency leases intact.
    pub(crate) fn try_start(&self, resource: ResourceId) -> bool {
        let mut state = lock(&self.state);
        if state.started.contains(&resource)
            || state.failed.contains(&resource)
            || state.released.contains(&resource)
        {
            return false;
        }
        let ready = state
            .edges
            .get(&resource)
            .into_iter()
            .flatten()
            .all(|edge| state.released.contains(&edge.prerequisite));
        if ready {
            state.started.insert(resource);
        }
        ready
    }

    /// Re-enables a failed resource for an explicit recovery attempt. This
    /// changes no report or owner state; the recovery coordinator is
    /// responsible for enforcing single-flight retries.
    pub(crate) fn retry(&self, resource: ResourceId) -> bool {
        let mut state = lock(&self.state);
        if state.started.contains(&resource) || state.released.contains(&resource) {
            return false;
        }
        state.failed.remove(&resource)
    }

    /// Records one cleanup result. On failure, dependency leases remain held
    /// so the complete dependent chain can be retained for recovery.
    pub(crate) fn finish(&self, resource: ResourceId, succeeded: bool) -> bool {
        let removed = {
            let mut state = lock(&self.state);
            if !state.started.remove(&resource) {
                return false;
            }
            if succeeded {
                state.released.insert(resource);
                state.edges.remove(&resource)
            } else {
                state.failed.insert(resource);
                None
            }
        };
        // Leases can run arbitrary destructor code; never drop them under the
        // graph mutex.
        drop(removed);
        true
    }

    pub(crate) fn is_released(&self, resource: ResourceId) -> bool {
        lock(&self.state).released.contains(&resource)
    }
}

fn reaches(state: &GraphState, from: ResourceId, target: ResourceId) -> bool {
    let mut pending = VecDeque::from([from]);
    let mut visited = HashSet::new();
    while let Some(current) = pending.pop_front() {
        if current == target {
            return true;
        }
        if !visited.insert(current) {
            continue;
        }
        if let Some(edges) = state.edges.get(&current) {
            pending.extend(edges.iter().map(|edge| edge.prerequisite));
        }
    }
    false
}

fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::DependencyGraph;
    use crate::v02::report::{CleanupDependencyError, ManagerId, ResourceId, ResourceScopeId};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    fn rid(manager: ManagerId, slot: u64) -> ResourceId {
        ResourceId::new(manager, slot, 1)
    }

    #[test]
    fn dependency_graph_rejects_cycles() {
        let graph = DependencyGraph::default();
        let manager = ManagerId::new_unique();
        let scope = ResourceScopeId::new_unique();
        let a = rid(manager, 1);
        let b = rid(manager, 2);
        graph.add(a, scope, b, scope, Box::new(())).unwrap();
        let (error, _) = graph.add(b, scope, a, scope, Box::new(())).unwrap_err();
        assert_eq!(error, CleanupDependencyError::Cycle);
    }

    #[test]
    fn dependent_holds_prerequisite_lease_until_its_cleanup() {
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
        assert!(graph.try_start(prerequisite));
        assert!(graph.finish(prerequisite, true));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(graph.try_start(dependent));
        assert!(graph.finish(dependent, true));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn failed_prerequisite_keeps_dependent_pending() {
        let graph = DependencyGraph::default();
        let manager = ManagerId::new_unique();
        let scope = ResourceScopeId::new_unique();
        let prerequisite = rid(manager, 1);
        let dependent = rid(manager, 2);
        graph
            .add(dependent, scope, prerequisite, scope, Box::new(()))
            .unwrap();
        assert!(graph.try_start(prerequisite));
        assert!(graph.finish(prerequisite, false));
        assert!(!graph.try_start(dependent));
        assert!(graph.retry(prerequisite));
        assert!(graph.try_start(prerequisite));
        assert!(graph.finish(prerequisite, true));
        assert!(graph.try_start(dependent));
    }

    #[test]
    fn released_resource_cannot_be_cleaned_twice() {
        let graph = DependencyGraph::default();
        let resource = rid(ManagerId::new_unique(), 1);
        assert!(graph.try_start(resource));
        assert!(graph.finish(resource, true));
        assert!(!graph.try_start(resource));
    }

    #[test]
    fn dependency_rejects_cross_scope_edges() {
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
        assert_eq!(error, CleanupDependencyError::WrongScope);
    }
}

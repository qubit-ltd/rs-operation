// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::collections::HashSet;
use std::collections::VecDeque;

use super::internal::DependencyEdge;
use super::internal::GraphState;
use crate::v02::report::CleanupDependencyError;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;

/// Type-erased strong owner retained while a dependency edge is active.
///
/// The graph drops this lease only after successful cleanup of the dependent;
/// failed cleanup keeps it alive for recovery.
pub(super) type DependencyLease = Box<dyn Any + Send + Sync + 'static>;

/// Tracks cleanup ordering and the owner leases that enforce dependency
/// lifetime.
///
/// An edge `dependent -> prerequisite` makes the dependent wait until the
/// prerequisite is released. Failed cleanup retains the edge and its lease so
/// recovery can retry without dropping the prerequisite owner.
#[derive(Default)]
pub(crate) struct DependencyGraph {
    /// Serializes edge updates and cleanup phase transitions.
    state: std::sync::Mutex<GraphState>,
}

impl DependencyGraph {
    /// Adds `dependent -> prerequisite`: the dependent must wait for the
    /// prerequisite, and its lease is retained until dependent cleanup
    /// succeeds.
    ///
    /// # Parameters
    ///
    /// - `self`: Graph that will own the dependency edge.
    /// - `dependent`: Resource whose cleanup waits for the prerequisite.
    /// - `dependent_scope`: Scope currently owning the dependent resource.
    /// - `prerequisite`: Resource that must be released first.
    /// - `prerequisite_scope`: Scope currently owning the prerequisite.
    /// - `lease`: Strong owner reference retained until dependent cleanup
    ///   succeeds.
    ///
    /// # Returns
    ///
    /// `Ok(())` after registering the edge. On error, returns the lease so the
    /// caller retains ownership and can handle the rejected dependency.
    ///
    /// # Errors
    ///
    /// Returns an error if resources belong to different managers or scopes,
    /// the edge is a self-dependency or cycle, or cleanup has already started.
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
        state.edges.entry(dependent).or_default().push(DependencyEdge {
            prerequisite,
            _lease: lease,
        });
        Ok(())
    }

    /// Atomically marks cleanup started only when all prerequisites were
    /// released. A failed or still-pending prerequisite keeps this resource
    /// unavailable for cleanup and leaves its dependency leases intact.
    ///
    /// # Parameters
    ///
    /// - `self`: Graph that tracks the resource's prerequisites.
    /// - `resource`: Resource whose cleanup is about to start.
    ///
    /// # Returns
    ///
    /// `true` if cleanup was marked started; otherwise `false` when it is
    /// already started, failed, released, or a prerequisite remains unreleased.
    pub(crate) fn try_start(&self, resource: ResourceId) -> bool {
        let mut state = lock(&self.state);
        if state.started.contains(&resource) || state.failed.contains(&resource) || state.released.contains(&resource) {
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
    ///
    /// # Parameters
    ///
    /// - `self`: Graph containing the failed cleanup state.
    /// - `resource`: Failed resource to make eligible for another attempt.
    ///
    /// # Returns
    ///
    /// `true` if a failed marker was removed; otherwise `false` when no failed
    /// marker exists or cleanup is started/released.
    pub(crate) fn retry(&self, resource: ResourceId) -> bool {
        let mut state = lock(&self.state);
        if state.started.contains(&resource) || state.released.contains(&resource) {
            return false;
        }
        state.failed.remove(&resource)
    }

    /// Records one cleanup result. On failure, dependency leases remain held
    /// so the complete dependent chain can be retained for recovery.
    ///
    /// Successful cleanup releases this resource and drops its outgoing
    /// dependency edges after unlocking the graph. Failed cleanup retains those
    /// edges and marks the resource failed.
    ///
    /// # Parameters
    ///
    /// - `self`: Graph that owns the cleanup phase and dependency edges.
    /// - `resource`: Resource whose started cleanup is being completed.
    /// - `succeeded`: Whether cleanup released the resource successfully.
    ///
    /// # Returns
    ///
    /// `true` if a started cleanup result was recorded; otherwise `false` when
    /// this resource was not marked started.
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

    /// Checks whether successful cleanup has released a resource.
    ///
    /// # Parameters
    ///
    /// - `self`: Graph that tracks resource release.
    /// - `resource`: Resource identity to inspect.
    ///
    /// # Returns
    ///
    /// `true` if `finish` recorded successful cleanup for this resource.
    pub(crate) fn is_released(&self, resource: ResourceId) -> bool {
        lock(&self.state).released.contains(&resource)
    }

    /// Returns the full undirected dependency component containing `resource`.
    /// Transfer callers compare their proposed member list with this set so a
    /// dependency group cannot be split across scopes.
    ///
    /// # Parameters
    ///
    /// - `self`: Graph containing the dependency component.
    /// - `resource`: Resource from which to traverse dependency edges.
    ///
    /// # Returns
    ///
    /// Every resource reachable through edges in either direction, including
    /// `resource` itself.
    pub(crate) fn group_members(&self, resource: ResourceId) -> HashSet<ResourceId> {
        let state = lock(&self.state);
        let mut members = HashSet::from([resource]);
        let mut pending = VecDeque::from([resource]);
        while let Some(current) = pending.pop_front() {
            for (dependent, edges) in &state.edges {
                for edge in edges {
                    if (*dependent == current || edge.prerequisite == current)
                        && members.insert(if *dependent == current {
                            edge.prerequisite
                        } else {
                            *dependent
                        })
                    {
                        pending.push_back(if *dependent == current {
                            edge.prerequisite
                        } else {
                            *dependent
                        });
                    }
                }
            }
        }
        members
    }

    /// Checks whether the complete dependency component can move to one scope.
    ///
    /// A group is transferable only if it matches the graph's full connected
    /// component and none of its members are started, failed, or released.
    ///
    /// # Parameters
    ///
    /// - `self`: Graph that validates group membership and cleanup state.
    /// - `members`: Proposed resource group to transfer.
    ///
    /// # Returns
    ///
    /// `true` when all members form one complete, not-yet-cleaned component.
    pub(crate) fn can_transfer_group(&self, members: &HashSet<ResourceId>) -> bool {
        let Some(anchor) = members.iter().next().copied() else {
            return false;
        };
        let state = lock(&self.state);
        let actual = group_members_locked(&state, anchor);
        if actual != *members {
            return false;
        }
        members.iter().all(|resource| {
            !state.started.contains(resource) && !state.released.contains(resource) && !state.failed.contains(resource)
        })
    }
}

/// Finds the undirected connected component while the caller holds the graph
/// lock.
///
/// # Parameters
///
/// - `state`: Locked dependency graph state.
/// - `resource`: Resource at which traversal begins.
///
/// # Returns
///
/// The component containing `resource`, including `resource` itself.
fn group_members_locked(state: &GraphState, resource: ResourceId) -> HashSet<ResourceId> {
    let mut members = HashSet::from([resource]);
    let mut pending = VecDeque::from([resource]);
    while let Some(current) = pending.pop_front() {
        for (dependent, edges) in &state.edges {
            for edge in edges {
                let adjacent = if *dependent == current {
                    Some(edge.prerequisite)
                } else if edge.prerequisite == current {
                    Some(*dependent)
                } else {
                    None
                };
                if let Some(adjacent) = adjacent
                    && members.insert(adjacent)
                {
                    pending.push_back(adjacent);
                }
            }
        }
    }
    members
}

/// Checks whether following dependent-to-prerequisite edges reaches a target.
///
/// # Parameters
///
/// - `state`: Dependency graph state being traversed.
/// - `from`: First resource in the directed search.
/// - `target`: Resource whose reachability is tested.
///
/// # Returns
///
/// `true` if a directed path from `from` to `target` exists.
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

/// Locks a graph mutex and recovers its inner state if a prior holder panicked.
///
/// # Parameters
///
/// - `mutex`: Graph state mutex to acquire.
///
/// # Type Parameters
///
/// - `T`: State stored in the mutex.
///
/// # Returns
///
/// The protected state guard, including the inner value of a poisoned mutex.
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::DependencyGraph;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ResourceId;

    #[test]
    fn test_release_query_tracks_only_successful_cleanup() {
        let graph = DependencyGraph::default();
        let resource = ResourceId::new(ManagerId::new_unique(), 1, 1);

        assert!(!graph.is_released(resource));
        assert!(graph.try_start(resource));
        assert!(graph.finish(resource, true));
        assert!(graph.is_released(resource));
    }
}

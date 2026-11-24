// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;

use super::super::GroupPermit;
use super::super::lock;
use super::group_state::GroupState;
use crate::v02::report::AdmissionError;
use crate::v02::report::GroupId;
use crate::v02::report::GroupSnapshot;
use crate::v02::report::OperationId;
use crate::v02::report::RecoveryPhase;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ShutdownMode;
use crate::v02::report::ShutdownSnapshot;
use crate::v02::report::StopReason;
use crate::v02::resource::RecoveryHandle;

/// Shared local admission state inherited by every operation submitted from
/// this group and by manager views bound to those operations.
#[must_use = "retain the group core while the group remains active"]
pub(crate) struct GroupCore {
    /// Stable identifier used in public group snapshots.
    id: GroupId,
    /// Maximum number of simultaneously admitted group operations.
    capacity: usize,
    /// Mutable admission, operation, service, and recovery state.
    pub(super) state: Mutex<GroupState>,
    /// Blocking wait notification for state transitions.
    pub(super) changed: Condvar,
    /// Async wait notification for state transitions.
    pub(super) notify: crate::v02::internal::notify::Notify,
}

impl GroupCore {
    /// Creates an accepting group core with the given operation capacity.
    ///
    /// # Parameters
    ///
    /// - `capacity`: Maximum number of active operations admitted by this
    ///   group.
    ///
    /// # Returns
    ///
    /// A shared group core initialized with empty operation, service, and
    /// recovery state.
    pub(in crate::v02::manager::group) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            id: GroupId::new_unique(),
            capacity,
            state: Mutex::new(GroupState {
                accepting: true,
                active: 0,
                unsettled_operations: 0,
                mode: ShutdownMode::Drain,
                scopes: std::collections::HashMap::new(),
                queued: std::collections::HashSet::new(),
                services: std::collections::HashMap::new(),
                recoveries: std::collections::HashMap::new(),
            }),
            changed: Condvar::new(),
            notify: crate::v02::internal::notify::Notify::new(),
        })
    }

    /// Reserves one group slot for a root submission or an accepted descendant.
    ///
    /// # Parameters
    ///
    /// - `descendant`: Whether the submission descends from already accepted
    ///   work.
    /// - `self`: Group core that grants or rejects the requested slot.
    ///
    /// # Returns
    ///
    /// A permit that releases the slot when dropped.
    ///
    /// # Errors
    ///
    /// Returns the admission reason when shutdown or capacity prevents
    /// reservation.
    #[cfg(test)]
    pub(crate) fn reserve(self: &Arc<Self>, descendant: bool) -> Result<GroupPermit, AdmissionError> {
        let memberships = if descendant { std::slice::from_ref(self) } else { &[] };
        Self::reserve_many_reusing(std::slice::from_ref(self), &[], memberships, false)
            .map(|mut permits| permits.pop().expect("one group produces one permit"))
    }

    #[cfg(test)]
    pub(crate) fn reserve_many(groups: &[Arc<Self>], descendant: bool) -> Result<Vec<GroupPermit>, AdmissionError> {
        let memberships = if descendant { groups } else { &[] };
        Self::reserve_many_reusing(groups, &[], memberships, false)
    }

    /// Atomically reserves membership in every distinct group.
    ///
    /// Group state locks are acquired in stable `GroupId` order. Every mode,
    /// closure, and capacity check completes before any active or unsettled
    /// count changes; all locks remain held until every count has been
    /// incremented. Single group reservations use this same path, so they
    /// cannot observe a partial multi-group reservation.
    ///
    /// # Parameters
    ///
    /// - `groups`: Group cores whose admission slots are reserved together.
    /// - `parent_memberships`: Groups already containing the submitting parent.
    ///
    /// # Returns
    ///
    /// One permit per distinct group, ordered by group identifier. A permit
    /// reserves an unsettled operation count for every group and reserves
    /// capacity only for groups not reused from the parent.
    ///
    /// # Errors
    ///
    /// Returns the first admission reason if any group rejects the request;
    /// no group is reserved in that case.
    ///
    /// Reserves new groups while borrowing the active slots held by an
    /// accepted parent for groups that parent already owns.
    pub(crate) fn reserve_many_reusing(
        groups: &[Arc<Self>],
        reused: &[Arc<Self>],
        parent_memberships: &[Arc<Self>],
        strict_reuse: bool,
    ) -> Result<Vec<GroupPermit>, AdmissionError> {
        let mut ordered = groups.to_vec();
        ordered.sort_unstable_by_key(|group| group.id);
        ordered.dedup_by_key(|group| group.id);

        let mut states = Vec::with_capacity(ordered.len());
        for group in &ordered {
            states.push(lock(&group.state));
        }
        for (group, state) in ordered.iter().zip(states.iter()) {
            if state.mode == ShutdownMode::Cancel {
                return Err(AdmissionError::Stopped);
            }
            let inherited = reused.iter().any(|existing| existing.id == group.id);
            let parent_member = parent_memberships
                .iter()
                .any(|existing| existing.id == group.id);
            if !state.accepting && (!parent_member || (strict_reuse && !inherited)) {
                return Err(AdmissionError::Closed);
            }
            if !inherited && state.active >= group.capacity {
                return Err(AdmissionError::Busy);
            }
        }
        let mut permits = Vec::new();
        for (group, state) in ordered.iter().zip(states.iter_mut()) {
            let inherited = reused.iter().any(|existing| existing.id == group.id);
            if !inherited {
                state.active += 1;
            }
            state.unsettled_operations += 1;
            permits.push(GroupPermit {
                group: Arc::clone(group),
                capacity_reserved: !inherited,
                settlement_released: false,
            });
        }
        drop(states);
        Ok(permits)
    }

    /// Checks whether a submission can retry group admission without reserving
    /// a slot.
    ///
    /// # Parameters
    ///
    /// - `self`: Group whose current admission state is checked.
    /// - `descendant`: Whether the submission belongs to accepted parent work.
    pub(crate) fn retry_admission(&self, parent_member: bool) -> Result<bool, AdmissionError> {
        let state = lock(&self.state);
        if state.mode == ShutdownMode::Cancel {
            return Err(AdmissionError::Stopped);
        }
        if !state.accepting && !parent_member {
            return Err(AdmissionError::Closed);
        }
        Ok(state.active < self.capacity)
    }

    /// Reads the current notification generation before checking admission.
    ///
    /// # Parameters
    ///
    /// - `self`: Group whose notifications are observed.
    pub(crate) fn admission_generation(&self) -> u64 {
        self.notify.generation()
    }

    /// Waits for a group notification newer than `generation`.
    ///
    /// # Parameters
    ///
    /// - `self`: Group whose notifications are observed.
    /// - `generation`: Previously sampled notification generation.
    pub(crate) fn admission_notified_since(&self, generation: u64) -> crate::v02::internal::notify::Notified {
        self.notify.notified_since(generation)
    }

    /// Stops new root submissions while preserving accepted descendants.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose root admission is closed.
    pub(in crate::v02::manager::group) fn close(&self) {
        lock(&self.state).accepting = false;
        self.changed.notify_all();
        self.notify.notify_waiters();
    }

    /// Atomically registers an operation in every distinct group.
    ///
    /// Group state locks use the same canonical `GroupId` order as admission.
    /// All groups are validated before scope and queue entries are inserted.
    /// The parent link is committed under the same group locks. If
    /// `commit_parent` rejects the child, every insertion is rolled back before
    /// any group can observe it or request cancellation. Cancellation racing
    /// this transaction therefore observes either every membership or none.
    ///
    /// # Type Parameters
    ///
    /// * `F` — callback that commits the child link after group validation.
    ///
    /// # Parameters
    ///
    /// * `groups` — groups that must register the operation atomically.
    /// * `id` — identifier shared by the operation's group memberships.
    /// * `scope` — weak reference to the operation scope to register.
    /// * `descendant` — whether closed groups still admit this descendant.
    /// * `commit_parent` — callback that commits the parent link.
    ///
    /// # Returns
    ///
    /// `Ok(())` after all memberships and the parent link commit, or the
    /// admission error that prevented the transaction.
    #[cfg(test)]
    pub(crate) fn register_scope_many_with<F>(
        groups: &[Arc<Self>],
        id: OperationId,
        scope: Weak<crate::v02::scope::ScopeCore>,
        descendant: bool,
        commit_parent: F,
    ) -> Result<(), AdmissionError>
    where
        F: FnOnce() -> Result<(), AdmissionError>,
    {
        let memberships = if descendant { groups } else { &[] };
        Self::register_scope_many_with_claimed(
            groups,
            id,
            scope,
            memberships,
            false,
            commit_parent,
        )
    }

    /// Registers an adapter claim that completed during the final admission
    /// preflight. A concurrent Cancel transition must produce a stopped
    /// accepted operation instead of returning an already claimed Arc.
    ///
    /// # Type Parameters
    ///
    /// * `F` — callback that commits the child link after group validation.
    ///
    /// # Parameters
    ///
    /// * `groups` — groups that must register the operation atomically.
    /// * `id` — identifier shared by the operation's group memberships.
    /// * `scope` — weak reference to the operation scope to register.
    /// * `parent_memberships` — groups already containing the submitting parent.
    /// * `claimed` — whether final admission preflight already claimed it.
    /// * `commit_parent` — callback that commits the parent link.
    ///
    /// # Returns
    ///
    /// `Ok(())` after all memberships and the parent link commit, or the
    /// admission error that prevented the transaction.
    pub(crate) fn register_scope_many_with_claimed<F>(
        groups: &[Arc<Self>],
        id: OperationId,
        scope: Weak<crate::v02::scope::ScopeCore>,
        parent_memberships: &[Arc<Self>],
        claimed: bool,
        commit_parent: F,
    ) -> Result<(), AdmissionError>
    where
        F: FnOnce() -> Result<(), AdmissionError>,
    {
        let mut ordered = groups.to_vec();
        ordered.sort_unstable_by_key(|group| group.id);
        ordered.dedup_by_key(|group| group.id);
        let mut states = Vec::with_capacity(ordered.len());
        for group in &ordered {
            states.push(lock(&group.state));
        }
        for (group, state) in ordered.iter().zip(states.iter()) {
            if state.mode == ShutdownMode::Cancel && !claimed {
                return Err(AdmissionError::Stopped);
            }
            let parent_member = parent_memberships
                .iter()
                .any(|existing| existing.id == group.id);
            if !state.accepting && !parent_member && !claimed {
                return Err(AdmissionError::Closed);
            }
        }
        for state in &mut states {
            state.scopes.insert(id, scope.clone());
            state.queued.insert(id);
        }
        if let Err(reason) = commit_parent() {
            for state in &mut states {
                state.scopes.remove(&id);
                state.queued.remove(&id);
            }
            return Err(reason);
        }
        Ok(())
    }

    /// Registers an operation scope and marks its operation as queued.
    ///
    /// # Parameters
    ///
    /// - `id`: Identifier of the operation represented by the scope.
    /// - `scope`: Weak reference used to request cancellation during shutdown.
    /// - `self`: Group core receiving the scope registration.
    ///
    /// # Returns
    ///
    /// `true` if registration succeeded; cancellation mode rejects it.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn register_scope(&self, id: OperationId, scope: Weak<crate::v02::scope::ScopeCore>) -> bool {
        let mut state = lock(&self.state);
        if state.mode == ShutdownMode::Cancel {
            return false;
        }
        state.scopes.insert(id, scope);
        state.queued.insert(id);
        true
    }

    /// Removes an operation from the queued set when its execution begins.
    ///
    /// # Parameters
    ///
    /// - `id`: Identifier of the operation that started executing.
    /// - `self`: Group core whose queued marker is removed.
    pub(crate) fn execution_started(&self, id: OperationId) {
        lock(&self.state).queued.remove(&id);
    }

    /// Removes an operation scope after its settlement has finished.
    ///
    /// # Parameters
    ///
    /// - `id`: Identifier of the settled operation.
    /// - `self`: Group core whose settled scope is removed.
    pub(crate) fn settlement_finished(&self, id: OperationId) {
        lock(&self.state).scopes.remove(&id);
    }

    /// Returns the number of admitted group submissions that have not settled.
    ///
    /// This includes reservations that have not yet been registered in the
    /// scope map, such as an external submission paused in its bind preflight.
    ///
    /// # Parameters
    ///
    /// - `self`: Group whose unsettled operation reservations are counted.
    ///
    /// # Returns
    ///
    /// The number of group operations whose settlement has not completed.
    #[must_use]
    pub(crate) fn unsettled_operation_count(&self) -> usize {
        lock(&self.state).unsettled_operations
    }

    /// Removes an operation scope and its queued marker after failed setup.
    ///
    /// # Parameters
    ///
    /// - `id`: Identifier of the operation to unregister.
    /// - `self`: Group core whose operation registration is removed.
    #[cfg(test)]
    pub(crate) fn unregister_scope(&self, id: OperationId) {
        let mut state = lock(&self.state);
        state.scopes.remove(&id);
        state.queued.remove(&id);
    }

    /// Stops admission and propagates cancellation to registered work if
    /// requested.
    ///
    /// # Parameters
    ///
    /// - `mode`: Drain accepted work or request cancellation.
    /// - `self`: Group core whose admission and tracked work are shut down.
    pub(in crate::v02::manager::group) fn begin_shutdown(&self, mode: ShutdownMode) {
        let (scopes, services) = {
            let mut state = lock(&self.state);
            state.accepting = false;
            if mode == ShutdownMode::Cancel {
                state.mode = mode;
                (
                    state.scopes.values().filter_map(Weak::upgrade).collect::<Vec<_>>(),
                    state
                        .services
                        .values()
                        .map(crate::v02::resource::ResourceScope::internal_clone)
                        .collect::<Vec<_>>(),
                )
            } else {
                (
                    Vec::new(),
                    state
                        .services
                        .values()
                        .map(crate::v02::resource::ResourceScope::internal_clone)
                        .collect::<Vec<_>>(),
                )
            }
        };
        for service in services {
            service.end();
        }
        for scope in scopes {
            scope.request_stop(StopReason::ManagerShutdown);
        }
        self.changed.notify_all();
        self.notify.notify_waiters();
    }

    /// Blocks until operations, service scopes, and recoveries are all
    /// finished.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose tracked state is awaited.
    pub(crate) fn wait_blocking(&self) {
        let mut state = lock(&self.state);
        while state.active != 0 || !state.services.is_empty() || !state.recoveries.is_empty() {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Asynchronously waits until operations, services, and recoveries are
    /// finished.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose tracked state is awaited.
    pub(crate) async fn wait(&self) {
        loop {
            let generation = self.notify.generation();
            {
                let state = lock(&self.state);
                if state.active == 0 && state.services.is_empty() && state.recoveries.is_empty() {
                    return;
                }
            }
            self.notify.notified_since(generation).await;
        }
    }

    /// Returns the shutdown mode currently recorded for the group.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose shutdown mode is returned.
    ///
    /// # Returns
    ///
    /// The current drain or cancel mode.
    #[must_use]
    #[inline]
    pub(crate) fn mode(&self) -> ShutdownMode {
        lock(&self.state).mode
    }

    /// Reports whether all tracked operations, services, and recoveries are
    /// finished.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose tracked work is checked.
    ///
    /// # Returns
    ///
    /// `true` when the group has no active work or tracked resources.
    #[must_use]
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        let state = lock(&self.state);
        state.active == 0 && state.services.is_empty() && state.recoveries.is_empty()
    }

    /// Waits up to `timeout` for all tracked group work to finish.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Maximum duration to wait.
    /// - `self`: Group core whose tracked work is awaited.
    ///
    /// # Returns
    ///
    /// `true` if the group becomes empty before the timeout expires.
    #[must_use]
    pub(crate) fn wait_for(&self, timeout: std::time::Duration) -> bool {
        let until = std::time::Instant::now().checked_add(timeout);
        let mut state = lock(&self.state);
        while state.active != 0 || !state.services.is_empty() || !state.recoveries.is_empty() {
            if let Some(until) = until {
                let now = std::time::Instant::now();
                if now >= until {
                    return false;
                }
                let (next, result) = self
                    .changed
                    .wait_timeout(state, until.saturating_duration_since(now))
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state = next;
                if result.timed_out()
                    && (state.active != 0 || !state.services.is_empty() || !state.recoveries.is_empty())
                {
                    return false;
                }
            } else {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
        true
    }

    /// Builds a shutdown snapshot from the current group state.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose shutdown progress is summarized.
    ///
    /// # Returns
    ///
    /// Counts of active, queued, cleaning, service, and recovery work.
    #[must_use = "use this snapshot to evaluate the group's shutdown progress"]
    pub(crate) fn shutdown_snapshot(&self) -> ShutdownSnapshot {
        let (recovery_count, unconfirmed_count) = {
            let handles = {
                let state = lock(&self.state);
                state.recoveries.values().cloned().collect::<Vec<_>>()
            };
            let unconfirmed = handles
                .iter()
                .filter(|handle| handle.snapshot().phase == RecoveryPhase::Unconfirmed)
                .count();
            (handles.len(), unconfirmed)
        };
        let active_operations = self.snapshot().1;
        let queued_operations = lock(&self.state).queued.len();
        let cleaning_resources = self.cleaning_count();
        let service_resources = self.service_count();
        crate::v02::report::ShutdownSnapshot {
            active_operations,
            queued_operations,
            unconfirmed_users: unconfirmed_count,
            cleaning_resources,
            recovery_groups: recovery_count,
            service_resources,
        }
    }

    /// Clones the currently unresolved recovery handles owned by this group.
    ///
    /// The group ledger is keyed by recovery identity, so the result is
    /// identity-deduplicated and is not limited by per-operation diagnostic
    /// retention.
    #[must_use]
    pub(crate) fn recovery_handles_snapshot(&self) -> Vec<RecoveryHandle> {
        lock(&self.state).recoveries.values().cloned().collect()
    }

    /// Returns whether admission is open and how many operations are active.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose local admission state is read.
    fn snapshot(&self) -> (bool, usize) {
        let state = lock(&self.state);
        (state.accepting, state.active)
    }

    /// Builds the public snapshot for this group.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose public state is summarized.
    ///
    /// # Returns
    ///
    /// The group identifier, admission state, active count, and service-scope
    /// count.
    #[must_use]
    pub(in crate::v02::manager::group) fn public_snapshot(&self) -> GroupSnapshot {
        let (accepting, active_operations) = self.snapshot();
        let service_resource_scopes = lock(&self.state).services.len();
        GroupSnapshot {
            id: self.id,
            accepting,
            active_operations,
            service_resource_scopes,
        }
    }

    /// Returns the number of registered service resource scopes.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose service scopes are counted.
    fn service_count(&self) -> usize {
        lock(&self.state).services.len()
    }

    /// Counts cleanup operations currently in flight across tracked scopes.
    ///
    /// # Parameters
    ///
    /// - `self`: Group core whose tracked scopes are inspected.
    fn cleaning_count(&self) -> usize {
        let (operations, services) = {
            let state = lock(&self.state);
            (
                state.scopes.values().filter_map(Weak::upgrade).collect::<Vec<_>>(),
                state
                    .services
                    .values()
                    .map(crate::v02::resource::ResourceScope::internal_clone)
                    .collect::<Vec<_>>(),
            )
        };
        operations.iter().map(|scope| scope.cleanup_in_flight()).sum::<usize>()
            + services
                .iter()
                .map(crate::v02::resource::ResourceScope::cleanup_in_flight)
                .sum::<usize>()
    }

    /// Registers a service resource scope while group admission remains open.
    ///
    /// # Parameters
    ///
    /// - `scope`: Service scope retained until it ends.
    /// - `self`: Group core receiving the service scope.
    ///
    /// # Returns
    ///
    /// `true` if registered; returns `false` after admission has closed.
    #[must_use]
    pub(crate) fn register_service(&self, scope: crate::v02::resource::ResourceScope) -> bool {
        let mut state = lock(&self.state);
        if !state.accepting {
            return false;
        }
        state.services.insert(scope.id(), scope);
        true
    }

    /// Removes a completed service scope and wakes shutdown waiters.
    ///
    /// # Parameters
    ///
    /// - `id`: Identifier of the service scope that finished.
    /// - `self`: Group core removing the completed service scope.
    pub(crate) fn service_finished(&self, id: ResourceScopeId) {
        let removed = lock(&self.state).services.remove(&id);
        drop(removed);
        self.changed.notify_all();
        self.notify.notify_waiters();
    }

    /// Adds or removes a recovery handle from the group shutdown accounting.
    ///
    /// # Parameters
    ///
    /// - `handle`: Recovery handle whose active state changed.
    /// - `active`: Whether the handle should remain counted as active recovery.
    /// - `self`: Group core whose recovery accounting is updated.
    pub(crate) fn recovery_changed(&self, handle: crate::v02::resource::RecoveryHandle, active: bool) {
        let removed = {
            let mut state = lock(&self.state);
            if active {
                state.recoveries.insert(handle.id(), handle)
            } else {
                state.recoveries.remove(&handle.id())
            }
        };
        drop(removed);
        self.changed.notify_all();
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Weak;
    use std::time::Duration;

    use super::GroupCore;
    use crate::v02::report::AdmissionError;
    use crate::v02::report::OperationId;
    use crate::v02::report::ShutdownMode;

    #[test]
    fn test_reservation_observes_capacity_close_and_cancel_transitions() {
        let group = GroupCore::new(1);
        let held = group.reserve(false).unwrap();
        assert!(matches!(group.reserve(false), Err(AdmissionError::Busy)));
        group.close();
        assert!(!group.public_snapshot().accepting);
        assert!(matches!(group.reserve(false), Err(AdmissionError::Closed)));
        assert!(matches!(group.reserve(true), Err(AdmissionError::Busy)));
        drop(held);
        let descendant = group.reserve(true).unwrap();
        drop(descendant);

        group.begin_shutdown(ShutdownMode::Cancel);
        assert_eq!(group.mode(), ShutdownMode::Cancel);
        assert!(matches!(group.reserve(true), Err(AdmissionError::Stopped)));
    }

    #[test]
    fn test_failed_later_reservation_does_not_expose_earlier_capacity() {
        use std::sync::mpsc;
        use std::time::Instant;

        let one = GroupCore::new(1);
        let two = GroupCore::new(1);
        let (first, later) = if one.id < two.id { (one, two) } else { (two, one) };
        let _later_full = GroupCore::reserve_many(std::slice::from_ref(&later), false).unwrap();
        let later_state = later.state.lock().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let first_for_worker = Arc::clone(&first);
        let later_for_worker = Arc::clone(&later);
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            GroupCore::reserve_many(&[first_for_worker, later_for_worker], false)
        });
        started_rx.recv().unwrap();

        // The batch holds the earlier mutex while blocked on the later one.
        // This directly verifies that no observer can read a partial count.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match first.state.try_lock() {
                Err(std::sync::TryLockError::WouldBlock) => break,
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    panic!("group mutex poisoned: {error}")
                }
                Ok(state) => {
                    drop(state);
                    assert!(
                        Instant::now() < deadline,
                        "batch did not acquire the earlier group lock"
                    );
                    std::thread::yield_now();
                }
            }
        }
        drop(later_state);
        assert!(matches!(worker.join().unwrap(), Err(AdmissionError::Busy)));
        assert_eq!(first.public_snapshot().active_operations, 0);
        assert_eq!(later.public_snapshot().active_operations, 1);
        let permit = GroupCore::reserve_many(std::slice::from_ref(&first), false).unwrap();
        assert_eq!(first.public_snapshot().active_operations, 1);
        drop(permit);
    }

    #[test]
    fn test_closed_group_admission_requires_parent_membership_per_group() {
        let parent_group = GroupCore::new(2);
        let external_group = GroupCore::new(2);
        external_group.close();

        assert!(matches!(
            GroupCore::reserve_many_reusing(
                &[Arc::clone(&parent_group), Arc::clone(&external_group)],
                &[],
                std::slice::from_ref(&parent_group),
                false,
            ),
            Err(AdmissionError::Closed)
        ));
        assert_eq!(parent_group.public_snapshot().active_operations, 0);
        assert_eq!(parent_group.unsettled_operation_count(), 0);

        let permits = GroupCore::reserve_many_reusing(
            std::slice::from_ref(&external_group),
            &[],
            std::slice::from_ref(&external_group),
            false,
        )
        .expect("a true member of the closed group may reserve a descendant");
        assert_eq!(external_group.unsettled_operation_count(), 1);
        drop(permits);
    }

    #[test]
    fn test_registration_rechecks_closed_groups_atomically() {
        let open_group = GroupCore::new(2);
        let closing_group = GroupCore::new(2);
        let unrelated_parent_group = GroupCore::new(2);
        let groups = [Arc::clone(&open_group), Arc::clone(&closing_group)];
        let permits = GroupCore::reserve_many_reusing(&groups, &[], &[], false).unwrap();
        closing_group.close();

        assert!(matches!(
            GroupCore::register_scope_many_with_claimed(
                &groups,
                OperationId::new_unique(),
                Weak::new(),
                std::slice::from_ref(&unrelated_parent_group),
                false,
                || Ok(()),
            ),
            Err(AdmissionError::Closed)
        ));
        assert_eq!(open_group.shutdown_snapshot().queued_operations, 0);
        assert_eq!(closing_group.shutdown_snapshot().queued_operations, 0);
        assert_eq!(open_group.unsettled_operation_count(), 1);
        assert_eq!(closing_group.unsettled_operation_count(), 1);

        GroupCore::register_scope_many_with_claimed(
            &groups,
            OperationId::new_unique(),
            Weak::new(),
            &[],
            true,
            || Ok(()),
        )
        .expect("a pre-close reservation with a provisional claim remains admissible");
        assert_eq!(open_group.shutdown_snapshot().queued_operations, 1);
        assert_eq!(closing_group.shutdown_snapshot().queued_operations, 1);
        drop(permits);
    }

    #[test]
    fn test_canceled_later_group_prevents_partial_scope_registration() {
        let first = GroupCore::new(1);
        let later = GroupCore::new(1);
        let (first, later) = if first.id < later.id {
            (first, later)
        } else {
            (later, first)
        };
        let permits = GroupCore::reserve_many(&[Arc::clone(&first), Arc::clone(&later)], false).unwrap();
        later.begin_shutdown(ShutdownMode::Cancel);
        let operation = OperationId::new_unique();

        assert!(matches!(
            GroupCore::register_scope_many_with(
                &[Arc::clone(&first), Arc::clone(&later)],
                operation,
                Weak::new(),
                false,
                || Ok(())
            ),
            Err(AdmissionError::Stopped)
        ));
        assert_eq!(first.shutdown_snapshot().queued_operations, 0);
        assert_eq!(later.shutdown_snapshot().queued_operations, 0);
        assert_eq!(first.public_snapshot().active_operations, 1);
        let claimed_operation = OperationId::new_unique();
        GroupCore::register_scope_many_with_claimed(
            &[Arc::clone(&first), Arc::clone(&later)],
            claimed_operation,
            Weak::new(),
            &[],
            true,
            || Ok(()),
        )
        .expect("a final-preflight claim is registered for stopped completion");
        assert_eq!(first.shutdown_snapshot().queued_operations, 1);
        assert_eq!(later.shutdown_snapshot().queued_operations, 1);
        drop(permits);
        assert_eq!(first.public_snapshot().active_operations, 0);
        assert_eq!(later.public_snapshot().active_operations, 0);

        let rejected_first = GroupCore::new(1);
        let rejected_later = GroupCore::new(1);
        let rejected_id = OperationId::new_unique();
        let result = GroupCore::register_scope_many_with(
            &[Arc::clone(&rejected_first), Arc::clone(&rejected_later)],
            rejected_id,
            Weak::new(),
            false,
            || {
                assert!(matches!(
                    rejected_first.state.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ));
                Err(AdmissionError::ScopeEnded)
            },
        );
        assert!(matches!(result, Err(AdmissionError::ScopeEnded)));
        assert_eq!(rejected_first.shutdown_snapshot().queued_operations, 0);
        assert_eq!(rejected_later.shutdown_snapshot().queued_operations, 0);

        let registered_first = GroupCore::new(1);
        let registered_later = GroupCore::new(1);
        let operation = OperationId::new_unique();
        GroupCore::register_scope_many_with(
            &[Arc::clone(&registered_first), Arc::clone(&registered_later)],
            operation,
            Weak::new(),
            false,
            || Ok(()),
        )
        .unwrap();
        registered_later.begin_shutdown(ShutdownMode::Cancel);
        assert_eq!(registered_first.shutdown_snapshot().queued_operations, 1);
        assert_eq!(registered_later.shutdown_snapshot().queued_operations, 1);
    }

    #[test]
    fn test_scope_registration_queue_updates_and_shutdown_waits() {
        let group = GroupCore::new(2);
        let first = OperationId::new_unique();
        let second = OperationId::new_unique();
        assert!(group.register_scope(first, Weak::new()));
        assert_eq!(group.shutdown_snapshot().queued_operations, 1);
        group.execution_started(first);
        assert_eq!(group.shutdown_snapshot().queued_operations, 0);
        assert!(group.register_scope(second, Weak::new()));
        group.unregister_scope(second);
        assert_eq!(group.shutdown_snapshot().queued_operations, 0);

        group.begin_shutdown(ShutdownMode::Cancel);
        assert!(!group.register_scope(OperationId::new_unique(), Weak::new()));
        assert!(group.is_empty());
        assert!(group.wait_for(Duration::ZERO));
        group.wait_blocking();
    }

    #[test]
    fn test_wait_for_reports_timeout_while_reserved_work_remains() {
        let group = GroupCore::new(1);
        let _permit = group.reserve(false).unwrap();
        assert!(!group.is_empty());
        assert!(!group.wait_for(Duration::ZERO));
        assert_eq!(group.shutdown_snapshot().active_operations, 1);
    }
}

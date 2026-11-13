// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::task::Waker;

use super::OperationSupervisor;
use super::internal::SharedServiceGroupState;
use super::internal::ShutdownSource;
use super::internal::util::lock;
use crate::context::ContextError;

/// A bounded shutdown waiter that unregisters when its observation ends.
pub(super) struct ShutdownWaitRegistration {
    /// Identifies the supervisor or service-group waiter registry to update.
    source: ShutdownSource,
    /// Stable registry key used to replace or remove this waiter's waker.
    waiter_id: Option<u64>,
}

impl ShutdownWaitRegistration {
    /// Creates an unregistered waiter for the selected shutdown source.
    ///
    /// # Parameters
    ///
    /// * `source` - Shared shutdown state whose bounded waiter list will track
    ///   this observer.
    ///
    /// # Returns
    ///
    /// A registration that owns no waiter slot until `register` succeeds.
    #[inline]
    pub(super) fn new(source: ShutdownSource) -> Self {
        Self {
            source,
            waiter_id: None,
        }
    }

    /// Registers or refreshes this observer's task waker in the selected
    /// registry.
    ///
    /// A successful call retains a stable waiter ID for later replacement and
    /// cleanup. The registry lock is released before any replaced waker is
    /// dropped.
    ///
    /// # Parameters
    ///
    /// * `waker` - Task waker to store or use to decide whether the stored
    ///   waker is still current.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after storing or retaining the waker for this observer.
    ///
    /// # Errors
    ///
    /// Returns `WaiterCapacityExceeded` if this observer needs a new slot and
    /// the registry is full.
    pub(super) fn register(&mut self, waker: &Waker) -> Result<(), ContextError> {
        let replacement = waker.clone();
        match self.source.clone() {
            ShutdownSource::Supervisor(supervisor) => self.register_supervisor(&supervisor, waker, replacement),
            ShutdownSource::Service(state, _) => {
                let guard = lock(&state.0);
                let capacity = guard.limit;
                drop(guard);
                // The state lock is acquired below so Waker replacement/drop stays outside it.
                self.register_service(&state, waker, replacement, capacity)
            }
        }
    }

    /// Removes this waiter's registration if it currently owns a registry slot.
    ///
    /// The ID is cleared before registry access, making repeated calls
    /// harmless. Any removed waker is dropped after releasing the registry
    /// lock.
    pub(super) fn unregister(&mut self) {
        let Some(id) = self.waiter_id.take() else {
            return;
        };
        let removed = match &self.source {
            ShutdownSource::Supervisor(supervisor) => {
                let mut state = lock(&supervisor.inner.state);
                state
                    .shutdown_waiters
                    .iter()
                    .position(|(waiter_id, _)| *waiter_id == id)
                    .map(|position| state.shutdown_waiters.remove(position).1)
            }
            ShutdownSource::Service(state, _) => {
                let mut guard = lock(&state.0);
                guard
                    .shutdown_waiters
                    .iter()
                    .position(|(waiter_id, _)| *waiter_id == id)
                    .map(|position| guard.shutdown_waiters.remove(position).1)
            }
        };
        drop(removed);
    }

    /// Inserts or replaces this waiter's waker in the supervisor registry.
    ///
    /// Any waker removed from the registry is dropped after releasing the state
    /// lock. A full registry rejects new IDs while allowing existing IDs to
    /// refresh.
    ///
    /// # Parameters
    ///
    /// * `supervisor` - Supervisor whose bounded shutdown waiter registry owns
    ///   this registration.
    /// * `waker` - Current task waker used to detect whether the stored waker
    ///   is still current.
    /// * `replacement` - Cloned waker installed when the stored waker no longer
    ///   wakes this task.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the waker is stored or the existing waiter entry is
    /// refreshed.
    ///
    /// # Errors
    ///
    /// Returns `WaiterCapacityExceeded` when a new waiter ID is needed but the
    /// registry is full.
    fn register_supervisor(
        &mut self,
        supervisor: &OperationSupervisor,
        waker: &Waker,
        replacement: Waker,
    ) -> Result<(), ContextError> {
        let capacity = supervisor.inner.limits.admission_waiters;
        let id = self.waiter_id.unwrap_or_else(|| {
            let mut state = lock(&supervisor.inner.state);
            let used = state.shutdown_waiters.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            allocate_waiter_id(&mut state.next_shutdown_waiter, &used)
        });
        let (old, result) = {
            let mut state = lock(&supervisor.inner.state);
            update_waiter(&mut state.shutdown_waiters, id, capacity, waker, replacement)
        };
        drop(old);
        if result.is_ok() {
            self.waiter_id = Some(id);
        }
        result
    }

    /// Inserts or replaces this waiter's waker in a service-group registry.
    ///
    /// Existing waiter IDs may refresh even at capacity. A newly allocated ID
    /// is rejected when the registry has reached the service group's
    /// configured limit.
    ///
    /// # Parameters
    ///
    /// * `state` - Service-group state whose bounded shutdown waiter registry
    ///   is updated.
    /// * `waker` - Current task waker used to detect whether the stored waker
    ///   is still current.
    /// * `replacement` - Cloned waker installed when the stored waker no longer
    ///   wakes this task.
    /// * `capacity` - Maximum number of waiter IDs permitted in this registry.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the waker is stored or the existing waiter entry is
    /// refreshed.
    ///
    /// # Errors
    ///
    /// Returns `WaiterCapacityExceeded` when a new waiter ID is needed but the
    /// registry is full.
    fn register_service(
        &mut self,
        state: &SharedServiceGroupState,
        waker: &Waker,
        replacement: Waker,
        capacity: usize,
    ) -> Result<(), ContextError> {
        let id = self.waiter_id.unwrap_or_else(|| {
            let mut guard = lock(&state.0);
            let used = guard.shutdown_waiters.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            allocate_waiter_id(&mut guard.next_shutdown_waiter, &used)
        });
        let (old, result) = {
            let mut guard = lock(&state.0);
            update_waiter(&mut guard.shutdown_waiters, id, capacity, waker, replacement)
        };
        drop(old);
        if result.is_ok() {
            self.waiter_id = Some(id);
        }
        result
    }
}

/// Updates a waiter entry while its owning registry lock is held.
///
/// Existing IDs may refresh regardless of capacity. New IDs are appended only
/// while the registry remains below its bound; callers receive any replaced
/// waker so they can drop it after releasing the lock.
///
/// # Parameters
///
/// * `waiters` - Waiter entries belonging to the registry whose lock the caller
///   holds.
/// * `id` - Stable nonzero identifier to refresh or insert.
/// * `capacity` - Maximum number of waiter entries permitted in the registry.
/// * `waker` - Current task waker used to avoid replacing an equivalent stored
///   waker.
/// * `replacement` - Cloned waker to store if an entry must be inserted or
///   refreshed.
///
/// # Returns
///
/// The removed waker and the registration result. `Some(waker)` means a
/// different stored waker was replaced and must be dropped after releasing the
/// registry lock; `None` means no waker was replaced.
///
/// # Errors
///
/// The registration result is `WaiterCapacityExceeded` when a new ID would
/// exceed `capacity`.
fn update_waiter(
    waiters: &mut Vec<(u64, Waker)>,
    id: u64,
    capacity: usize,
    waker: &Waker,
    replacement: Waker,
) -> (Option<Waker>, Result<(), ContextError>) {
    let position = waiters.iter().position(|(old_id, _)| *old_id == id);
    if position.is_none() && waiters.len() >= capacity {
        (None, Err(ContextError::WaiterCapacityExceeded))
    } else if let Some(position) = position {
        let old = if waiters[position].1.will_wake(waker) {
            None
        } else {
            Some(std::mem::replace(&mut waiters[position].1, replacement))
        };
        (old, Ok(()))
    } else {
        waiters.push((id, replacement));
        (None, Ok(()))
    }
}

/// Chooses a nonzero waiter ID not currently present in a registry.
///
/// The counter wraps while skipping zero. The supplied IDs are a snapshot of
/// the registry being updated, so callers must collect them while holding that
/// registry's state lock.
///
/// # Parameters
///
/// * `next` - Registry counter to advance after selecting an available ID.
/// * `waiters` - IDs currently reserved in that registry.
///
/// # Returns
///
/// An unused nonzero ID, with `next` advanced to the following nonzero
/// candidate.
fn allocate_waiter_id(next: &mut u64, waiters: &[u64]) -> u64 {
    let mut candidate = (*next).max(1);
    while waiters.contains(&candidate) {
        candidate = candidate.wrapping_add(1).max(1);
    }
    *next = candidate.wrapping_add(1).max(1);
    candidate
}

impl Drop for ShutdownWaitRegistration {
    /// Releases the registry slot if this registration is dropped before
    /// explicit cleanup.
    fn drop(&mut self) {
        self.unregister();
    }
}

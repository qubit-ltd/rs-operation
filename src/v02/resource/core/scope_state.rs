// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use super::ScopeCompletion;
use super::internal::State;
use super::internal::lock;
use super::scope_core::ScopeCore;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::report::ManagerId;
use crate::v02::report::ResourceProgress;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ResourceScopeReport;
#[cfg(test)]
use crate::v02::report::TransferReceipt;
use crate::v02::resource::cleanup_port::CleanupDispatch;
use crate::v02::resource::recovery::RecoveryHandle;

/// Synchronization and ownership state for one independently endable scope.
pub(in crate::v02::resource) struct ScopeState {
    /// Shared admission state and resource-owner slots for this scope.
    pub(in crate::v02::resource) core: Arc<ScopeCore>,
    /// Aggregates cleanup outcomes and publishes scope settlement.
    completion: Arc<ScopeCompletion>,
}
impl ScopeState {
    /// Creates an open scope with fresh admission and completion state.
    ///
    /// # Parameters
    ///
    /// * `manager` — identity of the manager that owns this scope.
    /// * `id` — stable identity assigned to the new scope.
    /// * `capacity` — shared limits and admission pools used by the scope.
    /// * `cleanup` — dispatcher that schedules resource cleanup jobs.
    ///
    /// # Returns
    ///
    /// A shared scope state that can be cloned by internal resource handles.
    #[must_use]
    pub(in crate::v02::resource) fn new(
        manager: ManagerId,
        id: ResourceScopeId,
        capacity: Arc<ScopeCapacity>,
        cleanup: Arc<dyn CleanupDispatch>,
    ) -> Arc<Self> {
        let completion = ScopeCompletion::new();
        Arc::new(Self {
            core: Arc::new(ScopeCore {
                manager,
                id,
                capacity,
                cleanup,
                completion: Arc::clone(&completion),
                state: Mutex::new(State {
                    open: true,
                    generation: 1,
                    reservations: 0,
                    retained: 0,
                    slots: HashMap::new(),
                }),
            }),
            completion,
        })
    }

    /// Installs the one-shot callback invoked when all scope owners settle.
    ///
    /// If completion already occurred, the callback runs immediately after
    /// the completion lock is released.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose final report is observed.
    /// * `callback` — consumer of the completed resource-scope report.
    #[inline]
    pub(in crate::v02::resource) fn set_settlement_callback(
        &self,
        callback: Box<dyn FnOnce(ResourceScopeReport) + Send + 'static>,
    ) {
        self.completion.set_callback(callback);
    }

    /// Registers a callback for changes in aggregate cleanup progress.
    ///
    /// It receives the current phase immediately, then later monotonic updates.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose progress is observed.
    /// * `callback` — thread-safe listener for resource cleanup progress.
    #[inline]
    pub(in crate::v02::resource) fn set_progress_callback(
        &self,
        callback: Arc<dyn Fn(ResourceProgress) + Send + Sync + 'static>,
    ) {
        self.completion.set_progress_callback(callback);
    }

    /// Registers a listener for recovery attempts attached to this scope.
    ///
    /// Existing recoveries are reported when the callback is installed; later
    /// recovery state changes are forwarded as they occur.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose recovery state is observed.
    /// * `callback` — listener receiving each recovery handle and presence
    ///   state.
    #[inline]
    pub(in crate::v02::resource) fn set_recovery_callback(
        &self,
        callback: Arc<dyn Fn(RecoveryHandle, bool) + Send + Sync + 'static>,
    ) {
        self.completion.set_recovery_callback(callback);
    }

    /// Adds transfer evidence while the scope is still accepting transfers.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope collecting transfer outcomes.
    /// * `receipt` — receipt to include in the eventual settlement report.
    ///
    /// # Returns
    ///
    /// `true` if the receipt was accepted before sealing, or `false` after
    /// settlement has sealed the owner count.
    #[cfg(test)]
    #[must_use]
    pub(in crate::v02::resource) fn record_transfer(&self, receipt: TransferReceipt) -> bool {
        self.completion.record_transfer(receipt)
    }

    /// Reports how many registered owners are currently cleaning resources.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose active cleanup count is read.
    ///
    /// # Returns
    ///
    /// The number of started but not yet completed resource cleanups.
    #[must_use]
    #[inline]
    pub(in crate::v02::resource) fn cleanup_in_flight(&self) -> usize {
        self.completion.cleaning()
    }

    /// Closes admission and begins cleanup for every resource owner in scope.
    ///
    /// The operation is idempotent. It seals the expected owner count before
    /// ending owners so settlement waits for outstanding reservations and
    /// resource users to report their final outcomes.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose admission and resource owners are closed.
    pub(in crate::v02::resource) fn end(&self) {
        let (slots, expected, reservations) = {
            let mut state = lock(&self.core.state);
            if !state.open {
                return;
            }
            state.open = false;
            state.generation = state.generation.wrapping_add(1);
            let expected = state
                .slots
                .len()
                .saturating_add(state.reservations)
                .saturating_add(state.retained);
            (std::mem::take(&mut state.slots), expected, state.reservations)
        };
        let waiting_for_users = reservations > 0 || slots.values().any(|entry| entry.owner.has_users());
        if waiting_for_users {
            self.completion.waiting_for_users();
        }
        self.completion.seal(expected);
        for entry in slots.into_values() {
            entry.owner.end();
        }
    }
}

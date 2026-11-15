// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

use super::super::CapacityPermit;
use super::super::PermitBundle;
use super::super::SubmissionLease;
use super::super::lock;
use super::lease_state::LeaseState;

/// Holds a production operation's permits until their lifecycle release points.
///
/// Settlement releases execution capacity, outcome delivery releases the
/// outcome slot, and child-path capacity waits for both events.
pub(super) struct ProductionLease {
    /// Lifecycle permits and outcome-delivery facts protected as one state.
    state: Mutex<LeaseState>,
}

impl ProductionLease {
    /// Creates a lease that retains permits until lifecycle facts are reported.
    ///
    /// # Parameters
    ///
    /// - `active_root`: Optional root execution permit to retain.
    /// - `child_path`: Optional hierarchical child permits to retain.
    /// - `outcome`: Outcome-delivery permit to retain.
    ///
    /// # Returns
    ///
    /// A lease initialized with both lifecycle facts unset.
    pub(super) fn new(
        active_root: Option<CapacityPermit>,
        child_path: Option<PermitBundle>,
        outcome: CapacityPermit,
    ) -> Self {
        Self {
            state: Mutex::new(LeaseState {
                active_root,
                child_path,
                outcome: Some(outcome),
                settled: false,
                delivered: false,
            }),
        }
    }
}

impl SubmissionLease for ProductionLease {
    /// Records execution completion without releasing settlement or outcome
    /// capacity.
    ///
    /// # Parameters
    ///
    /// - `self`: Production lease whose execution has finished.
    fn execution_finished(&self) {}

    /// Releases root capacity at settlement and child-path capacity after both
    /// settlement and outcome delivery.
    ///
    /// # Parameters
    ///
    /// - `self`: Production lease whose settlement has finished.
    fn settlement_finished(&self) {
        let (active, child) = {
            let mut state = lock(&self.state);
            state.settled = true;
            let active = state.active_root.take();
            let child = state.delivered.then(|| state.child_path.take()).flatten();
            (active, child)
        };
        drop(active);
        drop(child);
    }

    /// Releases the outcome permit and any already-settled child-path capacity.
    ///
    /// # Parameters
    ///
    /// - `self`: Production lease recording outcome delivery.
    /// - `_delivery`: The outcome delivery fact; the lease only needs the
    ///   event.
    fn outcome_delivered(&self, _delivery: crate::v02::operation::OutcomeDelivery) {
        let (outcome, child) = {
            let mut state = lock(&self.state);
            state.delivered = true;
            let outcome = state.outcome.take();
            let child = state.settled.then(|| state.child_path.take()).flatten();
            (outcome, child)
        };
        drop(outcome);
        drop(child);
    }
}

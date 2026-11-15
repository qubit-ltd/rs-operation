// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;

use super::ActivationCallback;
use super::AdapterError;
use super::AdapterErrorKind;
use super::ChildPhase;
use super::ChildStartPermit;
use super::Lifecycle;
use super::lock;
use crate::v02::protocol::OperationControl;
#[cfg(test)]
use crate::v02::report::StopReason;

/// Attached but not yet committed child. Dropping it abandons admission once.
pub(crate) struct AttachedChild {
    /// Capacity token retained until commit, activation or abandonment.
    pub(super) admission: Option<Box<dyn Any + Send>>,
    /// Parent operation control used to request and observe stopping.
    pub(super) control: Arc<dyn OperationControl>,
    /// External work payload transferred to the activation callback.
    pub(super) payload: Option<Box<dyn Any + Send>>,
    /// Shared lifecycle phase coordinating commit and activation.
    pub(super) lifecycle: Arc<Mutex<Lifecycle>>,
    /// Gate checked before committing the attached child.
    pub(super) commit_gate: Arc<dyn Fn() -> Result<(), AdapterErrorKind> + Send + Sync>,
    /// Gate checked immediately before activating the child.
    pub(super) activation_gate: Arc<dyn Fn() -> Result<(), AdapterErrorKind> + Send + Sync>,
    /// Whether gate outcomes are authoritative for stop acceptance.
    pub(super) gate_is_authoritative: bool,
    /// Callback that starts the child after successful activation.
    pub(super) activate: Option<ActivationCallback>,
    /// Callback that releases admission after rejection or drop.
    pub(super) abandon: Option<Box<dyn FnOnce() + Send>>,
}

impl AttachedChild {
    /// Requests stopping while preserving the one-shot lifecycle state.
    ///
    /// # Parameters
    ///
    /// * `self` is the attached child whose stop state may transition.
    /// * `reason` is forwarded to the parent operation control if accepted.
    #[cfg(test)]
    pub(crate) fn request_stop(&self, reason: StopReason) {
        let accepted = {
            let mut lifecycle = lock(&self.lifecycle);
            if lifecycle.phase == ChildPhase::Abandoned || lifecycle.stop_requested {
                false
            } else {
                lifecycle.stop_requested = true;
                true
            }
        };
        if accepted {
            self.control.request_stop(reason);
        }
    }

    /// Commits admission and returns a permit for the separate activation step.
    ///
    /// # Parameters
    ///
    /// * `self` is the attached child whose admission is being committed.
    ///
    /// # Returns
    ///
    /// A `ChildStartPermit` that can activate the admitted child.
    ///
    /// # Errors
    ///
    /// Returns the child and its admission error if it was already committed
    /// or the commit gate rejects it. Rejected admission is abandoned once.
    pub(crate) fn commit(mut self) -> Result<ChildStartPermit, AdapterError<Box<Self>>> {
        let (can_commit, stop_requested) = {
            let mut lifecycle = lock(&self.lifecycle);
            if lifecycle.phase != ChildPhase::Attached {
                drop(lifecycle);
                return Err(AdapterError {
                    reason: AdapterErrorKind::AlreadyCommitted,
                    payload: Box::new(self),
                });
            }
            lifecycle.phase = ChildPhase::Committing;
            (true, lifecycle.stop_requested)
        };
        debug_assert!(can_commit);
        let gate_result = (self.commit_gate)();
        let rejected = gate_result.err().or_else(|| {
            (!self.gate_is_authoritative && (stop_requested || self.control.snapshot().stop.is_some()))
                .then_some(AdapterErrorKind::Stopped)
        });
        if let Some(reason) = rejected {
            self.abandon_once();
            return Err(AdapterError {
                reason,
                payload: Box::new(self),
            });
        }
        lock(&self.lifecycle).phase = ChildPhase::Committed;
        Ok(ChildStartPermit {
            lifecycle: Arc::clone(&self.lifecycle),
            activation_gate: Arc::clone(&self.activation_gate),
            gate_is_authoritative: self.gate_is_authoritative,
            activate: self.activate.take(),
            abandon: self.abandon.take(),
            admission: self.admission.take(),
            payload: self.payload.take(),
            control: Arc::clone(&self.control),
        })
    }

    /// Performs the abandonment transition at most once.
    ///
    /// # Parameters
    ///
    /// * `self` is the attached child whose uncommitted state is abandoned.
    fn abandon_once(&mut self) {
        let should_abandon = {
            let mut phase = lock(&self.lifecycle);
            if matches!(
                phase.phase,
                ChildPhase::Abandoned | ChildPhase::Activated | ChildPhase::Committed
            ) {
                false
            } else {
                phase.phase = ChildPhase::Abandoned;
                true
            }
        };
        if should_abandon {
            drop(self.payload.take());
            drop(self.admission.take());
            if let Some(abandon) = self.abandon.take() {
                abandon();
            }
        }
    }
}

impl Drop for AttachedChild {
    /// Releases payload and admission when the child was not activated.
    ///
    /// # Parameters
    ///
    /// * `self` is the child state being dropped.
    fn drop(&mut self) {
        self.abandon_once();
    }
}

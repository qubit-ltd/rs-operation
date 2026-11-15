// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::sync::Arc;
use std::sync::Mutex;

use super::ActivationCallback;
use super::AdapterError;
use super::AdapterErrorKind;
use super::ChildPhase;
use super::Lifecycle;
use super::lock;
use crate::v02::protocol::OperationControl;

/// One-shot permission to start an already committed external child.
pub(crate) struct ChildStartPermit {
    /// Shared lifecycle state checked while consuming this permit.
    pub(super) lifecycle: Arc<Mutex<Lifecycle>>,
    /// Final validation performed immediately before starting the child.
    pub(super) activation_gate: Arc<dyn Fn() -> Result<(), AdapterErrorKind> + Send + Sync>,
    /// Whether the activation gate fully determines stop acceptance.
    pub(super) gate_is_authoritative: bool,
    /// Callback that starts the external child on successful activation.
    pub(super) activate: Option<ActivationCallback>,
    /// Releases parent admission if activation is abandoned.
    pub(super) abandon: Option<Box<dyn FnOnce() + Send>>,
    /// Capacity token retained through the activation decision.
    pub(super) admission: Option<Box<dyn Any + Send>>,
    /// Child payload passed to the activation callback.
    pub(super) payload: Option<Box<dyn Any + Send>>,
    /// Parent control used to observe stop requests.
    pub(super) control: Arc<dyn OperationControl>,
}

impl Debug for ChildStartPermit {
    /// Formats the permit without exposing callback or payload internals.
    ///
    /// # Parameters
    ///
    /// * `f` receives the opaque permit representation.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after formatting the permit representation.
    ///
    /// # Errors
    ///
    /// Returns the formatter error if writing the permit representation fails.
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ChildStartPermit").finish_non_exhaustive()
    }
}

impl ChildStartPermit {
    /// Runs the final gate and starts the child exactly once when accepted.
    ///
    /// # Parameters
    ///
    /// * `self` is the committed permit consumed by the activation attempt.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after starting the child.
    ///
    /// # Errors
    ///
    /// Returns the permit and rejection reason if the permit was already
    /// consumed or the activation gate rejected the child.
    pub(crate) fn activate(mut self) -> Result<(), AdapterError<Self>> {
        let stop_requested = {
            let mut lifecycle = lock(&self.lifecycle);
            if lifecycle.phase != ChildPhase::Committed {
                drop(lifecycle);
                return Err(AdapterError {
                    reason: AdapterErrorKind::AlreadyCommitted,
                    payload: self,
                });
            }
            lifecycle.phase = ChildPhase::Activating;
            lifecycle.stop_requested
        };
        let gate_result = (self.activation_gate)();
        let activation_error = gate_result.err().or_else(|| {
            (!self.gate_is_authoritative && (stop_requested || self.control.snapshot().stop.is_some()))
                .then_some(AdapterErrorKind::Stopped)
        });
        if let Some(reason) = activation_error {
            self.abandon_once();
            return Err(AdapterError { reason, payload: self });
        }
        lock(&self.lifecycle).phase = ChildPhase::Activated;
        if let Some(activate) = self.activate.take() {
            activate(self.payload.take().expect("committed child owns its payload"));
        }
        drop(self.admission.take());
        drop(self.abandon.take());
        Ok(())
    }

    /// Performs the abandonment transition at most once.
    ///
    /// # Parameters
    ///
    /// * `self` is the permit whose unconsumed state may be abandoned.
    fn abandon_once(&mut self) {
        let should_abandon = {
            let mut phase = lock(&self.lifecycle);
            if matches!(phase.phase, ChildPhase::Abandoned | ChildPhase::Activated) {
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

impl Drop for ChildStartPermit {
    /// Releases retained state when activation did not consume the permit.
    ///
    /// # Parameters
    ///
    /// * `self` is the permit whose remaining state is released.
    fn drop(&mut self) {
        self.abandon_once();
    }
}

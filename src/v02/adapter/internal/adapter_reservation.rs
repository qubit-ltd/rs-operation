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
use super::AttachedChild;
use super::ChildPhase;
use super::Lifecycle;
use crate::v02::protocol::OperationControl;

/// An owning reservation for child admission and its capacity token.
pub(crate) struct AdapterReservation {
    /// Capacity or admission token held until commit or abandonment.
    admission: Option<Box<dyn Any + Send>>,
    /// Validates that the child may cross the commit boundary.
    commit_gate: Arc<dyn Fn() -> Result<(), AdapterErrorKind> + Send + Sync>,
    /// Revalidates permission immediately before child activation.
    activation_gate: Arc<dyn Fn() -> Result<(), AdapterErrorKind> + Send + Sync>,
    /// Whether the gates fully determine stop acceptance.
    gate_is_authoritative: bool,
    /// Starts the external child after both gates accept it.
    activate: Option<ActivationCallback>,
    /// Releases parent-side admission when the reservation is abandoned.
    abandon: Option<Box<dyn FnOnce() + Send>>,
}

impl AdapterReservation {
    /// Creates a reservation with permissive gates for focused tests.
    ///
    /// # Parameters
    ///
    /// * `admission` is the test reservation token to transfer or release.
    /// * `activate` starts the child with the committed payload.
    /// * `abandon` releases parent-side admission if the reservation is
    ///   dropped.
    ///
    /// # Returns
    ///
    /// A reservation that holds the admission token until attachment or drop.
    #[cfg(test)]
    pub(crate) fn new(
        admission: Box<dyn Any + Send>,
        activate: impl FnOnce(Box<dyn Any + Send>) + Send + 'static,
        abandon: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self::build(admission, || Ok(()), || Ok(()), false, activate, abandon)
    }

    /// Creates a reservation with separate commit and activation checks.
    ///
    /// # Parameters
    ///
    /// * `admission` is held until attachment or abandonment.
    /// * `commit_gate` validates admission at the commit boundary.
    /// * `activation_gate` revalidates admission immediately before activation.
    /// * `activate` starts the child after both gates accept it.
    /// * `abandon` releases parent-side admission if the reservation is
    ///   dropped.
    ///
    /// # Returns
    ///
    /// A reservation that holds admission and revalidates both gates before
    /// child activation.
    pub(crate) fn new_with_gates(
        admission: Box<dyn Any + Send>,
        commit_gate: impl Fn() -> Result<(), AdapterErrorKind> + Send + Sync + 'static,
        activation_gate: impl Fn() -> Result<(), AdapterErrorKind> + Send + Sync + 'static,
        activate: impl FnOnce(Box<dyn Any + Send>) + Send + 'static,
        abandon: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self::build(admission, commit_gate, activation_gate, true, activate, abandon)
    }

    /// Stores callbacks and admission state shared by reservation constructors.
    ///
    /// # Parameters
    ///
    /// * `admission` is held until attachment or abandonment.
    /// * `commit_gate` validates admission at the commit boundary.
    /// * `activation_gate` revalidates admission immediately before activation.
    /// * `gate_is_authoritative` indicates whether the gates decide stop
    ///   acceptance.
    /// * `activate` starts the child with the committed payload.
    /// * `abandon` releases parent-side admission if the reservation is
    ///   dropped.
    ///
    /// # Returns
    ///
    /// A reservation containing the token, gates, and lifecycle callbacks.
    fn build(
        admission: Box<dyn Any + Send>,
        commit_gate: impl Fn() -> Result<(), AdapterErrorKind> + Send + Sync + 'static,
        activation_gate: impl Fn() -> Result<(), AdapterErrorKind> + Send + Sync + 'static,
        gate_is_authoritative: bool,
        activate: impl FnOnce(Box<dyn Any + Send>) + Send + 'static,
        abandon: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            admission: Some(admission),
            commit_gate: Arc::new(commit_gate),
            activation_gate: Arc::new(activation_gate),
            gate_is_authoritative,
            activate: Some(Box::new(activate)),
            abandon: Some(Box::new(abandon)),
        }
    }

    /// Associates the payload with an operation control and transfers
    /// admission.
    ///
    /// # Type Parameters
    ///
    /// * `P` is the child payload retained until activation.
    ///
    /// # Parameters
    ///
    /// * `self` is the reservation whose admission and callbacks are
    ///   transferred into the attached child.
    /// * `control` receives stop requests for the attached child.
    /// * `payload` is transferred to the child activation callback.
    ///
    /// # Returns
    ///
    /// An attached child on success; on failure, an error retaining the
    /// payload and reservation.
    ///
    /// # Errors
    ///
    /// Returns `AdapterErrorKind::AlreadyAttached` if the reservation no
    /// longer holds its admission token.
    pub(crate) fn attach<P: Send + 'static>(
        mut self,
        control: Arc<dyn OperationControl>,
        payload: P,
    ) -> Result<AttachedChild, AdapterError<(P, Self)>> {
        if self.admission.is_none() {
            return Err(AdapterError {
                reason: AdapterErrorKind::AlreadyAttached,
                payload: (payload, self),
            });
        }
        Ok(AttachedChild {
            admission: self.admission.take(),
            control,
            payload: Some(Box::new(payload)),
            lifecycle: Arc::new(Mutex::new(Lifecycle {
                phase: ChildPhase::Attached,
                stop_requested: false,
            })),
            commit_gate: Arc::clone(&self.commit_gate),
            activation_gate: Arc::clone(&self.activation_gate),
            gate_is_authoritative: self.gate_is_authoritative,
            activate: self.activate.take(),
            abandon: self.abandon.take(),
        })
    }
}

impl Drop for AdapterReservation {
    /// Abandons unconsumed admission and releases its capacity token.
    ///
    /// # Parameters
    ///
    /// * `self` is the reservation whose unconsumed admission is abandoned.
    fn drop(&mut self) {
        if self.admission.is_some() {
            drop(self.admission.take());
            if let Some(abandon) = self.abandon.take() {
                abandon();
            }
        }
    }
}

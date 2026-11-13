// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Two-phase child registration before delegated work may start.

use std::sync::Arc;
use std::sync::Mutex;

use super::ChildRequirement;
use super::ScopeError;
use super::internal::Child;
use super::internal::State;
use crate::protocol::ManagedOperationControl;
use crate::resource::ResourceUse;

/// Reserves a child slot while a domain executor prepares work for
/// registration.
///
/// The reservation must be attached to a suspended operation before it is
/// committed. Dropping it before commit releases the reserved slot without
/// registering a child.
///
/// # Examples
///
/// A supervisor provides an [`OperationScope`](super::OperationScope) to its
/// operation callback. A domain executor can reserve a slot while preparing
/// work, then release it if preparation is abandoned:
///
/// ```
/// use qubit_operation::scope::{ChildRequirement, OperationScope, ScopeError};
///
/// fn prepare_child(scope: &OperationScope) -> Result<(), ScopeError> {
///     let reservation = scope.reserve_child(ChildRequirement::Required)?;
///     drop(reservation);
///     Ok(())
/// }
/// ```
pub struct ChildReservation {
    /// Shared scope state used to serialize reservation and child registration.
    pub(super) scope: Arc<Mutex<State>>,
    /// Requiredness policy applied to the registered child.
    pub(super) requirement: ChildRequirement,
    /// Suspended operation control awaiting registration.
    pub(super) control: Option<Arc<dyn ManagedOperationControl>>,
    /// Resource-use leases transferred to the child when committed.
    pub(super) uses: Vec<ResourceUse>,
    /// Whether registration has been committed.
    pub(super) committed: bool,
    /// Whether this value still owns a reserved child slot.
    pub(super) reserved: bool,
}

impl ChildReservation {
    /// Attach a suspended domain operation control before it may be started.
    ///
    /// # Parameters
    ///
    /// * `control` — the suspended operation control to register.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeError::AlreadyAttached`] if a control is already present.
    pub fn attach(&mut self, control: Arc<dyn ManagedOperationControl>) -> Result<(), ScopeError> {
        if self.control.is_some() {
            return Err(ScopeError::AlreadyAttached);
        }
        self.control = Some(control);
        Ok(())
    }

    /// Attach a child control and transfer any resource-use leases it requires.
    ///
    /// # Parameters
    ///
    /// * `control` — the suspended operation control to register.
    /// * `uses` — resource-use leases whose ownership transfers to the child.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeError::AlreadyAttached`] if a control is already present.
    pub fn attach_with_uses(
        &mut self,
        control: Arc<dyn ManagedOperationControl>,
        uses: Vec<ResourceUse>,
    ) -> Result<(), ScopeError> {
        self.attach(control)?;
        self.uses = uses;
        Ok(())
    }

    /// Linearizes child registration against scope cancellation and closure.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeError::NotAttached`] if no control was attached,
    /// [`ScopeError::Cancelled`] if cancellation won the race, or
    /// [`ScopeError::Closed`] if the scope no longer accepts children.
    pub fn commit(mut self) -> Result<(), ScopeError> {
        let control = self.control.take().ok_or(ScopeError::NotAttached)?;
        let child_id = control.snapshot().operation_id;
        let mut state = self.scope.lock().unwrap_or_else(|e| e.into_inner());
        if state.stop.is_some() {
            return Err(ScopeError::Cancelled);
        }
        if !state.accepting {
            return Err(ScopeError::Closed);
        }
        state.children.push(Child {
            id: child_id,
            control,
            requirement: self.requirement,
            committed: true,
            uses: Some(std::mem::take(&mut self.uses)),
        });
        state.reserved_children -= 1;
        self.reserved = false;
        self.committed = true;
        Ok(())
    }
}

impl Drop for ChildReservation {
    /// Releases the reserved child slot if registration was not committed.
    fn drop(&mut self) {
        if self.reserved {
            let mut state = self.scope.lock().unwrap_or_else(|e| e.into_inner());
            state.reserved_children -= 1;
            self.reserved = false;
        }
    }
}

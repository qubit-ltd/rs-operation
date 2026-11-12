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

use crate::protocol::ManagedOperationControl;
use crate::resource::ResourceUse;

use super::internal::Child;
use super::internal::State;
use super::{ChildRequirement, ScopeError};

/// A two-phase registration reservation. Dropping before commit has no child
/// side effect.
pub struct ChildReservation {
    scope: Arc<Mutex<State>>,
    requirement: ChildRequirement,
    control: Option<Arc<dyn ManagedOperationControl>>,
    uses: Vec<ResourceUse>,
    committed: bool,
    reserved: bool,
}

impl ChildReservation {
    /// Attach a suspended domain operation control before it may be started.
    pub fn attach(&mut self, control: Arc<dyn ManagedOperationControl>) -> Result<(), ScopeError> {
        if self.control.is_some() {
            return Err(ScopeError::AlreadyAttached);
        }
        self.control = Some(control);
        Ok(())
    }

    /// Attach a child control and transfer any resource-use leases it requires.
    pub fn attach_with_uses(
        &mut self,
        control: Arc<dyn ManagedOperationControl>,
        uses: Vec<ResourceUse>,
    ) -> Result<(), ScopeError> {
        self.attach(control)?;
        self.uses = uses;
        Ok(())
    }

    /// Linearize child start authorization against cancellation.
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
    fn drop(&mut self) {
        if self.reserved {
            let mut state = self.scope.lock().unwrap_or_else(|e| e.into_inner());
            state.reserved_children -= 1;
            self.reserved = false;
        }
    }
}

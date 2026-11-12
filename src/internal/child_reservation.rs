// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The uncommitted reservation capability for a child operation.

use super::AttachRejected;
use super::ChildCommit;
use crate::protocol::ManagedOperationControl;

/// Holds a child registration until a control is attached and it is committed.
///
/// Dropping an unused reservation releases only that reservation. An attached
/// control remains owned by the registry until commit or stop.
pub(crate) trait ChildReservation: Send {
    /// Attaches the operation control to this reserved child.
    ///
    /// If attachment fails, the rejection returns ownership of the control.
    ///
    /// # Parameters
    ///
    /// - `control`: The registered operation's control handle.
    ///
    /// # Returns
    ///
    /// A commit capability for the attached child.
    ///
    /// # Errors
    ///
    /// Returns the admission failure and the control if attachment is rejected.
    fn attach(
        self: Box<Self>,
        control: Box<dyn ManagedOperationControl>,
    ) -> Result<Box<dyn ChildCommit>, AttachRejected>;
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The internal port for reserving child operation registrations.

use super::ChildOptions;
use super::ChildReservation;
use crate::report::AdmissionError;

/// Reserves a child slot before that child is allowed to start.
///
/// The two-phase protocol lets the caller attach an operation control before
/// consuming the returned reservation to commit the registration.
pub(crate) trait ChildRegistry: Send + Sync {
    /// Reserves capacity and registration state for a child operation.
    ///
    /// Returns an admission error when the child cannot be registered.
    ///
    /// # Parameters
    ///
    /// - `options`: The child failure policy to apply to the reservation.
    ///
    /// # Returns
    ///
    /// A reservation that can attach a control and later be committed.
    ///
    /// # Errors
    ///
    /// Returns the admission failure when no reservation can be made.
    fn reserve_child(&self, options: ChildOptions) -> Result<Box<dyn ChildReservation>, AdmissionError>;
}

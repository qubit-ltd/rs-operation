// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::ResourceReservation;
use crate::v02::report::ResourceManageErrorKind;

/// Error returned when a reserved resource commit is rejected.
///
/// The error preserves the original resource and reservation so the caller can
/// retry or route the value through the reserved cleanup path.
///
/// # Type Parameters
///
/// * `T` — resource value whose commit failed.
///
/// # Examples
///
/// A rejected commit returns its resource and reservation so the caller can
/// choose a retry or cleanup path based on the rejection reason:
///
/// ```
/// use qubit_operation::ResourceCommitError;
/// use qubit_operation::ResourceReservation;
/// use qubit_operation::ResourceManageErrorKind;
///
/// fn route_rejected_commit(
///     error: ResourceCommitError<String>,
/// ) -> (&'static str, String, ResourceReservation) {
///     let (reason, resource, reservation) = error.into_parts();
///     let action = match reason {
///         ResourceManageErrorKind::CapacityExceeded => "retry when capacity is available",
///         ResourceManageErrorKind::ScopeEnded => "route through cleanup",
///         ResourceManageErrorKind::Stopped => "route through cleanup",
///     };
///     (action, resource, reservation)
/// }
/// ```
#[must_use = "inspect the commit failure or retain the resource and reservation"]
pub struct ResourceCommitError<T> {
    /// Reason the operation scope rejected the reserved commit.
    reason: ResourceManageErrorKind,
    /// Original resource value retained for retry or alternate cleanup.
    resource: T,
    /// Reservation retained so the caller can retry or route cleanup.
    reservation: ResourceReservation,
}

impl<T> ResourceCommitError<T> {
    /// Creates an error containing the rejection reason and retained inputs.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value whose commit failed.
    ///
    /// # Parameters
    ///
    /// * `reason` — why the operation scope rejected the commit.
    /// * `resource` — original resource value.
    /// * `reservation` — capacity and cleanup reservation retained for retry.
    pub(in crate::v02::resource) fn new(
        reason: ResourceManageErrorKind,
        resource: T,
        reservation: ResourceReservation,
    ) -> Self {
        Self {
            reason,
            resource,
            reservation,
        }
    }

    /// Returns why the reserved commit was rejected.
    ///
    /// # Returns
    ///
    /// The scope-ended, stopped, or capacity-exceeded reason.
    #[must_use = "inspect why the reserved commit was rejected"]
    #[inline]
    pub fn reason(&self) -> ResourceManageErrorKind {
        self.reason
    }

    /// Returns the rejection reason, resource, and reservation.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value returned to the caller.
    ///
    /// # Returns
    ///
    /// A tuple containing the failure reason and both retained inputs.
    #[must_use = "recover the failure reason, resource, and reservation"]
    #[inline]
    pub fn into_parts(self) -> (ResourceManageErrorKind, T, ResourceReservation) {
        (self.reason, self.resource, self.reservation)
    }
}

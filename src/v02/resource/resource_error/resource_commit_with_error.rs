// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

use super::super::ResourceReservation;
use crate::v02::report::ResourceManageErrorKind;

/// Error returned when a reserved resource commit with cleanup policy is
/// rejected.
///
/// It preserves the resource value, cleanup strategy, and reservation so the
/// caller can retry or route the value through reserved cleanup.
///
/// # Type Parameters
///
/// * `T` — resource value whose commit failed.
/// * `C` — cleanup strategy supplied for that resource.
///
/// # Examples
///
/// A caller can recover all retained inputs after a commit is rejected:
///
/// ```
/// use qubit_operation::ResourceCommitWithError;
/// use qubit_operation::ResourceReservation;
///
/// fn recover<T, C>(
///     error: ResourceCommitWithError<T, C>,
/// ) -> (T, C, ResourceReservation) {
///     let (_reason, resource, cleanup, reservation) = error.into_parts();
///     (resource, cleanup, reservation)
/// }
/// ```
#[must_use = "a rejected resource and its cleanup strategy must be retained or handled"]
pub struct ResourceCommitWithError<T, C> {
    /// Reason the owning scope rejected this reserved commit.
    reason: ResourceManageErrorKind,
    /// Original resource value returned for retry or recovery.
    resource: T,
    /// Cleanup strategy returned with the original resource value.
    cleanup: C,
    /// Admission reservation retained so the caller can retry the commit.
    reservation: ResourceReservation,
}

impl<T, C> ResourceCommitWithError<T, C> {
    /// Creates an error containing the reason and all retained commit inputs.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value whose commit failed.
    /// * `C` — cleanup strategy supplied for that resource.
    ///
    /// # Parameters
    ///
    /// * `reason` — why the operation scope rejected the commit.
    /// * `resource` — original resource value.
    /// * `cleanup` — original cleanup strategy.
    /// * `reservation` — capacity and cleanup reservation retained for retry.
    pub(in crate::v02::resource) fn new(
        reason: ResourceManageErrorKind,
        resource: T,
        cleanup: C,
        reservation: ResourceReservation,
    ) -> Self {
        Self {
            reason,
            resource,
            cleanup,
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

    /// Returns the rejection reason, resource, cleanup strategy, and
    /// reservation.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value returned to the caller.
    /// * `C` — cleanup strategy returned with the resource value.
    ///
    /// # Returns
    ///
    /// A tuple containing the reason and all three retained inputs.
    #[must_use = "recover the failure reason, resource, cleanup strategy, and reservation"]
    #[inline]
    pub fn into_parts(self) -> (ResourceManageErrorKind, T, C, ResourceReservation) {
        (self.reason, self.resource, self.cleanup, self.reservation)
    }
}

impl<T, C> fmt::Debug for ResourceCommitWithError<T, C> {
    /// Formats the rejection reason without exposing retained commit inputs.
    ///
    /// # Parameters
    ///
    /// - `self`: Rejected commit whose public diagnostic is formatted.
    /// - `f`: Formatter receiving the diagnostic representation.
    ///
    /// # Returns
    ///
    /// `Ok(())` after writing the reason, or a formatter error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceCommitWithError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

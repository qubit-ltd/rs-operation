// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Rejected service resource registration retaining its exact owner.

use std::fmt;

use crate::protocol::OwnedCleanup;
use crate::report::AdmissionError;

/// Reports why a service resource could not be registered while retaining its
/// owner.
///
/// The error is produced by [`super::ServiceResourceScope::register_resource`].
/// Recover the rejected cleanup action with [`Self::into_owner`] before
/// deciding how to handle or retry the registration.
///
/// # Examples
///
/// ```
/// use qubit_operation::protocol::OwnedCleanup;
/// use qubit_operation::report::ResourceId;
/// use qubit_operation::supervisor::ServiceResourceError;
///
/// fn handle_registration(
///     result: Result<ResourceId, ServiceResourceError>,
/// ) {
///     if let Err(error) = result {
///         let reason = error.reason;
///         let owner: Box<dyn OwnedCleanup> = error.into_owner();
///         drop((reason, owner));
///     }
/// }
/// ```
#[must_use = "the rejected resource owner must be recovered or handled"]
pub struct ServiceResourceError {
    /// Admission failure that prevented the owner from entering the service
    /// scope.
    pub reason: AdmissionError,
    /// Cleanup authority retained so rejection does not discard resource
    /// ownership.
    pub(super) owner: Box<dyn OwnedCleanup>,
}

impl fmt::Debug for ServiceResourceError {
    /// Formats the admission reason without exposing the retained cleanup
    /// owner.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter that receives the diagnostic representation.
    ///
    /// # Returns
    ///
    /// Returns the formatter's result, including any formatting failure.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceResourceError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl ServiceResourceError {
    /// Recovers the exact cleanup owner rejected by resource registration.
    ///
    /// Consuming the error transfers its retained cleanup authority to the
    /// caller.
    ///
    /// # Returns
    ///
    /// Returns the original cleanup owner so the caller can retry registration
    /// or arrange cleanup.
    #[must_use]
    #[inline]
    pub fn into_owner(self) -> Box<dyn OwnedCleanup> {
        self.owner
    }
}

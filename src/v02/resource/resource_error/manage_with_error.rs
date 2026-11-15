// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

use crate::v02::report::ResourceManageErrorKind;

/// Error returned when a value and its asynchronous cleanup policy cannot be
/// registered.
///
/// It preserves both inputs so the caller can retry registration or choose a
/// different cleanup path.
///
/// # Type Parameters
///
/// * `T` — resource value whose registration failed.
/// * `C` — cleanup policy that was supplied for the resource.
///
/// # Examples
///
/// A caller can recover both values when a scope rejects registration:
///
/// ```
/// use qubit_operation::ManageWithError;
///
/// fn recover<T, C>(error: ManageWithError<T, C>) -> (T, C) {
///     let (_reason, resource, cleanup) = error.into_parts();
///     (resource, cleanup)
/// }
/// ```
#[must_use = "the resource and cleanup strategy must be retained or handled"]
pub struct ManageWithError<T, C> {
    /// Reason the resource scope rejected registration.
    kind: ResourceManageErrorKind,
    /// Original resource value returned to the caller for retry or recovery.
    resource: T,
    /// Original cleanup policy returned together with the resource value.
    cleanup: C,
}

impl<T, C> ManageWithError<T, C> {
    /// Creates an error preserving the resource and its cleanup policy.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value whose registration failed.
    /// * `C` — cleanup policy supplied for the resource.
    ///
    /// # Parameters
    ///
    /// * `kind` — reason the resource scope rejected registration.
    /// * `resource` — original resource value.
    /// * `cleanup` — original cleanup policy.
    pub(in crate::v02::resource) fn new(kind: ResourceManageErrorKind, resource: T, cleanup: C) -> Self {
        Self {
            kind,
            resource,
            cleanup,
        }
    }

    /// Returns why registration failed.
    ///
    /// # Returns
    ///
    /// The scope-ended, stopped, or capacity-exceeded reason.
    #[must_use = "inspect why the resource and cleanup policy were rejected"]
    #[inline]
    pub fn kind(&self) -> ResourceManageErrorKind {
        self.kind
    }

    /// Returns the failure reason, resource value, and cleanup policy.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value returned to the caller.
    /// * `C` — cleanup policy returned with the resource value.
    ///
    /// # Returns
    ///
    /// A tuple containing the reason and both original inputs.
    #[must_use = "recover the failure reason, resource, and cleanup policy"]
    #[inline]
    pub fn into_parts(self) -> (ResourceManageErrorKind, T, C) {
        (self.kind, self.resource, self.cleanup)
    }
}

impl<T, C> fmt::Debug for ManageWithError<T, C> {
    /// Formats the rejection reason without exposing the retained resource or
    /// cleanup policy.
    ///
    /// # Parameters
    ///
    /// - `self`: Registration error whose public diagnostic is formatted.
    /// - `f`: Formatter receiving the diagnostic representation.
    ///
    /// # Returns
    ///
    /// `Ok(())` after writing the reason, or a formatter error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManageWithError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::ManageWithError;
    use crate::v02::report::ResourceManageErrorKind;

    #[test]
    fn test_rejected_value_and_policy_can_be_inspected_and_recovered() {
        let error = ManageWithError::new(ResourceManageErrorKind::CapacityExceeded, String::from("payload"), 23);
        assert_eq!(error.kind(), ResourceManageErrorKind::CapacityExceeded);
        assert_eq!(format!("{error:?}"), "ManageWithError { kind: CapacityExceeded, .. }");
        let (kind, resource, cleanup) = error.into_parts();
        assert_eq!(kind, ResourceManageErrorKind::CapacityExceeded);
        assert_eq!(resource, "payload");
        assert_eq!(cleanup, 23);
    }
}

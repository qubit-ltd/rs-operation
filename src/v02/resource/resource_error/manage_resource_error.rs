// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

use crate::v02::report::ResourceManageErrorKind;

/// Error returned when a resource cannot be registered in a resource scope.
///
/// The error preserves the original resource value so the caller can retry or
/// choose another ownership path.
///
/// # Type Parameters
///
/// * `T` — resource value returned to the caller after registration fails.
///
/// # Examples
///
/// A failed `ResourceScope::manage` call returns this error, from which the
/// caller can recover the original resource and decide how to handle it:
///
/// ```
/// use qubit_operation::ManageResourceError;
///
/// fn recover_resource(error: ManageResourceError<String>) -> String {
///     let (_reason, resource) = error.into_parts();
///     resource
/// }
/// ```
#[must_use = "inspect the resource failure or recover the original value"]
pub struct ManageResourceError<T> {
    /// Reason the resource scope rejected registration.
    kind: ResourceManageErrorKind,
    /// Original value retained for the caller after rejection.
    resource: T,
}

impl<T> ManageResourceError<T> {
    /// Creates an error containing the failure kind and original resource.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value returned to the caller.
    ///
    /// # Parameters
    ///
    /// * `kind` — reason the scope could not accept the resource.
    /// * `resource` — original value whose registration failed.
    pub(crate) fn new(kind: ResourceManageErrorKind, resource: T) -> Self {
        Self { kind, resource }
    }

    /// Returns why the resource was rejected.
    ///
    /// # Returns
    ///
    /// The scope-ended, stopped, or capacity-exceeded reason.
    #[must_use = "inspect why the resource registration failed"]
    #[inline]
    pub fn kind(&self) -> ResourceManageErrorKind {
        self.kind
    }

    /// Returns the failure reason and original resource value.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value returned to the caller.
    ///
    /// # Returns
    ///
    /// A tuple containing the failure reason and the resource value.
    #[must_use = "recover the failure reason and original resource"]
    #[inline]
    pub fn into_parts(self) -> (ResourceManageErrorKind, T) {
        (self.kind, self.resource)
    }
}

impl<T> fmt::Debug for ManageResourceError<T> {
    /// Formats the rejection reason without requiring the resource to be
    /// `Debug`.
    ///
    /// # Parameters
    ///
    /// - `self`: Registration error being formatted.
    /// - `f`: Formatter receiving the diagnostic representation.
    ///
    /// # Returns
    ///
    /// A formatting result indicating whether writing succeeded.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManageResourceError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::ManageResourceError;
    use crate::v02::report::ResourceManageErrorKind;

    struct NonDebugResource;

    #[test]
    fn test_rejection_debug_and_kind_do_not_require_debug_resource() {
        let error = ManageResourceError::new(ResourceManageErrorKind::Stopped, NonDebugResource);

        assert_eq!(error.kind(), ResourceManageErrorKind::Stopped);
        assert_eq!(format!("{error:?}"), "ManageResourceError { kind: Stopped, .. }");
    }

    #[test]
    fn test_rejected_resource_can_be_recovered_with_reason() {
        let error = ManageResourceError::new(ResourceManageErrorKind::ScopeEnded, String::from("payload"));

        let (kind, resource) = error.into_parts();

        assert_eq!(kind, ResourceManageErrorKind::ScopeEnded);
        assert_eq!(resource, "payload");
    }
}

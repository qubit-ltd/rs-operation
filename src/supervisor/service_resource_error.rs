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

#[cfg(test)]
mod tests {
    use core::future::Future;
    use core::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use crate::protocol::CleanupContext;
    use crate::protocol::OwnedCleanup;
    use crate::report::AdmissionError;
    use crate::report::CleanupOutcome;
    use crate::report::SupervisorLimits;
    use crate::supervisor::OperationSupervisor;

    struct DropProbe(Arc<AtomicBool>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    impl OwnedCleanup for DropProbe {
        fn begin(
            self: Box<Self>,
            _context: CleanupContext,
        ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
            Box::pin(async { CleanupOutcome::Cleaned })
        }
    }

    #[test]
    fn test_rejected_service_resource_error_preserves_owner_until_recovered() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let group = supervisor.service_group(1).unwrap();
        let scope = group.resource_scope(1).unwrap();
        scope.close();
        let dropped = Arc::new(AtomicBool::new(false));

        let error = scope
            .register_resource(DropProbe(Arc::clone(&dropped)))
            .expect_err("closed scope rejects resource registration");
        assert_eq!(error.reason, AdmissionError::Closed);
        let debug = format!("{error:?}");
        assert!(debug.contains("Closed"));
        assert!(!debug.contains("DropProbe"));
        assert!(!dropped.load(Ordering::SeqCst));

        drop(error.into_owner());
        assert!(dropped.load(Ordering::SeqCst));
    }
}

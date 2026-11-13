// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::protocol::OwnedCleanup;
use crate::report::ResourceId;

/// Cleanup owner removed from the registry and ready for an external cleanup
/// driver.
///
/// # Examples
///
/// ```
/// use core::future::Future;
/// use core::pin::Pin;
/// use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
/// use qubit_operation::report::{CleanupOutcome, OperationId, ResourceId};
/// use qubit_operation::resource::ResourceRegistry;
///
/// struct Cleanup;
///
/// impl OwnedCleanup for Cleanup {
///     fn begin(
///         self: Box<Self>,
///         _context: CleanupContext,
///     ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
///         Box::pin(async { CleanupOutcome::Cleaned })
///     }
/// }
///
/// fn enqueue_cleanup(_resource_id: ResourceId, _owner: Box<dyn OwnedCleanup>) {}
///
/// let registry = ResourceRegistry::new(OperationId::new(7), 1);
/// let handle = registry.register(Box::new(Cleanup)).expect("capacity is available");
/// handle.close_for_use().expect("the resource is open");
/// let ready = registry.take_ready_cleanup().expect("cleanup is ready");
/// let (resource_id, owner) = ready.into_parts();
/// assert_eq!(resource_id, handle.id());
/// enqueue_cleanup(resource_id, owner);
/// ```
pub struct ReadyCleanup {
    /// Identifies the resource whose cleanup owner is held by this value.
    pub(super) resource_id: ResourceId,
    /// Holds the cleanup owner until it is transferred to the cleanup driver.
    pub(super) owner: Option<Box<dyn OwnedCleanup>>,
}

impl ReadyCleanup {
    /// Return the resource identity whose cleanup owner this value carries.
    pub const fn resource_id(&self) -> ResourceId {
        self.resource_id
    }

    /// Give the owned adapter to a cleanup driver without running it here.
    pub fn into_parts(mut self) -> (ResourceId, Box<dyn OwnedCleanup>) {
        (
            self.resource_id,
            self.owner.take().expect("ready cleanup owner is present"),
        )
    }
}

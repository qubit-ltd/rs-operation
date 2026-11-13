// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::report::ResourceId;

/// Panic fact for a cleanup future whose independent recovery capsule remains
/// registered.
///
/// The scheduler supplies this error through `CleanupObservation::Settled`;
/// callers can inspect the affected resource without owning the panic payload.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::CleanupPanic;
///
/// fn report_cleanup_panic(panic: CleanupPanic) {
///     let _resource = panic.resource_id();
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CleanupPanic {
    /// Resource whose cleanup future panicked.
    pub(super) resource_id: ResourceId,
}

impl CleanupPanic {
    /// Returns the resource whose cleanup future panicked.
    ///
    /// # Returns
    ///
    /// The identifier associated with the failed cleanup.
    pub const fn resource_id(self) -> ResourceId {
        self.resource_id
    }
}

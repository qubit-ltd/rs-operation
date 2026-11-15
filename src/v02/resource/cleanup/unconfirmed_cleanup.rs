// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::manager::capacity::ResourceRegistration;

/// Retained capacity and panic evidence when cleanup completion is unconfirmed.
///
/// The opaque capsule may keep the resource or policy alive after a panic.
/// Dropping this value deliberately forgets retained state rather than running
/// destructors whose cleanup outcome could not be confirmed.
#[must_use = "unconfirmed cleanup responsibility must remain owned"]
pub(crate) struct UnconfirmedCleanup {
    /// Capacity registration retained until a recovery owner accepts it.
    pub(in crate::v02::resource::cleanup) registration: Option<ResourceRegistration>,
    /// Original panic payload from resource destruction or cleanup polling.
    pub(in crate::v02::resource::cleanup) panic: Box<dyn std::any::Any + Send + 'static>,
    /// Opaque cleanup future capsule whose destructor must not run on drop.
    pub(in crate::v02::resource::cleanup) capsule: Option<Box<dyn std::any::Any + Send + 'static>>,
    /// Opaque shared policy owner retained after an async cleanup panic.
    pub(in crate::v02::resource::cleanup) policy: Option<Box<dyn std::any::Any + Send + Sync + 'static>>,
}

impl UnconfirmedCleanup {
    /// Creates unconfirmed cleanup state from a panic and optional
    /// registration.
    ///
    /// No resource or policy capsule is supplied by this constructor.
    ///
    /// # Parameters
    ///
    /// - `registration`: Capacity registration that must remain held.
    /// - `panic`: Panic payload to retain for diagnostics or recovery.
    ///
    /// # Returns
    ///
    /// An unconfirmed cleanup responsibility that must remain owned.
    pub(crate) fn from_panic(
        registration: Option<ResourceRegistration>,
        panic: Box<dyn std::any::Any + Send + 'static>,
    ) -> Self {
        Self {
            registration,
            panic,
            capsule: None,
            policy: None,
        }
    }

    /// Borrows the original panic payload without transferring its ownership.
    ///
    /// # Parameters
    ///
    /// - `self`: The unconfirmed cleanup responsibility being inspected.
    ///
    /// # Returns
    ///
    /// A borrowed type-erased panic payload.
    pub(crate) fn panic_payload(&self) -> &(dyn std::any::Any + Send + 'static) {
        self.panic.as_ref()
    }

    /// Transfers the retained capacity registration to a recovery owner.
    ///
    /// # Parameters
    ///
    /// - `self`: The unconfirmed cleanup responsibility being recovered.
    ///
    /// # Returns
    ///
    /// The registration, if one was retained.
    pub(crate) fn take_registration(&mut self) -> Option<ResourceRegistration> {
        self.registration.take()
    }
}

impl Drop for UnconfirmedCleanup {
    /// Forgets retained capsules and registration instead of running cleanup.
    ///
    /// This preserves the no-false-release guarantee if the responsibility is
    /// accidentally discarded before recovery.
    ///
    /// # Parameters
    ///
    /// - `self`: The unconfirmed state being destroyed.
    fn drop(&mut self) {
        // No release evidence exists. If the recovery owner itself is
        // accidentally discarded, leak its opaque capsule and permits rather
        // than running resource or policy destructors on the dropping thread.
        if let Some(capsule) = self.capsule.take() {
            std::mem::forget(capsule);
        }
        if let Some(policy) = self.policy.take() {
            std::mem::forget(policy);
        }
        if let Some(registration) = self.registration.take() {
            std::mem::forget(registration);
        }
    }
}

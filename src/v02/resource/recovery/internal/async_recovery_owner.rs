// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use super::AsyncRecoveryAction;
use crate::v02::report::ResourceId;
use crate::v02::resource::recovery::RecoveryAction;
use crate::v02::resource::recovery::RecoveryPayload;

/// Owns the resource and registration retained for asynchronous recovery.
///
/// A retry takes the resource from this owner, then restores it after a failed
/// cleanup or releases its capacity registration after a confirmed outcome.
///
/// # Type Parameters
///
/// * `T` — thread-safe resource value retained until cleanup is confirmed.
pub(in crate::v02::resource::recovery) struct AsyncRecoveryOwner<T: Send + Sync + 'static> {
    /// Identity used to attribute cleanup failures to the retained resource.
    pub(super) resource_id: ResourceId,
    /// Resource value available to one active cleanup attempt at a time.
    pub(super) resource: Mutex<Option<T>>,
    /// Capacity registration retained while the resource remains unresolved.
    pub(super) registration: Mutex<Option<crate::v02::manager::capacity::ResourceRegistration>>,
    /// Async cleanup policy reused for every recovery retry.
    pub(super) policy: Arc<dyn crate::v02::resource::cleanup::AsyncCleanup<T>>,
}

/// Creates erased recovery payload and retry action for an asynchronous owner.
///
/// The returned payload keeps the owner alive, while the action retries its
/// cleanup policy and preserves the registration until cleanup succeeds.
///
/// # Type Parameters
///
/// * `T` — thread-safe resource value stored in the recovery owner.
///
/// # Parameters
///
/// * `resource_id` — stable identity used in cleanup reports.
/// * `resource` — resource value whose cleanup was not confirmed.
/// * `policy` — asynchronous cleanup strategy used for retry.
/// * `registration` — capacity claim retained while the resource is unresolved.
///
/// # Returns
///
/// The type-erased owner payload and the retry action bound to that owner.
pub(crate) fn async_owner<T: Send + Sync + 'static>(
    resource_id: ResourceId,
    resource: T,
    policy: Arc<dyn crate::v02::resource::cleanup::AsyncCleanup<T>>,
    registration: crate::v02::manager::capacity::ResourceRegistration,
) -> (RecoveryPayload, Arc<dyn RecoveryAction>) {
    let owner = Arc::new(AsyncRecoveryOwner {
        resource_id,
        resource: Mutex::new(Some(resource)),
        registration: Mutex::new(Some(registration)),
        policy,
    });
    (
        Arc::clone(&owner) as RecoveryPayload,
        Arc::new(AsyncRecoveryAction(owner)),
    )
}

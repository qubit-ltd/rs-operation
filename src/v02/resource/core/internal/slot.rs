// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::super::ScopeCompletion;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::report::ResourceId;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;

/// Mutable lifecycle state owned by a typed resource owner.
///
/// The owner protects each slot with a mutex. It keeps resource identity,
/// active user count, capacity registration, and a reserved cleanup permit
/// together until the resource is safely transferred to cleanup.
///
/// # Type Parameters
///
/// - `T`: Resource value type retained by this lifecycle slot.
pub(in crate::v02::resource) struct Slot<T> {
    /// Generation captured when this owner was installed, used to reject stale
    /// keys.
    pub(in crate::v02::resource::core) key_generation: u64,
    /// Shared resource value retained until cleanup is dispatched.
    pub(in crate::v02::resource::core) value: Option<Arc<T>>,
    /// Number of live resource users whose references defer cleanup.
    pub(in crate::v02::resource::core) users: usize,
    /// Whether new users may acquire this resource from its scope.
    pub(in crate::v02::resource::core) open: bool,
    /// Resource capacity registration held until cleanup or recovery settles.
    pub(in crate::v02::resource::core) registration: Option<ResourceRegistration>,
    /// Cleanup queue slot reserved at admission so dispatch cannot lose the
    /// owner.
    pub(in crate::v02::resource::core) cleanup_permit: Option<Box<dyn CleanupDispatchPermit>>,
    /// Stable identity used in cleanup evidence and settlement reports.
    pub(in crate::v02::resource::core) resource_id: ResourceId,
    /// Scope state notified when resource cleanup completes or fails.
    pub(in crate::v02::resource::core) completion: Arc<ScopeCompletion>,
    /// Optional asynchronous cleanup policy retained until cleanup dispatch.
    pub(in crate::v02::resource::core) policy: Option<Arc<dyn crate::v02::resource::cleanup::AsyncCleanup<T>>>,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Request and resource records owned by a service admission group.

use std::sync::Arc;
use std::task::Waker;

use super::ServiceRequestEntry;
use crate::scope::OperationScope;
use crate::supervisor::ShutdownMode;

/// Tracks admission, shutdown, and resource cleanup for one service group.
pub(in crate::supervisor) struct ServiceGroupState {
    /// Whether the group has stopped accepting new requests and resources.
    pub(in crate::supervisor) closed: bool,
    /// Maximum number of requests or resource scopes admitted at once.
    pub(in crate::supervisor) limit: usize,
    /// Shutdown policy currently applied to outstanding managed requests.
    pub(in crate::supervisor) mode: ShutdownMode,
    /// Identifier assigned to the next admitted request.
    pub(in crate::supervisor) next_id: u64,
    /// Active requests, keyed by their group-local identifiers.
    pub(in crate::supervisor) requests: std::collections::BTreeMap<u64, ServiceRequestEntry>,
    /// Tasks waiting to be awakened when group shutdown makes progress.
    pub(in crate::supervisor) shutdown_waiters: Vec<(u64, Waker)>,
    /// Identifier assigned to the next shutdown waiter.
    pub(in crate::supervisor) next_shutdown_waiter: u64,
    /// Resource scopes tracked until their asynchronous cleanup completes.
    pub(in crate::supervisor) resource_scopes:
        std::collections::BTreeMap<u64, (OperationScope, Arc<std::sync::atomic::AtomicBool>, bool)>,
    /// Identifier assigned to the next resource scope.
    pub(in crate::supervisor) next_resource_id: u64,
}

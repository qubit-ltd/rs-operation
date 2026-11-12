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

use crate::scope::OperationScope;

use super::ServiceRequestEntry;
use crate::supervisor::ShutdownMode;

pub(super) struct ServiceGroupState {
    pub(in crate::supervisor) closed: bool,
    pub(in crate::supervisor) limit: usize,
    pub(in crate::supervisor) mode: ShutdownMode,
    pub(in crate::supervisor) next_id: u64,
    pub(in crate::supervisor) requests: std::collections::BTreeMap<u64, ServiceRequestEntry>,
    pub(in crate::supervisor) shutdown_waiters: Vec<(u64, Waker)>,
    pub(in crate::supervisor) next_shutdown_waiter: u64,
    pub(in crate::supervisor) resource_scopes:
        std::collections::BTreeMap<u64, (crate::scope::OperationScope, Arc<std::sync::atomic::AtomicBool>, bool)>,
    pub(in crate::supervisor) next_resource_id: u64,
}


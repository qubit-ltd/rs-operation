// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared mutex and condition variable for one service group.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;

use super::ServiceGroupState;

/// Shares one service group's mutable state and change notifications across
/// supervisor components.
///
/// Clones share the same allocation. The mutex protects `ServiceGroupState`,
/// and the condition variable accompanies that state for notifications when
/// lifecycle code updates it.
pub(in crate::supervisor) type SharedServiceGroupState = Arc<(Mutex<ServiceGroupState>, Condvar)>;

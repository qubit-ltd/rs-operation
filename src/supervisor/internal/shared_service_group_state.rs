// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared mutex and condition variable for one service group.

use std::sync::Arc;
use std::sync::Mutex;

use super::ServiceGroupState;

pub(super) type SharedServiceGroupState = Arc<(Mutex<ServiceGroupState>, std::sync::Condvar)>;


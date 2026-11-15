// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::PoolInner;
use super::lock;

/// Stops accepting pool work, marks it stopping, and wakes worker/waiter
/// threads.
///
/// # Parameters
///
/// * `inner` is the shared pool state whose admission is being closed.
pub(in crate::v02::executor) fn close_pool(inner: &PoolInner) {
    let mut state = lock(&inner.state);
    state.accepting = false;
    state.stopping = true;
    drop(state);
    inner.work_available.notify_all();
    inner.changed.notify_all();
}

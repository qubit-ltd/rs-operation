// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::context::DeadlineSignal;
use crate::v02::report::StopReason;

/// Fires a deadline signal, requests timeout, and releases its timer state.
///
/// # Parameters
///
/// * `signal` is the expired deadline signal to complete and clean up.
pub(in crate::v02::context) fn fire_timeout(signal: &mut DeadlineSignal) {
    if let Some(node) = signal.node.upgrade() {
        node.request_stop(StopReason::Timeout);
    }
    signal.fired = true;
    if let Some(registration) = signal.registration.take() {
        registration.cancel();
    }
    signal.registered_waker = None;
}

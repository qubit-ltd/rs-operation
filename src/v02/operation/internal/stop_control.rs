// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::StopReason;

/// Untyped stop-forwarding port supplied by the operation owner.
pub(crate) trait StopControl: Send + Sync {
    /// Forwards a stop request to the operation owner.
    ///
    /// # Parameters
    ///
    /// * `reason` describes why the operation is being asked to stop.
    fn request_stop(&self, reason: StopReason);
}

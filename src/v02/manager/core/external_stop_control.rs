// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Stop-control adapter for externally driven operations.

use std::sync::Arc;

use super::ManagedOperationControl;
use super::StopControl;

/// Routes manager stop requests to an externally driven operation.
pub struct ExternalStopControl {
    pub(super) managed: Arc<ManagedOperationControl>,
}

impl StopControl for ExternalStopControl {
    fn request_stop(&self, reason: crate::v02::report::StopReason) {
        StopControl::request_stop(self.managed.as_ref(), reason);
    }
}

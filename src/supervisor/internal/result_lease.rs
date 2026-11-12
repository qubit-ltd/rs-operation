// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Result-capacity token retained until the outcome is observed or discarded.

use std::sync::Arc;
use std::sync::Mutex;

use super::Inner;
use super::LeaseState;
use super::leases::release_result;

/// Capacity token retained with a result until it is observed or discarded.
pub(crate) struct ResultLease {
    pub(in crate::supervisor) inner: Arc<Inner>,
    pub(in crate::supervisor) lease: Arc<Mutex<LeaseState>>,
}

impl ResultLease {
    /// Release this slot after the result is taken or explicitly discarded.
    pub(crate) fn observe(self) {}
}

impl Drop for ResultLease {
    fn drop(&mut self) {
        release_result(&self.inner, &self.lease);
    }
}


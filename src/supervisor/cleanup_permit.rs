// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Independent cleanup capacity reservation.

use std::sync::Arc;

use super::internal::Inner;
use super::internal::util::lock;

/// Independent cleanup capacity reservation that releases its slot on drop.
///
/// Callers obtain permits from
/// [`OperationSupervisor::try_reserve_cleanup`](crate::supervisor::OperationSupervisor::try_reserve_cleanup);
/// discarding a permit releases the reserved capacity immediately.
///
/// # Examples
///
/// ```
/// use qubit_operation::CleanupPermit;
///
/// fn finish_cleanup(permit: CleanupPermit) {
///     // The permit is supplied by OperationSupervisor::try_reserve_cleanup.
///     drop(permit);
/// }
/// ```
#[must_use = "dropping the permit immediately releases cleanup capacity"]
pub struct CleanupPermit {
    /// Supervisor state whose cleanup capacity this permit holds.
    inner: Arc<Inner>,
    /// Tracks whether Drop has already returned the reserved slot.
    released: bool,
}

impl Drop for CleanupPermit {
    /// Returns the reserved cleanup slot exactly once.
    fn drop(&mut self) {
        if !self.released {
            let mut state = lock(&self.inner.state);
            state.cleanup_active = state.cleanup_active.saturating_sub(1);
            self.released = true;
        }
    }
}


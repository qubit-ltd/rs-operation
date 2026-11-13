// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

/// One cleanup concurrency slot. Dropping it returns the slot.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::CleanupScheduler;
///
/// let scheduler = CleanupScheduler::new(1);
/// let permit = scheduler.try_acquire().expect("the slot is available");
/// assert_eq!(scheduler.active(), 1);
/// drop(permit);
/// assert_eq!(scheduler.active(), 0);
/// ```
pub struct CleanupPermit {
    /// Counter decremented when this permit is dropped.
    pub(super) active: Arc<AtomicUsize>,
    /// Waiters woken after this permit releases its slot.
    pub(super) capacity_waiters: Arc<Mutex<Vec<Waker>>>,
}

impl Drop for CleanupPermit {
    /// Releases one capacity slot and wakes all registered waiters.
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        let waiters = {
            let mut waiters = self
                .capacity_waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }
}

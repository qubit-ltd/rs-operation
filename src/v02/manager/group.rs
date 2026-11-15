// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Operation groups add a local admission and shutdown boundary.

mod async_operation_group;
mod internal;
mod operation_group;

use std::sync::Mutex;

pub use async_operation_group::AsyncOperationGroup;
pub(crate) use internal::GroupCore;
pub(crate) use internal::GroupLease;
pub(crate) use internal::GroupPermit;
pub use operation_group::OperationGroup;

use super::core::OperationManager;
use crate::v02::operation::AsyncOperationHandle;
use crate::v02::operation::OperationHandle;
use crate::v02::report::GroupOptions;
use crate::v02::report::GroupSnapshot;
use crate::v02::report::ManagerCreateError;
use crate::v02::report::OperationOptions;
use crate::v02::report::SubmitError;

/// Locks a mutex and recovers its inner value if a previous holder panicked.
///
/// # Type Parameters
///
/// - `T`: Value protected by the mutex.
///
/// # Parameters
///
/// - `mutex`: Mutex to acquire.
///
/// # Returns
///
/// The mutex guard, including the inner value if the mutex was poisoned.
#[inline]
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

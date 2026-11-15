// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::resource::recovery::RecoveryFuture;
use crate::v02::resource::recovery::RecoveryPayload;

/// Retry strategy operates through a shared retained owner capsule. The
/// capsule remains in `RecoveryCore` across failures and future cancellation.
pub(crate) trait RecoveryAction: Send + Sync + 'static {
    /// Starts cleanup for one retained recovery owner.
    ///
    /// The payload keeps the owner alive if the attempt is abandoned before
    /// cleanup is confirmed.
    ///
    /// # Parameters
    ///
    /// * `self` — strategy responsible for retrying the retained owner.
    /// * `payload` — type-erased owner state retained across the attempt.
    ///
    /// # Returns
    ///
    /// A future that resolves to `Ok(())` after confirmed cleanup or to the
    /// cleanup failure when ownership remains unresolved.
    fn retry(&self, payload: RecoveryPayload) -> RecoveryFuture;
}

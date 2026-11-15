// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Errors reported while draining or joining a recovery store during shutdown.
///
/// A failed wait does not mean a recovery group was successfully released;
/// inspect the store's snapshot and retain its recovery records as needed.
/// A [`crate::v02::resource::recovery::RecoveryStoreShutdownHandle`] is
/// returned
/// by [`crate::v02::resource::recovery::RecoveryStore::request_shutdown`].
///
/// # Examples
///
/// ```
/// use qubit_operation::RecoveryStoreShutdownError;
/// use qubit_operation::RecoveryStoreShutdownHandle;
///
/// fn wait_for_shutdown(
///     shutdown: &RecoveryStoreShutdownHandle,
/// ) -> Result<(), RecoveryStoreShutdownError> {
///     shutdown.wait()
/// }
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryStoreShutdownError {
    /// A cleanup executor or recovery worker panicked while joining.
    WorkerPanicked,
    /// No executor was available to drive the store's pending shutdown work.
    ExecutorUnavailable,
}

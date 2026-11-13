// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Deadline-bounded operation observation results.

use super::ObserverSnapshot;
use super::OperationObserver;

/// A deadline-bounded snapshot and an observer that remains live afterward.
///
/// # Type Parameters
///
/// `T` and `E` describe the live observer's typed result.
/// This value is returned by [`OperationObserver::observe_until`]. Its observer
/// continues to provide updates after the snapshot is captured.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::Deadline;
/// use qubit_operation::operation::{ObserveUntil, OperationObserver};
/// use qubit_operation::report::ObservationError;
/// async fn inspect<T, E>(observer: &OperationObserver<T, E>, deadline: Deadline) {
///     let result: Result<ObserveUntil<T, E>, ObservationError> =
///         observer.observe_until(deadline).await;
///     match result {
///         Ok(observation) => drop(observation.snapshot),
///         Err(error) => drop(error),
///     }
/// }
/// ```
pub struct ObserveUntil<T, E> {
    /// State captured when the deadline or settlement was observed.
    pub snapshot: ObserverSnapshot,
    /// Observer that can continue observing after this snapshot.
    pub observer: OperationObserver<T, E>,
}

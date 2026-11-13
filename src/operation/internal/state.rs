// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable result and settlement state shared by operation handles.

use core::task::Waker;

use crate::report::OperationOutcome;
use crate::report::SettlementReport;
use crate::supervisor::ResultLease;

/// Mutable result and settlement state shared by all handles.
///
/// # Type Parameters
///
/// * `T` is the operation's successful result type.
/// * `E` is the operation's failure type.
pub(crate) struct State<T, E> {
    /// Business outcome until it is consumed or discarded.
    pub(crate) result: Option<OperationOutcome<T, E>>,
    /// Whether the unique typed consumer has taken the outcome.
    pub(crate) result_taken: bool,
    /// Whether the handle discarded typed result delivery.
    pub(crate) result_discarded: bool,
    /// Final resource settlement once cleanup completes.
    pub(crate) released: Option<SettlementReport>,
    /// Tasks waiting for the typed outcome.
    pub(crate) result_waiters: Vec<Waker>,
    /// Tasks waiting for resource settlement.
    pub(crate) release_waiters: Vec<Waker>,
    /// Admission lease retained until the outcome is observed or discarded.
    pub(crate) result_lease: Option<ResultLease>,
    /// Values retained until resource cleanup is complete.
    pub(crate) completion_guards: Vec<Box<dyn Send>>,
}

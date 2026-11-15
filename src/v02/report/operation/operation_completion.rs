// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::OperationOutcome;
use crate::v02::report::SettlementReport;

/// Combines a body result with the separate resource settlement report.
///
/// # Type Parameters
///
/// `T` is the body success value; `E` is its application error.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationCompletion;
///
/// fn inspect<T, E>(completion: OperationCompletion<T, E>) {
///     let _body_result = completion.outcome;
///     let _resource_report = completion.settlement;
/// }
/// ```
#[must_use = "the operation outcome and resource settlement must be observed"]
#[derive(Clone, Debug)]
pub struct OperationCompletion<T, E> {
    /// Success, application failure, stop, panic, or executor result.
    pub outcome: OperationOutcome<T, E>,
    /// Resource disposition produced after the operation completed.
    pub settlement: Arc<SettlementReport>,
}

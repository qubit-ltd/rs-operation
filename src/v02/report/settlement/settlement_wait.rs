// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::OperationSnapshot;
use crate::v02::report::SettlementReport;

/// Result of waiting for an operation's resource settlement.
///
/// # Examples
///
/// ```
/// use qubit_operation::SettlementWait;
///
/// fn describe(wait: SettlementWait) -> &'static str {
///     match wait {
///         SettlementWait::Ready(_) => "settlement is ready",
///         SettlementWait::Pending(_) => "settlement is pending",
///     }
/// }
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub enum SettlementWait {
    /// Resource cleanup reached a reportable settled state.
    Ready(#[doc = "Final settlement report for the operation."] Arc<SettlementReport>),
    /// Execution or cleanup remains in progress.
    Pending(#[doc = "Latest operation state while settlement is pending."] OperationSnapshot),
}

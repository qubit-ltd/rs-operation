// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The internal port for registering and observing cleanup operations.

use super::CleanupRejected;
use crate::protocol::OwnedCleanup;
use crate::report::CleanupOutcome;
use crate::report::OperationId;
use crate::report::SettlementReport;

/// Registers cleanup only after its owner is ready and exposes settlement data.
pub(crate) trait CleanupPort: Send + Sync {
    /// Begins cleanup for an operation and transfers ownership of the cleanup.
    ///
    /// # Parameters
    ///
    /// - `operation`: The identifier of the operation being cleaned up.
    /// - `cleanup`: The cleanup action whose ownership is transferred.
    ///
    /// # Returns
    ///
    /// Returns `()` when cleanup registration succeeds.
    ///
    /// # Errors
    ///
    /// Returns the rejected cleanup and admission failure if registration fails.
    fn begin_cleanup(&self, operation: OperationId, cleanup: Box<dyn OwnedCleanup>) -> Result<(), CleanupRejected>;

    /// Records the outcome produced by a completed cleanup operation.
    ///
    /// # Parameters
    ///
    /// - `outcome`: The cleanup result to retain for settlement reporting.
    fn record_cleanup(&self, outcome: CleanupOutcome);

    /// Returns the currently available settlement report for an operation.
    ///
    /// # Parameters
    ///
    /// - `operation`: The identifier whose settlement is requested.
    ///
    /// # Returns
    ///
    /// Returns `Some` with the report when settlement is available, or `None`
    /// while no report exists for the operation.
    fn settlement(&self, operation: OperationId) -> Option<SettlementReport>;
}

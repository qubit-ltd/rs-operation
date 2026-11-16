// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Stop and settlement helper excerpt for a worker that uses a leased temp
//! file.
//!
//! This file contains no file-system adapter or real lease implementation.
//! For the complete simulated provider, ownership, outcome, release, domain
//! settlement, and shutdown lifecycle, see [`provider_read.rs`].
use std::sync::Arc;

use qubit_operation::OperationControl;
use qubit_operation::OperationHandle;
use qubit_operation::SettlementReport;
use qubit_operation::StopReason;

/// Ask the worker to stop. Its release poll must remain pending until the temp
/// is unused; this helper does not itself establish release.
pub fn request_worker_stop(control: &impl OperationControl) {
    control.request_stop(StopReason::UserCancelled);
}

/// Observe settlement after the caller has observed outcome and worker release.
pub fn settlement<T: Send + 'static, E: Send + 'static>(
    handle: &OperationHandle<T, E>,
) -> Result<Arc<SettlementReport>, qubit_operation::ObservationError> {
    handle.wait_settlement()
}

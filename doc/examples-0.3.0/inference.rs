// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Resource-valued results must be consumed and dropped before settlement.
use qubit_operation::ObservationError;
use qubit_operation::OperationHandle;
use qubit_operation::OperationOutcome;
use qubit_operation::OutcomeError;
use qubit_operation::ResourceRef;
use qubit_operation::SettlementReport;
use std::sync::Arc;

/// Process either resource-valued result, release it, and then observe cleanup.
pub fn consume_and_settle(
    handle: &mut OperationHandle<ResourceRef<String>, ResourceRef<String>>,
) -> Result<Arc<SettlementReport>, ExampleError> {
    match handle.wait_outcome().map_err(ExampleError::Outcome)? {
        OperationOutcome::Succeeded(value) => {
            let _processed = value.len();
            drop(value);
        }
        OperationOutcome::Failed(value) => {
            let _reported = value.as_str();
            drop(value);
        }
        OperationOutcome::Panicked(_)
        | OperationOutcome::Stopped(_)
        | OperationOutcome::ChildFailed(_)
        | OperationOutcome::ExecutionFailed(_) => {}
    }
    handle.wait_settlement().map_err(ExampleError::Settlement)
}

/// Failure while observing the result or settlement.
pub enum ExampleError {
    Outcome(OutcomeError),
    Settlement(ObservationError),
}

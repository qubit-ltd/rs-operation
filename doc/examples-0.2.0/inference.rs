// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_operation::AdmissionError;
use qubit_operation::AsyncOperationHandle;
use qubit_operation::AsyncOperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ResourceScopeOptions;

/// Supplies the model implementation used by the inference example.
///
/// A backend receives a borrowed feature vector and returns a prediction vector
/// or an error describing why inference could not be completed.
pub trait ModelBackend: Send + Sync + 'static {
    /// Runs inference for one borrowed feature vector.
    fn infer(&self, features: &[f32]) -> Result<Vec<f32>, String>;
}

/// Submits inference as a child operation of an asynchronous parent operation.
///
/// The parent keeps the managed feature vector alive while the child invokes
/// the backend, then returns the child's prediction or outcome as its result.
///
/// # Parameters
///
/// - `manager`: Manager that accepts the parent operation.
/// - `model`: Thread-safe backend retained by the child operation.
/// - `features`: Feature values moved into the parent's managed resource scope.
///
/// # Returns
///
/// Returns a handle for observing the parent operation, or an admission error
/// when the manager cannot accept it.
pub fn submit_inference(
    manager: &AsyncOperationManager,
    model: Arc<dyn ModelBackend>,
    features: Vec<f32>,
) -> Result<AsyncOperationHandle<Vec<f32>, String>, AdmissionError> {
    manager
        .try_submit(OperationOptions::default(), move |scope| async move {
            let child = {
                let inputs = scope
                    .resource_scope(ResourceScopeOptions { capacity: 1 })
                    .map_err(|e| e.to_string())?;
                let input = inputs.manage(features).map_err(|error| format!("{:?}", error.kind()))?;
                scope
                    .manager()
                    .try_submit(OperationOptions::default(), move |_child_scope| model.infer(&input))
                    .map_err(|e| e.to_string())?
            }; // inputs 结束；input 已移给同步子操作，仍然存活。

            let mut child = child.into_async();
            match child.wait_outcome().await.map_err(|e| e.to_string())? {
                OperationOutcome::Succeeded(prediction) => Ok(prediction),
                other => Err(format!("model outcome: {other:?}")),
            }
        })
        .map_err(|error| error.into_parts().0)
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_operation::AdmissionError;
use qubit_operation::OperationHandle;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;

/// Submits a synchronous operation that reports the length of managed bytes.
///
/// The byte vector is moved into the operation scope and remains managed for
/// the closure's execution. The returned handle observes the operation result.
pub fn submit_scan(
    manager: &OperationManager,
    bytes: Vec<u8>,
) -> Result<OperationHandle<usize, String>, AdmissionError> {
    manager
        .try_submit(OperationOptions::default(), move |scope| {
            let input = scope.manage(bytes).map_err(|error| format!("{:?}", error.kind()))?;
            Ok::<_, String>(input.len())
        })
        .map_err(|error| error.into_parts().0)
}

/// Submits a request operation and starts its scan as a child operation.
///
/// The child handle is converted into an observer so the parent scope retains
/// the child relationship while returning the request identifier.
pub fn submit_scan_request(
    manager: &OperationManager,
    bytes: Vec<u8>,
) -> Result<OperationHandle<&'static str, String>, AdmissionError> {
    manager
        .try_submit(OperationOptions::default(), move |scope| {
            let child = submit_scan(&scope.manager(), bytes).map_err(|e| e.to_string())?;
            let _observer = child.into_observer();
            Ok::<_, String>("document-42")
        })
        .map_err(|error| error.into_parts().0)
}

//! A worker retains its lease until it has actually stopped using the temp file.
use qubit_operation::{OperationControl, OperationHandle, SettlementReport, StopReason};
use std::sync::Arc;

/// Ask the worker to stop. Its release poll must remain pending until the temp is unused.
pub fn request_worker_stop(control: &impl OperationControl) {
    control.request_stop(StopReason::UserCancelled);
}

/// Observe settlement after the caller has observed outcome and worker release.
pub fn settlement<T: Send + 'static, E: Send + 'static>(
    handle: &OperationHandle<T, E>,
) -> Result<Arc<SettlementReport>, qubit_operation::ObservationError> {
    handle.wait_settlement()
}

//! Resource-valued results must be consumed and dropped before settlement.
use qubit_operation::{OperationHandle, OperationOutcome, OutcomeError, SettlementReport};

/// Wait for the typed result first; drop any returned resource before settling.
pub fn take_result<T: Send + 'static, E: Send + 'static>(
    handle: &mut OperationHandle<T, E>,
) -> Result<OperationOutcome<T, E>, OutcomeError> {
    handle.wait_outcome()
}

/// Call only after the result and its resource references have been dropped.
pub fn settle<T: Send + 'static, E: Send + 'static>(
    handle: &OperationHandle<T, E>,
) -> Result<std::sync::Arc<SettlementReport>, qubit_operation::ObservationError> {
    handle.wait_settlement()
}

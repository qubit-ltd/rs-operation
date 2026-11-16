//! A provider adapter reports early stop independently from releasing input.
use qubit_operation::{OperationOutcome, StopReason};

/// The controller can request stop while the provider still owns its input.
pub fn request_provider_stop<T, E>(control: &impl qubit_operation::OperationControl) {
    let _ = (std::marker::PhantomData::<(T, E)>, control);
    control.request_stop(StopReason::UserCancelled);
}

/// Consume the typed outcome before waiting for the provider's release facts.
pub fn outcome_is_stopped<T, E>(outcome: &OperationOutcome<T, E>) -> bool {
    matches!(outcome, OperationOutcome::Stopped(_))
}

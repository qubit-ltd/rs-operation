//! Immutable lifecycle facts and typed outcomes.

mod errors;
mod ids;
mod limits;
mod outcome;
mod settlement;
mod state;
mod summary;

pub use errors::{AdmissionError, ObservationError, StartError};
pub use ids::{ClockDomainId, OperationId, RecoveryKey, ResourceId, TransferReceipt};
pub use limits::{ScopeLimits, SupervisorLimits};
pub use outcome::{
    CancelReason, ChildBusinessSummary, ChildFailureSummary, EffectRecord, OperationOutcome,
    PanicSummary, StopReason, StopReport,
};
pub use settlement::{
    ChildSettlement, CleanupFailure, CleanupOutcome, CleanupStage, RecoveryOwner, SettlementReport,
};
pub use state::{ChildSnapshot, MonotonicTime, OperationPhase, ResourcePhase};
pub use summary::DiagnosticSummary;

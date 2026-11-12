use super::{DiagnosticSummary, OperationId, ResourceId};

/// Typed business result, kept separate from resource settlement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationOutcome<T, E> {
    /// The body and required children completed successfully.
    Succeeded(T),
    /// Preserve the original domain error without requiring `Clone`.
    Failed(E),
    /// An unhandled required child failed with a different error type.
    ChildFailed(ChildFailureSummary),
    /// Delivery stopped before a business result was determined.
    Stopped(StopReport),
    /// The body panicked; payload details are deliberately omitted.
    Panicked(PanicSummary),
}

/// Bounded identity and summary for an unhandled child failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildFailureSummary {
    /// Child that failed.
    pub operation_id: OperationId,
    /// Sanitized, bounded diagnostic summary.
    pub summary: DiagnosticSummary,
}

/// Repeatable child business fact that does not consume its typed result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildBusinessSummary {
    /// Child that produced this fact.
    pub operation_id: OperationId,
    /// Stable business outcome category.
    pub outcome: ChildBusinessOutcome,
    /// Sanitized, bounded diagnostic summary.
    pub summary: DiagnosticSummary,
}

/// Erased business result category for child observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildBusinessOutcome {
    Succeeded,
    Failed,
    Stopped,
    Panicked,
}

/// Stop cause recorded by the supervisor; acceptance does not prove work has stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    /// Cancellation explicitly requested by the caller.
    UserCancelled,
    /// The operation deadline expired.
    Timeout,
    /// An ancestor scope requested stop.
    ParentStopped,
    /// Supervisor shutdown requested cancellation.
    SupervisorShutdown,
    /// A required child failure triggered fail-fast stop.
    RequiredChildFailed,
}

/// Caller-level stop request; the supervisor maps it to a lifecycle `StopReason`.
pub type CancelReason = StopReason;

/// Immutable stop fact attached to a business result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StopReport {
    /// First stop cause accepted by the operation state.
    pub reason: StopReason,
    /// Clock-domain timestamp of acceptance, when the clock was available.
    pub accepted_at: Option<super::MonotonicTime>,
}

/// Sanitized panic diagnostic without arbitrary panic payload disclosure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PanicSummary {
    /// Operation whose body panicked.
    pub operation_id: OperationId,
    /// Bounded lifecycle phase in which panic occurred.
    pub phase: DiagnosticSummary,
}

/// Caller-declared side-effect fact; the framework does not independently verify it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectRecord {
    /// Operation that reported the effect.
    pub operation_id: OperationId,
    /// Related resource, when known.
    pub resource_id: Option<ResourceId>,
    /// Bounded effect description provided by the caller.
    pub description: DiagnosticSummary,
    /// Whether the caller considers the effect known or uncertain.
    pub fact: EffectFact,
}

/// Certainty asserted by the effect's source, not inferred by this framework.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectFact {
    Known,
    Unknown,
}

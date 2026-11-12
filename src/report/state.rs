use super::{ClockDomainId, OperationId};

/// Monotonic tick value; values from different domains are incomparable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Monotonic time point; only ticks from the same domain can be compared.
pub struct MonotonicTime {
    /// Clock source identity.
    pub domain: ClockDomainId,
    /// Domain-local monotonically increasing tick.
    pub ticks: u64,
}

impl MonotonicTime {
    /// Construct a domain-local monotonic time.
    pub const fn new(domain: ClockDomainId, ticks: u64) -> Self {
        Self { domain, ticks }
    }
    /// Return elapsed ticks only when both values share a domain and order.
    pub const fn checked_duration_since(self, earlier: Self) -> Option<u64> {
        if self.domain.0 != earlier.domain.0 {
            None
        } else {
            self.ticks.checked_sub(earlier.ticks)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Business result-delivery phase; stop requests are represented separately.
pub enum OperationPhase {
    /// Accepted and queued for execution.
    Queued,
    /// Body has begun.
    Running,
    /// Typed business outcome is ready to observe.
    ResultReady,
    /// The one-shot typed result has been consumed.
    ResultTaken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Resource lifecycle phase. Only `Released` proves confirmed release.
pub enum ResourcePhase {
    Active,
    Stopping,
    Cleaning,
    Released,
    RecoveryRequired,
    Unconfirmed,
}

/// Bounded observation snapshot for a child operation.
#[derive(Clone, Debug, Eq, PartialEq)]
/// Bounded child-operation snapshot; it does not consume results or prove more than the fields state.
pub struct ChildSnapshot {
    /// Child operation identity.
    pub operation_id: OperationId,
    /// Business result phase.
    pub phase: OperationPhase,
    /// Resource cleanup and release phase.
    pub resource_phase: ResourcePhase,
    /// Whether a stop request was accepted.
    pub stop_requested: bool,
    /// Whether release has been confirmed.
    pub released: bool,
}

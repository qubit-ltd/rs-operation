use crate::report::{ChildBusinessSummary, ChildSettlement, ChildSnapshot, StopReason};
use core::task::{Context, Poll};

/// Nonblocking, repeatably observable control for a domain operation.
pub trait ManagedOperationControl: Send + Sync {
    /// Request stop. Implementations must be nonblocking and idempotent.
    fn request_stop(&self, reason: StopReason);
    /// Return a bounded snapshot without performing I/O.
    fn snapshot(&self) -> ChildSnapshot;
    /// Observe the stable business fact without consuming a typed domain result.
    fn poll_business(&self, cx: &mut Context<'_>) -> Poll<ChildBusinessSummary>;
    /// Observe domain-confirmed release; `Ready` is never inferred from a stop request.
    fn poll_released(&self, cx: &mut Context<'_>) -> Poll<ChildSettlement>;
}

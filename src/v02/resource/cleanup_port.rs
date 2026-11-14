//! Reserved, nonblocking ownership handoff for resource cleanup.

use crate::v02::executor::CleanupExecutor;
use std::sync::Arc;

/// A type-erased cleanup action that owns the resource until it is run.
pub(crate) trait CleanupJob: Send + 'static {
    /// Schedules the owner on its appropriate bounded executor.
    ///
    /// On admission failure, returns the same job intact so the reserved
    /// dispatch slot can keep it pending without dropping the resource.
    fn schedule(self: Box<Self>, executor: Arc<CleanupExecutor>)
    -> Result<(), Box<dyn CleanupJob>>;
}

/// Reserves bounded storage for one resource's eventual cleanup handoff.
pub(crate) trait CleanupDispatch: Send + Sync + 'static {
    fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError>;
}

/// Reservation that guarantees the later enqueue cannot reject or drop its job.
pub(crate) trait CleanupDispatchPermit: Send + 'static {
    fn enqueue(self: Box<Self>, job: Box<dyn CleanupJob>);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupQueueError {
    Full,
    Closed,
}

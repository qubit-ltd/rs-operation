//! Reserved, nonblocking ownership handoff for resource cleanup.

/// A type-erased cleanup action that owns the resource until it is run.
pub(crate) trait CleanupJob: Send + 'static {
    fn run(self: Box<Self>);
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

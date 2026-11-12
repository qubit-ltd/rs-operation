//! Narrow internal ports. These are intentionally not part of the public API.
#![allow(dead_code)] // Ports are consumed by the later driver and cleanup tasks.

// Shared generation-based condition variable and async waker notifications.
mod notify;
// Single-consumer result state and repeatable terminal snapshots.
mod state;

use core::future::Future;
use core::pin::Pin;

use crate::protocol::ManagedOperationControl;
use crate::protocol::OwnedCleanup;
use crate::report::AdmissionError;
use crate::report::CleanupOutcome;
use crate::report::OperationId;
use crate::report::SettlementReport;

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// A driver accepts already-owned work and returns a control handle only when
/// it can guarantee that the task is registered before execution begins.
pub(crate) trait Driver: Send + Sync {
    fn submit(&self, task: BoxFuture<()>) -> Result<Box<dyn ManagedOperationControl>, DriverRejected>;
}

/// Whether an unhandled child failure blocks the parent result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChildRequirement {
    Required,
    Optional,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChildOptions {
    pub requirement: ChildRequirement,
}

/// Two-phase registration port; work cannot start until the returned commit
/// permit is consumed successfully.
pub(crate) trait ChildRegistry: Send + Sync {
    fn reserve_child(&self, options: ChildOptions) -> Result<Box<dyn ChildReservation>, AdmissionError>;
}

/// Uncommitted reservation. Dropping it releases only the unused reservation;
/// an attached control remains owned by the registry until commit or stop.
pub(crate) trait ChildReservation: Send {
    fn attach(
        self: Box<Self>,
        control: Box<dyn ManagedOperationControl>,
    ) -> Result<Box<dyn ChildCommit>, AttachRejected>;
}

pub(crate) struct AttachRejected {
    pub error: AdmissionError,
    pub control: Box<dyn ManagedOperationControl>,
}

/// The only capability that authorizes the already-registered operation to
/// start.
pub(crate) trait ChildCommit: Send {
    fn commit(self: Box<Self>) -> Result<(), AdmissionError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DriverError {
    Unavailable,
    Closed,
    Capacity,
}

pub(crate) struct DriverRejected {
    pub error: DriverError,
    pub task: BoxFuture<()>,
}

/// Synchronous execution port supplied by the application or a domain executor.
pub(crate) trait BlockingExecutor: Send + Sync {
    fn submit(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), BlockingRejected>;
}

pub(crate) struct BlockingRejected {
    pub error: DriverError,
    pub task: Box<dyn FnOnce() + Send + 'static>,
}

/// Cleanup registration boundary: only ready owners may cross this port.
pub(crate) trait CleanupPort: Send + Sync {
    fn begin_cleanup(&self, operation: OperationId, cleanup: Box<dyn OwnedCleanup>) -> Result<(), CleanupRejected>;
    fn record_cleanup(&self, outcome: CleanupOutcome);
    fn settlement(&self, operation: OperationId) -> Option<SettlementReport>;
}

pub(crate) struct CleanupRejected {
    pub error: AdmissionError,
    pub cleanup: Box<dyn OwnedCleanup>,
}

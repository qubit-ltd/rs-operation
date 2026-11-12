//! Object-safe ports used to manage domain-owned operations and cleanup.

mod cleanup;
mod control;
mod time;

pub use cleanup::CleanupContext;
pub use cleanup::OwnedCleanup;
pub use control::ManagedOperationControl;
pub use time::ClockSource;
pub use time::TimerRegistration;

pub use crate::report::StopReason;

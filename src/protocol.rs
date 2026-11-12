//! Object-safe ports used to manage domain-owned operations and cleanup.

mod cleanup;
mod control;
mod time;

pub use crate::report::StopReason;
pub use cleanup::{CleanupContext, OwnedCleanup};
pub use control::ManagedOperationControl;
pub use time::{ClockSource, TimerRegistration};

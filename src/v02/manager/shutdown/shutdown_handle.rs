// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::time::Duration;

use super::AsyncShutdownHandle;
use super::internal::ShutdownTarget;
use crate::v02::manager::core::ManagerCore;
use crate::v02::manager::group::GroupCore;
use crate::v02::report::ObservationError;
use crate::v02::report::ShutdownMode;
use crate::v02::report::ShutdownReport;
use crate::v02::report::ShutdownSnapshot;
use crate::v02::report::ShutdownWait;
use crate::v02::scope::is_waiting_on_group;
use crate::v02::scope::is_waiting_on_manager;

/// A durable observer for the manager-wide shutdown already in progress.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationManager;
/// use qubit_operation::{ManagerOptions, ShutdownMode};
///
/// let manager = OperationManager::new(ManagerOptions::default()).unwrap();
/// let shutdown = manager.request_shutdown(ShutdownMode::Drain);
/// let report = shutdown.wait().unwrap();
/// assert_eq!(report.snapshot.active_operations, 0);
/// ```
///
/// The handle observes shutdown of either the global manager or a group target.
#[must_use = "retain this handle to observe shutdown progress"]
#[derive(Clone)]
pub struct ShutdownHandle {
    /// Target whose lifecycle is observed by this handle.
    pub(super) target: ShutdownTarget,
}

impl ShutdownHandle {
    /// Creates an observer for a manager-wide shutdown.
    ///
    /// # Parameters
    ///
    /// - `core`: Shared manager coordinator whose shutdown is observed.
    ///
    /// # Returns
    ///
    /// A handle observing manager-wide shutdown progress.
    #[inline]
    pub(crate) fn new(core: Arc<ManagerCore>) -> Self {
        Self {
            target: ShutdownTarget::Manager(core),
        }
    }

    /// Creates an observer for shutdown of one group within a manager.
    ///
    /// # Parameters
    ///
    /// - `core`: Shared manager coordinator.
    /// - `group`: Group whose shutdown state is observed.
    ///
    /// # Returns
    ///
    /// A handle observing the selected group shutdown.
    #[inline]
    pub(crate) fn for_group(core: Arc<ManagerCore>, group: Arc<GroupCore>) -> Self {
        Self {
            target: ShutdownTarget::Group(core, group),
        }
    }

    /// Returns the shutdown mode currently applied to the observed target.
    ///
    /// # Returns
    ///
    /// The current drain or cancel mode.
    #[must_use]
    #[inline]
    pub fn mode(&self) -> ShutdownMode {
        match &self.target {
            ShutdownTarget::Manager(core) => core.shutdown_mode(),
            ShutdownTarget::Group(_, group) => group.mode(),
        }
    }

    /// Returns the current shutdown progress snapshot.
    ///
    /// # Returns
    ///
    /// Counts of active and queued operations, cleanup, recovery, and service
    /// work.
    #[must_use = "inspect shutdown progress from this snapshot"]
    #[inline]
    pub fn snapshot(&self) -> ShutdownSnapshot {
        match &self.target {
            ShutdownTarget::Manager(core) => ShutdownSnapshot {
                active_operations: core.active_scope_count(),
                queued_operations: core.queued_operation_count(),
                unconfirmed_users: core.unconfirmed_recovery_count(),
                cleaning_resources: core.cleaning_resource_count(),
                recovery_groups: core.recovery_group_count(),
                service_resources: core.service_scope_count(),
            },
            ShutdownTarget::Group(_, group) => group.shutdown_snapshot(),
        }
    }

    /// Waits until the observed manager or group has fully shut down.
    ///
    /// Call this from outside an operation managed by the observed target.
    /// Waiting from one of that target's own operations returns
    /// [`ObservationError::WouldDeadlock`].
    ///
    /// # Returns
    ///
    /// A report containing the final shutdown snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ObservationError::WouldDeadlock`] when the current operation
    /// belongs to the target being observed.
    pub fn wait(&self) -> Result<ShutdownReport, ObservationError> {
        if self.would_deadlock() {
            return Err(ObservationError::WouldDeadlock);
        }
        match &self.target {
            ShutdownTarget::Manager(core) => core.shutdown_wait_blocking(),
            ShutdownTarget::Group(_, group) => group.wait_blocking(),
        }
        Ok(ShutdownReport {
            snapshot: self.snapshot(),
        })
    }

    /// Waits for shutdown completion for up to `timeout`.
    ///
    /// A zero duration performs a snapshot check and never blocks, including
    /// when called from an operation belonging to the target.
    /// If the platform cannot represent the timeout as an `Instant` deadline,
    /// the method waits until shutdown completes.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Maximum duration to wait for shutdown completion.
    ///
    /// # Returns
    ///
    /// `Completed` with a report when shutdown finishes, or `Pending` with a
    /// snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ObservationError::WouldDeadlock`] if a positive wait would
    /// wait for the current operation to finish.
    pub fn wait_for(&self, timeout: Duration) -> Result<ShutdownWait, ObservationError> {
        if !timeout.is_zero() && self.would_deadlock() {
            return Err(ObservationError::WouldDeadlock);
        }
        let completed = match &self.target {
            ShutdownTarget::Manager(core) => core.shutdown_wait_for(timeout),
            ShutdownTarget::Group(_, group) => group.wait_for(timeout),
        };
        if completed {
            Ok(ShutdownWait::Completed(ShutdownReport {
                snapshot: self.snapshot(),
            }))
        } else {
            Ok(ShutdownWait::Pending(self.snapshot()))
        }
    }

    /// Returns an async observer for the same shutdown target.
    ///
    /// # Returns
    ///
    /// An async handle sharing this target and its current shutdown state.
    #[must_use = "use the asynchronous handle to observe shutdown progress"]
    #[inline]
    pub fn as_async(&self) -> AsyncShutdownHandle {
        AsyncShutdownHandle::from_handle(self.clone())
    }

    /// Returns the manager coordinator shared by the observed target.
    #[inline]
    pub(super) fn manager(&self) -> &Arc<ManagerCore> {
        match &self.target {
            ShutdownTarget::Manager(core) | ShutdownTarget::Group(core, _) => core,
        }
    }

    /// Reports whether the observed target has no active tracked work.
    #[must_use]
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        match &self.target {
            ShutdownTarget::Manager(core) => core.is_shutdown_empty(),
            ShutdownTarget::Group(_, group) => group.is_empty(),
        }
    }

    /// Reports whether the currently executing body is part of this target.
    pub(super) fn would_deadlock(&self) -> bool {
        match &self.target {
            ShutdownTarget::Manager(core) => is_waiting_on_manager(core),
            ShutdownTarget::Group(core, group) => is_waiting_on_group(core, group),
        }
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Source state used by reusable shutdown observers.

use super::SharedServiceGroupState;
use crate::supervisor::OperationSupervisor;

/// Identifies the state source observed during supervisor shutdown.
///
/// The source retains the handles needed to inspect shutdown progress without
/// changing the underlying supervisor or service-group lifecycle.
#[derive(Clone)]
pub(in crate::supervisor) enum ShutdownSource {
    /// Observes shutdown progress for the root supervisor.
    ///
    /// The handle is internal and does not count as a public supervisor owner.
    Supervisor(
        /// Internal root handle used to observe shutdown progress.
        OperationSupervisor,
    ),
    /// Observes shutdown progress for a service group and its root supervisor.
    ///
    /// Both handles are retained so observers can coordinate group state with
    /// the owning supervisor lifecycle.
    Service(
        /// Shared state for the service group being observed.
        SharedServiceGroupState,
        /// Internal handle for the supervisor that owns the service group.
        OperationSupervisor,
    ),
}

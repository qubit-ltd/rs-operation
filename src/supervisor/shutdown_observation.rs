// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shutdown report paired with an observer that can wait again.

use super::ShutdownObserver;
use super::ShutdownReport;

/// A shutdown snapshot paired with the observer used to collect it.
///
/// The snapshot may still contain pending work when a wait reaches its
/// deadline. Use the observer to wait again after that work has had more time
/// to settle.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::SupervisorLimits;
/// use qubit_operation::supervisor::OperationSupervisor;
///
/// let supervisor = OperationSupervisor::new(SupervisorLimits::default());
/// let observation = supervisor
///     .shutdown_observer()
///     .wait_blocking(std::time::Duration::ZERO);
/// assert!(observation.report.is_complete());
/// ```
#[derive(Clone)]
pub struct ShutdownObservation {
    /// Counts work still pending at the time of the observation.
    pub report: ShutdownReport,
    /// Allows another wait for work represented by this snapshot.
    pub observer: ShutdownObserver,
}

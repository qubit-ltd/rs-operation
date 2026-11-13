// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Rollback-safe reservation for root execution and result capacity.

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;

use super::internal::AdmittedTask;
use super::internal::Inner;
use super::internal::LeaseState;
use super::internal::ResultLease;
use super::internal::TaskLease;
use super::internal::leases::release_all;
use super::internal::util::lock;

/// A rollback-safe root capacity reservation.
///
/// The supervisor creates this value after reserving both execution capacity
/// and a result slot. Dropping it before transfer releases those reservations;
/// transferring it gives their ownership to the operation coordinator.
///
/// # Type Parameters
///
/// * `F` — the accepted operation body retained until the coordinator takes it.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::SupervisorLimits;
/// use qubit_operation::supervisor::OperationSupervisor;
///
/// let supervisor = OperationSupervisor::new(SupervisorLimits::default());
/// let admission = supervisor.try_admit().expect("root capacity is available");
/// assert!(!admission.is_queued());
/// ```
pub struct RootAdmission<F> {
    /// Shared supervisor state used to return reserved capacity on drop.
    pub(super) inner: Arc<Inner>,
    /// Per-admission queue and capacity state shared with both leases.
    pub(super) lease: Arc<Mutex<LeaseState>>,
    /// Accepted body, retained until ownership is transferred.
    pub(super) body: Option<F>,
    /// Whether lease ownership has already moved to the coordinator.
    pub(super) transferred: bool,
}

impl<F> RootAdmission<F> {
    /// Reports whether this accepted operation is waiting for an active-root
    /// slot.
    ///
    /// # Returns
    ///
    /// Returns `true` while the admission is queued and `false` once it owns an
    /// active-root slot.
    #[must_use]
    #[inline]
    pub fn is_queued(&self) -> bool {
        lock(&self.lease).queued
    }

    /// Transfer an accepted body and its independent execution/result leases to
    /// the coordinator.
    ///
    /// This is crate-private until the operation handle layer can bind both
    /// leases to the actual task and result registry. Dropping this
    /// reservation after transfer cannot release either lease.
    ///
    /// # Returns
    ///
    /// Returns `Some` with the body and both leases on the first successful
    /// transfer. Returns `None` if the reservation was already transferred or
    /// no body remains.
    pub(crate) fn transfer(&mut self) -> Option<AdmittedTask<F>> {
        if self.transferred {
            return None;
        }
        let body = self.body.take()?;
        self.transferred = true;
        Some(AdmittedTask {
            body,
            execution: Some(TaskLease {
                inner: Arc::clone(&self.inner),
                lease: Arc::clone(&self.lease),
            }),
            result: Some(ResultLease {
                inner: Arc::clone(&self.inner),
                lease: Arc::clone(&self.lease),
            }),
        })
    }
}

impl<F> fmt::Debug for RootAdmission<F> {
    /// Formats the queue and transfer state without exposing the operation
    /// body.
    ///
    /// # Parameters
    ///
    /// * `f` — formatter that receives the debug representation.
    ///
    /// # Returns
    ///
    /// Returns the formatter's result after writing the debug representation.
    ///
    /// # Errors
    ///
    /// Returns the formatting error if the formatter cannot accept the output.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootAdmission")
            .field("queued", &self.is_queued())
            .field("transferred", &self.transferred)
            .finish_non_exhaustive()
    }
}

impl<F> Drop for RootAdmission<F> {
    /// Releases both reservations unless ownership has already been
    /// transferred.
    fn drop(&mut self) {
        if self.transferred {
            return;
        }
        release_all(&self.inner, &self.lease);
    }
}

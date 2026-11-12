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

use super::internal::LeaseState;
use super::internal::AdmittedTask;
use super::internal::ResultLease;
use super::internal::TaskLease;
use super::internal::leases::release_all;
use super::internal::util::lock;

/// A rollback-safe root capacity reservation.
pub struct RootAdmission<F> {
    inner: Arc<Inner>,
    lease: Arc<Mutex<LeaseState>>,
    body: Option<F>,
    transferred: bool,
}

impl<F> RootAdmission<F> {
    /// Whether this accepted operation is waiting for an active-root slot.
    pub fn is_queued(&self) -> bool {
        lock(&self.lease).queued
    }

    /// Transfer an accepted body and its independent execution/result leases to
    /// the coordinator.
    ///
    /// This is crate-private until the operation handle layer can bind both
    /// leases to the actual task and result registry. Dropping this
    /// reservation after transfer cannot release either lease.
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootAdmission")
            .field("queued", &self.is_queued())
            .field("transferred", &self.transferred)
            .finish_non_exhaustive()
    }
}

impl<F> Drop for RootAdmission<F> {
    fn drop(&mut self) {
        if self.transferred {
            return;
        }
        release_all(&self.inner, &self.lease);
    }
}


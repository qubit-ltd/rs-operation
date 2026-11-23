// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::super::GroupPermit;
use super::super::lock;
use crate::v02::manager::core::SubmissionLease;
use crate::v02::operation::OutcomeDelivery;

/// Tracks settlement while retaining any new group capacity until both the
/// outcome and settlement are complete.
#[must_use = "retain the lease until the operation outcome and settlement finish"]
pub(crate) struct GroupLease {
    /// Underlying lifecycle lease that receives every operation event.
    inner: Arc<dyn SubmissionLease>,
    /// Group capacity permit retained until both completion signals arrive.
    permit: Mutex<Option<GroupPermit>>,
    /// Whether resource settlement has completed.
    settlement: AtomicBool,
    /// Whether outcome delivery or discard has completed.
    outcome: AtomicBool,
}

impl GroupLease {
    /// Wraps a submission lease with a permit released after both completion
    /// signals.
    ///
    /// # Parameters
    ///
    /// - `inner`: Submission lifecycle lease to which events are forwarded.
    /// - `permit`: Group capacity permit held until outcome and settlement
    ///   complete.
    ///
    /// # Returns
    ///
    /// A group lease that releases the permit after both completion events
    /// arrive.
    pub(crate) fn new(inner: Arc<dyn SubmissionLease>, permit: GroupPermit) -> Self {
        Self {
            inner,
            permit: Mutex::new(Some(permit)),
            settlement: AtomicBool::new(false),
            outcome: AtomicBool::new(false),
        }
    }

    /// Releases the permit once outcome delivery and settlement have both
    /// completed.
    fn maybe_release(&self) {
        if self.settlement.load(Ordering::Acquire) && self.outcome.load(Ordering::Acquire) {
            lock(&self.permit).take();
        }
    }
}

impl SubmissionLease for GroupLease {
    /// Forwards execution completion to the underlying lifecycle lease.
    #[inline]
    fn execution_finished(&self) {
        self.inner.execution_finished();
    }
    /// Forwards settlement completion and releases group capacity if the
    /// outcome has also been delivered or discarded.
    #[inline]
    fn settlement_finished(&self) {
        self.inner.settlement_finished();
        if let Some(permit) = lock(&self.permit).as_mut() {
            permit.settlement_finished();
        }
        self.settlement.store(true, Ordering::Release);
        self.maybe_release();
    }
    /// Forwards outcome delivery and releases group capacity if settlement is
    /// also complete.
    ///
    /// # Parameters
    ///
    /// - `self`: Lease receiving the outcome-delivery event.
    /// - `delivery`: Whether the outcome was taken or discarded.
    #[inline]
    fn outcome_delivered(&self, delivery: OutcomeDelivery) {
        self.inner.outcome_delivered(delivery);
        self.outcome.store(true, Ordering::Release);
        self.maybe_release();
    }
}

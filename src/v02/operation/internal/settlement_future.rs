// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use super::operation_core::OperationCore;
use crate::v02::internal::notify::Notified;
use crate::v02::report::SettlementReport;

/// Waits for an operation's settlement report without blocking a worker.
pub(in crate::v02::operation) struct SettlementFuture<'a> {
    /// Operation state whose settlement is observed.
    pub(in crate::v02::operation::internal) core: &'a OperationCore,
    /// Registered notification used to wake polling after state changes.
    pub(in crate::v02::operation::internal) notification: Option<Notified>,
}

impl Future for SettlementFuture<'_> {
    /// Settlement report produced when the operation becomes settled.
    type Output = Arc<SettlementReport>;

    /// Checks for settlement and registers a notification if it is still
    /// pending.
    ///
    /// # Parameters
    ///
    /// - `self`: The future being polled.
    /// - `context`: Task context used to register the current waker.
    ///
    /// # Returns
    ///
    /// `Ready` with the settlement report, or `Pending` until state changes.
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let generation = self.core.changed.generation();
            if let Some(report) = self.core.settlement() {
                return Poll::Ready(report);
            }
            if self.notification.is_none() {
                self.notification = Some(self.core.changed.notified_since(generation));
            }
            match Pin::new(self.notification.as_mut().expect("notification was set")).poll(context) {
                Poll::Ready(()) => self.notification = None,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

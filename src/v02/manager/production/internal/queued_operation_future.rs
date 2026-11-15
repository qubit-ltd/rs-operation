// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;

use super::queued_operation_guard::QueuedOperationGuard;

/// Polls an async operation while tracking its executor queue occupancy.
///
/// The guard starts accounting on the first poll and releases it when the
/// future completes or is cancelled.
pub(super) struct QueuedOperationFuture {
    /// Operation future polled by the async executor.
    pub(super) future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    /// Queue accounting released on first poll or future cancellation.
    pub(super) guard: Option<QueuedOperationGuard>,
}

impl Future for QueuedOperationFuture {
    /// Completes when the queued operation future completes or is cancelled.
    type Output = ();

    /// Starts queue accounting on first poll, then polls the operation future.
    ///
    /// # Parameters
    ///
    /// - `self`: The pinned wrapper whose inner future is advanced.
    /// - `cx`: The executor context used to register the inner future's waker.
    ///
    /// # Returns
    ///
    /// The operation future's current polling state.
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        if let Some(guard) = self.guard.as_mut() {
            guard.start();
        }
        self.future.as_mut().poll(cx)
    }
}

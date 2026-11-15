// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::future::Future;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

/// Future adapter that converts a panic during polling into an owned payload.
///
/// # Type Parameters
///
/// `F` is the inner future whose poll operation is protected by `catch_unwind`.
#[must_use]
pub(in crate::v02::manager::core) struct CatchUnwindFuture<F>(
    /// Pinned inner future that is polled by this adapter.
    pub(in crate::v02::manager::core) Pin<Box<F>>,
);

impl<F: Future> Future for CatchUnwindFuture<F> {
    /// Inner future output or the payload from a panic during polling.
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    /// Polls the inner future under `catch_unwind`, preserving pending
    /// behavior.
    ///
    /// # Parameters
    ///
    /// - `context`: task context used to poll the inner future and register its
    ///   waker.
    ///
    /// # Returns
    ///
    /// `Pending` is propagated, `Ready(Ok(_))` carries the inner output, and
    /// `Ready(Err(_))` carries a panic payload.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.get_mut();
        match panic::catch_unwind(AssertUnwindSafe(|| future.0.as_mut().poll(context))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

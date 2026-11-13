// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Panic boundary for polling an operation body.

use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

/// Future wrapper that converts panics during polling into an error result.
///
/// # Type Parameters
///
/// - `F`: The future polled inside the panic boundary. The `Future` bound is
///   required only when this wrapper is used as a future.
pub(crate) struct CatchUnwind<F> {
    /// Pinned future polled inside the panic boundary.
    future: Pin<Box<F>>,
}

impl<F> CatchUnwind<F> {
    /// Pins a future for panic-contained polling.
    ///
    /// # Parameters
    ///
    /// - `future`: The future to pin and poll inside the panic boundary.
    ///
    /// # Returns
    ///
    /// A wrapper that polls `future` and reports unwind panics as an error.
    pub(crate) fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for CatchUnwind<F> {
    /// The inner output or unit marker when polling panics.
    type Output = Result<F::Output, ()>;

    /// Polls the inner future while converting unwind panics to `Err(())`.
    ///
    /// # Parameters
    ///
    /// - `self`: The pinned wrapper containing the future to poll.
    /// - `cx`: The task context passed to the inner future.
    ///
    /// # Returns
    ///
    /// `Pending` while the inner future is pending, `Ready(Ok(output))` when it
    /// completes, or `Ready(Err(()))` if polling unwinds.
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

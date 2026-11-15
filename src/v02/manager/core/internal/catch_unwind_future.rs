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

/// Owned payload returned when a protected user operation unwinds.
///
/// # Type Parameters
///
/// This alias has no type parameters.
pub(crate) type PanicPayload = Box<dyn Any + Send>;

/// Owns a pinned future whose poll and final drop can be caught separately.
///
/// # Type Parameters
///
/// - `F`: Future stored in a pinned allocation.
pub(crate) struct CatchUnwindFuture<F>(
    /// Pinned allocation that can be moved as a handle and taken exactly once.
    Option<Pin<Box<F>>>,
);

impl<F> CatchUnwindFuture<F> {
    /// Pins a future in a takeable allocation without moving its value.
    ///
    /// # Parameters
    ///
    /// - `future`: Future value to pin and own.
    pub(crate) fn new(future: F) -> Self {
        Self(Some(Box::pin(future)))
    }

    /// Polls the pinned future and returns any panic payload without unwinding.
    ///
    /// # Parameters
    ///
    /// - `context`: Task context passed to the pinned future.
    pub(crate) fn poll(&mut self, context: &mut Context<'_>) -> Result<Poll<F::Output>, PanicPayload>
    where
        F: Future,
    {
        let future = self.0.as_mut().expect("future is present while polling");
        panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context)))
    }

    /// Takes the pinned allocation before dropping it and reports a drop panic.
    pub(crate) fn drop_inner(&mut self) -> Result<(), PanicPayload> {
        let Some(future) = self.0.take() else {
            return Ok(());
        };
        panic::catch_unwind(AssertUnwindSafe(|| drop(future)))
    }
}

impl<F> Drop for CatchUnwindFuture<F> {
    /// Contains a remaining future's drop panic during cancellation or
    /// teardown.
    fn drop(&mut self) {
        if let Err(payload) = self.drop_inner() {
            dispose_panic_payload(payload);
        }
    }
}

/// Disposes of a panic payload while preventing its destructor from unwinding.
///
/// # Parameters
///
/// - `payload`: Panic payload to dispose of inside a second unwind boundary.
pub(crate) fn dispose_panic_payload(payload: PanicPayload) {
    if let Err(secondary) = panic::catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        // A panic payload whose destructor also panics cannot be safely freed.
        std::mem::forget(secondary);
    }
}

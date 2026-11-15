// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// A timer registration is an owning cancellation token.
///
/// It is returned by [`crate::ClockSource::register_deadline`] and consumes
/// itself when canceled.
///
/// # Examples
///
/// ```
/// use qubit_operation::{ClockSource, MonotonicTime};
/// use std::task::Waker;
///
/// fn cancel_deadline(clock: &dyn ClockSource, at: MonotonicTime, wake: Waker) {
///     clock.register_deadline(at, wake).cancel();
/// }
/// ```
pub trait TimerRegistration: Send {
    /// Cancels the registered wake and consumes this registration token.
    ///
    /// # Parameters
    ///
    /// - `self`: Owned registration whose scheduled wake is canceled.
    fn cancel(self: Box<Self>);
}

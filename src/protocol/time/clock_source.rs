// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::task::Waker;

use super::TimerRegistration;
use crate::report::MonotonicTime;

/// Clock domain and timer source shared by deadline users.
///
/// # Examples
///
/// The clock is supplied by the operation runtime. This helper registers a
/// deadline using the time and wakeup supplied by its caller:
///
/// ```
/// use qubit_operation::protocol::ClockSource;
/// use qubit_operation::report::MonotonicTime;
/// use std::task::Waker;
///
/// fn register(
///     clock: &dyn ClockSource,
///     at: MonotonicTime,
///     wake: Waker,
/// ) {
///     let registration = clock.register_deadline(at, wake);
///     registration.cancel();
/// }
/// ```
pub trait ClockSource: Send + Sync {
    /// Read the current time in this source's stable clock domain.
    ///
    /// # Returns
    ///
    /// The current domain-local monotonic time.
    fn now(&self) -> MonotonicTime;

    /// Register a wakeup; the registration is cancelled by consuming its
    /// handle. If `at` has elapsed before or during registration, the
    /// implementation must still wake `wake`; it must not lose the deadline
    /// between checking the current time and installing the timer.
    ///
    /// # Parameters
    ///
    /// - `at`: deadline in this clock source's domain. A time from another
    ///   domain is not comparable and must not be interpreted as a local tick.
    /// - `wake`: task wakeup to invoke when the deadline is reached or has
    ///   already elapsed.
    ///
    /// # Returns
    ///
    /// A handle that cancels this registration when consumed.
    #[must_use = "the registration handle is required to cancel this deadline"]
    fn register_deadline(&self, at: MonotonicTime, wake: Waker) -> Box<dyn TimerRegistration>;
}

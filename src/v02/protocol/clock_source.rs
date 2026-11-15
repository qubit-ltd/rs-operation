// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::task::Waker;

use super::monotonic_time::MonotonicTime;
use super::timer_registration::TimerRegistration;

/// Monotonic clock interface used by deadlines and asynchronous waits.
///
/// Supply an implementation that keeps time points in one stable clock domain
/// and can wake registered tasks when a deadline is reached.
///
/// # Examples
///
/// A runtime or application can pass its configured clock to code that needs
/// the current time:
///
/// ```
/// use qubit_operation::{ClockSource, MonotonicTime};
///
/// fn current_time(clock: &dyn ClockSource) -> MonotonicTime {
///     clock.now()
/// }
/// ```
pub trait ClockSource: Send + Sync {
    /// Returns the current monotonic time in this clock's domain.
    ///
    /// Successive calls must not move backward within that domain.
    ///
    /// # Parameters
    ///
    /// - `self`: Clock source providing the current time.
    ///
    /// # Returns
    ///
    /// The current monotonic time in the clock's stable domain.
    fn now(&self) -> MonotonicTime;

    /// Registers a task to be woken when the given time point is reached.
    ///
    /// # Parameters
    ///
    /// - `self`: Clock source that schedules the deadline.
    /// - `at`: deadline in this clock's domain.
    /// - `wake`: task waker to notify when the deadline is reached.
    ///
    /// # Returns
    ///
    /// A registration that can cancel the scheduled wake through
    /// [`TimerRegistration::cancel`].
    fn register_deadline(&self, at: MonotonicTime, wake: Waker) -> Box<dyn TimerRegistration>;
}

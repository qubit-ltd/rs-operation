// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Cancellation handle for one deadline registration.
///
/// # Examples
///
/// A clock runtime returns this handle when a deadline is registered. The
/// caller cancels that registration by consuming the returned handle:
///
/// ```
/// use qubit_operation::protocol::TimerRegistration;
///
/// fn cancel_registration(registration: Box<dyn TimerRegistration>) {
///     registration.cancel();
/// }
/// ```
pub trait TimerRegistration: Send + 'static {
    /// Cancel this registration only; it must not affect a later registration.
    fn cancel(self: Box<Self>);
}

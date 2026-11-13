// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Narrow runtime-side contract for task notification and completion.
///
/// # Examples
///
/// An external driver can implement the notification and liveness contract:
///
/// ```
/// use std::sync::atomic::AtomicUsize;
/// use std::sync::atomic::Ordering;
///
/// use qubit_operation::driver::ExternalDriver;
///
/// struct RuntimeDriver {
///     wake_count: AtomicUsize,
/// }
///
/// impl ExternalDriver for RuntimeDriver {
///     fn wake(&self, _task_id: u64) {
///         self.wake_count.fetch_add(1, Ordering::Relaxed);
///     }
///
///     fn is_alive(&self) -> bool {
///         true
///     }
/// }
///
/// let runtime = RuntimeDriver {
///     wake_count: AtomicUsize::new(0),
/// };
/// runtime.wake(42);
/// assert_eq!(runtime.wake_count.load(Ordering::Relaxed), 1);
/// assert!(runtime.is_alive());
/// ```
pub trait ExternalDriver: Send + Sync {
    /// Request a non-blocking wake of an accepted runtime task.
    ///
    /// # Parameters
    ///
    /// - `task_id`: Identifier of the task accepted by this runtime. The call
    ///   requests a wake without waiting for the task to run.
    fn wake(&self, task_id: u64);

    /// Return whether the runtime remains capable of driving accepted tasks.
    ///
    /// # Returns
    ///
    /// `true` when the runtime can continue driving accepted tasks; otherwise,
    /// `false`.
    #[must_use]
    fn is_alive(&self) -> bool;
}

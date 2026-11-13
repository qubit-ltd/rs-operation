// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Opaque identity for a task accepted by a [`super::PortableDriver`].
///
/// The numeric identity is stable for the task's lifetime and is not a task
/// completion signal. Construct handles through
/// [`super::PortableDriver::spawn`].
///
/// # Examples
///
/// ```
/// use qubit_operation::driver::PortableDriver;
/// use qubit_operation::driver::PortableDriverConfig;
///
/// let driver = PortableDriver::new(PortableDriverConfig {
///     worker_count: 1,
///     task_capacity: 4,
///     polls_per_turn: 8,
/// })?;
/// let task = driver.spawn(async {})?;
/// assert_ne!(task.as_u64(), 0);
/// # Ok::<(), qubit_operation::driver::PortableDriverError>(())
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TaskHandle(
    /// Numeric identity used internally to associate wakeups with this task.
    pub(super) u64,
);

impl TaskHandle {
    /// Return the stable identifier used by an external driver adapter.
    #[must_use]
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

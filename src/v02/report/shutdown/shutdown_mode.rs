// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Shutdown behavior selected for a manager or group.
///
/// # Examples
///
/// ```
/// use qubit_operation::ShutdownMode;
///
/// let mode = ShutdownMode::Drain;
/// let description = match mode {
///     ShutdownMode::Drain => "wait for admitted work",
///     ShutdownMode::Cancel => "request cancellation",
/// };
/// assert_eq!(description, "wait for admitted work");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownMode {
    /// Let admitted work finish before shutdown completes.
    Drain,
    /// Request cancellation of active work during shutdown.
    Cancel,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The task and reason returned when a blocking executor rejects work.

use super::DriverError;

/// Preserves the rejected one-shot task so its caller can recover it.
pub(crate) struct BlockingRejected {
    /// Identifies why the executor rejected the task.
    pub error: DriverError,
    /// The blocking task that the executor did not accept.
    pub task: Box<dyn FnOnce() + Send + 'static>,
}

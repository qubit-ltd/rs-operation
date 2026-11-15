// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ThreadPoolPermit;

/// Pre-reserved capacity for one blocking resource destructor.
pub(crate) struct CleanupPermit {
    /// Blocking pool slot retained until the destructor is submitted.
    pub(in crate::v02::executor) permit: ThreadPoolPermit,
}

impl CleanupPermit {
    /// Transfers `value` to a cleanup worker, where its destructor runs.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Resource value sent to the cleanup worker.
    ///
    /// # Parameters
    ///
    /// - `self`: Permit reserving a cleanup worker slot.
    /// - `value`: Resource value whose destructor runs on the worker.
    pub(crate) fn drop_value<T: Send + 'static>(self, value: T) {
        self.permit.submit(move || drop(value));
    }
}

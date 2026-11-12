// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Narrow runtime-side contract for task notification and completion.
pub trait ExternalDriver: Send + Sync {
    /// Request a non-blocking wake of an accepted runtime task.
    fn wake(&self, task_id: u64);

    /// Return whether the runtime remains capable of driving accepted tasks.
    fn is_alive(&self) -> bool;
}

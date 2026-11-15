// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Manager-wide capacity pools.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManagerCapacity {
    /// Concurrent root operations that have not completed execution.
    ActiveRoots,
    /// Root operations waiting in executor queues.
    QueuedRoots,
    /// Concurrent admission waiters held while capacity is unavailable.
    AdmissionWaiters,
    /// Outcomes that have not yet been consumed by callers.
    UnconsumedOutcomes,
    /// Resource cleanup jobs executing concurrently.
    CleanupConcurrency,
    /// Diagnostic events retained across active scopes.
    DiagnosticEvents,
}

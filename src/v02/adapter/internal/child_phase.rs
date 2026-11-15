// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Admission and activation phases shared by a child and its start permit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::v02::adapter) enum ChildPhase {
    /// Child payload and capacity have been attached but not committed.
    Attached,
    /// Commit validation is in progress.
    Committing,
    /// Commit succeeded and activation may be attempted.
    Committed,
    /// Activation validation is in progress.
    Activating,
    /// Activation succeeded and the external task was started.
    Activated,
    /// Admission was rejected or its owner was dropped.
    Abandoned,
}

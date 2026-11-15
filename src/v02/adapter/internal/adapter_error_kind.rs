// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Why an external child admission failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdapterErrorKind {
    /// The parent operation rejected admission because stopping had begun.
    Stopped,
    /// The owning operation scope had already ended.
    ScopeEnded,
    /// The reservation was already attached to a child.
    AlreadyAttached,
    /// The child had already committed or activated its reservation.
    AlreadyCommitted,
}

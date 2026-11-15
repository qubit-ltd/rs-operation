// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! StopState implementation.

use std::sync::Weak;
use std::task::Waker;

use super::StopNode;
use crate::v02::report::StopReason;

/// Mutable cancellation state shared by an operation context and its nodes.
pub(in crate::v02::context) struct StopState {
    /// First stop reason accepted by the node.
    pub(in crate::v02::context) reason: Option<StopReason>,
    /// Weak references to child nodes that inherit parent stopping.
    pub(in crate::v02::context) children: Vec<Weak<StopNode>>,
    /// Registered observers paired with their cancellation identifiers.
    pub(in crate::v02::context) waiters: Vec<(u64, Waker)>,
    /// Identifier assigned to the next registered observer.
    pub(in crate::v02::context) next_waiter: u64,
}

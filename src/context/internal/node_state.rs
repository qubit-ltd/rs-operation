// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Weak;
use std::task::Waker;

use super::Node;
use crate::protocol::StopReason;

/// Mutable cancellation data protected by a node's mutex.
pub(in crate::context) struct NodeState {
    /// First stop cause accepted by this node, if any.
    pub(in crate::context) stop_reason: Option<StopReason>,
    /// Weak links to descendants that should inherit future stop requests.
    pub(in crate::context) children: Vec<Weak<Node>>,
    /// Registered stop observers paired with their unique waiter identifiers.
    pub(in crate::context) stop_waiters: Vec<(u64, Waker)>,
    /// Identifier assigned to the next registered observer.
    pub(in crate::context) next_waiter: u64,
}

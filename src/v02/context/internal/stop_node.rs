// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;

use super::StopState;
use super::lock;
use crate::v02::report::StopReason;

/// A cancellation node shared with its descendants.
pub(in crate::v02::context) struct StopNode {
    /// Stop reason, descendant references, and observers for this node.
    pub(in crate::v02::context) state: Mutex<StopState>,
}

impl StopNode {
    /// Creates a stop node with no reason, descendants, or observers.
    pub(in crate::v02::context) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(StopState {
                reason: None,
                children: Vec::new(),
                waiters: Vec::new(),
                next_waiter: 1,
            }),
        })
    }

    /// Returns the first stop reason recorded for this node, if any.
    pub(in crate::v02::context) fn reason(&self) -> Option<StopReason> {
        lock(&self.state).reason
    }

    /// Records the first stop reason and propagates stopping to descendants.
    ///
    /// # Parameters
    ///
    /// * `reason` is stored for this node and passed to descendants as
    ///   `ParentStopped`.
    pub(in crate::v02::context) fn request_stop(&self, reason: StopReason) -> bool {
        let (children, waiters) = {
            let mut state = lock(&self.state);
            if state.reason.is_some() {
                return false;
            }
            state.reason = Some(reason);
            state.children.retain(|child| child.strong_count() != 0);
            let children = state.children.iter().filter_map(Weak::upgrade).collect::<Vec<_>>();
            let waiters = std::mem::take(&mut state.waiters);
            (children, waiters)
        };
        for (_, waiter) in waiters {
            waiter.wake();
        }
        for child in children {
            child.request_stop(StopReason::ParentStopped);
        }
        true
    }
}

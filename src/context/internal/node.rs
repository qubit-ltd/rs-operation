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

use super::NodeState;
use crate::protocol::StopReason;

/// Shared cancellation state for a context and its descendants.
pub(in crate::context) struct Node {
    /// Mutex protecting the first stop cause, child links, and registered
    /// waiters.
    pub(in crate::context) state: Mutex<NodeState>,
}

impl Node {
    /// Create a running node with no children or stop waiters.
    pub(in crate::context) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(NodeState {
                stop_reason: None,
                children: Vec::new(),
                stop_waiters: Vec::new(),
                next_waiter: 1,
            }),
        })
    }

    /// Return the first accepted stop cause, if readable.
    ///
    /// Returns `None` before a cause is recorded or when the mutex is poisoned.
    pub(in crate::context) fn stop_reason(&self) -> Option<StopReason> {
        self.state.lock().ok().and_then(|state| state.stop_reason)
    }

    /// Record the first stop cause, wake observers, and propagate to children.
    ///
    /// Returns `true` only when this call records the cause. Returns `false`
    /// when a cause already exists or the mutex is poisoned.
    ///
    /// # Parameters
    ///
    /// - `reason`: Cause recorded for this node and propagated to its
    ///   descendants.
    pub(in crate::context) fn request_stop(&self, reason: StopReason) -> bool {
        let (children, waiters) = {
            let Ok(mut state) = self.state.lock() else {
                return false;
            };
            if state.stop_reason.is_some() {
                return false;
            }
            state.stop_reason = Some(reason);
            state.children.retain(|child| child.strong_count() > 0);
            let children = state.children.iter().filter_map(Weak::upgrade).collect::<Vec<_>>();
            (children, std::mem::take(&mut state.stop_waiters))
        };
        for waker in waiters.into_iter().map(|(_, waker)| waker) {
            waker.wake();
        }
        for child in children {
            child.request_stop(StopReason::ParentStopped);
        }
        true
    }
}

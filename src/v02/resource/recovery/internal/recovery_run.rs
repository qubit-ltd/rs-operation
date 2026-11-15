// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::CleanupFailure;
use crate::v02::report::RecoveryPhase;
use crate::v02::resource::recovery::RecoveryAction;
use crate::v02::resource::recovery::RecoveryCore;
use crate::v02::resource::recovery::RecoveryPayload;
use crate::v02::resource::recovery::lock;

/// State owned by one in-flight retry until its outcome is published.
///
/// The run retains the recovery payload, action, and optional executor permit.
/// If the future is abandoned, dropping the run marks the matching retry
/// unconfirmed while preserving the owner payload in its recovery core.
pub(in crate::v02::resource::recovery) struct RecoveryRun {
    /// Core whose phase and retained owner this attempt updates.
    pub(in crate::v02::resource::recovery) core: Arc<RecoveryCore>,
    /// Generation token that prevents an old attempt from changing newer state.
    pub(in crate::v02::resource::recovery) generation: u64,
    /// Shared owner payload kept alive until this retry finishes or is dropped.
    pub(in crate::v02::resource::recovery) payload: Option<RecoveryPayload>,
    /// Action that performs cleanup for this retry.
    pub(in crate::v02::resource::recovery) action: Arc<dyn RecoveryAction>,
    /// Whether this run has already published or abandoned its outcome.
    pub(in crate::v02::resource::recovery) completed: bool,
    /// Optional capacity reserved for scheduling the async cleanup action.
    pub(in crate::v02::resource::recovery) permit: Option<crate::v02::executor::PortablePermit>,
}

impl RecoveryRun {
    /// Records an unconfirmed outcome after an action panic without releasing
    /// the payload or action retained by the recovery core.
    pub(in crate::v02::resource::recovery) fn complete_unconfirmed(
        &mut self,
        panic: Box<dyn std::any::Any + Send + 'static>,
    ) {
        self.core.mark_unconfirmed(self.generation, panic);
        self.completed = true;
        self.payload.take();
    }

    /// Applies the cleanup result if this run still owns the active generation.
    ///
    /// Success resolves the group and releases retained owner state outside
    /// the state lock. Failure records the error and returns the group to its
    /// pending phase for another retry.
    ///
    /// # Parameters
    ///
    /// * `self` — in-flight run publishing its cleanup result.
    /// * `result` — success confirmation or the cleanup failure to retain.
    pub(in crate::v02::resource::recovery) fn complete(&mut self, result: Result<(), CleanupFailure>) {
        let mut released_payload = None;
        let mut released_action = None;
        let mut old_failures = Vec::new();
        {
            let mut state = lock(&self.core.state);
            if state.phase != RecoveryPhase::Running || state.generation != self.generation {
                self.completed = true;
                return;
            }
            match result {
                Ok(()) => {
                    state.phase = RecoveryPhase::Resolved;
                    old_failures = std::mem::take(&mut state.failures);
                    released_action = state.action.take();
                    released_payload = state.payload.take();
                }
                Err(failure) => {
                    state.phase = RecoveryPhase::Pending;
                    state.failures.push(failure);
                }
            }
        }
        self.completed = true;
        self.payload.take();
        // A resource's last Drop may call arbitrary user code.
        drop(released_payload);
        drop(released_action);
        drop(old_failures);
        if self.core.snapshot().phase == RecoveryPhase::Resolved {
            self.core.notify_terminal(false);
        }
    }
}

impl Drop for RecoveryRun {
    /// Marks an abandoned active run unconfirmed and retains its owner.
    ///
    /// The generation check prevents an obsolete run from changing a newer
    /// attempt. The payload clone owned by this run is dropped after releasing
    /// the state lock.
    ///
    /// # Parameters
    ///
    /// * `self` — run being dropped before publishing a cleanup result.
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = lock(&self.core.state);
        if state.phase == RecoveryPhase::Running && state.generation == self.generation {
            state.phase = RecoveryPhase::Unconfirmed;
        }
        drop(state);
        self.payload.take();
    }
}

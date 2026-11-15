// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::AsyncRecoveryOwner;
use crate::v02::report::CleanupFailure;
use crate::v02::resource::recovery::RecoveryAction;
use crate::v02::resource::recovery::RecoveryFuture;
use crate::v02::resource::recovery::RecoveryPayload;
use crate::v02::resource::recovery::lock;

/// Retries asynchronous cleanup through the owner retained for recovery.
///
/// A failed attempt puts the resource back in the owner for a later retry;
/// successful release or transfer drops its resource-capacity registration.
///
/// # Type Parameters
///
/// * `T` — thread-safe resource value retained by the recovery owner.
pub(in crate::v02::resource::recovery) struct AsyncRecoveryAction<T: Send + Sync + 'static>(
    /// Owner holding the resource, cleanup policy, and capacity registration.
    pub(super) Arc<AsyncRecoveryOwner<T>>,
);

impl<T: Send + Sync + 'static> RecoveryAction for AsyncRecoveryAction<T> {
    /// Runs the retained owner's cleanup policy for one recovery attempt.
    ///
    /// A released or transferred resource drops its registration. A failed
    /// cleanup restores the resource to the owner so recovery retains it.
    ///
    /// # Parameters
    ///
    /// * `self` — action bound to the retained resource owner.
    /// * `_payload` — generic recovery payload; this action uses its typed
    ///   owner.
    ///
    /// # Returns
    ///
    /// A future that resolves to success after release or transfer, or to the
    /// cleanup failure after restoring the resource for another attempt.
    ///
    /// # Panic Handling
    ///
    /// A cleanup policy panic is caught by the recovery attempt and reported as
    /// `Unconfirmed`. If cleanup construction panics after consuming the
    /// resource, that value cannot be recovered, but the recovery owner and its
    /// capacity registration remain retained. A poll panic retains the
    /// unfinished future without dropping it.
    fn retry(&self, _payload: RecoveryPayload) -> RecoveryFuture {
        let owner = Arc::clone(&self.0);
        Box::pin(async move {
            let resource = lock(&owner.resource).take().ok_or_else(|| CleanupFailure {
                resource_id: owner.resource_id,
                stage: crate::v02::report::CleanupStage::AsyncRelease,
                summary: crate::v02::report::DiagnosticSummary::new("recovery owner is unavailable"),
                source: None,
            })?;
            let context =
                crate::v02::context::CleanupContext::new(Default::default()).expect("empty cleanup context is valid");
            match Arc::clone(&owner.policy).cleanup(resource, context).await {
                crate::v02::resource::cleanup::CleanupOutcome::Released
                | crate::v02::resource::cleanup::CleanupOutcome::Transferred(_) => {
                    drop(lock(&owner.registration).take());
                    Ok(())
                }
                crate::v02::resource::cleanup::CleanupOutcome::Failed { failure, resource } => {
                    *lock(&owner.resource) = Some(resource);
                    Err(failure)
                }
            }
        })
    }
}

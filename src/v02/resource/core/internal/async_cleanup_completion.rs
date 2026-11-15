// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::sync::Arc;
use std::sync::Mutex;

use super::super::ScopeCompletion;
use crate::v02::report::CleanupEvidence;
use crate::v02::report::ResourceId;
use crate::v02::resource::cleanup::AsyncCleanupCompletion;
use crate::v02::resource::cleanup::CleanupOutcome;
use crate::v02::resource::recovery::RecoveryCore;

/// Settles a scope after observing the result of an asynchronous cleanup.
///
/// A released resource records confirmed cleanup; a transfer records its
/// receipt; and a failed cleanup creates a recovery handle that retains the
/// resource. An unconfirmed result also creates a recovery handle when it
/// carries an unconfirmed cleanup capsule. The result is consumed so its
/// resource, registration, and recovery policy move to their final owners.
///
/// # Parameters
///
/// - `resource_id`: Resource whose cleanup result is being settled.
/// - `completion`: Shared scope completion state updated with evidence,
///   failures, or a recovery handle.
/// - `result`: Asynchronous cleanup result and any resource state retained for
///   recovery.
///
/// # Type Parameters
///
/// - `T`: Resource value type retained by a failed or unconfirmed cleanup.
pub(in crate::v02::resource::core) fn finish_async_cleanup<T: Send + Sync + 'static>(
    resource_id: ResourceId,
    completion: Arc<ScopeCompletion>,
    mut result: AsyncCleanupCompletion<T>,
) {
    let outcome = result.outcome.take();
    match outcome {
        Some(CleanupOutcome::Released) => {
            drop(result.registration.take());
            completion.complete(resource_id, Some(CleanupEvidence::Confirmed), None, None);
        }
        Some(CleanupOutcome::Transferred(receipt)) => {
            drop(result.registration.take());
            let _ = completion.record_transfer(receipt);
            completion.complete(resource_id, None, None, None);
        }
        Some(CleanupOutcome::Failed { failure, resource }) => {
            let registration = result.registration.take().expect("failed cleanup retains registration");
            let (payload, action) =
                crate::v02::resource::recovery::async_owner(resource_id, resource, result.policy, registration);
            let core = Arc::new(RecoveryCore::new(vec![resource_id], payload, action));
            completion.complete(
                resource_id,
                None,
                Some(failure),
                Some(crate::v02::resource::recovery::RecoveryHandle::new(core)),
            );
        }
        None => {
            if let Some(unconfirmed) = result.unconfirmed.take() {
                let payload = Arc::new(Mutex::new(Some(unconfirmed))) as Arc<dyn Any + Send + Sync>;
                let core = Arc::new(RecoveryCore::new_unconfirmed(vec![resource_id], payload));
                completion.complete(
                    resource_id,
                    None,
                    None,
                    Some(crate::v02::resource::recovery::RecoveryHandle::new(core)),
                );
            }
        }
    }
}

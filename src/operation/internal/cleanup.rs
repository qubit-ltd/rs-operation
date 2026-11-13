// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Recursive resource cleanup for completed operation scopes.

use core::future::Future;
use core::pin::Pin;
use core::task::Poll;

use super::CleanupFacts;
use crate::protocol::CleanupContext;
use crate::report::CleanupFailureSummary;
use crate::report::CleanupOutcome;
use crate::report::ClockDomainId;
use crate::report::MonotonicTime;
use crate::report::ResourcePhase;
use crate::resource::CleanupObservation;
use crate::resource::CleanupScheduler;
use crate::scope::OperationScope;

/// Cleans child scopes before this scope and aggregates their cleanup facts.
///
/// # Parameters
///
/// * `scope` — root of the scope subtree whose resources must be settled.
///
/// # Returns
///
/// A future that resolves to aggregate cleanup facts after every reachable
/// child scope and the supplied scope have been processed.
pub(crate) fn cleanup_scope_tree<'a>(
    scope: &'a OperationScope,
) -> Pin<Box<dyn Future<Output = CleanupFacts> + Send + 'a>> {
    Box::pin(async move {
        let mut all = CleanupFacts {
            phase: ResourcePhase::Released,
            failures: Vec::new(),
            transfers: Vec::new(),
            unconfirmed: 0,
        };
        for child in scope.child_scopes() {
            let child = cleanup_scope_tree(&child).await;
            all.failures.extend(child.failures);
            all.transfers.extend(child.transfers);
            all.unconfirmed += child.unconfirmed;
            if child.phase == ResourcePhase::Unconfirmed {
                all.phase = ResourcePhase::Unconfirmed;
            }
            if child.phase == ResourcePhase::RecoveryRequired {
                all.phase = ResourcePhase::RecoveryRequired;
            }
        }
        let own = cleanup_resources(scope).await;
        all.failures.extend(own.failures);
        all.transfers.extend(own.transfers);
        all.unconfirmed += own.unconfirmed;
        if own.phase == ResourcePhase::Unconfirmed {
            all.phase = ResourcePhase::Unconfirmed;
        }
        if own.phase == ResourcePhase::RecoveryRequired {
            all.phase = ResourcePhase::RecoveryRequired;
        }
        all
    })
}

/// Drives cleanup for resources registered directly with one scope.
///
/// The scheduler bounds concurrent cleanup work. Resources whose cleanup
/// cannot be confirmed are counted as unconfirmed rather than reported as
/// released.
///
/// # Parameters
///
/// * `scope` — scope whose directly owned resources are to be settled.
///
/// # Returns
///
/// Facts describing confirmed releases, transfers, failures requiring
/// recovery, and resources whose final state remains unconfirmed.
async fn cleanup_resources(scope: &OperationScope) -> CleanupFacts {
    let registry = scope.resource_registry();
    let scheduler: CleanupScheduler = scope.cleanup_scheduler();
    let recovery = scope.recovery_registry();
    let mut failures = Vec::new();
    let mut transfers = Vec::new();
    let mut unconfirmed: usize = 0;
    loop {
        let seen = scope.resource_epoch();
        let permit = match std::future::poll_fn(|cx| scheduler.poll_acquire(cx)).await {
            Some(permit) => permit,
            None => {
                unconfirmed = registry.pending_cleanup_count().max(1);
                break;
            }
        };
        let Some(ready) = registry.take_ready_cleanup() else {
            drop(permit);
            let pending = registry.pending_cleanup_count();
            if pending == 0 {
                break;
            }
            if !failures.is_empty() || unconfirmed > 0 {
                unconfirmed = unconfirmed.max(pending);
                break;
            }
            scope.wait_for_resource_change(seen).await;
            continue;
        };
        let id = ready.resource_id();
        let (_, owner) = ready.into_parts();
        let future = owner.begin(CleanupContext { deadline: None });
        let mut task = match scheduler.schedule_with_permit(id, permit, future) {
            Ok(task) => task,
            Err(_future) => {
                unconfirmed += 1;
                continue;
            }
        };
        let now = MonotonicTime::new(ClockDomainId(0), 0);
        let observation = std::future::poll_fn(|cx| match task.poll_at(cx, now) {
            Poll::Ready(CleanupObservation::Pending) => Poll::Pending,
            other => other,
        })
        .await;
        match observation {
            CleanupObservation::Settled(Ok(CleanupOutcome::Cleaned)) => {
                let _ = registry.confirm_released(id);
            }
            CleanupObservation::Settled(Ok(CleanupOutcome::Transferred(receipt))) => {
                if registry.confirm_transferred(id, receipt).is_ok() {
                    transfers.push(receipt);
                } else {
                    unconfirmed += 1;
                }
            }
            CleanupObservation::Settled(Ok(CleanupOutcome::FailedWithRecovery(failure, owner))) => {
                failures.push(CleanupFailureSummary {
                    resource_id: failure.resource_id,
                    stage: failure.stage,
                    summary: failure.summary,
                });
                match recovery.register(id, owner) {
                    Ok(key) => {
                        scope.save_recovery_key(key);
                        if registry.accept_recovery(id).is_err() {
                            unconfirmed += 1;
                        }
                    }
                    Err(_owner) => unconfirmed += 1,
                }
            }
            CleanupObservation::Settled(Err(_))
            | CleanupObservation::Unconfirmed { .. }
            | CleanupObservation::ClockDomainMismatch { .. }
            | CleanupObservation::Pending
            | CleanupObservation::AlreadySettled { .. } => {
                unconfirmed += 1;
            }
        }
    }
    let phase = if !failures.is_empty() {
        ResourcePhase::RecoveryRequired
    } else if unconfirmed > 0 {
        ResourcePhase::Unconfirmed
    } else {
        ResourcePhase::Released
    };
    CleanupFacts {
        phase,
        failures,
        transfers,
        unconfirmed,
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Child observation and recursive scope-settlement polling.

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use super::super::ChildRequirement;
use super::super::OperationScope;
use super::super::ScopePolicy;
use super::super::join_facts::JoinFacts;
use crate::report::ChildBusinessOutcome;
use crate::report::StopReason;

/// Collects completed business outcomes from committed direct children.
///
/// Required-child failures are retained in `facts` and request cancellation
/// when the scope uses the fail-fast policy. Pending children are polled again
/// after their registered wakers are notified.
///
/// # Parameters
///
/// * `scope` is the parent whose committed children are observed.
/// * `cx` provides the task waker used by pending child operations.
/// * `facts` accumulates outcomes and the first required-child failure.
///
/// # Returns
///
/// `Poll::Ready(())` when all committed direct children have reported their
/// business outcome; otherwise `Poll::Pending`.
pub(crate) fn poll_child_business(scope: &OperationScope, cx: &mut Context<'_>, facts: &mut JoinFacts) -> Poll<()> {
    let children = {
        let state = scope.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .children
            .iter()
            .filter(|child| child.committed)
            .map(|child| (Arc::clone(&child.control), child.requirement))
            .collect::<Vec<_>>()
    };
    let mut pending = false;
    for (control, requirement) in children {
        let id = control.snapshot().operation_id;
        if facts.business.iter().any(|summary| summary.operation_id == id) {
            continue;
        }
        if let Poll::Ready(summary) = control.poll_business(cx) {
            if requirement == ChildRequirement::Required
                && summary.outcome != ChildBusinessOutcome::Succeeded
                && facts.failed_required.is_none()
            {
                facts.failed_required = Some(summary.clone());
                if scope.policy == ScopePolicy::FailFast {
                    scope.request_cancel(StopReason::RequiredChildFailed);
                }
            }
            facts.business.push(summary);
        } else {
            pending = true;
        }
    }
    if pending { Poll::Pending } else { Poll::Ready(()) }
}

/// Collects release settlements from committed direct children.
///
/// Each observed settlement releases that child's retained resource-use
/// authorizations. Pending children are polled again after their registered
/// wakers are notified.
///
/// # Parameters
///
/// * `scope` is the parent whose committed children are observed.
/// * `cx` provides the task waker used by pending child operations.
/// * `facts` accumulates settlements already observed during this join.
///
/// # Returns
///
/// `Poll::Ready(())` when every committed direct child has settled; otherwise
/// `Poll::Pending`.
pub(crate) fn poll_child_releases(scope: &OperationScope, cx: &mut Context<'_>, facts: &mut JoinFacts) -> Poll<()> {
    let children = {
        let state = scope.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .children
            .iter()
            .filter(|child| child.committed)
            .map(|child| (child.id, Arc::clone(&child.control)))
            .collect::<Vec<_>>()
    };
    let mut pending = false;
    for (id, control) in children {
        if facts.settlements.iter().any(|settlement| settlement.operation_id == id) {
            continue;
        }
        match control.poll_released(cx) {
            Poll::Ready(settlement) => {
                facts.settlements.push(settlement);
                let uses = {
                    let mut state = scope.state.lock().unwrap_or_else(|e| e.into_inner());
                    state
                        .children
                        .iter_mut()
                        .find(|child| child.id == id)
                        .and_then(|child| child.uses.take())
                };
                drop(uses);
            }
            Poll::Pending => pending = true,
        }
    }
    if pending { Poll::Pending } else { Poll::Ready(()) }
}

/// Collects business outcomes throughout a scope and its descendant scopes.
///
/// Direct children are observed before nested scopes, and traversal stops at
/// the first pending scope so the caller can resume polling after a wakeup.
///
/// # Parameters
///
/// * `scope` is the root scope of the recursive observation.
/// * `cx` provides the task waker used by pending child operations.
/// * `facts` accumulates child outcomes from the traversed scope tree.
///
/// # Returns
///
/// `Poll::Ready(())` when all observed scopes have completed business polling;
/// otherwise `Poll::Pending`.
pub(crate) fn poll_scope_business(scope: &OperationScope, cx: &mut Context<'_>, facts: &mut JoinFacts) -> Poll<()> {
    if poll_child_business(scope, cx, facts).is_pending() {
        return Poll::Pending;
    }
    for child in scope.child_scopes() {
        if poll_scope_business(&child, cx, facts).is_pending() {
            return Poll::Pending;
        }
    }
    Poll::Ready(())
}

/// Collects release settlements throughout a scope and its descendant scopes.
///
/// Direct children are settled before nested scopes, and traversal stops at
/// the first pending scope so the caller can resume polling after a wakeup.
///
/// # Parameters
///
/// * `scope` is the root scope of the recursive settlement pass.
/// * `cx` provides the task waker used by pending child operations.
/// * `facts` accumulates settlements from the traversed scope tree.
///
/// # Returns
///
/// `Poll::Ready(())` when all observed scopes have completed release polling;
/// otherwise `Poll::Pending`.
pub(crate) fn poll_scope_releases(scope: &OperationScope, cx: &mut Context<'_>, facts: &mut JoinFacts) -> Poll<()> {
    if poll_child_releases(scope, cx, facts).is_pending() {
        return Poll::Pending;
    }
    for child in scope.child_scopes() {
        if poll_scope_releases(&child, cx, facts).is_pending() {
            return Poll::Pending;
        }
    }
    Poll::Ready(())
}

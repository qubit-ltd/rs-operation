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

use crate::report::ChildBusinessOutcome;
use crate::report::StopReason;

use super::super::join_facts::JoinFacts;
use super::super::{ChildRequirement, OperationScope, ScopePolicy};

pub(crate) fn poll_child_business(
    scope: &OperationScope,
    cx: &mut Context<'_>,
    facts: &mut JoinFacts,
) -> Poll<()> {
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
        if facts
            .business
            .iter()
            .any(|summary| summary.operation_id == id)
        {
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
    if pending {
        Poll::Pending
    } else {
        Poll::Ready(())
    }
}

pub(crate) fn poll_child_releases(
    scope: &OperationScope,
    cx: &mut Context<'_>,
    facts: &mut JoinFacts,
) -> Poll<()> {
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
        if facts
            .settlements
            .iter()
            .any(|settlement| settlement.operation_id == id)
        {
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
    if pending {
        Poll::Pending
    } else {
        Poll::Ready(())
    }
}

pub(crate) fn poll_scope_business(
    scope: &OperationScope,
    cx: &mut Context<'_>,
    facts: &mut JoinFacts,
) -> Poll<()> {
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

pub(crate) fn poll_scope_releases(
    scope: &OperationScope,
    cx: &mut Context<'_>,
    facts: &mut JoinFacts,
) -> Poll<()> {
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

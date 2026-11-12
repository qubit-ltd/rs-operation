// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use qubit_operation::protocol::CleanupContext;
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::report::CleanupFailure;
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::RecoveryOwner;

use super::Gate;
use super::GateWait;

enum CleanupPlan {
    Succeed,
    #[allow(dead_code)] // Recovery owners are issued by the crate-private registry in later tasks.
    Recover(CleanupFailure, RecoveryOwner),
    Panic,
    Pending(Gate),
}

/// Cleanup fixture with explicit success, recovery, panic, and pending scripts.
pub struct FakeCleanup {
    plan: CleanupPlan,
}

impl FakeCleanup {
    /// Create a cleanup future that completes successfully when polled.
    pub fn success() -> Self {
        Self {
            plan: CleanupPlan::Succeed,
        }
    }

    /// Create a cleanup future that returns the supplied failure and recovery
    /// owner.
    #[allow(dead_code)] // Later lifecycle tests pass in a real registry-issued owner.
    pub fn recovery(failure: CleanupFailure, owner: RecoveryOwner) -> Self {
        Self {
            plan: CleanupPlan::Recover(failure, owner),
        }
    }

    /// Create a cleanup future that panics on its first poll.
    pub fn panicking() -> Self {
        Self {
            plan: CleanupPlan::Panic,
        }
    }

    /// Create a cleanup future that waits for the supplied release gate.
    pub fn pending(gate: Gate) -> Self {
        Self {
            plan: CleanupPlan::Pending(gate),
        }
    }

    /// Convert this script to a manually polled cleanup future.
    pub fn into_future(self) -> FakeCleanupFuture {
        let gate = match &self.plan {
            CleanupPlan::Pending(gate) => Some(Box::pin(gate.wait())),
            CleanupPlan::Succeed | CleanupPlan::Recover(_, _) | CleanupPlan::Panic => None,
        };
        FakeCleanupFuture {
            plan: Some(self.plan),
            gate,
        }
    }
}

impl OwnedCleanup for FakeCleanup {
    fn begin(
        self: Box<Self>,
        _context: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin((*self).into_future())
    }
}

/// Future produced by [`FakeCleanup`], driven only by explicit polls and gates.
pub struct FakeCleanupFuture {
    plan: Option<CleanupPlan>,
    gate: Option<Pin<Box<GateWait>>>,
}

impl Future for FakeCleanupFuture {
    type Output = CleanupOutcome;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(gate) = self.gate.as_mut() {
            if gate.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            self.gate = None;
        }
        match self.plan.take().expect("cleanup future polled after completion") {
            CleanupPlan::Succeed | CleanupPlan::Pending(_) => Poll::Ready(CleanupOutcome::Cleaned),
            CleanupPlan::Recover(failure, owner) => Poll::Ready(CleanupOutcome::FailedWithRecovery(failure, owner)),
            CleanupPlan::Panic => panic!("scripted cleanup panic"),
        }
    }
}

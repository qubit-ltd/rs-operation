// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use super::ExecutionContextGuard;
use crate::v02::manager::core::ManagerCore;
use crate::v02::manager::group::GroupCore;
use crate::v02::report::OperationId;

/// Future that installs an operation context around each call to `poll`.
///
/// # Type Parameters
///
/// - `F`: User future whose poll runs within the managed operation context.
pub(crate) struct ExecutionContextFuture<F> {
    /// Pinned user future, kept separately so this wrapper remains movable.
    future: Pin<Box<F>>,
    /// Manager identity exposed during each future poll.
    manager: Arc<ManagerCore>,
    /// Operation identity exposed during each future poll.
    operation: OperationId,
    /// Optional group identity exposed during each future poll.
    group: Option<Arc<GroupCore>>,
}

impl<F> ExecutionContextFuture<F> {
    /// Wraps a user future with its managed execution context.
    ///
    /// # Parameters
    ///
    /// - `future`: User future to poll.
    /// - `manager`: Manager that owns the operation.
    /// - `operation`: Operation currently being executed.
    /// - `group`: Optional group that contains the operation.
    ///
    /// # Returns
    ///
    /// A future that installs and removes context for each poll.
    pub(crate) fn new(
        future: F,
        manager: Arc<ManagerCore>,
        operation: OperationId,
        group: Option<Arc<GroupCore>>,
    ) -> Self {
        Self {
            future: Box::pin(future),
            manager,
            operation,
            group,
        }
    }
}

impl<F: Future> Future for ExecutionContextFuture<F> {
    /// Output produced by the wrapped future.
    type Output = F::Output;

    /// Polls the wrapped future with the owning operation context installed.
    ///
    /// # Parameters
    ///
    /// - `self`: Pinned context wrapper.
    /// - `cx`: Task context used to poll the inner future.
    ///
    /// # Returns
    ///
    /// The wrapped future's current poll result.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The boxed future is pinned independently, so the wrapper remains
        // movable and can stay Send when F is Send.
        let this = self.as_mut().get_mut();
        let _context = ExecutionContextGuard::enter(&this.manager, this.operation, this.group.as_ref());
        this.future.as_mut().poll(cx)
    }
}

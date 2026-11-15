// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::thread::JoinHandle;

use super::CleanupPermit;
use super::ExecutorCreateError;
use super::ExecutorError;
use super::PortableExecutor;
use super::PortablePermit;
use super::ShutdownHandle;
use super::ThreadPoolExecutor;
use super::close_pool;
use super::lock;
use crate::v02::report::PortableExecutorOptions;
use crate::v02::report::ThreadPoolExecutorOptions;

/// Independent bounded dispatch endpoint reserved for resource cleanup.
///
/// Construct a separate instance from the ordinary business pool so cleanup
/// cannot be starved by user work. Values passed to `CleanupPermit::drop_value`
/// are destroyed only by this endpoint's blocking worker.
pub(crate) struct CleanupExecutor {
    /// Bounded blocking pool that runs resource destructors away from callers.
    pub(in crate::v02::executor) blocking: ThreadPoolExecutor,
    /// Bounded executor that polls asynchronous cleanup futures.
    pub(in crate::v02::executor) asynchronous: PortableExecutor,
}

impl CleanupExecutor {
    /// Creates separate blocking and asynchronous cleanup execution paths.
    ///
    /// # Parameters
    ///
    /// * `blocking_options` configures worker and queue capacity for
    ///   destructors.
    /// * `async_options` configures task capacity and polling for async
    ///   cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if either executor cannot be created.
    pub(crate) fn new(
        blocking_options: ThreadPoolExecutorOptions,
        async_options: PortableExecutorOptions,
    ) -> Result<Self, ExecutorCreateError> {
        Ok(Self {
            blocking: ThreadPoolExecutor::new(blocking_options)?,
            asynchronous: PortableExecutor::new(async_options)?,
        })
    }

    /// Reserves bounded capacity before removing a resource owner.
    pub(crate) fn reserve_drop(&self) -> Result<CleanupPermit, ExecutorError> {
        self.blocking.reserve().map(|permit| CleanupPermit { permit })
    }

    /// Reserves a slot for asynchronous cleanup without polling user code.
    pub(crate) fn reserve_async(&self) -> Result<PortablePermit, ExecutorError> {
        self.asynchronous.reserve()
    }

    /// Starts worker threads that poll scheduled asynchronous cleanup futures.
    ///
    /// # Parameters
    ///
    /// * `count` is the number of workers to start.
    /// * `name` is the prefix used for their thread names.
    ///
    /// # Errors
    ///
    /// Returns an error if a worker thread cannot be started.
    pub(crate) fn start_async_workers(
        &self,
        count: usize,
        name: &'static str,
    ) -> Result<Vec<JoinHandle<()>>, ExecutorCreateError> {
        self.asynchronous.start_workers(count, name)
    }

    /// Installs a callback notified when either cleanup path gains capacity.
    ///
    /// The callback is cloned into the blocking pool and moved into the
    /// asynchronous executor so both can notify the same observer.
    ///
    /// # Parameters
    ///
    /// * `callback` is invoked when either cleanup path gains capacity.
    pub(crate) fn set_capacity_callback(&self, callback: Arc<dyn Fn() + Send + Sync + 'static>) {
        *lock(&self.blocking.inner.capacity_callback) = Some(Arc::clone(&callback));
        self.asynchronous.set_capacity_callback(callback);
    }

    /// Advances one scheduled asynchronous cleanup future.
    #[cfg(test)]
    pub(crate) fn poll_async_once(&self) -> bool {
        self.asynchronous.poll_once()
    }

    /// Polls up to the configured per-turn limit of asynchronous cleanup tasks.
    #[cfg(test)]
    pub(crate) fn poll_async_turn(&self) -> usize {
        self.asynchronous.poll_turn()
    }

    /// Prevents new cleanup reservations and closes both executor admission
    /// paths.
    pub(crate) fn close_admission(&self) {
        self.asynchronous.close_admission();
        close_pool(&self.blocking.inner);
    }

    /// Closes both cleanup paths and returns an observer for blocking workers.
    pub(crate) fn shutdown(&self) -> ShutdownHandle {
        self.close_admission();
        self.blocking.shutdown()
    }
}

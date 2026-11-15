// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::mem::take;
use std::sync::PoisonError;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use crate::v02::report::RecoveryStoreSnapshot;
use crate::v02::resource::recovery::RecoveryStore;
use crate::v02::resource::recovery::RecoveryStoreShutdownError;
use crate::v02::resource::recovery::lock;

/// Shared handle that drains a recovery store and shuts down its workers.
///
/// A handle is returned by `RecoveryStore::request_shutdown`, which closes
/// admission but does not start shutdown by itself. A wait method must be
/// called to drain the store, shut down its executor and join its recovery
/// workers. Clones share the same shutdown state.
///
/// # Examples
///
/// ```
/// use qubit_operation::RecoveryStore;
/// use qubit_operation::RecoveryStoreShutdownError;
///
/// fn close_store(store: &RecoveryStore) -> Result<(), RecoveryStoreShutdownError> {
///     store.request_shutdown().wait()
/// }
/// ```
#[must_use = "call a wait method to complete recovery store shutdown"]
#[derive(Clone)]
pub struct RecoveryStoreShutdownHandle {
    /// Shared store whose admission is closed and workers are being drained.
    pub(in crate::v02::resource::recovery) store: RecoveryStore,
}

impl RecoveryStoreShutdownHandle {
    /// Returns the current store snapshot while shutdown is in progress.
    ///
    /// # Parameters
    ///
    /// - `self`: Handle for the store being shut down.
    ///
    /// # Returns
    ///
    /// Group and resource counts, including whether the store still accepts
    /// groups.
    #[must_use = "inspect the store snapshot during shutdown"]
    pub fn snapshot(&self) -> RecoveryStoreSnapshot {
        self.store.snapshot()
    }

    /// Reports whether executor and worker shutdown finalization has finished.
    ///
    /// This can be `true` after a failed finalization. Call [`Self::wait`] or
    /// another wait method to receive the shared result.
    ///
    /// # Parameters
    ///
    /// - `self`: Handle whose shared shutdown state is inspected.
    ///
    /// # Returns
    ///
    /// `true` after shutdown finalization, whether successful or failed.
    #[must_use = "use the result to determine whether shutdown has finalized"]
    pub fn is_complete(&self) -> bool {
        self.store
            .shutdown
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .result
            .is_some()
    }

    /// Blocks until all recovery groups are resolved, then joins store workers.
    ///
    /// Concurrent waiters and clones share one shutdown operation and its
    /// final result. This method does not return until the store contains no
    /// recovery groups and executor/worker finalization has completed.
    ///
    /// # Parameters
    ///
    /// - `self`: Handle controlling and observing shutdown of the shared store.
    ///
    /// # Returns
    ///
    /// `Ok(())` when executor and recovery workers stop cleanly.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryStoreShutdownError::WorkerPanicked`] if the executor
    /// or a recovery worker panics while joining.
    pub fn wait(&self) -> Result<(), RecoveryStoreShutdownError> {
        while self.store.snapshot().groups != 0 {
            sleep(Duration::from_millis(5));
        }
        let (state_lock, changed) = &*self.store.shutdown;
        let mut state = lock(state_lock);
        if let Some(result) = state.result {
            return result;
        }
        if state.started {
            while state.result.is_none() {
                state = changed.wait(state).unwrap_or_else(PoisonError::into_inner);
            }
            return state.result.expect("shutdown result was set before notification");
        }
        state.started = true;
        drop(state);
        let result = self
            .store
            .executor
            .shutdown()
            .wait()
            .map_err(|_| RecoveryStoreShutdownError::WorkerPanicked);
        let workers = take(&mut *lock(&self.store.async_workers));
        let mut panicked = false;
        for worker in workers {
            panicked |= worker.join().is_err();
        }
        let final_result = if panicked {
            Err(RecoveryStoreShutdownError::WorkerPanicked)
        } else {
            result
        };
        let mut state = lock(state_lock);
        state.result = Some(final_result);
        drop(state);
        changed.notify_all();
        final_result
    }

    /// Waits asynchronously for groups to drain, then finalizes shutdown.
    ///
    /// Waiting for groups does not block the polling task. Dropping this future
    /// cancels only this observer; it does not cancel store shutdown. Once the
    /// store is empty, this method performs executor shutdown and worker joins
    /// synchronously. Repeated polls reuse one notification registration.
    ///
    /// # Parameters
    ///
    /// - `self`: Handle for the shared shutdown operation.
    ///
    /// # Returns
    ///
    /// The shared finalization result, including a previously recorded error.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryStoreShutdownError::WorkerPanicked`] if a shutdown
    /// worker panics while joining.
    pub async fn wait_async(&self) -> Result<(), RecoveryStoreShutdownError> {
        loop {
            let generation = self.store.shutdown_notify.generation();
            if self.store.snapshot().groups == 0 {
                return self.wait();
            }
            self.store.shutdown_notify.notified_since(generation).await;
        }
    }

    /// Waits up to `timeout` for groups to drain and then finalizes shutdown.
    ///
    /// A timeout leaves the store closed to new groups; a later call may
    /// continue waiting on the same shutdown handle. The final result is
    /// shared with all other wait methods and handle clones.
    ///
    /// # Parameters
    ///
    /// - `self`: Handle for the shared shutdown operation.
    /// - `timeout`: Maximum time to wait for recovery groups to resolve.
    ///
    /// # Returns
    ///
    /// `Ok(true)` if shutdown completed, or `Ok(false)` if the timeout expired
    /// while groups remained. A successful wait can still return a worker
    /// error.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryStoreShutdownError::WorkerPanicked`] if the executor
    /// or a recovery worker panics while joining.
    ///
    /// # Panics
    ///
    /// Panics if the platform cannot represent the deadline as an [`Instant`].
    pub fn wait_for(&self, timeout: Duration) -> Result<bool, RecoveryStoreShutdownError> {
        let deadline = Instant::now() + timeout;
        while self.store.snapshot().groups != 0 {
            if Instant::now() >= deadline {
                return Ok(false);
            }
            sleep(Duration::from_millis(5).min(deadline.saturating_duration_since(Instant::now())));
        }
        self.wait()?;
        Ok(true)
    }
}

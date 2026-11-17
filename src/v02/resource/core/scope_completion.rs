// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashSet;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;

use super::internal::ScopeCompletionState;
use super::internal::lock;
use crate::v02::report::CleanupEvidence;
use crate::v02::report::CleanupFailure;
use crate::v02::report::ResourceCleanupReport;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceProgress;
use crate::v02::report::ResourceScopeReport;
use crate::v02::report::TransferReceipt;

/// Settles a resource scope only after every handed-off resource owner reports.
pub(in crate::v02::resource) struct ScopeCompletion {
    /// Synchronizes owner outcomes, recovery observers, progress, and
    /// callbacks.
    state: Mutex<ScopeCompletionState>,
}

impl ScopeCompletion {
    /// Creates an open completion tracker with no registered owners.
    ///
    /// The tracker accumulates owner outcomes until its expected owner count
    /// is sealed, then invokes the scope callback once all owners report.
    ///
    /// # Returns
    ///
    /// A shared completion tracker for the resource scope.
    #[must_use = "the scope completion tracker must be retained"]
    pub(in crate::v02::resource::core) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ScopeCompletionState {
                expected: None,
                reservations_released_before_seal: 0,
                started: 0,
                completed: 0,
                seen: HashSet::new(),
                resources: Vec::new(),
                failures: Vec::new(),
                recoveries: Vec::new(),
                recovery_callback: None,
                transfers: Vec::new(),
                transfers_total: 0,
                transfers_truncated: false,
                callback: None,
                progress: ResourceProgress::ScopeOpen,
                progress_callback: None,
            }),
        })
    }

    /// Counts owners currently running cleanup.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker whose owner counts are read.
    ///
    /// # Returns
    ///
    /// The started owner count minus the completed owner count, saturating at
    /// zero if state is inconsistent.
    #[must_use = "the active cleanup count should be inspected"]
    pub(in crate::v02::resource::core) fn cleaning(&self) -> usize {
        let state = lock(&self.state);
        state.started.saturating_sub(state.completed)
    }

    /// Registers a listener for monotonic changes in cleanup progress.
    ///
    /// The listener is called immediately with the current progress after it
    /// has been stored; later progress updates are delivered outside the lock.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker receiving the progress listener.
    /// * `callback` — thread-safe listener for scope progress.
    pub(in crate::v02::resource) fn set_progress_callback(
        &self,
        callback: Arc<dyn Fn(ResourceProgress) + Send + Sync + 'static>,
    ) {
        let progress = {
            let mut state = lock(&self.state);
            state.progress_callback = Some(Arc::clone(&callback));
            state.progress
        };
        callback(progress);
    }

    /// Installs the recovery observer and reports recoveries already recorded.
    ///
    /// Each handle also receives a terminal callback so later recovery state
    /// changes are forwarded to the observer.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker whose recovery state is observed.
    /// * `callback` — observer receiving a recovery handle and its presence
    ///   state.
    pub(in crate::v02::resource::core) fn set_recovery_callback(
        &self,
        callback: Arc<dyn Fn(crate::v02::resource::recovery::RecoveryHandle, bool) + Send + Sync + 'static>,
    ) {
        let existing = {
            let mut state = lock(&self.state);
            state.recovery_callback = Some(Arc::clone(&callback));
            state.recoveries.clone()
        };
        for handle in existing {
            let terminal = Arc::clone(&callback);
            let terminal_core = Arc::downgrade(&handle.core);
            handle.core.set_terminal_callback(Arc::new(move |present| {
                if let Some(core) = terminal_core.upgrade() {
                    terminal(crate::v02::resource::recovery::RecoveryHandle::new(core), present);
                }
            }));
            callback(handle, true);
        }
    }

    /// Sets the one-shot callback that receives the final scope report.
    ///
    /// If all expected owners have already completed, the callback runs
    /// immediately after the state lock is released.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker that will deliver the final report.
    /// * `callback` — one-shot receiver of the completed report.
    ///
    /// # Panics
    ///
    /// Panics if a completion callback was already installed.
    pub(in crate::v02::resource) fn set_callback(
        &self,
        callback: Box<dyn FnOnce(ResourceScopeReport) + Send + 'static>,
    ) {
        let (callback, report) = {
            let mut state = lock(&self.state);
            assert!(state.callback.is_none(), "resource scope callback is one-shot");
            if Self::ready(&state) {
                (Some(callback), Some(Self::report(&state)))
            } else {
                state.callback = Some(callback);
                (None, None)
            }
        };
        if let (Some(callback), Some(report)) = (callback, report) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(report)));
        }
    }

    /// Records a transfer while the scope's owner count is still unsealed.
    ///
    /// At most 256 receipts are retained for the report; the total count keeps
    /// increasing and marks the list truncated after that limit.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker collecting transfer outcomes.
    /// * `receipt` — transfer evidence to include in the eventual report.
    ///
    /// # Returns
    ///
    /// `true` if the receipt was accepted before sealing, or `false` if the
    /// scope's owner count has already been sealed.
    #[must_use = "check whether the transfer receipt was accepted"]
    pub(in crate::v02::resource) fn record_transfer(&self, receipt: TransferReceipt) -> bool {
        let mut state = lock(&self.state);
        if state.expected.is_some() {
            return false;
        }
        state.transfers_total = state.transfers_total.saturating_add(1);
        if state.transfers.len() < 256 {
            state.transfers.push(receipt);
        } else {
            state.transfers_truncated = true;
        }
        true
    }

    /// Records that an owner has started cleanup and advances progress.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker whose active cleanup count is updated.
    pub(in crate::v02::resource::core) fn start(&self) {
        {
            let mut state = lock(&self.state);
            state.started = state.started.saturating_add(1);
        }
        self.advance_progress(ResourceProgress::Cleaning);
    }

    /// Publishes that cleanup is waiting for outstanding resource users.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker whose progress is advanced.
    pub(in crate::v02::resource::core) fn waiting_for_users(&self) {
        self.advance_progress(ResourceProgress::WaitingForUsers);
    }

    /// Seals the number of owners expected to report completion.
    ///
    /// Reservations released before sealing are subtracted from `expected`.
    /// If that makes the scope ready, its final callback runs after unlocking.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker whose expected owner count is sealed.
    /// * `expected` — number of owner reservations created for the scope.
    pub(in crate::v02::resource::core) fn seal(&self, expected: usize) {
        let (callback, report) = {
            let mut state = lock(&self.state);
            state.expected = Some(expected.saturating_sub(state.reservations_released_before_seal));
            state.reservations_released_before_seal = 0;
            if Self::ready(&state) {
                (state.callback.take(), Some(Self::report(&state)))
            } else {
                (None, None)
            }
        };
        if let (Some(callback), Some(report)) = (callback, report) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(report)));
        }
    }

    /// Removes one owner reservation that will not produce a completion report.
    ///
    /// This adjusts the sealed expected count, or records the release for the
    /// later `seal` call when reservations are released before sealing.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker whose expected owner count is reduced.
    pub(in crate::v02::resource::core) fn release_pending_reservation(&self) {
        let (callback, report) = {
            let mut state = lock(&self.state);
            if let Some(expected) = state.expected.as_mut() {
                *expected = expected.saturating_sub(1);
            } else {
                state.reservations_released_before_seal = state.reservations_released_before_seal.saturating_add(1);
            }
            if Self::ready(&state) {
                (state.callback.take(), Some(Self::report(&state)))
            } else {
                (None, None)
            }
        };
        if let (Some(callback), Some(report)) = (callback, report) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(report)));
        }
    }

    /// Records one resource owner's settled outcome and recovery handle.
    ///
    /// Duplicate resource IDs are ignored. A newly ready scope invokes its
    /// report callback after unlocking; recovery callbacks are also invoked
    /// outside the lock.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker that aggregates owner outcomes.
    /// * `resource_id` — identity used to deduplicate owner completion.
    /// * `evidence` — successful cleanup evidence, when available.
    /// * `failure` — cleanup failure, when cleanup was not confirmed.
    /// * `recovery` — recovery handle retaining an unconfirmed owner, if any.
    pub(in crate::v02::resource::core) fn complete(
        &self,
        resource_id: ResourceId,
        evidence: Option<CleanupEvidence>,
        failure: Option<CleanupFailure>,
        recovery: Option<crate::v02::resource::recovery::RecoveryHandle>,
    ) {
        let (callback, report, recovery_callback) = {
            let mut state = lock(&self.state);
            if !state.seen.insert(resource_id) {
                return;
            }
            state.completed = state.completed.saturating_add(1);
            if let Some(evidence) = evidence {
                state.resources.push(ResourceCleanupReport { resource_id, evidence });
            }
            if let Some(failure) = failure {
                state.failures.push(failure);
            }
            let recovery_callback = recovery.as_ref().and_then(|handle| {
                state.recoveries.push(handle.clone());
                state
                    .recovery_callback
                    .clone()
                    .map(|callback| (callback, handle.clone()))
            });
            if Self::ready(&state) {
                (state.callback.take(), Some(Self::report(&state)), recovery_callback)
            } else {
                (None, None, recovery_callback)
            }
        };
        if let Some((callback, handle)) = recovery_callback {
            let terminal = Arc::clone(&callback);
            let terminal_core = Arc::downgrade(&handle.core);
            handle.core.set_terminal_callback(Arc::new(move |present| {
                if let Some(core) = terminal_core.upgrade() {
                    terminal(crate::v02::resource::recovery::RecoveryHandle::new(core), present);
                }
            }));
            callback(handle, true);
        }
        if recovery.is_some() {
            self.advance_progress(ResourceProgress::Unconfirmed);
        }
        if let (Some(callback), Some(report)) = (callback, report) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(report)));
        }
    }

    /// Advances progress when the requested phase ranks after the current one.
    ///
    /// Duplicate or earlier phases leave the current state unchanged. Any
    /// registered listener runs after the state lock is released.
    ///
    /// # Parameters
    ///
    /// * `self` — this tracker whose progress may advance.
    /// * `progress` — candidate phase to publish.
    fn advance_progress(&self, progress: ResourceProgress) {
        let callback = {
            let mut state = lock(&self.state);
            if progress_rank(progress) > progress_rank(state.progress) {
                state.progress = progress;
            }
            state
                .progress_callback
                .as_ref()
                .map(|callback| (Arc::clone(callback), state.progress))
        };
        if let Some((callback, progress)) = callback {
            callback(progress);
        }
    }

    /// Reports whether the sealed owner count has been fully completed.
    ///
    /// # Parameters
    ///
    /// * `state` — completion state to evaluate.
    ///
    /// # Returns
    ///
    /// `true` once the expected count is sealed and all expected owners report.
    #[must_use = "the completion readiness state should be used"]
    fn ready(state: &ScopeCompletionState) -> bool {
        state.expected.is_some_and(|expected| state.completed >= expected)
    }

    /// Builds an immutable snapshot of the accumulated scope outcomes.
    ///
    /// # Parameters
    ///
    /// * `state` — completion state to snapshot.
    ///
    /// # Returns
    ///
    /// A report containing copies of the resource, failure, recovery, and
    /// transfer collections accumulated so far.
    #[must_use = "the accumulated resource report should be used"]
    fn report(state: &ScopeCompletionState) -> ResourceScopeReport {
        ResourceScopeReport {
            resources: Arc::from(state.resources.clone()),
            failures: Arc::from(state.failures.clone()),
            recoveries: Arc::from(state.recoveries.clone()),
            transfers: Arc::from(state.transfers.clone()),
            transfers_total: state.transfers_total,
            transfers_truncated: state.transfers_truncated,
        }
    }
}

#[cfg(test)]
mod recovery_lifetime_tests {
    use std::sync::Arc;
    use std::sync::Weak;

    use super::ScopeCompletion;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ResourceId;
    use crate::v02::resource::recovery::RecoveryCore;
    use crate::v02::resource::recovery::RecoveryHandle;

    #[test]
    fn test_recovery_terminal_observer_does_not_retain_its_own_core() {
        for install_before_completion in [true, false] {
            let core = Arc::new(RecoveryCore::new_unconfirmed(
                vec![ResourceId::new(ManagerId::from_raw(1), 1, 0)],
                Arc::new(()),
            ));
            let weak: Weak<_> = Arc::downgrade(&core);
            let handle = RecoveryHandle::new(Arc::clone(&core));
            let completion = ScopeCompletion::new();
            let observer: Arc<dyn Fn(RecoveryHandle, bool) + Send + Sync> = Arc::new(|_, _| {});

            if install_before_completion {
                completion.set_recovery_callback(Arc::clone(&observer));
            }
            completion.complete(
                ResourceId::new(ManagerId::from_raw(1), 1, 0),
                None,
                None,
                Some(handle.clone()),
            );
            if !install_before_completion {
                completion.set_recovery_callback(observer);
            }

            drop(handle);
            drop(core);
            drop(completion);
            assert!(weak.upgrade().is_none(), "terminal observer retained its recovery core");
        }
    }
}

/// Assigns each progress phase an ordinal for monotonic advancement checks.
///
/// # Parameters
///
/// * `progress` — phase whose position in the cleanup sequence is requested.
///
/// # Returns
///
/// The phase rank, increasing from scope-open through unconfirmed cleanup.
#[must_use = "the progress rank should be compared with the current phase"]
fn progress_rank(progress: ResourceProgress) -> u8 {
    match progress {
        ResourceProgress::ScopeOpen => 0,
        ResourceProgress::WaitingForUsers => 1,
        ResourceProgress::Cleaning => 2,
        ResourceProgress::Unconfirmed => 3,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use super::ScopeCompletion;
    use crate::v02::report::CleanupEvidence;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ResourceId;

    #[test]
    fn test_callback_installed_after_empty_scope_seal_runs_immediately() {
        let completion = ScopeCompletion::new();
        completion.seal(0);
        let (sender, receiver) = mpsc::channel();

        completion.set_callback(Box::new(move |report| {
            sender.send(report.resources.len()).unwrap();
        }));

        assert_eq!(receiver.recv().unwrap(), 0);
    }

    #[test]
    fn test_releasing_pending_reservation_completes_sealed_scope() {
        let completion = ScopeCompletion::new();
        let (sender, receiver) = mpsc::channel();
        completion.set_callback(Box::new(move |report| {
            sender.send(report.resources.len()).unwrap();
        }));
        completion.seal(1);

        completion.release_pending_reservation();

        assert_eq!(receiver.recv().unwrap(), 0);
    }

    #[test]
    fn test_duplicate_owner_completion_is_ignored_after_first_report() {
        let completion = ScopeCompletion::new();
        let (sender, receiver) = mpsc::channel();
        completion.set_callback(Box::new(move |report| {
            sender.send((report.resources.len(), report.failures.len())).unwrap();
        }));
        completion.seal(1);
        let resource = ResourceId::new(ManagerId::new_unique(), 1, 1);

        completion.complete(resource, Some(CleanupEvidence::DropCompleted), None, None);
        completion.complete(resource, None, None, None);

        assert_eq!(receiver.recv().unwrap(), (1, 0));
        assert!(receiver.try_recv().is_err());
    }
}

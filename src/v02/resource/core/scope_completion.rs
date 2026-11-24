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
                pending_recovery_publications: 0,
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
            let existing = state.recoveries.clone();
            state.pending_recovery_publications = state.pending_recovery_publications.saturating_add(existing.len());
            existing
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
            self.release_recovery_publication();
            self.publish_if_ready();
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
        let (recovery_callback, publish_recovery) = {
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
            if recovery_callback.is_some() {
                state.pending_recovery_publications = state.pending_recovery_publications.saturating_add(1);
            }
            (recovery_callback, recovery.is_some())
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
            if publish_recovery {
                // Registration returned successfully, so ledger publication
                // is complete. Update readiness and progress atomically, then
                // notify observers outside the state lock.
                self.release_recovery_publication_and_advance_progress();
            } else {
                self.release_recovery_publication();
            }
        } else if publish_recovery {
            self.advance_progress(ResourceProgress::Unconfirmed);
        }
        self.publish_if_ready();
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
        state.expected.is_some_and(|expected| state.completed >= expected) && state.pending_recovery_publications == 0
    }

    /// Releases one successfully returned recovery publication and checks
    /// whether that makes the scope ready to publish its final report.
    fn release_recovery_publication(&self) {
        let mut state = lock(&self.state);
        state.pending_recovery_publications = state.pending_recovery_publications.saturating_sub(1);
    }

    /// Releases a registered recovery publication and records its progress
    /// phase in one state transition, then notifies the observer outside lock.
    fn release_recovery_publication_and_advance_progress(&self) {
        let callback = {
            let mut state = lock(&self.state);
            state.pending_recovery_publications = state.pending_recovery_publications.saturating_sub(1);
            if progress_rank(ResourceProgress::Unconfirmed) > progress_rank(state.progress) {
                state.progress = ResourceProgress::Unconfirmed;
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

    /// Publishes the final report when all owners and recovery observers are
    /// complete. The callback runs after releasing the state lock.
    fn publish_if_ready(&self) {
        let (callback, report) = {
            let mut state = lock(&self.state);
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
    use std::future::Future;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;

    use super::ScopeCompletion;
    use crate::v02::report::CleanupEvidence;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ResourceId;
    use crate::v02::report::ResourceProgress;
    use crate::v02::resource::recovery::RecoveryCore;
    use crate::v02::resource::recovery::RecoveryHandle;

    struct PanickingWake;

    impl Wake for PanickingWake {
        fn wake(self: std::sync::Arc<Self>) {
            panic!("observer waker failed");
        }
    }

    struct CountingWake(std::sync::Arc<AtomicUsize>);

    impl Wake for CountingWake {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_progress_waker_panic_does_not_block_recovery_publication() {
        let completion = ScopeCompletion::new();
        let notify = std::sync::Arc::new(crate::v02::internal::notify::Notify::new());
        let observed_wakes = std::sync::Arc::new(AtomicUsize::new(0));
        let mut panicking = Box::pin(notify.notified());
        let panicking_waker = Waker::from(std::sync::Arc::new(PanickingWake));
        assert!(matches!(
            panicking.as_mut().poll(&mut Context::from_waker(&panicking_waker)),
            Poll::Pending
        ));
        let mut observing = Box::pin(notify.notified());
        let observing_waker = Waker::from(std::sync::Arc::new(CountingWake(std::sync::Arc::clone(
            &observed_wakes,
        ))));
        assert!(matches!(
            observing.as_mut().poll(&mut Context::from_waker(&observing_waker)),
            Poll::Pending
        ));

        let progress_notify = std::sync::Arc::clone(&notify);
        completion.set_progress_callback(std::sync::Arc::new(move |progress| {
            if progress == ResourceProgress::Unconfirmed {
                progress_notify.notify_waiters();
            }
        }));
        let (report_sender, report_receiver) = mpsc::channel();
        completion.set_callback(Box::new(move |report| report_sender.send(report).unwrap()));
        completion.seal(1);

        let id = ResourceId::new(ManagerId::new_unique(), 1, 1);
        let recovery = RecoveryHandle::new(std::sync::Arc::new(RecoveryCore::new_unconfirmed(
            vec![id],
            std::sync::Arc::new(()),
        )));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            completion.complete(
                id,
                None,
                Some(crate::v02::report::CleanupFailure {
                    resource_id: id,
                    stage: crate::v02::report::CleanupStage::Drop,
                    summary: crate::v02::report::DiagnosticSummary::new("cleanup failed"),
                    source: None,
                }),
                Some(recovery),
            );
        }));

        assert!(result.is_ok(), "observer panic escaped recovery publication");
        assert_eq!(observed_wakes.load(Ordering::SeqCst), 1);
        assert_eq!(
            report_receiver
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .recoveries
                .len(),
            1
        );
    }

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

    #[test]
    fn test_scope_report_waits_for_recovery_registration() {
        let completion = ScopeCompletion::new();
        let (report_sender, report_receiver) = mpsc::channel();
        completion.set_callback(Box::new(move |report| report_sender.send(report).unwrap()));
        let (progress_sender, progress_receiver) = mpsc::channel();
        completion.set_progress_callback(std::sync::Arc::new(move |progress| {
            progress_sender.send(progress).unwrap();
        }));
        assert_eq!(progress_receiver.recv().unwrap(), ResourceProgress::ScopeOpen);
        completion.seal(2);

        let entered = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let release = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let entered_callback = std::sync::Arc::clone(&entered);
        let release_callback = std::sync::Arc::clone(&release);
        completion.set_recovery_callback(std::sync::Arc::new(move |_, present| {
            assert!(present);
            let (lock, condition) = &*entered_callback;
            *lock.lock().unwrap() = true;
            condition.notify_all();
            let (lock, condition) = &*release_callback;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = condition.wait(released).unwrap();
            }
        }));

        let first_id = ResourceId::new(ManagerId::new_unique(), 1, 1);
        let second_id = ResourceId::new(ManagerId::new_unique(), 2, 1);
        let recovery = RecoveryHandle::new(std::sync::Arc::new(RecoveryCore::new_unconfirmed(
            vec![first_id],
            std::sync::Arc::new(()),
        )));
        let completing = std::sync::Arc::clone(&completion);
        let first = std::thread::spawn(move || {
            completing.complete(
                first_id,
                None,
                Some(crate::v02::report::CleanupFailure {
                    resource_id: first_id,
                    stage: crate::v02::report::CleanupStage::Drop,
                    summary: crate::v02::report::DiagnosticSummary::new("cleanup failed"),
                    source: None,
                }),
                Some(recovery),
            );
        });

        let (lock, condition) = &*entered;
        let mut callback_entered = lock.lock().unwrap();
        while !*callback_entered {
            let (next, timeout) = condition
                .wait_timeout(callback_entered, Duration::from_secs(2))
                .unwrap();
            callback_entered = next;
            assert!(!timeout.timed_out(), "recovery registration callback did not start");
        }
        drop(callback_entered);

        completion.complete(second_id, None, None, None);
        assert!(matches!(report_receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));

        let (lock, condition) = &*release;
        *lock.lock().unwrap() = true;
        condition.notify_all();
        first.join().unwrap();

        let report = report_receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(progress_receiver.recv().unwrap(), ResourceProgress::Unconfirmed);
        assert_eq!(report.recoveries.len(), 1);
        assert_eq!(report.failures.len(), 1);
    }

    #[test]
    fn test_panicking_recovery_registration_keeps_scope_unsettled() {
        let completion = ScopeCompletion::new();
        let (report_sender, report_receiver) = mpsc::channel();
        completion.set_callback(Box::new(move |report| report_sender.send(report).unwrap()));
        completion.seal(1);
        completion.set_recovery_callback(std::sync::Arc::new(|_, _| panic!("registration failed")));

        let id = ResourceId::new(ManagerId::new_unique(), 1, 1);
        let recovery = RecoveryHandle::new(std::sync::Arc::new(RecoveryCore::new_unconfirmed(
            vec![id],
            std::sync::Arc::new(()),
        )));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            completion.complete(id, None, None, Some(recovery));
        }));

        assert!(result.is_err());
        assert!(matches!(report_receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
        let state = super::lock(&completion.state);
        assert_eq!(state.pending_recovery_publications, 1);
        assert_eq!(state.recoveries.len(), 1);
    }

    #[test]
    fn test_late_recovery_observer_registration_blocks_scope_report() {
        let completion = ScopeCompletion::new();
        let (report_sender, report_receiver) = mpsc::channel();
        completion.set_callback(Box::new(move |report| report_sender.send(report).unwrap()));
        completion.seal(2);

        let first_id = ResourceId::new(ManagerId::new_unique(), 1, 1);
        let second_id = ResourceId::new(ManagerId::new_unique(), 2, 1);
        let recovery = RecoveryHandle::new(std::sync::Arc::new(RecoveryCore::new_unconfirmed(
            vec![first_id],
            std::sync::Arc::new(()),
        )));
        completion.complete(first_id, None, None, Some(recovery));

        let entered = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let release = std::sync::Arc::new((Mutex::new(false), Condvar::new()));
        let entered_callback = std::sync::Arc::clone(&entered);
        let release_callback = std::sync::Arc::clone(&release);
        let installing = std::sync::Arc::clone(&completion);
        let installer = std::thread::spawn(move || {
            installing.set_recovery_callback(std::sync::Arc::new(move |_, present| {
                assert!(present);
                let (lock, condition) = &*entered_callback;
                *lock.lock().unwrap() = true;
                condition.notify_all();
                let (lock, condition) = &*release_callback;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }));
        });

        let (lock, condition) = &*entered;
        let mut callback_entered = lock.lock().unwrap();
        while !*callback_entered {
            let (next, timeout) = condition
                .wait_timeout(callback_entered, Duration::from_secs(2))
                .unwrap();
            callback_entered = next;
            assert!(!timeout.timed_out(), "existing recovery observer did not start");
        }
        drop(callback_entered);

        completion.complete(second_id, None, None, None);
        assert!(matches!(report_receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
        let (lock, condition) = &*release;
        *lock.lock().unwrap() = true;
        condition.notify_all();
        installer.join().unwrap();

        let report = report_receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(report.recoveries.len(), 1);
    }

    #[test]
    fn test_reentrant_reservation_release_cannot_publish_during_recovery_registration() {
        let completion = ScopeCompletion::new();
        let report_published = std::sync::Arc::new(AtomicBool::new(false));
        let published_callback = std::sync::Arc::clone(&report_published);
        completion.set_callback(Box::new(move |_| {
            published_callback.store(true, Ordering::SeqCst);
        }));
        completion.seal(2);

        let callback_completion = std::sync::Arc::clone(&completion);
        let callback_published = std::sync::Arc::clone(&report_published);
        completion.set_recovery_callback(std::sync::Arc::new(move |_, present| {
            assert!(present);
            callback_completion.release_pending_reservation();
            assert!(
                !callback_published.load(Ordering::SeqCst),
                "reentrant readiness change published before registration returned"
            );
        }));

        let id = ResourceId::new(ManagerId::new_unique(), 1, 1);
        let recovery = RecoveryHandle::new(std::sync::Arc::new(RecoveryCore::new_unconfirmed(
            vec![id],
            std::sync::Arc::new(()),
        )));
        completion.complete(id, None, None, Some(recovery));
        assert!(report_published.load(Ordering::SeqCst));
    }

    #[test]
    fn test_duplicate_resource_completion_publishes_one_recovery_registration() {
        let completion = ScopeCompletion::new();
        let (report_sender, report_receiver) = mpsc::channel();
        completion.set_callback(Box::new(move |report| report_sender.send(report).unwrap()));
        let registrations = std::sync::Arc::new(AtomicUsize::new(0));
        let observed_registrations = std::sync::Arc::clone(&registrations);
        completion.set_recovery_callback(std::sync::Arc::new(move |_, present| {
            assert!(present);
            observed_registrations.fetch_add(1, Ordering::SeqCst);
        }));
        completion.seal(1);

        let id = ResourceId::new(ManagerId::new_unique(), 1, 1);
        let first = RecoveryHandle::new(std::sync::Arc::new(RecoveryCore::new_unconfirmed(
            vec![id],
            std::sync::Arc::new(()),
        )));
        let second = RecoveryHandle::new(std::sync::Arc::new(RecoveryCore::new_unconfirmed(
            vec![id],
            std::sync::Arc::new(()),
        )));
        let first_id = first.id();
        completion.complete(id, None, None, Some(first));
        completion.complete(id, None, None, Some(second));

        let report = report_receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(report.recoveries.len(), 1);
        assert_eq!(report.recoveries[0].id(), first_id);
        assert_eq!(registrations.load(Ordering::SeqCst), 1);
        assert!(matches!(
            report_receiver.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Synchronization primitives shared by typed resource handles.

mod internal;
mod owner;
mod scope_completion;
mod scope_core;
mod scope_state;

pub(super) use owner::Owner;
pub(super) use scope_completion::ScopeCompletion;
pub(super) use scope_core::ScopeCore;
pub(super) use scope_state::ScopeState;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::Owner;
    use super::ScopeCompletion;
    use crate::v02::executor::CleanupExecutor;
    use crate::v02::manager::capacity::ManagerCapacityPools;
    use crate::v02::manager::capacity::ResourceRegistration;
    use crate::v02::manager::capacity::ScopeCapacity;
    use crate::v02::report::ManagerLimits;
    use crate::v02::report::PortableExecutorOptions;
    use crate::v02::report::ResourceId;
    use crate::v02::report::ResourceProgress;
    use crate::v02::report::ScopeLimits;
    use crate::v02::report::ThreadPoolExecutorOptions;
    use crate::v02::resource::cleanup_port::CleanupDispatch;
    use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
    use crate::v02::resource::cleanup_port::CleanupJob;
    use crate::v02::resource::cleanup_port::CleanupQueueError;

    struct Slot {
        reserved: bool,
        job: Option<Box<dyn CleanupJob>>,
    }
    struct ManualDispatch {
        slots: Arc<Mutex<Vec<Slot>>>,
    }
    impl ManualDispatch {
        fn new() -> Self {
            Self {
                slots: Arc::new(Mutex::new(
                    (0..4)
                        .map(|_| Slot {
                            reserved: false,
                            job: None,
                        })
                        .collect(),
                )),
            }
        }
        fn reserve_permit(&self) -> Box<dyn CleanupDispatchPermit> {
            let mut slots = self.slots.lock().unwrap();
            let (index, slot) = slots.iter_mut().enumerate().find(|(_, slot)| !slot.reserved).unwrap();
            slot.reserved = true;
            Box::new(ManualPermit {
                slots: Arc::clone(&self.slots),
                index,
            })
        }
        fn run_all(&self, executor: &Arc<CleanupExecutor>) {
            let count = self.slots.lock().unwrap().len();
            for index in 0..count {
                let job = self.slots.lock().unwrap()[index].job.take();
                let Some(job) = job else { continue };
                match job.schedule(Arc::clone(executor)) {
                    Ok(()) => self.slots.lock().unwrap()[index].reserved = false,
                    Err(job) => {
                        self.slots.lock().unwrap()[index].job = Some(job);
                        break;
                    }
                }
            }
        }
        fn queued(&self) -> usize {
            self.slots
                .lock()
                .unwrap()
                .iter()
                .filter(|slot| slot.job.is_some())
                .count()
        }
    }
    impl CleanupDispatch for ManualDispatch {
        fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
            Ok(self.reserve_permit())
        }
    }
    struct ManualPermit {
        slots: Arc<Mutex<Vec<Slot>>>,
        index: usize,
    }
    impl CleanupDispatchPermit for ManualPermit {
        fn enqueue(self: Box<Self>, job: Box<dyn CleanupJob>) {
            let mut slots = self.slots.lock().unwrap();
            assert!(slots[self.index].reserved && slots[self.index].job.is_none());
            slots[self.index].job = Some(job);
        }
    }
    impl Drop for ManualPermit {
        fn drop(&mut self) {
            let mut slots = self.slots.lock().unwrap();
            if slots[self.index].job.is_none() {
                slots[self.index].reserved = false;
            }
        }
    }

    struct DropMark(Arc<AtomicUsize>);
    impl Drop for DropMark {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn registration() -> ResourceRegistration {
        let pools = ManagerCapacityPools::new(ManagerLimits::default());
        ScopeCapacity::root(&pools, 51, ScopeLimits::default())
            .resource_registration()
            .unwrap()
    }
    fn executor() -> Arc<CleanupExecutor> {
        Arc::new(
            CleanupExecutor::new(
                ThreadPoolExecutorOptions {
                    worker_count: 1,
                    queue_capacity: 8,
                },
                PortableExecutorOptions::default(),
            )
            .unwrap(),
        )
    }
    fn finish(dispatch: &ManualDispatch, executor: &Arc<CleanupExecutor>) {
        dispatch.run_all(executor);
        executor.shutdown().wait().unwrap();
    }
    fn owner<T: Send + Sync + 'static>(value: T, generation: u64, dispatch: &ManualDispatch) -> Arc<Owner<T>> {
        Owner::new(
            value,
            generation,
            registration(),
            dispatch.reserve_permit(),
            ResourceId::new(crate::v02::report::ManagerId::new_unique(), generation, generation),
            super::ScopeCompletion::new(),
            None,
        )
    }

    #[test]
    fn test_reservation_release_before_scope_seal_does_not_strand_settlement() {
        let completion = ScopeCompletion::new();
        let report = Arc::new(Mutex::new(None));
        let observed = Arc::clone(&report);
        completion.set_callback(Box::new(move |value| {
            *observed.lock().unwrap() = Some(value);
        }));

        completion.release_pending_reservation();
        completion.seal(1);

        let report = report.lock().unwrap().take().unwrap();
        assert!(report.resources.is_empty());
        assert!(report.failures.is_empty());
    }

    #[test]
    fn test_open_with_zero_users_keeps_value() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = owner(DropMark(Arc::clone(&drops)), 1, &dispatch);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(dispatch.queued(), 0);
        owner.end();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        finish(&dispatch, &executor);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_ended_scope_with_live_user_keeps_value() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = owner(DropMark(Arc::clone(&drops)), 1, &dispatch);
        let user = owner.acquire(1).unwrap();
        owner.end();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(dispatch.queued(), 0);
        user.release();
        assert_eq!(dispatch.queued(), 1);
        finish(&dispatch, &executor);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_end_races_last_ref_cleans_once() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = owner(DropMark(Arc::clone(&drops)), 1, &dispatch);
        let user = owner.acquire(1).unwrap();
        let other = Arc::clone(&owner);
        std::thread::scope(|scope| {
            scope.spawn(move || other.end());
            scope.spawn(move || user.release());
        });
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(dispatch.queued(), 1);
        finish(&dispatch, &executor);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_get_races_end_never_returns_dead_value() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = owner(DropMark(Arc::clone(&drops)), 7, &dispatch);
        let ender = Arc::clone(&owner);
        std::thread::scope(|scope| {
            scope.spawn(move || ender.end());
            if let Ok(user) = owner.acquire(7) {
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                user.release();
            }
        });
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(dispatch.queued(), 1);
        finish(&dispatch, &executor);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_old_key_generation_is_rejected() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let owner = owner((), 2, &dispatch);
        assert!(owner.acquire(1).is_err());
        owner.end();
        finish(&dispatch, &executor);
    }

    #[test]
    fn test_default_drop_panic_retains_unconfirmed_owner_and_report_handle() {
        struct Panics;
        impl Drop for Panics {
            fn drop(&mut self) {
                panic!("release uncertain");
            }
        }
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let completion = ScopeCompletion::new();
        let progress = Arc::new(Mutex::new(Vec::new()));
        let observed_progress = Arc::clone(&progress);
        completion.set_progress_callback(Arc::new(move |value| {
            observed_progress.lock().unwrap().push(value);
        }));
        let report = Arc::new(Mutex::new(None));
        let report_slot = Arc::clone(&report);
        completion.set_callback(Box::new(move |value| *report_slot.lock().unwrap() = Some(value)));
        let owner = Owner::new(
            Panics,
            9,
            registration(),
            dispatch.reserve_permit(),
            ResourceId::new(crate::v02::report::ManagerId::new_unique(), 9, 9),
            Arc::clone(&completion),
            None,
        );
        owner.end();
        completion.seal(1);
        finish(&dispatch, &executor);
        let report = report.lock().unwrap().take().unwrap();
        assert_eq!(report.recoveries.len(), 1);
        assert_eq!(
            *progress.lock().unwrap(),
            vec![
                ResourceProgress::ScopeOpen,
                ResourceProgress::Cleaning,
                ResourceProgress::Unconfirmed
            ]
        );
        assert_eq!(
            report.recoveries[0].snapshot().phase,
            crate::v02::report::RecoveryPhase::Unconfirmed
        );
        assert_eq!(report.failures.len(), 1);
    }

    #[test]
    fn test_ref_after_end_can_clone_and_read() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let owner = owner(String::from("still alive"), 1, &dispatch);
        let held = owner.acquire(1).unwrap();
        owner.end();
        let cloned = held.clone_user(1).unwrap();
        assert_eq!(
            cloned.value_arc().as_deref().map(|value| value.as_str()),
            Some("still alive")
        );
        cloned.release();
        held.release();
        assert_eq!(dispatch.queued(), 1);
        finish(&dispatch, &executor);
    }
}

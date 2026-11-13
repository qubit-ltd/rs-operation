// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

/// One cleanup concurrency slot. Dropping it returns the slot.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::CleanupScheduler;
///
/// let scheduler = CleanupScheduler::new(1);
/// let permit = scheduler.try_acquire().expect("the slot is available");
/// assert_eq!(scheduler.active(), 1);
/// drop(permit);
/// assert_eq!(scheduler.active(), 0);
/// ```
pub struct CleanupPermit {
    /// Counter decremented when this permit is dropped.
    pub(super) active: Arc<AtomicUsize>,
    /// Waiters woken after this permit releases its slot.
    pub(super) capacity_waiters: Arc<Mutex<Vec<Waker>>>,
}

impl Drop for CleanupPermit {
    /// Releases one capacity slot and wakes all registered waiters.
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        let waiters = {
            let mut waiters = self
                .capacity_waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;
    use std::task::Waker;

    use super::CleanupPermit;

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_dropping_permit_releases_slot_and_wakes_waiters() {
        let active = Arc::new(AtomicUsize::new(1));
        let waiters = Arc::new(Mutex::new(Vec::new()));
        let wake_counter = Arc::new(WakeCounter::default());
        waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(Waker::from(Arc::clone(&wake_counter)));
        let permit = CleanupPermit {
            active: Arc::clone(&active),
            capacity_waiters: Arc::clone(&waiters),
        };

        drop(permit);

        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);
        assert!(
            waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
    }
}

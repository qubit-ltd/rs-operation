//! Synchronization primitives shared by typed resource handles.

use std::sync::{Arc, Mutex};

use super::resource_error::ResourceAccessError;
use crate::v02::manager::capacity::ResourceRegistration;

pub(super) struct Slot<T> {
    pub(super) key_generation: u64,
    pub(super) value: Option<Arc<T>>,
    pub(super) users: usize,
    pub(super) open: bool,
    pub(super) registration: Option<ResourceRegistration>,
}

pub(super) struct Owner<T> {
    pub(super) state: Mutex<Slot<T>>,
}

impl<T> Owner<T> {
    pub(super) fn new(
        value: T,
        key_generation: u64,
        registration: Option<ResourceRegistration>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Slot {
                key_generation,
                value: Some(Arc::new(value)),
                users: 0,
                open: true,
                registration,
            }),
        })
    }

    pub(super) fn acquire(
        self: &Arc<Self>,
        generation: u64,
    ) -> Result<Arc<Self>, ResourceAccessError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.key_generation != generation {
            return Err(ResourceAccessError::StaleKey);
        }
        if !state.open || state.value.is_none() {
            return Err(ResourceAccessError::ScopeEnded);
        }
        state.users += 1;
        Ok(Arc::clone(self))
    }

    pub(super) fn clone_user(
        self: &Arc<Self>,
        generation: u64,
    ) -> Result<Arc<Self>, ResourceAccessError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.key_generation != generation {
            return Err(ResourceAccessError::StaleKey);
        }
        if state.users == 0 || state.value.is_none() {
            return Err(ResourceAccessError::ScopeEnded);
        }
        state.users += 1;
        Ok(Arc::clone(self))
    }

    pub(super) fn value_arc(&self) -> Option<Arc<T>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .value
            .as_ref()
            .map(Arc::clone)
    }

    pub(super) fn release(&self) {
        let (value, registration) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.users -= 1;
            Self::take_if_ready(&mut state)
        };
        drop(registration);
        drop(value);
    }

    pub(super) fn end(&self) {
        let (value, registration) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.open = false;
            Self::take_if_ready(&mut state)
        };
        drop(registration);
        drop(value);
    }

    fn take_if_ready(state: &mut Slot<T>) -> (Option<T>, Option<ResourceRegistration>) {
        if !state.open && state.users == 0 {
            let value = state.value.take().map(|value| {
                Arc::into_inner(value).expect("resource user count matches strong references")
            });
            let registration = state.registration.take();
            (value, registration)
        } else {
            (None, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Owner;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct DropMark(Arc<AtomicUsize>);
    impl Drop for DropMark {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn open_with_zero_users_keeps_value() {
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Owner::new(DropMark(Arc::clone(&drops)), 1, None);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(owner);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn ended_scope_with_live_user_keeps_value() {
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Owner::new(DropMark(Arc::clone(&drops)), 1, None);
        let user = owner.acquire(1).unwrap();
        owner.end();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        user.release();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn end_races_last_ref_cleans_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Owner::new(DropMark(Arc::clone(&drops)), 1, None);
        let user = owner.acquire(1).unwrap();
        let other = Arc::clone(&owner);
        std::thread::scope(|scope| {
            scope.spawn(move || other.end());
            scope.spawn(move || user.release());
        });
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn get_races_end_never_returns_dead_value() {
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = Owner::new(DropMark(Arc::clone(&drops)), 7, None);
        let ender = Arc::clone(&owner);
        std::thread::scope(|scope| {
            scope.spawn(move || ender.end());
            let result = owner.acquire(7);
            if let Ok(user) = result {
                assert!(drops.load(Ordering::SeqCst) == 0);
                user.release();
            }
        });
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn old_key_generation_is_rejected() {
        let owner = Owner::new((), 2, None);
        assert!(owner.acquire(1).is_err());
    }

    #[test]
    fn ref_after_end_can_clone_and_read() {
        let owner = Owner::new(String::from("alive"), 1, None);
        let held = owner.acquire(1).unwrap();
        owner.end();
        let cloned = held.clone_user(1).unwrap();
        assert_eq!(
            cloned.value_arc().as_deref().map(|s| s.as_str()),
            Some("alive")
        );
        cloned.release();
        held.release();
    }
}

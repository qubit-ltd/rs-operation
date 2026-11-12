use crate::protocol::{CleanupContext, OwnedCleanup};
use crate::report::{RecoveryKey, RecoveryOwner, ResourceId};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::{future::Future, pin::Pin};

static NEXT_RECOVERY_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

fn next_registry_id() -> u64 {
    NEXT_RECOVERY_REGISTRY_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .expect("recovery registry identity space exhausted")
}

fn next_nonce(counter: &AtomicU64) -> u64 {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |nonce| {
            nonce.checked_add(1)
        })
        .expect("recovery key nonce space exhausted")
}

/// Failure when retaining a recovery owner in the bounded recovery table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    CapacityExceeded,
    RegistryPoisoned,
}

/// Bounded table of typed recovery owners, independent of business admission.
#[derive(Clone)]
pub struct RecoveryRegistry {
    inner: Arc<RecoveryInner>,
}

#[cfg(test)]
mod tests {
    use super::RecoveryRegistry;
    use crate::report::{OperationId, RecoveryKey, RecoveryOwner, ResourceId};

    #[test]
    fn stale_key_is_returned_and_owner_cannot_be_taken_twice() {
        let id = ResourceId::new(OperationId::new(1), 2, 3);
        let registry = RecoveryRegistry::new(1);
        let key = registry.register(id, String::from("owner")).unwrap();
        let nonce = key.nonce;
        assert_eq!(registry.take_recovery(key).unwrap(), "owner");
        let stale = RecoveryKey::<String>::new(registry.inner.registry_id, id, nonce);
        assert!(registry.take_recovery(stale).is_err());
        assert!(registry.is_empty());
    }

    #[test]
    fn wrong_generation_or_type_key_does_not_consume_recovery_owner() {
        let id = ResourceId::new(OperationId::new(2), 1, 8);
        let key = RecoveryKey::<String>::new(9, id, 44);
        let owner = RecoveryOwner::new(&key, String::from("still owned"));
        let old_generation =
            RecoveryKey::<String>::new(9, ResourceId::new(OperationId::new(2), 1, 7), 44);
        let owner = owner.take(old_generation).unwrap_err();
        let wrong_type = RecoveryKey::<u64>::new(9, id, 44);
        let owner = owner.take(wrong_type).unwrap_err();
        assert_eq!(owner.take(key).unwrap(), "still owned");
    }
}

struct RecoveryInner {
    registry_id: u64,
    capacity: usize,
    next_nonce: AtomicU64,
    owners: Mutex<BTreeMap<(u64, ResourceId, u64), RecoveryOwner>>,
    waiters: Mutex<Vec<Waker>>,
}

impl RecoveryRegistry {
    /// Create a recovery table with a fixed number of owner slots.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RecoveryInner {
                registry_id: next_registry_id(),
                capacity,
                next_nonce: AtomicU64::new(1),
                owners: Mutex::new(BTreeMap::new()),
                waiters: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Accept and retain a typed recovery value, returning its one-use key.
    /// If the table is full or poisoned, the value is returned unchanged.
    pub fn register<R: Send + 'static>(
        &self,
        resource: ResourceId,
        value: R,
    ) -> Result<RecoveryKey<R>, R> {
        let nonce = next_nonce(&self.inner.next_nonce);
        let key = RecoveryKey::new(self.inner.registry_id, resource, nonce);
        let owner = RecoveryOwner::new(&key, value);
        let mut owners = match self.inner.owners.lock() {
            Ok(owners) => owners,
            Err(_) => return Err(owner.take(key).ok().expect("matching recovery key")),
        };
        if owners.len() >= self.inner.capacity {
            return Err(owner.take(key).ok().expect("matching recovery key"));
        }
        owners.insert((self.inner.registry_id, resource, nonce), owner);
        Ok(key)
    }

    /// Retain a panic capsule while preparing the separate owner returned by a
    /// `FailedWithRecovery` cleanup outcome. The two values are independently
    /// owned; a panic in the cleanup future cannot consume the retained capsule.
    pub fn register_with_capsule<R: Send + 'static>(
        &self,
        resource: ResourceId,
        capsule: R,
        failure_owner: R,
    ) -> Result<(RecoveryKey<R>, RecoveryOwner), (R, R)> {
        let nonce = next_nonce(&self.inner.next_nonce);
        let key = RecoveryKey::new(self.inner.registry_id, resource, nonce);
        let owner = RecoveryOwner::new(&key, capsule);
        let failed = RecoveryOwner::new(&key, failure_owner);
        let mut owners = match self.inner.owners.lock() {
            Ok(owners) => owners,
            Err(_) => {
                return Err((
                    owner.take(key).ok().expect("matching recovery key"),
                    failed
                        .take(RecoveryKey::new(self.inner.registry_id, resource, nonce))
                        .ok()
                        .expect("matching recovery key"),
                ));
            }
        };
        if owners.len() >= self.inner.capacity {
            return Err((
                owner.take(key).ok().expect("matching recovery key"),
                failed
                    .take(RecoveryKey::new(self.inner.registry_id, resource, nonce))
                    .ok()
                    .expect("matching recovery key"),
            ));
        }
        owners.insert((self.inner.registry_id, resource, nonce), owner);
        Ok((key, failed))
    }

    /// Take a retained value exactly once using the key issued at acceptance.
    /// A missing entry returns the key so the caller can preserve its authority.
    pub fn take_recovery<R: Send + 'static>(
        &self,
        key: RecoveryKey<R>,
    ) -> Result<R, RecoveryKey<R>> {
        let Ok(mut owners) = self.inner.owners.lock() else {
            return Err(key);
        };
        if key.registry_id != self.inner.registry_id {
            return Err(key);
        }
        let Some(owner) = owners.remove(&(key.registry_id, key.resource_id(), key.nonce)) else {
            return Err(key);
        };
        drop(owners);
        match owner.take(key) {
            Ok(value) => {
                let waiters = self
                    .inner
                    .waiters
                    .lock()
                    .map(|mut waiters| std::mem::take(&mut *waiters))
                    .unwrap_or_default();
                for waiter in waiters {
                    waiter.wake();
                }
                Ok(value)
            }
            Err(owner) => {
                // A key issued by this registry cannot mismatch its stored entry.
                // Preserve the payload if internal corruption ever violates that invariant.
                drop(owner);
                unreachable!("recovery entry and key identity diverged")
            }
        }
    }

    /// Explicitly keep an owner retained for later action.
    pub fn keep<R: 'static>(&self, key: &RecoveryKey<R>) -> bool {
        self.inner
            .owners
            .lock()
            .map(|owners| owners.contains_key(&(key.registry_id, key.resource, key.nonce)))
            .unwrap_or(false)
    }

    /// Start a caller-selected retry while leaving the recovery capsule retained.
    /// The retry adapter is consumed only when the key is still registered.
    pub fn retry_cleanup<R: 'static>(
        &self,
        key: &RecoveryKey<R>,
        retry: Box<dyn OwnedCleanup>,
        context: CleanupContext,
    ) -> Result<
        Pin<Box<dyn Future<Output = crate::report::CleanupOutcome> + Send + 'static>>,
        Box<dyn OwnedCleanup>,
    > {
        if !self.keep(key) {
            return Err(retry);
        }
        Ok(retry.begin(context))
    }

    /// Number of retained recovery owners.
    pub fn len(&self) -> usize {
        self.inner
            .owners
            .lock()
            .map(|owners| owners.len())
            .unwrap_or(0)
    }

    /// Whether no recovery owners are currently retained.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Configured recovery capacity.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Register a wakeup while at least one recovery owner remains.
    pub(crate) fn register_waker_if_nonempty(&self, waker: &Waker) -> bool {
        let replacement = waker.clone();
        let owners = match self.inner.owners.lock() {
            Ok(owners) => owners,
            Err(_) => return false,
        };
        if owners.is_empty() {
            drop(owners);
            drop(replacement);
            return false;
        }
        let mut waiters = match self.inner.waiters.lock() {
            Ok(waiters) => waiters,
            Err(_) => {
                drop(owners);
                drop(replacement);
                return true;
            }
        };
        if waiters.iter().any(|old| old.will_wake(waker)) {
            drop(waiters);
            drop(owners);
            drop(replacement);
        } else {
            waiters.push(replacement);
            drop(waiters);
            drop(owners);
        }
        true
    }
}

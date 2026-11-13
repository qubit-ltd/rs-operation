// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

use super::internal::RecoveryInner;
use crate::protocol::CleanupContext;
use crate::protocol::OwnedCleanup;
use crate::report::CleanupOutcome;
use crate::report::RecoveryKey;
use crate::report::RecoveryOwner;
use crate::report::ResourceId;

static NEXT_RECOVERY_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

/// Owned future returned after a retry cleanup action begins.
type CleanupFuture = Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>;

/// Allocates a unique identity for each recovery registry.
///
/// # Returns
///
/// The next process-wide registry identity.
#[must_use]
fn next_registry_id() -> u64 {
    NEXT_RECOVERY_REGISTRY_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .expect("recovery registry identity space exhausted")
}

/// Advances a registry's nonce counter to identify a newly issued key.
///
/// # Parameters
///
/// - `counter`: atomic counter owned by the registry.
///
/// # Returns
///
/// The next key nonce reserved by the registry.
#[must_use]
fn next_nonce(counter: &AtomicU64) -> u64 {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |nonce| nonce.checked_add(1))
        .expect("recovery key nonce space exhausted")
}

/// Bounded table of typed recovery owners, independent of business admission.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::ResourceId;
/// use qubit_operation::resource::RecoveryRegistry;
///
/// let resource = ResourceId::new(OperationId::new(7), 2, 3);
/// let registry = RecoveryRegistry::new(1);
/// let key = registry.register(resource, String::from("owner"))
///     .expect("the registry has capacity");
/// assert_eq!(registry.take_recovery(key).expect("key is current"), "owner");
/// assert!(registry.is_empty());
/// ```
#[derive(Clone)]
pub struct RecoveryRegistry {
    /// Shares bounded owner storage and identity with registry clones.
    inner: Arc<RecoveryInner>,
}

/// Returns both owned values when registration cannot retain the capsule.
///
/// # Type Parameters
///
/// - `R`: type stored by both recovery owners.
///
/// # Parameters
///
/// - `owner`: capsule owner rejected by the registry.
/// - `failed`: separate failure owner rejected with the capsule.
/// - `key`: issued key for the capsule owner.
/// - `resource`: resource identity shared by both owners.
/// - `nonce`: issuance nonce shared by both owners.
/// - `registry_id`: issuing registry identity.
///
/// # Returns
///
/// Both original values recovered with their matching keys.
#[must_use]
fn recover_rejected_owners<R: Send + 'static>(
    owner: RecoveryOwner,
    failed: RecoveryOwner,
    key: RecoveryKey<R>,
    resource: ResourceId,
    nonce: u64,
    registry_id: u64,
) -> (R, R) {
    let retained = owner.take(key).unwrap_or_else(|_| panic!("matching recovery key"));
    let failure_key = RecoveryKey::new(registry_id, resource, nonce);
    let failure = failed
        .take(failure_key)
        .unwrap_or_else(|_| panic!("matching recovery key"));
    (retained, failure)
}

impl RecoveryRegistry {
    /// Create a recovery table with a fixed number of owner slots.
    ///
    /// # Parameters
    ///
    /// - `capacity`: maximum number of recovery owners retained at once.
    ///
    /// # Returns
    ///
    /// An empty recovery registry with the requested bounded capacity.
    ///
    /// # Panics
    ///
    /// Panics if the process exhausts the registry identity space.
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

    /// Number of retained recovery owners.
    ///
    /// # Returns
    ///
    /// The number of owners, or zero if the owner lock is poisoned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.owners.lock().map(|owners| owners.len()).unwrap_or(0)
    }

    /// Whether no recovery owners are currently retained.
    ///
    /// # Returns
    ///
    /// Whether the registry currently retains no owners.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Configured recovery capacity.
    ///
    /// # Returns
    ///
    /// The maximum number of owners the registry retains.
    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Explicitly keep an owner retained for later action.
    ///
    /// # Type Parameters
    ///
    /// - `R`: type associated with the recovery key.
    ///
    /// # Parameters
    ///
    /// - `key`: key whose retained entry should remain available.
    ///
    /// # Returns
    ///
    /// Whether the key currently identifies a retained owner.
    #[must_use]
    pub fn keep<R: 'static>(&self, key: &RecoveryKey<R>) -> bool {
        self.inner
            .owners
            .lock()
            .map(|owners| owners.contains_key(&(key.registry_id, key.resource, key.nonce)))
            .unwrap_or(false)
    }

    /// Accept and retain a typed recovery value, returning its one-use key.
    /// If the table is full or poisoned, the value is returned unchanged.
    ///
    /// # Type Parameters
    ///
    /// - `R`: `Send` value type stored as the recovery owner.
    ///
    /// # Parameters
    ///
    /// - `resource`: resource registration associated with the owner.
    /// - `value`: value to retain until its key is consumed.
    ///
    /// # Returns
    ///
    /// The key on success, or the unchanged value if storage is unavailable.
    ///
    /// # Errors
    ///
    /// Returns the original value if the registry is full or its owner lock is
    /// poisoned.
    ///
    /// # Panics
    ///
    /// Panics if key nonce space is exhausted or an internally constructed
    /// owner fails to match its key.
    pub fn register<R: Send + 'static>(&self, resource: ResourceId, value: R) -> Result<RecoveryKey<R>, R> {
        let nonce = next_nonce(&self.inner.next_nonce);
        let key = RecoveryKey::new(self.inner.registry_id, resource, nonce);
        let owner = RecoveryOwner::new(&key, value);
        let mut owners = match self.inner.owners.lock() {
            Ok(owners) => owners,
            Err(_) => {
                return Err(owner.take(key).unwrap_or_else(|_| panic!("matching recovery key")));
            }
        };
        if owners.len() >= self.inner.capacity {
            return Err(owner.take(key).unwrap_or_else(|_| panic!("matching recovery key")));
        }
        owners.insert((self.inner.registry_id, resource, nonce), owner);
        Ok(key)
    }

    /// Retain a panic capsule while preparing the separate owner returned by a
    /// `FailedWithRecovery` cleanup outcome. The two values are independently
    /// owned; a panic in the cleanup future cannot consume the retained
    /// capsule.
    ///
    /// # Type Parameters
    ///
    /// - `R`: `Send` type of both independently owned values.
    ///
    /// # Parameters
    ///
    /// - `resource`: resource registration associated with both owners.
    /// - `capsule`: value retained for later recovery.
    /// - `failure_owner`: separate value returned to the cleanup outcome.
    ///
    /// # Returns
    ///
    /// Both the key and failure owner on success, or both original values if
    /// the registry cannot retain the capsule.
    ///
    /// # Errors
    ///
    /// Returns the capsule and failure owner unchanged if the registry is full
    /// or its owner lock is poisoned.
    ///
    /// # Panics
    ///
    /// Panics if key nonce space is exhausted or either internally constructed
    /// owner fails to match its key.
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
                return Err(recover_rejected_owners(
                    owner,
                    failed,
                    key,
                    resource,
                    nonce,
                    self.inner.registry_id,
                ));
            }
        };
        if owners.len() >= self.inner.capacity {
            return Err(recover_rejected_owners(
                owner,
                failed,
                key,
                resource,
                nonce,
                self.inner.registry_id,
            ));
        }
        owners.insert((self.inner.registry_id, resource, nonce), owner);
        Ok((key, failed))
    }

    /// Take a retained value exactly once using the key issued at acceptance.
    /// A missing entry returns the key so the caller can preserve its
    /// authority.
    ///
    /// # Type Parameters
    ///
    /// - `R`: expected type of the retained value.
    ///
    /// # Parameters
    ///
    /// - `key`: one-use key issued when the value was registered.
    ///
    /// # Returns
    ///
    /// The retained value, or the unchanged key if it does not identify an
    /// available value.
    ///
    /// # Errors
    ///
    /// Returns the key if its registry, resource, generation, or value type
    /// does not match a retained owner, or if the owner lock is poisoned.
    ///
    /// # Panics
    ///
    /// Panics if the stored owner does not match its registry-issued key, which
    /// indicates internal state corruption.
    pub fn take_recovery<R: Send + 'static>(&self, key: RecoveryKey<R>) -> Result<R, RecoveryKey<R>> {
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

    /// Start a caller-selected retry while leaving the recovery capsule
    /// retained. The retry adapter is consumed only when the key is still
    /// registered.
    ///
    /// # Type Parameters
    ///
    /// - `R`: type associated with the recovery key.
    ///
    /// # Parameters
    ///
    /// - `key`: key that must still identify a retained owner.
    /// - `retry`: cleanup adapter to begin if the key is current.
    /// - `context`: context passed to the cleanup adapter.
    ///
    /// # Returns
    ///
    /// The started cleanup future, or the unchanged adapter if the key is
    /// stale or missing.
    ///
    /// # Errors
    ///
    /// Returns the retry adapter unchanged when no retained owner matches the
    /// key.
    pub fn retry_cleanup<R: 'static>(
        &self,
        key: &RecoveryKey<R>,
        retry: Box<dyn OwnedCleanup>,
        context: CleanupContext,
    ) -> Result<CleanupFuture, Box<dyn OwnedCleanup>> {
        if !self.keep(key) {
            return Err(retry);
        }
        Ok(retry.begin(context))
    }

    /// Register a wakeup while at least one recovery owner remains.
    ///
    /// # Parameters
    ///
    /// - `waker`: task to wake after a retained owner is removed.
    ///
    /// # Returns
    ///
    /// Whether an owner was present when the wakeup was registered.
    #[must_use]
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

#[cfg(test)]
mod tests {
    use super::RecoveryRegistry;
    use crate::report::OperationId;
    use crate::report::RecoveryKey;
    use crate::report::ResourceId;

    #[test]
    fn test_stale_key_is_returned_and_owner_cannot_be_taken_twice() {
        let id = ResourceId::new(OperationId::new(1), 2, 3);
        let registry = RecoveryRegistry::new(1);
        let key = registry.register(id, String::from("owner")).unwrap();
        let nonce = key.nonce;
        assert_eq!(registry.take_recovery(key).unwrap(), "owner");
        let stale = RecoveryKey::<String>::new(registry.inner.registry_id, id, nonce);
        assert!(registry.take_recovery(stale).is_err());
        assert!(registry.is_empty());
    }
}

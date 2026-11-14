use super::core::Owner;
use super::resource_error::{ResourceAccessError, ResourceCapacityError, ResourceCommitError};
use super::{ResourceKey, ResourceRef, ResourceReservation};
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::report::{ManagerId, ResourceScopeId};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

trait ErasedOwner: Any + Send + Sync {
    fn end(&self);
    fn as_any(&self) -> &dyn Any;
}
impl<T: Send + Sync + 'static> ErasedOwner for Arc<Owner<T>> {
    fn end(&self) {
        Owner::end(self);
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
struct Entry {
    owner: Box<dyn ErasedOwner>,
}
struct State {
    open: bool,
    next_slot: u64,
    generation: u64,
    slots: HashMap<u64, Entry>,
}
/// Independently endable heterogeneous resource scope.
pub struct ResourceScope {
    manager: ManagerId,
    id: ResourceScopeId,
    capacity: Arc<ScopeCapacity>,
    state: Mutex<State>,
}
impl ResourceScope {
    pub(crate) fn new(
        manager: ManagerId,
        id: ResourceScopeId,
        capacity: Arc<ScopeCapacity>,
    ) -> Self {
        Self {
            manager,
            id,
            capacity,
            state: Mutex::new(State {
                open: true,
                next_slot: 1,
                generation: 1,
                slots: HashMap::new(),
            }),
        }
    }
    pub fn reserve(&self) -> Result<ResourceReservation, ResourceCapacityError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.open {
            return Err(ResourceCapacityError::ScopeEnded);
        }
        self.capacity
            .resource_registration()
            .map(ResourceReservation::new)
            .map_err(|_| ResourceCapacityError::Exhausted)
    }
    pub fn manage<T: Send + Sync + 'static>(
        &self,
        value: T,
    ) -> Result<ResourceRef<T>, (ResourceCapacityError, T)> {
        let reservation = match self.reserve() {
            Ok(r) => r,
            Err(e) => return Err((e, value)),
        };
        self.manage_reserved(value, reservation).map_err(|error| {
            let (reason, value, reservation) = error.into_parts();
            drop(reservation);
            (reason, value)
        })
    }
    pub fn manage_reserved<T: Send + Sync + 'static>(
        &self,
        value: T,
        mut reservation: ResourceReservation,
    ) -> Result<ResourceRef<T>, ResourceCommitError<T>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.open {
            return Err(ResourceCommitError::new(
                ResourceCapacityError::ScopeEnded,
                value,
                reservation,
            ));
        }
        let slot = state.next_slot;
        state.next_slot = state.next_slot.wrapping_add(1);
        let generation = state.generation;
        let registration = reservation
            .take()
            .expect("reservation contains capacity permit");
        let owner = Owner::new(value, generation, Some(registration));
        let value = owner.value_arc().expect("new owner contains its resource");
        let owner_ref = owner.acquire(generation).expect("new owner is open");
        state.slots.insert(
            slot,
            Entry {
                owner: Box::new(owner),
            },
        );
        Ok(ResourceRef {
            owner: owner_ref,
            value: Some(value),
            generation,
            manager: self.manager,
            scope: self.id,
            slot,
        })
    }
    pub fn get_resource<T: Send + Sync + 'static>(
        &self,
        key: ResourceKey<T>,
    ) -> Result<ResourceRef<T>, ResourceAccessError> {
        if key.manager != self.manager {
            return Err(ResourceAccessError::WrongManager);
        }
        if key.scope != self.id {
            return Err(ResourceAccessError::WrongScope);
        }
        if key.type_id != std::any::TypeId::of::<T>() {
            return Err(ResourceAccessError::WrongType);
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.open {
            return Err(ResourceAccessError::ScopeEnded);
        }
        let entry = state
            .slots
            .get(&key.slot)
            .ok_or(ResourceAccessError::StaleKey)?;
        let owner = entry
            .owner
            .as_any()
            .downcast_ref::<Arc<Owner<T>>>()
            .ok_or(ResourceAccessError::WrongType)?;
        let owner_ref = owner.acquire(key.generation)?;
        let value = owner.value_arc().ok_or(ResourceAccessError::StaleKey)?;
        Ok(ResourceRef {
            owner: owner_ref,
            value: Some(value),
            generation: key.generation,
            manager: self.manager,
            scope: self.id,
            slot: key.slot,
        })
    }
    pub fn end(&self) {
        let slots = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.open {
                return;
            }
            state.open = false;
            state.generation = state.generation.wrapping_add(1);
            std::mem::take(&mut state.slots)
        };
        for (_, entry) in slots {
            entry.owner.end();
        }
    }
}

impl Drop for ResourceScope {
    fn drop(&mut self) {
        self.end();
    }
}

#[cfg(test)]
mod tests {
    use super::ResourceScope;
    use crate::v02::manager::capacity::{ManagerCapacityPools, ScopeCapacity};
    use crate::v02::report::{ManagerId, ManagerLimits, ResourceScopeId, ScopeLimits};
    use std::sync::Arc;

    fn scope() -> ResourceScope {
        let manager = ManagerCapacityPools::new(ManagerLimits::default());
        ResourceScope::new(
            ManagerId::new_unique(),
            ResourceScopeId::new_unique(),
            Arc::new(ScopeCapacity::root(&manager, 42, ScopeLimits::default())),
        )
    }

    #[test]
    fn open_scope_reacquires_resource_by_typed_key() {
        let scope = scope();
        let first = scope.manage(String::from("ready")).unwrap();
        let key = first.key();
        let second = scope.get_resource(key).unwrap();
        assert_eq!(&*second, "ready");
        assert_eq!(first.id(), second.id());
    }

    #[test]
    fn ended_scope_keeps_live_reference_readable_and_disallows_get() {
        let scope = scope();
        let resource = scope.manage(18_u32).unwrap();
        let key = resource.key();
        scope.end();
        assert_eq!(*resource, 18);
        assert!(scope.get_resource(key).is_err());
    }

    #[test]
    fn ended_commit_returns_resource_and_reservation() {
        let scope = scope();
        let reservation = scope.reserve().unwrap();
        scope.end();
        let error = match scope.manage_reserved(String::from("owner"), reservation) {
            Ok(_) => panic!("ended scope accepted resource"),
            Err(error) => error,
        };
        let (reason, resource, reservation) = error.into_parts();
        assert_eq!(
            reason,
            crate::v02::resource::ResourceCapacityError::ScopeEnded
        );
        assert_eq!(resource, "owner");
        drop(reservation);
    }
}

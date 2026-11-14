use crate::v02::report::{ManagerId, ResourceScopeId};
use std::any::TypeId;
use std::marker::PhantomData;

/// A typed, scope-bound lookup key. It does not keep the resource alive.
pub struct ResourceKey<T: Send + Sync + 'static> {
    pub(crate) manager: ManagerId,
    pub(crate) scope: ResourceScopeId,
    pub(crate) slot: u64,
    pub(crate) generation: u64,
    pub(crate) type_id: TypeId,
    pub(crate) marker: PhantomData<fn() -> T>,
}
impl<T: Send + Sync + 'static> Clone for ResourceKey<T> {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager,
            scope: self.scope,
            slot: self.slot,
            generation: self.generation,
            type_id: self.type_id,
            marker: PhantomData,
        }
    }
}
impl<T: Send + Sync + 'static> std::fmt::Debug for ResourceKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceKey")
            .field("slot", &self.slot)
            .field("generation", &self.generation)
            .finish()
    }
}

use super::core::Owner;
use super::resource_key::ResourceKey;
use crate::v02::report::{ManagerId, ResourceId, ResourceScopeId};
use std::ops::Deref;
use std::sync::Arc;

/// A strong user reference. Each clone independently delays scope cleanup.
pub struct ResourceRef<T: Send + Sync + 'static> {
    pub(super) owner: Arc<Owner<T>>,
    pub(super) value: Option<Arc<T>>,
    pub(super) generation: u64,
    pub(super) manager: ManagerId,
    pub(super) scope: ResourceScopeId,
    pub(super) slot: u64,
}
impl<T: Send + Sync + 'static> Clone for ResourceRef<T> {
    fn clone(&self) -> Self {
        let owner = self
            .owner
            .clone_user(self.generation)
            .expect("a live ResourceRef keeps its generation valid");
        Self {
            owner,
            value: self.value.as_ref().map(Arc::clone),
            generation: self.generation,
            manager: self.manager,
            scope: self.scope,
            slot: self.slot,
        }
    }
}
impl<T: Send + Sync + 'static> Deref for ResourceRef<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value
            .as_deref()
            .expect("live ResourceRef owns its resource")
    }
}
impl<T: Send + Sync + 'static> Drop for ResourceRef<T> {
    fn drop(&mut self) {
        drop(self.value.take());
        self.owner.release();
    }
}
impl<T: Send + Sync + 'static> ResourceRef<T> {
    pub fn id(&self) -> ResourceId {
        ResourceId::new(self.manager, self.slot, self.generation)
    }
    pub fn key(&self) -> ResourceKey<T> {
        ResourceKey {
            manager: self.manager,
            scope: self.scope,
            slot: self.slot,
            generation: self.generation,
            type_id: std::any::TypeId::of::<T>(),
            marker: std::marker::PhantomData,
        }
    }
}

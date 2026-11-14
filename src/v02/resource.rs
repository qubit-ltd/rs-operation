//! Typed resources whose lifetime is bounded by an operation scope.

mod core;
mod resource_error;
mod resource_key;
mod resource_ref;
mod resource_reservation;
mod resource_scope;

pub use resource_error::{ResourceAccessError, ResourceCapacityError, ResourceCommitError};
pub use resource_key::ResourceKey;
pub use resource_ref::ResourceRef;
pub use resource_reservation::ResourceReservation;
pub use resource_scope::ResourceScope;

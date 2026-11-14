//! Typed resources whose lifetime is bounded by an operation scope.

mod core;
pub(crate) mod cleanup;
pub(crate) mod cleanup_port;
pub(crate) mod dependency;
pub(crate) mod recovery;
pub(crate) mod transfer;
mod resource_error;
mod resource_key;
mod resource_ref;
mod resource_reservation;
mod resource_scope;

pub use resource_error::{ManageResourceError, ResourceCapacityError, ResourceCommitError};
pub use resource_key::ResourceKey;
pub use resource_ref::ResourceRef;
pub use resource_reservation::ResourceReservation;
pub use resource_scope::ResourceScope;

//! Typed resources whose lifetime is bounded by an operation scope.

pub(crate) mod cleanup;
pub(crate) mod cleanup_port;
mod core;
pub(crate) mod dependency;
pub mod recovery;
mod resource_error;
mod resource_key;
mod resource_ref;
mod resource_reservation;
mod resource_scope;
pub mod service;
pub(crate) mod transfer;

pub use cleanup::AsyncCleanup;
pub use cleanup::CleanupFuture;
pub use cleanup::CleanupOutcome;
pub use recovery::AsyncRecoveryAttempt;
pub use recovery::RecoveryAttempt;
pub use recovery::RecoveryHandle;
pub use recovery::RecoveryStore;
pub use recovery::RecoveryStoreShutdownError;
pub use recovery::RecoveryStoreShutdownHandle;
pub use resource_error::ManageResourceError;
pub use resource_error::ManageWithError;
pub use resource_error::ResourceCommitError;
pub use resource_error::ResourceCommitWithError;
pub use resource_key::ResourceKey;
pub use resource_ref::ResourceRef;
pub use resource_reservation::ResourceReservation;
pub use resource_scope::ResourceScope;
pub use service::ServiceResourceScope;

pub use crate::v02::report::RecoveryStoreOptions;
pub use crate::v02::report::ResourceCapacityError;

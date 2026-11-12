use core::marker::PhantomData;

/// Stable identifier for an operation in a supervisor.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Stable identifier for an operation within its supervisor.
pub struct OperationId(pub u128);

impl OperationId {
    /// Construct an ID assigned by an owning supervisor or test fixture.
    pub const fn new(value: u128) -> Self {
        Self(value)
    }
    /// Return the numeric representation without changing ownership.
    pub const fn get(self) -> u128 {
        self.0
    }
}

/// Stable identifier for a resource registration, including its generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Resource registration identity; slot reuse must increment `generation`.
pub struct ResourceId {
    /// Owning operation registration.
    pub operation: OperationId,
    /// Slot within that operation's bounded resource registry.
    pub slot: u32,
    /// Generation preventing an old ID from matching a reused slot.
    pub generation: u32,
}

impl ResourceId {
    /// Create an identifier from an owning operation and registry coordinates.
    pub const fn new(operation: OperationId, slot: u32, generation: u32) -> Self {
        Self {
            operation,
            slot,
            generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Identity of one monotonic clock domain.
pub struct ClockDomainId(pub u64);

/// Opaque typed key for taking a resource's recovery owner exactly once.
///
/// It intentionally exposes no `Any` or downcast operation.
/// A key is issued by the resource registry and is not `Clone`.
pub struct RecoveryKey<R: 'static> {
    pub(crate) resource: ResourceId,
    pub(crate) nonce: u64,
    pub(crate) marker: PhantomData<fn() -> R>,
}

impl<R> RecoveryKey<R> {
    #[allow(dead_code)] // Issued by the recovery registry in the recovery implementation task.
    pub(crate) const fn new(resource: ResourceId, nonce: u64) -> Self {
        Self {
            resource,
            nonce,
            marker: PhantomData,
        }
    }
    /// Return the exact resource registration bound to this key.
    pub const fn resource_id(&self) -> ResourceId {
        self.resource
    }
}

impl<R> core::fmt::Debug for RecoveryKey<R> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RecoveryKey")
            .field("resource", &self.resource)
            .finish_non_exhaustive()
    }
}

/// Evidence that a cleanup owner was accepted by a new recovery owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
/// Evidence that a cleanup owner was accepted by a new recovery owner.
pub struct TransferReceipt {
    /// Resource whose responsibility was transferred.
    pub resource: ResourceId,
    /// Generation accepted by the receiving recovery registry.
    pub generation: u32,
}

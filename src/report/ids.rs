use core::marker::PhantomData;

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

/// Identity of one monotonic clock domain.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
/// Evidence that a cleanup owner was accepted by a new recovery owner.
///
/// Callers can inspect receipts but cannot construct one before a receiving
/// registry accepts responsibility.
///
/// ```compile_fail
/// use qubit_operation::report::{ResourceId, OperationId, TransferReceipt};
/// let resource = ResourceId::new(OperationId::new(1), 2, 3);
/// let _ = TransferReceipt { resource, generation: 3 };
/// ```
pub struct TransferReceipt {
    resource: ResourceId,
    generation: u32,
    receiver: ResourceId,
}

impl TransferReceipt {
    /// Create a receipt only after the receiving registry accepted responsibility.
    #[allow(dead_code)] // Called by the recovery registry after accepting an owner.
    pub(crate) const fn accepted_by_registry(resource: ResourceId, receiver: ResourceId) -> Self {
        Self {
            resource,
            generation: resource.generation,
            receiver,
        }
    }

    /// Return the resource whose responsibility was transferred.
    pub const fn resource(self) -> ResourceId {
        self.resource
    }

    /// Return the accepted resource generation.
    pub const fn generation(self) -> u32 {
        self.generation
    }

    /// Resource identity assigned by the registry that accepted responsibility.
    pub const fn receiver(self) -> ResourceId {
        self.receiver
    }
}

/// Receiver-issued evidence that an accepted transferred resource was released.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReleaseReceipt {
    source: ResourceId,
    receiver: ResourceId,
}

impl ReleaseReceipt {
    pub(crate) const fn confirmed(source: ResourceId, receiver: ResourceId) -> Self {
        Self { source, receiver }
    }

    /// Original resource identity whose transferred tombstone can be settled.
    pub const fn source(self) -> ResourceId {
        self.source
    }

    /// Receiver resource identity whose release was confirmed.
    pub const fn receiver(self) -> ResourceId {
        self.receiver
    }
}

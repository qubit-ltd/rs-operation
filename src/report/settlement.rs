use super::{DiagnosticSummary, OperationId, RecoveryKey, ResourceId, TransferReceipt};
use std::{error::Error, fmt, sync::Arc};

/// Phase in which cleanup failed or transferred responsibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupStage {
    Closing,
    Releasing,
    Removing,
    Transferring,
    Recovery,
}

/// Cleanup failure with a bounded summary and optional shared typed source.
#[derive(Clone)]
pub struct CleanupFailure {
    /// Resource whose cleanup failed.
    pub resource_id: ResourceId,
    /// Cleanup phase at failure.
    pub stage: CleanupStage,
    /// Sanitized bounded diagnostic text.
    pub summary: DiagnosticSummary,
    /// Typed source shared by `Arc`; arbitrary errors need not implement `Clone`.
    pub source: Option<Arc<dyn Error + Send + Sync>>,
}

impl fmt::Debug for CleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CleanupFailure")
            .field("resource_id", &self.resource_id)
            .field("stage", &self.stage)
            .field("summary", &self.summary)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

/// Terminal fact returned by a cleanup action.
#[derive(Debug)]
pub enum CleanupOutcome {
    /// Cleanup completed and release was confirmed.
    Cleaned,
    /// Another owner accepted responsibility, with explicit receipt.
    Transferred(TransferReceipt),
    /// Cleanup failed while retaining an owned recovery payload.
    FailedWithRecovery(CleanupFailure, RecoveryOwner),
}

/// Opaque owned recovery payload. Taking it requires the original typed key.
/// This object cannot be cloned or inspected through `Any`.
///
/// ```compile_fail
/// use qubit_operation::report::{RecoveryOwner, RecoveryKey};
/// let _ = RecoveryOwner::new::<String>(unreachable!(), String::new());
/// ```
pub struct RecoveryOwner {
    pub(crate) resource_id: ResourceId,
    pub(crate) nonce: u64,
    pub(crate) value: Box<dyn std::any::Any + Send>,
}

impl RecoveryOwner {
    /// Erase an owner behind the key issued for its resource registration.
    #[allow(dead_code)] // Called by the recovery registry introduced with T08.
    pub(crate) fn new<R: Send + 'static>(key: &RecoveryKey<R>, value: R) -> Self {
        Self {
            resource_id: key.resource_id(),
            nonce: key.nonce,
            value: Box::new(value),
        }
    }

    /// Return the resource registration whose owner is held here.
    pub fn resource_id(&self) -> ResourceId {
        self.resource_id
    }

    /// Recover the original owner with its matching typed key.
    ///
    /// On a type, resource-generation, or key mismatch, returns this owner
    /// unchanged. The erased value is never exposed for arbitrary downcasting.
    pub fn take<R: Send + 'static>(self, key: RecoveryKey<R>) -> Result<R, Self> {
        if key.resource_id() != self.resource_id || key.nonce != self.nonce {
            return Err(self);
        }
        match self.value.downcast::<R>() {
            Ok(value) => Ok(*value),
            Err(value) => Err(Self {
                resource_id: self.resource_id,
                nonce: self.nonce,
                value,
            }),
        }
    }
}

impl fmt::Debug for RecoveryOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryOwner")
            .field("resource_id", &self.resource_id)
            .finish_non_exhaustive()
    }
}

/// Stable child settlement fact, separate from its business result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildSettlement {
    /// Child operation identity.
    pub operation_id: OperationId,
    /// Whether the domain confirmed that the child released its resources.
    pub released: bool,
    /// Sanitized bounded diagnostic summary.
    pub summary: DiagnosticSummary,
}

/// Historical settlement snapshot; future recovery actions do not mutate it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettlementReport {
    /// Operation being settled.
    pub operation_id: OperationId,
    /// Aggregate resource lifecycle phase.
    pub resource_phase: super::ResourcePhase,
    /// True only when all required release/cleanup responsibilities are settled.
    pub all_released: bool,
    /// Bounded child settlement facts.
    pub children: Arc<[ChildSettlement]>,
    /// Bounded cleanup failure summaries, without recovery payloads.
    pub cleanup_failures: Arc<[CleanupFailureSummary]>,
    /// Explicitly accepted responsibility transfers.
    pub transfers: Arc<[TransferReceipt]>,
    /// Number of resources whose final release is still unconfirmed.
    pub unconfirmed_count: usize,
}

/// Copyable cleanup failure fact that excludes typed sources and owner payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupFailureSummary {
    /// Resource whose cleanup failed.
    pub resource_id: ResourceId,
    /// Cleanup stage that failed.
    pub stage: CleanupStage,
    /// Sanitized bounded summary.
    pub summary: DiagnosticSummary,
}

#[cfg(test)]
mod tests {
    use super::{RecoveryOwner, ResourceId};
    use crate::report::{OperationId, RecoveryKey};

    #[test]
    fn recovery_key_must_match_resource_generation_and_nonce() {
        let id = ResourceId::new(OperationId::new(1), 4, 8);
        let owner = RecoveryOwner::new(&RecoveryKey::<String>::new(id, 13), String::from("owned"));
        let wrong = RecoveryKey::<String>::new(ResourceId::new(OperationId::new(1), 4, 7), 13);
        let owner = owner.take(wrong).unwrap_err();
        let wrong = RecoveryKey::<String>::new(id, 12);
        let owner = owner.take(wrong).unwrap_err();
        let key = RecoveryKey::<String>::new(id, 13);
        assert_eq!(owner.take(key).unwrap(), "owned");
    }

    #[test]
    fn recovery_key_must_match_the_erased_owner_type() {
        let id = ResourceId::new(OperationId::new(2), 1, 3);
        let owner = RecoveryOwner::new(&RecoveryKey::<String>::new(id, 21), String::from("owned"));
        let wrong_type = RecoveryKey::<u64>::new(id, 21);

        let owner = owner.take(wrong_type).unwrap_err();
        assert_eq!(
            owner.take(RecoveryKey::<String>::new(id, 21)).unwrap(),
            "owned"
        );
    }
}

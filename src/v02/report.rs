//! Version 0.2 operation identities, outcomes, limits, and reports.

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::protocol::{Deadline, MonotonicTime};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u128 {
    u128::from(NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u128);

        impl $name {
            #[allow(dead_code)]
            pub(crate) fn new_unique() -> Self {
                Self(next_id())
            }

            #[allow(dead_code)]
            pub(crate) const fn from_raw(value: u128) -> Self {
                Self(value)
            }

            #[allow(dead_code)]
            pub(crate) const fn raw(self) -> u128 {
                self.0
            }
        }
    };
}

opaque_id!(ManagerId);
opaque_id!(OperationId);
opaque_id!(GroupId);
opaque_id!(ResourceScopeId);
opaque_id!(RecoveryId);
opaque_id!(RecoveryStoreId);

/// Identity of one resource slot and generation within a manager.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceId {
    manager: ManagerId,
    slot: u64,
    generation: u64,
}

impl ResourceId {
    #[allow(dead_code)]
    pub(crate) const fn new(manager: ManagerId, slot: u64, generation: u64) -> Self {
        Self {
            manager,
            slot,
            generation,
        }
    }

    #[allow(dead_code)]
    pub(crate) const fn manager(self) -> ManagerId {
        self.manager
    }
    #[allow(dead_code)]
    pub(crate) const fn slot(self) -> u64 {
        self.slot
    }
    #[allow(dead_code)]
    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }
}

/// A bounded diagnostic string. Truncation never splits a UTF-8 code point.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticSummary {
    text: String,
    truncated: bool,
}

impl DiagnosticSummary {
    pub const MAX_BYTES: usize = 256;

    pub fn new(text: &str) -> Self {
        if text.len() <= Self::MAX_BYTES {
            return Self {
                text: text.to_owned(),
                truncated: false,
            };
        }
        let mut end = Self::MAX_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            text: text[..end].to_owned(),
            truncated: true,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
    pub const fn is_truncated(&self) -> bool {
        self.truncated
    }
}

impl fmt::Display for DiagnosticSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagerLimits {
    pub active_operations: usize,
    pub queued_operations: usize,
    pub admission_waiters: usize,
    pub unconsumed_outcomes: usize,
    pub cleanup_concurrency: usize,
    pub diagnostic_events: usize,
}

impl Default for ManagerLimits {
    fn default() -> Self {
        Self {
            active_operations: 4,
            queued_operations: 64,
            admission_waiters: 64,
            unconsumed_outcomes: 64,
            cleanup_concurrency: 4,
            diagnostic_events: 256,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeLimits {
    pub child_operations: usize,
    pub resources: usize,
    pub effect_records: usize,
}

impl Default for ScopeLimits {
    fn default() -> Self {
        Self {
            child_operations: 64,
            resources: 128,
            effect_records: 64,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortableExecutorOptions {
    pub worker_count: usize,
    pub task_capacity: usize,
    pub polls_per_turn: usize,
}
impl Default for PortableExecutorOptions {
    fn default() -> Self {
        Self {
            worker_count: 4,
            task_capacity: 68,
            polls_per_turn: 16,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThreadPoolExecutorOptions {
    pub worker_count: usize,
    pub queue_capacity: usize,
}
impl Default for ThreadPoolExecutorOptions {
    fn default() -> Self {
        Self {
            worker_count: 4,
            queue_capacity: 64,
        }
    }
}

#[derive(Clone, Default)]
pub struct ContextOptions {
    pub clock: Option<Arc<dyn super::protocol::ClockSource>>,
    pub deadline: Option<Deadline>,
    pub budget: Option<Arc<dyn super::protocol::BudgetPolicy>>,
}

#[derive(Clone)]
pub struct ManagerOptions {
    pub limits: ManagerLimits,
    pub scope_limits: ScopeLimits,
    pub async_executor: PortableExecutorOptions,
    pub sync_executor: ThreadPoolExecutorOptions,
    pub cleanup_executor: ThreadPoolExecutorOptions,
    pub clock: Option<Arc<dyn super::protocol::ClockSource>>,
}

impl Default for ManagerOptions {
    fn default() -> Self {
        Self {
            limits: ManagerLimits::default(),
            scope_limits: ScopeLimits::default(),
            async_executor: PortableExecutorOptions::default(),
            sync_executor: ThreadPoolExecutorOptions::default(),
            cleanup_executor: ThreadPoolExecutorOptions::default(),
            clock: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildFailurePolicy {
    StopSiblings,
    ContinueSiblings,
}
impl Default for ChildFailurePolicy {
    fn default() -> Self {
        Self::StopSiblings
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildRequirement {
    Required,
    Optional,
}
impl Default for ChildRequirement {
    fn default() -> Self {
        Self::Required
    }
}

#[derive(Clone)]
pub struct OperationOptions {
    pub requirement: ChildRequirement,
    pub limits: ScopeLimits,
    pub child_failure_policy: ChildFailurePolicy,
    pub context: ContextOptions,
}

impl Default for OperationOptions {
    fn default() -> Self {
        Self {
            requirement: ChildRequirement::Required,
            limits: ScopeLimits::default(),
            child_failure_policy: ChildFailurePolicy::StopSiblings,
            context: ContextOptions::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeAvailability {
    Pending,
    Available,
    Taken,
    Discarded,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionPhase {
    Queued,
    Running,
    Finished,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceProgress {
    ScopeOpen,
    WaitingForUsers,
    Cleaning,
    Unconfirmed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettlementStatus {
    Released,
    Transferred,
    RecoveryRequired,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildOutcomeKind {
    Succeeded,
    Failed,
    Stopped,
    Panicked,
    ExecutionFailed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopRequestStatus {
    Accepted,
    AlreadyRequested,
    AlreadyFinished,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownMode {
    Drain,
    Cancel,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupEvidence {
    DropCompleted,
    Confirmed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupStage {
    Drop,
    AsyncRelease,
    Dependency,
    Transfer,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectFact {
    Known,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    UserCancelled,
    Timeout,
    ParentStopped,
    ManagerShutdown,
    RequiredChildFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StopReport {
    pub reason: StopReason,
    pub accepted_at: Option<MonotonicTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildOutcomeSummary {
    pub operation_id: OperationId,
    pub kind: ChildOutcomeKind,
    pub summary: DiagnosticSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildFailureSummary {
    pub operation_id: OperationId,
    pub outcome: ChildOutcomeSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PanicSummary {
    pub operation_id: OperationId,
    pub phase: ExecutionPhase,
    pub summary: DiagnosticSummary,
}

#[derive(Clone)]
pub struct ExecutionFailure {
    pub operation_id: OperationId,
    pub summary: DiagnosticSummary,
    pub source: Option<Arc<dyn Error + Send + Sync>>,
}

impl fmt::Debug for ExecutionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionFailure")
            .field("operation_id", &self.operation_id)
            .field("summary", &self.summary)
            .field("source", &self.source.as_ref().map(|e| e.to_string()))
            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum OperationOutcome<T, E> {
    Succeeded(T),
    Failed(E),
    ChildFailed(ChildFailureSummary),
    Stopped(StopReport),
    Panicked(PanicSummary),
    ExecutionFailed(ExecutionFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeError {
    AlreadyTaken,
    Discarded,
    WouldDeadlock,
}
impl fmt::Display for OutcomeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for OutcomeError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationError {
    WouldDeadlock,
    ClockUnavailable,
    ClockDomainMismatch,
}
impl fmt::Display for ObservationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for ObservationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    Busy,
    Closed,
    ScopeEnded,
    Stopped,
    DeadlineExpired,
    CapacityExceeded,
    ExecutorUnavailable,
    WouldDeadlock,
}
impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for AdmissionError {}

#[must_use = "a rejected submission retains its uncalled body"]
pub struct SubmitError<F> {
    reason: AdmissionError,
    body: F,
}
impl<F> SubmitError<F> {
    pub fn new(reason: AdmissionError, body: F) -> Self {
        Self { reason, body }
    }
    pub fn reason(&self) -> AdmissionError {
        self.reason
    }
    pub fn into_parts(self) -> (AdmissionError, F) {
        (self.reason, self.body)
    }
}
impl<F> fmt::Debug for SubmitError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubmitError")
            .field("reason", &self.reason)
            .field("body", &"<retained>")
            .finish()
    }
}
impl<F> fmt::Display for SubmitError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "submission rejected: {}", self.reason)
    }
}
impl<F> Error for SubmitError<F> {}

#[must_use = "the operation runner may retain an uncalled submission body"]
pub enum RunError<F> {
    Submit(SubmitError<F>),
    Outcome(OutcomeError),
    Observation(ObservationError),
}
impl<F> fmt::Debug for RunError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Submit(error) => f.debug_tuple("Submit").field(error).finish(),
            Self::Outcome(error) => f.debug_tuple("Outcome").field(error).finish(),
            Self::Observation(error) => f.debug_tuple("Observation").field(error).finish(),
        }
    }
}
impl<F> fmt::Display for RunError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Submit(error) => fmt::Display::fmt(error, f),
            Self::Outcome(error) => fmt::Display::fmt(error, f),
            Self::Observation(error) => fmt::Display::fmt(error, f),
        }
    }
}
impl<F> Error for RunError<F> {}

#[derive(Clone, Debug)]
pub struct OperationCompletion<T, E> {
    pub outcome: OperationOutcome<T, E>,
    pub settlement: Arc<SettlementReport>,
}

#[derive(Clone, Debug)]
pub struct OperationSnapshot {
    pub operation_id: OperationId,
    pub execution: ExecutionPhase,
    pub outcome: OutcomeAvailability,
    pub stop: Option<StopReport>,
    pub resources: Option<ResourceProgress>,
    pub settlement: Option<Arc<SettlementReport>>,
}

#[derive(Clone, Debug)]
pub struct ChildSettlement {
    pub operation_id: OperationId,
    pub outcome: ChildOutcomeSummary,
    pub settlement: Arc<SettlementReport>,
}
#[derive(Clone, Debug)]
pub struct ChildSnapshot {
    pub operation_id: OperationId,
    pub execution: ExecutionPhase,
    pub stop: Option<StopReport>,
    pub outcome: Option<ChildOutcomeSummary>,
    pub input_released: bool,
    pub settlement: Option<Arc<SettlementReport>>,
}
#[derive(Clone, Debug)]
pub struct ChildRelease {
    pub operation_id: OperationId,
    pub confirmed_at: Option<MonotonicTime>,
}

#[derive(Clone, Debug)]
pub struct ResourceCleanupReport {
    pub resource_id: ResourceId,
    pub evidence: CleanupEvidence,
}
#[derive(Clone, Debug)]
pub struct CleanupFailure {
    pub resource_id: ResourceId,
    pub stage: CleanupStage,
    pub summary: DiagnosticSummary,
    pub source: Option<Arc<dyn Error + Send + Sync>>,
}
impl fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cleanup {:?} failed for {:?}: {}",
            self.stage, self.resource_id, self.summary
        )
    }
}
impl Error for CleanupFailure {}

#[derive(Clone, Debug)]
pub struct EffectRecord {
    pub operation_id: OperationId,
    pub resource_id: Option<ResourceId>,
    pub fact: EffectFact,
    pub description: DiagnosticSummary,
}

#[derive(Clone, Debug)]
pub struct SettlementReport {
    pub operation_id: OperationId,
    pub status: SettlementStatus,
    pub released_count: usize,
    pub transferred_count: usize,
    pub recovery_count: usize,
    pub children: Arc<[ChildSettlement]>,
    pub children_total: usize,
    pub children_truncated: bool,
    pub cleanup_failures: Arc<[CleanupFailure]>,
    pub cleanup_failures_total: usize,
    pub cleanup_failures_truncated: bool,
    pub resource_reports: Arc<[ResourceCleanupReport]>,
    pub resource_reports_total: usize,
    pub resource_reports_truncated: bool,
    pub transfers: Arc<[TransferReceipt]>,
    pub transfers_total: usize,
    pub transfers_truncated: bool,
    pub recoveries: Arc<[RecoveryHandle]>,
    pub recoveries_total: usize,
    pub recoveries_truncated: bool,
    pub effects: Arc<[EffectRecord]>,
    pub effects_total: usize,
    pub effect_overflow: usize,
}
impl SettlementReport {
    pub fn all_released(&self) -> bool {
        self.status == SettlementStatus::Released
            && self.transferred_count == 0
            && self.recovery_count == 0
    }
}

#[derive(Clone, Debug)]
pub struct ResourceScopeSnapshot {
    pub owner_scope_ended: bool,
    pub user_references: usize,
    pub cleanup_started: usize,
    pub pending_resources: usize,
}
#[derive(Clone, Debug)]
pub struct ResourceScopeReport {
    pub resources: Arc<[ResourceCleanupReport]>,
    pub failures: Arc<[CleanupFailure]>,
    pub recoveries: Arc<[RecoveryHandle]>,
    pub transfers: Arc<[TransferReceipt]>,
    pub transfers_total: usize,
    pub transfers_truncated: bool,
}

pub use crate::v02::resource::recovery::RecoveryHandle;

/// Receipt fields remain private; only a receiving protocol may mint one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferReceipt {
    resource: ResourceId,
    source_scope: ResourceScopeId,
    target_scope: ResourceScopeId,
    nonce: u64,
}
impl TransferReceipt {
    #[allow(dead_code)]
    pub(crate) const fn new(
        resource: ResourceId,
        source_scope: ResourceScopeId,
        target_scope: ResourceScopeId,
        nonce: u64,
    ) -> Self {
        Self {
            resource,
            source_scope,
            target_scope,
            nonce,
        }
    }
    pub const fn resource(&self) -> ResourceId {
        self.resource
    }
    pub const fn source_scope(&self) -> ResourceScopeId {
        self.source_scope
    }
    pub const fn target_scope(&self) -> ResourceScopeId {
        self.target_scope
    }
    #[allow(dead_code)]
    pub(crate) const fn nonce(&self) -> u64 {
        self.nonce
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ResourceTransfer {
    pub members: Arc<[ResourceId]>,
    pub source_scope: ResourceScopeId,
    pub target_scope: ResourceScopeId,
    pub receipt: TransferReceipt,
}

#[derive(Debug, Eq, PartialEq)]
pub struct RecoveryTransferReceipt {
    recovery: RecoveryId,
    receiver: RecoveryStoreId,
    nonce: u64,
}
impl RecoveryTransferReceipt {
    #[allow(dead_code)]
    pub(crate) const fn new(recovery: RecoveryId, receiver: RecoveryStoreId, nonce: u64) -> Self {
        Self {
            recovery,
            receiver,
            nonce,
        }
    }
    pub const fn recovery(&self) -> RecoveryId {
        self.recovery
    }
    pub const fn receiver(&self) -> RecoveryStoreId {
        self.receiver
    }
}
#[derive(Debug, Eq, PartialEq)]
pub struct RecoveryTransfer {
    pub recovery: RecoveryId,
    pub receiver: RecoveryStoreId,
    pub receipt: RecoveryTransferReceipt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceCapacityError {
    ScopeEnded,
    Stopped,
    CapacityExceeded,
}
impl fmt::Display for ResourceCapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for ResourceCapacityError {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceScopeError {
    ScopeEnded,
    Stopped,
    InvalidOptions,
    CapacityExceeded,
}
impl fmt::Display for ResourceScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for ResourceScopeError {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupDependencyError {
    UnknownResource,
    WrongManager,
    WrongScope,
    SelfDependency,
    Cycle,
    CleanupStarted,
}
impl fmt::Display for CleanupDependencyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for CleanupDependencyError {}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceTransferError {
    UnknownResource,
    WrongManager,
    SameScope,
    TargetEnded,
    TargetStopped,
    CapacityExceeded,
    CleanupStarted,
    IncompleteDependencyGroup,
}
impl fmt::Display for ResourceTransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for ResourceTransferError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceManageErrorKind {
    CapacityExceeded,
    ScopeEnded,
    Stopped,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceLookupError {
    ScopeEnded,
    Stopped,
    WrongScope,
    StaleKey,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectRecordError {
    CapacityExceeded,
    ScopeEnded,
    Stopped,
}
impl fmt::Display for EffectRecordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for EffectRecordError {}

#[derive(Clone, Debug)]
pub enum ContextError {
    ClockUnavailable,
    ClockDomainMismatch,
    Stopped,
    BudgetRejected(super::protocol::BudgetError),
}
impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for ContextError {}

#[derive(Clone, Debug)]
pub enum RecoveryStoreCreateError {
    InvalidOptions {
        field: &'static str,
    },
    ExecutorUnavailable,
    ThreadSpawn {
        source: Arc<dyn Error + Send + Sync>,
    },
}
impl fmt::Display for RecoveryStoreCreateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions { field } => write!(f, "invalid option: {field}"),
            Self::ExecutorUnavailable => f.write_str("executor unavailable"),
            Self::ThreadSpawn { source } => write!(f, "worker thread spawn failed: {source}"),
        }
    }
}
impl Error for RecoveryStoreCreateError {}

/// Type and manager credential for reacquiring a resource from its owning scope.
pub struct ResourceKey<T: Send + Sync + 'static> {
    manager: ManagerId,
    resource: ResourceId,
    marker: std::marker::PhantomData<fn() -> T>,
}
impl<T: Send + Sync + 'static> Copy for ResourceKey<T> {}
impl<T: Send + Sync + 'static> Clone for ResourceKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: Send + Sync + 'static> fmt::Debug for ResourceKey<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceKey")
            .field("manager", &self.manager)
            .field("resource", &self.resource)
            .finish()
    }
}
impl<T: Send + Sync + 'static> ResourceKey<T> {
    #[allow(dead_code)]
    pub(crate) const fn new(manager: ManagerId, resource: ResourceId) -> Self {
        Self {
            manager,
            resource,
            marker: std::marker::PhantomData,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ManagerCreateError {
    InvalidOptions {
        field: &'static str,
    },
    ExecutorUnavailable,
    ThreadSpawn {
        source: Arc<dyn Error + Send + Sync>,
    },
}
impl fmt::Display for ManagerCreateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions { field } => write!(f, "invalid option: {field}"),
            Self::ExecutorUnavailable => f.write_str("executor unavailable"),
            Self::ThreadSpawn { source } => write!(f, "worker thread spawn failed: {source}"),
        }
    }
}
impl Error for ManagerCreateError {}

pub struct ResourceScopeOptions {
    pub capacity: usize,
}
impl fmt::Debug for ResourceScopeOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceScopeOptions")
            .field("capacity", &self.capacity)
            .finish()
    }
}

pub struct GroupOptions {
    pub capacity: usize,
}
impl fmt::Debug for GroupOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupOptions")
            .field("capacity", &self.capacity)
            .finish()
    }
}
pub struct ServiceResourceOptions {
    pub capacity: usize,
}
impl fmt::Debug for ServiceResourceOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceResourceOptions")
            .field("capacity", &self.capacity)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct ManagerSnapshot {
    pub active_operations: usize,
    pub queued_operations: usize,
    pub admission_waiters: usize,
    pub unconsumed_outcomes: usize,
    pub cleaning_resources: usize,
    pub recovery_groups: usize,
    pub unconfirmed_users: usize,
    pub diagnostic_overflow: usize,
    pub scope_open_resources: usize,
    pub waiting_for_users_resources: usize,
}

#[derive(Clone, Debug)]
pub struct GroupSnapshot {
    pub id: GroupId,
    pub accepting: bool,
    pub active_operations: usize,
    pub service_resource_scopes: usize,
}

#[derive(Clone, Debug)]
pub struct ShutdownSnapshot {
    pub active_operations: usize,
    pub queued_operations: usize,
    pub unconfirmed_users: usize,
    pub cleaning_resources: usize,
    pub recovery_groups: usize,
    pub service_resources: usize,
}
#[derive(Clone, Debug)]
pub struct ShutdownReport {
    pub snapshot: ShutdownSnapshot,
}
#[derive(Clone, Debug)]
pub enum ShutdownWait {
    Completed(ShutdownReport),
    Pending(ShutdownSnapshot),
}
#[derive(Clone, Debug)]
pub enum SettlementWait {
    Ready(Arc<SettlementReport>),
    Pending(OperationSnapshot),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryPhase {
    Pending,
    Running,
    Resolved,
    Transferred,
    Unconfirmed,
}
#[derive(Clone, Debug)]
pub struct RecoverySnapshot {
    pub id: RecoveryId,
    pub phase: RecoveryPhase,
    pub resources: Arc<[ResourceId]>,
    pub attempts: usize,
    pub last_failures: Arc<[CleanupFailure]>,
}
#[derive(Clone, Debug)]
pub struct RecoveryAttemptReport {
    pub recovery: RecoverySnapshot,
    pub failures: Arc<[CleanupFailure]>,
}
#[derive(Clone, Debug)]
pub struct RecoveryStoreSnapshot {
    pub accepting: bool,
    pub groups: usize,
    pub resources: usize,
    pub running_attempts: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryError {
    AlreadyRunning,
    AlreadyResolved,
    NoCapacity,
    ExecutorUnavailable,
}
impl fmt::Display for RetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for RetryError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryTransferError {
    Closed,
    CapacityExceeded,
    AlreadyTransferred,
    InvalidGroup,
}
impl fmt::Display for RecoveryTransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for RecoveryTransferError {}

#[cfg(test)]
mod tests {
    use super::{DiagnosticSummary, ManagerLimits, ManagerOptions, OperationOptions, ScopeLimits};
    use crate::v02::protocol::ClockDomainId;

    #[test]
    fn diagnostic_summary_truncates_only_at_utf8_boundaries() {
        let text = format!("{}é", "a".repeat(255));
        let summary = DiagnosticSummary::new(&text);

        assert_eq!(summary.as_str().len(), 255);
        assert!(summary.is_truncated());
        assert!(summary.as_str().is_char_boundary(summary.as_str().len()));
    }

    #[test]
    fn clock_domain_ids_are_unique() {
        let first = ClockDomainId::new_unique();
        let second = ClockDomainId::new_unique();
        assert_ne!(first, second);
    }

    #[test]
    fn default_limits_match_the_contract() {
        let manager = ManagerLimits::default();
        assert_eq!(
            (manager.active_operations, manager.queued_operations),
            (4, 64)
        );
        assert_eq!(
            (manager.admission_waiters, manager.unconsumed_outcomes),
            (64, 64)
        );
        assert_eq!(
            (manager.cleanup_concurrency, manager.diagnostic_events),
            (4, 256)
        );
        assert_eq!(
            ScopeLimits::default(),
            ScopeLimits {
                child_operations: 64,
                resources: 128,
                effect_records: 64
            }
        );
        assert_eq!(ManagerOptions::default().async_executor.task_capacity, 68);
        assert_eq!(OperationOptions::default().limits.resources, 128);
    }

    #[test]
    fn submit_error_does_not_require_the_body_to_implement_debug() {
        struct NotDebug;
        fn assert_error<T: std::error::Error>() {}
        assert_error::<super::SubmitError<NotDebug>>();
        assert_error::<super::RunError<NotDebug>>();
    }
}

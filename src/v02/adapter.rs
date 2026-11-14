//! Admission and lifecycle ports for externally executed child operations.

use crate::v02::protocol::OperationControl;
use crate::v02::report::{
    ChildOutcomeSummary, ChildRelease, ChildSnapshot, OperationId, OperationOptions,
    SettlementReport, StopReason, StopReport,
};
use crate::v02::resource::ResourceRef;
use crate::v02::scope::OperationScope;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

#[cfg(test)]
#[path = "adapter/tests.rs"]
mod tests;

/// Why an external child admission failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdapterErrorKind {
    Stopped,
    ScopeEnded,
    CapacityExceeded,
    AlreadyAttached,
    AlreadyCommitted,
}

/// An admission error that returns ownership of the rejected payload.
pub(crate) struct AdapterError<P> {
    pub(crate) reason: AdapterErrorKind,
    pub(crate) payload: P,
}

impl<P> std::fmt::Debug for AdapterError<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

/// An owning reservation for child admission and its capacity token.
pub(crate) struct AdapterReservation {
    admission: Option<Box<dyn Any + Send>>,
    activate: Option<Box<dyn FnOnce(Box<dyn Any + Send>) + Send>>,
    abandon: Option<Box<dyn FnOnce() + Send>>,
}

impl AdapterReservation {
    pub(crate) fn new(
        admission: Box<dyn Any + Send>,
        activate: impl FnOnce(Box<dyn Any + Send>) + Send + 'static,
        abandon: impl FnOnce() + Send + 'static,
    ) -> Self {
        Self {
            admission: Some(admission),
            activate: Some(Box::new(activate)),
            abandon: Some(Box::new(abandon)),
        }
    }

    pub(crate) fn attach<P: Send + 'static>(
        mut self,
        control: Arc<dyn OperationControl>,
        payload: P,
    ) -> Result<AttachedChild, AdapterError<(P, Self)>> {
        if self.admission.is_none() {
            return Err(AdapterError {
                reason: AdapterErrorKind::AlreadyAttached,
                payload: (payload, self),
            });
        }
        Ok(AttachedChild {
            admission: self.admission.take(),
            control,
            payload: Some(Box::new(payload)),
            lifecycle: Arc::new(Mutex::new(Lifecycle {
                phase: ChildPhase::Attached,
                stop_requested: false,
            })),
            activate: self.activate.take(),
            abandon: self.abandon.take(),
        })
    }
}

/// Attached but not yet committed child. Dropping it abandons admission once.
pub(crate) struct AttachedChild {
    admission: Option<Box<dyn Any + Send>>,
    control: Arc<dyn OperationControl>,
    payload: Option<Box<dyn Any + Send>>,
    lifecycle: Arc<Mutex<Lifecycle>>,
    activate: Option<Box<dyn FnOnce(Box<dyn Any + Send>) + Send>>,
    abandon: Option<Box<dyn FnOnce() + Send>>,
}

impl AttachedChild {
    pub(crate) fn request_stop(&self, reason: StopReason) {
        let accepted = {
            let mut lifecycle = lock(&self.lifecycle);
            if lifecycle.phase == ChildPhase::Abandoned || lifecycle.stop_requested {
                false
            } else {
                lifecycle.stop_requested = true;
                true
            }
        };
        if accepted {
            self.control.request_stop(reason);
        }
    }

    pub(crate) fn commit(mut self) -> Result<ChildStartPermit, AdapterError<Self>> {
        {
            let mut lifecycle = lock(&self.lifecycle);
            if lifecycle.phase != ChildPhase::Attached {
                drop(lifecycle);
                return Err(AdapterError {
                    reason: AdapterErrorKind::AlreadyCommitted,
                    payload: self,
                });
            }
            if lifecycle.stop_requested || self.control.snapshot().stop.is_some() {
                lifecycle.phase = ChildPhase::Abandoned;
                drop(lifecycle);
                if let Some(abandon) = self.abandon.take() {
                    abandon();
                }
                drop(self.admission.take());
                return Err(AdapterError {
                    reason: AdapterErrorKind::Stopped,
                    payload: self,
                });
            }
            lifecycle.phase = ChildPhase::Committed;
        }
        Ok(ChildStartPermit {
            lifecycle: Arc::clone(&self.lifecycle),
            activate: self.activate.take(),
            abandon: self.abandon.take(),
            admission: self.admission.take(),
            payload: self.payload.take(),
            control: Arc::clone(&self.control),
        })
    }

    fn abandon_once(&mut self) {
        let should_abandon = {
            let mut phase = lock(&self.lifecycle);
            if matches!(
                phase.phase,
                ChildPhase::Abandoned | ChildPhase::Activated | ChildPhase::Committed
            ) {
                false
            } else {
                phase.phase = ChildPhase::Abandoned;
                true
            }
        };
        if should_abandon {
            if let Some(abandon) = self.abandon.take() {
                abandon();
            }
            drop(self.admission.take());
        }
    }
}

impl Drop for AttachedChild {
    fn drop(&mut self) {
        self.abandon_once();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChildPhase {
    Attached,
    Committed,
    Activated,
    Abandoned,
}

struct Lifecycle {
    phase: ChildPhase,
    stop_requested: bool,
}

/// One-shot permission to start an already committed external child.
pub(crate) struct ChildStartPermit {
    lifecycle: Arc<Mutex<Lifecycle>>,
    activate: Option<Box<dyn FnOnce(Box<dyn Any + Send>) + Send>>,
    abandon: Option<Box<dyn FnOnce() + Send>>,
    admission: Option<Box<dyn Any + Send>>,
    payload: Option<Box<dyn Any + Send>>,
    control: Arc<dyn OperationControl>,
}

impl std::fmt::Debug for ChildStartPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildStartPermit").finish_non_exhaustive()
    }
}

impl ChildStartPermit {
    pub(crate) fn activate(mut self) -> Result<(), AdapterError<Self>> {
        let stopped = {
            let mut lifecycle = lock(&self.lifecycle);
            if lifecycle.phase != ChildPhase::Committed {
                drop(lifecycle);
                return Err(AdapterError {
                    reason: AdapterErrorKind::AlreadyCommitted,
                    payload: self,
                });
            }
            if lifecycle.stop_requested || self.control.snapshot().stop.is_some() {
                lifecycle.phase = ChildPhase::Abandoned;
                true
            } else {
                lifecycle.phase = ChildPhase::Activated;
                false
            }
        };
        if stopped {
            if let Some(abandon) = self.abandon.take() {
                abandon();
            }
            drop(self.admission.take());
            return Err(AdapterError {
                reason: AdapterErrorKind::Stopped,
                payload: self,
            });
        }
        if let Some(activate) = self.activate.take() {
            activate(
                self.payload
                    .take()
                    .expect("committed child owns its payload"),
            );
        }
        drop(self.admission.take());
        self.abandon.take();
        Ok(())
    }

    pub(crate) fn abandon(mut self) {
        self.abandon_once();
    }

    fn abandon_once(&mut self) {
        let should_abandon = {
            let mut phase = lock(&self.lifecycle);
            if matches!(phase.phase, ChildPhase::Abandoned | ChildPhase::Activated) {
                false
            } else {
                phase.phase = ChildPhase::Abandoned;
                true
            }
        };
        if should_abandon {
            if let Some(abandon) = self.abandon.take() {
                abandon();
            }
            drop(self.admission.take());
        }
    }
}

impl Drop for ChildStartPermit {
    fn drop(&mut self) {
        self.abandon_once();
    }
}

/// Adapter boundary for reserving, attaching, and committing external children.
pub(crate) trait ChildAdapter: Send + Sync {
    fn reserve_child(
        &self,
        parent: &OperationScope,
        options: &OperationOptions,
    ) -> Result<AdapterReservation, AdapterError<()>>;

    fn attach<P: Send + 'static>(
        &self,
        reservation: AdapterReservation,
        control: Arc<dyn OperationControl>,
        payload: P,
    ) -> Result<AttachedChild, AdapterError<(P, AdapterReservation)>>;

    fn commit(&self, child: AttachedChild)
    -> Result<ChildStartPermit, AdapterError<AttachedChild>>;
}

/// External work together with the resource reference that keeps its input alive.
pub(crate) struct ExternalTask<T: Send + Sync + 'static> {
    state: Arc<Mutex<TaskState<T>>>,
}

struct TaskState<T: Send + Sync + 'static> {
    operation_id: OperationId,
    execution: crate::v02::report::ExecutionPhase,
    stop: Option<StopReport>,
    outcome: Option<ChildOutcomeSummary>,
    release: Option<ChildRelease>,
    settlement: Option<Arc<SettlementReport>>,
    input: Option<ResourceRef<T>>,
    runtime_lost: bool,
    outcome_waiters: Vec<Waker>,
    release_waiters: Vec<Waker>,
    settlement_waiters: Vec<Waker>,
}

type LostInputs = HashMap<OperationId, Box<dyn Any + Send + Sync>>;
static LOST_INPUTS: OnceLock<Mutex<LostInputs>> = OnceLock::new();

impl<T: Send + Sync + 'static> Drop for TaskState<T> {
    fn drop(&mut self) {
        if self.runtime_lost {
            if let Some(input) = self.input.take() {
                lock(LOST_INPUTS.get_or_init(|| Mutex::new(HashMap::new())))
                    .insert(self.operation_id, Box::new(input));
            }
        }
    }
}

/// Releases a retained lost-runtime input after independent release evidence arrives.
///
/// The reference is removed under the registry lock and dropped after unlocking so
/// normal resource cleanup can continue without running user destructors in the lock.
pub(crate) fn recover_lost_input(operation_id: OperationId) -> bool {
    let input = LOST_INPUTS
        .get()
        .and_then(|inputs| lock(inputs).remove(&operation_id));
    let found = input.is_some();
    drop(input);
    found
}

impl<T: Send + Sync + 'static> ExternalTask<T> {
    pub(crate) fn new(
        operation_id: OperationId,
        input: ResourceRef<T>,
    ) -> (Self, Arc<ExternalTaskControl<T>>) {
        let state = Arc::new(Mutex::new(TaskState {
            operation_id,
            execution: crate::v02::report::ExecutionPhase::Queued,
            stop: None,
            outcome: None,
            release: None,
            settlement: None,
            input: Some(input),
            runtime_lost: false,
            outcome_waiters: Vec::new(),
            release_waiters: Vec::new(),
            settlement_waiters: Vec::new(),
        }));
        (
            Self {
                state: Arc::clone(&state),
            },
            Arc::new(ExternalTaskControl { state }),
        )
    }

    pub(crate) fn publish_outcome(&self, outcome: ChildOutcomeSummary) {
        let waiters = {
            let mut state = lock(&self.state);
            assert_eq!(outcome.operation_id, state.operation_id);
            if state.outcome.is_some() {
                return;
            }
            state.execution = crate::v02::report::ExecutionPhase::Finished;
            state.outcome = Some(outcome);
            std::mem::take(&mut state.outcome_waiters)
        };
        wake_all(waiters);
    }

    pub(crate) fn confirm_input_release(
        &self,
        release: ChildRelease,
    ) -> Result<(), AdapterError<ChildRelease>> {
        let (input, waiters) = {
            let mut state = lock(&self.state);
            if release.operation_id != state.operation_id || state.release.is_some() {
                return Err(AdapterError {
                    reason: AdapterErrorKind::AlreadyCommitted,
                    payload: release,
                });
            }
            state.release = Some(release);
            (
                state.input.take(),
                std::mem::take(&mut state.release_waiters),
            )
        };
        drop(input);
        wake_all(waiters);
        Ok(())
    }

    pub(crate) fn publish_settlement(
        &self,
        settlement: SettlementReport,
    ) -> Result<(), AdapterError<SettlementReport>> {
        let waiters = {
            let mut state = lock(&self.state);
            if settlement.operation_id != state.operation_id || state.settlement.is_some() {
                return Err(AdapterError {
                    reason: AdapterErrorKind::AlreadyCommitted,
                    payload: settlement,
                });
            }
            state.settlement = Some(Arc::new(settlement));
            std::mem::take(&mut state.settlement_waiters)
        };
        wake_all(waiters);
        Ok(())
    }

    pub(crate) fn mark_runtime_lost(&self) {
        let waiters = {
            let mut state = lock(&self.state);
            state.runtime_lost = true;
            std::mem::take(&mut state.settlement_waiters)
        };
        wake_all(waiters);
    }

    pub(crate) fn is_unconfirmed(&self) -> bool {
        let state = lock(&self.state);
        state.runtime_lost && state.release.is_none()
    }

    pub(crate) fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.outcome_waiters,
            |state| state.outcome.clone(),
        )
    }

    pub(crate) fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.release_waiters,
            |state| state.release.clone(),
        )
    }
}

impl<T: Send + Sync + 'static> Clone for ExternalTask<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

/// Cloneable observation port independent of the task's ownership.
pub(crate) struct ExternalTaskControl<T: Send + Sync + 'static> {
    state: Arc<Mutex<TaskState<T>>>,
}

impl<T: Send + Sync + 'static> OperationControl for ExternalTaskControl<T> {
    fn request_stop(&self, reason: StopReason) {
        let waiters = {
            let mut state = lock(&self.state);
            if state.stop.is_some()
                || state.execution == crate::v02::report::ExecutionPhase::Finished
            {
                return;
            }
            state.stop = Some(StopReport {
                reason,
                accepted_at: None,
            });
            Vec::new()
        };
        wake_all(waiters);
    }

    fn snapshot(&self) -> ChildSnapshot {
        let state = lock(&self.state);
        ChildSnapshot {
            operation_id: state.operation_id,
            execution: state.execution,
            stop: state.stop.clone(),
            outcome: state.outcome.clone(),
            input_released: state.release.is_some(),
            settlement: state.settlement.clone(),
        }
    }

    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.outcome_waiters,
            |state| state.outcome.clone(),
        )
    }

    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.release_waiters,
            |state| state.release.clone(),
        )
    }

    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.settlement_waiters,
            |state| state.settlement.clone(),
        )
    }
}

fn poll_fact<T, V: Clone>(
    state: &Arc<Mutex<TaskState<T>>>,
    cx: &mut Context<'_>,
    waiters: impl for<'a> Fn(&'a mut TaskState<T>) -> &'a mut Vec<Waker>,
    value: impl Fn(&TaskState<T>) -> Option<V>,
) -> Poll<V>
where
    T: Send + Sync + 'static,
{
    if let Some(value) = value(&lock(state)) {
        return Poll::Ready(value);
    }
    let candidate = cx.waker().clone();
    let (result, retired) = {
        let mut state = lock(state);
        if let Some(value) = value(&state) {
            (Poll::Ready(value), None)
        } else {
            let waiters = waiters(&mut state);
            if let Some(index) = waiters.iter().position(|old| old.will_wake(&candidate)) {
                (
                    Poll::Pending,
                    Some(std::mem::replace(&mut waiters[index], candidate)),
                )
            } else {
                waiters.push(candidate);
                (Poll::Pending, None)
            }
        }
    };
    drop(retired);
    result
}

fn wake_all(waiters: Vec<Waker>) {
    for waker in waiters {
        waker.wake();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

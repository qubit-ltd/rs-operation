//! Public managed operation handles and supervisor submission.

use crate::context::{ContextError, Deadline, OperationContext};
use crate::driver::{BlockingExecutor, ExecutorError, PortableDriverError};
use crate::protocol::CleanupContext;
use crate::report::{
    AdmissionError, ChildFailureSummary, CleanupFailureSummary, CleanupOutcome, ClockDomainId,
    DiagnosticSummary, MonotonicTime, ObservationError, OperationId, OperationOutcome,
    PanicSummary, ResourcePhase, SettlementReport, StartError, StopReason, StopReport,
};
use crate::resource::{CleanupObservation, CleanupScheduler};
use crate::scope::{
    JoinFacts, OperationScope, ScopeOptions, poll_scope_business, poll_scope_releases,
};
use crate::supervisor::{OperationSupervisor, ResultLease};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct Shared<T, E> {
    state: Mutex<State<T, E>>,
    changed: Condvar,
    context: OperationContext,
}
struct State<T, E> {
    result: Option<OperationOutcome<T, E>>,
    result_taken: bool,
    result_discarded: bool,
    released: Option<SettlementReport>,
    result_waiters: Vec<Waker>,
    release_waiters: Vec<Waker>,
    result_lease: Option<ResultLease>,
    completion_guards: Vec<Box<dyn Send>>,
}

/// Unique typed result consumer for one accepted operation.
///
/// ```compile_fail
/// use qubit_operation::operation::OperationHandle;
/// fn assert_clone<T: Clone>() {}
/// assert_clone::<OperationHandle<(), ()>>();
/// ```
pub struct OperationHandle<T, E> {
    shared: Arc<Shared<T, E>>,
    context: OperationContext,
    detached: bool,
}

impl<T, E> OperationHandle<T, E> {
    /// Request cooperative cancellation. This never runs domain code inline.
    pub fn request_cancel(&self, reason: StopReason) {
        self.context.request_stop(reason);
    }

    pub(crate) fn cancellation_context(&self) -> OperationContext {
        self.context.clone()
    }

    pub(crate) fn hold_until_released(&self, guard: Box<dyn Send>) {
        let mut state = lock(&self.shared.state);
        if state.released.is_some() {
            drop(state);
            drop(guard);
        } else {
            state.completion_guards.push(guard);
        }
    }
    /// Create a cloneable observer that cannot consume the typed result.
    pub fn observer(&self) -> OperationObserver<T, E> {
        OperationObserver {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Consume the typed outcome exactly once.
    pub async fn result(&mut self) -> Result<OperationOutcome<T, E>, ObservationError> {
        std::future::poll_fn(|cx| poll_result(&self.shared, cx)).await
    }

    /// Consume the typed outcome using a blocking wait.
    pub fn result_blocking(&mut self) -> Result<OperationOutcome<T, E>, ObservationError> {
        if crate::driver::is_portable_worker_thread() || crate::driver::is_thread_executor_worker()
        {
            return Err(ObservationError::WouldDeadlock);
        }
        let mut state = lock(&self.shared.state);
        loop {
            if state.result_taken {
                return Err(ObservationError::AlreadyTaken);
            }
            if state.result_discarded {
                return Err(ObservationError::ResultDiscarded);
            }
            if state.result.is_some() {
                state.result_taken = true;
                let result = state.result.take().expect("result checked");
                let lease = state.result_lease.take();
                drop(state);
                if let Some(lease) = lease {
                    lease.observe();
                }
                return Ok(result);
            }
            state = self
                .shared
                .changed
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Wait repeatedly for an immutable resource settlement snapshot.
    pub async fn released(&self) -> SettlementReport {
        std::future::poll_fn(|cx| poll_released(&self.shared, cx)).await
    }

    /// Wait for settlement using a blocking wait.
    pub fn released_blocking(&self) -> Result<SettlementReport, ObservationError> {
        if crate::driver::is_portable_worker_thread() || crate::driver::is_thread_executor_worker()
        {
            return Err(ObservationError::WouldDeadlock);
        }
        let mut state = lock(&self.shared.state);
        loop {
            if let Some(report) = &state.released {
                return Ok(report.clone());
            }
            state = self
                .shared
                .changed
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Drop typed result delivery while keeping operation supervision active.
    pub fn detach(mut self) -> OperationObserver<T, E> {
        self.detached = true;
        self.discard_result();
        self.observer()
    }

    fn discard_result(&mut self) {
        let (value, lease) = {
            let mut state = lock(&self.shared.state);
            state.result_discarded = true;
            (state.result.take(), state.result_lease.take())
        };
        drop(value);
        drop(lease);
        self.shared.changed.notify_all();
    }
}

impl<T, E> Drop for OperationHandle<T, E> {
    fn drop(&mut self) {
        if !self.detached {
            self.context.request_stop(StopReason::UserCancelled);
            self.discard_result();
        }
    }
}

/// Cloneable observation access that never consumes the typed result.
pub struct OperationObserver<T, E> {
    shared: Arc<Shared<T, E>>,
}
impl<T, E> Clone for OperationObserver<T, E> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}
impl<T, E> OperationObserver<T, E> {
    /// Observe a stable settlement report once available.
    pub async fn released(&self) -> SettlementReport {
        std::future::poll_fn(|cx| poll_released(&self.shared, cx)).await
    }
    /// Wait for a stable settlement snapshot using a blocking wait.
    pub fn released_blocking(&self) -> Result<SettlementReport, ObservationError> {
        if crate::driver::is_portable_worker_thread() || crate::driver::is_thread_executor_worker()
        {
            return Err(ObservationError::WouldDeadlock);
        }
        let mut state = lock(&self.shared.state);
        loop {
            if let Some(report) = &state.released {
                return Ok(report.clone());
            }
            state = self
                .shared
                .changed
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
    }
    /// Return the latest resource state without waiting.
    pub fn resource_phase(&self) -> ResourcePhase {
        lock(&self.shared.state)
            .released
            .as_ref()
            .map_or(ResourcePhase::Active, |r| r.resource_phase)
    }

    /// Observe the current state until a caller-owned deadline without stopping the operation.
    pub async fn observe_until(
        &self,
        deadline: Deadline,
    ) -> Result<ObserveUntil<T, E>, ObservationError> {
        let mut timer = self
            .shared
            .context
            .signal_at(deadline)
            .map_err(|error| match error {
                ContextError::ClockUnavailable => ObservationError::ClockUnavailable,
                ContextError::ClockDomainMismatch => ObservationError::ClockDomainMismatch,
                _ => ObservationError::InvalidObserver,
            })?;
        let snapshot = std::future::poll_fn(|cx| {
            let mut state = lock(&self.shared.state);
            if let Some(report) = &state.released {
                return Poll::Ready(observer_snapshot(&state, Some(report.clone())));
            }
            if !state
                .release_waiters
                .iter()
                .any(|w| w.will_wake(cx.waker()))
            {
                state.release_waiters.push(cx.waker().clone());
            }
            if Pin::new(&mut timer).poll(cx).is_ready() {
                Poll::Ready(observer_snapshot(&state, None))
            } else {
                Poll::Pending
            }
        })
        .await;
        Ok(ObserveUntil {
            snapshot,
            observer: self.clone(),
        })
    }
}

/// A deadline-bounded snapshot and an observer that remains live afterward.
pub struct ObserveUntil<T, E> {
    pub snapshot: ObserverSnapshot,
    pub observer: OperationObserver<T, E>,
}

/// Non-consuming operation state available at an observation deadline.
#[derive(Clone, Debug)]
pub struct ObserverSnapshot {
    pub resource_phase: ResourcePhase,
    pub business_result_ready: bool,
    pub result_taken: bool,
    pub result_discarded: bool,
    pub settlement: Option<SettlementReport>,
}

fn observer_snapshot<T, E>(
    state: &State<T, E>,
    settlement: Option<SettlementReport>,
) -> ObserverSnapshot {
    ObserverSnapshot {
        resource_phase: settlement
            .as_ref()
            .map_or(ResourcePhase::Active, |report| report.resource_phase),
        business_result_ready: state.result.is_some()
            || state.result_taken
            || state.result_discarded,
        result_taken: state.result_taken,
        result_discarded: state.result_discarded,
        settlement,
    }
}

fn poll_result<T, E>(
    shared: &Arc<Shared<T, E>>,
    cx: &mut Context<'_>,
) -> Poll<Result<OperationOutcome<T, E>, ObservationError>> {
    let (result, lease, discarded, already) = {
        let mut state = lock(&shared.state);
        if state.result_taken {
            (None, None, false, true)
        } else if state.result_discarded {
            (None, None, true, false)
        } else if let Some(result) = state.result.take() {
            state.result_taken = true;
            (Some(result), state.result_lease.take(), false, false)
        } else {
            if !state.result_waiters.iter().any(|w| w.will_wake(cx.waker())) {
                state.result_waiters.push(cx.waker().clone());
            }
            (None, None, false, false)
        }
    };
    if already {
        return Poll::Ready(Err(ObservationError::AlreadyTaken));
    }
    if discarded {
        return Poll::Ready(Err(ObservationError::ResultDiscarded));
    }
    if let Some(result) = result {
        if let Some(lease) = lease {
            lease.observe();
        }
        Poll::Ready(Ok(result))
    } else {
        Poll::Pending
    }
}
fn poll_released<T, E>(shared: &Arc<Shared<T, E>>, cx: &mut Context<'_>) -> Poll<SettlementReport> {
    let mut state = lock(&shared.state);
    if let Some(report) = &state.released {
        return Poll::Ready(report.clone());
    }
    if !state
        .release_waiters
        .iter()
        .any(|w| w.will_wake(cx.waker()))
    {
        state.release_waiters.push(cx.waker().clone());
    }
    Poll::Pending
}

impl OperationSupervisor {
    /// Admit and immediately submit an owned asynchronous body.
    pub fn start<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let context = OperationContext::new(options.context.clone());
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let retained = Arc::clone(&body_cell);
        let mut admission = match self.try_admit_with_context(
            move || lock(&retained).take().expect("body invoked once"),
            &context,
        ) {
            Ok(admission) => admission,
            Err(error) => {
                return Err(StartError::new(
                    error.reason(),
                    lock(&body_cell).take().expect("unstarted body retained"),
                ));
            }
        };
        let admitted = admission
            .transfer()
            .expect("fresh admission transfers once");
        let (_, task_lease, result_lease) = admitted.into_parts();
        self.start_admitted(options, context, body_cell, task_lease, result_lease)
    }

    /// Wait for bounded root admission before submitting the body.
    pub async fn start_wait<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let context = OperationContext::new(options.context.clone());
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let retained = Arc::clone(&body_cell);
        let admission_body = move || lock(&retained).take().expect("body invoked once");
        let waiter = self
            .start_wait_body_with_context(admission_body, Some(context.clone()))
            .map_err(|error| {
                let reason = error.reason();
                StartError::new(reason, error.into_body()())
            })?;
        let mut admission = waiter.await.map_err(|error| {
            let reason = error.reason();
            StartError::new(reason, error.into_body()())
        })?;
        let admitted = admission
            .transfer()
            .expect("fresh admission transfers once");
        let (_, task_lease, result_lease) = admitted.into_parts();
        self.start_admitted(options, context, body_cell, task_lease, result_lease)
    }

    /// Block the calling thread until bounded root admission is available.
    ///
    /// Calls from a managed worker are rejected with `WouldDeadlock`.
    pub fn start_wait_blocking<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        if crate::driver::is_portable_worker_thread() || crate::driver::is_thread_executor_worker()
        {
            return Err(StartError::new(AdmissionError::WouldDeadlock, body));
        }
        block_on_start_wait(self.start_wait(options, body))
    }

    /// Immediately admit a synchronous body and run it on the caller-supplied bounded executor.
    ///
    /// If root admission would queue, this rolls the reservation back and returns `Busy` with
    /// the original body. Use [`Self::start_wait`] or [`Self::start_wait_blocking`] to wait.
    pub fn start_blocking<X, F, T, E>(
        &self,
        executor: &X,
        options: ScopeOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        X: BlockingExecutor,
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        if crate::driver::is_portable_worker_thread() || executor.is_current_thread_worker() {
            return Err(StartError::new(AdmissionError::WouldDeadlock, body));
        }
        let context = OperationContext::new(options.context.clone());
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let retained = Arc::clone(&body_cell);
        let mut admission = match self.try_admit_with_context(
            move || lock(&retained).take().expect("blocking body invoked once"),
            &context,
        ) {
            Ok(admission) => admission,
            Err(error) => {
                return Err(StartError::new(
                    error.reason(),
                    lock(&body_cell).take().expect("rejected body retained"),
                ));
            }
        };
        if admission.is_queued() {
            drop(admission);
            return Err(StartError::new(
                AdmissionError::Busy,
                lock(&body_cell)
                    .take()
                    .expect("queued blocking body retained"),
            ));
        }
        let admitted = admission
            .transfer()
            .expect("fresh admission transfers once");
        let (_, task_lease, result_lease) = admitted.into_parts();
        let id = OperationId::new(NEXT_ID.fetch_add(1, Ordering::Relaxed) as u128);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                result: None,
                result_taken: false,
                result_discarded: false,
                released: None,
                result_waiters: Vec::new(),
                release_waiters: Vec::new(),
                result_lease: Some(result_lease),
                completion_guards: Vec::new(),
            }),
            changed: Condvar::new(),
            context: context.clone(),
        });
        let scope =
            OperationScope::new(id, options.limits, options.policy, self.cleanup_scheduler());
        let scope_for_job = scope.clone();
        let completion = Arc::new(Mutex::new(BlockingCompletion {
            result: None,
            aborted: false,
            waker: None,
        }));
        let manager_completion = Arc::clone(&completion);
        let manager_scope = scope.clone();
        let manager_shared = Arc::clone(&shared);
        let manager_supervisor = self.internal_handle();
        let manager_context = context.clone();
        let manager_stop = manager_context.stop_signal();
        let lease = Arc::new(Mutex::new(Some(task_lease)));
        let worker_lease = Arc::clone(&lease);
        let manager = async move {
            manager_supervisor.track_scope_recovery(&manager_scope);
            let result = Cancellable {
                future: Box::pin(std::future::poll_fn(|cx| {
                    let mut state = lock(&manager_completion);
                    if let Some(result) = state.result.take() {
                        Poll::Ready(Some(result))
                    } else if state.aborted {
                        Poll::Ready(None)
                    } else {
                        state.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })),
                stop: manager_stop,
                scope: manager_scope.clone(),
                stopped: false,
                stop_reason: None,
            }
            .await;
            let (Some(result), stopped) = result else {
                return;
            };
            manager_scope.close_body();
            manager_scope.close_body_resources();
            if matches!(result, BlockingBodyResult::Panicked) {
                manager_scope.request_cancel(StopReason::ParentStopped);
            }
            let mut facts = JoinFacts::default();
            std::future::poll_fn(|cx| poll_scope_business(&manager_scope, cx, &mut facts)).await;
            let outcome = if let Some(reason) = stopped {
                match result {
                    BlockingBodyResult::Returned(result) => drop(result),
                    BlockingBodyResult::Panicked => {}
                }
                OperationOutcome::Stopped(StopReport {
                    reason,
                    accepted_at: None,
                })
            } else {
                match result {
                    BlockingBodyResult::Panicked => OperationOutcome::Panicked(PanicSummary {
                        operation_id: id,
                        phase: DiagnosticSummary::new("blocking body"),
                    }),
                    BlockingBodyResult::Returned(Ok(value)) if facts.failed_required.is_some() => {
                        drop(value);
                        let child = facts.failed_required.as_ref().expect("checked");
                        OperationOutcome::ChildFailed(ChildFailureSummary {
                            operation_id: child.operation_id,
                            summary: child.summary.clone(),
                        })
                    }
                    BlockingBodyResult::Returned(Ok(value)) => OperationOutcome::Succeeded(value),
                    BlockingBodyResult::Returned(Err(error)) => OperationOutcome::Failed(error),
                }
            };
            publish_result(&manager_shared, outcome);
            std::future::poll_fn(|cx| poll_scope_releases(&manager_scope, cx, &mut facts)).await;
            let settlement = cleanup_scope_tree(&manager_scope).await;
            let resource_phase = settlement.phase;
            let unconfirmed_count = settlement.unconfirmed;
            publish_released(
                &manager_shared,
                SettlementReport {
                    operation_id: id,
                    resource_phase,
                    all_released: resource_phase == ResourcePhase::Released
                        || (resource_phase == ResourcePhase::RecoveryRequired
                            && unconfirmed_count == 0),
                    children: facts.settlements.into(),
                    cleanup_failures: settlement.failures.into(),
                    transfers: settlement.transfers.into(),
                    unconfirmed_count,
                },
            );
        };
        if let Err(error) = self.spawn_managed(async move {
            manager.await;
            if let Some(lease) = lock(&worker_lease).take() {
                lease.complete();
            }
        }) {
            let reason = match error {
                PortableDriverError::Capacity => AdmissionError::Busy,
                _ => AdmissionError::DriverUnavailable,
            };
            drop(lock(&lease).take());
            return Err(StartError::new(
                reason,
                lock(&body_cell).take().expect("unstarted body retained"),
            ));
        }
        let job_completion = Arc::clone(&completion);
        let job_body = Arc::clone(&body_cell);
        if let Err(rejected) = executor.execute(move || {
            let body = lock(&job_body)
                .take()
                .expect("accepted blocking body invoked once");
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(scope_for_job)))
                    .map_or(BlockingBodyResult::Panicked, BlockingBodyResult::Returned);
            let waker = {
                let mut state = lock(&job_completion);
                state.result = Some(result);
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }) {
            let reason = if rejected.error == ExecutorError::Capacity {
                AdmissionError::Busy
            } else {
                AdmissionError::DriverUnavailable
            };
            drop(rejected);
            let waker = {
                let mut state = lock(&completion);
                state.aborted = true;
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
            return Err(StartError::new(
                reason,
                lock(&body_cell)
                    .take()
                    .expect("rejected blocking body retained"),
            ));
        }
        Ok(OperationHandle {
            shared,
            context,
            detached: false,
        })
    }

    fn start_admitted<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        context: OperationContext,
        body_cell: Arc<Mutex<Option<F>>>,
        task_lease: crate::supervisor::TaskLease,
        result_lease: ResultLease,
    ) -> Result<OperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let id = OperationId::new(NEXT_ID.fetch_add(1, Ordering::Relaxed) as u128);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                result: None,
                result_taken: false,
                result_discarded: false,
                released: None,
                result_waiters: Vec::new(),
                release_waiters: Vec::new(),
                result_lease: Some(result_lease),
                completion_guards: Vec::new(),
            }),
            changed: Condvar::new(),
            context: context.clone(),
        });
        let worker_shared = Arc::clone(&shared);
        let gate_shared = Arc::clone(&shared);
        let worker_context = context.clone();
        let body_context = worker_context.clone();
        let mut admission_stop = worker_context.stop_signal();
        let worker_body = Arc::clone(&body_cell);
        let cleanup_scheduler = self.cleanup_scheduler();
        let manager_supervisor = self.internal_handle();
        let future = async move {
            let scope = OperationScope::new(id, options.limits, options.policy, cleanup_scheduler);
            manager_supervisor.track_scope_recovery(&scope);
            let f = lock(&worker_body).take().expect("body invoked once");
            let body_future = f(scope.clone());
            let (result, stopped) = Cancellable {
                future: Box::pin(body_future),
                stop: body_context.stop_signal(),
                scope: scope.clone(),
                stopped: false,
                stop_reason: None,
            }
            .await;
            scope.close_body();
            scope.close_body_resources();
            let mut facts = JoinFacts::default();
            std::future::poll_fn(|cx| poll_scope_business(&scope, cx, &mut facts)).await;
            let outcome = if let Some(reason) = stopped {
                drop(result);
                OperationOutcome::Stopped(StopReport {
                    reason,
                    accepted_at: None,
                })
            } else {
                match result {
                    Ok(value) if facts.failed_required.is_some() => {
                        drop(value);
                        let child = facts.failed_required.as_ref().expect("checked");
                        OperationOutcome::ChildFailed(ChildFailureSummary {
                            operation_id: child.operation_id,
                            summary: child.summary.clone(),
                        })
                    }
                    Ok(value) => OperationOutcome::Succeeded(value),
                    Err(error) => OperationOutcome::Failed(error),
                }
            };
            publish_result(&worker_shared, outcome);
            std::future::poll_fn(|cx| poll_scope_releases(&scope, cx, &mut facts)).await;
            let settlement = cleanup_scope_tree(&scope).await;
            let resource_phase = settlement.phase;
            let failures = settlement.failures;
            let transfers = settlement.transfers;
            let unconfirmed_count = settlement.unconfirmed;
            let report = SettlementReport {
                operation_id: id,
                resource_phase,
                all_released: resource_phase == ResourcePhase::Released
                    || (resource_phase == ResourcePhase::RecoveryRequired
                        && unconfirmed_count == 0),
                children: facts.settlements.into(),
                cleanup_failures: failures.into(),
                transfers: transfers.into(),
                unconfirmed_count,
            };
            publish_released(&worker_shared, report);
        };
        let lease = Arc::new(Mutex::new(Some(task_lease)));
        let worker_lease = Arc::clone(&lease);
        match self.spawn_managed(async move {
            let ready = std::future::poll_fn(|cx| {
                if let Poll::Ready(active) = lock(&worker_lease)
                    .as_ref()
                    .expect("task lease")
                    .poll_active(cx)
                {
                    return Poll::Ready(active);
                }
                if let Poll::Ready(Ok(_reason)) = Pin::new(&mut admission_stop).poll(cx) {
                    return Poll::Ready(false);
                }
                Poll::Pending
            })
            .await;
            if ready && worker_context.admission_check().is_ok() {
                future.await;
            } else {
                let reason = worker_context
                    .stop_reason()
                    .unwrap_or(StopReason::UserCancelled);
                publish_result(
                    &gate_shared,
                    OperationOutcome::Stopped(StopReport {
                        reason,
                        accepted_at: None,
                    }),
                );
                publish_released(
                    &gate_shared,
                    SettlementReport {
                        operation_id: id,
                        resource_phase: ResourcePhase::Released,
                        all_released: true,
                        children: Arc::from([]),
                        cleanup_failures: Arc::from([]),
                        transfers: Arc::from([]),
                        unconfirmed_count: 0,
                    },
                );
            }
            if let Some(lease) = lock(&worker_lease).take() {
                lease.complete();
            }
        }) {
            Ok(_) => Ok(OperationHandle {
                shared,
                context,
                detached: false,
            }),
            Err(error) => {
                let reason = match error {
                    PortableDriverError::Capacity => AdmissionError::Busy,
                    _ => AdmissionError::DriverUnavailable,
                };
                drop(lock(&lease).take());
                let body = lock(&body_cell).take().expect("unstarted body retained");
                Err(StartError::new(reason, body))
            }
        }
    }

    /// Lazily start on first poll and return the typed business outcome.
    pub async fn run_async<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        body: F,
    ) -> Result<OperationOutcome<T, E>, StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let mut handle = self.start(options, body)?;
        Ok(handle
            .result()
            .await
            .expect("private run handle result is observed once"))
    }

    /// Run a body and observe both its typed result and final resource settlement.
    /// Resource-bearing outputs must be transferred or have their use leases returned
    /// before the body completes, otherwise this future correctly remains pending.
    pub async fn run_to_completion<F, Fut, T, E>(
        &self,
        options: ScopeOptions,
        body: F,
    ) -> Result<(OperationOutcome<T, E>, SettlementReport), StartError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let mut handle = self.start(options, body)?;
        let result = handle
            .result()
            .await
            .expect("private run handle result is observed once");
        let released = handle.released().await;
        Ok((result, released))
    }
}

fn publish_result<T, E>(shared: &Shared<T, E>, outcome: OperationOutcome<T, E>) {
    let (waiters, discarded) = {
        let mut state = lock(&shared.state);
        if state.result_discarded {
            (Vec::new(), Some(outcome))
        } else {
            state.result = Some(outcome);
            (std::mem::take(&mut state.result_waiters), None)
        }
    };
    drop(discarded);
    shared.changed.notify_all();
    for waiter in waiters {
        waiter.wake();
    }
}
fn publish_released<T, E>(shared: &Shared<T, E>, report: SettlementReport) {
    let (waiters, guards) = {
        let mut state = lock(&shared.state);
        state.released = Some(report);
        (
            std::mem::take(&mut state.release_waiters),
            std::mem::take(&mut state.completion_guards),
        )
    };
    drop(guards);
    shared.changed.notify_all();
    for waiter in waiters {
        waiter.wake();
    }
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

enum BlockingBodyResult<T, E> {
    Returned(Result<T, E>),
    Panicked,
}
struct BlockingCompletion<T, E> {
    result: Option<BlockingBodyResult<T, E>>,
    aborted: bool,
    waker: Option<Waker>,
}

struct CurrentThreadWaker(std::thread::Thread);
impl std::task::Wake for CurrentThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on_start_wait<F: Future>(future: F) -> F::Output {
    let waker = std::task::Waker::from(Arc::new(CurrentThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
        std::thread::park();
    }
}

pub(crate) struct CleanupFacts {
    pub(crate) phase: ResourcePhase,
    pub(crate) failures: Vec<CleanupFailureSummary>,
    pub(crate) transfers: Vec<crate::report::TransferReceipt>,
    pub(crate) unconfirmed: usize,
}

pub(crate) fn cleanup_scope_tree<'a>(
    scope: &'a OperationScope,
) -> Pin<Box<dyn Future<Output = CleanupFacts> + Send + 'a>> {
    Box::pin(async move {
        let mut all = CleanupFacts {
            phase: ResourcePhase::Released,
            failures: Vec::new(),
            transfers: Vec::new(),
            unconfirmed: 0,
        };
        for child in scope.child_scopes() {
            let child = cleanup_scope_tree(&child).await;
            all.failures.extend(child.failures);
            all.transfers.extend(child.transfers);
            all.unconfirmed += child.unconfirmed;
            if child.phase == ResourcePhase::Unconfirmed {
                all.phase = ResourcePhase::Unconfirmed;
            }
            if child.phase == ResourcePhase::RecoveryRequired {
                all.phase = ResourcePhase::RecoveryRequired;
            }
        }
        let own = cleanup_resources(scope).await;
        all.failures.extend(own.failures);
        all.transfers.extend(own.transfers);
        all.unconfirmed += own.unconfirmed;
        if own.phase == ResourcePhase::Unconfirmed {
            all.phase = ResourcePhase::Unconfirmed;
        }
        if own.phase == ResourcePhase::RecoveryRequired {
            all.phase = ResourcePhase::RecoveryRequired;
        }
        all
    })
}

async fn cleanup_resources(scope: &OperationScope) -> CleanupFacts {
    let registry = scope.resource_registry();
    let scheduler: CleanupScheduler = scope.cleanup_scheduler();
    let recovery = scope.recovery_registry();
    let mut failures = Vec::new();
    let mut transfers = Vec::new();
    let mut unconfirmed: usize = 0;
    loop {
        let seen = scope.resource_epoch();
        let Some(ready) = registry.take_ready_cleanup() else {
            let pending = registry.pending_cleanup_count();
            if pending == 0 {
                break;
            }
            if !failures.is_empty() {
                unconfirmed = unconfirmed.saturating_add(pending.saturating_sub(failures.len()));
                break;
            }
            scope.wait_for_resource_change(seen).await;
            continue;
        };
        let id = ready.resource_id();
        let (_, owner) = ready.into_parts();
        let future = owner.begin(CleanupContext { deadline: None });
        let mut task = match scheduler.schedule(id, future) {
            Ok(task) => task,
            Err(_future) => {
                unconfirmed += 1;
                continue;
            }
        };
        let now = MonotonicTime::new(ClockDomainId(0), 0);
        let observation = std::future::poll_fn(|cx| match task.poll_at(cx, now) {
            Poll::Ready(CleanupObservation::Pending) => Poll::Pending,
            other => other,
        })
        .await;
        match observation {
            CleanupObservation::Settled(Ok(CleanupOutcome::Cleaned)) => {
                let _ = registry.confirm_released(id);
            }
            CleanupObservation::Settled(Ok(CleanupOutcome::Transferred(receipt))) => {
                if registry.confirm_transferred(id, receipt).is_ok() {
                    transfers.push(receipt);
                } else {
                    unconfirmed += 1;
                }
            }
            CleanupObservation::Settled(Ok(CleanupOutcome::FailedWithRecovery(failure, owner))) => {
                failures.push(CleanupFailureSummary {
                    resource_id: failure.resource_id,
                    stage: failure.stage,
                    summary: failure.summary,
                });
                match recovery.register(id, owner) {
                    Ok(key) => {
                        scope.save_recovery_key(key);
                        if registry.accept_recovery(id).is_err() {
                            unconfirmed += 1;
                        }
                    }
                    Err(_owner) => unconfirmed += 1,
                }
            }
            CleanupObservation::Settled(Err(_))
            | CleanupObservation::Unconfirmed { .. }
            | CleanupObservation::ClockDomainMismatch { .. } => {
                unconfirmed += 1;
            }
            CleanupObservation::Pending | CleanupObservation::AlreadySettled { .. } => {
                unconfirmed += 1;
            }
        }
    }
    let phase = if !failures.is_empty() {
        ResourcePhase::RecoveryRequired
    } else if unconfirmed > 0 {
        ResourcePhase::Unconfirmed
    } else {
        ResourcePhase::Released
    };
    CleanupFacts {
        phase,
        failures,
        transfers,
        unconfirmed,
    }
}

struct Cancellable<F> {
    future: Pin<Box<F>>,
    stop: crate::context::StopSignal,
    scope: OperationScope,
    stopped: bool,
    stop_reason: Option<StopReason>,
}
impl<F: Future> Future for Cancellable<F> {
    type Output = (F::Output, Option<StopReason>);
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.stopped
            && let Poll::Ready(Ok(reason)) = Pin::new(&mut self.stop).poll(cx)
        {
            self.stopped = true;
            self.stop_reason = Some(reason);
            self.scope.request_cancel(reason);
        }
        match self.future.as_mut().poll(cx) {
            Poll::Ready(output) => Poll::Ready((output, self.stop_reason)),
            Poll::Pending => Poll::Pending,
        }
    }
}

//! Bounded root admission for managed operations.
//!
//! This module owns admission bookkeeping only. Operation execution and result
//! observation are attached by the operation handle layer.

use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::Waker;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::context::ContextError;
use crate::context::Deadline;
use crate::context::DeadlineSignal;
use crate::context::OperationContext;
use crate::context::StopSignal;
use crate::driver::PortableDriver;
use crate::driver::PortableDriverConfig;
use crate::driver::PortableDriverError;
use crate::driver::TaskHandle;
use crate::operation::OperationHandle;
use crate::protocol::ClockSource;
use crate::report::AdmissionError;
use crate::report::OperationId;
use crate::report::ResourceId;
use crate::report::ScopeLimits;
use crate::report::StartError;
use crate::report::SupervisorLimits;
use crate::resource::CleanupResponsibility;
use crate::resource::CleanupScheduler;
use crate::resource::RecoveryRegistry;

mod shutdown_wait_registration;
use shutdown_wait_registration::ShutdownWaitRegistration;

type SharedServiceGroupState = Arc<(Mutex<ServiceGroupState>, std::sync::Condvar)>;

struct Inner {
    limits: SupervisorLimits,
    state: Mutex<State>,
    driver: Mutex<Option<Arc<PortableDriver>>>,
    cleanup_scheduler: CleanupScheduler,
    cleanup_responsibility: CleanupResponsibility,
    changed: std::sync::Condvar,
    recovery_registries: Mutex<Vec<RecoveryRegistry>>,
    public_handles: AtomicUsize,
    service_groups: Mutex<Vec<SharedServiceGroupState>>,
}

struct State {
    closed: bool,
    active: usize,
    queued: VecDeque<Weak<Mutex<LeaseState>>>,
    results: usize,
    cleanup_active: usize,
    waiters: Vec<(u64, Option<Waker>)>,
    next_waiter: u64,
    cancel: bool,
    contexts: Vec<(Weak<Mutex<LeaseState>>, OperationContext)>,
    shutdown_waiters: Vec<(u64, Waker)>,
    next_shutdown_waiter: u64,
}

#[derive(Default)]
struct LeaseState {
    queued: bool,
    active: bool,
    execution_released: bool,
    result_reserved: bool,
    active_waiters: Vec<Waker>,
}

/// Application-owned bounded root operation admission service.
pub struct OperationSupervisor {
    inner: Arc<Inner>,
    public: bool,
}

impl Clone for OperationSupervisor {
    fn clone(&self) -> Self {
        self.inner.public_handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
            public: true,
        }
    }
}

impl Drop for OperationSupervisor {
    fn drop(&mut self) {
        if self.public && self.inner.public_handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            let handle = Self {
                inner: Arc::clone(&self.inner),
                public: false,
            };
            handle.begin_shutdown(ShutdownMode::Cancel);
        }
    }
}

/// Shutdown policy for an admission service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownMode {
    /// Stop new roots while allowing already admitted work to finish.
    Drain,
    /// Stop admission and request cancellation of managed work.
    Cancel,
}

/// Snapshot returned when waiting for service-group work to finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Number of group requests still holding an admission lease.
    pub pending_requests: usize,
    /// Recovery owners still retained by accepted operations.
    pub pending_recovery: usize,
    /// Service resource scopes still awaiting cleanup confirmation.
    pub pending_resources: usize,
}

impl ShutdownReport {
    pub fn is_complete(&self) -> bool {
        self.pending_requests == 0 && self.pending_recovery == 0 && self.pending_resources == 0
    }
}

struct ServiceGroupState {
    closed: bool,
    limit: usize,
    mode: ShutdownMode,
    next_id: u64,
    requests: std::collections::BTreeMap<u64, ServiceRequestEntry>,
    shutdown_waiters: Vec<(u64, Waker)>,
    next_shutdown_waiter: u64,
    resource_scopes:
        std::collections::BTreeMap<u64, (crate::scope::OperationScope, Arc<std::sync::atomic::AtomicBool>, bool)>,
    next_resource_id: u64,
}

enum ServiceRequestEntry {
    Standalone,
    Managed(Option<OperationContext>),
}

/// Bounded admission group for long-lived services. It consumes no root slot.
pub struct ServiceGroup {
    state: SharedServiceGroupState,
    supervisor: OperationSupervisor,
}

/// Reusable observer for a supervisor or service-group shutdown.
pub struct ShutdownObserver(ShutdownSource);

#[derive(Clone)]
enum ShutdownSource {
    Supervisor(OperationSupervisor),
    Service(SharedServiceGroupState, OperationSupervisor),
}

impl Clone for ShutdownObserver {
    fn clone(&self) -> Self {
        Self(match &self.0 {
            ShutdownSource::Supervisor(supervisor) => ShutdownSource::Supervisor(supervisor.internal_handle()),
            ShutdownSource::Service(state, supervisor) => {
                ShutdownSource::Service(Arc::clone(state), supervisor.internal_handle())
            }
        })
    }
}

/// A shutdown snapshot and an observer that can wait again after a timeout.
#[derive(Clone)]
pub struct ShutdownObservation {
    pub report: ShutdownReport,
    pub observer: ShutdownObserver,
}

/// Admission lease held for the lifetime of one service request.
pub struct ServiceRequest {
    state: SharedServiceGroupState,
    id: u64,
    supervisor: OperationSupervisor,
    released: bool,
}

/// A managed operation whose service-group lease lasts through resource
/// settlement.
pub struct ServiceOperationHandle<T, E> {
    inner: OperationHandle<T, E>,
}

/// Independent finite resource owner scope associated with a service group.
pub struct ServiceResourceScope {
    scope: crate::scope::OperationScope,
    closed: Arc<std::sync::atomic::AtomicBool>,
}

/// A service resource registration was rejected before ownership transfer.
pub struct ServiceResourceError {
    pub reason: AdmissionError,
    owner: Box<dyn crate::protocol::OwnedCleanup>,
}

impl fmt::Debug for ServiceResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceResourceError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl ServiceResourceError {
    /// Recover the exact unaccepted cleanup owner.
    pub fn into_owner(self) -> Box<dyn crate::protocol::OwnedCleanup> {
        self.owner
    }
}

impl ServiceResourceScope {
    /// Register one resource while the service group remains open.
    pub fn register_resource<C: crate::protocol::OwnedCleanup>(
        &self,
        owner: C,
    ) -> Result<ResourceId, ServiceResourceError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ServiceResourceError {
                reason: AdmissionError::Closed,
                owner: Box::new(owner),
            });
        }
        self.scope
            .register_resource(owner)
            .map_err(|error| ServiceResourceError {
                reason: AdmissionError::ReservationLimit,
                owner: error.into_owner(),
            })
    }

    /// Acquire one lease for a registered service resource.
    pub fn acquire_use(&self, id: ResourceId) -> Result<crate::resource::ResourceUse, crate::resource::ResourceError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::resource::ResourceError::ClosedForUse);
        }
        self.scope.acquire_use(id)
    }

    /// Close new resource use. Existing leases remain until returned.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.scope.close_body_resources();
        }
    }

    /// Take the one-use recovery key for a cleanup owner retained by this
    /// scope.
    pub fn take_recovery_key(&self) -> Option<crate::report::RecoveryKey<crate::report::RecoveryOwner>> {
        self.scope.take_recovery_key()
    }

    /// Recover a typed owner using the key returned by
    /// [`Self::take_recovery_key`].
    pub fn take_recovery<R: Send + 'static>(
        &self,
        key: crate::report::RecoveryKey<R>,
    ) -> Result<R, crate::report::RecoveryKey<R>> {
        self.scope.take_recovery(key)
    }
}

impl Drop for ServiceResourceScope {
    fn drop(&mut self) {
        self.close();
    }
}

impl<T, E> ServiceOperationHandle<T, E> {
    pub fn request_cancel(&self, reason: crate::report::StopReason) {
        self.inner.request_cancel(reason);
    }
    pub fn observer(&self) -> crate::operation::OperationObserver<T, E> {
        self.inner.observer()
    }
    pub async fn result(&mut self) -> Result<crate::report::OperationOutcome<T, E>, crate::report::ObservationError> {
        self.inner.result().await
    }
    pub fn result_blocking(
        &mut self,
    ) -> Result<crate::report::OperationOutcome<T, E>, crate::report::ObservationError> {
        self.inner.result_blocking()
    }
    pub async fn released(&self) -> crate::report::SettlementReport {
        self.inner.released().await
    }
    pub fn released_blocking(&self) -> Result<crate::report::SettlementReport, crate::report::ObservationError> {
        self.inner.released_blocking()
    }
}

impl OperationSupervisor {
    pub(crate) fn internal_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            public: false,
        }
    }

    /// Create a service admission group with its own finite request capacity.
    pub fn service_group(&self, capacity: usize) -> Result<ServiceGroup, AdmissionError> {
        if capacity == 0 {
            return Err(AdmissionError::ReservationLimit);
        }
        let state = Arc::new((
            Mutex::new(ServiceGroupState {
                closed: false,
                limit: capacity,
                mode: ShutdownMode::Drain,
                next_id: 1,
                requests: std::collections::BTreeMap::new(),
                shutdown_waiters: Vec::new(),
                next_shutdown_waiter: 1,
                resource_scopes: std::collections::BTreeMap::new(),
                next_resource_id: 1,
            }),
            std::sync::Condvar::new(),
        ));
        lock(&self.inner.service_groups).push(Arc::clone(&state));
        Ok(ServiceGroup {
            state,
            supervisor: self.internal_handle(),
        })
    }
}

impl ServiceGroup {
    /// Create a resource owner scope with a finite quota independent from root
    /// admission.
    pub fn resource_scope(&self, capacity: usize) -> Result<ServiceResourceScope, AdmissionError> {
        if capacity == 0 || capacity > self.supervisor.inner.limits.scope.resources {
            return Err(AdmissionError::ReservationLimit);
        }
        let mut state = lock(&self.state.0);
        if state.closed {
            return Err(AdmissionError::Closed);
        }
        if state.resource_scopes.len() >= state.limit {
            return Err(AdmissionError::Busy);
        }
        let id = state.next_resource_id;
        state.next_resource_id = id.checked_add(1).ok_or(AdmissionError::Busy)?;
        let operation = OperationId::new((1_u128 << 120) | id as u128);
        let scope = crate::scope::OperationScope::new(
            operation,
            ScopeLimits {
                child_operations: 0,
                resources: capacity,
                effect_records: 0,
            },
            crate::scope::ScopePolicy::FailFast,
            self.supervisor.cleanup_scheduler(),
            crate::context::OperationContext::new(crate::context::ContextOptions::default()),
        );
        self.supervisor.track_scope_recovery(&scope);
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        state
            .resource_scopes
            .insert(id, (scope.clone(), Arc::clone(&closed), false));
        Ok(ServiceResourceScope { scope, closed })
    }

    /// Atomically reserve one request slot unless this group is closed or full.
    pub fn try_admit(&self) -> Result<ServiceRequest, AdmissionError> {
        self.reserve_request(false)
    }

    fn reserve_request(&self, managed: bool) -> Result<ServiceRequest, AdmissionError> {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(AdmissionError::Closed);
        }
        if state.requests.len() >= state.limit {
            return Err(AdmissionError::Busy);
        }
        let id = state.next_id;
        state.next_id = id.checked_add(1).ok_or(AdmissionError::Busy)?;
        state.requests.insert(
            id,
            if managed {
                ServiceRequestEntry::Managed(None)
            } else {
                ServiceRequestEntry::Standalone
            },
        );
        Ok(ServiceRequest {
            state: Arc::clone(&self.state),
            id,
            supervisor: self.supervisor.internal_handle(),
            released: false,
        })
    }

    /// Admit and supervise one real root operation under this service group.
    pub fn start<F, Fut, T, E>(
        &self,
        options: crate::scope::ScopeOptions,
        body: F,
    ) -> Result<ServiceOperationHandle<T, E>, StartError<F>>
    where
        F: FnOnce(crate::scope::OperationScope) -> Fut + Send + 'static,
        Fut: core::future::Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let request = match self.reserve_request(true) {
            Ok(request) => request,
            Err(reason) => return Err(StartError::new(reason, body)),
        };
        let handle = match self.supervisor.start(options, body) {
            Ok(handle) => handle,
            Err(error) => {
                drop(request);
                return Err(error);
            }
        };
        let context = handle.cancellation_context();
        {
            let mut state = lock(&self.state.0);
            if let Some(ServiceRequestEntry::Managed(slot)) = state.requests.get_mut(&request.id) {
                *slot = Some(context.clone());
            }
            if state.mode == ShutdownMode::Cancel {
                context.request_stop(crate::report::StopReason::ParentStopped);
            }
        }
        handle.hold_until_released(Box::new(request));
        Ok(ServiceOperationHandle { inner: handle })
    }

    /// Stop group admission and wait up to `timeout` for request leases to
    /// return.
    pub fn shutdown_blocking(&self, mode: ShutdownMode, timeout: std::time::Duration) -> ShutdownReport {
        self.begin_shutdown(mode);
        let (mutex, changed) = &*self.state;
        let state = mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (state, _) = changed
            .wait_timeout_while(state, timeout, |state| {
                !state.requests.is_empty() || !state.resource_scopes.is_empty()
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ShutdownReport {
            pending_requests: state.requests.len(),
            pending_recovery: self.supervisor.pending_recovery(),
            pending_resources: state.resource_scopes.len(),
        }
    }

    /// Current number of live request leases.
    pub fn active_requests(&self) -> usize {
        self.state
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requests
            .len()
    }

    /// Stop this group and observe its shutdown asynchronously.
    pub async fn shutdown_async(
        &self,
        mode: ShutdownMode,
        deadline: Deadline,
        clock: Arc<dyn ClockSource>,
    ) -> Result<ShutdownObservation, ContextError> {
        self.begin_shutdown(mode);
        ShutdownObserver(ShutdownSource::Service(
            Arc::clone(&self.state),
            self.supervisor.internal_handle(),
        ))
        .wait_async(deadline, clock)
        .await
    }

    fn begin_shutdown(&self, mode: ShutdownMode) {
        begin_service_group_state(Arc::clone(&self.state), mode, self.supervisor.internal_handle());
    }
}

impl Drop for ServiceGroup {
    fn drop(&mut self) {
        begin_service_group_state(
            Arc::clone(&self.state),
            ShutdownMode::Cancel,
            self.supervisor.internal_handle(),
        );
    }
}

impl Drop for ServiceRequest {
    fn drop(&mut self) {
        if !self.released {
            let (lock, changed) = &*self.state;
            let (waiters, _) = {
                let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state.requests.remove(&self.id);
                self.released = true;
                (std::mem::take(&mut state.shutdown_waiters), ())
            };
            changed.notify_all();
            wake_shutdown(waiters);
            notify_shutdown(&self.supervisor);
            start_service_resource_cleanup(Arc::clone(&self.state), self.supervisor.internal_handle());
        }
    }
}

fn start_service_resource_cleanup(state: SharedServiceGroupState, supervisor: OperationSupervisor) {
    let ready = {
        let mut guard = lock(&state.0);
        if !guard.closed || !guard.requests.is_empty() {
            return;
        }
        guard
            .resource_scopes
            .iter_mut()
            .filter_map(|(id, (scope, closed, started))| {
                if *started {
                    return None;
                }
                *started = true;
                closed.store(true, Ordering::Release);
                scope.close_body_resources();
                Some((*id, scope.clone()))
            })
            .collect::<Vec<_>>()
    };
    if ready.is_empty() {
        remove_completed_service_group(&supervisor, &state);
    }
    for (id, scope) in ready {
        let task_state = Arc::clone(&state);
        let task_supervisor = supervisor.internal_handle();
        let result = supervisor.spawn_managed(async move {
            let _ = scope.join().await;
            let waiters = {
                let mut guard = lock(&task_state.0);
                guard.resource_scopes.remove(&id);
                std::mem::take(&mut guard.shutdown_waiters)
            };
            task_state.1.notify_all();
            wake_shutdown(waiters);
            notify_shutdown(&task_supervisor);
            remove_completed_service_group(&task_supervisor, &task_state);
            drop(task_supervisor);
        });
        if result.is_err() {
            let mut guard = lock(&state.0);
            if let Some((_, _, started)) = guard.resource_scopes.get_mut(&id) {
                *started = false;
            }
        }
    }
}

fn notify_shutdown(supervisor: &OperationSupervisor) {
    let waiters = {
        let mut state = lock(&supervisor.inner.state);
        std::mem::take(&mut state.shutdown_waiters)
    };
    supervisor.inner.changed.notify_all();
    wake_shutdown(waiters);
}

fn retry_service_cleanups_after_capacity_change(inner: &Arc<Inner>) {
    let supervisor = OperationSupervisor {
        inner: Arc::clone(inner),
        public: false,
    };
    retry_closed_service_group_cleanups(&supervisor);
}

fn begin_service_group_state(state: SharedServiceGroupState, mode: ShutdownMode, supervisor: OperationSupervisor) {
    let (contexts, waiters) = {
        let mut guard = lock(&state.0);
        guard.closed = true;
        if mode == ShutdownMode::Cancel {
            guard.mode = ShutdownMode::Cancel;
        }
        let contexts = if guard.mode == ShutdownMode::Cancel {
            guard
                .requests
                .values()
                .filter_map(|entry| match entry {
                    ServiceRequestEntry::Managed(Some(context)) => Some(context.clone()),
                    _ => None,
                })
                .collect()
        } else {
            Vec::new()
        };
        (contexts, std::mem::take(&mut guard.shutdown_waiters))
    };
    for context in contexts {
        context.request_stop(crate::report::StopReason::ParentStopped);
    }
    wake_shutdown(waiters);
    start_service_resource_cleanup(state, supervisor);
}

fn retry_closed_service_group_cleanups(supervisor: &OperationSupervisor) {
    let groups = lock(&supervisor.inner.service_groups).clone();
    for group in groups {
        let ready = {
            let state = lock(&group.0);
            state.closed && state.requests.is_empty()
        };
        if ready {
            start_service_resource_cleanup(Arc::clone(&group), supervisor.internal_handle());
            remove_completed_service_group(supervisor, &group);
        }
    }
}

fn remove_completed_service_group(supervisor: &OperationSupervisor, group: &SharedServiceGroupState) {
    let complete = {
        let state = lock(&group.0);
        state.closed && state.requests.is_empty() && state.resource_scopes.is_empty()
    };
    if complete {
        lock(&supervisor.inner.service_groups).retain(|candidate| !Arc::ptr_eq(candidate, group));
    }
}

impl ShutdownObserver {
    /// Wait again for active work to settle, or return a fresh pending snapshot
    /// at deadline.
    pub async fn wait_async(
        &self,
        deadline: Deadline,
        clock: Arc<dyn ClockSource>,
    ) -> Result<ShutdownObservation, ContextError> {
        match &self.0 {
            ShutdownSource::Service(state, supervisor) => {
                start_service_resource_cleanup(Arc::clone(state), supervisor.internal_handle())
            }
            ShutdownSource::Supervisor(supervisor) => retry_closed_service_group_cleanups(supervisor),
        }
        let context = OperationContext::new(crate::context::ContextOptions {
            clock: Some(clock),
            ..Default::default()
        });
        let mut timer = context.signal_at(deadline)?;
        let mut registration = ShutdownWaitRegistration::new(self.0.clone());
        let report = core::future::poll_fn(|cx| {
            let report = self.snapshot();
            if report.is_complete() {
                return Poll::Ready(Ok(report));
            }
            if Pin::new(&mut timer).poll(cx).is_ready() {
                return Poll::Ready(Ok(report));
            }
            if let Err(error) = registration.register(cx.waker()) {
                return Poll::Ready(Err(error));
            }
            match &self.0 {
                ShutdownSource::Supervisor(supervisor) | ShutdownSource::Service(_, supervisor) => {
                    supervisor.register_recovery_wakers(cx.waker());
                }
            }
            if self.snapshot().is_complete() {
                return Poll::Ready(Ok(self.snapshot()));
            }
            Poll::Pending
        })
        .await?;
        registration.unregister();
        Ok(ShutdownObservation {
            report,
            observer: self.clone(),
        })
    }

    /// Wait synchronously for active work to settle or timeout.
    pub fn wait_blocking(&self, timeout: std::time::Duration) -> ShutdownObservation {
        match &self.0 {
            ShutdownSource::Service(state, supervisor) => {
                start_service_resource_cleanup(Arc::clone(state), supervisor.internal_handle())
            }
            ShutdownSource::Supervisor(supervisor) => retry_closed_service_group_cleanups(supervisor),
        }
        let report = match &self.0 {
            ShutdownSource::Supervisor(supervisor) => {
                let state = lock(&supervisor.inner.state);
                let (state, _) = supervisor
                    .inner
                    .changed
                    .wait_timeout_while(state, timeout, |s| {
                        s.active > 0
                            || !s.queued.is_empty()
                            || supervisor.service_totals().1 > 0
                            || supervisor.service_totals().0 > 0
                    })
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                ShutdownReport {
                    pending_requests: state.active + state.queued.len() + supervisor.service_totals().0,
                    pending_recovery: supervisor.pending_recovery(),
                    pending_resources: supervisor.service_totals().1,
                }
            }
            ShutdownSource::Service(state, supervisor) => {
                let (mutex, changed) = &**state;
                let guard = mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                let (guard, _) = changed
                    .wait_timeout_while(guard, timeout, |s| {
                        !s.requests.is_empty() || !s.resource_scopes.is_empty()
                    })
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                ShutdownReport {
                    pending_requests: guard.requests.len(),
                    pending_recovery: supervisor.pending_recovery(),
                    pending_resources: guard.resource_scopes.len(),
                }
            }
        };
        ShutdownObservation {
            report,
            observer: self.clone(),
        }
    }

    fn snapshot(&self) -> ShutdownReport {
        match &self.0 {
            ShutdownSource::Supervisor(supervisor) => {
                let state = lock(&supervisor.inner.state);
                ShutdownReport {
                    pending_requests: state.active + state.queued.len() + supervisor.service_totals().0,
                    pending_recovery: supervisor.pending_recovery(),
                    pending_resources: supervisor.service_totals().1,
                }
            }
            ShutdownSource::Service(state, supervisor) => {
                let guard = lock(&state.0);
                ShutdownReport {
                    pending_requests: guard.requests.len(),
                    pending_recovery: supervisor.pending_recovery(),
                    pending_resources: guard.resource_scopes.len(),
                }
            }
        }
    }
}

impl OperationSupervisor {
    /// Create a supervisor with explicit finite capacities.
    pub fn new(limits: SupervisorLimits) -> Self {
        let cleanup_scheduler = CleanupScheduler::new(limits.cleanup_concurrency);
        let cleanup_responsibility = cleanup_scheduler.responsibility_owner();
        Self {
            inner: Arc::new(Inner {
                limits,
                state: Mutex::new(State {
                    closed: false,
                    active: 0,
                    queued: VecDeque::new(),
                    results: 0,
                    cleanup_active: 0,
                    waiters: Vec::new(),
                    next_waiter: 1,
                    cancel: false,
                    contexts: Vec::new(),
                    shutdown_waiters: Vec::new(),
                    next_shutdown_waiter: 1,
                }),
                driver: Mutex::new(None),
                cleanup_scheduler,
                cleanup_responsibility,
                changed: std::sync::Condvar::new(),
                recovery_registries: Mutex::new(Vec::new()),
                public_handles: AtomicUsize::new(1),
                service_groups: Mutex::new(Vec::new()),
            }),
            public: true,
        }
    }

    pub(crate) fn cleanup_scheduler(&self) -> CleanupScheduler {
        self.inner.cleanup_scheduler.clone()
    }

    pub(crate) fn track_scope_recovery(&self, scope: &crate::scope::OperationScope) {
        let registry = scope.recovery_registry();
        let mut registries = lock(&self.inner.recovery_registries);
        registries.retain(|entry| !entry.is_empty());
        registries.push(registry);
    }

    fn pending_recovery(&self) -> usize {
        lock(&self.inner.recovery_registries)
            .iter()
            .map(RecoveryRegistry::len)
            .sum()
    }

    fn register_recovery_wakers(&self, waker: &Waker) {
        let registries = lock(&self.inner.recovery_registries).clone();
        for registry in registries {
            registry.register_waker_if_nonempty(waker);
        }
    }

    fn service_totals(&self) -> (usize, usize) {
        let groups = { lock(&self.inner.service_groups).clone() };
        groups.into_iter().fold((0, 0), |(requests, resources), group| {
            let state = lock(&group.0);
            (
                requests.saturating_add(
                    state
                        .requests
                        .values()
                        .filter(|entry| matches!(entry, ServiceRequestEntry::Standalone))
                        .count(),
                ),
                resources.saturating_add(state.resource_scopes.len()),
            )
        })
    }

    /// Number of cleanup permits retained by this shared supervisor.
    pub fn cleanup_tasks_active(&self) -> usize {
        self.inner.cleanup_responsibility.active()
    }

    /// Submit a managed future to the supervisor's shared bounded portable
    /// driver. This is crate-private so public operation APIs remain the
    /// only submission path.
    pub(crate) fn spawn_managed<F>(&self, future: F) -> Result<TaskHandle, PortableDriverError>
    where
        F: core::future::Future<Output = ()> + Send + 'static,
    {
        let mut driver = lock(&self.inner.driver);
        if driver.is_none() {
            let Some(task_capacity) = self
                .inner
                .limits
                .active_roots
                .checked_add(self.inner.limits.queued_roots)
            else {
                return Err(PortableDriverError::InvalidConfiguration);
            };
            let task_capacity = task_capacity.max(1);
            let driver_handle = PortableDriver::new(PortableDriverConfig {
                worker_count: self.inner.limits.active_roots.max(1),
                task_capacity,
                polls_per_turn: 16,
            })
            .map_err(|_| PortableDriverError::WorkerSpawn)?;
            *driver = Some(Arc::new(driver_handle));
        }
        driver.as_ref().expect("driver initialized").spawn(future)
    }

    /// Reserve a root execution position and its typed-result slot.
    pub fn try_admit(&self) -> Result<RootAdmission<()>, AdmissionError> {
        self.try_admit_body(()).map_err(|error| error.reason())
    }

    /// Reserve capacity while retaining the unstarted body with the admission.
    /// The body is never invoked by this method.
    pub fn try_admit_body<F>(&self, body: F) -> Result<RootAdmission<F>, StartError<F>> {
        let lease = Arc::new(Mutex::new(LeaseState::default()));
        let mut state = lock(&self.inner.state);
        if state.closed {
            return Err(StartError::new(AdmissionError::Closed, body));
        }
        if state.results >= self.inner.limits.unobserved_results {
            return Err(StartError::new(AdmissionError::Busy, body));
        }
        let queued = state.active >= self.inner.limits.active_roots;
        if queued && state.queued.len() >= self.inner.limits.queued_roots {
            return Err(StartError::new(AdmissionError::Busy, body));
        }
        state.results += 1;
        {
            let mut lease_state = lock(&lease);
            lease_state.result_reserved = true;
            lease_state.queued = queued;
            lease_state.active = !queued;
        }
        if queued {
            state.queued.push_back(Arc::downgrade(&lease));
        } else {
            state.active += 1;
        }
        Ok(RootAdmission {
            inner: Arc::clone(&self.inner),
            lease,
            body: Some(body),
            transferred: false,
        })
    }

    /// Validate inherited cancellation, deadline, and budget before reserving
    /// root capacity.
    pub fn try_admit_with_context<F>(
        &self,
        body: F,
        context: &OperationContext,
    ) -> Result<RootAdmission<F>, StartError<F>> {
        if let Err(error) = context.admission_check() {
            return Err(StartError::new(context_admission_error(context, error), body));
        }
        let admission = self.try_admit_body(body)?;
        let cancel = {
            let mut state = lock(&self.inner.state);
            state.contexts.retain(|(lease, _)| lease.strong_count() > 0);
            state.contexts.push((Arc::downgrade(&admission.lease), context.clone()));
            state.cancel
        };
        if cancel {
            context.request_stop(crate::report::StopReason::ParentStopped);
        }
        Ok(admission)
    }

    /// Create a bounded asynchronous admission waiter.
    pub fn start_wait_body<F>(&self, body: F) -> Result<AdmissionWait<F>, StartError<F>> {
        self.start_wait_body_with_context(body, None)
    }

    /// Create a bounded asynchronous admission waiter that also observes its
    /// context deadline.
    pub fn start_wait_body_with_context<F>(
        &self,
        body: F,
        context: Option<OperationContext>,
    ) -> Result<AdmissionWait<F>, StartError<F>> {
        if let Some(context) = &context
            && let Err(error) = context.admission_check()
        {
            return Err(StartError::new(context_admission_error(context, error), body));
        }
        let mut state = lock(&self.inner.state);
        let error = if state.closed {
            Some(AdmissionError::Closed)
        } else if state.waiters.len() >= self.inner.limits.admission_waiters {
            Some(AdmissionError::Busy)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(StartError::new(error, body));
        }
        let id = state.next_waiter;
        let Some(next_waiter) = id.checked_add(1) else {
            return Err(StartError::new(AdmissionError::Busy, body));
        };
        state.next_waiter = next_waiter;
        state.waiters.push((id, None));
        drop(state);
        let deadline_signal = context.as_ref().and_then(OperationContext::deadline_signal);
        let stop_signal = context.as_ref().map(OperationContext::stop_signal);
        Ok(AdmissionWait {
            supervisor: self.internal_handle(),
            waiter_id: id,
            body: Some(body),
            context,
            deadline_signal,
            stop_signal,
            done: false,
        })
    }

    /// Stop accepting new root operations and wake all registered waiters.
    pub fn close(&self) {
        let waiters = {
            let mut state = lock(&self.inner.state);
            state.closed = true;
            take_waiter_wakers(&mut state)
        };
        wake(waiters);
        self.inner.changed.notify_all();
    }

    /// Stop root admission and wait up to the caller-owned timeout for accepted
    /// work. Drain can only be upgraded to Cancel; every call observes the
    /// shared lifecycle.
    pub fn shutdown_blocking(&self, mode: ShutdownMode, timeout: std::time::Duration) -> ShutdownReport {
        self.begin_shutdown(mode);
        let state = lock(&self.inner.state);
        let (state, _) = self
            .inner
            .changed
            .wait_timeout_while(state, timeout, |state| {
                state.active > 0
                    || !state.queued.is_empty()
                    || self.service_totals().1 > 0
                    || self.service_totals().0 > 0
            })
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ShutdownReport {
            pending_requests: state.active + state.queued.len() + self.service_totals().0,
            pending_recovery: self.pending_recovery(),
            pending_resources: self.service_totals().1,
        }
    }

    /// Begin shutdown and return a reusable observer after `timeout` expires or
    /// work settles.
    pub async fn shutdown_async(
        &self,
        mode: ShutdownMode,
        deadline: Deadline,
        clock: Arc<dyn ClockSource>,
    ) -> Result<ShutdownObservation, ContextError> {
        self.begin_shutdown(mode);
        ShutdownObserver(ShutdownSource::Supervisor(self.internal_handle()))
            .wait_async(deadline, clock)
            .await
    }

    /// Create a reusable observer for an already requested or future shutdown.
    pub fn shutdown_observer(&self) -> ShutdownObserver {
        ShutdownObserver(ShutdownSource::Supervisor(self.internal_handle()))
    }

    fn begin_shutdown(&self, mode: ShutdownMode) {
        let (contexts, waiters, shutdown_waiters) = {
            let mut state = lock(&self.inner.state);
            state.closed = true;
            if mode == ShutdownMode::Cancel {
                state.cancel = true;
            }
            state.contexts.retain(|(lease, _)| lease.strong_count() > 0);
            let contexts = if state.cancel {
                state.contexts.iter().map(|(_, context)| context.clone()).collect()
            } else {
                Vec::new()
            };
            (
                contexts,
                take_waiter_wakers(&mut state),
                std::mem::take(&mut state.shutdown_waiters),
            )
        };
        for context in contexts {
            context.request_stop(crate::report::StopReason::ParentStopped);
        }
        let groups = lock(&self.inner.service_groups).clone();
        for group in groups {
            begin_service_group_state(Arc::clone(&group), mode, self.internal_handle());
            remove_completed_service_group(self, &group);
        }
        wake(waiters);
        wake_shutdown(shutdown_waiters);
        self.inner.changed.notify_all();
    }

    /// Number of root bodies currently holding execution capacity.
    pub fn active_roots(&self) -> usize {
        lock(&self.inner.state).active
    }

    /// Number of accepted roots waiting for execution capacity.
    pub fn queued_roots(&self) -> usize {
        lock(&self.inner.state).queued.len()
    }

    /// Number of reserved typed-result slots, including completed results.
    pub fn reserved_results(&self) -> usize {
        lock(&self.inner.state).results
    }

    /// Reserve a cleanup slot that does not consume business admission
    /// capacity.
    pub fn try_reserve_cleanup(&self) -> Result<CleanupPermit, AdmissionError> {
        let mut state = lock(&self.inner.state);
        if state.cleanup_active >= self.inner.limits.cleanup_concurrency {
            return Err(AdmissionError::Busy);
        }
        state.cleanup_active += 1;
        Ok(CleanupPermit {
            inner: Arc::clone(&self.inner),
            released: false,
        })
    }

    /// Number of active cleanup reservations.
    pub fn cleanup_active(&self) -> usize {
        lock(&self.inner.state).cleanup_active
    }
}

/// Independent cleanup capacity reservation.
pub struct CleanupPermit {
    inner: Arc<Inner>,
    released: bool,
}

impl Drop for CleanupPermit {
    fn drop(&mut self) {
        if !self.released {
            let mut state = lock(&self.inner.state);
            state.cleanup_active = state.cleanup_active.saturating_sub(1);
            self.released = true;
        }
    }
}

/// A rollback-safe root capacity reservation.
pub struct RootAdmission<F> {
    inner: Arc<Inner>,
    lease: Arc<Mutex<LeaseState>>,
    body: Option<F>,
    transferred: bool,
}

impl<F> RootAdmission<F> {
    /// Whether this accepted operation is waiting for an active-root slot.
    pub fn is_queued(&self) -> bool {
        lock(&self.lease).queued
    }

    /// Transfer an accepted body and its independent execution/result leases to
    /// the coordinator.
    ///
    /// This is crate-private until the operation handle layer can bind both
    /// leases to the actual task and result registry. Dropping this
    /// reservation after transfer cannot release either lease.
    pub(crate) fn transfer(&mut self) -> Option<AdmittedTask<F>> {
        if self.transferred {
            return None;
        }
        let body = self.body.take()?;
        self.transferred = true;
        Some(AdmittedTask {
            body,
            execution: Some(TaskLease {
                inner: Arc::clone(&self.inner),
                lease: Arc::clone(&self.lease),
            }),
            result: Some(ResultLease {
                inner: Arc::clone(&self.inner),
                lease: Arc::clone(&self.lease),
            }),
        })
    }
}

impl<F> fmt::Debug for RootAdmission<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootAdmission")
            .field("queued", &self.is_queued())
            .field("transferred", &self.transferred)
            .finish_non_exhaustive()
    }
}

impl<F> Drop for RootAdmission<F> {
    fn drop(&mut self) {
        if self.transferred {
            return;
        }
        release_all(&self.inner, &self.lease);
    }
}

/// Body transferred to a driver with separately owned lifecycle leases.
pub(crate) struct AdmittedTask<F> {
    body: F,
    execution: Option<TaskLease>,
    result: Option<ResultLease>,
}

impl<F> AdmittedTask<F> {
    pub(crate) fn into_parts(mut self) -> (F, TaskLease, ResultLease) {
        (
            self.body,
            self.execution.take().expect("execution lease exists"),
            self.result.take().expect("result lease exists"),
        )
    }
}

/// Capacity token retained by the running task until it finishes or is dropped.
pub(crate) struct TaskLease {
    inner: Arc<Inner>,
    lease: Arc<Mutex<LeaseState>>,
}

impl TaskLease {
    /// Mark task completion and release execution capacity.
    pub(crate) fn complete(self) {}

    /// Poll until this reservation owns an active root execution slot.
    pub(crate) fn poll_active(&self, cx: &mut Context<'_>) -> Poll<bool> {
        let mut lease = lock(&self.lease);
        if lease.active {
            return Poll::Ready(true);
        }
        if lease.execution_released {
            return Poll::Ready(false);
        }
        if !lease.active_waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
            lease.active_waiters.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl Drop for TaskLease {
    fn drop(&mut self) {
        release_execution(&self.inner, &self.lease);
    }
}

/// Capacity token retained with a result until it is observed or discarded.
pub(crate) struct ResultLease {
    inner: Arc<Inner>,
    lease: Arc<Mutex<LeaseState>>,
}

impl ResultLease {
    /// Release this slot after the result is taken or explicitly discarded.
    pub(crate) fn observe(self) {}
}

impl Drop for ResultLease {
    fn drop(&mut self) {
        release_result(&self.inner, &self.lease);
    }
}

fn release_execution(inner: &Arc<Inner>, lease: &Arc<Mutex<LeaseState>>) {
    let (waiters, shutdown_waiters) = {
        let mut state = lock(&inner.state);
        let mut lease_state = lock(lease);
        if lease_state.execution_released {
            return;
        }
        lease_state.execution_released = true;
        let mut promoted = Vec::new();
        if lease_state.active {
            lease_state.active = false;
            state.active = state.active.saturating_sub(1);
            promoted.extend(promote_one(&mut state, inner.limits.active_roots));
        }
        if lease_state.queued {
            lease_state.queued = false;
            state
                .queued
                .retain(|entry| entry.upgrade().is_some_and(|candidate| !Arc::ptr_eq(&candidate, lease)));
        }
        promoted.extend(take_waiter_wakers(&mut state));
        (promoted, std::mem::take(&mut state.shutdown_waiters))
    };
    wake(waiters);
    wake_shutdown(shutdown_waiters);
    inner.changed.notify_all();
    retry_service_cleanups_after_capacity_change(inner);
}

fn release_result(inner: &Arc<Inner>, lease: &Arc<Mutex<LeaseState>>) {
    let waiters = {
        let mut state = lock(&inner.state);
        let mut lease_state = lock(lease);
        if !lease_state.result_reserved {
            return;
        }
        lease_state.result_reserved = false;
        state.results = state.results.saturating_sub(1);
        take_waiter_wakers(&mut state)
    };
    wake(waiters);
}

fn release_all(inner: &Arc<Inner>, lease: &Arc<Mutex<LeaseState>>) {
    let (waiters, shutdown_waiters) = {
        let mut state = lock(&inner.state);
        let mut lease_state = lock(lease);
        let mut promoted = Vec::new();
        if !lease_state.execution_released {
            lease_state.execution_released = true;
            if lease_state.active {
                lease_state.active = false;
                state.active = state.active.saturating_sub(1);
                promoted.extend(promote_one(&mut state, inner.limits.active_roots));
            }
            if lease_state.queued {
                lease_state.queued = false;
                state
                    .queued
                    .retain(|entry| entry.upgrade().is_some_and(|candidate| !Arc::ptr_eq(&candidate, lease)));
            }
        }
        if lease_state.result_reserved {
            lease_state.result_reserved = false;
            state.results = state.results.saturating_sub(1);
        }
        promoted.extend(take_waiter_wakers(&mut state));
        (promoted, std::mem::take(&mut state.shutdown_waiters))
    };
    wake(waiters);
    wake_shutdown(shutdown_waiters);
    inner.changed.notify_all();
    retry_service_cleanups_after_capacity_change(inner);
}

/// A bounded waiter for root admission. Dropping it removes its registration;
/// after admission, dropping the returned reservation follows the ordinary root
/// path.
pub struct AdmissionWait<F> {
    supervisor: OperationSupervisor,
    waiter_id: u64,
    body: Option<F>,
    context: Option<OperationContext>,
    deadline_signal: Option<DeadlineSignal>,
    stop_signal: Option<StopSignal>,
    done: bool,
}

impl<F> fmt::Debug for AdmissionWait<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdmissionWait")
            .field("waiter_id", &self.waiter_id)
            .finish_non_exhaustive()
    }
}

impl<F> Unpin for AdmissionWait<F> {}

impl<F> Future for AdmissionWait<F> {
    type Output = Result<RootAdmission<F>, StartError<F>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.done {
            panic!("admission waiter polled after completion");
        }
        if let Some(signal) = &mut this.stop_signal {
            match Pin::new(signal).poll(cx) {
                Poll::Ready(Ok(reason)) => {
                    let error = if reason == crate::report::StopReason::Timeout {
                        AdmissionError::DeadlineExpired
                    } else {
                        AdmissionError::Stopped
                    };
                    let error = StartError::new(error, this.body.take().expect("body retained"));
                    this.unregister();
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Err(ContextError::WaiterCapacityExceeded)) => {
                    let error = StartError::new(AdmissionError::Busy, this.body.take().expect("body retained"));
                    this.unregister();
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Err(error)) => {
                    let error = StartError::new(
                        context_admission_error(this.context.as_ref().expect("signal has context"), error),
                        this.body.take().expect("body retained"),
                    );
                    this.unregister();
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => {}
            }
        }
        if let Some(context) = &this.context {
            if let Some(signal) = &mut this.deadline_signal
                && Pin::new(signal).poll(cx).is_ready()
            {
                let error = StartError::new(AdmissionError::DeadlineExpired, this.body.take().unwrap());
                this.unregister();
                return Poll::Ready(Err(error));
            }
            if let Err(error) = context.admission_check() {
                let error = StartError::new(context_admission_error(context, error), this.body.take().unwrap());
                this.unregister();
                return Poll::Ready(Err(error));
            }
        }
        match this.supervisor.try_admit_body(this.body.take().expect("body retained")) {
            Ok(admission) => {
                this.unregister();
                Poll::Ready(Ok(admission))
            }
            Err(error) if error.reason() == AdmissionError::Busy => {
                this.body = Some(error.into_body());
                this.register_waker(cx.waker());
                match this.supervisor.try_admit_body(this.body.take().expect("body retained")) {
                    Ok(admission) => {
                        this.unregister();
                        Poll::Ready(Ok(admission))
                    }
                    Err(error) if error.reason() == AdmissionError::Busy => {
                        this.body = Some(error.into_body());
                        Poll::Pending
                    }
                    Err(error) => {
                        this.unregister();
                        Poll::Ready(Err(error))
                    }
                }
            }
            Err(error) => {
                this.unregister();
                Poll::Ready(Err(error))
            }
        }
    }
}

impl<F> AdmissionWait<F> {
    fn register_waker(&self, waker: &Waker) {
        let mut replacement = Some(waker.clone());
        let replaced = {
            let mut state = lock(&self.supervisor.inner.state);
            state
                .waiters
                .iter_mut()
                .find(|(id, _)| *id == self.waiter_id)
                .and_then(|(_, registered)| {
                    if registered
                        .as_ref()
                        .is_some_and(|old| old.will_wake(replacement.as_ref().expect("waker cloned")))
                    {
                        None
                    } else {
                        registered.replace(replacement.take().expect("waker cloned"))
                    }
                })
        };
        drop(replaced);
        drop(replacement);
    }

    fn unregister(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        let removed = {
            let mut state = lock(&self.supervisor.inner.state);
            state
                .waiters
                .iter()
                .position(|(id, _)| *id == self.waiter_id)
                .map(|index| state.waiters.swap_remove(index).1)
        };
        drop(removed);
        self.deadline_signal.take();
        self.stop_signal.take();
    }
}

impl<F> Drop for AdmissionWait<F> {
    fn drop(&mut self) {
        self.unregister();
    }
}

fn context_admission_error(context: &OperationContext, error: ContextError) -> AdmissionError {
    match error {
        ContextError::DeadlineExpired => AdmissionError::DeadlineExpired,
        ContextError::Stopped if context.stop_reason() == Some(crate::report::StopReason::Timeout) => {
            AdmissionError::DeadlineExpired
        }
        ContextError::Stopped => AdmissionError::Stopped,
        ContextError::BudgetUnavailable => AdmissionError::ReservationLimit,
        ContextError::WaiterCapacityExceeded => AdmissionError::Busy,
        ContextError::ClockUnavailable | ContextError::ClockDomainMismatch => AdmissionError::DriverUnavailable,
    }
}

fn take_waiter_wakers(state: &mut State) -> Vec<Waker> {
    state.waiters.iter_mut().filter_map(|(_, waker)| waker.take()).collect()
}

fn wake(waiters: Vec<Waker>) {
    for waker in waiters {
        waker.wake();
    }
}

fn wake_shutdown(waiters: Vec<(u64, Waker)>) {
    for (_, waker) in waiters {
        waker.wake();
    }
}

fn promote_one(state: &mut State, active_limit: usize) -> Vec<Waker> {
    while state.active < active_limit {
        let Some(next) = state.queued.pop_front() else {
            return Vec::new();
        };
        let Some(next) = next.upgrade() else {
            continue;
        };
        let mut lease = lock(&next);
        if lease.execution_released || !lease.queued {
            continue;
        }
        lease.queued = false;
        lease.active = true;
        state.active += 1;
        return std::mem::take(&mut lease.active_waiters);
    }
    Vec::new()
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    fn limits(active_roots: usize, queued_roots: usize, unobserved_results: usize) -> SupervisorLimits {
        SupervisorLimits {
            active_roots,
            queued_roots,
            unobserved_results,
            admission_waiters: 1,
            ..SupervisorLimits::default()
        }
    }

    #[test]
    fn root_and_result_capacity_are_reserved_and_released_separately() {
        let supervisor = OperationSupervisor::new(limits(1, 1, 2));
        let active = supervisor.try_admit().unwrap();
        let queued = supervisor.try_admit().unwrap();
        assert!(queued.is_queued());
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        drop(active);
        assert_eq!(supervisor.active_roots(), 1);
        assert_eq!(supervisor.queued_roots(), 0);
        let replacement = supervisor.try_admit().unwrap();
        assert!(replacement.is_queued());
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        drop(queued);
        assert_eq!(supervisor.active_roots(), 1);
        drop(replacement);
        assert_eq!(supervisor.active_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 0);
        assert!(supervisor.try_admit().is_ok());
    }

    #[test]
    fn dropping_queued_admission_rolls_back_without_calling_its_body() {
        let supervisor = OperationSupervisor::new(limits(0, 1, 1));
        let count = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&count);
        let admission = supervisor
            .try_admit_body(move || captured.fetch_add(1, Ordering::SeqCst))
            .unwrap();
        assert!(admission.is_queued());
        drop(admission);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(supervisor.queued_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 0);
    }

    #[test]
    fn waiters_are_bounded_cancel_safely_and_recover_rejected_input() {
        use std::task::Wake;
        use std::task::Waker;
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }

        let supervisor = OperationSupervisor::new(limits(0, 1, 2));
        let accepted = supervisor.try_admit().unwrap();
        let mut waiting = Box::pin(supervisor.start_wait_body(String::from("kept")).unwrap());
        let waker = Waker::from(Arc::new(Noop));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        let rejected = supervisor.start_wait_body(String::from("rejected")).unwrap_err();
        assert_eq!(rejected.reason(), AdmissionError::Busy);
        assert_eq!(rejected.into_body(), "rejected");
        drop(waiting);
        drop(accepted);
        assert_eq!(lock(&supervisor.inner.state).waiters.len(), 0);
    }

    #[test]
    fn close_wakes_waiters_and_they_recover_their_input() {
        use std::task::Wake;
        use std::task::Waker;
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }
        let supervisor = OperationSupervisor::new(limits(0, 1, 2));
        let _accepted = supervisor.try_admit().unwrap();
        let mut waiting = Box::pin(supervisor.start_wait_body(String::from("body")).unwrap());
        let waker = Waker::from(Arc::new(Noop));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        supervisor.close();
        match waiting.as_mut().poll(&mut context) {
            Poll::Ready(Err(error)) => {
                assert_eq!(error.reason(), AdmissionError::Closed);
                assert_eq!(error.into_body(), "body");
            }
            other => panic!("expected closed error, got {other:?}"),
        }
    }

    #[test]
    fn result_slot_limit_is_one_shot_and_released_separately() {
        let supervisor = OperationSupervisor::new(limits(1, 0, 1));
        let mut first = supervisor.try_admit_body(String::from("body")).unwrap();
        let work = first.transfer().unwrap();
        drop(first);
        let (body, task_lease, result_lease) = work.into_parts();
        assert_eq!(body, "body");
        assert_eq!(supervisor.active_roots(), 1);
        assert_eq!(supervisor.reserved_results(), 1);
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        task_lease.complete();
        assert_eq!(supervisor.active_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 1);
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

        result_lease.observe();
        assert_eq!(supervisor.reserved_results(), 0);
        assert!(supervisor.try_admit().is_ok());
    }

    #[test]
    fn cleanup_capacity_remains_available_when_business_admission_is_full() {
        let supervisor = OperationSupervisor::new(limits(1, 0, 1));
        let _root = supervisor.try_admit().unwrap();
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
        let _cleanup = supervisor.try_reserve_cleanup().unwrap();
        assert_eq!(supervisor.cleanup_active(), 1);
    }

    #[test]
    fn expired_context_rejects_and_returns_the_original_body() {
        use std::task::Waker;

        use crate::context::ContextOptions;
        use crate::context::Deadline;
        use crate::protocol::ClockSource;
        use crate::protocol::TimerRegistration;
        use crate::report::ClockDomainId;
        use crate::report::MonotonicTime;

        struct FixedClock;
        struct NoTimer;
        impl TimerRegistration for NoTimer {
            fn cancel(self: Box<Self>) {}
        }
        impl ClockSource for FixedClock {
            fn now(&self) -> MonotonicTime {
                MonotonicTime::new(ClockDomainId(9), 2)
            }
            fn register_deadline(&self, _: MonotonicTime, _: Waker) -> Box<dyn TimerRegistration> {
                Box::new(NoTimer)
            }
        }

        let supervisor = OperationSupervisor::new(limits(1, 1, 1));
        let context = OperationContext::new(ContextOptions {
            clock: Some(Arc::new(FixedClock)),
            deadline: Some(Deadline::at(MonotonicTime::new(ClockDomainId(9), 1))),
            ..ContextOptions::default()
        });
        let error = supervisor
            .try_admit_with_context(String::from("body"), &context)
            .unwrap_err();
        assert_eq!(error.reason(), AdmissionError::DeadlineExpired);
        assert_eq!(error.into_body(), "body");
        assert_eq!(supervisor.active_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 0);
    }

    #[test]
    fn close_rejects_new_work() {
        let supervisor = OperationSupervisor::new(limits(0, 1, 1));
        supervisor.close();
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Closed);
    }
}

#[cfg(test)]
mod review_regressions {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::report::SupervisorLimits;

    #[test]
    fn dropping_admission_handle_does_not_release_transferred_running_body_quota() {
        let supervisor = OperationSupervisor::new(SupervisorLimits {
            active_roots: 1,
            queued_roots: 0,
            unobserved_results: 1,
            ..SupervisorLimits::default()
        });
        let runs = Arc::new(AtomicUsize::new(0));
        let body_runs = Arc::clone(&runs);
        let mut admission = supervisor
            .try_admit_body(move || body_runs.fetch_add(1, Ordering::SeqCst))
            .unwrap();
        let work = admission.transfer().unwrap();
        let (body, execution, result) = work.into_parts();
        drop(admission);

        assert_eq!(body(), 0);
        assert_eq!(supervisor.active_roots(), 1);
        assert_eq!(supervisor.reserved_results(), 1);
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        execution.complete();
        assert_eq!(supervisor.active_roots(), 0);
        assert_eq!(supervisor.reserved_results(), 1);
        assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
        result.observe();
        assert_eq!(supervisor.reserved_results(), 0);
    }
}

#[cfg(test)]
mod reentrant_waker_regression {
    use core::task::Context;
    use core::task::RawWaker;
    use core::task::RawWakerVTable;
    use core::task::Waker;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    struct Probe {
        supervisor: OperationSupervisor,
        lock_timeouts: Arc<AtomicUsize>,
    }

    fn check_reentry(probe: &Probe) {
        let supervisor = probe.supervisor.clone();
        let (send, receive) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = supervisor.active_roots();
            let _ = send.send(());
        });
        if receive.recv_timeout(Duration::from_millis(100)).is_err() {
            probe.lock_timeouts.fetch_add(1, Ordering::SeqCst);
        }
    }

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        let probe = unsafe { Arc::<Probe>::from_raw(data.cast()) };
        check_reentry(&probe);
        let clone = Arc::clone(&probe);
        std::mem::forget(probe);
        RawWaker::new(Arc::into_raw(clone).cast(), &VTABLE)
    }

    unsafe fn wake_waker(data: *const ()) {
        unsafe { drop_waker(data) };
    }

    unsafe fn wake_by_ref(data: *const ()) {
        let probe = unsafe { &*data.cast::<Probe>() };
        check_reentry(probe);
    }

    unsafe fn drop_waker(data: *const ()) {
        let probe = unsafe { Arc::<Probe>::from_raw(data.cast()) };
        check_reentry(&probe);
        drop(probe);
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref, drop_waker);

    fn reentrant_waker(supervisor: &OperationSupervisor, lock_timeouts: &Arc<AtomicUsize>) -> Waker {
        let probe = Arc::new(Probe {
            supervisor: supervisor.clone(),
            lock_timeouts: Arc::clone(lock_timeouts),
        });
        let raw = RawWaker::new(Arc::into_raw(probe).cast(), &VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    #[test]
    fn raw_waker_clone_replacement_and_removal_can_reenter_without_lock() {
        let supervisor = OperationSupervisor::new(SupervisorLimits {
            active_roots: 0,
            queued_roots: 0,
            unobserved_results: 1,
            ..SupervisorLimits::default()
        });
        let lock_timeouts = Arc::new(AtomicUsize::new(0));
        let first_waker = reentrant_waker(&supervisor, &lock_timeouts);
        let second_waker = reentrant_waker(&supervisor, &lock_timeouts);
        let mut first_cx = Context::from_waker(&first_waker);
        let mut second_cx = Context::from_waker(&second_waker);
        let mut waiter = Box::pin(supervisor.start_wait_body(()).unwrap());
        assert!(matches!(waiter.as_mut().poll(&mut first_cx), Poll::Pending));
        // Different RawWaker data forces a replacement and destruction of the old
        // waker.
        assert!(matches!(waiter.as_mut().poll(&mut second_cx), Poll::Pending));
        drop(waiter);
        assert_eq!(lock_timeouts.load(Ordering::SeqCst), 0);
    }
}

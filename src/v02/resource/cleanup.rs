//! Bounded, per-scope handoff queue for resource cleanup.
#![allow(dead_code)] // The manager and scope assembly is completed by T11.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::{self};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::thread::JoinHandle;
use std::thread::{self};

use super::cleanup_port::CleanupDispatch;
use super::cleanup_port::CleanupDispatchPermit;
use super::cleanup_port::CleanupJob;
use super::cleanup_port::CleanupQueueError;
use crate::v02::context::CleanupContext;
use crate::v02::executor::CleanupExecutor;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::report::CleanupEvidence;
use crate::v02::report::CleanupFailure;
use crate::v02::report::ManagerId;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::TransferReceipt;

pub type CleanupFuture<T> = Pin<Box<dyn Future<Output = CleanupOutcome<T>> + Send + 'static>>;

/// Explicit asynchronous cleanup policy. A failed result returns ownership of
/// the resource, while the retained `Arc<Self>` remains available for retry.
pub trait AsyncCleanup<T: Send + Sync + 'static>: Send + Sync + 'static {
    fn cleanup(self: Arc<Self>, resource: T, context: CleanupContext) -> CleanupFuture<T>;
}

impl<T, C> AsyncCleanup<T> for Arc<C>
where
    T: Send + Sync + 'static,
    C: AsyncCleanup<T> + ?Sized,
{
    fn cleanup(self: Arc<Self>, resource: T, context: CleanupContext) -> CleanupFuture<T> {
        Arc::clone(self.as_ref()).cleanup(resource, context)
    }
}

pub enum CleanupOutcome<T> {
    Released,
    Transferred(TransferReceipt),
    Failed { failure: CleanupFailure, resource: T },
}

/// Default-drop owner. T05's blocking executor owns this value until its
/// destructor has returned, while the business resource registration remains
/// live for the same interval.
pub(crate) struct DefaultDropJob<T: Send + 'static> {
    resource: Option<T>,
    registration: Option<ResourceRegistration>,
    completed: Option<Box<dyn FnOnce(Result<CleanupEvidence, UnconfirmedCleanup>) + Send + 'static>>,
}

/// Retained business capacity and panic evidence after a destructor fails.
#[must_use = "unconfirmed cleanup responsibility must remain owned"]
pub(crate) struct UnconfirmedCleanup {
    registration: Option<ResourceRegistration>,
    panic: Box<dyn std::any::Any + Send + 'static>,
    capsule: Option<Box<dyn std::any::Any + Send + 'static>>,
    policy: Option<Box<dyn std::any::Any + Send + Sync + 'static>>,
}

impl UnconfirmedCleanup {
    pub(crate) fn from_panic(
        registration: Option<ResourceRegistration>,
        panic: Box<dyn std::any::Any + Send + 'static>,
    ) -> Self {
        Self {
            registration,
            panic,
            capsule: None,
            policy: None,
        }
    }

    pub(crate) fn panic_payload(&self) -> &(dyn std::any::Any + Send + 'static) {
        self.panic.as_ref()
    }

    pub(crate) fn take_registration(&mut self) -> Option<ResourceRegistration> {
        self.registration.take()
    }
}

impl Drop for UnconfirmedCleanup {
    fn drop(&mut self) {
        // No release evidence exists. If the recovery owner itself is
        // accidentally discarded, leak its opaque capsule and permits rather
        // than running resource or policy destructors on the dropping thread.
        if let Some(capsule) = self.capsule.take() {
            std::mem::forget(capsule);
        }
        if let Some(policy) = self.policy.take() {
            std::mem::forget(policy);
        }
        if let Some(registration) = self.registration.take() {
            std::mem::forget(registration);
        }
    }
}

impl<T: Send + 'static> DefaultDropJob<T> {
    pub(crate) fn new(
        resource: T,
        registration: Option<ResourceRegistration>,
        completed: impl FnOnce(Result<CleanupEvidence, UnconfirmedCleanup>) + Send + 'static,
    ) -> Self {
        Self {
            resource: Some(resource),
            registration,
            completed: Some(Box::new(completed)),
        }
    }
}

impl<T: Send + 'static> CleanupJob for DefaultDropJob<T> {
    fn schedule(self: Box<Self>, executor: Arc<CleanupExecutor>) -> Result<(), Box<dyn CleanupJob>> {
        match executor.reserve_drop() {
            Ok(permit) => {
                permit.drop_value(self);
                Ok(())
            }
            Err(_) => Err(self),
        }
    }
}

impl<T: Send + 'static> Drop for DefaultDropJob<T> {
    fn drop(&mut self) {
        let resource = self.resource.take();
        let drop_result = panic::catch_unwind(AssertUnwindSafe(|| drop(resource)));
        if let Some(completed) = self.completed.take() {
            match drop_result {
                Ok(()) => {
                    drop(self.registration.take());
                    completed(Ok(CleanupEvidence::DropCompleted));
                }
                Err(panic) => completed(Err(UnconfirmedCleanup {
                    registration: self.registration.take(),
                    panic,
                    capsule: None,
                    policy: None,
                })),
            }
        }
    }
}

pub(crate) struct AsyncCleanupCompletion<T: Send + Sync + 'static> {
    pub(crate) outcome: Option<CleanupOutcome<T>>,
    pub(crate) evidence: Option<CleanupEvidence>,
    pub(crate) policy: Arc<dyn AsyncCleanup<T>>,
    pub(crate) registration: Option<ResourceRegistration>,
    pub(crate) unconfirmed: Option<UnconfirmedCleanup>,
}

/// Async cleanup job scheduled through the same bounded dispatcher as
/// default drops. Failed results preserve the exact resource and policy.
pub(crate) struct AsyncCleanupJob<T: Send + Sync + 'static> {
    resource: Option<T>,
    policy: Arc<dyn AsyncCleanup<T>>,
    context: Option<CleanupContext>,
    registration: Option<ResourceRegistration>,
    completion: Arc<Mutex<Option<AsyncCleanupCompletion<T>>>>,
    callback: Option<Box<dyn FnOnce(AsyncCleanupCompletion<T>) + Send + 'static>>,
}

impl<T: Send + Sync + 'static> AsyncCleanupJob<T> {
    pub(crate) fn new(
        resource: T,
        policy: Arc<dyn AsyncCleanup<T>>,
        context: CleanupContext,
        completion: Arc<Mutex<Option<AsyncCleanupCompletion<T>>>>,
    ) -> Self {
        Self {
            resource: Some(resource),
            policy,
            context: Some(context),
            registration: None,
            completion,
            callback: None,
        }
    }

    pub(crate) fn with_registration(mut self, registration: ResourceRegistration) -> Self {
        self.registration = Some(registration);
        self
    }

    pub(crate) fn with_callback(mut self, callback: impl FnOnce(AsyncCleanupCompletion<T>) + Send + 'static) -> Self {
        self.callback = Some(Box::new(callback));
        self
    }
}

impl<T: Send + Sync + 'static> CleanupJob for AsyncCleanupJob<T> {
    fn schedule(mut self: Box<Self>, executor: Arc<CleanupExecutor>) -> Result<(), Box<dyn CleanupJob>> {
        let permit = match executor.reserve_async() {
            Ok(permit) => permit,
            Err(_) => return Err(self),
        };
        permit.spawn(AsyncCleanupTask {
            resource: self.resource.take(),
            context: self.context.take(),
            policy: Some(Arc::clone(&self.policy)),
            future: None,
            registration: self.registration.take(),
            completion: Arc::clone(&self.completion),
            callback: self.callback.take(),
            finished: false,
        });
        Ok(())
    }
}

struct AsyncCleanupTask<T: Send + Sync + 'static> {
    resource: Option<T>,
    context: Option<CleanupContext>,
    policy: Option<Arc<dyn AsyncCleanup<T>>>,
    future: Option<CleanupFuture<T>>,
    registration: Option<ResourceRegistration>,
    completion: Arc<Mutex<Option<AsyncCleanupCompletion<T>>>>,
    finished: bool,
    callback: Option<Box<dyn FnOnce(AsyncCleanupCompletion<T>) + Send + 'static>>,
}

impl<T: Send + Sync + 'static> Unpin for AsyncCleanupTask<T> {}

impl<T: Send + Sync + 'static> Future for AsyncCleanupTask<T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(());
        }
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            if this.future.is_none() {
                let resource = this.resource.take().expect("resource is present");
                let cleanup_context = this.context.take().expect("cleanup context is present");
                let policy = Arc::clone(this.policy.as_ref().expect("policy is present"));
                this.future = Some(policy.cleanup(resource, cleanup_context));
            }
            this.future
                .as_mut()
                .expect("future was initialized")
                .as_mut()
                .poll(context)
        }));
        match outcome {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(outcome)) => {
                let evidence = matches!(&outcome, CleanupOutcome::Released).then_some(CleanupEvidence::Confirmed);
                let policy = this.policy.take().expect("policy is present");
                let completion = AsyncCleanupCompletion {
                    outcome: Some(outcome),
                    evidence,
                    policy,
                    registration: this.registration.take(),
                    unconfirmed: None,
                };
                if let Some(callback) = this.callback.take() {
                    callback(completion);
                } else {
                    *lock(&this.completion) = Some(completion);
                }
                this.future.take();
                this.finished = true;
                Poll::Ready(())
            }
            Err(panic) => {
                let capsule = this
                    .future
                    .take()
                    .map(|future| Box::new(future) as Box<dyn std::any::Any + Send>);
                let policy = this.policy.take().expect("policy is present");
                let policy_capsule: Box<dyn std::any::Any + Send + Sync> = Box::new(Arc::clone(&policy));
                let completion = AsyncCleanupCompletion {
                    outcome: None,
                    evidence: None,
                    policy,
                    registration: None,
                    unconfirmed: Some(UnconfirmedCleanup {
                        registration: this.registration.take(),
                        panic,
                        capsule,
                        policy: Some(policy_capsule),
                    }),
                };
                if let Some(callback) = this.callback.take() {
                    callback(completion);
                } else {
                    *lock(&this.completion) = Some(completion);
                }
                this.finished = true;
                Poll::Ready(())
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupDispatcherError {
    ZeroCapacity,
}

struct QueueState {
    accepting: bool,
    /// Reservations include uncommitted resources and jobs still pending in
    /// this queue. Each registered resource owns at most one reservation.
    reserved: usize,
    pending: VecDeque<Box<dyn CleanupJob>>,
}

/// Per-resource-scope pending queue backed by the manager's T05 cleanup
/// executor. The queue is bounded by that scope's effective resource limit;
/// worker concurrency remains independently bounded by `CleanupExecutor`.
pub(crate) struct CleanupDispatcher {
    inner: Arc<CleanupDispatcherInner>,
}

/// Event-driven scheduler for pending per-scope dispatchers. The index is
/// weak and contains only scopes with queued cleanup work.
pub(crate) struct CleanupPump {
    inner: Arc<CleanupPumpInner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

struct CleanupPumpInner {
    state: Mutex<CleanupPumpState>,
    changed: Condvar,
}

struct CleanupPumpState {
    stopping: bool,
    signaled: bool,
    next_id: u64,
    dispatchers: HashMap<u64, Weak<CleanupDispatcherInner>>,
}

impl CleanupPump {
    pub(crate) fn new() -> std::io::Result<Arc<Self>> {
        let inner = Arc::new(CleanupPumpInner {
            state: Mutex::new(CleanupPumpState {
                stopping: false,
                signaled: false,
                next_id: 1,
                dispatchers: HashMap::new(),
            }),
            changed: Condvar::new(),
        });
        let worker_inner = Arc::clone(&inner);
        let worker = thread::Builder::new()
            .name("qubit-operation-cleanup-pump".into())
            .spawn(move || cleanup_pump_worker(worker_inner))?;
        Ok(Arc::new(Self {
            inner,
            worker: Mutex::new(Some(worker)),
        }))
    }

    fn register(&self, dispatcher: &Arc<CleanupDispatcherInner>) {
        let mut registration = lock(&dispatcher.pump_registration);
        if registration.is_some() {
            return;
        }
        let id = {
            let mut state = lock(&self.inner.state);
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1).max(1);
            state.dispatchers.insert(id, Arc::downgrade(dispatcher));
            id
        };
        *registration = Some(id);
    }

    fn unregister(&self, dispatcher: &Arc<CleanupDispatcherInner>) {
        let id = lock(&dispatcher.pump_registration).take();
        if let Some(id) = id {
            lock(&self.inner.state).dispatchers.remove(&id);
        }
    }

    pub(crate) fn kick(&self) {
        let mut state = lock(&self.inner.state);
        state.signaled = true;
        drop(state);
        self.inner.changed.notify_one();
    }

    pub(crate) fn shutdown(&self) {
        {
            let mut state = lock(&self.inner.state);
            state.stopping = true;
            state.signaled = true;
        }
        self.inner.changed.notify_all();
        if let Some(worker) = lock(&self.worker).take() {
            let _ = worker.join();
        }
    }
}

impl Drop for CleanupPump {
    fn drop(&mut self) {
        {
            let mut state = lock(&self.inner.state);
            state.stopping = true;
            state.signaled = true;
        }
        self.inner.changed.notify_all();
        // Public manager Drop detaches this handle; explicit shutdown joins.
        lock(&self.worker).take();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupTransferError {
    Closed,
    Full,
    WrongManager,
    SameScope,
    Duplicate,
}

static NEXT_CLEANUP_TRANSFER_NONCE: AtomicU64 = AtomicU64::new(1);

struct TransferQueueState {
    accepting: bool,
    reserved: usize,
    pending: VecDeque<(ResourceId, Box<dyn CleanupJob>)>,
    ids: HashSet<ResourceId>,
}

/// A bounded receiver for owners explicitly transferred during cleanup.
/// The receipt is minted only after the receiver has stored the real job.
pub(crate) struct CleanupTransferReceiver {
    manager: ManagerId,
    scope: ResourceScopeId,
    capacity: usize,
    executor: Arc<CleanupExecutor>,
    state: Mutex<TransferQueueState>,
    driver: Mutex<()>,
    receipt_callback: Option<Arc<dyn Fn(TransferReceipt) + Send + Sync + 'static>>,
}

impl Drop for CleanupTransferReceiver {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, job) in state.pending.drain(..) {
            // Accepted work must remain owned by a live manager/receiver.
            // Preserve it if the receiver is dropped prematurely.
            std::mem::forget(job);
        }
    }
}

impl CleanupTransferReceiver {
    pub(crate) fn new(
        manager: ManagerId,
        scope: ResourceScopeId,
        capacity: usize,
        executor: Arc<CleanupExecutor>,
    ) -> Result<Self, CleanupDispatcherError> {
        Self::new_inner(manager, scope, capacity, executor, None)
    }

    pub(crate) fn new_with_receipt_callback(
        manager: ManagerId,
        scope: ResourceScopeId,
        capacity: usize,
        executor: Arc<CleanupExecutor>,
        callback: impl Fn(TransferReceipt) + Send + Sync + 'static,
    ) -> Result<Self, CleanupDispatcherError> {
        Self::new_inner(manager, scope, capacity, executor, Some(Arc::new(callback)))
    }

    fn new_inner(
        manager: ManagerId,
        scope: ResourceScopeId,
        capacity: usize,
        executor: Arc<CleanupExecutor>,
        receipt_callback: Option<Arc<dyn Fn(TransferReceipt) + Send + Sync + 'static>>,
    ) -> Result<Self, CleanupDispatcherError> {
        if capacity == 0 {
            return Err(CleanupDispatcherError::ZeroCapacity);
        }
        Ok(Self {
            manager,
            scope,
            capacity,
            executor,
            state: Mutex::new(TransferQueueState {
                accepting: true,
                reserved: 0,
                pending: VecDeque::with_capacity(capacity),
                ids: HashSet::with_capacity(capacity),
            }),
            driver: Mutex::new(()),
            receipt_callback,
        })
    }

    pub(crate) fn accept(
        &self,
        resource: ResourceId,
        source_scope: ResourceScopeId,
        job: Box<dyn CleanupJob>,
    ) -> Result<TransferReceipt, (CleanupTransferError, Box<dyn CleanupJob>)> {
        if resource.manager() != self.manager {
            return Err((CleanupTransferError::WrongManager, job));
        }
        if source_scope == self.scope {
            return Err((CleanupTransferError::SameScope, job));
        }
        let mut state = lock(&self.state);
        if !state.accepting {
            return Err((CleanupTransferError::Closed, job));
        }
        if state.reserved >= self.capacity {
            return Err((CleanupTransferError::Full, job));
        }
        if state.ids.contains(&resource) {
            return Err((CleanupTransferError::Duplicate, job));
        }
        // The identity and owner enter receiver custody atomically before a
        // receipt can be created.
        state.ids.insert(resource);
        state.pending.push_back((resource, job));
        state.reserved += 1;
        let nonce = NEXT_CLEANUP_TRANSFER_NONCE.fetch_add(1, Ordering::Relaxed);
        let receipt = TransferReceipt::new(resource, source_scope, self.scope, nonce);
        drop(state);
        if let Some(callback) = &self.receipt_callback {
            let receipt_for_callback = receipt.clone();
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(receipt_for_callback)));
        }
        Ok(receipt)
    }

    pub(crate) fn drive(&self, limit: usize) -> usize {
        let _driver = lock(&self.driver);
        let mut scheduled = 0;
        while scheduled < limit {
            let Some((resource, job)) = lock(&self.state).pending.pop_front() else {
                break;
            };
            match job.schedule(Arc::clone(&self.executor)) {
                Ok(()) => {
                    let mut state = lock(&self.state);
                    state.ids.remove(&resource);
                    state.reserved -= 1;
                    scheduled += 1;
                }
                Err(job) => {
                    lock(&self.state).pending.push_front((resource, job));
                    break;
                }
            }
        }
        scheduled
    }

    pub(crate) fn close_admission(&self) {
        lock(&self.state).accepting = false;
    }

    pub(crate) fn contains(&self, resource: ResourceId) -> bool {
        lock(&self.state).ids.contains(&resource)
    }
}

struct CleanupDispatcherInner {
    capacity: usize,
    executor: Arc<CleanupExecutor>,
    state: Mutex<QueueState>,
    driver: Mutex<()>,
    pump: Option<Arc<CleanupPump>>,
    pump_registration: Mutex<Option<u64>>,
}

impl Drop for CleanupDispatcherInner {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        for job in state.pending.drain(..) {
            // A dispatcher must be retained by its manager while pending is
            // nonzero. Preserve owners if that lifetime invariant is broken.
            std::mem::forget(job);
        }
        if let (Some(pump), Some(id)) = (&self.pump, *lock(&self.pump_registration)) {
            lock(&pump.inner.state).dispatchers.remove(&id);
        }
    }
}

fn cleanup_pump_worker(inner: Arc<CleanupPumpInner>) {
    loop {
        let dispatchers = {
            let mut state = lock(&inner.state);
            while !state.signaled && !state.stopping {
                state = inner
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            if state.stopping {
                return;
            }
            state.signaled = false;
            state
                .dispatchers
                .iter()
                .map(|(id, dispatcher)| (*id, dispatcher.clone()))
                .collect::<Vec<_>>()
        };

        for (id, weak) in dispatchers {
            if let Some(dispatcher) = weak.upgrade() {
                CleanupDispatcher::drive_inner(&dispatcher, dispatcher.capacity);
            } else {
                lock(&inner.state).dispatchers.remove(&id);
            }
        }
    }
}

impl CleanupDispatcher {
    pub(crate) fn new(
        scope_resource_capacity: usize,
        executor: Arc<CleanupExecutor>,
    ) -> Result<Self, CleanupDispatcherError> {
        Self::new_inner(scope_resource_capacity, executor, None)
    }

    pub(crate) fn new_with_pump(
        scope_resource_capacity: usize,
        executor: Arc<CleanupExecutor>,
        pump: Arc<CleanupPump>,
    ) -> Result<Self, CleanupDispatcherError> {
        Self::new_inner(scope_resource_capacity, executor, Some(pump))
    }

    fn new_inner(
        scope_resource_capacity: usize,
        executor: Arc<CleanupExecutor>,
        pump: Option<Arc<CleanupPump>>,
    ) -> Result<Self, CleanupDispatcherError> {
        if scope_resource_capacity == 0 {
            return Err(CleanupDispatcherError::ZeroCapacity);
        }
        Ok(Self {
            inner: Arc::new(CleanupDispatcherInner {
                capacity: scope_resource_capacity,
                executor,
                state: Mutex::new(QueueState {
                    accepting: true,
                    reserved: 0,
                    pending: VecDeque::with_capacity(scope_resource_capacity),
                }),
                driver: Mutex::new(()),
                pump,
                pump_registration: Mutex::new(None),
            }),
        })
    }

    /// Schedules up to `limit` pending jobs. If the T05 executor has no slot,
    /// the first rejected job remains at the front of the queue with its
    /// resource owner intact. T11 drives this method from manager progress and
    /// shutdown polling; the method never blocks or invokes user cleanup
    /// inline.
    pub(crate) fn drive(&self, limit: usize) -> usize {
        Self::drive_inner(&self.inner, limit)
    }

    fn drive_inner(inner: &Arc<CleanupDispatcherInner>, limit: usize) -> usize {
        let _driver = lock(&inner.driver);
        let mut scheduled = 0;
        while scheduled < limit {
            let Some(job) = lock(&inner.state).pending.pop_front() else {
                break;
            };
            match job.schedule(Arc::clone(&inner.executor)) {
                Ok(()) => {
                    let mut state = lock(&inner.state);
                    debug_assert!(state.reserved > 0);
                    state.reserved -= 1;
                    scheduled += 1;
                }
                Err(job) => {
                    lock(&inner.state).pending.push_front(job);
                    break;
                }
            }
        }
        if lock(&inner.state).pending.is_empty()
            && let Some(pump) = &inner.pump
        {
            pump.unregister(inner);
        }
        scheduled
    }

    /// Stops new scope registrations. Previously reserved permits remain
    /// valid and can enqueue after close, and `drive` continues to drain them.
    pub(crate) fn close_admission(&self) {
        lock(&self.inner.state).accepting = false;
    }

    pub(crate) fn pending_count(&self) -> usize {
        lock(&self.inner.state).pending.len()
    }

    pub(crate) fn reserved_count(&self) -> usize {
        lock(&self.inner.state).reserved
    }

    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

impl CleanupDispatch for CleanupDispatcher {
    fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
        let mut state = lock(&self.inner.state);
        if !state.accepting {
            return Err(CleanupQueueError::Closed);
        }
        if state.reserved >= self.inner.capacity {
            return Err(CleanupQueueError::Full);
        }
        state.reserved += 1;
        Ok(Box::new(DispatchPermit {
            inner: Arc::clone(&self.inner),
            active: true,
        }))
    }
}

struct DispatchPermit {
    inner: Arc<CleanupDispatcherInner>,
    active: bool,
}

impl CleanupDispatchPermit for DispatchPermit {
    fn enqueue(mut self: Box<Self>, job: Box<dyn CleanupJob>) {
        {
            let mut state = lock(&self.inner.state);
            debug_assert!(self.active);
            debug_assert!(state.pending.len() < self.inner.capacity);
            state.pending.push_back(job);
            self.active = false;
        }
        if let Some(pump) = &self.inner.pump {
            pump.register(&self.inner);
            pump.kick();
        }
        CleanupDispatcher::drive_inner(&self.inner, self.inner.capacity);
    }
}

impl Drop for DispatchPermit {
    fn drop(&mut self) {
        if self.active {
            let mut state = lock(&self.inner.state);
            state.reserved -= 1;
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::AsyncCleanup;
    use super::AsyncCleanupJob;
    use super::CleanupDispatch;
    use super::CleanupDispatcher;
    use super::CleanupJob;
    use super::CleanupOutcome;
    use super::CleanupTransferError;
    use super::CleanupTransferReceiver;
    use super::DefaultDropJob;
    use crate::v02::executor::CleanupExecutor;
    use crate::v02::report::CleanupFailure;
    use crate::v02::report::CleanupStage;
    use crate::v02::report::ContextOptions;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::ManagerId;
    use crate::v02::report::PortableExecutorOptions;
    use crate::v02::report::ResourceId;
    use crate::v02::report::ResourceScopeId;
    use crate::v02::report::ThreadPoolExecutorOptions;

    fn executor() -> Arc<CleanupExecutor> {
        Arc::new(
            CleanupExecutor::new(
                ThreadPoolExecutorOptions {
                    worker_count: 1,
                    queue_capacity: 1,
                },
                PortableExecutorOptions {
                    worker_count: 1,
                    task_capacity: 2,
                    polls_per_turn: 2,
                },
            )
            .unwrap(),
        )
    }

    struct FailCleanup;
    impl AsyncCleanup<usize> for FailCleanup {
        fn cleanup(
            self: Arc<Self>,
            resource: usize,
            _context: crate::v02::context::CleanupContext,
        ) -> super::CleanupFuture<usize> {
            Box::pin(std::future::ready(CleanupOutcome::Failed {
                failure: CleanupFailure {
                    resource_id: ResourceId::new(ManagerId::new_unique(), 1, 1),
                    stage: CleanupStage::AsyncRelease,
                    summary: DiagnosticSummary::new("retry required"),
                    source: None,
                },
                resource,
            }))
        }
    }

    struct ConfirmCleanup;
    impl AsyncCleanup<usize> for ConfirmCleanup {
        fn cleanup(
            self: Arc<Self>,
            _resource: usize,
            _context: crate::v02::context::CleanupContext,
        ) -> super::CleanupFuture<usize> {
            Box::pin(std::future::ready(CleanupOutcome::Released))
        }
    }

    struct PanicCleanup;
    impl AsyncCleanup<usize> for PanicCleanup {
        fn cleanup(
            self: Arc<Self>,
            resource: usize,
            _context: crate::v02::context::CleanupContext,
        ) -> super::CleanupFuture<usize> {
            Box::pin(async move {
                std::hint::black_box(&resource);
                panic!("async cleanup panic");
            })
        }
    }

    struct DropOnSchedule(mpsc::Sender<String>);
    impl Drop for DropOnSchedule {
        fn drop(&mut self) {
            self.0
                .send(std::thread::current().name().unwrap_or("unnamed").into())
                .ok();
        }
    }
    impl CleanupJob for DropOnSchedule {
        fn schedule(self: Box<Self>, executor: Arc<CleanupExecutor>) -> Result<(), Box<dyn CleanupJob>> {
            match executor.reserve_drop() {
                Ok(permit) => {
                    permit.drop_value(self);
                    Ok(())
                }
                Err(_) => Err(self),
            }
        }
    }

    #[test]
    fn default_drop_runs_on_cleanup_worker() {
        let dispatcher = CleanupDispatcher::new(1, executor()).unwrap();
        let (drop_send, drop_receive) = mpsc::channel();
        let (complete_send, complete_receive) = mpsc::channel();
        struct Probe(mpsc::Sender<()>);
        impl Drop for Probe {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let job = DefaultDropJob::new(Probe(drop_send), None, move |evidence| {
            let thread = std::thread::current().name().unwrap_or("unnamed").to_owned();
            if let Ok(evidence) = evidence {
                let _ = complete_send.send((evidence, thread));
            }
        });
        dispatcher.reserve().unwrap().enqueue(Box::new(job));
        // Enqueue immediately kicks the dispatcher; the explicit drive sees
        // no remaining pending work.
        assert_eq!(dispatcher.drive(1), 0);
        drop_receive.recv_timeout(Duration::from_secs(1)).unwrap();
        let (evidence, thread_name) = complete_receive.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(evidence, crate::v02::report::CleanupEvidence::DropCompleted);
        assert!(thread_name.starts_with("qubit-operation-"));
    }

    #[test]
    fn panicking_default_drop_is_unconfirmed() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("cleanup panic");
            }
        }
        let executor = executor();
        let (send, receive) = mpsc::channel();
        let job = DefaultDropJob::new(PanicOnDrop, None, move |result| {
            let _ = send.send(result);
        });
        assert!(Box::new(job).schedule(Arc::clone(&executor)).is_ok());
        let result = receive.recv_timeout(Duration::from_secs(1)).unwrap();
        let Err(mut unconfirmed) = result else {
            panic!("a panicking destructor cannot publish DropCompleted");
        };
        assert!(unconfirmed.panic_payload().is::<&'static str>());
        assert!(unconfirmed.take_registration().is_none());
    }

    #[test]
    fn closed_admission_does_not_discard_reserved_job() {
        let dispatcher = CleanupDispatcher::new(1, executor()).unwrap();
        let permit = dispatcher.reserve().unwrap();
        dispatcher.close_admission();
        let (send, receive) = mpsc::channel();
        permit.enqueue(Box::new(DropOnSchedule(send)));
        assert_eq!(dispatcher.drive(1), 0);
        assert!(receive.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn executor_saturation_keeps_owner_in_pending_queue() {
        struct HoldWorker(mpsc::Receiver<()>);
        impl Drop for HoldWorker {
            fn drop(&mut self) {
                let _ = self.0.recv();
            }
        }
        let executor = executor();
        let (started_send, started_receive) = mpsc::channel();
        let (release_send, release_receive) = mpsc::channel();
        let (release_second, release_second_receive) = mpsc::channel();
        struct SignalThenHold(mpsc::Sender<()>, mpsc::Receiver<()>);
        impl Drop for SignalThenHold {
            fn drop(&mut self) {
                let _ = self.0.send(());
                let _ = self.1.recv();
            }
        }
        executor
            .reserve_drop()
            .unwrap()
            .drop_value(SignalThenHold(started_send, release_receive));
        started_receive.recv_timeout(Duration::from_secs(1)).unwrap();
        executor
            .reserve_drop()
            .unwrap()
            .drop_value(HoldWorker(release_second_receive));

        let dispatcher = CleanupDispatcher::new(1, Arc::clone(&executor)).unwrap();
        let (job_send, job_receive) = mpsc::channel();
        dispatcher
            .reserve()
            .unwrap()
            .enqueue(Box::new(DropOnSchedule(job_send)));
        assert_eq!(dispatcher.drive(1), 0);
        assert_eq!(dispatcher.pending_count(), 1);
        assert_eq!(dispatcher.reserved_count(), 1);
        assert!(job_receive.try_recv().is_err());

        release_send.send(()).unwrap();
        release_second.send(()).unwrap();
        for _ in 0..1_000 {
            if dispatcher.drive(1) == 1 {
                break;
            }
            std::thread::yield_now();
        }
        assert!(job_receive.recv_timeout(Duration::from_secs(1)).is_ok());
        assert_eq!(dispatcher.pending_count(), 0);
        assert_eq!(dispatcher.reserved_count(), 0);
    }

    #[test]
    fn async_failure_preserves_t_and_policy() {
        let executor = executor();
        let policy = Arc::new(FailCleanup);
        let policy_erased: Arc<dyn AsyncCleanup<usize>> = Arc::clone(&policy) as Arc<_>;
        let completion = Arc::new(std::sync::Mutex::new(None));
        let job = AsyncCleanupJob::new(
            41usize,
            Arc::clone(&policy_erased),
            crate::v02::context::CleanupContext::new(ContextOptions::default()).unwrap(),
            Arc::clone(&completion),
        );
        assert!(Box::new(job).schedule(Arc::clone(&executor)).is_ok());
        assert_eq!(executor.poll_async_turn(), 1);

        let completion = completion.lock().unwrap().take().unwrap();
        assert!(Arc::ptr_eq(&completion.policy, &policy_erased));
        assert_eq!(completion.evidence, None);
        match completion.outcome.unwrap() {
            CleanupOutcome::Failed { resource, .. } => assert_eq!(resource, 41),
            _ => panic!("failed cleanup must retain the resource"),
        }
    }

    #[test]
    fn async_executor_saturation_keeps_resource_and_policy_queued() {
        let executor = executor();
        executor.reserve_async().unwrap().spawn(async {});
        executor.reserve_async().unwrap().spawn(async {});
        let policy: Arc<dyn AsyncCleanup<usize>> = Arc::new(FailCleanup);
        let completion = Arc::new(std::sync::Mutex::new(None));
        let dispatcher = CleanupDispatcher::new(1, Arc::clone(&executor)).unwrap();
        let job = AsyncCleanupJob::new(
            73,
            Arc::clone(&policy),
            crate::v02::context::CleanupContext::new(ContextOptions::default()).unwrap(),
            Arc::clone(&completion),
        );
        dispatcher.reserve().unwrap().enqueue(Box::new(job));
        assert_eq!(dispatcher.drive(1), 0);
        assert_eq!(dispatcher.pending_count(), 1);
        assert_eq!(dispatcher.reserved_count(), 1);
        executor.poll_async_turn();
        assert_eq!(dispatcher.drive(1), 1);
        executor.poll_async_turn();
        assert_eq!(dispatcher.pending_count(), 0);
        let result = completion.lock().unwrap().take().unwrap();
        assert!(Arc::ptr_eq(&result.policy, &policy));
        match result.outcome.unwrap() {
            CleanupOutcome::Failed { resource, .. } => assert_eq!(resource, 73),
            _ => panic!("failed cleanup must retain its resource"),
        }
    }

    #[test]
    fn async_success_reports_confirmation() {
        let executor = executor();
        let policy: Arc<dyn AsyncCleanup<usize>> = Arc::new(ConfirmCleanup);
        let completion = Arc::new(std::sync::Mutex::new(None));
        let job = AsyncCleanupJob::new(
            19,
            Arc::clone(&policy),
            crate::v02::context::CleanupContext::new(ContextOptions::default()).unwrap(),
            Arc::clone(&completion),
        );
        assert!(Box::new(job).schedule(Arc::clone(&executor)).is_ok());
        executor.poll_async_turn();
        let completion = completion.lock().unwrap().take().unwrap();
        assert!(matches!(completion.outcome, Some(CleanupOutcome::Released)));
        assert_eq!(
            completion.evidence,
            Some(crate::v02::report::CleanupEvidence::Confirmed)
        );
    }

    #[test]
    fn cleanup_transfer_requires_receiver_acceptance() {
        let manager = ManagerId::new_unique();
        let source_scope = ResourceScopeId::new_unique();
        let receiver_scope = ResourceScopeId::new_unique();
        let first_receiver = CleanupTransferReceiver::new(manager, receiver_scope, 1, executor()).unwrap();
        let first_id = ResourceId::new(manager, 1, 1);
        assert!(
            first_receiver
                .accept(first_id, source_scope, Box::new(DropOnSchedule(mpsc::channel().0)),)
                .is_ok()
        );

        let (drop_send, drop_receive) = mpsc::channel();
        struct Probe(mpsc::Sender<()>);
        impl Drop for Probe {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let second_id = ResourceId::new(manager, 2, 1);
        let job: Box<dyn CleanupJob> = Box::new(DefaultDropJob::new(Probe(drop_send), None, |_| {}));
        let (error, returned_job) = match first_receiver.accept(second_id, source_scope, job) {
            Ok(_) => panic!("a full receiver must not sign a transfer"),
            Err(failure) => failure,
        };
        assert_eq!(error, CleanupTransferError::Full);
        assert!(drop_receive.try_recv().is_err());

        let second_receiver =
            CleanupTransferReceiver::new(manager, ResourceScopeId::new_unique(), 1, executor()).unwrap();
        let receipt = match second_receiver.accept(second_id, source_scope, returned_job) {
            Ok(receipt) => receipt,
            Err(_) => panic!("open receiver with capacity must accept owner"),
        };
        assert_eq!(receipt.resource(), second_id);
        assert!(second_receiver.contains(second_id));
        assert_eq!(second_receiver.drive(1), 1);
        drop_receive.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(first_receiver.drive(1), 1);
    }

    #[test]
    fn cleanup_transfer_callback_runs_after_acceptance_outside_queue_lock() {
        let manager = ManagerId::new_unique();
        let source_scope = ResourceScopeId::new_unique();
        let receiver_scope = ResourceScopeId::new_unique();
        let holder = Arc::new(std::sync::Mutex::new(None::<Arc<CleanupTransferReceiver>>));
        let callback_holder = Arc::clone(&holder);
        let callbacks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callback_count = Arc::clone(&callbacks);
        let receiver = Arc::new(
            CleanupTransferReceiver::new_with_receipt_callback(
                manager,
                receiver_scope,
                1,
                executor(),
                move |receipt| {
                    let receiver = callback_holder.lock().unwrap().as_ref().unwrap().clone();
                    assert!(receiver.contains(receipt.resource()));
                    callback_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                },
            )
            .unwrap(),
        );
        *holder.lock().unwrap() = Some(Arc::clone(&receiver));
        let resource = ResourceId::new(manager, 19, 1);
        let receipt = match receiver.accept(resource, source_scope, Box::new(DropOnSchedule(mpsc::channel().0))) {
            Ok(receipt) => receipt,
            Err(_) => panic!("open receiver should accept transfer"),
        };
        assert_eq!(receipt.resource(), resource);
        assert_eq!(callbacks.load(std::sync::atomic::Ordering::SeqCst), 1);

        let second = ResourceId::new(manager, 20, 1);
        let job: Box<dyn CleanupJob> = Box::new(DropOnSchedule(mpsc::channel().0));
        let (error, _job) = receiver.accept(second, source_scope, job).unwrap_err();
        assert_eq!(error, CleanupTransferError::Full);
        assert_eq!(callbacks.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn cleanup_transfer_callback_panic_does_not_revoke_acceptance() {
        let manager = ManagerId::new_unique();
        let source_scope = ResourceScopeId::new_unique();
        let receiver = CleanupTransferReceiver::new_with_receipt_callback(
            manager,
            ResourceScopeId::new_unique(),
            1,
            executor(),
            |_| panic!("observer callback panic"),
        )
        .unwrap();
        let resource = ResourceId::new(manager, 21, 1);
        let receipt = match receiver.accept(resource, source_scope, Box::new(DropOnSchedule(mpsc::channel().0))) {
            Ok(receipt) => receipt,
            Err(_) => panic!("open receiver should accept transfer"),
        };
        assert_eq!(receipt.resource(), resource);
        assert!(receiver.contains(resource));
    }

    #[test]
    fn unconfirmed_async_panic_keeps_owner_and_has_no_evidence() {
        let executor = executor();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resource = ProbeResource(Arc::clone(&dropped));
        let policy: Arc<dyn AsyncCleanup<ProbeResource>> = Arc::new(PanicProbeCleanup);
        let completion = Arc::new(std::sync::Mutex::new(None));
        let job = AsyncCleanupJob::new(
            resource,
            Arc::clone(&policy),
            crate::v02::context::CleanupContext::new(ContextOptions::default()).unwrap(),
            Arc::clone(&completion),
        );
        assert!(Box::new(job).schedule(Arc::clone(&executor)).is_ok());
        assert_eq!(executor.poll_async_turn(), 1);
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 0);
        let completion = completion.lock().unwrap().take().unwrap();
        assert!(completion.outcome.is_none());
        assert!(completion.evidence.is_none());
        assert!(completion.unconfirmed.is_some());
        assert!(Arc::ptr_eq(&completion.policy, &policy));
    }

    struct PanicProbeCleanup;
    impl AsyncCleanup<ProbeResource> for PanicProbeCleanup {
        fn cleanup(
            self: Arc<Self>,
            resource: ProbeResource,
            _context: crate::v02::context::CleanupContext,
        ) -> super::CleanupFuture<ProbeResource> {
            Box::pin(PanicFuture(Some(resource)))
        }
    }

    struct PanicFuture(Option<ProbeResource>);
    impl std::future::Future for PanicFuture {
        type Output = CleanupOutcome<ProbeResource>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::hint::black_box(&self.0);
            panic!("async cleanup panic");
        }
    }

    struct ProbeResource(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for ProbeResource {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

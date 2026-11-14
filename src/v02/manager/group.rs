//! Operation groups add a local admission and shutdown boundary.

use super::core::{OperationManager, SubmissionLease};
use crate::v02::operation::{AsyncOperationHandle, OperationHandle, OutcomeDelivery};
use crate::v02::report::{
    AdmissionError, GroupId, GroupOptions, GroupSnapshot, ManagerCreateError, OperationOptions,
    SubmitError,
};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

struct GroupState {
    accepting: bool,
    active: usize,
    mode: crate::v02::report::ShutdownMode,
    scopes: std::collections::HashMap<
        crate::v02::report::OperationId,
        Weak<crate::v02::scope::ScopeCore>,
    >,
    services: std::collections::HashMap<
        crate::v02::report::ResourceScopeId,
        crate::v02::resource::ResourceScope,
    >,
}

/// Shared local admission state inherited by every operation submitted from
/// this group and by manager views bound to those operations.
pub(crate) struct GroupCore {
    id: GroupId,
    capacity: usize,
    state: Mutex<GroupState>,
    changed: Condvar,
    notify: crate::v02::internal::notify::Notify,
}

impl GroupCore {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            id: GroupId::new_unique(),
            capacity,
            state: Mutex::new(GroupState {
                accepting: true,
                active: 0,
                mode: crate::v02::report::ShutdownMode::Drain,
                scopes: std::collections::HashMap::new(),
                services: std::collections::HashMap::new(),
            }),
            changed: Condvar::new(),
            notify: crate::v02::internal::notify::Notify::new(),
        })
    }

    pub(crate) fn reserve(
        self: &Arc<Self>,
        descendant: bool,
    ) -> Result<GroupPermit, AdmissionError> {
        let mut state = lock(&self.state);
        if state.mode == crate::v02::report::ShutdownMode::Cancel {
            return Err(AdmissionError::Stopped);
        }
        if !state.accepting && !descendant {
            return Err(AdmissionError::Closed);
        }
        if state.active >= self.capacity {
            return Err(AdmissionError::Busy);
        }
        state.active += 1;
        Ok(GroupPermit {
            group: Arc::clone(self),
            released: false,
        })
    }

    fn close(&self) {
        lock(&self.state).accepting = false;
    }

    pub(crate) fn register_scope(
        &self,
        id: crate::v02::report::OperationId,
        scope: Weak<crate::v02::scope::ScopeCore>,
    ) -> bool {
        let mut state = lock(&self.state);
        if state.mode == crate::v02::report::ShutdownMode::Cancel {
            return false;
        }
        state.scopes.insert(id, scope);
        true
    }

    pub(crate) fn settlement_finished(&self, id: crate::v02::report::OperationId) {
        lock(&self.state).scopes.remove(&id);
    }

    pub(crate) fn unregister_scope(&self, id: crate::v02::report::OperationId) {
        lock(&self.state).scopes.remove(&id);
    }

    fn begin_shutdown(&self, mode: crate::v02::report::ShutdownMode) {
        let (scopes, services) = {
            let mut state = lock(&self.state);
            state.accepting = false;
            if mode == crate::v02::report::ShutdownMode::Cancel {
                state.mode = mode;
                (
                    state
                        .scopes
                        .values()
                        .filter_map(Weak::upgrade)
                        .collect::<Vec<_>>(),
                    state
                        .services
                        .values()
                        .map(crate::v02::resource::ResourceScope::internal_clone)
                        .collect::<Vec<_>>(),
                )
            } else {
                (
                    Vec::new(),
                    state
                        .services
                        .values()
                        .map(crate::v02::resource::ResourceScope::internal_clone)
                        .collect::<Vec<_>>(),
                )
            }
        };
        for service in services {
            service.end();
        }
        for scope in scopes {
            scope.request_stop(crate::v02::report::StopReason::ManagerShutdown);
        }
        self.changed.notify_all();
        self.notify.notify_waiters();
    }

    fn wait_blocking(&self) {
        let mut state = lock(&self.state);
        while state.active != 0 || !state.services.is_empty() {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    async fn wait(&self) {
        loop {
            let generation = self.notify.generation();
            {
                let state = lock(&self.state);
                if state.active == 0 && state.services.is_empty() {
                    return;
                }
            }
            self.notify.notified_since(generation).await;
        }
    }

    fn snapshot(&self) -> (bool, usize) {
        let state = lock(&self.state);
        (state.accepting, state.active)
    }

    fn public_snapshot(&self) -> GroupSnapshot {
        let (accepting, active_operations) = self.snapshot();
        let service_resource_scopes = lock(&self.state).services.len();
        GroupSnapshot {
            id: self.id,
            accepting,
            active_operations,
            service_resource_scopes,
        }
    }

    fn service_count(&self) -> usize {
        lock(&self.state).services.len()
    }

    fn cleaning_count(&self) -> usize {
        let (operations, services) = {
            let state = lock(&self.state);
            (
                state
                    .scopes
                    .values()
                    .filter_map(Weak::upgrade)
                    .collect::<Vec<_>>(),
                state
                    .services
                    .values()
                    .map(crate::v02::resource::ResourceScope::internal_clone)
                    .collect::<Vec<_>>(),
            )
        };
        operations
            .iter()
            .map(|scope| scope.cleanup_in_flight())
            .sum::<usize>()
            + services
                .iter()
                .map(crate::v02::resource::ResourceScope::cleanup_in_flight)
                .sum::<usize>()
    }

    pub(crate) fn register_service(&self, scope: crate::v02::resource::ResourceScope) -> bool {
        let mut state = lock(&self.state);
        if !state.accepting {
            return false;
        }
        state.services.insert(scope.id(), scope);
        true
    }

    pub(crate) fn service_finished(&self, id: crate::v02::report::ResourceScopeId) {
        lock(&self.state).services.remove(&id);
        self.changed.notify_all();
        self.notify.notify_waiters();
    }
}

pub(crate) struct GroupPermit {
    group: Arc<GroupCore>,
    released: bool,
}

impl Drop for GroupPermit {
    fn drop(&mut self) {
        if !self.released {
            lock(&self.group.state).active -= 1;
            self.released = true;
            self.group.changed.notify_all();
            self.group.notify.notify_waiters();
        }
    }
}

pub(crate) struct GroupLease {
    inner: Arc<dyn SubmissionLease>,
    permit: Mutex<Option<GroupPermit>>,
    settlement: AtomicBool,
    outcome: AtomicBool,
}

impl GroupLease {
    pub(crate) fn new(inner: Arc<dyn SubmissionLease>, permit: GroupPermit) -> Self {
        Self {
            inner,
            permit: Mutex::new(Some(permit)),
            settlement: AtomicBool::new(false),
            outcome: AtomicBool::new(false),
        }
    }

    fn maybe_release(&self) {
        if self.settlement.load(Ordering::Acquire) && self.outcome.load(Ordering::Acquire) {
            lock(&self.permit).take();
        }
    }
}

impl SubmissionLease for GroupLease {
    fn execution_finished(&self) {
        self.inner.execution_finished();
    }
    fn settlement_finished(&self) {
        self.inner.settlement_finished();
        self.settlement.store(true, Ordering::Release);
        self.maybe_release();
    }
    fn outcome_delivered(&self, delivery: OutcomeDelivery) {
        self.inner.outcome_delivered(delivery);
        self.outcome.store(true, Ordering::Release);
        self.maybe_release();
    }
}

/// A bounded submission group with its own shutdown boundary.
pub struct OperationGroup {
    manager: OperationManager,
    core: Arc<GroupCore>,
    capacity: usize,
}

impl OperationGroup {
    pub(crate) fn new(
        manager: OperationManager,
        options: GroupOptions,
    ) -> Result<Self, ManagerCreateError> {
        if options.capacity == 0 {
            return Err(ManagerCreateError::InvalidOptions {
                field: "group.capacity",
            });
        }
        let core = GroupCore::new(options.capacity);
        let manager = manager.with_group(Arc::clone(&core));
        Ok(Self {
            manager,
            core,
            capacity: options.capacity,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn snapshot(&self) -> GroupSnapshot {
        self.core.public_snapshot()
    }

    /// Stops direct group submissions while allowing already accepted
    /// operations to submit descendants through their bound scope views.
    pub fn close_admission(&self) {
        self.core.close();
    }

    pub fn try_submit<F, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.manager.try_submit(options, body)
    }

    pub fn submit<F, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.manager.submit(options, body)
    }

    pub async fn try_submit_async<F, Fut, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.manager.as_async().try_submit(options, body)
    }

    pub fn resource_scope(
        &self,
        options: crate::v02::report::ServiceResourceOptions,
    ) -> Result<
        crate::v02::resource::service::ServiceResourceScope,
        crate::v02::report::ResourceScopeError,
    > {
        self.manager.service_resource_scope(options.capacity)
    }

    pub fn request_shutdown(&self, mode: crate::v02::report::ShutdownMode) -> GroupShutdownHandle {
        self.core.begin_shutdown(mode);
        GroupShutdownHandle {
            core: Arc::clone(&self.core),
        }
    }
}

#[derive(Clone)]
pub struct GroupShutdownHandle {
    core: Arc<GroupCore>,
}

impl GroupShutdownHandle {
    pub fn snapshot(&self) -> crate::v02::report::ShutdownSnapshot {
        let active = self.core.snapshot().1;
        crate::v02::report::ShutdownSnapshot {
            active_operations: active,
            queued_operations: 0,
            unconfirmed_users: 0,
            cleaning_resources: self.core.cleaning_count(),
            recovery_groups: 0,
            service_resources: self.core.service_count(),
        }
    }

    pub async fn wait(&self) -> crate::v02::report::ShutdownReport {
        self.core.wait().await;
        crate::v02::report::ShutdownReport {
            snapshot: self.snapshot(),
        }
    }

    pub fn blocking(&self) -> BlockingGroupShutdown {
        BlockingGroupShutdown {
            handle: self.clone(),
        }
    }
}

pub struct BlockingGroupShutdown {
    handle: GroupShutdownHandle,
}

impl BlockingGroupShutdown {
    pub fn wait(&self) -> crate::v02::report::ShutdownReport {
        self.handle.core.wait_blocking();
        crate::v02::report::ShutdownReport {
            snapshot: self.handle.snapshot(),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v02::report::{ManagerOptions, OperationOutcome};
    use std::sync::mpsc;

    #[test]
    fn group_limit_is_local_and_released_after_settlement() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let mut held = group
            .try_submit(OperationOptions::default(), move |_| {
                started_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                Ok::<_, ()>(())
            })
            .unwrap();
        started_rx.recv().unwrap();
        let rejected = group.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
        assert!(matches!(rejected, Err(error) if error.reason() == AdmissionError::Busy));

        let mut outside = manager
            .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
            .unwrap();
        assert!(matches!(
            outside.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));
        finish_tx.send(()).unwrap();
        assert!(matches!(
            held.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));
        held.wait_settlement().unwrap();
        assert_eq!(group.snapshot().active_operations, 0);
        assert!(
            group
                .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
                .is_ok()
        );
    }

    #[test]
    fn local_drain_rejects_group_roots_but_accepts_bound_descendants() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
        let (bound_tx, bound_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let mut parent = group
            .try_submit(OperationOptions::default(), move |scope| {
                bound_tx.send(scope.manager()).unwrap();
                finish_rx.recv().unwrap();
                Ok::<_, ()>(())
            })
            .unwrap();
        let bound = bound_rx.recv().unwrap();
        group.close_admission();
        assert!(
            matches!(group.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
            Err(error) if error.reason() == AdmissionError::Closed)
        );
        let mut child = bound
            .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
            .unwrap();
        assert!(matches!(
            child.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));
        finish_tx.send(()).unwrap();
        assert!(matches!(
            parent.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));
        parent.wait_settlement().unwrap();
    }

    #[test]
    fn local_cancel_stops_group_without_closing_manager() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
        let (finish_tx, finish_rx) = mpsc::channel();
        let mut operation = group
            .try_submit(OperationOptions::default(), move |_| {
                finish_rx.recv().unwrap();
                Ok::<_, ()>(())
            })
            .unwrap();
        let shutdown = group.request_shutdown(crate::v02::report::ShutdownMode::Cancel);
        assert!(
            matches!(group.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
            Err(error) if error.reason() == AdmissionError::Stopped)
        );
        let mut outside = manager
            .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
            .unwrap();
        assert!(matches!(
            outside.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));
        finish_tx.send(()).unwrap();
        assert!(matches!(
            operation.wait_outcome().unwrap(),
            OperationOutcome::Stopped(_)
        ));
        operation.wait_settlement().unwrap();
        shutdown.blocking().wait();
    }
}

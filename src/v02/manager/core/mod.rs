//! Shared admission and operation-tree coordination for manager views.

use crate::v02::context::OperationContext;
use crate::v02::report::{
    AdmissionError, ContextError, ContextOptions, OperationId, OperationOptions, ScopeLimits,
    SubmitError,
};
use crate::v02::scope::{ExecutionFrameGuard, OperationScope, ScopeCore};
use std::sync::{Arc, Mutex, Weak};

/// The immutable submission location carried by manager views.
#[derive(Clone)]
pub(crate) enum SubmissionTarget {
    Root,
    Parent {
        id: OperationId,
        scope: Weak<ScopeCore>,
    },
}

/// Pre-reserved, infallible handoff to an execution engine.
pub(crate) trait SubmissionPermit: Send + 'static {
    fn dispatch(self: Box<Self>, job: Box<dyn FnOnce() + Send + 'static>);
}

/// Capacity and execution boundary supplied by the production manager assembly.
pub(crate) trait SubmissionPort: Send + Sync + 'static {
    fn reserve(
        &self,
        parent: Option<OperationId>,
        options: &OperationOptions,
    ) -> Result<Box<dyn SubmissionPermit>, AdmissionError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManagerPhase {
    Open,
    Draining,
    Cancelling,
    Closed,
}

struct ManagerState {
    phase: ManagerPhase,
    relationships: Vec<(OperationId, Option<OperationId>)>,
    values: Vec<usize>,
}

/// State shared by all sync and async manager views.
pub(crate) struct ManagerCore {
    state: Mutex<ManagerState>,
    port: Arc<dyn SubmissionPort>,
    root_context: Arc<OperationContext>,
    scope_limits: ScopeLimits,
}

impl ManagerCore {
    /// Creates the real coordinator around supplied capacity and execution ports.
    pub(crate) fn new(
        port: Arc<dyn SubmissionPort>,
        root_context: OperationContext,
        scope_limits: ScopeLimits,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ManagerState {
                phase: ManagerPhase::Open,
                relationships: Vec::new(),
                values: Vec::new(),
            }),
            port,
            root_context: Arc::new(root_context),
            scope_limits,
        })
    }

    pub(crate) fn root_view(self: &Arc<Self>) -> OperationManager {
        OperationManager {
            core: Arc::clone(self),
            target: SubmissionTarget::Root,
        }
    }

    pub(crate) fn bound(self: Arc<Self>, scope: &Arc<ScopeCore>) -> OperationManager {
        OperationManager {
            core: self,
            target: SubmissionTarget::Parent {
                id: scope.id(),
                scope: Arc::downgrade(scope),
            },
        }
    }

    fn submit_scope<F>(
        self: &Arc<Self>,
        target: &SubmissionTarget,
        mut options: OperationOptions,
        body: F,
    ) -> Result<OperationId, SubmitError<F>>
    where
        F: FnOnce(OperationScope) + Send + 'static,
    {
        let parent = match target {
            SubmissionTarget::Root => None,
            SubmissionTarget::Parent { id, scope } => {
                let Some(scope) = scope.upgrade() else {
                    return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
                };
                if !scope.is_open() {
                    return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
                }
                if let Err(error) = scope.context().check() {
                    return Err(SubmitError::new(map_context_error(error), body));
                }
                Some((*id, scope))
            }
        };
        if options.limits == ScopeLimits::default() {
            options.limits = self.scope_limits;
        }
        let context = match parent.as_ref() {
            Some((_, scope)) => scope.context().child(options.context.clone()),
            None => self.root_context.child(options.context.clone()),
        };
        let context = match context {
            Ok(context) => context,
            Err(error) => return Err(SubmitError::new(map_context_error(error), body)),
        };
        if let Err(error) = context.check() {
            return Err(SubmitError::new(map_context_error(error), body));
        }
        let parent_id = parent.as_ref().map(|(id, _)| *id);
        let permit = match self.port.reserve(parent_id, &options) {
            Ok(permit) => permit,
            Err(reason) => return Err(SubmitError::new(reason, body)),
        };
        let id = OperationId::new_unique();
        let scope_core = ScopeCore::new(
            id,
            Arc::downgrade(self),
            parent.as_ref().map(|(_, scope)| Arc::downgrade(scope)),
            parent_id,
            context,
            options,
        );
        {
            let mut state = lock(&self.state);
            let allowed = match (state.phase, parent_id) {
                (ManagerPhase::Open, _) => true,
                (ManagerPhase::Draining, Some(_)) => true,
                (ManagerPhase::Draining, None) => false,
                (ManagerPhase::Cancelling | ManagerPhase::Closed, _) => false,
            };
            if !allowed {
                drop(state);
                return Err(SubmitError::new(
                    if parent_id.is_some() {
                        AdmissionError::Stopped
                    } else {
                        AdmissionError::Closed
                    },
                    body,
                ));
            }
            if let Some((_, parent_scope)) = parent.as_ref()
                && !parent_scope.mark_child(id, Arc::downgrade(&scope_core))
            {
                drop(state);
                return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
            }
            state.relationships.push((id, parent_id));
        }
        let core = Arc::clone(self);
        permit.dispatch(Box::new(move || {
            let scope = OperationScope::new(Arc::clone(&scope_core), core);
            let _frame = ExecutionFrameGuard::new(Arc::clone(&scope_core));
            body(scope);
        }));
        Ok(id)
    }

    #[cfg(test)]
    pub(crate) fn record_test_value(&self, value: usize) {
        lock(&self.state).values.push(value);
    }

    #[cfg(test)]
    fn test_relationships(&self) -> Vec<(OperationId, Option<OperationId>)> {
        lock(&self.state).relationships.clone()
    }

    #[cfg(test)]
    fn test_values(&self) -> Vec<usize> {
        lock(&self.state).values.clone()
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> (Arc<Self>, OperationManager) {
        let core = Self::new(
            Arc::new(TestPort),
            OperationContext::new(ContextOptions::default()).unwrap(),
            ScopeLimits::default(),
        );
        let root = core.root_view();
        (core, root)
    }
}

/// A sync view over the shared coordinator and one fixed target.
#[derive(Clone)]
pub struct OperationManager {
    core: Arc<ManagerCore>,
    target: SubmissionTarget,
}
impl OperationManager {
    pub(crate) fn bound(core: Arc<ManagerCore>, scope: &Arc<ScopeCore>) -> Self {
        core.bound(scope)
    }

    /// Returns a sync view with the same fixed submission target.
    pub fn as_sync(&self) -> Self {
        self.clone()
    }

    /// Returns the corresponding async view without changing its target.
    pub fn as_async(&self) -> AsyncOperationManager {
        AsyncOperationManager {
            core: Arc::clone(&self.core),
            target: self.target.clone(),
        }
    }

    /// Submits a body through the shared operation-tree coordinator.
    pub(crate) fn try_submit<F>(&self, body: F) -> Result<OperationId, SubmitError<F>>
    where
        F: FnOnce(OperationScope) + Send + 'static,
    {
        self.core
            .submit_scope(&self.target, OperationOptions::default(), body)
    }

    pub(crate) fn try_submit_with<F>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationId, SubmitError<F>>
    where
        F: FnOnce(OperationScope) + Send + 'static,
    {
        self.core.submit_scope(&self.target, options, body)
    }
}

/// An async view over the shared coordinator and one fixed target.
#[derive(Clone)]
pub struct AsyncOperationManager {
    core: Arc<ManagerCore>,
    target: SubmissionTarget,
}
impl AsyncOperationManager {
    /// Returns the corresponding sync view without changing its target.
    pub fn as_sync(&self) -> OperationManager {
        OperationManager {
            core: Arc::clone(&self.core),
            target: self.target.clone(),
        }
    }
}

fn map_context_error(error: ContextError) -> AdmissionError {
    match error {
        ContextError::Stopped => AdmissionError::Stopped,
        ContextError::ClockUnavailable | ContextError::ClockDomainMismatch => {
            AdmissionError::DeadlineExpired
        }
        ContextError::BudgetRejected(_) => AdmissionError::CapacityExceeded,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
struct TestPort;
#[cfg(test)]
struct TestPermit;
#[cfg(test)]
impl SubmissionPort for TestPort {
    fn reserve(
        &self,
        _parent: Option<OperationId>,
        _options: &OperationOptions,
    ) -> Result<Box<dyn SubmissionPermit>, AdmissionError> {
        Ok(Box::new(TestPermit))
    }
}
#[cfg(test)]
impl SubmissionPermit for TestPermit {
    fn dispatch(self: Box<Self>, job: Box<dyn FnOnce() + Send + 'static>) {
        job();
    }
}

#[cfg(test)]
mod tests;

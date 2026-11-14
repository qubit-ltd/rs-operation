//! Non-cloneable operation scope views and execution-frame lifetime.

use crate::v02::context::OperationContext;
use crate::v02::manager::core::{ManagerCore, OperationManager};
use crate::v02::report::{OperationId, OperationOptions, StopReason};
use std::sync::{Arc, Mutex, Weak};

/// Internal scope state referenced weakly by child-bound manager views.
pub(crate) struct ScopeCore {
    id: OperationId,
    manager: Weak<ManagerCore>,
    parent: Option<Weak<ScopeCore>>,
    parent_id: Option<OperationId>,
    context: OperationContext,
    options: OperationOptions,
    state: Mutex<ScopeState>,
}

struct ScopeState {
    open: bool,
    children: Vec<(OperationId, Weak<ScopeCore>)>,
}

impl ScopeCore {
    pub(crate) fn new(
        id: OperationId,
        manager: Weak<ManagerCore>,
        parent: Option<Weak<ScopeCore>>,
        parent_id: Option<OperationId>,
        context: OperationContext,
        options: OperationOptions,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            manager,
            parent,
            parent_id,
            context,
            options,
            state: Mutex::new(ScopeState {
                open: true,
                children: Vec::new(),
            }),
        })
    }

    pub(crate) fn id(&self) -> OperationId {
        self.id
    }

    pub(crate) fn is_open(&self) -> bool {
        lock(&self.state).open
    }

    pub(crate) fn context(&self) -> &OperationContext {
        &self.context
    }

    pub(crate) fn parent(&self) -> Option<Arc<ScopeCore>> {
        self.parent.as_ref().and_then(Weak::upgrade)
    }

    pub(crate) fn parent_id(&self) -> Option<OperationId> {
        self.parent_id
    }

    pub(crate) fn options(&self) -> &OperationOptions {
        &self.options
    }

    pub(crate) fn mark_child(&self, id: OperationId, scope: Weak<ScopeCore>) -> bool {
        let mut state = lock(&self.state);
        if !state.open {
            return false;
        }
        state.children.push((id, scope));
        true
    }

    fn end(&self) {
        lock(&self.state).open = false;
    }

    #[cfg(test)]
    fn _test_child_count(&self) -> usize {
        lock(&self.state).children.len()
    }
}

/// Non-cloneable authority scoped to one operation body.
pub struct OperationScope {
    core: Arc<ScopeCore>,
    manager: Arc<ManagerCore>,
}
impl OperationScope {
    pub(crate) fn new(core: Arc<ScopeCore>, manager: Arc<ManagerCore>) -> Self {
        Self { core, manager }
    }

    /// Returns this operation's stable identifier.
    pub fn id(&self) -> OperationId {
        self.core.id
    }

    /// Creates an owned manager view bound to this operation's direct children.
    pub fn manager(&self) -> OperationManager {
        let manager = self
            .core
            .manager
            .upgrade()
            .unwrap_or_else(|| Arc::clone(&self.manager));
        OperationManager::bound(manager, &self.core)
    }

    /// Requests stop for this scope and propagates it to descendants.
    pub fn request_stop(&self, reason: StopReason) -> bool {
        self.core.context.request_stop(reason)
    }

    /// Returns whether this body frame still accepts children.
    pub(crate) fn is_open(&self) -> bool {
        self.core.is_open()
    }

    #[cfg(test)]
    pub(crate) fn record_test_value(&self, value: usize) {
        self.manager.record_test_value(value);
    }
}

/// Ends a scope when its operation body returns or unwinds, even if moved out.
pub(crate) struct ExecutionFrameGuard {
    scope: Arc<ScopeCore>,
}
impl ExecutionFrameGuard {
    pub(crate) fn new(scope: Arc<ScopeCore>) -> Self {
        Self { scope }
    }
}
impl Drop for ExecutionFrameGuard {
    fn drop(&mut self) {
        self.scope.end();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use crate::v02::manager::core::ManagerCore;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[test]
    fn scope_manager_is_owned_and_frame_bound() {
        let escaped = Arc::new(Mutex::new(None));
        let saved = Arc::clone(&escaped);
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit(move |scope| {
                *saved.lock().unwrap() = Some(scope.manager());
            })
            .unwrap();
        let manager = escaped.lock().unwrap().take().unwrap();
        assert!(manager.try_submit(|_| {}).is_err());
    }
}

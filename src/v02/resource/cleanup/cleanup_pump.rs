// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;

use super::CleanupDispatcher;
use super::internal::CleanupDispatcherInner;
use super::internal::CleanupPumpInner;
use super::internal::CleanupPumpState;
use super::lock;

/// Wakes a background worker to retry cleanup queues when executor capacity
/// opens.
///
/// The worker stores weak references to dispatchers, so the pump does not keep
/// resource scopes alive. Explicit shutdown joins the worker; dropping the
/// pump signals it to stop and detaches its join handle.
pub(crate) struct CleanupPump {
    /// Shared wake state and weak dispatcher registry observed by the worker.
    pub(in crate::v02::resource::cleanup) inner: Arc<CleanupPumpInner>,
    /// Worker handle, consumed by explicit shutdown or detached on drop.
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl CleanupPump {
    /// Starts the cleanup pump worker.
    ///
    /// # Returns
    ///
    /// A shared pump with one worker waiting for queue wakeups.
    ///
    /// # Errors
    ///
    /// Returns the operating-system thread creation error if the worker cannot
    /// be spawned.
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

    /// Adds a dispatcher to the worker's weak registry once.
    ///
    /// # Parameters
    ///
    /// - `self`: The pump receiving the dispatcher registration.
    /// - `dispatcher`: Shared dispatcher state referenced weakly by the worker.
    pub(in crate::v02::resource::cleanup) fn register(&self, dispatcher: &Arc<CleanupDispatcherInner>) {
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

    /// Removes a dispatcher's current registration from the weak registry.
    ///
    /// # Parameters
    ///
    /// - `self`: The pump that owns the registration registry.
    /// - `dispatcher`: Dispatcher whose registration is cleared.
    pub(in crate::v02::resource::cleanup) fn unregister(&self, dispatcher: &Arc<CleanupDispatcherInner>) {
        let id = lock(&dispatcher.pump_registration).take();
        if let Some(id) = id {
            lock(&self.inner.state).dispatchers.remove(&id);
        }
    }

    /// Coalesces a wake signal and wakes the worker if it is waiting.
    ///
    /// # Parameters
    ///
    /// - `self`: The pump whose worker should retry registered queues.
    pub(crate) fn kick(&self) {
        let mut state = lock(&self.inner.state);
        state.signaled = true;
        drop(state);
        self.inner.changed.notify_one();
    }

    /// Stops the worker and joins it if its handle has not already been taken.
    ///
    /// This operation is safe to call more than once. Queue registrations are
    /// not driven after the worker observes the stopping state.
    ///
    /// # Parameters
    ///
    /// - `self`: The pump whose worker is being stopped.
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
    /// Signals the worker to stop and detaches its join handle.
    ///
    /// Explicit shutdown should be used when the caller must wait for worker
    /// termination; drop cannot block to join during manager destruction.
    ///
    /// # Parameters
    ///
    /// - `self`: The pump whose worker is signaled and detached.
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

/// Waits for wake signals and retries each live registered dispatcher.
///
/// Dead dispatcher weak references are removed from the registry. The worker
/// exits as soon as it observes the stopping flag.
///
/// # Parameters
///
/// - `inner`: Shared wake state and weak dispatcher registry for the worker.
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

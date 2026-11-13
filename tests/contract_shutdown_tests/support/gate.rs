// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

#[derive(Default)]
struct GateState {
    started: bool,
    released: bool,
    waiters: Vec<Waker>,
}

/// A two-phase test gate that records entry and waits for explicit release.
#[derive(Clone, Default)]
pub struct Gate {
    inner: Arc<GateInner>,
}

#[derive(Default)]
struct GateInner {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl Gate {
    /// Create a closed gate with neither phase observed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a future that marks the started phase and waits for release.
    #[must_use = "futures do nothing unless they are awaited or polled"]
    pub fn wait(&self) -> GateWait {
        GateWait {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Report whether a waiter has actually polled the gate.
    pub fn started(&self) -> bool {
        self.inner.state.lock().expect("gate mutex poisoned").started
    }

    /// Report whether the release phase has been opened.
    pub fn is_released(&self) -> bool {
        self.inner.state.lock().expect("gate mutex poisoned").released
    }

    /// Open the release phase and wake futures that reached the gate.
    pub fn release(&self) {
        let waiters = {
            let mut state = self.inner.state.lock().expect("gate mutex poisoned");
            if state.released {
                return;
            }
            state.released = true;
            self.inner.changed.notify_all();
            std::mem::take(&mut state.waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    /// Wait for the started phase, failing after a bounded test watchdog.
    pub fn wait_started(&self) {
        self.wait_for_phase(|state| state.started, "gate was not started before watchdog");
    }

    /// Wait for the released phase, failing after a bounded test watchdog.
    pub fn wait_released(&self) {
        self.wait_for_phase(|state| state.released, "gate was not released before watchdog");
    }

    fn wait_for_phase(&self, ready: impl Fn(&GateState) -> bool, message: &str) {
        let state = self.inner.state.lock().expect("gate mutex poisoned");
        let (state, timeout) = self
            .inner
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |state| !ready(state))
            .expect("gate mutex poisoned while waiting");
        assert!(!timeout.timed_out() || ready(&state), "{message}");
    }
}

/// Future returned by [`Gate::wait`].
pub struct GateWait {
    inner: Arc<GateInner>,
}

impl Future for GateWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.inner.state.lock().expect("gate mutex poisoned");
        state.started = true;
        self.inner.changed.notify_all();
        if state.released {
            return Poll::Ready(());
        }
        if !state.waiters.iter().any(|waiter| waiter.will_wake(context.waker())) {
            state.waiters.push(context.waker().clone());
        }
        Poll::Pending
    }
}

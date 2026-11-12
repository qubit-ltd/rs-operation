// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::task::Waker;

use super::ContextError;
use super::OperationSupervisor;
use super::SharedServiceGroupState;
use super::ShutdownSource;
use super::lock;

/// A bounded shutdown waiter that unregisters when its observation ends.
pub(super) struct ShutdownWaitRegistration {
    source: ShutdownSource,
    waiter_id: Option<u64>,
}

impl ShutdownWaitRegistration {
    pub(super) fn new(source: ShutdownSource) -> Self {
        Self {
            source,
            waiter_id: None,
        }
    }

    pub(super) fn register(&mut self, waker: &Waker) -> Result<(), ContextError> {
        let replacement = waker.clone();
        match self.source.clone() {
            ShutdownSource::Supervisor(supervisor) => self.register_supervisor(&supervisor, waker, replacement),
            ShutdownSource::Service(state, _) => {
                let guard = lock(&state.0);
                let capacity = guard.limit;
                drop(guard);
                // The state lock is acquired below so Waker replacement/drop stays outside it.
                self.register_service(&state, waker, replacement, capacity)
            }
        }
    }

    fn register_supervisor(
        &mut self,
        supervisor: &OperationSupervisor,
        waker: &Waker,
        replacement: Waker,
    ) -> Result<(), ContextError> {
        let capacity = supervisor.inner.limits.admission_waiters;
        let id = self.waiter_id.unwrap_or_else(|| {
            let mut state = lock(&supervisor.inner.state);
            let used = state.shutdown_waiters.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            allocate_waiter_id(&mut state.next_shutdown_waiter, &used)
        });
        let (old, result) = {
            let mut state = lock(&supervisor.inner.state);
            let position = state.shutdown_waiters.iter().position(|(old_id, _)| *old_id == id);
            if position.is_none() && state.shutdown_waiters.len() >= capacity {
                (None, Err(ContextError::WaiterCapacityExceeded))
            } else if let Some(position) = position {
                let old = if state.shutdown_waiters[position].1.will_wake(waker) {
                    None
                } else {
                    Some(std::mem::replace(&mut state.shutdown_waiters[position].1, replacement))
                };
                (old, Ok(()))
            } else {
                state.shutdown_waiters.push((id, replacement));
                (None, Ok(()))
            }
        };
        drop(old);
        if result.is_ok() {
            self.waiter_id = Some(id);
        }
        result
    }

    fn register_service(
        &mut self,
        state: &SharedServiceGroupState,
        waker: &Waker,
        replacement: Waker,
        capacity: usize,
    ) -> Result<(), ContextError> {
        let id = self.waiter_id.unwrap_or_else(|| {
            let mut guard = lock(&state.0);
            let used = guard.shutdown_waiters.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            allocate_waiter_id(&mut guard.next_shutdown_waiter, &used)
        });
        let (old, result) = {
            let mut guard = lock(&state.0);
            let position = guard.shutdown_waiters.iter().position(|(old_id, _)| *old_id == id);
            if position.is_none() && guard.shutdown_waiters.len() >= capacity {
                (None, Err(ContextError::WaiterCapacityExceeded))
            } else if let Some(position) = position {
                let old = if guard.shutdown_waiters[position].1.will_wake(waker) {
                    None
                } else {
                    Some(std::mem::replace(&mut guard.shutdown_waiters[position].1, replacement))
                };
                (old, Ok(()))
            } else {
                guard.shutdown_waiters.push((id, replacement));
                (None, Ok(()))
            }
        };
        drop(old);
        if result.is_ok() {
            self.waiter_id = Some(id);
        }
        result
    }

    pub(super) fn unregister(&mut self) {
        let Some(id) = self.waiter_id.take() else {
            return;
        };
        let removed = match &self.source {
            ShutdownSource::Supervisor(supervisor) => {
                let mut state = lock(&supervisor.inner.state);
                state
                    .shutdown_waiters
                    .iter()
                    .position(|(waiter_id, _)| *waiter_id == id)
                    .map(|position| state.shutdown_waiters.remove(position).1)
            }
            ShutdownSource::Service(state, _) => {
                let mut guard = lock(&state.0);
                guard
                    .shutdown_waiters
                    .iter()
                    .position(|(waiter_id, _)| *waiter_id == id)
                    .map(|position| guard.shutdown_waiters.remove(position).1)
            }
        };
        drop(removed);
    }
}

fn allocate_waiter_id(next: &mut u64, waiters: &[u64]) -> u64 {
    let mut candidate = (*next).max(1);
    while waiters.contains(&candidate) {
        candidate = candidate.wrapping_add(1).max(1);
    }
    *next = candidate.wrapping_add(1).max(1);
    candidate
}

impl Drop for ShutdownWaitRegistration {
    fn drop(&mut self) {
        self.unregister();
    }
}

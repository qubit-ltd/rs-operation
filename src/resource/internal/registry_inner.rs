// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

use super::RegistryState;
use crate::report::OperationId;

pub(in crate::resource) struct RegistryInner {
    pub(in crate::resource) id: u64,
    pub(in crate::resource) operation_id: OperationId,
    pub(in crate::resource) state: Mutex<RegistryState>,
    pub(in crate::resource) change_epoch: AtomicU64,
    pub(in crate::resource) change_wakers: Mutex<Vec<Waker>>,
}

impl RegistryInner {
    pub(in crate::resource) fn change_epoch(&self) -> u64 {
        self.change_epoch.load(Ordering::Acquire)
    }

    pub(in crate::resource) fn notify_change(&self) {
        self.change_epoch.fetch_add(1, Ordering::AcqRel);
        let waiters = {
            let mut waiters = self
                .change_wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    pub(in crate::resource) fn register_change_waker(&self, waker: &Waker) {
        let mut candidate = Some(waker.clone());
        {
            let mut waiters = self
                .change_wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !waiters.iter().any(|waiter| waiter.will_wake(waker)) {
                waiters.push(candidate.take().expect("candidate waker is present"));
            }
        }
        drop(candidate);
    }

    pub(in crate::resource) fn remove_change_waker(&self, waker: &Waker) {
        let removed = {
            let mut waiters = self
                .change_wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut retained = Vec::with_capacity(waiters.len());
            let mut removed = Vec::new();
            for waiter in waiters.drain(..) {
                if waiter.will_wake(waker) {
                    removed.push(waiter);
                } else {
                    retained.push(waiter);
                }
            }
            *waiters = retained;
            removed
        };
        drop(removed);
    }
}

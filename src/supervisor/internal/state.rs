// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Root admission counters, leases, and wake registrations.

use std::collections::VecDeque;
use std::sync::Weak;
use std::sync::Mutex;
use std::task::Waker;

use crate::context::OperationContext;

use super::LeaseState;

pub(super) struct State {
    pub(in crate::supervisor) closed: bool,
    pub(in crate::supervisor) active: usize,
    pub(in crate::supervisor) queued: VecDeque<Weak<Mutex<LeaseState>>>,
    pub(in crate::supervisor) results: usize,
    pub(in crate::supervisor) cleanup_active: usize,
    pub(in crate::supervisor) waiters: Vec<(u64, Option<Waker>)>,
    pub(in crate::supervisor) next_waiter: u64,
    pub(in crate::supervisor) cancel: bool,
    pub(in crate::supervisor) contexts: Vec<(Weak<Mutex<LeaseState>>, OperationContext)>,
    pub(in crate::supervisor) shutdown_waiters: Vec<(u64, Waker)>,
    pub(in crate::supervisor) next_shutdown_waiter: u64,
}


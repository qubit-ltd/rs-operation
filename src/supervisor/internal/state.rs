// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Root admission counters, leases, and wake registrations.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Waker;

use super::LeaseState;
use crate::context::OperationContext;

/// Coordinates root admission, result capacity, cleanup slots, and lifecycle
/// wakeups.
///
/// The supervisor protects this state with its mutex. Queue entries and context
/// registrations use weak lease references so completed or dropped admissions
/// do not remain alive solely because they were registered here.
pub(in crate::supervisor) struct State {
    /// Rejects new admissions after close or shutdown begins.
    pub(in crate::supervisor) closed: bool,
    /// Number of root leases currently occupying execution capacity.
    pub(in crate::supervisor) active: usize,
    /// FIFO leases waiting for a root execution slot; dead leases are discarded
    /// when promoted.
    pub(in crate::supervisor) queued: VecDeque<Weak<Mutex<LeaseState>>>,
    /// Number of typed-result reservations, including results not yet observed.
    pub(in crate::supervisor) results: usize,
    /// Number of cleanup operations currently holding cleanup capacity.
    pub(in crate::supervisor) cleanup_active: usize,
    /// Admission waiters paired with their stable registration ID and optional
    /// task waker.
    pub(in crate::supervisor) waiters: Vec<(u64, Option<Waker>)>,
    /// Next ID assigned to an admission waiter; checked before advancing to
    /// avoid wraparound.
    pub(in crate::supervisor) next_waiter: u64,
    /// Whether shutdown has requested cancellation of all registered operation
    /// contexts.
    pub(in crate::supervisor) cancel: bool,
    /// Live operation contexts paired with weak lease references for shutdown
    /// cancellation.
    pub(in crate::supervisor) contexts: Vec<(Weak<Mutex<LeaseState>>, OperationContext)>,
    /// Shutdown observers paired with their stable registration IDs and task
    /// wakers.
    pub(in crate::supervisor) shutdown_waiters: Vec<(u64, Waker)>,
    /// Next ID assigned to a shutdown observer; checked before advancing to
    /// avoid wraparound.
    pub(in crate::supervisor) next_shutdown_waiter: u64,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Lifecycle state of a resource recovery record.
///
/// The phase describes cleanup and recovery ownership; it is separate from an
/// operation's outcome. `Pending` and `Unconfirmed` records need attention,
/// while a `Transferred` record is owned by a recovery store.
///
/// # Examples
///
/// ```
/// use qubit_operation::RecoveryPhase;
///
/// fn needs_attention(phase: RecoveryPhase) -> bool {
///     matches!(phase, RecoveryPhase::Pending | RecoveryPhase::Unconfirmed)
/// }
///
/// assert!(needs_attention(RecoveryPhase::Pending));
/// assert!(!needs_attention(RecoveryPhase::Resolved));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryPhase {
    /// Cleanup has not resolved and a retry can be requested.
    Pending,
    /// A cleanup retry is currently running.
    Running,
    /// Cleanup succeeded, so the recovery record no longer retains its owner.
    Resolved,
    /// Ownership of the unresolved recovery record was accepted by a store.
    Transferred,
    /// Cleanup is unresolved but no automatic retry action is available.
    Unconfirmed,
}

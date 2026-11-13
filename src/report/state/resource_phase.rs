// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Resource lifecycle phase. Only `Released` proves confirmed release.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::ResourcePhase;
///
/// let phase = ResourcePhase::Released;
/// assert_eq!(phase, ResourcePhase::Released);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourcePhase {
    /// The resource is active and has not entered shutdown.
    Active,
    /// A stop request has begun resource shutdown.
    Stopping,
    /// Cleanup is running after the operation has stopped.
    Cleaning,
    /// The owning domain confirmed that resource release completed.
    Released,
    /// Cleanup transferred responsibility to a recovery owner.
    RecoveryRequired,
    /// The owning domain has not confirmed whether release completed.
    Unconfirmed,
}

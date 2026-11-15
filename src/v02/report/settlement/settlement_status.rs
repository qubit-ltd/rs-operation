// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Resource disposition after an operation settles.
///
/// # Examples
///
/// ```
/// use qubit_operation::SettlementStatus;
///
/// let status = SettlementStatus::Transferred;
/// let description = match status {
///     SettlementStatus::Released => "all resources released",
///     SettlementStatus::Transferred => "resources transferred",
///     SettlementStatus::RecoveryRequired => "resources need recovery",
/// };
/// assert_eq!(description, "resources transferred");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettlementStatus {
    /// All managed resources were released.
    Released,
    /// One or more resources moved to another scope.
    Transferred,
    /// One or more resources require explicit recovery.
    RecoveryRequired,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Phase in which cleanup failed or transferred responsibility.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::CleanupStage;
///
/// let failed_during = CleanupStage::Releasing;
/// assert_eq!(failed_during, CleanupStage::Releasing);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupStage {
    /// The resource was being closed.
    Closing,
    /// The resource's registration or lease was being released.
    Releasing,
    /// The resource was being removed from its owner.
    Removing,
    /// Cleanup responsibility was transferred to another owner.
    Transferring,
    /// A retained recovery owner was being handled.
    Recovery,
}

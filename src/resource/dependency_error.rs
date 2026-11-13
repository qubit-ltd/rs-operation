// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

/// Invalid cleanup dependency registration.
///
/// The error identifies why a requested cleanup dependency could not be added
/// to the resource registry.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::DependencyError;
///
/// let error = DependencyError::Cycle;
/// assert_eq!(error.to_string(), "cleanup dependency rejected: Cycle");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyError {
    /// Resources belong to different scope registries.
    DifferentScope,
    /// A resource cannot depend on itself.
    SelfDependency,
    /// The edge would create a directed dependency cycle.
    Cycle,
    /// A resource handle no longer names an active registration.
    NotRegistered,
    /// Registry state was poisoned by a prior panic.
    RegistryPoisoned,
}

impl fmt::Display for DependencyError {
    /// Formats the rejected dependency reason for diagnostic output.
    ///
    /// # Parameters
    ///
    /// * `f` - The formatter that receives the diagnostic text.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when the text is written successfully, or the
    /// formatter's error if its destination rejects the write.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cleanup dependency rejected: {self:?}")
    }
}

impl std::error::Error for DependencyError {}

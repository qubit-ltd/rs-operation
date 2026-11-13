// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

use super::RegisterErrorKind;
use crate::protocol::OwnedCleanup;

/// A resource registry failure that preserves the cleanup owner it rejected.
///
/// The registry returns this error when registration fails, allowing callers
/// to inspect the failure and recover the unchanged owner for another action.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::{RegisterError, RegisterErrorKind};
/// use qubit_operation::protocol::OwnedCleanup;
///
/// // Receive this error from a failed ResourceRegistry registration attempt.
/// fn recover_registration(error: RegisterError) -> (RegisterErrorKind, Box<dyn OwnedCleanup>) {
///     let kind = error.kind();
///     let owner = error.into_owner();
///     (kind, owner)
/// }
/// ```
pub struct RegisterError {
    /// Identifies whether capacity was exhausted or registry state was
    /// poisoned.
    pub(super) kind: RegisterErrorKind,
    /// Retains the cleanup owner unchanged until the caller recovers or drops
    /// it.
    pub(super) owner: Option<Box<dyn OwnedCleanup>>,
}

impl RegisterError {
    /// Return the finite capacity failure or poisoned registry condition.
    #[must_use]
    #[inline]
    pub const fn kind(&self) -> RegisterErrorKind {
        self.kind
    }

    /// Recover the exact cleanup owner that the registry did not accept.
    ///
    /// # Returns
    ///
    /// The unchanged boxed owner supplied to the failed registration attempt.
    #[must_use = "recover or explicitly drop the cleanup owner returned by a failed registration"]
    pub fn into_owner(mut self) -> Box<dyn OwnedCleanup> {
        self.owner.take().expect("unaccepted owner is present")
    }
}

impl fmt::Debug for RegisterError {
    /// Formats the failure kind without exposing the retained cleanup owner.
    ///
    /// # Parameters
    ///
    /// - `self`: The registration failure being formatted.
    /// - `f`: The formatter that receives the debug representation.
    ///
    /// # Returns
    ///
    /// `Ok(())` when formatting succeeds, or the formatter's error otherwise.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisterError").field("kind", &self.kind).finish()
    }
}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use super::RegisterErrorKind;

use crate::protocol::OwnedCleanup;

use std::fmt;

/// Registration failure; `into_owner` returns the unaccepted cleanup owner.
pub struct RegisterError {
    pub(super) kind: RegisterErrorKind,
    pub(super) owner: Option<Box<dyn OwnedCleanup>>,
}

impl RegisterError {
    /// Return the finite capacity failure or poisoned registry condition.
    pub const fn kind(&self) -> RegisterErrorKind {
        self.kind
    }

    /// Return the exact owner that the registry did not accept.
    pub fn into_owner(mut self) -> Box<dyn OwnedCleanup> {
        self.owner.take().expect("unaccepted owner is present")
    }
}

impl fmt::Debug for RegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisterError")
            .field("kind", &self.kind)
            .finish()
    }
}

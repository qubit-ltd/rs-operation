// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;

/// Invalid transition while reporting external driver lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "external driver errors should be handled"]
pub enum ExternalDriverError {
    /// The lease has already been marked lost or released.
    AlreadySettled,
}

impl Display for ExternalDriverError {
    /// Writes the fixed message describing an already-settled external lease.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the message; formatting errors are
    ///   returned to the caller.
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.write_str("external driver lease has already been settled")
    }
}

impl Error for ExternalDriverError {}

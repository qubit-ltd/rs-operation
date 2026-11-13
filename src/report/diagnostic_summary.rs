// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;
use std::sync::Arc;

/// UTF-8 diagnostic text retained up to a fixed byte limit.
///
/// Construction truncates only at a UTF-8 character boundary, so the value
/// always remains valid text and can be cheaply shared across clones.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::DiagnosticSummary;
///
/// let summary = DiagnosticSummary::new("operation completed");
/// assert_eq!(summary.as_str(), "operation completed");
/// ```
#[must_use]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DiagnosticSummary(
    /// Shared, bounded diagnostic text stored as UTF-8.
    Arc<str>,
);

impl DiagnosticSummary {
    /// Maximum retained UTF-8 bytes per summary.
    pub const MAX_BYTES: usize = 256;

    /// Retain at most `MAX_BYTES`, truncating only at a UTF-8 boundary.
    ///
    /// # Parameters
    ///
    /// * `value` - Text to retain, borrowed through `AsRef<str>`.
    ///
    /// # Returns
    ///
    /// A shared summary containing at most `MAX_BYTES` bytes.
    pub fn new(value: impl AsRef<str>) -> Self {
        let value = value.as_ref();
        if value.len() <= Self::MAX_BYTES {
            return Self(Arc::from(value));
        }
        let mut end = Self::MAX_BYTES;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        Self(Arc::from(&value[..end]))
    }

    /// Borrow the retained diagnostic text.
    ///
    /// # Returns
    ///
    /// The summary's text, valid for the lifetime of this borrow.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for DiagnosticSummary {
    /// Borrows the bounded text for APIs accepting string references.
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for DiagnosticSummary {
    /// Writes the bounded text without exposing additional formatting state.
    ///
    /// # Parameters
    ///
    /// * `f` - Formatter receiving the diagnostic text.
    ///
    /// # Returns
    ///
    /// The formatter's result, including any write error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

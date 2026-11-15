// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

/// A diagnostic string capped at [`DiagnosticSummary::MAX_BYTES`].
///
/// Truncation stops at a UTF-8 boundary and is reported by
/// [`Self::is_truncated`].
///
/// # Examples
///
/// ```
/// use qubit_operation::DiagnosticSummary;
///
/// let summary = DiagnosticSummary::new("resource cleanup failed");
/// assert_eq!(summary.as_str(), "resource cleanup failed");
/// assert!(!summary.is_truncated());
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticSummary {
    /// Retained prefix of the original diagnostic text.
    text: String,
    /// Whether construction shortened the input to fit the byte limit.
    truncated: bool,
}

impl DiagnosticSummary {
    /// Maximum number of UTF-8 bytes retained from a diagnostic.
    pub const MAX_BYTES: usize = 256;

    /// Stores the input, truncating it only at a valid UTF-8 boundary.
    ///
    /// # Parameters
    ///
    /// * `text` is the diagnostic text to retain or truncate.
    pub fn new(text: &str) -> Self {
        if text.len() <= Self::MAX_BYTES {
            return Self {
                text: text.to_owned(),
                truncated: false,
            };
        }
        let mut end = Self::MAX_BYTES;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            text: text[..end].to_owned(),
            truncated: true,
        }
    }

    /// Borrows the retained diagnostic text.
    pub fn as_str(&self) -> &str {
        &self.text
    }
    /// Reports whether [`Self::new`] omitted part of the input.
    pub const fn is_truncated(&self) -> bool {
        self.truncated
    }
}

impl fmt::Display for DiagnosticSummary {
    /// Formats the retained diagnostic text for callers.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the retained text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

#[cfg(test)]
mod tests {
    use super::DiagnosticSummary;

    #[test]
    fn test_display_and_truncation_report_retained_utf8_text() {
        let plain = DiagnosticSummary::new("short diagnostic");
        assert_eq!(plain.to_string(), "short diagnostic");
        assert!(!plain.is_truncated());

        let input = format!("{}界", "a".repeat(DiagnosticSummary::MAX_BYTES - 1));
        let truncated = DiagnosticSummary::new(&input);
        assert!(truncated.is_truncated());
        assert_eq!(truncated.as_str(), "a".repeat(DiagnosticSummary::MAX_BYTES - 1));
        assert_eq!(truncated.to_string(), truncated.as_str());
    }
}

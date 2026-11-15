// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reason a scope could not retain another effect record.
///
/// # Examples
///
/// Handle capacity exhaustion separately from lifecycle rejection:
///
/// ```
/// use qubit_operation::EffectRecordError;
///
/// fn error_kind(error: EffectRecordError) -> &'static str {
///     match error {
///         EffectRecordError::CapacityExceeded => "capacity exceeded",
///         EffectRecordError::ScopeEnded => "scope ended",
///         EffectRecordError::Stopped => "scope stopped",
///     }
/// }
///
/// assert_eq!(error_kind(EffectRecordError::CapacityExceeded), "capacity exceeded");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectRecordError {
    /// The configured effect-record capacity is exhausted.
    CapacityExceeded,
    /// The owning scope has ended.
    ScopeEnded,
    /// The owning scope has been stopped.
    Stopped,
}

impl fmt::Display for EffectRecordError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for EffectRecordError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::EffectRecordError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = EffectRecordError::CapacityExceeded;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

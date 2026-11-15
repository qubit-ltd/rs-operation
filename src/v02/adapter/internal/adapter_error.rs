// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::AdapterErrorKind;

/// An admission error that returns ownership of the rejected payload.
///
/// # Type Parameters
///
/// * `P` is the payload returned to the caller after rejection.
pub(crate) struct AdapterError<P> {
    /// Identifies why admission or activation was rejected.
    pub(crate) reason: AdapterErrorKind,
    /// Returns the payload and ownership state to the caller.
    pub(crate) payload: P,
}

impl<P> std::fmt::Debug for AdapterError<P> {
    /// Formats the rejection reason without requiring `P: Debug`.
    ///
    /// # Parameters
    ///
    /// * `f` receives the formatted rejection details.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after formatting the rejection reason.
    ///
    /// # Errors
    ///
    /// Returns the formatter error if writing the rejection details fails.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::AdapterError;
    use super::AdapterErrorKind;

    struct NonDebugPayload;

    #[test]
    fn test_adapter_error_debug_omits_payload_details() {
        let error = AdapterError {
            reason: AdapterErrorKind::AlreadyAttached,
            payload: NonDebugPayload,
        };

        let diagnostic = format!("{error:?}");
        assert!(diagnostic.contains("AlreadyAttached"));
        assert!(!diagnostic.contains("payload"));
    }
}

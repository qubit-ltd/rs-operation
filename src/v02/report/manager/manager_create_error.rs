// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;
use std::sync::Arc;

/// Failure while constructing a manager and its executors.
///
/// # Examples
///
/// Inspect the named option when manager configuration is rejected:
///
/// ```
/// use qubit_operation::ManagerCreateError;
///
/// let error = ManagerCreateError::InvalidOptions { field: "scope_limits" };
/// assert_eq!(error.to_string(), "invalid option: scope_limits");
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub enum ManagerCreateError {
    /// A named manager option is invalid.
    InvalidOptions {
        /// Name of the manager option that failed validation.
        field: &'static str,
    },
    /// An executor could not be made available.
    ExecutorUnavailable,
    /// The operating system refused to start a worker thread.
    ThreadSpawn {
        /// Operating-system or runtime error returned by thread creation.
        source: Arc<dyn Error + Send + Sync>,
    },
}

impl fmt::Display for ManagerCreateError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions { field } => write!(f, "invalid option: {field}"),
            Self::ExecutorUnavailable => f.write_str("executor unavailable"),
            Self::ThreadSpawn { source } => write!(f, "worker thread spawn failed: {source}"),
        }
    }
}

impl Error for ManagerCreateError {
    /// Returns the underlying cause when this error wraps one.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ThreadSpawn { source } => Some(source.as_ref()),
            Self::InvalidOptions { .. } | Self::ExecutorUnavailable => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::ManagerCreateError;

    #[test]
    fn test_display_describes_each_creation_failure() {
        assert_eq!(
            ManagerCreateError::InvalidOptions { field: "workers" }.to_string(),
            "invalid option: workers"
        );
        assert_eq!(
            ManagerCreateError::ExecutorUnavailable.to_string(),
            "executor unavailable"
        );
        let error = ManagerCreateError::ThreadSpawn {
            source: std::sync::Arc::new(std::io::Error::other("denied")),
        };
        assert_eq!(error.to_string(), "worker thread spawn failed: denied");
        assert_eq!(Error::source(&error).unwrap().to_string(), "denied");
    }
}

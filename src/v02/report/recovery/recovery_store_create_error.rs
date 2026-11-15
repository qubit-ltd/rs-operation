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

/// Failure while creating a recovery store and its cleanup executor.
///
/// # Examples
///
/// ```
/// use qubit_operation::{RecoveryStore, RecoveryStoreCreateError, RecoveryStoreOptions};
///
/// let mut options = RecoveryStoreOptions::default();
/// options.group_capacity = 0;
/// let result = RecoveryStore::new(options);
/// assert!(matches!(
///     result,
///     Err(RecoveryStoreCreateError::InvalidOptions {
///         field: "group_capacity"
///     })
/// ));
/// ```
#[derive(Clone, Debug)]
pub enum RecoveryStoreCreateError {
    /// A named configuration field is invalid.
    InvalidOptions {
        /// Name of the option that failed validation.
        field: &'static str,
    },
    /// The configured executor could not be created.
    ExecutorUnavailable,
    /// The operating system refused to start a worker thread.
    ThreadSpawn {
        /// Operating-system or runtime error returned by thread creation.
        source: Arc<dyn Error + Send + Sync>,
    },
}

impl fmt::Display for RecoveryStoreCreateError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// - `self`: Creation failure to format.
    /// - `f`: Formatter receiving the diagnostic text.
    ///
    /// # Returns
    ///
    /// The formatting result from writing the diagnostic.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions { field } => write!(f, "invalid option: {field}"),
            Self::ExecutorUnavailable => f.write_str("executor unavailable"),
            Self::ThreadSpawn { source } => write!(f, "worker thread spawn failed: {source}"),
        }
    }
}

impl Error for RecoveryStoreCreateError {
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

    use super::RecoveryStoreCreateError;

    #[test]
    fn test_display_describes_each_store_creation_failure() {
        assert_eq!(
            RecoveryStoreCreateError::InvalidOptions { field: "capacity" }.to_string(),
            "invalid option: capacity"
        );
        assert_eq!(
            RecoveryStoreCreateError::ExecutorUnavailable.to_string(),
            "executor unavailable"
        );
        let error = RecoveryStoreCreateError::ThreadSpawn {
            source: std::sync::Arc::new(std::io::Error::other("denied")),
        };
        assert_eq!(error.to_string(), "worker thread spawn failed: denied");
        assert_eq!(Error::source(&error).unwrap().to_string(), "denied");
    }
}

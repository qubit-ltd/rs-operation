// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reports configuration, admission, shutdown, or worker-startup failures
/// from the portable driver.
///
/// # Examples
///
/// ```
/// use qubit_operation::driver::PortableDriverError;
///
/// let error = PortableDriverError::Capacity;
/// assert_eq!(error.to_string(), "portable driver task capacity is full");
/// ```
#[must_use = "errors should be handled or reported"]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortableDriverError {
    /// Worker count, task capacity, and poll budget must all be positive.
    InvalidConfiguration,
    /// The driver has accepted its maximum number of unfinished tasks.
    Capacity,
    /// The driver is shutting down and accepts no new tasks.
    Closed,
    /// A worker thread could not be created.
    WorkerSpawn,
}

impl fmt::Display for PortableDriverError {
    /// Writes the stable, human-readable message for this driver failure.
    ///
    /// # Parameters
    ///
    /// * `f` - Formatter that receives the error message.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when the message is written successfully.
    ///
    /// # Errors
    ///
    /// Returns the formatter's error if writing the message fails.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfiguration => "portable driver limits must be positive",
            Self::Capacity => "portable driver task capacity is full",
            Self::Closed => "portable driver is closed",
            Self::WorkerSpawn => "portable driver worker could not be started",
        })
    }
}

impl Error for PortableDriverError {}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Portable driver configuration or admission failure.
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

impl std::fmt::Display for PortableDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidConfiguration => "portable driver limits must be positive",
            Self::Capacity => "portable driver task capacity is full",
            Self::Closed => "portable driver is closed",
            Self::WorkerSpawn => "portable driver worker could not be started",
        })
    }
}

impl std::error::Error for PortableDriverError {}

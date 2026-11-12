// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Why a blocking closure could not be accepted or an executor could not start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutorError {
    /// Worker count and queue capacity must both be positive.
    InvalidConfiguration,
    /// The pending job queue reached its configured capacity.
    Capacity,
    /// The executor is shutting down.
    Closed,
    /// A worker thread could not be created.
    WorkerSpawn,
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidConfiguration => "executor limits must be positive",
            Self::Capacity => "executor queue capacity is full",
            Self::Closed => "executor is closed",
            Self::WorkerSpawn => "executor worker could not be started",
        })
    }
}

impl std::error::Error for ExecutorError {}

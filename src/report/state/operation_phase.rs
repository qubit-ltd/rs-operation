// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Business result-delivery phase; stop requests are represented separately.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::OperationPhase;
///
/// let phase = OperationPhase::ResultReady;
/// assert_eq!(phase, OperationPhase::ResultReady);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationPhase {
    /// Accepted by the supervisor and queued for execution.
    Queued,
    /// The operation body has begun executing.
    Running,
    /// The typed business outcome is ready to observe.
    ResultReady,
    /// The one-shot typed result has been consumed.
    ResultTaken,
}

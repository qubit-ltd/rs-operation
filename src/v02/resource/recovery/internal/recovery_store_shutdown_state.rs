// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Completion flags shared by recovery-store shutdown observers.
pub(in crate::v02::resource::recovery) struct RecoveryStoreShutdownState {
    /// Whether a caller has claimed the one-shot shutdown operation.
    pub(in crate::v02::resource::recovery) started: bool,
    /// Whether the store has finished processing all retained recoveries.
    pub(in crate::v02::resource::recovery) finished: bool,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::ExecutionPhase;
use crate::v02::report::ExternalProtocolFault;
use crate::v02::report::ResourceProgress;
use crate::v02::report::SettlementReport;
use crate::v02::report::StopReport;

/// Untyped lifecycle facts used to build operation snapshots.
pub(in crate::v02::operation) struct CoreFacts {
    /// Current execution phase.
    pub(in crate::v02::operation) execution: ExecutionPhase,
    /// First accepted stop report, if any.
    pub(in crate::v02::operation) stop: Option<StopReport>,
    /// Latest resource progress while resources remain unsettled.
    pub(in crate::v02::operation) resources: Option<ResourceProgress>,
    /// Immutable settlement report once settlement has completed.
    pub(in crate::v02::operation) settlement: Option<Arc<SettlementReport>>,
    /// First external release protocol fault, if any.
    pub(in crate::v02::operation) release_fault: Option<ExternalProtocolFault>,
    /// First external settlement protocol fault, if any.
    pub(in crate::v02::operation) settlement_fault: Option<ExternalProtocolFault>,
}

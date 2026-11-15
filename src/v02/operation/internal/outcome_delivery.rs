// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// One-shot notification that the typed outcome was consumed or abandoned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutcomeDelivery {
    /// The consumer took ownership of the published outcome.
    Delivered,
    /// The outcome was abandoned without being delivered to a consumer.
    Discarded,
}

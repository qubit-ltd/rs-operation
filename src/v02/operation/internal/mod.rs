// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod core_facts;
mod handle_parts;
mod helpers;
mod operation_core;
mod outcome_delivery;
mod outcome_slot;
mod settlement_future;
mod settlement_until;
mod stop_control;

const AVAILABILITY_PENDING: u8 = 0;
const AVAILABILITY_AVAILABLE: u8 = 1;
const AVAILABILITY_TAKEN: u8 = 2;
const AVAILABILITY_DISCARDED: u8 = 3;
const DELIVERY_PENDING: u8 = 0;
const DELIVERY_TAKEN: u8 = 1;
const DELIVERY_DISCARDED: u8 = 2;

pub(in crate::v02::operation) use handle_parts::HandleParts;
pub(crate) use operation_core::OperationCore;
pub(crate) use outcome_delivery::OutcomeDelivery;
pub(crate) use outcome_slot::OutcomeSlot;
pub(crate) use stop_control::StopControl;

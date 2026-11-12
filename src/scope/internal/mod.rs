// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state and polling helpers used by scope implementations.

mod child;
mod effect_state;
pub(super) mod poll;
mod state;

pub(super) use child::Child;
pub(super) use effect_state::EffectState;
pub(super) use state::State;

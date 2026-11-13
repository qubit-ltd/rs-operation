// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod clock;
mod control;
mod gate;

pub use clock::ManualClock;
pub use control::BusinessResult;
pub use control::FakeControl;
pub use gate::Gate;

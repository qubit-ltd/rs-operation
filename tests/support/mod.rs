// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod cleanup;
mod clock;
mod control;
mod driver;
mod gate;
mod probe;
mod waker;

pub use cleanup::FakeCleanup;
pub use cleanup::FakeCleanupFuture;
pub use clock::ClockAdvanceError;
pub use clock::ManualClock;
pub use control::BusinessResult;
pub use control::FakeControl;
pub use driver::ManualDriver;
pub use driver::TaskId;
pub use driver::recording_step;
pub use gate::Gate;
pub use gate::GateWait;
pub use probe::DropProbe;
pub use probe::DropProbeCounter;
pub use waker::CountingWaker;

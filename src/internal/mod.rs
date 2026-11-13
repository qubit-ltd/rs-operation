// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Narrow internal ports. These are intentionally not part of the public API.
#![allow(dead_code)] // Ports are consumed by the later driver and cleanup tasks.

// Preserves a rejected child control attachment.
mod attach_rejected;
// Defines the synchronous execution port.
mod blocking_executor;
// Preserves rejected synchronous work for its caller.
mod blocking_rejected;
// Defines the owned future type shared by internal ports.
mod box_future;
// Holds the capability to commit a registered child operation.
mod child_commit;
// Describes the child policy used during registration.
mod child_options;
// Defines the child registration port.
mod child_registry;
// Defines the child failure propagation policy.
mod child_requirement;
// Reserves child operation registration state.
mod child_reservation;
// Exposes cleanup registration and settlement observation.
mod cleanup_port;
// Rejects failed cleanup registration while preserving cleanup ownership.
mod cleanup_rejected;
// Defines the asynchronous execution port.
mod driver;
// Defines execution driver failure categories.
mod driver_error;
// Preserves rejected asynchronous work for its caller.
mod driver_rejected;
// Shared generation-based condition variable and async waker notifications.
pub(crate) mod notify;
// Single-consumer result state and repeatable terminal snapshots.
pub(crate) mod state;

pub(crate) use attach_rejected::AttachRejected;
pub(crate) use blocking_rejected::BlockingRejected;
pub(crate) use box_future::BoxFuture;
pub(crate) use child_commit::ChildCommit;
pub(crate) use child_options::ChildOptions;
pub(crate) use child_requirement::ChildRequirement;
pub(crate) use child_reservation::ChildReservation;
pub(crate) use cleanup_rejected::CleanupRejected;
pub(crate) use driver_error::DriverError;
pub(crate) use driver_rejected::DriverRejected;

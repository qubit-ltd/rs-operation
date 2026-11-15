// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Internal adapter types and shared lifecycle helpers.

use std::any::Any;

mod adapter_error;
mod adapter_error_kind;
mod adapter_reservation;
mod attached_child;
mod child_phase;
mod child_start_permit;
#[cfg(test)]
mod external_task;
#[cfg(test)]
mod external_task_control;
mod lifecycle;
#[cfg(test)]
mod lost_inputs;
mod managed_facts;
mod managed_operation_control;
mod sync;
#[cfg(test)]
mod task_state;

pub(crate) use adapter_error::AdapterError;
pub(crate) use adapter_error_kind::AdapterErrorKind;
pub(crate) use adapter_reservation::AdapterReservation;
pub(crate) use attached_child::AttachedChild;
pub(super) use child_phase::ChildPhase;
pub(crate) use child_start_permit::ChildStartPermit;
#[cfg(test)]
pub(crate) use external_task::ExternalTask;
#[cfg(test)]
pub(crate) use external_task::recover_lost_input;
#[cfg(test)]
pub(crate) use external_task_control::ExternalTaskControl;
pub(super) use lifecycle::Lifecycle;
#[cfg(test)]
pub(super) use lost_inputs::LOST_INPUTS;
pub(super) use managed_facts::ManagedFacts;
pub(crate) use managed_operation_control::ManagedOperationControl;
pub(super) use sync::lock;
pub(super) use sync::wake_all;
#[cfg(test)]
pub(super) use task_state::TaskState;
#[cfg(test)]
pub(super) use task_state::poll_fact;

/// Starts an external child with its committed opaque payload.
type ActivationCallback = Box<dyn FnOnce(Box<dyn Any + Send>) + Send>;

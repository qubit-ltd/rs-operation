// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private implementation helpers for operation handles and supervision.

mod blocking_body_result;
mod blocking_completion;
mod cancellable;
mod catch_unwind;
mod cleanup;
mod cleanup_facts;
mod current_thread_waker;
mod shared;
mod state;

pub(crate) use blocking_body_result::BlockingBodyResult;
pub(crate) use blocking_completion::BlockingCompletion;
pub(crate) use cancellable::Cancellable;
pub(crate) use catch_unwind::CatchUnwind;
pub(crate) use cleanup::cleanup_scope_tree;
pub(crate) use cleanup_facts::CleanupFacts;
pub(crate) use current_thread_waker::CurrentThreadWaker;
pub(crate) use shared::Shared;
pub(crate) use state::State;

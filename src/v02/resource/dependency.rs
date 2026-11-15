// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Resource cleanup dependency graph.
#![allow(dead_code)] // Consumed by T11 resource-scope wiring.

mod dependency_graph;
mod internal;

pub(crate) use dependency_graph::DependencyGraph;

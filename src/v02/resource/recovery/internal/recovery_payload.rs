// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::sync::Arc;

/// Type-erased payload retained until a recovery action resolves.
pub(crate) type RecoveryPayload = Arc<dyn Any + Send + Sync + 'static>;

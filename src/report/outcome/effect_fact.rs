// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Certainty asserted by the effect's source, not inferred by this framework.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::EffectFact;
///
/// let confirmed = EffectFact::Known;
/// let uncertain = EffectFact::Unknown;
/// assert_ne!(confirmed, uncertain);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectFact {
    /// The effect source asserts that the described fact is known.
    Known,
    /// The effect source cannot confirm whether the described fact occurred.
    Unknown,
}

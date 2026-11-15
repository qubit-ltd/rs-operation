// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Certainty of an effect record's description.
///
/// # Examples
///
/// Keep uncertain effects distinct when interpreting an effect record:
///
/// ```
/// use qubit_operation::EffectFact;
///
/// fn is_confirmed(fact: EffectFact) -> bool {
///     matches!(fact, EffectFact::Known)
/// }
///
/// assert!(is_confirmed(EffectFact::Known));
/// assert!(!is_confirmed(EffectFact::Unknown));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectFact {
    /// The recorded effect is known to have occurred.
    Known,
    /// Available evidence cannot determine whether the effect occurred.
    Unknown,
}

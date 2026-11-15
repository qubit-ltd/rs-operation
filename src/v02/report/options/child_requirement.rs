// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// States whether a child operation is necessary for its parent to succeed.
///
/// # Examples
///
/// ```
/// use qubit_operation::ChildRequirement;
///
/// let requirement = ChildRequirement::Optional;
/// let contributes_to_parent_failure = match requirement {
///     ChildRequirement::Required => true,
///     ChildRequirement::Optional => false,
/// };
/// assert!(!contributes_to_parent_failure);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildRequirement {
    /// A failed child contributes to parent failure.
    Required,
    /// A failed child is reported but does not fail the parent by itself.
    Optional,
}

impl Default for ChildRequirement {
    /// Constructs the default configuration for [`ChildRequirement`].
    fn default() -> Self {
        Self::Required
    }
}

#[cfg(test)]
mod tests {
    use super::ChildRequirement;

    #[test]
    fn test_default_child_requirement_is_required() {
        assert_eq!(ChildRequirement::default(), ChildRequirement::Required);
    }

    #[test]
    fn test_child_requirement_debug_names_each_variant() {
        assert_eq!(format!("{:?}", ChildRequirement::Required), "Required");
        assert_eq!(format!("{:?}", ChildRequirement::Optional), "Optional");
    }
}

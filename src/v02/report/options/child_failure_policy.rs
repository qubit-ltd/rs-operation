// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Determines whether a failed child prevents its siblings from continuing.
///
/// # Examples
///
/// ```
/// use qubit_operation::ChildFailurePolicy;
///
/// let policy = ChildFailurePolicy::ContinueSiblings;
/// let description = match policy {
///     ChildFailurePolicy::StopSiblings => "stop sibling operations",
///     ChildFailurePolicy::ContinueSiblings => "continue sibling operations",
/// };
/// assert_eq!(description, "continue sibling operations");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildFailurePolicy {
    /// Request sibling shutdown after a required child fails.
    StopSiblings,
    /// Let sibling operations continue after a child fails.
    ContinueSiblings,
}

impl Default for ChildFailurePolicy {
    /// Constructs the default configuration for [`ChildFailurePolicy`].
    fn default() -> Self {
        Self::StopSiblings
    }
}

#[cfg(test)]
mod tests {
    use super::ChildFailurePolicy;

    #[test]
    fn test_default_child_failure_policy_stops_siblings() {
        assert_eq!(ChildFailurePolicy::default(), ChildFailurePolicy::StopSiblings);
    }

    #[test]
    fn test_child_failure_policy_debug_names_each_variant() {
        assert_eq!(format!("{:?}", ChildFailurePolicy::StopSiblings), "StopSiblings");
        assert_eq!(
            format!("{:?}", ChildFailurePolicy::ContinueSiblings),
            "ContinueSiblings"
        );
    }
}

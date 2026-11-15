// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

/// Capacity used when creating a child resource scope.
///
/// # Examples
///
/// ```
/// use qubit_operation::ResourceScopeOptions;
///
/// let options = ResourceScopeOptions { capacity: 12 };
/// assert_eq!(options.capacity, 12);
/// ```
pub struct ResourceScopeOptions {
    /// Maximum resource entries accepted by the child scope.
    pub capacity: usize,
}

impl fmt::Debug for ResourceScopeOptions {
    /// Formats this value for diagnostic inspection.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the options representation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceScopeOptions")
            .field("capacity", &self.capacity)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::ResourceScopeOptions;

    #[test]
    fn test_resource_scope_options_debug_includes_capacity() {
        let options = ResourceScopeOptions { capacity: 12 };
        assert_eq!(format!("{options:?}"), "ResourceScopeOptions { capacity: 12 }");
    }
}

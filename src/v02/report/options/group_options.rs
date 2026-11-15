// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

/// Capacity used when creating a resource group.
///
/// # Examples
///
/// ```
/// use qubit_operation::GroupOptions;
///
/// let options = GroupOptions { capacity: 8 };
/// assert_eq!(options.capacity, 8);
/// ```
pub struct GroupOptions {
    /// Maximum resources that may join the group.
    pub capacity: usize,
}

impl fmt::Debug for GroupOptions {
    /// Formats this value for diagnostic inspection.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the options representation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupOptions")
            .field("capacity", &self.capacity)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::GroupOptions;

    #[test]
    fn test_group_options_debug_includes_capacity() {
        let options = GroupOptions { capacity: 8 };
        assert_eq!(format!("{options:?}"), "GroupOptions { capacity: 8 }");
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

/// Capacity used when creating a service resource scope.
///
/// # Examples
///
/// ```
/// use qubit_operation::ServiceResourceOptions;
///
/// let options = ServiceResourceOptions { capacity: 16 };
/// assert_eq!(options.capacity, 16);
/// ```
pub struct ServiceResourceOptions {
    /// Maximum service resources retained by the scope.
    pub capacity: usize,
}

impl fmt::Debug for ServiceResourceOptions {
    /// Formats this value for diagnostic inspection.
    ///
    /// # Parameters
    ///
    /// - `self`: Options whose capacity is being formatted.
    /// - `f`: Formatter receiving the debug representation.
    ///
    /// # Returns
    ///
    /// A formatting result indicating whether writing succeeded.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceResourceOptions")
            .field("capacity", &self.capacity)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::ServiceResourceOptions;

    #[test]
    fn test_service_resource_options_debug_includes_capacity() {
        let options = ServiceResourceOptions { capacity: 16 };
        assert_eq!(format!("{options:?}"), "ServiceResourceOptions { capacity: 16 }");
    }
}

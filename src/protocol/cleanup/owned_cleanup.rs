// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::future::Future;
use core::pin::Pin;

use super::CleanupContext;
use crate::report::CleanupOutcome;

/// Owned cleanup action; the future cannot borrow the business stack.
///
/// # Examples
///
/// ```
/// use core::future::Future;
/// use core::pin::Pin;
/// use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
/// use qubit_operation::report::CleanupOutcome;
///
/// struct CloseFile;
///
/// impl OwnedCleanup for CloseFile {
///     fn begin(
///         self: Box<Self>,
///         _context: CleanupContext,
///     ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
///         Box::pin(async { CleanupOutcome::Cleaned })
///     }
/// }
///
/// let cleanup: Box<dyn OwnedCleanup> = Box::new(CloseFile);
/// let _future = cleanup.begin(CleanupContext { deadline: None });
/// ```
///
/// Borrowed owners cannot cross the `'static` boundary:
/// ```compile_fail
/// use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
/// use qubit_operation::report::CleanupOutcome;
/// use std::{future::Future, pin::Pin};
/// struct Borrowed<'a>(&'a str);
/// impl<'a> OwnedCleanup for Borrowed<'a> {
///     fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
///         Box::pin(async move { let _: &'static str = self.0; CleanupOutcome::Cleaned })
///     }
/// }
/// ```
///
/// Non-Send owners cannot cross the managed cleanup boundary:
/// ```compile_fail
/// use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
/// use qubit_operation::report::CleanupOutcome;
/// use std::{future::Future, pin::Pin, rc::Rc};
/// struct Local(Rc<()>);
/// impl OwnedCleanup for Local {
///     fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
///         Box::pin(async move { let _ = self.0; CleanupOutcome::Cleaned })
///     }
/// }
/// ```
pub trait OwnedCleanup: Send + 'static {
    /// Consume the cleanup action and produce an owned, independently driven
    /// future. The future must report only confirmed cleanup, accepted
    /// transfer, or a failure that retains recovery ownership.
    ///
    /// # Parameters
    ///
    /// - `self`: The boxed cleanup owner whose ownership is transferred into
    ///   the returned future.
    /// - `context`: Cleanup-specific deadline authority, independent of
    ///   business cancellation.
    ///
    /// # Returns
    ///
    /// An owned future that performs cleanup when it is driven to completion.
    fn begin(
        self: Box<Self>,
        context: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>;
}

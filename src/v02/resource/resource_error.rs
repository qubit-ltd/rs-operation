use std::fmt;

use crate::v02::report::ResourceManageErrorKind;

pub struct ManageResourceError<T> {
    kind: ResourceManageErrorKind,
    resource: T,
}
impl<T> ManageResourceError<T> {
    pub(crate) fn new(kind: ResourceManageErrorKind, resource: T) -> Self {
        Self { kind, resource }
    }
    pub fn kind(&self) -> ResourceManageErrorKind {
        self.kind
    }
    pub fn into_parts(self) -> (ResourceManageErrorKind, T) {
        (self.kind, self.resource)
    }
}

/// Failed asynchronous registration that preserves both caller-owned inputs.
#[must_use = "the resource and cleanup strategy must be retained or handled"]
pub struct ManageWithError<T, C> {
    kind: ResourceManageErrorKind,
    resource: T,
    cleanup: C,
}
impl<T, C> ManageWithError<T, C> {
    pub(super) fn new(kind: ResourceManageErrorKind, resource: T, cleanup: C) -> Self {
        Self {
            kind,
            resource,
            cleanup,
        }
    }
    pub fn kind(&self) -> ResourceManageErrorKind {
        self.kind
    }
    pub fn into_parts(self) -> (ResourceManageErrorKind, T, C) {
        (self.kind, self.resource, self.cleanup)
    }
}
impl<T, C> fmt::Debug for ManageWithError<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManageWithError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}
impl<T> fmt::Debug for ManageResourceError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManageResourceError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Failed commit retaining both the resource and the original reservations.
pub struct ResourceCommitError<T> {
    reason: ResourceManageErrorKind,
    resource: T,
    reservation: super::ResourceReservation,
}
impl<T> ResourceCommitError<T> {
    pub(super) fn new(reason: ResourceManageErrorKind, resource: T, reservation: super::ResourceReservation) -> Self {
        Self {
            reason,
            resource,
            reservation,
        }
    }
    pub fn reason(&self) -> ResourceManageErrorKind {
        self.reason
    }
    pub fn into_parts(self) -> (ResourceManageErrorKind, T, super::ResourceReservation) {
        (self.reason, self.resource, self.reservation)
    }
}

/// Failed commit that returns the resource, cleanup strategy, and reservation.
#[must_use = "a rejected resource and its cleanup strategy must be retained or handled"]
pub struct ResourceCommitWithError<T, C> {
    reason: ResourceManageErrorKind,
    resource: T,
    cleanup: C,
    reservation: super::ResourceReservation,
}
impl<T, C> ResourceCommitWithError<T, C> {
    pub(super) fn new(
        reason: ResourceManageErrorKind,
        resource: T,
        cleanup: C,
        reservation: super::ResourceReservation,
    ) -> Self {
        Self {
            reason,
            resource,
            cleanup,
            reservation,
        }
    }
    pub fn reason(&self) -> ResourceManageErrorKind {
        self.reason
    }
    pub fn into_parts(self) -> (ResourceManageErrorKind, T, C, super::ResourceReservation) {
        (self.reason, self.resource, self.cleanup, self.reservation)
    }
}
impl<T, C> fmt::Debug for ResourceCommitWithError<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceCommitWithError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

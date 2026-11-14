use crate::v02::report::ResourceManageErrorKind;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceCapacityError {
    Exhausted,
    ScopeEnded,
    CleanupQueueFull,
    CleanupQueueClosed,
}
impl fmt::Display for ResourceCapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource capacity unavailable: {self:?}")
    }
}
impl std::error::Error for ResourceCapacityError {}

pub struct ManageResourceError<T> {
    kind: ResourceManageErrorKind,
    resource: T,
}
impl<T> ManageResourceError<T> {
    pub(super) fn new(kind: ResourceManageErrorKind, resource: T) -> Self {
        Self { kind, resource }
    }
    pub fn kind(&self) -> ResourceManageErrorKind {
        self.kind
    }
    pub fn into_parts(self) -> (ResourceManageErrorKind, T) {
        (self.kind, self.resource)
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
    pub(super) fn new(
        reason: ResourceManageErrorKind,
        resource: T,
        reservation: super::ResourceReservation,
    ) -> Self {
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

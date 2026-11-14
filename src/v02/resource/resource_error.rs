use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceAccessError {
    ScopeEnded,
    StaleKey,
    WrongScope,
    WrongManager,
    WrongType,
}
impl fmt::Display for ResourceAccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource access denied: {self:?}")
    }
}
impl std::error::Error for ResourceAccessError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceCapacityError {
    Exhausted,
    ScopeEnded,
}
impl fmt::Display for ResourceCapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource capacity unavailable: {self:?}")
    }
}
impl std::error::Error for ResourceCapacityError {}

/// Failed commit that returns both the resource and its still-valid reservation.
pub struct ResourceCommitError<T> {
    reason: ResourceCapacityError,
    resource: T,
    reservation: super::ResourceReservation,
}
impl<T> ResourceCommitError<T> {
    pub(super) fn new(
        reason: ResourceCapacityError,
        resource: T,
        reservation: super::ResourceReservation,
    ) -> Self {
        Self {
            reason,
            resource,
            reservation,
        }
    }
    pub fn reason(&self) -> ResourceCapacityError {
        self.reason
    }
    pub fn into_parts(self) -> (ResourceCapacityError, T, super::ResourceReservation) {
        (self.reason, self.resource, self.reservation)
    }
}

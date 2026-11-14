use crate::v02::manager::capacity::ResourceRegistration;

/// Capacity reserved before constructing a resource.
pub struct ResourceReservation {
    pub(crate) registration: Option<ResourceRegistration>,
}
impl ResourceReservation {
    pub(crate) fn new(registration: ResourceRegistration) -> Self {
        Self {
            registration: Some(registration),
        }
    }
    pub(crate) fn take(&mut self) -> Option<ResourceRegistration> {
        self.registration.take()
    }
}

//! Long-lived resources shared across operations in an application group.

use super::{ManageResourceError, ResourceKey, ResourceRef, ResourceScope};
use crate::v02::report::ResourceLookupError;

/// A separately bounded resource owner whose close waits for its last user.
pub struct ServiceResourceScope {
    resources: ResourceScope,
}

impl ServiceResourceScope {
    pub(crate) fn new(resources: ResourceScope) -> Self {
        Self { resources }
    }

    pub fn manage<T: Send + Sync + 'static>(
        &self,
        resource: T,
    ) -> Result<ResourceRef<T>, ManageResourceError<T>> {
        self.resources.manage(resource)
    }

    pub fn get_resource<T: Send + Sync + 'static>(
        &self,
        key: ResourceKey<T>,
    ) -> Result<ResourceRef<T>, ResourceLookupError> {
        self.resources.get_resource(key)
    }

    pub fn close(&self) {
        self.resources.end();
    }
}

#[cfg(test)]
mod tests {
    use crate::v02::manager::core::OperationManager;
    use crate::v02::report::{
        GroupOptions, ManagerOptions, ServiceResourceOptions, ShutdownMode,
        ThreadPoolExecutorOptions,
    };
    use std::sync::mpsc;
    use std::time::Duration;

    struct DropNotice(Option<mpsc::Sender<()>>);
    impl Drop for DropNotice {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                sender.send(()).unwrap();
            }
        }
    }

    #[test]
    fn service_refs_delay_close() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
        let service = group
            .resource_scope(ServiceResourceOptions { capacity: 1 })
            .unwrap();
        assert_eq!(group.snapshot().service_resource_scopes, 1);
        let (drop_tx, drop_rx) = mpsc::channel();
        let value = service.manage(DropNotice(Some(drop_tx))).unwrap();
        let key = value.key();
        drop(value);
        let held = service.get_resource(key).unwrap();
        let shutdown = manager.request_shutdown(ShutdownMode::Drain);
        assert_eq!(shutdown.snapshot().service_resources, 1);
        assert!(drop_rx.try_recv().is_err());
        assert!(matches!(
            shutdown.blocking().wait_for(Duration::ZERO),
            crate::v02::report::ShutdownWait::Pending(_)
        ));
        drop(held);
        drop_rx
            .recv()
            .expect("last service reference enables cleanup");
        assert!(matches!(
            shutdown.blocking().wait_for(Duration::from_secs(2)),
            crate::v02::report::ShutdownWait::Completed(_)
        ));
        assert_eq!(group.snapshot().service_resource_scopes, 0);
    }

    #[test]
    fn bounded_cleanup_queue_is_consumed_after_worker_capacity_returns() {
        use std::sync::Arc;
        use std::time::Duration;
        struct Gate {
            open: std::sync::Mutex<bool>,
            changed: std::sync::Condvar,
        }
        impl Gate {
            fn wait(&self) {
                let mut open = self.open.lock().unwrap();
                while !*open {
                    open = self.changed.wait(open).unwrap();
                }
            }
            fn open(&self) {
                *self.open.lock().unwrap() = true;
                self.changed.notify_all();
            }
        }
        struct Probe {
            id: usize,
            starts: mpsc::Sender<usize>,
            gate: Arc<Gate>,
        }
        impl Drop for Probe {
            fn drop(&mut self) {
                let _ = self.starts.send(self.id);
                self.gate.wait();
            }
        }
        let mut options = ManagerOptions::default();
        options.cleanup_executor = ThreadPoolExecutorOptions {
            worker_count: 1,
            queue_capacity: 1,
        };
        let manager = OperationManager::new(options).unwrap();
        let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
        let service = group
            .resource_scope(ServiceResourceOptions { capacity: 3 })
            .unwrap();
        let (starts_tx, starts_rx) = mpsc::channel();
        let gate = Arc::new(Gate {
            open: std::sync::Mutex::new(false),
            changed: std::sync::Condvar::new(),
        });
        let mut refs = Vec::new();
        for id in 0..3 {
            let sender = starts_tx.clone();
            refs.push(
                service
                    .manage(Probe {
                        id,
                        starts: sender,
                        gate: Arc::clone(&gate),
                    })
                    .unwrap(),
            );
        }
        service.close();
        drop(refs);
        let _running = starts_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("cleanup worker starts first job");
        gate.open();
        let mut observed = vec![_running];
        while observed.len() < 3 {
            observed.push(
                starts_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("pump retries pending cleanup after permit return"),
            );
        }
        observed.sort_unstable();
        assert_eq!(observed, vec![0, 1, 2]);
        let shutdown = manager.request_shutdown(ShutdownMode::Drain);
        assert!(matches!(
            shutdown.blocking().wait_for(Duration::from_secs(2)),
            crate::v02::report::ShutdownWait::Completed(_)
        ));
    }
}

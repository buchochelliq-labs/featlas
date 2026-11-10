//! Adapter implementations for featlas.
//!
//! The core depends on none of this crate. Adapters are registered under
//! their exact namespaced implementation and version; deployments select them
//! by those identifiers. Vendor-specific code (Azure Databricks Spark SQL
//! lowering and Unity Catalog statements) lives only here and in the thin
//! Python host.

pub mod databricks;
pub mod databricks_caps;
pub mod local;

use featlas_core::capability::{AdapterDescriptor, AdapterRegistry};
use featlas_core::consumer::{PublicationStore, StoreRegistry};

pub struct Registry {
    adapters: Vec<Box<dyn AdapterDescriptor>>,
}

impl Default for Registry {
    fn default() -> Self {
        Registry {
            adapters: vec![
                Box::new(local::LocalReference),
                Box::new(local::MemoryStoreAdapter),
                Box::new(databricks::Databricks),
            ],
        }
    }
}

impl AdapterRegistry for Registry {
    fn get(&self, implementation: &str, version: &str) -> Option<&dyn AdapterDescriptor> {
        self.adapters
            .iter()
            .find(|a| a.implementation() == implementation && a.version() == version)
            .map(|a| a.as_ref())
    }
    fn list(&self) -> Vec<(String, String)> {
        self.adapters
            .iter()
            .map(|a| (a.implementation().to_string(), a.version().to_string()))
            .collect()
    }
}

/// Store registry backed by local stores (the Databricks store is driven by the Python host).
pub struct LocalStores {
    pub memory: local::InMemoryStore,
    pub files: local::JsonlFileStore,
}

impl LocalStores {
    pub fn new(clock: impl Fn() -> String + Send + Sync + Clone + 'static) -> Self {
        LocalStores {
            memory: local::InMemoryStore::new(Box::new(clock.clone())),
            files: local::JsonlFileStore {
                clock: Box::new(clock),
            },
        }
    }
}

impl StoreRegistry for LocalStores {
    fn store(&self, implementation: &str, version: &str) -> Option<&dyn PublicationStore> {
        match (implementation, version) {
            (local::MEMORY_IMPLEMENTATION, local::VERSION) => Some(&self.memory),
            (local::LOCAL_IMPLEMENTATION, local::VERSION) => Some(&self.files),
            _ => None,
        }
    }
}

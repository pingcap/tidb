// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! PD's two metric packages, with one shared initialization/consumer lifecycle.
//! Definitions and prebound observers are generated from the pinned Go packages.
//! Construction is unregistered; the first PD initialization chooses labels.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use prometheus::Registry;

#[path = "metrics/pd_definitions.rs"]
mod pd_definitions;
#[path = "metrics/resource_group_definitions.rs"]
mod resource_group_definitions;

pub use pd_definitions::Metrics;

/// A consumer is called immediately and again when labeled metrics are installed.
/// Keep only collector handles from this snapshot; callbacks run serially.
pub type Consumer = Box<dyn Fn(&Metrics) + Send + Sync>;

struct MetricsOwner {
    initialized: AtomicBool,
    metrics: RwLock<Arc<Metrics>>,
    consumers: Mutex<Vec<Consumer>>,
    resource_group: Arc<ResourceGroupOwner>,
}

impl MetricsOwner {
    fn new(resource_group: Arc<ResourceGroupOwner>) -> Self {
        Self {
            initialized: AtomicBool::new(false),
            metrics: RwLock::new(Arc::new(Metrics::new(HashMap::new()).unwrap())),
            consumers: Mutex::new(Vec::new()),
            resource_group,
        }
    }

    fn snapshot(&self) -> Arc<Metrics> {
        self.metrics
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn register_consumer(&self, consumer: Consumer) {
        let mut consumers = self.consumers.lock().unwrap_or_else(|e| e.into_inner());
        // Keep the consumer even if its first invocation panics, like Go's append.
        consumers.push(consumer);
        consumers.last().unwrap()(&self.snapshot());
    }

    fn init_and_register(&self, labels: HashMap<String, String>, registry: &Registry) {
        if self
            .initialized
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let metrics = Arc::new(Metrics::new(labels.clone()).expect("invalid PD metrics"));
        {
            // Publishing and rebinding share the registration lock, so a racing
            // consumer cannot miss the new generation or bind an older one last.
            let consumers = self.consumers.lock().unwrap_or_else(|e| e.into_inner());
            *self.metrics.write().unwrap_or_else(|e| e.into_inner()) = metrics.clone();
            for consumer in consumers.iter() {
                consumer(&metrics);
            }
        }
        metrics
            .register_metrics(registry)
            .expect("PD metric registration failed");
        self.resource_group.init_and_register(labels, registry);
    }
}

struct ResourceGroupOwner {
    metrics: RwLock<Arc<resource_group_definitions::Metrics>>,
}

impl ResourceGroupOwner {
    fn new() -> Self {
        Self {
            metrics: RwLock::new(Arc::new(
                resource_group_definitions::Metrics::new(HashMap::new()).unwrap(),
            )),
        }
    }

    fn snapshot(&self) -> Arc<resource_group_definitions::Metrics> {
        self.metrics
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn init_and_register(&self, labels: HashMap<String, String>, registry: &Registry) {
        let metrics = Arc::new(
            resource_group_definitions::Metrics::new(labels)
                .expect("invalid resource-group metrics"),
        );
        *self.metrics.write().unwrap_or_else(|e| e.into_inner()) = metrics.clone();
        // This package has no once guard in Go. Duplicate registration fails.
        metrics
            .register_metrics(registry)
            .expect("resource-group metric registration failed");
    }
}

lazy_static::lazy_static! {
    static ref RESOURCE_GROUP: Arc<ResourceGroupOwner> = Arc::new(ResourceGroupOwner::new());
    static ref OWNER: MetricsOwner = MetricsOwner::new(RESOURCE_GROUP.clone());
}

/// Snapshot of PD's current collectors and prebound observers.
pub fn global_metrics() -> Arc<Metrics> {
    OWNER.snapshot()
}

/// Register and initialize a consumer, as in PD `metrics.RegisterConsumer`.
pub fn register_consumer(consumer: Consumer) {
    OWNER.register_consumer(consumer);
}

/// Initialize both packages and register them once in the default registry.
/// The first caller chooses the constant labels; registration failures panic.
pub fn init_and_register_metrics(labels: HashMap<String, String>) {
    OWNER.init_and_register(labels, prometheus::default_registry());
}

/// Go's `resource_group/controller/metrics` package.
pub mod resource_group {
    pub use super::resource_group_definitions::Metrics;
    use super::*;

    /// Snapshot of the resource-group collectors and prebound observers.
    pub fn global_metrics() -> Arc<Metrics> {
        RESOURCE_GROUP.snapshot()
    }

    /// Reinitialize and register resource-group metrics, without a once guard.
    pub fn init_and_register_metrics(labels: HashMap<String, String>) {
        RESOURCE_GROUP.init_and_register(labels, prometheus::default_registry());
    }
}

#[cfg(test)]
#[path = "metrics/tests.rs"]
mod tests;

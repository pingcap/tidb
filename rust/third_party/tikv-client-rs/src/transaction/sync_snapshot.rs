use crate::transaction::sync_client::safe_block_on;
use crate::{
    BoundRange, Key, KvPair, Priority, ReplicaReadAdjuster, ReplicaReadConfig, ReplicaReadType,
    Result, RpcInterceptorHandle, RuDetails, Snapshot, Value,
};
use std::sync::Arc;
use std::time::Duration;

/// A synchronous read-only snapshot.
///
/// This is a wrapper around the async [`Snapshot`] that provides blocking methods.
/// All operations block the current thread until completed.
pub struct SyncSnapshot {
    inner: Snapshot,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl SyncSnapshot {
    pub(crate) fn new(inner: Snapshot, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self { inner, runtime }
    }

    /// Set the priority for subsequent read requests.
    pub fn set_priority(&mut self, priority: Priority) {
        self.inner.set_priority(priority);
    }

    /// Choose the TiKV replica-read type for subsequent snapshot reads.
    pub fn set_replica_read(&mut self, read_type: ReplicaReadType) {
        self.inner.set_replica_read(read_type);
    }

    /// Choose replica-read type plus stable selector constraints.
    pub fn set_replica_read_config(&mut self, config: ReplicaReadConfig) {
        self.inner.set_replica_read_config(config);
    }

    /// Mark subsequent snapshot reads as stale reads.
    pub fn set_stale_read(&mut self, stale_read: bool) {
        self.inner.set_stale_read(stale_read);
    }

    /// Replace store-label constraints used by subsequent replica selection.
    pub fn set_match_store_labels(
        &mut self,
        labels: impl IntoIterator<Item = crate::proto::metapb::StoreLabel>,
    ) {
        self.inner.set_match_store_labels(labels);
    }

    /// Set the TiKV queue-wait threshold that permits load-based replica
    /// selection for subsequent snapshot reads.
    pub fn set_load_based_replica_read_threshold(&mut self, busy_threshold: Duration) {
        self.inner
            .set_load_based_replica_read_threshold(busy_threshold);
    }

    /// Set the per-get/batch-get replica selector adjustment callback.
    pub fn set_replica_read_adjuster(&mut self, adjuster: ReplicaReadAdjuster) {
        self.inner.set_replica_read_adjuster(adjuster);
    }

    /// Replace the RPC interceptor used by subsequent snapshot requests.
    pub fn set_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        self.inner.set_rpc_interceptor(interceptor);
    }

    /// Add an RPC interceptor after the existing snapshot interceptor chain.
    pub fn add_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        self.inner.add_rpc_interceptor(interceptor);
    }

    /// Set the source-compatible resource group on subsequent snapshot RPCs.
    pub fn set_resource_group_name(&mut self, resource_group_name: impl Into<String>) {
        self.inner.set_resource_group_name(resource_group_name);
    }

    /// Attach a PD resource-group controller to subsequent snapshot RPCs.
    pub fn set_resource_control(&mut self, controller: crate::ResourceGroupControllerHandle) {
        self.inner.set_resource_control(controller);
    }

    /// Attach resource-unit accounting to subsequent snapshot RPCs.
    pub fn set_ru_details(&mut self, ru_details: Arc<RuDetails>) {
        self.inner.set_ru_details(ru_details);
    }

    /// Get the value associated with the given key.
    pub fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        safe_block_on(&self.runtime, self.inner.get(key))
    }

    /// Check whether the key exists.
    pub fn key_exists(&mut self, key: impl Into<Key>) -> Result<bool> {
        safe_block_on(&self.runtime, self.inner.key_exists(key))
    }

    /// Get the values associated with the given keys.
    pub fn batch_get(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.batch_get(keys))
    }

    /// Scan a range, return at most `limit` key-value pairs that lie in the range.
    pub fn scan(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.scan(range, limit))
    }

    /// Scan a range, return at most `limit` keys that lie in the range.
    pub fn scan_keys(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        safe_block_on(&self.runtime, self.inner.scan_keys(range, limit))
    }

    /// Similar to scan, but in the reverse direction.
    pub fn scan_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.scan_reverse(range, limit))
    }

    /// Similar to scan_keys, but in the reverse direction.
    pub fn scan_keys_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        safe_block_on(&self.runtime, self.inner.scan_keys_reverse(range, limit))
    }
}

use crate::pd::{PdClient, PdRpcClient};
use crate::transaction::sync_client::safe_block_on;
use crate::{
    transaction::Mutation, BoundRange, Key, KvPair, Priority, Result, RpcInterceptorHandle,
    RuDetails, Timestamp, Transaction, Value,
};
use std::sync::Arc;

/// A synchronous transaction.
///
/// This is a wrapper around the async [`Transaction`] that provides blocking methods.
/// All operations block the current thread until completed.
///
/// The PD client is a type parameter, mirroring [`Transaction`], so downstream
/// synchronous consumers (client-go's `KVTxn` is natively synchronous, and
/// TiDB's store driver is written against that shape) can run the same wrapper
/// over an injected in-process client instead of only over a live cluster.
pub struct SyncTransaction<PdC: PdClient = PdRpcClient> {
    inner: Transaction<PdC>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl<PdC: PdClient> SyncTransaction<PdC> {
    /// Wraps one async transaction with a runtime that blocks on its futures.
    pub fn new(inner: Transaction<PdC>, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self { inner, runtime }
    }

    /// Borrows the wrapped asynchronous transaction.
    pub fn inner_mut(&mut self) -> &mut Transaction<PdC> {
        &mut self.inner
    }

    /// Runs one asynchronous transaction operation to completion on this
    /// wrapper's runtime, under the same nested-runtime guard as every other
    /// method here.
    ///
    /// The blocking methods below cover client-go's `KVTxn` surface; this is
    /// the escape hatch for the option-carrying variants (mutation
    /// assertions, prewrite constraint checks) that a store driver needs
    /// without having to build a second runtime beside this one.
    pub fn block_on<'a, F, T>(
        &'a mut self,
        operation: impl FnOnce(&'a mut Transaction<PdC>) -> F,
    ) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>> + 'a,
    {
        let runtime = self.runtime.clone();
        safe_block_on(&runtime, operation(&mut self.inner))
    }

    /// Set the priority for subsequent read and write requests.
    pub fn set_priority(&mut self, priority: Priority) {
        self.inner.set_priority(priority);
    }

    /// Replace the RPC interceptor used by this transaction.
    pub fn set_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        self.inner.set_rpc_interceptor(interceptor);
    }

    /// Add an RPC interceptor after the existing transaction chain.
    pub fn add_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        self.inner.add_rpc_interceptor(interceptor);
    }

    /// Assign the source-compatible resource group to subsequent transaction RPCs.
    pub fn set_resource_group_name(&mut self, resource_group_name: impl Into<String>) {
        self.inner.set_resource_group_name(resource_group_name);
    }

    /// Attach a PD resource-group controller to subsequent transaction RPCs.
    pub fn set_resource_control(&mut self, controller: crate::ResourceGroupControllerHandle) {
        self.inner.set_resource_control(controller);
    }

    /// Attach resource-unit accounting to subsequent transaction RPCs.
    pub fn set_ru_details(&mut self, ru_details: Arc<RuDetails>) {
        self.inner.set_ru_details(ru_details);
    }

    /// Get the value associated with the given key.
    pub fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        safe_block_on(&self.runtime, self.inner.get(key))
    }

    /// Get the value associated with the given key, and lock the key.
    pub fn get_for_update(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        safe_block_on(&self.runtime, self.inner.get_for_update(key))
    }

    /// Check if the given key exists.
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

    /// Get the values associated with the given keys, and lock the keys.
    pub fn batch_get_for_update(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        safe_block_on(&self.runtime, self.inner.batch_get_for_update(keys))
    }

    /// Scan a range and return the key-value pairs.
    pub fn scan(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.scan(range, limit))
    }

    /// Scan a range and return only the keys.
    pub fn scan_keys(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        safe_block_on(&self.runtime, self.inner.scan_keys(range, limit))
    }

    /// Scan a range in reverse order.
    pub fn scan_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.scan_reverse(range, limit))
    }

    /// Scan keys in a range in reverse order.
    pub fn scan_keys_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        safe_block_on(&self.runtime, self.inner.scan_keys_reverse(range, limit))
    }

    /// Set the value associated with the given key.
    pub fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.put(key, value))
    }

    /// Insert the key-value pair. Returns an error if the key already exists.
    pub fn insert(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.insert(key, value))
    }

    /// Delete the given key.
    pub fn delete(&mut self, key: impl Into<Key>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.delete(key))
    }

    /// Apply multiple mutations atomically.
    pub fn batch_mutate(&mut self, mutations: impl IntoIterator<Item = Mutation>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.batch_mutate(mutations))
    }

    /// Returns the exact staged MemDB used by reads and commit.
    pub fn get_mem_buffer(&mut self) -> &mut crate::transaction::unionstore::MemDb {
        self.inner.get_mem_buffer()
    }

    /// Lock the given keys without associating any values.
    pub fn lock_keys(&mut self, keys: impl IntoIterator<Item = impl Into<Key>>) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.lock_keys(keys))
    }

    /// Commit the transaction.
    pub fn commit(&mut self) -> Result<Option<Timestamp>> {
        safe_block_on(&self.runtime, self.inner.commit())
    }

    /// Rollback the transaction.
    pub fn rollback(&mut self) -> Result<()> {
        safe_block_on(&self.runtime, self.inner.rollback())
    }

    /// Send a heart beat message to keep the transaction alive.
    pub fn send_heart_beat(&mut self) -> Result<u64> {
        safe_block_on(&self.runtime, self.inner.send_heart_beat())
    }
}

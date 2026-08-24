use crate::pd::{PdClient, PdRpcClient};
use crate::transaction::sync_client::safe_block_on;
use crate::{
    BoundRange, GetOption, Key, KvPair, Priority, ReplicaReadAdjuster, ReplicaReadConfig,
    ReplicaReadType, Result, RpcInterceptorHandle, RuDetails, Snapshot, Value, ValueEntry,
};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

/// A synchronous read-only snapshot.
///
/// This is a wrapper around the async [`Snapshot`] that provides blocking methods.
/// All operations block the current thread until completed.
pub struct SyncSnapshot<PdC: PdClient = PdRpcClient> {
    inner: Snapshot<PdC>,
    runtime: Arc<tokio::runtime::Runtime>,
}

/// Blocking counterpart of [`crate::SnapshotIterator`].
pub struct SyncSnapshotIterator<'a, PdC: PdClient = PdRpcClient> {
    snapshot: &'a mut SyncSnapshot<PdC>,
    range: BoundRange,
    reverse: bool,
    batch_size: u32,
    buffered: VecDeque<KvPair>,
    exhausted: bool,
    valid: bool,
}

impl<'a, PdC: PdClient> SyncSnapshotIterator<'a, PdC> {
    fn new(snapshot: &'a mut SyncSnapshot<PdC>, range: BoundRange, reverse: bool) -> Self {
        Self {
            batch_size: snapshot.inner.iterator_batch_size(),
            snapshot,
            range,
            reverse,
            buffered: VecDeque::new(),
            exhausted: false,
            valid: true,
        }
    }

    fn refill(&mut self) -> Result<()> {
        if !self.valid || !self.buffered.is_empty() {
            return Ok(());
        }
        if self.exhausted {
            self.valid = false;
            return Ok(());
        }
        let pairs = if self.reverse {
            safe_block_on(
                &self.snapshot.runtime,
                self.snapshot
                    .inner
                    .scan_reverse(self.range.clone(), self.batch_size),
            )?
            .collect::<Vec<_>>()
        } else {
            safe_block_on(
                &self.snapshot.runtime,
                self.snapshot
                    .inner
                    .scan(self.range.clone(), self.batch_size),
            )?
            .collect::<Vec<_>>()
        };
        if pairs.is_empty() {
            self.exhausted = true;
            self.valid = false;
            return Ok(());
        }
        self.exhausted = pairs.len() < self.batch_size as usize;
        let last_key = pairs.last().expect("non-empty scan batch").key().clone();
        if self.reverse {
            self.range.to = Bound::Excluded(last_key);
        } else {
            self.range.from = Bound::Included(last_key.next_key());
        }
        self.buffered = pairs.into();
        Ok(())
    }

    /// Fetch and return the next pair, or `None` after the scan is exhausted.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<KvPair>> {
        self.refill()?;
        Ok(self.buffered.pop_front())
    }

    pub fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn close(&mut self) {
        self.buffered.clear();
        self.exhausted = true;
        self.valid = false;
    }
}

impl<PdC: PdClient> SyncSnapshot<PdC> {
    pub(crate) fn new(inner: Snapshot<PdC>, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self { inner, runtime }
    }

    /// Set the priority for subsequent read requests.
    pub fn set_priority(&mut self, priority: Priority) {
        self.inner.set_priority(priority);
    }

    /// Set the TiKV scan sampling step for subsequent snapshot scans.
    pub fn set_sample_step(&mut self, sample_step: u32) {
        self.inner.set_sample_step(sample_step);
    }

    /// Return only keys from subsequent snapshot scans.
    pub fn set_key_only(&mut self, key_only: bool) {
        self.inner.set_key_only(key_only);
    }

    /// Set the maximum number of pairs requested by each TiKV scan RPC.
    pub fn set_scan_batch_size(&mut self, batch_size: u32) {
        self.inner.set_scan_batch_size(batch_size);
    }

    /// Attach runtime statistics to subsequent physical snapshot read RPCs.
    pub fn set_runtime_stats(&mut self, stats: Option<Arc<crate::SnapshotRuntimeStats>>) {
        self.inner.set_runtime_stats(stats);
    }

    /// Set retry variables for subsequent snapshot reads, matching client-go
    /// `KVSnapshot.SetVars`.
    pub fn set_variables(&mut self, variables: Arc<crate::Variables>) {
        self.inner.set_variables(variables);
    }

    /// Allow reads to proceed through locks flushed by this pipelined transaction.
    pub fn set_pipelined(&mut self, timestamp: u64) {
        self.inner.set_pipelined(timestamp);
    }

    /// Set the deadline for each physical snapshot read. A zero duration
    /// clears the override.
    pub fn set_kv_read_timeout(&mut self, timeout: Duration) {
        self.inner.set_kv_read_timeout(timeout);
    }

    /// Set the static TiKV resource-group tag for subsequent snapshot reads.
    pub fn set_resource_group_tag(&mut self, resource_group_tag: Option<Vec<u8>>) {
        self.inner.set_resource_group_tag(resource_group_tag);
    }

    /// Set the resource-group tag callback for subsequent snapshot reads.
    pub fn set_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<crate::SnapshotResourceGroupTagger>,
    ) {
        self.inner.set_resource_group_tagger(resource_group_tagger);
    }

    /// Set the transaction and replica-read scope for subsequent snapshot
    /// Get and BatchGet timestamp validation.
    pub fn set_read_replica_scope(&mut self, scope: impl Into<String>) {
        self.inner.set_read_replica_scope(scope);
    }

    /// Alias for [`Self::set_read_replica_scope`].
    pub fn set_txn_scope(&mut self, scope: impl Into<String>) {
        self.inner.set_txn_scope(scope);
    }

    /// Return the configured snapshot read deadline, if any.
    pub fn kv_read_timeout(&self) -> Option<Duration> {
        self.inner.kv_read_timeout()
    }

    /// Return the cumulative snapshot-cache hit count.
    pub fn snap_cache_hit_count(&self) -> usize {
        self.inner.snap_cache_hit_count()
    }

    /// Return the number of cached snapshot entries, including misses.
    pub fn snap_cache_size(&self) -> usize {
        self.inner.snap_cache_size()
    }

    /// Return a copy of the snapshot cache.
    pub fn snap_cache(&self) -> BTreeMap<Key, ValueEntry> {
        self.inner.snap_cache()
    }

    /// Seed snapshot-cache entries for the supplied keys.
    pub fn update_snapshot_cache(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        values: BTreeMap<Key, ValueEntry>,
    ) {
        self.inner.update_snapshot_cache(keys, values);
    }

    /// Remove cached snapshot entries for the supplied keys.
    pub fn clean_snapshot_cache(&mut self, keys: impl IntoIterator<Item = impl Into<Key>>) {
        self.inner.clean_snapshot_cache(keys);
    }

    /// Control whether TiKV should bypass cache population for subsequent
    /// snapshot reads.
    pub fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        self.inner.set_not_fill_cache(not_fill_cache);
    }

    /// Set the TiKV isolation level used by subsequent snapshot reads.
    pub fn set_isolation_level(&mut self, isolation_level: crate::proto::kvrpcpb::IsolationLevel) {
        self.inner.set_isolation_level(isolation_level);
    }

    /// Set TiKV's scheduling task ID for subsequent snapshot reads.
    pub fn set_task_id(&mut self, task_id: u64) {
        self.inner.set_task_id(task_id);
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

    /// Mark this snapshot as a staleness read.
    pub fn set_is_staleness_read_only(&mut self, stale_read: bool) {
        self.inner.set_is_staleness_read_only(stale_read);
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

    /// Get a value plus optional commit timestamp using source `GetOption`
    /// semantics.
    pub fn get_with_options(
        &mut self,
        key: impl Into<Key>,
        options: &[GetOption],
    ) -> Result<Option<ValueEntry>> {
        safe_block_on(&self.runtime, self.inner.get_with_options(key, options))
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

    /// Batch-get values plus optional commit timestamps using source
    /// `GetOption` semantics.
    pub fn batch_get_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        options: &[GetOption],
    ) -> Result<BTreeMap<Key, ValueEntry>> {
        safe_block_on(
            &self.runtime,
            self.inner.batch_get_with_options(keys, options),
        )
    }

    /// Read values from the pipelined transaction buffer tier.
    pub fn batch_get_from_buffer(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        safe_block_on(&self.runtime, self.inner.batch_get_from_buffer(keys))
    }

    /// Create and prefetch a blocking stateful forward scanner.
    pub fn iter(&mut self, range: impl Into<BoundRange>) -> Result<SyncSnapshotIterator<'_, PdC>> {
        let mut iterator = SyncSnapshotIterator::new(self, range.into(), false);
        iterator.refill()?;
        Ok(iterator)
    }

    /// Create and prefetch a blocking stateful reverse scanner.
    pub fn iter_reverse(
        &mut self,
        range: impl Into<BoundRange>,
    ) -> Result<SyncSnapshotIterator<'_, PdC>> {
        let mut iterator = SyncSnapshotIterator::new(self, range.into(), true);
        iterator.refill()?;
        Ok(iterator)
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

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};

    use super::SyncSnapshot;
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::proto::kvrpcpb;
    use crate::request::Keyspace;
    use crate::timestamp::TimestampExt;
    use crate::{KvPair, Snapshot, Timestamp, Transaction, TransactionOptions};

    #[test]
    fn sync_scanner_uses_pair_local_point_read_recovery() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::ScanRequest>() {
                    captured_requests.lock().unwrap().push("scan");
                    return Ok(Box::new(kvrpcpb::ScanResponse {
                        pairs: vec![
                            kvrpcpb::KvPair {
                                error: Some(kvrpcpb::KeyError {
                                    locked: Some(kvrpcpb::LockInfo {
                                        key: b"a".to_vec(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            kvrpcpb::KvPair {
                                key: b"b".to_vec(),
                                value: b"b-value".to_vec(),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    captured_requests.lock().unwrap().push("get");
                    assert_eq!(request.key, b"a");
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"a-value".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected sync scanner request")
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_scan_batch_size(2);
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let mut snapshot = SyncSnapshot::new(Snapshot::new(transaction), runtime);

        let mut iterator = snapshot.iter(b"a".to_vec()..b"z".to_vec()).unwrap();
        assert_eq!(
            iterator.next().unwrap(),
            Some(KvPair(b"a".to_vec().into(), b"a-value".to_vec()))
        );
        assert_eq!(
            iterator.next().unwrap(),
            Some(KvPair(b"b".to_vec().into(), b"b-value".to_vec()))
        );
        assert_eq!(*requests.lock().unwrap(), ["scan", "get"]);
    }
}

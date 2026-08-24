// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::ops::Bound;
use std::time::Duration;

use derive_new::new;
use log::{debug, trace};

use crate::pd::{PdClient, PdRpcClient};
use crate::BoundRange;
use crate::GetOption;
use crate::Key;
use crate::KvPair;
use crate::Priority;
use crate::Result;
use crate::RpcInterceptorHandle;
use crate::Transaction;
use crate::Value;
use crate::ValueEntry;
use crate::{ReplicaReadAdjuster, ReplicaReadConfig, ReplicaReadType};

/// A read-only transaction which reads at the given timestamp.
///
/// It behaves as if the snapshot was taken at the given timestamp,
/// i.e. it can read operations happened before the timestamp,
/// but ignores operations after the timestamp.
///
/// See the [Transaction](struct@crate::Transaction) docs for more information on the methods.
#[derive(new)]
pub struct Snapshot<PdC: PdClient = PdRpcClient> {
    transaction: Transaction<PdC>,
}

/// Stateful counterpart of client-go's `txnkv/txnsnapshot.Scanner`.
///
/// The iterator borrows its snapshot for its lifetime and fetches at most one
/// configured scanner batch whenever its local buffer becomes empty.
pub struct SnapshotIterator<'a, PdC: PdClient = PdRpcClient> {
    snapshot: &'a mut Snapshot<PdC>,
    range: BoundRange,
    reverse: bool,
    batch_size: u32,
    buffered: VecDeque<KvPair>,
    exhausted: bool,
    valid: bool,
}

impl<'a, PdC: PdClient> SnapshotIterator<'a, PdC> {
    fn new(snapshot: &'a mut Snapshot<PdC>, range: BoundRange, reverse: bool) -> Self {
        Self {
            batch_size: snapshot.transaction.snapshot_scan_batch_size(),
            snapshot,
            range,
            reverse,
            buffered: VecDeque::new(),
            exhausted: false,
            valid: true,
        }
    }

    async fn refill(&mut self) -> Result<()> {
        if !self.valid || !self.buffered.is_empty() {
            return Ok(());
        }
        if self.exhausted {
            self.valid = false;
            return Ok(());
        }
        let pairs = if self.reverse {
            self.snapshot
                .scan_reverse(self.range.clone(), self.batch_size)
                .await?
                .collect::<Vec<_>>()
        } else {
            self.snapshot
                .scan(self.range.clone(), self.batch_size)
                .await?
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
    pub async fn next(&mut self) -> Result<Option<KvPair>> {
        self.refill().await?;
        Ok(self.buffered.pop_front())
    }

    /// Whether the iterator can still yield a pair.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// End the scan early. Dropping the iterator has the same effect.
    pub fn close(&mut self) {
        self.buffered.clear();
        self.exhausted = true;
        self.valid = false;
    }
}

impl<PdC: PdClient> Snapshot<PdC> {
    pub(crate) fn iterator_batch_size(&self) -> u32 {
        self.transaction.snapshot_scan_batch_size()
    }

    /// Reset the read timestamp for subsequent snapshot operations.
    ///
    /// This is the Rust counterpart of client-go `KVSnapshot.SetSnapshotTS`.
    /// It discards cached reads and timestamp-scoped resolved-lock hints;
    /// committed read-through hints remain intact as in client-go.
    pub fn set_snapshot_timestamp(&mut self, timestamp: crate::Timestamp) {
        self.transaction.set_snapshot_timestamp(timestamp);
    }

    /// Choose the TiKV replica-read type for subsequent snapshot reads.
    /// This is the Rust counterpart of client-go `KVSnapshot.SetReplicaRead`.
    pub fn set_replica_read(&mut self, read_type: ReplicaReadType) {
        self.set_replica_read_config(ReplicaReadConfig {
            read_type,
            ..Default::default()
        });
    }

    /// Choose replica-read type plus source selector constraints for
    /// subsequent snapshot reads.
    pub fn set_replica_read_config(&mut self, config: ReplicaReadConfig) {
        self.transaction.set_replica_read_config(config);
    }

    /// Mark subsequent snapshot reads as stale reads. This enables source
    /// mixed-replica selection and the distinct TiKV stale-read context bit.
    /// Lock/error fallback remains owned by the request selector.
    pub fn set_stale_read(&mut self, stale_read: bool) {
        self.transaction.set_stale_read(stale_read);
    }

    /// Mark this snapshot as a staleness read, matching client-go
    /// `KVSnapshot.SetIsStalenessReadOnly`.
    pub fn set_is_staleness_read_only(&mut self, stale_read: bool) {
        self.set_stale_read(stale_read);
    }

    /// Replace the store-label constraints used by subsequent replica
    /// selection. This is the Rust counterpart of client-go
    /// `KVSnapshot.SetMatchStoreLabels`.
    pub fn set_match_store_labels(
        &mut self,
        labels: impl IntoIterator<Item = crate::proto::metapb::StoreLabel>,
    ) {
        self.transaction
            .set_match_store_labels(labels.into_iter().collect());
    }

    /// Set the TiKV queue-wait threshold that permits a leader read to use an
    /// idle replica. This is the Rust counterpart of client-go
    /// `KVSnapshot.SetLoadBasedReplicaReadThreshold`.
    ///
    /// Zero and values that cannot fit TiKV's `u32` millisecond context field
    /// disable load-based replica selection, matching the source boundary.
    pub fn set_load_based_replica_read_threshold(&mut self, busy_threshold: Duration) {
        self.transaction
            .set_load_based_replica_read_threshold(busy_threshold);
    }

    /// Set the per-get/batch-get replica selector adjustment callback. This
    /// is the native counterpart of client-go `KVSnapshot.SetReplicaReadAdjuster`.
    pub fn set_replica_read_adjuster(&mut self, adjuster: ReplicaReadAdjuster) {
        self.transaction.set_replica_read_adjuster(adjuster);
    }

    /// Set the priority for subsequent read requests.
    pub fn set_priority(&mut self, priority: Priority) {
        self.transaction.set_priority(priority);
    }

    /// Skip `sample_step - 1` keys after each returned scan key. A step of
    /// zero disables sampling, matching client-go `KVSnapshot.SetSampleStep`.
    pub fn set_sample_step(&mut self, sample_step: u32) {
        self.transaction.set_sample_step(sample_step);
    }

    /// Return only keys from subsequent snapshot scans, matching client-go
    /// `KVSnapshot.SetKeyOnly`.
    pub fn set_key_only(&mut self, key_only: bool) {
        self.transaction.set_snapshot_key_only(key_only);
    }

    /// Set the maximum number of pairs requested by each TiKV scan RPC.
    /// Values zero and one use client-go's default batch size of 256.
    /// The `limit` passed to [`Self::scan`] remains the total result limit.
    pub fn set_scan_batch_size(&mut self, batch_size: u32) {
        self.transaction.set_snapshot_scan_batch_size(batch_size);
    }

    /// Attach runtime statistics to subsequent physical snapshot read RPCs.
    /// Pass `None` to stop collection, matching client-go
    /// `KVSnapshot.SetRuntimeStats(nil)`.
    pub fn set_runtime_stats(
        &mut self,
        stats: Option<std::sync::Arc<crate::SnapshotRuntimeStats>>,
    ) {
        self.transaction.set_snapshot_runtime_stats(stats);
    }

    /// Set retry variables for subsequent snapshot reads, matching client-go
    /// `KVSnapshot.SetVars`.
    pub fn set_variables(&mut self, variables: std::sync::Arc<crate::Variables>) {
        self.transaction.set_snapshot_variables(variables);
    }

    /// Allow reads to proceed through locks flushed by this pipelined
    /// transaction, matching client-go `KVSnapshot.SetPipelined`.
    pub fn set_pipelined(&mut self, timestamp: u64) {
        self.transaction.set_snapshot_pipelined(timestamp);
    }

    /// Set the deadline for each physical snapshot read. A zero duration
    /// clears the override, matching client-go `KVSnapshot.SetKVReadTimeout`.
    pub fn set_kv_read_timeout(&mut self, timeout: Duration) {
        self.transaction.set_snapshot_read_timeout(timeout);
    }

    /// Set the static TiKV resource-group tag for subsequent snapshot reads.
    /// `None` restores the source nil-tag state.
    pub fn set_resource_group_tag(&mut self, resource_group_tag: Option<Vec<u8>>) {
        self.transaction.set_resource_group_tag(resource_group_tag);
    }

    /// Set the resource-group tag callback for subsequent snapshot reads.
    /// A static tag set with [`Self::set_resource_group_tag`] takes precedence,
    /// matching client-go `KVSnapshot.SetResourceGroupTagger`.
    pub fn set_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<crate::SnapshotResourceGroupTagger>,
    ) {
        self.transaction
            .set_resource_group_tagger(resource_group_tagger);
    }

    /// Set the transaction and replica-read scope used by subsequent Get and
    /// BatchGet read-timestamp validation. This is the native counterpart of
    /// both client-go `SetTxnScope` and `SetReadReplicaScope`.
    pub fn set_read_replica_scope(&mut self, scope: impl Into<String>) {
        self.transaction.set_read_replica_scope(scope);
    }

    /// Alias for [`Self::set_read_replica_scope`], retained for client-go API
    /// compatibility.
    pub fn set_txn_scope(&mut self, scope: impl Into<String>) {
        self.set_read_replica_scope(scope);
    }

    /// Return the configured snapshot read deadline, or `None` when the
    /// client-wide transport timeout remains in effect.
    pub fn kv_read_timeout(&self) -> Option<Duration> {
        self.transaction.snapshot_read_timeout()
    }

    /// Return the number of reads served from the snapshot cache. This is the
    /// Rust counterpart of client-go `KVSnapshot.SnapCacheHitCount`.
    pub fn snap_cache_hit_count(&self) -> usize {
        self.transaction.snapshot_cache_hit_count()
    }

    /// Return the number of cached snapshot-read entries. Cached missing keys
    /// are included, matching client-go `KVSnapshot.SnapCacheSize`.
    pub fn snap_cache_size(&self) -> usize {
        self.transaction.snapshot_cache_size()
    }

    /// Return a copy of the snapshot cache, including cached missing keys as
    /// default [`ValueEntry`] values. This is the native counterpart of
    /// client-go `KVSnapshot.SnapCache`.
    pub fn snap_cache(&self) -> BTreeMap<Key, ValueEntry> {
        self.transaction.snapshot_cache()
    }

    /// Seed snapshot-cache entries for the supplied keys. Keys absent from
    /// `values` are cached as missing; latest-timestamp snapshots deliberately
    /// ignore this call, matching client-go `UpdateSnapshotCache`.
    pub fn update_snapshot_cache(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        values: BTreeMap<Key, ValueEntry>,
    ) {
        self.transaction
            .update_snapshot_cache(keys.into_iter().map(Into::into), values);
    }

    /// Remove only cached snapshot-read entries for the supplied keys. Local
    /// transaction mutations and lock state are retained.
    pub fn clean_snapshot_cache(&mut self, keys: impl IntoIterator<Item = impl Into<Key>>) {
        self.transaction
            .clean_snapshot_cache(keys.into_iter().map(Into::into));
    }

    /// Control whether TiKV should bypass cache population for subsequent
    /// snapshot reads, matching client-go `KVSnapshot.SetNotFillCache`.
    pub fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        self.transaction.set_not_fill_cache(not_fill_cache);
    }

    /// Set the TiKV isolation level used by subsequent snapshot reads.
    pub fn set_isolation_level(&mut self, isolation_level: crate::proto::kvrpcpb::IsolationLevel) {
        self.transaction.set_isolation_level(isolation_level);
    }

    /// Set TiKV's scheduling task ID for subsequent snapshot reads.
    pub fn set_task_id(&mut self, task_id: u64) {
        self.transaction.set_task_id(task_id);
    }

    /// Set the source-compatible resource group on subsequent snapshot RPCs.
    pub fn set_resource_group_name(&mut self, resource_group_name: impl Into<String>) {
        self.transaction
            .set_resource_group_name(resource_group_name);
    }

    /// Attach a PD resource-group controller to subsequent snapshot RPCs.
    pub fn set_resource_control(&mut self, controller: crate::ResourceGroupControllerHandle) {
        self.transaction.set_resource_control(controller);
    }

    /// Attach resource-unit accounting to subsequent snapshot RPCs.
    pub fn set_ru_details(&mut self, ru_details: std::sync::Arc<crate::RuDetails>) {
        self.transaction.set_ru_details(ru_details);
    }

    /// Replace the RPC interceptor used by subsequent snapshot requests.
    pub fn set_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        self.transaction.set_rpc_interceptor(interceptor);
    }

    /// Add an RPC interceptor after the existing snapshot interceptor chain.
    pub fn add_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        self.transaction.add_rpc_interceptor(interceptor);
    }

    /// Get the value associated with the given key.
    pub async fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        trace!("invoking get request on snapshot");
        self.transaction.get(key).await
    }

    /// Get a value plus optional commit timestamp using source `GetOption`
    /// semantics. A missing key returns `None` in the native Rust API.
    pub async fn get_with_options(
        &mut self,
        key: impl Into<Key>,
        options: &[GetOption],
    ) -> Result<Option<ValueEntry>> {
        trace!("invoking get request with options on snapshot");
        self.transaction.get_with_options(key, options).await
    }

    /// Check whether the key exists.
    pub async fn key_exists(&mut self, key: impl Into<Key>) -> Result<bool> {
        debug!("invoking key_exists request on snapshot");
        self.transaction.key_exists(key).await
    }

    /// Get the values associated with the given keys.
    pub async fn batch_get(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking batch_get request on snapshot");
        self.transaction.batch_get(keys).await
    }

    /// Batch-get values plus optional commit timestamps using source
    /// `GetOption` semantics. Missing keys are absent from the returned map.
    pub async fn batch_get_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        options: &[GetOption],
    ) -> Result<BTreeMap<Key, ValueEntry>> {
        self.transaction.batch_get_with_options(keys, options).await
    }

    /// Read values from the pipelined transaction buffer tier.
    ///
    /// This is the native counterpart of client-go
    /// `KVSnapshot.BatchGetWithTier(BatchGetBufferTier)`. Call
    /// [`Self::set_pipelined`] first; ordinary snapshots return the source
    /// pipelined-mode error.
    pub async fn batch_get_from_buffer(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        self.transaction.batch_get_from_buffer(keys).await
    }

    /// Create and prefetch a stateful forward scanner, matching client-go
    /// `KVSnapshot.Iter`'s construction-time first fetch.
    pub async fn iter(
        &mut self,
        range: impl Into<BoundRange>,
    ) -> Result<SnapshotIterator<'_, PdC>> {
        let mut iterator = SnapshotIterator::new(self, range.into(), false);
        iterator.refill().await?;
        Ok(iterator)
    }

    /// Create and prefetch a stateful reverse scanner, matching client-go
    /// `KVSnapshot.IterReverse`'s construction-time first fetch.
    pub async fn iter_reverse(
        &mut self,
        range: impl Into<BoundRange>,
    ) -> Result<SnapshotIterator<'_, PdC>> {
        let mut iterator = SnapshotIterator::new(self, range.into(), true);
        iterator.refill().await?;
        Ok(iterator)
    }

    /// Scan a range, return at most `limit` key-value pairs that lying in the range.
    pub async fn scan(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking scan request on snapshot");
        self.transaction.scan(range, limit).await
    }

    /// Scan a range, return at most `limit` keys that lying in the range.
    pub async fn scan_keys(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking scan_keys request on snapshot");
        self.transaction.scan_keys(range, limit).await
    }

    /// Similar to scan, but in the reverse direction.
    pub async fn scan_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking scan_reverse request on snapshot");
        self.transaction.scan_reverse(range, limit).await
    }

    /// Similar to scan_keys, but in the reverse direction.
    pub async fn scan_keys_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking scan_keys_reverse request on snapshot");
        self.transaction.scan_keys_reverse(range, limit).await
    }
}

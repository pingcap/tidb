// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `txnkv/txnsnapshot.KVSnapshot` as a point read uses it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::backoff::Backoff;
use crate::interceptor::{RpcInterceptorChain, RpcInterceptorHandle};
use crate::kv::{ReplicaReadAdjuster, ReplicaReadConfig, Variables, DEFAULT_VARIABLES};
use crate::oracle::ReadTimestampValidator;
use crate::pd::{PdClient, PdRpcClient};
use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::request::{EncodeKeyspace, KeyMode, Keyspace};
use crate::resource_control::ResourceGroupControllerHandle;
use crate::timestamp::TimestampExt;
use crate::transaction::buffer::{snapshot_cache_entry_size, SNAPSHOT_CACHE_SIZE_LIMIT};
use crate::transaction::transaction::{
    snapshot_read_trace_context_of, snapshot_runtime_interceptor, PointGet,
};
use crate::transaction::{
    Priority, ReadLockContext, RequestSource, ResolveLocksContext, SnapshotResourceGroupTagger,
    SnapshotRuntimeStats, SnapshotVisibilityValidator, TransactionResourceGroupTagger,
};
use crate::{Key, Result, Value, ValueEntry};

/// client-go `txnsnapshot.KVSnapshot`: reads at one version through the
/// store, with the settings a statement gives it and no transaction around
/// it. [`crate::TransactionClient::kv_snapshot`] (`KVStore.GetSnapshot`)
/// makes one.
///
/// A [`crate::Transaction`] also carries a write buffer, its commit state
/// and its own lock and heartbeat bookkeeping. A statement that only reads
/// at one version, as Go's point read at `MaxUint64` does, reads through
/// this instead. Its settings and their setters are the snapshot half of
/// [`crate::Transaction`]'s, and its [`Self::get`] sends the same request.
pub struct KvSnapshot<PdC: PdClient = PdRpcClient> {
    timestamp: Timestamp,
    rpc: Arc<PdC>,
    keyspace: Keyspace,
    keyspace_name: Option<String>,
    lock_backoff: Backoff,
    priority: Priority,
    rpc_interceptor: Option<RpcInterceptorChain>,
    runtime_stats: Option<Arc<SnapshotRuntimeStats>>,
    variables: Arc<Variables>,
    resource_group_name: Option<String>,
    snapshot_resource_group_name: Option<String>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    exec_details: Option<Arc<crate::util::ExecDetails>>,
    caller_context: Option<crate::trace::TraceContext>,
    replica_read_config: ReplicaReadConfig,
    replica_read_adjuster: Option<ReplicaReadAdjuster>,
    not_fill_cache: bool,
    isolation_level: kvrpcpb::IsolationLevel,
    task_id: u64,
    resource_group_tag: Option<Vec<u8>>,
    resource_group_tagger: Option<SnapshotResourceGroupTagger>,
    transaction_resource_group_tagger: Option<TransactionResourceGroupTagger>,
    read_timeout: Option<Duration>,
    read_timestamp_validator: Option<Arc<dyn ReadTimestampValidator>>,
    visibility_validator: Option<Arc<dyn SnapshotVisibilityValidator>>,
    read_replica_scope: String,
    read_lock_context: ReadLockContext,
    lock_resolver_context: ResolveLocksContext,
    request_source: RequestSource,
    cache: Mutex<SnapshotCache>,
}

/// client-go `KVSnapshot.mu.cached` and `cachedSize`: the values the
/// snapshot read, keyed by physical key; `None` is a key that does not exist.
#[derive(Default)]
struct SnapshotCache {
    cached: HashMap<Key, Option<Value>>,
    cached_size: i64,
}

impl SnapshotCache {
    /// client-go `KVSnapshot.UpdateSnapshotCache` for one key.
    fn update(&mut self, key: Key, value: Option<Value>) {
        let entry = value.clone().map(|value| ValueEntry::new(value, 0));
        self.cached_size += snapshot_cache_entry_size(&key, &entry);
        if let Some(old) = self.cached.insert(key.clone(), value) {
            // client-go subtracts only the old entry, not its key.
            let old = old.map(|value| ValueEntry::new(value, 0));
            self.cached_size -= snapshot_cache_entry_size(&key, &old) - key.len() as i64;
        }
        if self.cached_size < SNAPSHOT_CACHE_SIZE_LIMIT {
            return;
        }
        let evictable = self
            .cached
            .keys()
            .filter(|cached| **cached != key)
            .cloned()
            .collect::<Vec<_>>();
        for evicted in evictable {
            let value = self.cached.remove(&evicted).flatten();
            let entry = value.map(|value| ValueEntry::new(value, 0));
            self.cached_size -= snapshot_cache_entry_size(&evicted, &entry);
            if self.cached_size < SNAPSHOT_CACHE_SIZE_LIMIT {
                break;
            }
        }
    }
}

impl<PdC: PdClient> KvSnapshot<PdC> {
    /// A snapshot at `timestamp` over an injected PD/KV client, with the
    /// store's lock resolver and read validation, and every other setting at
    /// the default an optimistic [`crate::Transaction`] starts with.
    ///
    /// [`crate::TransactionClient::kv_snapshot`] builds one for its store;
    /// like [`crate::Transaction::new`], this serves embedded and
    /// in-process stores that hold their own PD/KV client.
    #[doc(hidden)]
    pub fn new(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        keyspace: Keyspace,
        keyspace_name: Option<String>,
        lock_resolver_context: ResolveLocksContext,
        read_timestamp_validator: Option<Arc<dyn ReadTimestampValidator>>,
        visibility_validator: Option<Arc<dyn SnapshotVisibilityValidator>>,
    ) -> Self {
        // client-go `NewTiKVSnapshot`'s sanity check.
        let version = timestamp.version();
        assert!(
            version < i64::MAX as u64 || version == u64::MAX,
            "try to get snapshot with a large ts {version}"
        );
        KvSnapshot {
            timestamp,
            rpc,
            keyspace,
            keyspace_name,
            // An optimistic transaction's (`TransactionOptions::new_optimistic`).
            lock_backoff: crate::request::RetryOptions::default_optimistic().lock_backoff,
            priority: Priority::Normal,
            rpc_interceptor: None,
            runtime_stats: None,
            variables: DEFAULT_VARIABLES.clone(),
            resource_group_name: None,
            snapshot_resource_group_name: None,
            resource_control: None,
            ru_details: None,
            exec_details: None,
            caller_context: None,
            replica_read_config: ReplicaReadConfig::default(),
            replica_read_adjuster: None,
            not_fill_cache: false,
            isolation_level: kvrpcpb::IsolationLevel::Si,
            task_id: 0,
            resource_group_tag: None,
            resource_group_tagger: None,
            transaction_resource_group_tagger: None,
            read_timeout: None,
            read_timestamp_validator,
            visibility_validator,
            read_replica_scope: String::new(),
            read_lock_context: ReadLockContext::default(),
            lock_resolver_context,
            request_source: RequestSource::default(),
            cache: Mutex::default(),
        }
    }

    /// client-go `KVSnapshot.Get`: the value of `key` at the snapshot's
    /// version, from the snapshot's cache or else read from TiKV, resolving
    /// the locks the read meets. A read at `u64::MAX` is never cached.
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let internal = self.request_source.is_internal();
        let read = async move {
            let _timer = crate::stats::snapshot_command_timer("get", internal);
            if let Some(value) = self.cache().cached.get(&key) {
                return Ok(value.clone());
            }
            // client-go `KVSnapshot.get` opens its span and discards the
            // derived ctx.
            let value = crate::trace::detached_client_span(
                "tikvSnapshot.get",
                self.point_get().run(key.clone()),
            )
            .await?;
            // `UpdateSnapshotCache`: a read at `MaxUint64` always reads the
            // latest data, so it is not cached.
            if self.timestamp.version() != u64::MAX {
                self.cache().update(key, value.clone());
            }
            Ok(value)
        };
        match snapshot_read_trace_context_of(
            self.exec_details.as_ref(),
            self.caller_context.as_ref(),
            self.timestamp.version(),
        ) {
            Some(statement) => crate::trace::with_trace_context(statement, Box::pin(read)).await,
            None => read.await,
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, SnapshotCache> {
        self.cache.lock().expect("snapshot cache lock poisoned")
    }

    fn point_get(&self) -> PointGet<PdC> {
        let mut replica_read_config = self.replica_read_config.clone();
        if replica_read_config.read_type.is_follower_read() {
            if let Some(adjuster) = &self.replica_read_adjuster {
                replica_read_config.apply_adjustment(adjuster(1));
            }
        }
        PointGet {
            timestamp: self.timestamp.clone(),
            rpc: self.rpc.clone(),
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
            lock_backoff: self.lock_backoff.clone(),
            rpc_interceptor: snapshot_runtime_interceptor(
                self.rpc_interceptor.clone(),
                self.runtime_stats.clone(),
            ),
            runtime_stats: self.runtime_stats.clone(),
            variables: self.variables.clone(),
            resource_group_name: self
                .snapshot_resource_group_name
                .as_deref()
                .or(self.resource_group_name.as_deref())
                .map(str::to_owned),
            resource_control: self.resource_control.clone(),
            ru_details: self.ru_details.clone(),
            priority: self.priority,
            not_fill_cache: self.not_fill_cache,
            isolation_level: self.isolation_level,
            task_id: self.task_id,
            resource_group_tag: self.resource_group_tag.clone(),
            resource_group_tagger: self.resource_group_tagger.clone(),
            transaction_resource_group_tagger: self.transaction_resource_group_tagger.clone(),
            read_timeout: self.read_timeout,
            read_timestamp_validator: self.read_timestamp_validator.clone(),
            visibility_validator: self.visibility_validator.clone(),
            read_replica_scope: self.read_replica_scope.clone(),
            replica_read_config,
            read_lock_context: self.read_lock_context.clone(),
            lock_resolver_context: self.lock_resolver_context.clone(),
            request_source: self.request_source.context_value(),
        }
    }

    /// The snapshot's version.
    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// See [`crate::Transaction::set_replica_read`].
    pub fn set_replica_read(&mut self, read_type: crate::ReplicaReadType) {
        self.replica_read_config = ReplicaReadConfig {
            read_type,
            ..Default::default()
        };
    }

    /// See [`crate::Transaction::set_stale_read`].
    pub fn set_stale_read(&mut self, stale_read: bool) {
        self.replica_read_config.stale_read = stale_read;
    }

    /// See [`crate::Transaction::set_match_store_labels`].
    pub fn set_match_store_labels(&mut self, labels: Vec<crate::proto::metapb::StoreLabel>) {
        self.replica_read_config.labels = labels;
    }

    /// See [`crate::Transaction::set_load_based_replica_read_threshold`].
    pub fn set_load_based_replica_read_threshold(&mut self, busy_threshold: Duration) {
        self.replica_read_config.busy_threshold_ms = u32::try_from(busy_threshold.as_millis())
            .ok()
            .filter(|threshold| *threshold != 0)
            .unwrap_or_default();
    }

    /// See [`crate::Transaction::set_replica_read_adjuster`].
    pub fn set_replica_read_adjuster(&mut self, adjuster: ReplicaReadAdjuster) {
        self.replica_read_adjuster = Some(adjuster);
    }

    /// See [`crate::Transaction::set_isolation_level`].
    pub fn set_isolation_level(&mut self, isolation_level: kvrpcpb::IsolationLevel) {
        self.isolation_level = isolation_level;
    }

    /// See [`crate::Transaction::set_read_replica_scope`].
    pub fn set_read_replica_scope(&mut self, scope: impl Into<String>) {
        self.read_replica_scope = scope.into();
    }

    /// See [`crate::Transaction::set_snapshot_read_timeout`].
    pub fn set_snapshot_read_timeout(&mut self, timeout: Duration) {
        self.read_timeout = (!timeout.is_zero()).then_some(timeout);
    }

    /// See [`crate::Transaction::set_snapshot_resource_group_name`].
    pub fn set_snapshot_resource_group_name(&mut self, name: Option<String>) {
        self.snapshot_resource_group_name = name;
    }

    /// See [`crate::Transaction::set_resource_group_name`].
    pub fn set_resource_group_name(&mut self, resource_group_name: impl Into<String>) {
        self.resource_group_name = Some(resource_group_name.into());
        self.snapshot_resource_group_name = None;
    }

    /// See [`crate::Transaction::set_resource_control`].
    pub fn set_resource_control(&mut self, controller: ResourceGroupControllerHandle) {
        self.resource_control = Some(controller);
    }

    /// See [`crate::Transaction::set_ru_details`].
    pub fn set_ru_details(&mut self, ru_details: Arc<crate::RuDetails>) {
        self.ru_details = Some(ru_details);
    }

    /// See [`crate::Transaction::set_exec_details`].
    pub fn set_exec_details(&mut self, exec_details: Arc<crate::util::ExecDetails>) {
        self.exec_details = Some(exec_details);
    }

    /// See [`crate::Transaction::set_caller_context`]: the ctx client-go's
    /// `KVSnapshot.Get(ctx, k)` is called with, whose RPC interceptor the
    /// read runs.
    pub fn set_caller_context(&mut self, context: crate::trace::TraceContext) {
        self.caller_context = Some(context);
    }

    /// See [`crate::Transaction::set_resource_group_tagger`]: the tagger a
    /// transaction's requests share, which a snapshot read applies after its
    /// own tag and tagger.
    pub fn set_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<TransactionResourceGroupTagger>,
    ) {
        self.transaction_resource_group_tagger = resource_group_tagger;
    }

    /// See [`crate::Transaction::set_snapshot_resource_group_tagger`].
    pub fn set_snapshot_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<SnapshotResourceGroupTagger>,
    ) {
        self.resource_group_tagger = resource_group_tagger;
    }

    /// See [`crate::Transaction::set_resource_group_tag`].
    pub fn set_resource_group_tag(&mut self, resource_group_tag: Option<Vec<u8>>) {
        self.resource_group_tag = resource_group_tag;
    }

    /// See [`crate::Transaction::set_request_source_type`].
    pub fn set_request_source_type(&mut self, source_type: impl Into<String>) {
        self.request_source.source_type = source_type.into();
    }

    /// See [`crate::Transaction::set_request_source_internal`].
    pub fn set_request_source_internal(&mut self, internal: bool) {
        self.request_source.internal = internal;
    }

    /// See [`crate::Transaction::add_rpc_interceptor`].
    pub fn add_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        match &mut self.rpc_interceptor {
            Some(chain) => {
                chain.link(interceptor);
            }
            None => {
                let mut chain = RpcInterceptorChain::new();
                chain.link(interceptor);
                self.rpc_interceptor = Some(chain);
            }
        }
    }

    /// See [`crate::Transaction::set_snapshot_runtime_stats`].
    pub fn set_snapshot_runtime_stats(&mut self, stats: Option<Arc<SnapshotRuntimeStats>>) {
        self.runtime_stats = stats;
    }

    /// See [`crate::Transaction::set_snapshot_variables`].
    pub fn set_snapshot_variables(&mut self, variables: Arc<Variables>) {
        self.variables = variables;
    }

    /// See [`crate::Transaction::set_priority`].
    pub fn set_priority(&mut self, priority: Priority) {
        self.priority = priority;
    }

    /// See [`crate::Transaction::set_not_fill_cache`].
    pub fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        self.not_fill_cache = not_fill_cache;
    }

    /// See [`crate::Transaction::set_task_id`].
    pub fn set_task_id(&mut self, task_id: u64) {
        self.task_id = task_id;
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Mutex;

    use super::*;
    use crate::kv::{ReplicaReadAdjustment, ReplicaReadSelectorOption};
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::timestamp::TimestampExt;
    use crate::{CheckLevel, ReplicaReadType, Transaction, TransactionOptions};

    type Requests = Arc<Mutex<Vec<kvrpcpb::GetRequest>>>;

    /// A store that records every Get and answers `value`, or not found for
    /// the key `missing`.
    fn recording_store() -> (Arc<MockPdClient>, Requests) {
        let requests = Requests::default();
        let captured = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request.downcast_ref::<kvrpcpb::GetRequest>().unwrap();
                captured.lock().unwrap().push(request.clone());
                let not_found = request.key == b"missing";
                Ok(Box::new(kvrpcpb::GetResponse {
                    value: if not_found {
                        Vec::new()
                    } else {
                        b"value".to_vec()
                    },
                    not_found,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        (pd_client, requests)
    }

    fn read_pair(
        pd_client: &Arc<MockPdClient>,
        version: u64,
    ) -> (Transaction<MockPdClient>, KvSnapshot<MockPdClient>) {
        let transaction = Transaction::new(
            Timestamp::from_version(version),
            Arc::clone(pd_client),
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let snapshot = KvSnapshot::new(
            Timestamp::from_version(version),
            Arc::clone(pd_client),
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
            None,
            None,
        );
        (transaction, snapshot)
    }

    /// Applies the same setters to a transaction and a snapshot.
    macro_rules! on_both {
        ($transaction:expr, $snapshot:expr, |$reader:ident| $body:block) => {{
            {
                let $reader = &mut $transaction;
                $body
            }
            {
                let $reader = &mut $snapshot;
                $body
            }
        }};
    }

    async fn assert_same_get(
        transaction: &mut Transaction<MockPdClient>,
        snapshot: &KvSnapshot<MockPdClient>,
        requests: &Requests,
    ) {
        requests.lock().unwrap().clear();
        assert_eq!(
            transaction.get(b"key".to_vec()).await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(
            snapshot.get(b"key".to_vec()).await.unwrap(),
            Some(b"value".to_vec())
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
    }

    /// client-go's `KVTxn.Get` reads through its `KVSnapshot`: a snapshot
    /// read sends the request a transaction's read at the same version and
    /// settings sends.
    #[tokio::test]
    async fn a_snapshot_get_sends_the_request_a_transaction_get_sends() {
        let (pd_client, requests) = recording_store();
        let (mut transaction, mut snapshot) = read_pair(&pd_client, u64::MAX);
        on_both!(transaction, snapshot, |reader| {
            reader.set_priority(Priority::High);
            reader.set_not_fill_cache(true);
            reader.set_task_id(7);
            reader.set_isolation_level(kvrpcpb::IsolationLevel::Rc);
            reader.set_resource_group_name("rg");
            reader.set_resource_group_tag(Some(b"tag".to_vec()));
            reader.set_request_source_internal(true);
            reader.set_request_source_type("lightning");
            reader.set_snapshot_read_timeout(Duration::from_millis(250));
        });
        assert_same_get(&mut transaction, &snapshot, &requests).await;

        // A follower read the adjuster sends to the leader, with load-based
        // replica read and the snapshot's own resource group. The adjuster
        // is asked once per read, for its one key.
        let adjusted = Arc::new(Mutex::new(Vec::new()));
        let (mut transaction, mut snapshot) = read_pair(&pd_client, 10);
        on_both!(transaction, snapshot, |reader| {
            let adjusted = Arc::clone(&adjusted);
            reader.set_replica_read(ReplicaReadType::Follower);
            reader.set_replica_read_adjuster(Arc::new(move |keys| {
                adjusted.lock().unwrap().push(keys);
                ReplicaReadAdjustment::new(
                    Some(ReplicaReadSelectorOption::LeaderOnly),
                    ReplicaReadType::Leader,
                )
            }));
            reader.set_load_based_replica_read_threshold(Duration::from_millis(50));
            reader.set_resource_group_name("rg");
            reader.set_snapshot_resource_group_name(Some("snapshot-rg".to_owned()));
        });
        assert_same_get(&mut transaction, &snapshot, &requests).await;
        assert_eq!(*adjusted.lock().unwrap(), [1, 1]);

        // A stale read.
        let (mut transaction, mut snapshot) = read_pair(&pd_client, 10);
        on_both!(transaction, snapshot, |reader| {
            reader.set_stale_read(true);
        });
        assert_same_get(&mut transaction, &snapshot, &requests).await;
    }

    /// client-go `KVSnapshot.Get` answers a key it read before from its cache,
    /// a key that does not exist included, except at `MaxUint64`
    /// (`UpdateSnapshotCache`).
    #[tokio::test]
    async fn a_snapshot_caches_its_reads_except_at_max_ts() {
        let (pd_client, requests) = recording_store();
        let (_, snapshot) = read_pair(&pd_client, 10);
        for _ in 0..2 {
            assert_eq!(
                snapshot.get(b"key".to_vec()).await.unwrap(),
                Some(b"value".to_vec())
            );
            assert_eq!(snapshot.get(b"missing".to_vec()).await.unwrap(), None);
        }
        assert_eq!(requests.lock().unwrap().len(), 2);

        requests.lock().unwrap().clear();
        let (_, snapshot) = read_pair(&pd_client, u64::MAX);
        for _ in 0..2 {
            assert_eq!(
                snapshot.get(b"key".to_vec()).await.unwrap(),
                Some(b"value".to_vec())
            );
        }
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    /// client-go `KVSnapshot.Get(ctx, k)` runs the RPC interceptor its caller
    /// ctx carries, as `KVTxn.Get` does, whether or not the statement charges
    /// `ExecDetails`; one already in the active ctx is not linked twice.
    #[tokio::test]
    async fn a_snapshot_get_runs_the_caller_contexts_interceptor() {
        let manager = crate::MockInterceptorManager::new();
        let (pd_client, _) = recording_store();
        let (_, mut snapshot) = read_pair(&pd_client, u64::MAX);
        snapshot.set_caller_context(crate::interceptor::with_rpc_interceptor(
            &crate::trace::TraceContext::default(),
            manager.create_mock_interceptor("CALLER"),
        ));
        assert_eq!(
            snapshot.get(b"key".to_vec()).await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(manager.exec_log(), ["CALLER"]);
        manager.reset();

        let caller = crate::interceptor::with_rpc_interceptor(
            &crate::trace::TraceContext::default(),
            manager.create_mock_interceptor("CALLER"),
        );
        snapshot.set_caller_context(caller.clone());
        snapshot.set_exec_details(Arc::new(crate::util::ExecDetails::default()));
        let value = crate::trace::with_trace_context(caller, async {
            snapshot.get(b"other".to_vec()).await
        })
        .await
        .unwrap();
        assert_eq!(value, Some(b"value".to_vec()));
        assert_eq!(manager.exec_log(), ["CALLER"]);
    }

    /// client-go `NewTiKVSnapshot`'s sanity check.
    #[test]
    #[should_panic(expected = "try to get snapshot with a large ts")]
    fn a_snapshot_rejects_a_ts_beyond_max_int64() {
        let (pd_client, _) = recording_store();
        KvSnapshot::new(
            Timestamp::from_version(i64::MAX as u64),
            pd_client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
            None,
            None,
        );
    }
}

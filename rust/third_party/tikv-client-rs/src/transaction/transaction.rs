// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::ops::Bound;
use std::sync::atomic;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use async_trait::async_trait;
use derive_new::new;
use fail::fail_point;
use futures::prelude::*;
use log::{debug, error, info, trace, warn};
use tokio::time::Duration;

use crate::backoff::Backoff;
use crate::backoff::DEFAULT_REGION_BACKOFF;
use crate::backoff::DEFAULT_STORE_BACKOFF;
use crate::interceptor::RpcInterceptorChain;
use crate::interceptor::RpcInterceptorHandle;
use crate::kv::{
    GetOption, GetOptions, LockContext, ReplicaReadAdjuster, ReplicaReadConfig, ReturnedValue,
    ValueEntry, Variables, DEFAULT_VARIABLES, LOCK_ALWAYS_WAIT, LOCK_NO_WAIT,
};
use crate::oracle::ReadTimestampValidator;
use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::request::Collect;
use crate::request::CollectError;
use crate::request::CollectSingle;
use crate::request::Dispatch;
use crate::request::EncodeKeyspace;
use crate::request::KeyMode;
use crate::request::Keyspace;
use crate::request::KvRequest;
use crate::request::NoTarget;
use crate::request::Plan;
use crate::request::PlanBuilder;
use crate::request::RetryOptions;
use crate::request::TruncateKeyspace;
use crate::resource_control::ResourceGroupControllerHandle;
use crate::retry::{RetryBackoffer, BO_COMMIT_TS_LAG};
use crate::store::Request as StoreRequest;
use crate::timestamp::TimestampExt;
use crate::transaction::buffer::Buffer;
use crate::transaction::extract_lock_from_key_error;
use crate::transaction::latch::LatchesScheduler;
use crate::transaction::lock::format_key_for_log;
use crate::transaction::lowering::*;
use crate::transaction::requests::{
    new_resolve_lock_request, CollectPessimisticLock, CollectScannerPairs,
    CollectScannerRegionBatch, PreserveScannerPairErrors,
};
use crate::transaction::snapshot_stats::snapshot_read_sli_interceptor;
use crate::transaction::txn_file::{
    build_txn_chunks, request_source_allows_txn_file, txn_file_max_chunks_in_parallel,
    txn_file_pre_split_keys, ChunkBatch, TxnChunkSlice,
};
use crate::transaction::unionstore::{FORCE_FLUSH_MEMORY, MIN_FLUSH_KEYS, MIN_FLUSH_MEMORY};
use crate::transaction::ReadLockContext;
use crate::transaction::ResolveLocksContext;
use crate::transaction::SnapshotRuntimeStats;
use crate::transaction::SnapshotVisibilityValidator;
use crate::BoundRange;

/// Result returned by a transaction binlog prewrite.
pub trait BinlogWriteResult: Send + Sync {
    fn skipped(&self) -> bool;
    fn get_error(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)>;
}

/// Replicates binlog records during transaction commit.
#[async_trait]
pub trait BinlogExecutor: Send + Sync {
    async fn prewrite(
        &self,
        cancellation: crate::async_util::Cancellation,
        primary: &[u8],
    ) -> Box<dyn BinlogWriteResult>;

    async fn commit(&self, cancellation: crate::async_util::Cancellation, commit_timestamp: i64);

    fn skip(&self);
}

/// Source request identity used by resource control and txn-file admission.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestSource {
    pub internal: bool,
    pub source_type: String,
    pub explicit_source_type: String,
}

impl RequestSource {
    pub fn context_value(&self) -> String {
        if self.source_type.is_empty() && self.explicit_source_type.is_empty() {
            return "unknown".to_owned();
        }
        let origin = if self.internal {
            "internal"
        } else {
            "external"
        };
        let source = if self.source_type.is_empty() {
            "unknown"
        } else {
            self.source_type.as_str()
        };
        let mut value = format!("{origin}_{source}");
        if !self.explicit_source_type.is_empty() && self.explicit_source_type != self.source_type {
            value.push('_');
            value.push_str(&self.explicit_source_type);
        }
        value
    }

    pub fn is_internal(&self) -> bool {
        self.context_value().starts_with("internal")
    }
}

/// Options for client-go's pipelined transaction path.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PipelinedTxnOptions {
    pub enable: bool,
    pub flush_concurrency: usize,
    pub resolve_lock_concurrency: usize,
    /// `[0, 1)`: zero disables throttling and one suppresses every write.
    pub write_throttle_ratio: f64,
}

/// Hooks invoked immediately before and after a transaction-owned background task.
#[derive(Clone, Default)]
pub struct LifecycleHooks {
    pub pre: Option<Arc<dyn Fn() + Send + Sync>>,
    pub post: Option<Arc<dyn Fn() + Send + Sync>>,
}

pub trait SchemaVersion: Send + Sync {
    fn schema_meta_version(&self) -> i64;
}

pub struct RelatedSchemaChange {
    pub physical_table_ids: Vec<i64>,
    pub action_types: Vec<u64>,
    pub latest_info_schema: Arc<dyn SchemaVersion>,
}

const MAX_EXECUTION_TIME_EXCEEDED_SIGNAL: u32 = 2;

fn effective_pessimistic_lock_wait_time(context: &mut LockContext, now: SystemTime) -> Result<i64> {
    if let Some(killed) = &context.killed {
        let signal = killed.load(atomic::Ordering::Acquire);
        if signal != 0 {
            return Err(crate::error::QueryInterruptedWithSignalError { signal }.into());
        }
    }
    if context
        .max_execution_deadline
        .is_some_and(|deadline| now > deadline)
    {
        return Err(crate::error::QueryInterruptedWithSignalError {
            signal: MAX_EXECUTION_TIME_EXCEEDED_SIGNAL,
        }
        .into());
    }

    let wait_time = context.lock_wait_time();
    if wait_time == LOCK_NO_WAIT || wait_time <= 0 {
        return Ok(LOCK_NO_WAIT);
    }
    let mut effective = wait_time;
    if wait_time != LOCK_ALWAYS_WAIT {
        let elapsed = context
            .wait_start_time
            .and_then(|started| now.duration_since(started).ok())
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX);
        effective = wait_time.saturating_sub(elapsed);
        if effective <= 0 {
            return Ok(LOCK_NO_WAIT);
        }
    }
    if let Some(deadline) = context.max_execution_deadline {
        let remaining = deadline
            .duration_since(now)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX);
        if remaining <= 0 {
            return Ok(LOCK_NO_WAIT);
        }
        effective = effective.min(remaining);
    }
    Ok(effective)
}

fn pessimistic_deadlock(error: &Error) -> Option<kvrpcpb::Deadlock> {
    match error {
        Error::KeyError(error) => error.deadlock.clone(),
        Error::ExtractedErrors(errors) | Error::MultipleKeyErrors(errors) => {
            errors.iter().find_map(pessimistic_deadlock)
        }
        Error::PessimisticLockError { inner, .. } => pessimistic_deadlock(inner),
        Error::Deadlock(error) => Some(error.deadlock.clone()),
        _ => None,
    }
}

fn normalize_prewrite_error(error: Error) -> Error {
    let errors = match error {
        Error::ExtractedErrors(errors) | Error::MultipleKeyErrors(errors) if !errors.is_empty() => {
            errors
        }
        error => return error,
    };
    let mut errors = errors
        .into_iter()
        .map(normalize_prewrite_error)
        .collect::<Vec<_>>();
    let selected = errors
        .iter()
        .position(|error| !matches!(error, Error::AssertionFailed(_)))
        .unwrap_or(0);
    errors.remove(selected)
}

pub trait SchemaLeaseChecker: Send + Sync {
    fn check_by_schema_version(
        &self,
        transaction_timestamp: u64,
        start_schema_version: &dyn SchemaVersion,
    ) -> Result<RelatedSchemaChange>;
}

pub trait KvFilter: Send + Sync {
    fn is_unnecessary_key_value(
        &self,
        key: &[u8],
        value: &[u8],
        flags: MutationFlags,
    ) -> Result<bool>;
}

/// Assertion attached to one buffered mutation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MutationAssertion {
    #[default]
    None,
    Exist,
    NotExist,
    /// The caller cannot make an existence assertion. This intentionally
    /// emits `Assertion::None`, while preserving the source flag state.
    Unknown,
}

impl MutationAssertion {
    pub(crate) fn to_proto(self) -> kvrpcpb::Assertion {
        match self {
            Self::Exist => kvrpcpb::Assertion::Exist,
            Self::NotExist => kvrpcpb::Assertion::NotExist,
            Self::None | Self::Unknown => kvrpcpb::Assertion::None,
        }
    }
}

/// Per-mutation controls corresponding to client-go's mem-buffer flags.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MutationOptions {
    pub(crate) assertion: MutationAssertion,
    pub(crate) need_constraint_check_in_prewrite: bool,
}

impl MutationOptions {
    #[must_use]
    pub fn assertion(mut self, assertion: MutationAssertion) -> Self {
        self.assertion = assertion;
        self
    }

    #[must_use]
    pub fn need_constraint_check_in_prewrite(mut self, enabled: bool) -> Self {
        self.need_constraint_check_in_prewrite = enabled;
        self
    }

    pub fn mutation_assertion(self) -> MutationAssertion {
        self.assertion
    }

    pub fn needs_constraint_check_in_prewrite(self) -> bool {
        self.need_constraint_check_in_prewrite
    }

    fn checks_existence(self) -> bool {
        matches!(
            self.assertion,
            MutationAssertion::Exist | MutationAssertion::NotExist
        )
    }
}

/// Read-only mutation state supplied to [`KvFilter`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MutationFlags {
    pub(crate) options: MutationOptions,
    pub(crate) pessimistic_locked: bool,
    pub(crate) shared_locked: bool,
    pub(crate) presume_key_not_exists: bool,
}

impl MutationFlags {
    pub fn assertion(self) -> MutationAssertion {
        self.options.assertion
    }

    pub fn needs_constraint_check_in_prewrite(self) -> bool {
        self.options.need_constraint_check_in_prewrite
    }

    pub fn is_pessimistic_locked(self) -> bool {
        self.pessimistic_locked
    }

    pub fn is_shared_locked(self) -> bool {
        self.shared_locked
    }

    pub fn presumes_key_not_exists(self) -> bool {
        self.presume_key_not_exists
    }
}

/// Policy used when prewrite encounters another transaction's lock.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PrewriteEncounterLockPolicy {
    #[default]
    TryResolve,
    NoResolve,
}

impl std::fmt::Display for PrewriteEncounterLockPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::TryResolve => "TryResolvePolicy",
            Self::NoResolve => "NoResolvePolicy",
        })
    }
}

type CommitTimestampUpperBound = Arc<dyn Fn(u64) -> bool + Send + Sync>;
type CommitCallback = Arc<dyn Fn(String, Option<String>) + Send + Sync>;

#[derive(Clone)]
struct CommitSettings {
    variables: Arc<Variables>,
    force_sync_log: bool,
    causal_consistency: bool,
    scope: String,
    disk_full_option: kvrpcpb::DiskFullOpt,
    transaction_source: u64,
    session_id: u64,
    assertion_level: kvrpcpb::AssertionLevel,
    prewrite_lock_policy: PrewriteEncounterLockPolicy,
    request_source: RequestSource,
    resource_group_tag: Option<Vec<u8>>,
    resource_group_tagger: Option<TransactionResourceGroupTagger>,
    pipelined: PipelinedTxnOptions,
    txn_file_disabled: bool,
    binlog: Option<Arc<dyn BinlogExecutor>>,
    commit_timestamp_upper_bound: Option<CommitTimestampUpperBound>,
    commit_callback: Option<CommitCallback>,
    lifecycle_hooks: LifecycleHooks,
    schema_version: Option<Arc<dyn SchemaVersion>>,
    schema_lease_checker: Option<Arc<dyn SchemaLeaseChecker>>,
    kv_filter: Option<Arc<dyn KvFilter>>,
    commit_wait_until_tso: u64,
    commit_wait_until_tso_timeout: Duration,
}

impl Default for CommitSettings {
    fn default() -> Self {
        Self {
            variables: DEFAULT_VARIABLES.clone(),
            force_sync_log: false,
            causal_consistency: false,
            scope: crate::oracle::GLOBAL_TXN_SCOPE.to_owned(),
            disk_full_option: kvrpcpb::DiskFullOpt::NotAllowedOnFull,
            transaction_source: 0,
            session_id: 0,
            assertion_level: kvrpcpb::AssertionLevel::Off,
            prewrite_lock_policy: PrewriteEncounterLockPolicy::TryResolve,
            request_source: RequestSource::default(),
            resource_group_tag: None,
            resource_group_tagger: None,
            pipelined: PipelinedTxnOptions::default(),
            txn_file_disabled: false,
            binlog: None,
            commit_timestamp_upper_bound: None,
            commit_callback: None,
            lifecycle_hooks: LifecycleHooks::default(),
            schema_version: None,
            schema_lease_checker: None,
            kv_filter: None,
            commit_wait_until_tso: 0,
            commit_wait_until_tso_timeout: Duration::from_secs(1),
        }
    }
}

impl CommitSettings {
    fn apply_request_context<R: StoreRequest>(&self, request: &mut R, timeout: Duration) {
        let mut context = request.tikv_context().cloned().unwrap_or_default();
        context.sync_log = self.force_sync_log;
        context.disk_full_opt = self.disk_full_option as i32;
        context.txn_source = self.transaction_source;
        context.request_source = self.request_source.context_value();
        context.max_execution_duration_ms = timeout.as_millis().try_into().unwrap_or(u64::MAX);
        if let Some(tag) = &self.resource_group_tag {
            context.resource_group_tag.clone_from(tag);
        }
        assert!(request.attach_context(context));
    }

    fn apply_request<R: StoreRequest>(&self, request: &mut R, timeout: Duration) {
        self.apply_request_context(request, timeout);
        if self.resource_group_tag.is_none() {
            if let Some(tagger) = &self.resource_group_tagger {
                tagger(request);
            }
        }
    }

    fn apply_txn_file_prewrite(
        &self,
        request: &mut kvrpcpb::PrewriteRequest,
        first_key: &[u8],
        timeout: Duration,
    ) {
        self.apply_request_context(request, timeout);
        if self.resource_group_tag.is_some() {
            return;
        }
        let Some(tagger) = &self.resource_group_tagger else {
            return;
        };
        let mut tag_request = request.clone();
        if !first_key.is_empty() {
            tag_request.mutations = vec![kvrpcpb::Mutation {
                key: first_key.to_vec(),
                ..Default::default()
            }];
        }
        tagger(&mut tag_request);
        let tag = tag_request
            .context
            .as_ref()
            .map(|context| context.resource_group_tag.clone())
            .unwrap_or_default();
        request
            .context
            .get_or_insert_with(kvrpcpb::Context::default)
            .resource_group_tag = tag;
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WriteAccessLevel {
    Ttl = 1,
    TwoPc = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TxnFileAction {
    Prewrite,
    Commit,
    Rollback,
}

struct MinCommitTsState {
    value: u64,
    required_write_access: WriteAccessLevel,
}

#[derive(Clone)]
struct MinCommitTsManager(Arc<Mutex<MinCommitTsState>>);

impl Default for MinCommitTsManager {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(MinCommitTsState {
            value: 0,
            required_write_access: WriteAccessLevel::Ttl,
        })))
    }
}

impl MinCommitTsManager {
    fn try_update(&self, new_value: u64, write_access: WriteAccessLevel) {
        let mut state = self.0.lock().unwrap();
        if write_access >= state.required_write_access && new_value > state.value {
            state.value = new_value;
        }
    }

    fn elevate_write_access(&self, new_level: WriteAccessLevel) -> u64 {
        let mut state = self.0.lock().unwrap();
        state.required_write_access = state.required_write_access.max(new_level);
        state.value
    }

    fn get(&self) -> u64 {
        self.0.lock().unwrap().value
    }

    #[cfg(test)]
    fn required_write_access(&self) -> WriteAccessLevel {
        self.0.lock().unwrap().required_write_access
    }
}
use crate::Error;
use crate::Key;
use crate::KvPair;
use crate::Priority;
use crate::Result;
use crate::Value;

const SNAPSHOT_READ_TIMEOUT_SHORT: Duration = Duration::from_secs(30);
const SNAPSHOT_READ_TIMEOUT_MEDIUM: Duration = Duration::from_secs(60);
/// client-go's `txnkv/txnsnapshot.DefaultScanBatchSize`.
pub(crate) const DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE: u32 = super::snapshot::DEFAULT_SCAN_BATCH_SIZE;

fn snapshot_runtime_interceptor(
    interceptor: Option<RpcInterceptorChain>,
    stats: Option<Arc<SnapshotRuntimeStats>>,
) -> Option<RpcInterceptorChain> {
    let mut interceptor = interceptor.unwrap_or_default();
    interceptor.link(snapshot_read_sli_interceptor());
    if let Some(stats) = stats {
        interceptor.link(stats.interceptor());
    }
    Some(interceptor)
}

/// The snapshot read operation for which a resource-group tag is being built.
///
/// This is the Rust counterpart of the request type visible to client-go's
/// `tikvrpc.ResourceGroupTagger`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotRequestType {
    Get,
    BatchGet,
    BufferBatchGet,
    Scan,
}

pub(crate) struct SnapshotScannerBatch {
    pub(crate) pairs: Vec<KvPair>,
    pub(crate) next_range: BoundRange,
    pub(crate) exhausted: bool,
}

/// Builds a TiKV resource-group tag for a snapshot read when no static tag is
/// configured. The operation kind identifies the source request being sent.
pub type SnapshotResourceGroupTagger = Arc<dyn Fn(SnapshotRequestType) -> Vec<u8> + Send + Sync>;

/// Transaction write-request tagger corresponding to client-go's
/// `tikvrpc.ResourceGroupTagger`.
pub type TransactionResourceGroupTagger = crate::store::ResourceGroupTagger;

#[derive(Clone)]
struct AggressiveLockEntry {
    has_return_value: bool,
    has_check_existence: bool,
    value: ReturnedValue,
    actual_for_update_ts: Timestamp,
}

#[derive(Default)]
struct AggressiveLockingContext {
    current: BTreeMap<Key, AggressiveLockEntry>,
    previous: BTreeMap<Key, AggressiveLockEntry>,
}

#[derive(Clone, Default)]
struct PipelinedTransactionState {
    generation: u64,
    flushed_mutations: BTreeMap<Vec<u8>, kvrpcpb::Mutation>,
    range_start: Option<Vec<u8>>,
    range_end: Option<Vec<u8>>,
    flush_wait_duration: Duration,
    flush_duration_ewma_ms: f64,
}

/// An undo-able set of actions on the dataset.
///
/// Create a transaction using a [`TransactionClient`](crate::TransactionClient), then run actions
/// (such as `get`, or `put`) on the transaction. Reads are executed immediately, writes are
/// buffered locally. Once complete, `commit` the transaction. Behind the scenes, the client will
/// perform a two phase commit and return success as soon as the writes are guaranteed to be
/// committed (some finalisation may continue in the background after the return, but no data can be
/// lost).
///
/// TiKV transactions use multi-version concurrency control. All reads logically happen at the start
/// of the transaction (at the start timestamp, `start_ts`). Once a transaction is commited, a
/// its writes atomically become visible to other transactions at (logically) the commit timestamp.
///
/// In other words, a transaction can read data that was committed at `commit_ts` < its `start_ts`,
/// and its writes are readable by transactions with `start_ts` >= its `commit_ts`.
///
/// Mutations are buffered locally and sent to the TiKV cluster at the time of commit.
/// In a pessimistic transaction, all write operations and `xxx_for_update` operations will immediately
/// acquire locks from TiKV. Such a lock blocks other transactions from writing to that key.
/// A lock exists until the transaction is committed or rolled back, or the lock reaches its time to
/// live (TTL).
///
/// For details, the [SIG-Transaction](https://github.com/tikv/sig-transaction)
/// provides materials explaining designs and implementations of TiKV transactions.
///
/// # Examples
///
/// ```rust,no_run
/// # use tikv_client::{Config, TransactionClient};
/// # use futures::prelude::*;
/// # futures::executor::block_on(async {
/// let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
/// let mut txn = client.begin_optimistic().await.unwrap();
/// let foo = txn.get("foo".to_owned()).await.unwrap().unwrap();
/// txn.put("bar".to_owned(), foo).await.unwrap();
/// txn.commit().await.unwrap();
/// # });
/// ```
pub struct Transaction<PdC: PdClient = PdRpcClient> {
    status: Arc<AtomicU8>,
    timestamp: Timestamp,
    buffer: Buffer,
    rpc: Arc<PdC>,
    options: TransactionOptions,
    commit_settings: CommitSettings,
    commit_timestamp: Option<Timestamp>,
    keyspace: Keyspace,
    /// Canonical API V2 keyspace name retained from the creating client so
    /// direct and retry/shard-cloned requests receive the same context.
    keyspace_name: Option<String>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<String>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    /// Snapshot-only callers may replace this with source replica-read
    /// settings; ordinary transactions retain direct leader reads.
    replica_read_config: ReplicaReadConfig,
    /// Source `KVSnapshot.replicaReadAdjuster`, retained independently from
    /// the stable selection settings because it runs per get/batch-get.
    replica_read_adjuster: Option<ReplicaReadAdjuster>,
    /// Number of keys TiKV skips after each scan result. Zero disables
    /// sampling, matching client-go `KVSnapshot.sampleStep`.
    sample_step: u32,
    /// Forces TiKV scan requests to return keys without values, matching
    /// client-go `KVSnapshot.keyOnly`.
    snapshot_key_only: bool,
    /// Maximum number of pairs requested by one snapshot scan RPC. The
    /// caller's scan limit remains the overall result limit.
    snapshot_scan_batch_size: u32,
    /// Optional source-compatible collector for physical snapshot read RPCs.
    snapshot_runtime_stats: Option<Arc<SnapshotRuntimeStats>>,
    /// Source `KVSnapshot.vars`, retained by snapshot retry owners.
    snapshot_variables: Arc<Variables>,
    /// Enables client-go's pipelined BufferBatchGet tier for this snapshot.
    snapshot_pipelined: bool,
    /// Snapshot-only request-context settings retained through physical read
    /// retries, matching client-go `KVSnapshot`.
    not_fill_cache: bool,
    isolation_level: kvrpcpb::IsolationLevel,
    task_id: u64,
    resource_group_tag: Option<Vec<u8>>,
    resource_group_tagger: Option<SnapshotResourceGroupTagger>,
    snapshot_read_timeout: Option<Duration>,
    /// Client-go validates each physical snapshot read timestamp before it is
    /// sent. Directly constructed transactions intentionally retain no
    /// validator; transaction clients provide the PD-backed implementation.
    read_timestamp_validator: Option<Arc<dyn ReadTimestampValidator>>,
    /// Source `kvstore.CheckVisibility` dependency. The snapshot owns the
    /// post-response call sites; the root store owns the GC safe-point state.
    snapshot_visibility_validator: Option<Arc<dyn SnapshotVisibilityValidator>>,
    /// Source `KVSnapshot.readReplicaScope`, used by Get and BatchGet
    /// timestamp validation. Scanner requests retain client-go's global
    /// default because the source scanner does not copy this field.
    read_replica_scope: String,
    /// client-go keeps resolved/committed transaction IDs on each snapshot;
    /// they must not leak across transactions with different read timestamps.
    read_lock_context: ReadLockContext,
    /// Shared client-side final-status and resolving-lock observer state.
    lock_resolver_context: ResolveLocksContext,
    is_heartbeat_started: bool,
    pessimistic_lock_count: usize,
    /// Set once the transaction enters the commit path (`StartedCommit`), where
    /// prewrite may place 2PC locks. Kept as a dedicated flag because the status
    /// transitions to `StartedRollback` on rollback, losing the fact that commit
    /// had started — which a rollback retry would otherwise need to know.
    prewritten: bool,
    aggressive_locking: Option<AggressiveLockingContext>,
    aggressive_locking_dirty: bool,
    pipelined_state: PipelinedTransactionState,
    memory_footprint_hook: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    start_instant: Instant,
    latches: Option<Arc<LatchesScheduler>>,
}

impl<PdC: PdClient> Transaction<PdC> {
    #[cfg(test)]
    pub(crate) fn new(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
    ) -> Transaction<PdC> {
        Self::new_with_latches(timestamp, rpc, options, keyspace, None)
    }

    #[cfg(test)]
    pub(crate) fn new_with_latches(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
        latches: Option<Arc<LatchesScheduler>>,
    ) -> Transaction<PdC> {
        Self::new_with_latches_and_keyspace_name(timestamp, rpc, options, keyspace, None, latches)
    }

    pub(crate) fn new_with_latches_and_keyspace_name(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
        keyspace_name: Option<String>,
        latches: Option<Arc<LatchesScheduler>>,
    ) -> Transaction<PdC> {
        let status = if options.read_only {
            TransactionStatus::ReadOnly
        } else {
            TransactionStatus::Active
        };
        let mut commit_settings = CommitSettings::default();
        commit_settings.scope = options.scope.clone();
        commit_settings.pipelined = options.pipelined;
        Transaction {
            status: Arc::new(AtomicU8::new(status as u8)),
            timestamp,
            buffer: Buffer::new(options.is_pessimistic()),
            rpc,
            options,
            commit_settings,
            commit_timestamp: None,
            keyspace,
            keyspace_name,
            rpc_interceptor: None,
            resource_group_name: None,
            resource_control: None,
            ru_details: None,
            replica_read_config: ReplicaReadConfig::default(),
            replica_read_adjuster: None,
            sample_step: 0,
            snapshot_key_only: false,
            snapshot_scan_batch_size: DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE,
            snapshot_runtime_stats: None,
            snapshot_variables: DEFAULT_VARIABLES.clone(),
            snapshot_pipelined: false,
            not_fill_cache: false,
            isolation_level: kvrpcpb::IsolationLevel::Si,
            task_id: 0,
            resource_group_tag: None,
            resource_group_tagger: None,
            snapshot_read_timeout: None,
            read_timestamp_validator: None,
            snapshot_visibility_validator: None,
            read_replica_scope: String::new(),
            read_lock_context: ReadLockContext::default(),
            lock_resolver_context: ResolveLocksContext::default(),
            is_heartbeat_started: false,
            pessimistic_lock_count: 0,
            prewritten: false,
            aggressive_locking: None,
            aggressive_locking_dirty: false,
            pipelined_state: PipelinedTransactionState::default(),
            memory_footprint_hook: None,
            start_instant: std::time::Instant::now(),
            latches,
        }
    }

    fn plan<Req: KvRequest>(&self, request: Req) -> PlanBuilder<PdC, Dispatch<Req>, NoTarget> {
        PlanBuilder::new(self.rpc.clone(), self.keyspace, request)
            .request_source(self.commit_settings.request_source.context_value())
            .keyspace_name_option(self.keyspace_name.as_deref())
            .rpc_interceptor_option(self.rpc_interceptor.clone())
            .resource_group_option(self.resource_group_name.as_deref())
            .resource_control_option(self.resource_control.clone())
            .ru_details_option(self.ru_details.clone())
            .replica_read(self.replica_read_config.clone())
    }

    pub(crate) fn set_replica_read_config(&mut self, config: ReplicaReadConfig) {
        self.replica_read_config = config;
    }

    pub(crate) fn set_lock_resolver_context(&mut self, context: ResolveLocksContext) {
        self.lock_resolver_context = context;
    }

    /// Reset a read-only snapshot's timestamp and discard retry hints that
    /// were valid only for the previous read version.
    pub(crate) fn set_snapshot_timestamp(&mut self, timestamp: Timestamp) {
        let version = timestamp.version();
        assert!(
            version < i64::MAX as u64 || version == u64::MAX,
            "try to get snapshot with a large ts {version}"
        );
        self.timestamp = timestamp;
        self.buffer.clear_cached_reads();
        self.read_lock_context.clear_resolved();
    }

    pub(crate) fn snapshot_cache_hit_count(&self) -> usize {
        self.buffer.snapshot_cache_hit_count()
    }

    pub(crate) fn snapshot_cache_size(&self) -> usize {
        self.buffer.snapshot_cache_size()
    }

    pub(crate) fn snapshot_cache(&self) -> BTreeMap<Key, ValueEntry> {
        self.buffer
            .snapshot_cache()
            .into_iter()
            .map(|(key, value)| (key.truncate_keyspace(self.keyspace), value))
            .collect()
    }

    pub(crate) fn update_snapshot_cache(
        &mut self,
        keys: impl IntoIterator<Item = Key>,
        values: BTreeMap<Key, ValueEntry>,
    ) {
        if self.timestamp.version() == u64::MAX {
            return;
        }
        let keys: Vec<_> = keys
            .into_iter()
            .map(|key| key.encode_keyspace(self.keyspace, KeyMode::Txn))
            .collect();
        let values = values
            .into_iter()
            .map(|(key, value)| (key.encode_keyspace(self.keyspace, KeyMode::Txn), value))
            .collect();
        self.buffer.update_snapshot_cache(keys, &values);
    }

    pub(crate) fn clean_snapshot_cache(&mut self, keys: impl IntoIterator<Item = Key>) {
        self.buffer.clean_snapshot_cache(
            keys.into_iter()
                .map(|key| key.encode_keyspace(self.keyspace, KeyMode::Txn)),
        );
    }

    pub(crate) fn set_stale_read(&mut self, stale_read: bool) {
        self.replica_read_config.stale_read = stale_read;
    }

    pub(crate) fn set_match_store_labels(&mut self, labels: Vec<crate::proto::metapb::StoreLabel>) {
        self.replica_read_config.labels = labels;
    }

    pub(crate) fn set_load_based_replica_read_threshold(&mut self, busy_threshold: Duration) {
        self.replica_read_config.busy_threshold_ms = u32::try_from(busy_threshold.as_millis())
            .ok()
            .filter(|threshold| *threshold != 0)
            .unwrap_or_default();
    }

    pub(crate) fn set_replica_read_adjuster(&mut self, adjuster: ReplicaReadAdjuster) {
        self.replica_read_adjuster = Some(adjuster);
    }

    pub(crate) fn set_sample_step(&mut self, sample_step: u32) {
        self.sample_step = sample_step;
    }

    pub(crate) fn set_snapshot_key_only(&mut self, key_only: bool) {
        self.snapshot_key_only = key_only;
    }

    pub(crate) fn set_snapshot_scan_batch_size(&mut self, batch_size: u32) {
        self.snapshot_scan_batch_size = batch_size;
    }

    pub(crate) fn snapshot_scan_batch_size(&self) -> u32 {
        if self.snapshot_scan_batch_size <= 1 {
            DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE
        } else {
            self.snapshot_scan_batch_size
        }
    }

    pub(crate) fn set_snapshot_runtime_stats(&mut self, stats: Option<Arc<SnapshotRuntimeStats>>) {
        self.snapshot_runtime_stats = stats;
    }

    pub(crate) fn set_snapshot_variables(&mut self, variables: Arc<Variables>) {
        self.snapshot_variables = variables;
    }

    /// Source pipelined snapshots must read through locks flushed by their
    /// own transaction rather than trying to resolve them.
    pub(crate) fn set_snapshot_pipelined(&mut self, timestamp: u64) {
        self.snapshot_pipelined = true;
        self.read_lock_context.add_resolved(timestamp);
    }

    pub(crate) fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        self.not_fill_cache = not_fill_cache;
    }

    pub(crate) fn set_isolation_level(&mut self, isolation_level: kvrpcpb::IsolationLevel) {
        self.isolation_level = isolation_level;
    }

    pub(crate) fn set_task_id(&mut self, task_id: u64) {
        self.task_id = task_id;
    }

    pub fn set_resource_group_tag(&mut self, resource_group_tag: Option<Vec<u8>>) {
        self.commit_settings.resource_group_tag = resource_group_tag.clone();
        self.resource_group_tag = resource_group_tag;
    }

    pub(crate) fn set_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<SnapshotResourceGroupTagger>,
    ) {
        self.resource_group_tagger = resource_group_tagger;
    }

    pub fn set_transaction_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<TransactionResourceGroupTagger>,
    ) {
        self.commit_settings.resource_group_tagger = resource_group_tagger;
    }

    pub(crate) fn set_read_timestamp_validator(
        &mut self,
        validator: Arc<dyn ReadTimestampValidator>,
    ) {
        self.read_timestamp_validator = Some(validator);
    }

    pub(crate) fn set_snapshot_visibility_validator(
        &mut self,
        validator: Arc<dyn SnapshotVisibilityValidator>,
    ) {
        self.snapshot_visibility_validator = Some(validator);
    }

    pub(crate) fn set_read_replica_scope(&mut self, scope: impl Into<String>) {
        self.read_replica_scope = scope.into();
    }

    pub(crate) fn set_snapshot_read_timeout(&mut self, timeout: Duration) {
        self.snapshot_read_timeout = (!timeout.is_zero()).then_some(timeout);
    }

    pub(crate) fn snapshot_read_timeout(&self) -> Option<Duration> {
        self.snapshot_read_timeout
    }

    fn pipelined_pending_mutations(&self) -> Vec<kvrpcpb::Mutation> {
        self.buffer
            .to_proto_mutations()
            .into_iter()
            .map(|mut mutation| {
                if self.commit_settings.assertion_level == kvrpcpb::AssertionLevel::Off {
                    mutation.assertion = kvrpcpb::Assertion::None as i32;
                }
                mutation
            })
            .filter(|mutation| {
                self.pipelined_state.flushed_mutations.get(&mutation.key) != Some(mutation)
            })
            .collect()
    }

    fn notify_memory_footprint_change(&self) {
        if let Some(hook) = &self.memory_footprint_hook {
            hook(self.buffer.memory_footprint());
        }
    }

    async fn maybe_flush_pipelined(&mut self, force: bool) -> Result<bool> {
        if !self.is_pipelined() {
            return Ok(false);
        }
        if self.buffer.has_shared_locks() {
            return Err(Error::StringError(
                "shared lock is not supported in pipelined transaction".to_owned(),
            ));
        }
        let pending = self.pipelined_pending_mutations();
        if pending.is_empty() {
            return Ok(false);
        }
        let pending_size = pending.iter().fold(0_u64, |total, mutation| {
            total.saturating_add((mutation.key.len() + mutation.value.len()) as u64)
        });
        if !force
            && (pending_size < MIN_FLUSH_MEMORY
                || (pending.len() < MIN_FLUSH_KEYS && pending_size < FORCE_FLUSH_MEMORY))
        {
            return Ok(false);
        }

        let primary = self
            .buffer
            .get_primary_key()
            .or_else(|| {
                pending
                    .iter()
                    .find(|mutation| {
                        kvrpcpb::Op::try_from(mutation.op) != Ok(kvrpcpb::Op::CheckNotExists)
                    })
                    .map(|mutation| Key::from(mutation.key.clone()))
            })
            .ok_or_else(|| {
                Error::StringError(
                    "[pipelined dml] primary key should be set before pipelined flush".to_owned(),
                )
            })?;
        let generation = self.pipelined_state.generation.saturating_add(1);
        let mut request = new_flush_request(
            pending.clone(),
            primary,
            self.timestamp.clone(),
            Timestamp::from_version(self.timestamp.version().saturating_add(1)),
            generation,
            MAX_TTL.max(DEFAULT_LOCK_TTL),
        );
        request.assertion_level = self.commit_settings.assertion_level as i32;
        self.commit_settings
            .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
        request
            .context
            .get_or_insert_with(kvrpcpb::Context::default)
            .request_source = "external_pdml".to_owned();

        let ratio = self.commit_settings.pipelined.write_throttle_ratio;
        if (0.0..1.0).contains(&ratio) && ratio > 0.0 {
            let sleep_ms = ratio / (1.0 - ratio) * self.pipelined_state.flush_duration_ewma_ms;
            if sleep_ms >= 1.0 {
                tokio::time::sleep(Duration::from_millis(sleep_ms as u64)).await;
            }
        }
        let started = Instant::now();
        let flushed_len = pending.len();
        plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            self.resource_group_name.as_deref(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
        )
        .priority(self.options.priority)
        .resolve_lock_with_context(
            self.timestamp.clone(),
            self.options.retry_options.lock_backoff.clone(),
            self.keyspace,
            self.lock_resolver_context.clone(),
        )
        .retry_multi_region_with_concurrency(
            self.options.retry_options.region_backoff.clone(),
            self.commit_settings.pipelined.flush_concurrency.max(1),
        )
        .merge(CollectError)
        .extract_error()
        .plan()
        .execute()
        .await?;
        let elapsed = started.elapsed();
        crate::stats::observe_pipelined_flush(flushed_len, pending_size as usize, elapsed);

        let first_key = pending.first().map(|mutation| mutation.key.clone());
        let last_key = pending.last().map(|mutation| mutation.key.clone());
        for mutation in pending {
            self.pipelined_state
                .flushed_mutations
                .insert(mutation.key.clone(), mutation);
        }
        self.pipelined_state.generation = generation;
        if let Some(first_key) = first_key {
            if self
                .pipelined_state
                .range_start
                .as_ref()
                .is_none_or(|current| first_key < *current)
            {
                self.pipelined_state.range_start = Some(first_key);
            }
        }
        if let Some(last_key) = last_key {
            if self
                .pipelined_state
                .range_end
                .as_ref()
                .is_none_or(|current| last_key > *current)
            {
                self.pipelined_state.range_end = Some(last_key);
            }
        }
        let elapsed_ms = elapsed.as_secs_f64() * 1_000.0;
        self.pipelined_state.flush_duration_ewma_ms =
            if self.pipelined_state.flush_duration_ewma_ms == 0.0 {
                elapsed_ms
            } else {
                self.pipelined_state.flush_duration_ewma_ms * 0.8 + elapsed_ms * 0.2
            };
        Ok(true)
    }

    fn replica_read_config_for_items(&self, item_count: usize) -> ReplicaReadConfig {
        let mut config = self.replica_read_config.clone();
        if config.read_type.is_follower_read() {
            if let Some(adjuster) = &self.replica_read_adjuster {
                config.apply_adjustment(adjuster(item_count));
            }
        }
        config
    }

    /// Replace the RPC interceptor used by this transaction's reads, writes,
    /// retries, and lock-resolution requests.
    pub fn set_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        let mut chain = RpcInterceptorChain::new();
        chain.link(interceptor);
        self.rpc_interceptor = Some(chain);
    }

    /// Add an RPC interceptor after the existing chain.
    pub fn add_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        match &mut self.rpc_interceptor {
            Some(chain) => {
                chain.link(interceptor);
            }
            None => self.set_rpc_interceptor(interceptor),
        }
    }

    /// Assign subsequent transaction requests to `resource_group_name`.
    /// The group is sent on every physical TiKV request, including retries.
    pub fn set_resource_group_name(&mut self, resource_group_name: impl Into<String>) {
        self.resource_group_name = Some(resource_group_name.into());
    }

    /// Native shorthand for [`Self::set_resource_group_name`].
    pub fn set_resource_group(&mut self, resource_group_name: impl Into<String>) {
        self.set_resource_group_name(resource_group_name);
    }

    /// Attach a PD resource-group controller to subsequent transaction RPCs.
    pub fn set_resource_control(&mut self, controller: ResourceGroupControllerHandle) {
        self.resource_control = Some(controller);
    }

    /// Attach source-compatible resource-unit accounting to subsequent
    /// transaction RPCs, including retries and commit/rollback requests.
    pub fn set_ru_details(&mut self, ru_details: Arc<crate::RuDetails>) {
        self.ru_details = Some(ru_details);
    }

    /// Create a new 'get' request
    ///
    /// Once resolved this request will result in the fetching of the value associated with the
    /// given key.
    ///
    /// Retuning `Ok(None)` indicates the key does not exist in TiKV.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use tikv_client::{Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let result: Option<Value> = txn.get(key).await.unwrap();
    /// # });
    /// ```
    pub async fn get(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        let _timer = crate::stats::snapshot_command_timer(
            "get",
            self.commit_settings.request_source.is_internal(),
        );
        trace!("invoking transactional get request");
        self.check_allow_operation().await?;
        let timestamp = self.timestamp.clone();
        let snapshot_version = timestamp.version();
        let cache_snapshot_read = !self.options.read_only || timestamp.version() != u64::MAX;
        let rpc = self.rpc.clone();
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let retry_options = self.options.retry_options.clone();
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let rpc_interceptor = snapshot_runtime_interceptor(
            self.rpc_interceptor.clone(),
            self.snapshot_runtime_stats.clone(),
        );
        let snapshot_runtime_stats = self.snapshot_runtime_stats.clone();
        let snapshot_variables = self.snapshot_variables.clone();
        let resource_group_name = self.resource_group_name.clone();
        let resource_control = self.resource_control.clone();
        let ru_details = self.ru_details.clone();
        let priority = self.options.priority;
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let snapshot_read_timeout = self.snapshot_read_timeout;
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let read_replica_scope = self.read_replica_scope.clone();
        let replica_read_config = self.replica_read_config_for_items(1);
        let read_lock_context = self.read_lock_context.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let request_source = self.commit_settings.request_source.context_value();
        let max_timestamp_point_get = timestamp.version() == u64::MAX;

        self.buffer
            .get_or_else_with_cache(key, cache_snapshot_read, |key| async move {
                let request = new_get_request(key, timestamp.clone());
                let resource_group_tag = resource_group_tag.clone().or_else(|| {
                    resource_group_tagger
                        .as_ref()
                        .map(|tagger| tagger(SnapshotRequestType::Get))
                });
                let plan = plan_with_keyspace_name(
                    rpc,
                    keyspace,
                    keyspace_name.as_deref(),
                    rpc_interceptor,
                    resource_group_name.as_deref(),
                    resource_control,
                    ru_details,
                    replica_read_config.clone(),
                    request,
                )
                .request_source(request_source)
                .priority(priority)
                .not_fill_cache(not_fill_cache)
                .isolation_level(isolation_level)
                .task_id(task_id)
                .resource_group_tag(resource_group_tag)
                .snapshot_read_timeout(snapshot_read_timeout, SNAPSHOT_READ_TIMEOUT_SHORT)
                .validate_read_timestamp(
                    read_timestamp_validator,
                    timestamp.version(),
                    replica_read_config.stale_read,
                    read_replica_scope,
                )
                .resolve_lock_for_read(
                    timestamp,
                    retry_options.lock_backoff,
                    keyspace,
                    read_lock_context,
                    lock_resolver_context,
                    snapshot_runtime_stats.clone(),
                    snapshot_variables.clone(),
                )
                .max_timestamp_point_get(max_timestamp_point_get)
                .retry_multi_region_with_snapshot_stats(
                    DEFAULT_REGION_BACKOFF,
                    snapshot_runtime_stats.clone(),
                    snapshot_variables,
                )
                .merge(CollectSingle)
                .post_process_default()
                .plan();
                let value = plan.execute().await?;
                if let Some(validator) = snapshot_visibility_validator {
                    validator.check_visibility(snapshot_version).await?;
                }
                Ok(value)
            })
            .await
    }

    /// Read a snapshot entry with source `GetOption` behavior. In particular,
    /// [`GetOption::ReturnCommitTs`] requests TiKV's commit timestamp and will
    /// not reuse a cached non-empty entry whose timestamp is unknown.
    pub async fn get_with_options(
        &mut self,
        key: impl Into<Key>,
        options: &[GetOption],
    ) -> Result<Option<ValueEntry>> {
        let _timer = crate::stats::snapshot_command_timer(
            "get",
            self.commit_settings.request_source.is_internal(),
        );
        trace!("invoking transactional get request with options");
        self.check_allow_operation().await?;
        let mut get_options = GetOptions::default();
        get_options.apply(options);
        let return_commit_ts = get_options.return_commit_ts();
        let timestamp = self.timestamp.clone();
        let snapshot_version = timestamp.version();
        let cache_snapshot_read = !self.options.read_only || timestamp.version() != u64::MAX;
        let rpc = self.rpc.clone();
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let retry_options = self.options.retry_options.clone();
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let rpc_interceptor = snapshot_runtime_interceptor(
            self.rpc_interceptor.clone(),
            self.snapshot_runtime_stats.clone(),
        );
        let snapshot_runtime_stats = self.snapshot_runtime_stats.clone();
        let snapshot_variables = self.snapshot_variables.clone();
        let resource_group_name = self.resource_group_name.clone();
        let resource_control = self.resource_control.clone();
        let ru_details = self.ru_details.clone();
        let priority = self.options.priority;
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let snapshot_read_timeout = self.snapshot_read_timeout;
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let read_replica_scope = self.read_replica_scope.clone();
        let replica_read_config = self.replica_read_config_for_items(1);
        let read_lock_context = self.read_lock_context.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let request_source = self.commit_settings.request_source.context_value();
        let max_timestamp_point_get = timestamp.version() == u64::MAX;

        self.buffer
            .get_snapshot_entry_or_else(
                key,
                return_commit_ts,
                cache_snapshot_read,
                |key| async move {
                    let mut request = new_get_request(key, timestamp.clone());
                    request.need_commit_ts = return_commit_ts;
                    let resource_group_tag = resource_group_tag.clone().or_else(|| {
                        resource_group_tagger
                            .as_ref()
                            .map(|tagger| tagger(SnapshotRequestType::Get))
                    });
                    let plan = plan_with_keyspace_name(
                        rpc,
                        keyspace,
                        keyspace_name.as_deref(),
                        rpc_interceptor,
                        resource_group_name.as_deref(),
                        resource_control,
                        ru_details,
                        replica_read_config.clone(),
                        request,
                    )
                    .request_source(request_source)
                    .priority(priority)
                    .not_fill_cache(not_fill_cache)
                    .isolation_level(isolation_level)
                    .task_id(task_id)
                    .resource_group_tag(resource_group_tag)
                    .snapshot_read_timeout(snapshot_read_timeout, SNAPSHOT_READ_TIMEOUT_SHORT)
                    .validate_read_timestamp(
                        read_timestamp_validator,
                        timestamp.version(),
                        replica_read_config.stale_read,
                        read_replica_scope,
                    )
                    .resolve_lock_for_read(
                        timestamp,
                        retry_options.lock_backoff,
                        keyspace,
                        read_lock_context,
                        lock_resolver_context,
                        snapshot_runtime_stats.clone(),
                        snapshot_variables.clone(),
                    )
                    .max_timestamp_point_get(max_timestamp_point_get)
                    .retry_multi_region_with_snapshot_stats(
                        DEFAULT_REGION_BACKOFF,
                        snapshot_runtime_stats.clone(),
                        snapshot_variables,
                    )
                    .merge(CollectSingle)
                    .plan();
                    let response = plan.execute().await?;
                    if let Some(validator) = snapshot_visibility_validator {
                        validator.check_visibility(snapshot_version).await?;
                    }
                    let entry = (!response.not_found)
                        .then(|| ValueEntry::new(response.value, response.commit_ts));
                    ensure_snapshot_commit_ts(return_commit_ts, entry.as_ref())?;
                    Ok(entry)
                },
            )
            .await
    }

    /// Create a `get for update` request.
    ///
    /// The request reads and "locks" a key. It is similar to `SELECT ... FOR
    /// UPDATE` in TiDB, and has different behavior in optimistic and
    /// pessimistic transactions.
    ///
    /// # Optimistic transaction
    ///
    /// It reads at the "start timestamp" and caches the value, just like normal
    /// get requests. The lock is written in prewrite and commit, so it cannot
    /// prevent concurrent transactions from writing the same key, but can only
    /// prevent itself from committing.
    ///
    /// # Pessimistic transaction
    ///
    /// It reads at the "current timestamp" and thus does not cache the value.
    /// So following read requests won't be affected by the `get_for_udpate`.
    /// A lock will be acquired immediately with this request, which prevents
    /// concurrent transactions from mutating the keys.
    ///
    /// The "current timestamp" (also called `for_update_ts` of the request) is fetched from PD.
    ///
    /// Note: The behavior of this command under pessimistic transaction does not follow snapshot.
    /// It reads the latest value (using current timestamp), and the value is not cached in the
    /// local buffer. So normal `get`-like commands after `get_for_update` will not be influenced,
    /// they still read values at the transaction's `start_ts`.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_pessimistic().await.unwrap();
    /// let key = "TiKV".to_owned();
    /// let result: Value = txn.get_for_update(key).await.unwrap().unwrap();
    /// // now the key "TiKV" is locked, other transactions cannot modify it
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn get_for_update(&mut self, key: impl Into<Key>) -> Result<Option<Value>> {
        debug!("invoking transactional get_for_update request");
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            let key = key.into();
            self.lock_keys(iter::once(key.clone())).await?;
            self.get(key).await
        } else {
            let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
            let mut pairs = self.pessimistic_lock(iter::once(key), true).await?;
            debug_assert!(pairs.len() <= 1);
            match pairs.pop() {
                Some(pair) => Ok(Some(pair.1)),
                None => Ok(None),
            }
        }
    }

    /// Check whether a key exists.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_pessimistic().await.unwrap();
    /// let exists = txn.key_exists("k1".to_owned()).await.unwrap();
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn key_exists(&mut self, key: impl Into<Key>) -> Result<bool> {
        debug!("invoking transactional key_exists request");
        Ok(self.get(key).await?.is_some())
    }

    /// Create a new 'batch get' request.
    ///
    /// Once resolved this request will result in the fetching of the values associated with the
    /// given keys.
    ///
    /// Non-existent entries will not appear in the result. The order of the keys is not retained in
    /// the result.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let keys = vec!["TiKV".to_owned(), "TiDB".to_owned()];
    /// let result: HashMap<Key, Value> = txn
    ///     .batch_get(keys)
    ///     .await
    ///     .unwrap()
    ///     .map(|pair| (pair.0, pair.1))
    ///     .collect();
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn batch_get(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking transactional batch_get request");
        self.check_allow_operation().await?;
        let timestamp = self.timestamp.clone();
        let snapshot_version = timestamp.version();
        let cache_snapshot_read = !self.options.read_only || timestamp.version() != u64::MAX;
        let rpc = self.rpc.clone();
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let rpc_interceptor = snapshot_runtime_interceptor(
            self.rpc_interceptor.clone(),
            self.snapshot_runtime_stats.clone(),
        );
        let snapshot_runtime_stats = self.snapshot_runtime_stats.clone();
        let snapshot_variables = self.snapshot_variables.clone();
        let resource_group_name = self.resource_group_name.clone();
        let resource_control = self.resource_control.clone();
        let ru_details = self.ru_details.clone();
        let keys = keys
            .into_iter()
            .map(move |k| k.into().encode_keyspace(keyspace, KeyMode::Txn));
        let retry_options = self.options.retry_options.clone();
        let priority = self.options.priority;
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let snapshot_read_timeout = self.snapshot_read_timeout;
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let read_replica_scope = self.read_replica_scope.clone();
        let replica_read_config = self.replica_read_config.clone();
        let replica_read_adjuster = self.replica_read_adjuster.clone();
        let read_lock_context = self.read_lock_context.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let request_source = self.commit_settings.request_source.context_value();
        let internal = self.commit_settings.request_source.is_internal();

        self.buffer
            .batch_get_or_else_with_cache(keys, cache_snapshot_read, move |keys| async move {
                let _timer = crate::stats::snapshot_command_timer("batch_get", internal);
                let keys = keys.collect::<Vec<_>>();
                let replica_read_config = adjusted_replica_read_config(
                    &replica_read_config,
                    replica_read_adjuster.as_ref(),
                    keys.len(),
                );
                let stale_read = replica_read_config.stale_read;
                let request = new_batch_get_request(keys.into_iter(), timestamp.clone());
                let resource_group_tag = resource_group_tag.clone().or_else(|| {
                    resource_group_tagger
                        .as_ref()
                        .map(|tagger| tagger(SnapshotRequestType::BatchGet))
                });
                let plan = plan_with_keyspace_name(
                    rpc,
                    keyspace,
                    keyspace_name.as_deref(),
                    rpc_interceptor,
                    resource_group_name.as_deref(),
                    resource_control,
                    ru_details,
                    replica_read_config,
                    request,
                )
                .request_source(request_source)
                .priority(priority)
                .not_fill_cache(not_fill_cache)
                .isolation_level(isolation_level)
                .task_id(task_id)
                .resource_group_tag(resource_group_tag)
                .snapshot_read_timeout(snapshot_read_timeout, SNAPSHOT_READ_TIMEOUT_MEDIUM)
                .validate_read_timestamp(
                    read_timestamp_validator,
                    timestamp.version(),
                    stale_read,
                    read_replica_scope,
                )
                .resolve_lock_for_read(
                    timestamp,
                    retry_options.lock_backoff,
                    keyspace,
                    read_lock_context,
                    lock_resolver_context,
                    snapshot_runtime_stats.clone(),
                    snapshot_variables.clone(),
                )
                .retry_multi_region_with_snapshot_stats(
                    retry_options.region_backoff,
                    snapshot_runtime_stats.clone(),
                    snapshot_variables,
                )
                .observe_snapshot_regions(internal)
                .merge(Collect)
                .plan();
                let pairs = plan.execute().await?;
                if let Some(validator) = snapshot_visibility_validator {
                    validator.check_visibility(snapshot_version).await?;
                }
                Ok(pairs
                    .into_iter()
                    .map(|pair| pair.encode_keyspace(keyspace, KeyMode::Txn))
                    .collect())
            })
            .await
            .map(move |pairs| pairs.map(move |pair| pair.truncate_keyspace(keyspace)))
    }

    /// Batch-read snapshot entries with source `GetOption` behavior. Missing
    /// keys are omitted and every returned [`ValueEntry`] retains its commit
    /// timestamp only when [`GetOption::ReturnCommitTs`] is requested.
    pub async fn batch_get_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        options: &[GetOption],
    ) -> Result<BTreeMap<Key, ValueEntry>> {
        debug!("invoking transactional batch_get request with options");
        self.check_allow_operation().await?;
        let mut get_options = GetOptions::default();
        get_options.apply(options);
        let return_commit_ts = get_options.return_commit_ts();
        let timestamp = self.timestamp.clone();
        let snapshot_version = timestamp.version();
        let cache_snapshot_read = !self.options.read_only || timestamp.version() != u64::MAX;
        let rpc = self.rpc.clone();
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let rpc_interceptor = snapshot_runtime_interceptor(
            self.rpc_interceptor.clone(),
            self.snapshot_runtime_stats.clone(),
        );
        let snapshot_runtime_stats = self.snapshot_runtime_stats.clone();
        let snapshot_variables = self.snapshot_variables.clone();
        let resource_group_name = self.resource_group_name.clone();
        let resource_control = self.resource_control.clone();
        let ru_details = self.ru_details.clone();
        let keys = keys
            .into_iter()
            .map(move |key| key.into().encode_keyspace(keyspace, KeyMode::Txn));
        let retry_options = self.options.retry_options.clone();
        let priority = self.options.priority;
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let snapshot_read_timeout = self.snapshot_read_timeout;
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let read_replica_scope = self.read_replica_scope.clone();
        let replica_read_config = self.replica_read_config.clone();
        let replica_read_adjuster = self.replica_read_adjuster.clone();
        let read_lock_context = self.read_lock_context.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let request_source = self.commit_settings.request_source.context_value();
        let internal = self.commit_settings.request_source.is_internal();

        self.buffer
            .batch_get_snapshot_entries_or_else(
                keys,
                return_commit_ts,
                cache_snapshot_read,
                move |keys| async move {
                    let _timer = crate::stats::snapshot_command_timer("batch_get", internal);
                    let keys = keys.collect::<Vec<_>>();
                    let replica_read_config = adjusted_replica_read_config(
                        &replica_read_config,
                        replica_read_adjuster.as_ref(),
                        keys.len(),
                    );
                    let stale_read = replica_read_config.stale_read;
                    let mut request = new_batch_get_request(keys.into_iter(), timestamp.clone());
                    request.need_commit_ts = return_commit_ts;
                    let resource_group_tag = resource_group_tag.clone().or_else(|| {
                        resource_group_tagger
                            .as_ref()
                            .map(|tagger| tagger(SnapshotRequestType::BatchGet))
                    });
                    let plan = plan_with_keyspace_name(
                        rpc,
                        keyspace,
                        keyspace_name.as_deref(),
                        rpc_interceptor,
                        resource_group_name.as_deref(),
                        resource_control,
                        ru_details,
                        replica_read_config,
                        request,
                    )
                    .request_source(request_source)
                    .priority(priority)
                    .not_fill_cache(not_fill_cache)
                    .isolation_level(isolation_level)
                    .task_id(task_id)
                    .resource_group_tag(resource_group_tag)
                    .snapshot_read_timeout(snapshot_read_timeout, SNAPSHOT_READ_TIMEOUT_MEDIUM)
                    .validate_read_timestamp(
                        read_timestamp_validator,
                        timestamp.version(),
                        stale_read,
                        read_replica_scope,
                    )
                    .resolve_lock_for_read(
                        timestamp,
                        retry_options.lock_backoff,
                        keyspace,
                        read_lock_context,
                        lock_resolver_context,
                        snapshot_runtime_stats.clone(),
                        snapshot_variables.clone(),
                    )
                    .retry_multi_region_with_snapshot_stats(
                        retry_options.region_backoff,
                        snapshot_runtime_stats.clone(),
                        snapshot_variables,
                    )
                    .observe_snapshot_regions(internal)
                    .plan();
                    let responses = plan.execute().await?;
                    if let Some(validator) = snapshot_visibility_validator {
                        validator.check_visibility(snapshot_version).await?;
                    }
                    let responses = responses.into_iter().collect::<Result<Vec<_>>>()?;
                    let entries: BTreeMap<_, _> = responses
                        .into_iter()
                        .flat_map(|response| response.pairs)
                        .map(|pair| {
                            (
                                Key::from(pair.key).encode_keyspace(keyspace, KeyMode::Txn),
                                ValueEntry::new(pair.value, pair.commit_ts),
                            )
                        })
                        .collect();
                    for entry in entries.values() {
                        ensure_snapshot_commit_ts(return_commit_ts, Some(entry))?;
                    }
                    Ok(entries)
                },
            )
            .await
            .map(move |entries| {
                entries
                    .into_iter()
                    .map(|(key, entry)| (key.truncate_keyspace(keyspace), entry))
                    .collect()
            })
    }

    /// Read values from the pipelined transaction buffer tier.
    ///
    /// This is the native counterpart of client-go
    /// `KVSnapshot.BatchGetWithTier(BatchGetBufferTier)`. The tier is only
    /// available after [`Snapshot::set_pipelined`](super::Snapshot::set_pipelined).
    pub(crate) async fn batch_get_from_buffer(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        let _timer = crate::stats::snapshot_command_timer(
            "batch_get",
            self.commit_settings.request_source.is_internal(),
        );
        if !self.snapshot_pipelined {
            return Err(Error::StringError(
                "only snapshot with pipelined dml can read from buffer".to_owned(),
            ));
        }
        self.check_allow_operation().await?;
        let timestamp = self.timestamp.clone();
        let snapshot_version = timestamp.version();
        let rpc = self.rpc.clone();
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let rpc_interceptor = snapshot_runtime_interceptor(
            self.rpc_interceptor.clone(),
            self.snapshot_runtime_stats.clone(),
        );
        let snapshot_runtime_stats = self.snapshot_runtime_stats.clone();
        let snapshot_variables = self.snapshot_variables.clone();
        let resource_group_name = self.resource_group_name.clone();
        let resource_control = self.resource_control.clone();
        let ru_details = self.ru_details.clone();
        let keys = keys
            .into_iter()
            .map(move |key| key.into().encode_keyspace(keyspace, KeyMode::Txn))
            .collect::<Vec<_>>();
        let retry_options = self.options.retry_options.clone();
        let priority = self.options.priority;
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let snapshot_read_timeout = self.snapshot_read_timeout;
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let read_replica_scope = self.read_replica_scope.clone();
        let replica_read_config = adjusted_replica_read_config(
            &self.replica_read_config,
            self.replica_read_adjuster.as_ref(),
            keys.len(),
        );
        let stale_read = replica_read_config.stale_read;
        let read_lock_context = self.read_lock_context.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let request_source = self.commit_settings.request_source.context_value();
        let internal = self.commit_settings.request_source.is_internal();
        let request = new_buffer_batch_get_request(keys.into_iter(), timestamp.clone());
        let resource_group_tag = resource_group_tag.or_else(|| {
            resource_group_tagger
                .as_ref()
                .map(|tagger| tagger(SnapshotRequestType::BufferBatchGet))
        });
        let plan = plan_with_keyspace_name(
            rpc,
            keyspace,
            keyspace_name.as_deref(),
            rpc_interceptor,
            resource_group_name.as_deref(),
            resource_control,
            ru_details,
            replica_read_config,
            request,
        )
        .request_source(request_source)
        .priority(priority)
        .not_fill_cache(not_fill_cache)
        .isolation_level(isolation_level)
        .task_id(task_id)
        .resource_group_tag(resource_group_tag)
        .snapshot_read_timeout(snapshot_read_timeout, SNAPSHOT_READ_TIMEOUT_MEDIUM)
        .validate_read_timestamp(
            read_timestamp_validator,
            timestamp.version(),
            stale_read,
            read_replica_scope,
        )
        .resolve_lock_for_read(
            timestamp,
            retry_options.lock_backoff,
            keyspace,
            read_lock_context,
            lock_resolver_context,
            snapshot_runtime_stats.clone(),
            snapshot_variables.clone(),
        )
        .retry_multi_region_with_snapshot_stats(
            retry_options.region_backoff,
            snapshot_runtime_stats.clone(),
            snapshot_variables,
        )
        .observe_snapshot_regions(internal)
        .merge(Collect)
        .plan();
        let pairs = plan.execute().await?;
        if let Some(validator) = snapshot_visibility_validator {
            validator.check_visibility(snapshot_version).await?;
        }
        Ok(pairs
            .into_iter()
            .map(|pair| pair.encode_keyspace(keyspace, KeyMode::Txn))
            .collect::<Vec<_>>()
            .into_iter()
            .map(move |pair| pair.truncate_keyspace(keyspace)))
    }

    /// Create a new 'batch get for update' request.
    ///
    /// Similar to [`get_for_update`](Transaction::get_for_update), but it works
    /// for a batch of keys.
    ///
    /// Non-existent entries will not appear in the result. The order of the
    /// keys is not retained in the result.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, TransactionClient, KvPair};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_pessimistic().await.unwrap();
    /// let keys = vec!["foo".to_owned(), "bar".to_owned()];
    /// let result: Vec<KvPair> = txn
    ///     .batch_get_for_update(keys)
    ///     .await
    ///     .unwrap();
    /// // now "foo" and "bar" are both locked
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn batch_get_for_update(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>> {
        debug!("invoking transactional batch_get_for_update request");
        self.check_allow_operation().await?;
        if !self.is_pessimistic() {
            let keys: Vec<Key> = keys.into_iter().map(|k| k.into()).collect();
            self.lock_keys(keys.clone()).await?;
            Ok(self.batch_get(keys).await?.collect())
        } else {
            let keyspace = self.keyspace;
            let keys = keys
                .into_iter()
                .map(move |k| k.into().encode_keyspace(keyspace, KeyMode::Txn));
            let pairs = self
                .pessimistic_lock(keys, true)
                .await?
                .truncate_keyspace(keyspace);
            Ok(pairs)
        }
    }

    /// Create a new 'scan' request.
    ///
    /// Once resolved this request will result in a `Vec` of all key-value pairs that lie in the
    /// specified range.
    ///
    /// If the number of eligible key-value pairs are greater than `limit`,
    /// only the first `limit` pairs are returned, ordered by key.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, KvPair, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key1: Key = b"foo".to_vec().into();
    /// let key2: Key = b"bar".to_vec().into();
    /// let result: Vec<KvPair> = txn
    ///     .scan(key1..key2, 10)
    ///     .await
    ///     .unwrap()
    ///     .collect();
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn scan(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking transactional scan request");
        self.scan_inner(range, limit, false, false).await
    }

    /// Create a new 'scan' request that only returns the keys.
    ///
    /// Once resolved this request will result in a `Vec` of keys that lies in the specified range.
    ///
    /// If the number of eligible keys are greater than `limit`,
    /// only the first `limit` keys are returned, ordered by key.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, KvPair, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # use std::collections::HashMap;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key1: Key = b"foo".to_vec().into();
    /// let key2: Key = b"bar".to_vec().into();
    /// let result: Vec<Key> = txn
    ///     .scan_keys(key1..key2, 10)
    ///     .await
    ///     .unwrap()
    ///     .collect();
    /// // Finish the transaction...
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn scan_keys(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking transactional scan_keys request");
        Ok(self
            .scan_inner(range, limit, true, false)
            .await?
            .map(KvPair::into_key))
    }

    /// Create a 'scan_reverse' request.
    ///
    /// Similar to [`scan`](Transaction::scan), but scans in the reverse direction.
    pub async fn scan_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>> {
        debug!("invoking transactional scan_reverse request");
        self.scan_inner(range, limit, false, true).await
    }

    /// Create a 'scan_keys_reverse' request.
    ///
    /// Similar to [`scan`](Transaction::scan_keys), but scans in the reverse direction.
    pub async fn scan_keys_reverse(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = Key>> {
        debug!("invoking transactional scan_keys_reverse request");
        Ok(self
            .scan_inner(range, limit, true, true)
            .await?
            .map(KvPair::into_key))
    }

    /// Sets the value associated with the given key.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key = "foo".to_owned();
    /// let val = "FOO".to_owned();
    /// txn.put(key, val);
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn put(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        self.put_inner(key, value, None).await
    }

    /// Sets a value and atomically attaches source-compatible mutation flags.
    pub async fn put_with_options(
        &mut self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        options: MutationOptions,
    ) -> Result<()> {
        self.put_inner(key, value, Some(options)).await
    }

    async fn put_inner(
        &mut self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        options: Option<MutationOptions>,
    ) -> Result<()> {
        trace!("invoking transactional put request");
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let value = value.into();
        if value.is_empty() {
            return Err(crate::error::StaticError::CannotSetNilValue.into());
        }
        if self.is_pessimistic() {
            self.pessimistic_lock(
                iter::once(key.clone()),
                options.is_some_and(MutationOptions::checks_existence),
            )
            .await?;
        }
        self.buffer.put(key.clone(), value);
        if let Some(options) = options {
            self.buffer.set_mutation_options(&key, options)?;
        }
        self.notify_memory_footprint_change();
        self.maybe_flush_pipelined(false).await?;
        Ok(())
    }

    /// Inserts the value associated with the given key.
    ///
    /// Similar to [`Self::put`], but it has an additional constraint that the key should not exist
    /// before this operation.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Value, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key = "foo".to_owned();
    /// let val = "FOO".to_owned();
    /// txn.insert(key, val);
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn insert(&mut self, key: impl Into<Key>, value: impl Into<Value>) -> Result<()> {
        self.insert_inner(key, value, None).await
    }

    /// Inserts a value and atomically attaches source-compatible mutation flags.
    pub async fn insert_with_options(
        &mut self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        options: MutationOptions,
    ) -> Result<()> {
        self.insert_inner(key, value, Some(options)).await
    }

    async fn insert_inner(
        &mut self,
        key: impl Into<Key>,
        value: impl Into<Value>,
        options: Option<MutationOptions>,
    ) -> Result<()> {
        debug!("invoking transactional insert request");
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let value = value.into();
        if value.is_empty() {
            return Err(crate::error::StaticError::CannotSetNilValue.into());
        }
        if self.buffer.get(&key).is_some() {
            return Err(Error::DuplicateKeyInsertion);
        }
        if self.is_pessimistic() {
            self.pessimistic_lock(
                iter::once((key.clone(), kvrpcpb::Assertion::NotExist)),
                false,
            )
            .await?;
        }
        self.buffer.insert(key.clone(), value);
        if let Some(options) = options {
            self.buffer.set_mutation_options(&key, options)?;
        }
        self.notify_memory_footprint_change();
        self.maybe_flush_pipelined(false).await?;
        Ok(())
    }

    /// Deletes the given key and its value from the database.
    ///
    /// Deleting a non-existent key will not result in an error.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let key = "foo".to_owned();
    /// txn.delete(key);
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn delete(&mut self, key: impl Into<Key>) -> Result<()> {
        self.delete_inner(key, None).await
    }

    /// Deletes a key and atomically attaches source-compatible mutation flags.
    pub async fn delete_with_options(
        &mut self,
        key: impl Into<Key>,
        options: MutationOptions,
    ) -> Result<()> {
        self.delete_inner(key, Some(options)).await
    }

    async fn delete_inner(
        &mut self,
        key: impl Into<Key>,
        options: Option<MutationOptions>,
    ) -> Result<()> {
        debug!("invoking transactional delete request");
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        if self.is_pessimistic() {
            self.pessimistic_lock(
                iter::once(key.clone()),
                options.is_some_and(MutationOptions::checks_existence),
            )
            .await?;
        }
        self.buffer.delete(key.clone());
        if let Some(options) = options {
            self.buffer.set_mutation_options(&key, options)?;
        }
        self.notify_memory_footprint_change();
        self.maybe_flush_pipelined(false).await?;
        Ok(())
    }

    /// Replaces the source-compatible flags of an existing buffered mutation.
    pub async fn set_mutation_options(
        &mut self,
        key: impl Into<Key>,
        options: MutationOptions,
    ) -> Result<()> {
        self.check_allow_operation().await?;
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        self.buffer.set_mutation_options(&key, options)
    }

    /// Batch mutate the database.
    ///
    /// Only `Put` and `Delete` are supported.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Key, Config, TransactionClient, transaction::Mutation};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100", "192.168.0.101"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// let mutations = vec![
    ///     Mutation::Delete("k0".to_owned().into()),
    ///     Mutation::Put("k1".to_owned().into(), b"v1".to_vec()),
    /// ];
    /// txn.batch_mutate(mutations).await.unwrap();
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn batch_mutate(
        &mut self,
        mutations: impl IntoIterator<Item = Mutation>,
    ) -> Result<()> {
        debug!("invoking transactional batch mutate request");
        self.check_allow_operation().await?;
        let mutations: Vec<Mutation> = mutations
            .into_iter()
            .map(|mutation| mutation.encode_keyspace(self.keyspace, KeyMode::Txn))
            .collect();
        if mutations
            .iter()
            .any(|mutation| matches!(mutation, Mutation::Put(_, value) if value.is_empty()))
        {
            return Err(crate::error::StaticError::CannotSetNilValue.into());
        }
        if self.is_pessimistic() {
            self.pessimistic_lock(mutations.iter().map(|m| m.key().clone()), false)
                .await?;
            for m in mutations {
                self.buffer.mutate(m);
            }
        } else {
            for m in mutations.into_iter() {
                self.buffer.mutate(m);
            }
        }
        self.notify_memory_footprint_change();
        self.maybe_flush_pipelined(false).await?;
        Ok(())
    }

    /// Lock the given keys without mutating their values.
    ///
    /// In optimistic mode, write conflicts are not checked until commit.
    /// So use this command to indicate that
    /// "I do not want to commit if the value associated with this key has been modified".
    /// It's useful to avoid the *write skew* anomaly.
    ///
    /// In pessimistic mode, it is similar to [`batch_get_for_update`](Transaction::batch_get_for_update),
    /// except that it does not read values.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// txn.lock_keys(vec!["TiKV".to_owned(), "Rust".to_owned()]);
    /// // ... Do some actions.
    /// txn.commit().await.unwrap();
    /// # });
    /// ```
    pub async fn lock_keys(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<()> {
        self.lock_keys_with_wait_time(0, keys).await
    }

    pub async fn lock_keys_with_wait_time(
        &mut self,
        lock_wait_time_ms: i64,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<()> {
        let for_update_timestamp = if self.is_pessimistic() {
            self.rpc.clone().get_timestamp().await?.version()
        } else {
            self.timestamp.version()
        };
        let mut context =
            LockContext::new(for_update_timestamp, lock_wait_time_ms, SystemTime::now());
        self.lock_keys_with_context(&mut context, keys).await
    }

    /// Lock keys with the complete client-go pessimistic-lock context.
    pub async fn lock_keys_with_context(
        &mut self,
        context: &mut LockContext,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<()> {
        self.lock_keys_with_context_and_callback(context, keys, || {})
            .await
    }

    /// Lock keys and invoke `callback` after the lock attempt, including an
    /// attempt that returns an error.
    pub async fn lock_keys_with_context_and_callback(
        &mut self,
        context: &mut LockContext,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        callback: impl FnOnce(),
    ) -> Result<()> {
        let result = self
            .lock_keys_with_context_inner(context, keys.into_iter().map(Into::into).collect())
            .await;
        callback();
        result
    }

    async fn lock_keys_with_context_inner(
        &mut self,
        context: &mut LockContext,
        keys: Vec<Key>,
    ) -> Result<()> {
        debug!("invoking transactional lock_keys request");
        self.check_allow_operation().await?;
        let keyspace = self.keyspace;
        let mut keys: Vec<Key> = keys
            .into_iter()
            .map(move |key| key.encode_keyspace(keyspace, KeyMode::Txn))
            .collect();
        keys.sort_unstable();
        keys.dedup();

        if context.in_share_mode && self.is_pipelined() {
            return Err(Error::StringError(
                "shared lock is not supported in pipelined transaction".to_owned(),
            ));
        }
        if context.in_share_mode && self.aggressive_locking.is_some() {
            return Err(Error::StringError(
                "shared lock is not supported in aggressive/fair locking mode".to_owned(),
            ));
        }
        if self.aggressive_locking.is_some() && !self.is_pessimistic() {
            return Err(Error::StringError(
                "trying to perform aggressive locking in optimistic transaction".to_owned(),
            ));
        }

        let mut pending = Vec::with_capacity(keys.len());
        for key in keys {
            if self.buffer.is_shared_locked(&key) && !context.in_share_mode {
                return Err(Error::StringError(
                    "upgrading a shared lock to an exclusive lock is not supported".to_owned(),
                ));
            }
            if self.buffer.is_locked(&key) {
                if context.return_values {
                    let logical_key = key.clone().truncate_keyspace(self.keyspace);
                    context.insert_returned_value(
                        <&[u8]>::from(&logical_key).to_vec(),
                        ReturnedValue {
                            already_locked: true,
                            ..Default::default()
                        },
                    );
                }
            } else {
                pending.push(key);
            }
        }
        if pending.is_empty() {
            return Ok(());
        }

        if context.lock_only_if_exists {
            if !context.return_values {
                return Err(crate::error::LockOnlyIfExistsNoReturnValueError {
                    start_timestamp: self.timestamp.version(),
                    for_update_timestamp: context.for_update_ts,
                    lock_key: <&[u8]>::from(&pending[0]).to_vec(),
                }
                .into());
            }
            if self.buffer.get_primary_key().is_none() && pending.len() > 1 {
                return Err(crate::error::LockOnlyIfExistsNoPrimaryKeyError {
                    start_timestamp: self.timestamp.version(),
                    for_update_timestamp: context.for_update_ts,
                    lock_key: <&[u8]>::from(&pending[0]).to_vec(),
                }
                .into());
            }
        }

        if !self.is_pessimistic() || context.for_update_ts == 0 {
            for key in pending {
                self.buffer.lock(key);
            }
            self.notify_memory_footprint_change();
            self.maybe_flush_pipelined(false).await?;
            return Ok(());
        }

        if context.in_share_mode && self.buffer.get_primary_key().is_none() {
            return Err(Error::StringError(
                "pessimistic lock in share mode requires primary key to be selected".to_owned(),
            ));
        }
        if self.aggressive_locking.is_some() && pending.len() == 1 {
            let key = pending[0].clone();
            let reused = self
                .aggressive_locking
                .as_mut()
                .and_then(|aggressive| aggressive.previous.remove(&key));
            if let Some(entry) = reused {
                if context.for_update_ts < entry.value.locked_with_conflict_ts {
                    return Err(Error::StringError(format!(
                        "transaction {} retries aggressive locking with for-update timestamp {} below prior conflict timestamp {}",
                        self.timestamp.version(),
                        context.for_update_ts,
                        entry.value.locked_with_conflict_ts,
                    )));
                }
                if (!context.return_values || entry.has_return_value)
                    && (!context.check_existence || entry.has_check_existence)
                {
                    if context.return_values || context.check_existence {
                        let logical_key = key.clone().truncate_keyspace(self.keyspace);
                        context.insert_returned_value(
                            <&[u8]>::from(&logical_key).to_vec(),
                            entry.value.clone(),
                        );
                    }
                    self.aggressive_locking
                        .as_mut()
                        .expect("aggressive locking checked above")
                        .current
                        .insert(key, entry);
                    return Ok(());
                }
            }
        }
        if self.aggressive_locking.is_some() && pending.len() > 1 {
            self.done_aggressive_locking().await?;
        }
        let wake_up_mode = if self.aggressive_locking.is_some() && pending.len() == 1 {
            kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock
        } else {
            kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal
        };
        let lock_type = if context.in_share_mode {
            kvrpcpb::Op::SharedPessimisticLock
        } else {
            kvrpcpb::Op::PessimisticLock
        };
        self.pessimistic_lock_with_context_options(pending, lock_type, wake_up_mode, context)
            .await?;
        self.notify_memory_footprint_change();
        self.maybe_flush_pipelined(false).await?;
        Ok(())
    }

    pub fn start_aggressive_locking(&mut self) {
        assert!(
            self.aggressive_locking.is_none(),
            "trying to start aggressive locking while it is already started"
        );
        self.aggressive_locking = Some(AggressiveLockingContext::default());
    }

    pub async fn retry_aggressive_locking(&mut self) -> Result<()> {
        let mut context = self
            .aggressive_locking
            .take()
            .expect("trying to retry aggressive locking while it is not started");
        self.pessimistic_lock_count = self
            .pessimistic_lock_count
            .saturating_sub(context.previous.len());
        self.rollback_aggressive_keys(&context.previous).await?;
        context.previous = std::mem::take(&mut context.current);
        self.aggressive_locking = Some(context);
        Ok(())
    }

    pub async fn cancel_aggressive_locking(&mut self) -> Result<()> {
        let context = self
            .aggressive_locking
            .take()
            .expect("trying to cancel aggressive locking while it is not started");
        let mut keys = context.previous;
        keys.extend(context.current);
        self.pessimistic_lock_count = self.pessimistic_lock_count.saturating_sub(keys.len());
        self.rollback_aggressive_keys(&keys).await
    }

    pub async fn done_aggressive_locking(&mut self) -> Result<()> {
        let context = self
            .aggressive_locking
            .take()
            .expect("trying to finish aggressive locking while it is not started");
        self.pessimistic_lock_count = self
            .pessimistic_lock_count
            .saturating_sub(context.previous.len());
        self.rollback_aggressive_keys(&context.previous).await?;
        for (key, mut entry) in context.current {
            if !entry.has_return_value && !entry.has_check_existence {
                entry.value.exists = true;
            }
            self.buffer
                .lock_with_returned_value(key, false, Some(&entry.value))
                .expect("aggressive exclusive lock cannot violate lock-mode invariants");
        }
        self.notify_memory_footprint_change();
        Ok(())
    }

    async fn rollback_aggressive_keys(
        &mut self,
        keys: &BTreeMap<Key, AggressiveLockEntry>,
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let for_update_ts = keys
            .values()
            .map(|entry| &entry.actual_for_update_ts)
            .max_by_key(|timestamp| timestamp.version())
            .cloned()
            .expect("non-empty keys");
        self.pessimistic_lock_rollback(keys.keys().cloned(), self.timestamp.clone(), for_update_ts)
            .await
    }

    pub fn is_in_aggressive_locking_mode(&self) -> bool {
        self.aggressive_locking.is_some()
    }

    pub fn is_in_aggressive_locking_stage(&self, key: impl Into<Key>) -> bool {
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        self.aggressive_locking
            .as_ref()
            .is_some_and(|context| context.current.contains_key(&key))
    }

    /// Acquire pessimistic shared locks. Optimistic transactions retain their
    /// exclusive local lock marker because client-go does not support shared
    /// optimistic locking.
    pub async fn lock_keys_shared(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<()> {
        self.lock_keys_shared_with_wait_time(0, keys).await
    }

    pub async fn lock_keys_shared_with_wait_time(
        &mut self,
        lock_wait_time_ms: i64,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<()> {
        let for_update_timestamp = if self.is_pessimistic() {
            self.rpc.clone().get_timestamp().await?.version()
        } else {
            self.timestamp.version()
        };
        let mut context =
            LockContext::new(for_update_timestamp, lock_wait_time_ms, SystemTime::now());
        context.in_share_mode = true;
        self.lock_keys_with_context(&mut context, keys).await
    }

    /// Commits the actions of the transaction. On success, we return the commit timestamp (or
    /// `None` if there was nothing to commit).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, Timestamp, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// // ... Do some actions.
    /// let result: Timestamp = txn.commit().await.unwrap().unwrap();
    /// # });
    /// ```
    pub async fn commit(&mut self) -> Result<Option<Timestamp>> {
        debug!(
            "committing transaction, start_ts: {}",
            self.timestamp.version()
        );
        if !self.transit_status(
            |status| matches!(status, TransactionStatus::Active),
            TransactionStatus::StartedCommit,
        ) {
            return Err(crate::error::StaticError::InvalidTransaction.into());
        }
        let _close_on_error =
            TransactionAttemptGuard::new(self.status.clone(), TransactionStatus::StartedCommit);
        // Record that the commit path has been entered; prewrite may place 2PC
        // locks. A later rollback needs this even after the status has moved on
        // to `StartedRollback` (see `rollback`).
        self.prewritten = true;

        if self.aggressive_locking.is_some() {
            if self
                .aggressive_locking
                .as_ref()
                .is_some_and(|context| !context.current.is_empty())
            {
                return Err(Error::StringError(
                    "trying to commit transaction when aggressive locking is pending".to_owned(),
                ));
            }
            self.cancel_aggressive_locking().await?;
        }

        let mut mutations = Vec::new();
        let mut stashed_assertion = None;
        for mut mutation in self.buffer.to_proto_mutations() {
            if self.commit_settings.assertion_level == kvrpcpb::AssertionLevel::Off {
                mutation.assertion = kvrpcpb::Assertion::None as i32;
            }
            let operation = kvrpcpb::Op::try_from(mutation.op).unwrap_or(kvrpcpb::Op::Put);
            let filterable = matches!(
                operation,
                kvrpcpb::Op::Put
                    | kvrpcpb::Op::Insert
                    | kvrpcpb::Op::Del
                    | kvrpcpb::Op::CheckNotExists
            );
            let filtered = if filterable {
                match &self.commit_settings.kv_filter {
                    Some(filter) => filter.is_unnecessary_key_value(
                        &mutation.key,
                        &mutation.value,
                        self.buffer.mutation_flags(&Key::from(mutation.key.clone())),
                    )?,
                    None => false,
                }
            } else {
                false
            };
            if filtered {
                if self.is_pessimistic() && operation != kvrpcpb::Op::CheckNotExists {
                    mutation.op = kvrpcpb::Op::Lock as i32;
                    mutation.value.clear();
                } else {
                    continue;
                }
            }
            if self.is_pessimistic()
                && self.commit_settings.assertion_level != kvrpcpb::AssertionLevel::Off
                && stashed_assertion.is_none()
            {
                stashed_assertion = self
                    .buffer
                    .assertion_failure(&Key::from(mutation.key.clone()), self.timestamp.version());
            }
            mutations.push(mutation);
        }
        let has_shared_locks = mutations.iter().any(|mutation| {
            matches!(
                kvrpcpb::Op::try_from(mutation.op),
                Ok(kvrpcpb::Op::SharedLock | kvrpcpb::Op::SharedPessimisticLock)
            )
        });
        let primary_key = self
            .buffer
            .get_primary_key()
            .filter(|primary| {
                mutations.iter().any(|mutation| {
                    mutation.key.as_slice() == <&[u8]>::from(primary)
                        && !matches!(
                            kvrpcpb::Op::try_from(mutation.op),
                            Ok(kvrpcpb::Op::SharedLock | kvrpcpb::Op::SharedPessimisticLock)
                        )
                })
            })
            .or_else(|| {
                mutations
                    .iter()
                    .find(|mutation| {
                        !matches!(
                            kvrpcpb::Op::try_from(mutation.op),
                            Ok(kvrpcpb::Op::CheckNotExists | kvrpcpb::Op::SharedLock)
                        )
                    })
                    .map(|mutation| Key::from(mutation.key.clone()))
            })
            .or_else(|| {
                (!has_shared_locks)
                    .then(|| {
                        mutations
                            .first()
                            .map(|mutation| Key::from(mutation.key.clone()))
                    })
                    .flatten()
            });
        if mutations.is_empty() {
            assert!(primary_key.is_none());
            self.prewritten = false;
            self.commit_timestamp = None;
            self.set_status(TransactionStatus::Committed);
            return Ok(None);
        }
        if primary_key.is_none() {
            self.prewritten = false;
            if has_shared_locks {
                return Err(Error::StringError(
                    "shared lock key cannot be used as transaction primary key".to_owned(),
                ));
            }
            return Err(Error::NoPrimaryKey);
        }
        if self.timestamp.version() == u64::MAX {
            return Err(Error::StringError(format!(
                "try to commit with invalid txnStartTS: {}",
                self.timestamp.version()
            )));
        }

        self.start_auto_heartbeat().await;

        let latch = if self.is_pessimistic() {
            None
        } else if let Some(scheduler) = &self.latches {
            let keys = mutations
                .iter()
                .map(|mutation| mutation.key.clone())
                .collect();
            let latch = scheduler.lock(self.timestamp.version(), keys).await;
            if latch.is_stale() {
                return Err(crate::error::WriteConflictInLatchError {
                    start_timestamp: self.timestamp.version(),
                }
                .into());
            }
            Some(latch)
        } else {
            None
        };

        let res = Committer::new(
            primary_key,
            mutations,
            self.timestamp.clone(),
            self.rpc.clone(),
            self.options.clone(),
            self.commit_settings.clone(),
            self.keyspace,
            self.keyspace_name.clone(),
            self.rpc_interceptor.clone(),
            self.resource_group_name.clone(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            self.lock_resolver_context.clone(),
            self.pipelined_state.clone(),
            self.buffer.get_write_size() as u64,
            self.start_instant,
        )
        .with_pessimistic_lock_keys(self.buffer.pessimistic_lock_keys())
        .with_constraint_check_keys(self.buffer.constraint_check_keys())
        .with_stashed_assertion(stashed_assertion)
        .commit()
        .await;

        if let (Some(latch), Ok(Some(commit_timestamp))) = (&latch, &res) {
            latch.set_commit_timestamp(commit_timestamp.version());
        }

        if let Ok(commit_ts) = &res {
            self.commit_timestamp = commit_ts.clone();
            self.set_status(TransactionStatus::Committed);
            debug!(
                "transaction committed, start_ts: {}, commit_ts: {:?}, elapsed: {:?}",
                self.timestamp.version(),
                commit_ts.as_ref().map(|ts| ts.version()),
                self.start_instant.elapsed(),
            );
        }
        res
    }

    /// Rollback the transaction.
    ///
    /// If it succeeds, all mutations made by this transaction will be discarded.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use tikv_client::{Config, Timestamp, TransactionClient};
    /// # use futures::prelude::*;
    /// # futures::executor::block_on(async {
    /// # let client = TransactionClient::new(vec!["192.168.0.100"]).await.unwrap();
    /// let mut txn = client.begin_optimistic().await.unwrap();
    /// // ... Do some actions.
    /// txn.rollback().await.unwrap();
    /// # });
    /// ```
    pub async fn rollback(&mut self) -> Result<()> {
        debug!(
            "rolling back transaction, start_ts: {}",
            self.timestamp.version()
        );
        // A transaction that already started committing may have placed prewrite
        // (2PC) locks; use the persisted flag so the committer rolls those back
        // with `BatchRollback` rather than `PessimisticRollback` (which cannot
        // clear them). Reading it from the status would be wrong on a rollback
        // retry: the status is already `StartedRollback` by then, so the fact
        // that commit had started would be lost.
        let prewritten = self.prewritten;
        if !self.transit_status(
            |status| matches!(status, TransactionStatus::Active),
            TransactionStatus::StartedRollback,
        ) {
            return Err(crate::error::StaticError::InvalidTransaction.into());
        }
        let _close_on_error =
            TransactionAttemptGuard::new(self.status.clone(), TransactionStatus::StartedRollback);
        if self
            .aggressive_locking
            .as_ref()
            .is_some_and(|context| !context.current.is_empty())
        {
            return Err(Error::StringError(
                "trying to rollback transaction when aggressive locking is pending".to_owned(),
            ));
        }
        if self.aggressive_locking.is_some() {
            if let Err(error) = self.cancel_aggressive_locking().await {
                warn!(
                    "failed to clean up aggressive pessimistic locks during rollback, start_ts: {}, error: {}",
                    self.timestamp.version(),
                    error
                );
            }
        }

        let primary_key = self.buffer.get_primary_key();
        let mutations = self.buffer.to_proto_mutations();
        let committer = Committer::new(
            primary_key,
            mutations,
            self.timestamp.clone(),
            self.rpc.clone(),
            self.options.clone(),
            self.commit_settings.clone(),
            self.keyspace,
            self.keyspace_name.clone(),
            self.rpc_interceptor.clone(),
            self.resource_group_name.clone(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            self.lock_resolver_context.clone(),
            self.pipelined_state.clone(),
            self.buffer.get_write_size() as u64,
            self.start_instant,
        );
        if self.is_pipelined() {
            let hooks = self.commit_settings.lifecycle_hooks.clone();
            let start_timestamp = self.timestamp.version();
            tokio::spawn(async move {
                if let Some(pre) = hooks.pre {
                    pre();
                }
                if let Err(error) = committer.rollback(prewritten).await {
                    warn!(
                        "failed to clean up pipelined transaction during rollback, start_ts: {}, error: {}",
                        start_timestamp, error
                    );
                }
                if let Some(post) = hooks.post {
                    post();
                }
            });
        } else if let Err(error) = committer.rollback(prewritten).await {
            warn!(
                "failed to clean up transaction during rollback, start_ts: {}, error: {}",
                self.timestamp.version(),
                error
            );
        }
        self.set_status(TransactionStatus::Rolledback);
        debug!(
            "transaction rolled back, start_ts: {}",
            self.timestamp.version()
        );
        Ok(())
    }

    /// Get the start timestamp of this transaction.
    pub fn start_timestamp(&self) -> Timestamp {
        self.timestamp.clone()
    }

    /// Set the priority for subsequent read and write requests.
    pub fn set_priority(&mut self, priority: Priority) {
        self.options.priority = priority;
    }

    pub fn set_variables(&mut self, variables: Arc<Variables>) {
        self.commit_settings.variables = variables.clone();
        self.snapshot_variables = variables;
    }

    pub fn variables(&self) -> &Arc<Variables> {
        &self.commit_settings.variables
    }

    pub fn enable_force_sync_log(&mut self) {
        self.commit_settings.force_sync_log = true;
    }

    pub fn set_enable_async_commit(&mut self, enabled: bool) {
        self.options.async_commit = enabled;
    }

    pub fn set_enable_one_pc(&mut self, enabled: bool) {
        self.options.try_one_pc = enabled;
    }

    pub fn set_pessimistic(&mut self, pessimistic: bool) {
        if self.is_pipelined() {
            panic!("can not set a txn with pipelined memdb to pessimistic mode");
        }
        self.buffer.set_pessimistic(pessimistic);
        self.options.kind = if pessimistic {
            TransactionKind::Pessimistic(Timestamp::from_version(0))
        } else {
            TransactionKind::Optimistic
        };
    }

    pub fn set_causal_consistency(&mut self, enabled: bool) {
        self.commit_settings.causal_consistency = enabled;
    }

    pub fn causal_consistency(&self) -> bool {
        self.commit_settings.causal_consistency
    }

    pub fn set_scope(&mut self, scope: impl Into<String>) {
        self.commit_settings.scope = scope.into();
    }

    pub fn scope(&self) -> &str {
        &self.commit_settings.scope
    }

    pub fn set_disk_full_option(&mut self, option: kvrpcpb::DiskFullOpt) {
        self.commit_settings.disk_full_option = option;
    }

    pub fn disk_full_option(&self) -> kvrpcpb::DiskFullOpt {
        self.commit_settings.disk_full_option
    }

    pub fn clear_disk_full_option(&mut self) {
        self.commit_settings.disk_full_option = kvrpcpb::DiskFullOpt::NotAllowedOnFull;
    }

    pub fn set_transaction_source(&mut self, source: u64) {
        self.commit_settings.transaction_source = source;
    }

    pub fn set_session_id(&mut self, session_id: u64) {
        self.commit_settings.session_id = session_id;
    }

    pub fn set_assertion_level(&mut self, level: kvrpcpb::AssertionLevel) {
        self.commit_settings.assertion_level = level;
    }

    pub fn set_prewrite_encounter_lock_policy(&mut self, policy: PrewriteEncounterLockPolicy) {
        self.commit_settings.prewrite_lock_policy = policy;
    }

    pub fn disable_txn_file(&mut self) {
        self.commit_settings.txn_file_disabled = true;
    }

    pub fn set_binlog_executor(&mut self, executor: Arc<dyn BinlogExecutor>) {
        self.commit_settings.binlog = Some(executor);
    }

    pub fn set_commit_timestamp_upper_bound_check(
        &mut self,
        check: impl Fn(u64) -> bool + Send + Sync + 'static,
    ) {
        self.commit_settings.commit_timestamp_upper_bound = Some(Arc::new(check));
    }

    pub fn set_commit_callback(
        &mut self,
        callback: impl Fn(String, Option<String>) + Send + Sync + 'static,
    ) {
        self.commit_settings.commit_callback = Some(Arc::new(callback));
    }

    pub fn set_background_task_lifecycle_hooks(&mut self, hooks: LifecycleHooks) {
        self.commit_settings.lifecycle_hooks = hooks;
    }

    pub fn set_schema_version(&mut self, version: Arc<dyn SchemaVersion>) {
        self.commit_settings.schema_version = Some(version);
    }

    pub fn set_schema_lease_checker(&mut self, checker: Arc<dyn SchemaLeaseChecker>) {
        self.commit_settings.schema_lease_checker = Some(checker);
    }

    pub fn set_kv_filter(&mut self, filter: Arc<dyn KvFilter>) {
        self.commit_settings.kv_filter = Some(filter);
    }

    pub fn set_memory_footprint_change_hook(&mut self, hook: impl Fn(u64) + Send + Sync + 'static) {
        self.memory_footprint_hook = Some(Arc::new(hook));
    }

    pub fn memory_footprint(&self) -> u64 {
        self.buffer.memory_footprint()
    }

    pub fn memory_hook_set(&self) -> bool {
        self.memory_footprint_hook.is_some()
    }

    pub fn set_request_source_internal(&mut self, internal: bool) {
        self.commit_settings.request_source.internal = internal;
    }

    pub fn set_request_source_type(&mut self, source_type: impl Into<String>) {
        self.commit_settings.request_source.source_type = source_type.into();
    }

    pub fn set_explicit_request_source_type(&mut self, source_type: impl Into<String>) {
        self.commit_settings.request_source.explicit_source_type = source_type.into();
    }

    pub fn request_source(&self) -> &RequestSource {
        &self.commit_settings.request_source
    }

    pub fn set_commit_wait_until_tso(&mut self, timestamp: u64) {
        self.commit_settings.commit_wait_until_tso =
            self.commit_settings.commit_wait_until_tso.max(timestamp);
    }

    pub fn commit_wait_until_tso(&self) -> u64 {
        self.commit_settings.commit_wait_until_tso
    }

    pub fn set_commit_wait_until_tso_timeout(&mut self, timeout: Duration) {
        self.commit_settings.commit_wait_until_tso_timeout = timeout;
    }

    pub fn commit_wait_until_tso_timeout(&self) -> Duration {
        self.commit_settings.commit_wait_until_tso_timeout
    }

    pub fn is_pipelined(&self) -> bool {
        self.commit_settings.pipelined.enable
    }

    pub fn is_read_only(&self) -> bool {
        self.buffer.len() == 0 && !self.aggressive_locking_dirty
    }

    pub fn is_valid(&self) -> bool {
        matches!(
            self.get_status(),
            TransactionStatus::Active | TransactionStatus::ReadOnly
        )
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.len() == 0
    }

    pub fn size(&self) -> usize {
        self.buffer.get_write_size()
    }

    pub fn commit_timestamp(&self) -> Option<Timestamp> {
        self.commit_timestamp.clone()
    }

    pub async fn cluster_id(&self) -> u64 {
        self.rpc.cluster_id().await
    }

    /// Send a heart beat message to keep the transaction alive on the server and update its TTL.
    ///
    /// Returns the TTL set on the transaction's locks by TiKV.
    #[doc(hidden)]
    pub async fn send_heart_beat(&mut self) -> Result<u64> {
        debug!("sending heartbeat, start_ts: {}", self.timestamp.version());
        self.check_allow_operation().await?;
        let primary_key = match self.buffer.get_primary_key() {
            Some(k) => k,
            None => return Err(Error::NoPrimaryKey),
        };
        let mut request = new_heart_beat_request(
            self.timestamp.clone(),
            primary_key,
            self.start_instant.elapsed().as_millis() as u64 + MAX_TTL,
        );
        self.commit_settings
            .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
        let plan = self
            .plan(request)
            .resolve_lock_with_context(
                self.timestamp.clone(),
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
                self.lock_resolver_context.clone(),
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .extract_error()
            .merge(CollectSingle)
            .post_process_default()
            .plan();
        plan.execute().await
    }

    /// Fetch exactly one source scanner region batch, retrying response-level
    /// locks in place and recovering pair-level locks with point reads.
    pub(crate) async fn scan_iterator_batch(
        &mut self,
        range: BoundRange,
        batch_size: u32,
        reverse: bool,
    ) -> Result<SnapshotScannerBatch> {
        self.check_allow_operation().await?;
        let timestamp = self.timestamp.clone();
        let snapshot_version = timestamp.version();
        let rpc = self.rpc.clone();
        let retry_options = self.options.retry_options.clone();
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let scan_rpc_interceptor = snapshot_runtime_interceptor(self.rpc_interceptor.clone(), None);
        let get_rpc_interceptor = snapshot_runtime_interceptor(
            self.rpc_interceptor.clone(),
            self.snapshot_runtime_stats.clone(),
        );
        let snapshot_runtime_stats = self.snapshot_runtime_stats.clone();
        let snapshot_variables = self.snapshot_variables.clone();
        let scanner_retry_owner =
            crate::request::plan::new_snapshot_retry_owner(Arc::clone(&snapshot_variables));
        let resource_group_name = self.resource_group_name.clone();
        let resource_control = self.resource_control.clone();
        let ru_details = self.ru_details.clone();
        let priority = self.options.priority;
        let sample_step = self.sample_step;
        let key_only = self.snapshot_key_only;
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let replica_read_config = self.replica_read_config.clone();
        let get_replica_read_config = self.replica_read_config_for_items(1);
        let snapshot_read_timeout = self.snapshot_read_timeout;
        let read_replica_scope = self.read_replica_scope.clone();
        let read_lock_context = self.read_lock_context.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let request_source = self.commit_settings.request_source.context_value();
        let max_timestamp_point_get = timestamp.version() == u64::MAX;
        let mut logical_range = range;
        let mut encoded_range = logical_range
            .clone()
            .encode_keyspace(keyspace, KeyMode::Txn);
        let (scan_start, scan_end) = encoded_range.clone().into_keys();

        loop {
            let request = new_scan_request(
                encoded_range.clone(),
                timestamp.clone(),
                batch_size,
                key_only,
                reverse,
                sample_step,
            );
            let scan_resource_group_tag = resource_group_tag.clone().or_else(|| {
                resource_group_tagger
                    .as_ref()
                    .map(|tagger| tagger(SnapshotRequestType::Scan))
            });
            let plan = plan_with_keyspace_name(
                rpc.clone(),
                keyspace,
                keyspace_name.as_deref(),
                scan_rpc_interceptor.clone(),
                resource_group_name.as_deref(),
                resource_control.clone(),
                ru_details.clone(),
                replica_read_config.clone(),
                request,
            )
            .request_source(request_source.clone())
            .priority(priority)
            .not_fill_cache(not_fill_cache)
            .isolation_level(isolation_level)
            .task_id(task_id)
            .resource_group_tag(scan_resource_group_tag)
            .snapshot_read_timeout(None, SNAPSHOT_READ_TIMEOUT_MEDIUM)
            .validate_read_timestamp(
                read_timestamp_validator.clone(),
                timestamp.version(),
                replica_read_config.stale_read,
                String::new(),
            )
            .resolve_response_lock_for_read(
                timestamp.clone(),
                retry_options.lock_backoff.clone(),
                keyspace,
                read_lock_context.clone(),
                lock_resolver_context.clone(),
                snapshot_runtime_stats.clone(),
                snapshot_variables.clone(),
            )
            .without_snapshot_lock_backoff_stats()
            .process(PreserveScannerPairErrors)
            .preserve_shard()
            .retry_multi_region_with_snapshot_stats(
                retry_options.region_backoff.clone(),
                None,
                snapshot_variables.clone(),
            )
            .snapshot_retry_owner(Arc::clone(&scanner_retry_owner))
            .one_region(reverse)
            .merge(CollectScannerRegionBatch)
            .plan();
            let region_batch = plan.execute().await?;
            if let Some(validator) = &snapshot_visibility_validator {
                validator.check_visibility(snapshot_version).await?;
            }
            let raw_batch_len = region_batch.pairs.len();
            let raw_last_key = region_batch.pairs.last().map(|pair| -> Result<Key> {
                if pair.key.is_empty() {
                    let error = pair.error.as_ref().ok_or_else(|| {
                        Error::StringError("scan pair has neither a key nor a key error".to_owned())
                    })?;
                    Ok(extract_lock_from_key_error(error)?.key.into())
                } else {
                    Ok(pair.key.clone().into())
                }
            });

            let mut pairs = Vec::new();
            for mut pair in region_batch.pairs {
                if pair.error.is_none() {
                    pairs.push(KvPair::from(pair));
                    continue;
                }
                let logical_key: Key = if pair.key.is_empty() {
                    extract_lock_from_key_error(
                        pair.error.as_ref().expect("pair error checked above"),
                    )?
                    .key
                    .into()
                } else {
                    std::mem::take(&mut pair.key).into()
                };
                let request_key = logical_key.clone().encode_keyspace(keyspace, KeyMode::Txn);
                let request = new_get_request(request_key, timestamp.clone());
                let get_resource_group_tag = resource_group_tag.clone().or_else(|| {
                    resource_group_tagger
                        .as_ref()
                        .map(|tagger| tagger(SnapshotRequestType::Get))
                });
                let get_plan = plan_with_keyspace_name(
                    rpc.clone(),
                    keyspace,
                    keyspace_name.as_deref(),
                    get_rpc_interceptor.clone(),
                    resource_group_name.as_deref(),
                    resource_control.clone(),
                    ru_details.clone(),
                    get_replica_read_config.clone(),
                    request,
                )
                .request_source(request_source.clone())
                .priority(priority)
                .not_fill_cache(not_fill_cache)
                .isolation_level(isolation_level)
                .task_id(task_id)
                .resource_group_tag(get_resource_group_tag)
                .snapshot_read_timeout(snapshot_read_timeout, SNAPSHOT_READ_TIMEOUT_SHORT)
                .validate_read_timestamp(
                    read_timestamp_validator.clone(),
                    timestamp.version(),
                    get_replica_read_config.stale_read,
                    read_replica_scope.clone(),
                )
                .resolve_lock_for_read(
                    timestamp.clone(),
                    retry_options.lock_backoff.clone(),
                    keyspace,
                    read_lock_context.clone(),
                    lock_resolver_context.clone(),
                    snapshot_runtime_stats.clone(),
                    snapshot_variables.clone(),
                )
                .without_snapshot_lock_backoff_stats()
                .max_timestamp_point_get(max_timestamp_point_get)
                .retry_multi_region_with_snapshot_stats(
                    DEFAULT_REGION_BACKOFF,
                    snapshot_runtime_stats.clone(),
                    snapshot_variables.clone(),
                )
                .snapshot_retry_owner(Arc::clone(&scanner_retry_owner))
                .without_snapshot_region_backoff_stats()
                .merge(CollectSingle)
                .post_process_default()
                .plan();
                if let Some(value) = get_plan.execute().await? {
                    if !value.is_empty() {
                        pairs.push(KvPair::new(logical_key, value));
                    }
                }
            }

            pairs.sort_unstable_by(|left, right| {
                if reverse {
                    right.key().cmp(left.key())
                } else {
                    left.key().cmp(right.key())
                }
            });

            let short_batch = raw_batch_len < batch_size as usize;
            let region_start: Key = region_batch.range.0.into();
            let region_end: Key = region_batch.range.1.into();
            let next_encoded = if short_batch {
                if reverse {
                    region_start
                } else {
                    region_end
                }
            } else {
                let raw_last_key = raw_last_key
                    .ok_or_else(|| Error::StringError("full scanner batch is empty".to_owned()))??
                    .encode_keyspace(keyspace, KeyMode::Txn);
                if reverse {
                    raw_last_key
                } else {
                    raw_last_key.next_key()
                }
            };
            let exhausted = if reverse {
                next_encoded.is_empty() || next_encoded <= scan_start
            } else {
                next_encoded.is_empty() || scan_end.as_ref().is_some_and(|end| next_encoded >= *end)
            };

            if !exhausted {
                let next_logical = next_encoded.truncate_keyspace(keyspace);
                if reverse {
                    logical_range.to = Bound::Excluded(next_logical);
                } else {
                    logical_range.from = Bound::Included(next_logical);
                }
                encoded_range = logical_range
                    .clone()
                    .encode_keyspace(keyspace, KeyMode::Txn);
            }

            let pairs = pairs
                .into_iter()
                .map(|pair| {
                    pair.encode_keyspace(keyspace, KeyMode::Txn)
                        .truncate_keyspace(keyspace)
                })
                .collect::<Vec<_>>();
            if !pairs.is_empty() || exhausted {
                return Ok(SnapshotScannerBatch {
                    pairs,
                    next_range: logical_range,
                    exhausted,
                });
            }
        }
    }

    async fn scan_inner(
        &mut self,
        range: impl Into<BoundRange>,
        limit: u32,
        key_only: bool,
        reverse: bool,
    ) -> Result<impl Iterator<Item = KvPair>> {
        self.check_allow_operation().await?;
        let timestamp = self.timestamp.clone();
        let snapshot_version = timestamp.version();
        let rpc = self.rpc.clone();
        let retry_options = self.options.retry_options.clone();
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let scan_rpc_interceptor = snapshot_runtime_interceptor(self.rpc_interceptor.clone(), None);
        let get_rpc_interceptor = snapshot_runtime_interceptor(
            self.rpc_interceptor.clone(),
            self.snapshot_runtime_stats.clone(),
        );
        let snapshot_runtime_stats = self.snapshot_runtime_stats.clone();
        let snapshot_variables = self.snapshot_variables.clone();
        let scanner_retry_owner =
            crate::request::plan::new_snapshot_retry_owner(Arc::clone(&snapshot_variables));
        let resource_group_name = self.resource_group_name.clone();
        let resource_control = self.resource_control.clone();
        let ru_details = self.ru_details.clone();
        let priority = self.options.priority;
        let sample_step = self.sample_step;
        let key_only = key_only || self.snapshot_key_only;
        // client-go defers this normalization to `newScanner`, so a stored
        // zero or one cannot make a scanner fail to advance.
        let scan_batch_size = if self.snapshot_scan_batch_size <= 1 {
            DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE
        } else {
            self.snapshot_scan_batch_size
        };
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let replica_read_config = self.replica_read_config.clone();
        let get_replica_read_config = self.replica_read_config_for_items(1);
        let snapshot_read_timeout = self.snapshot_read_timeout;
        let read_replica_scope = self.read_replica_scope.clone();
        let read_lock_context = self.read_lock_context.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let request_source = self.commit_settings.request_source.context_value();
        let max_timestamp_point_get = timestamp.version() == u64::MAX;
        let range = range.into().encode_keyspace(self.keyspace, KeyMode::Txn);

        self.buffer
            .scan_and_fetch(
                range,
                limit,
                !key_only && !self.options.read_only,
                reverse,
                move |new_range, new_limit| async move {
                    let mut range = new_range;
                    let mut pairs = Vec::new();

                    while pairs.len() < new_limit as usize {
                        let remaining = new_limit.saturating_sub(pairs.len() as u32);
                        let request_limit = remaining.min(scan_batch_size);
                        let request = new_scan_request(
                            range.clone(),
                            timestamp.clone(),
                            request_limit,
                            key_only,
                            reverse,
                            sample_step,
                        );
                        let scan_resource_group_tag = resource_group_tag.clone().or_else(|| {
                            resource_group_tagger
                                .as_ref()
                                .map(|tagger| tagger(SnapshotRequestType::Scan))
                        });
                        let plan = plan_with_keyspace_name(
                            rpc.clone(),
                            keyspace,
                            keyspace_name.as_deref(),
                            scan_rpc_interceptor.clone(),
                            resource_group_name.as_deref(),
                            resource_control.clone(),
                            ru_details.clone(),
                            replica_read_config.clone(),
                            request,
                        )
                        .request_source(request_source.clone())
                        .priority(priority)
                        .not_fill_cache(not_fill_cache)
                        .isolation_level(isolation_level)
                        .task_id(task_id)
                        .resource_group_tag(scan_resource_group_tag)
                        .snapshot_read_timeout(None, SNAPSHOT_READ_TIMEOUT_MEDIUM)
                        .validate_read_timestamp(
                            read_timestamp_validator.clone(),
                            timestamp.version(),
                            replica_read_config.stale_read,
                            String::new(),
                        )
                        .resolve_response_lock_for_read(
                            timestamp.clone(),
                            retry_options.lock_backoff.clone(),
                            keyspace,
                            read_lock_context.clone(),
                            lock_resolver_context.clone(),
                            snapshot_runtime_stats.clone(),
                            snapshot_variables.clone(),
                        )
                        .without_snapshot_lock_backoff_stats()
                        .process(PreserveScannerPairErrors)
                        .retry_multi_region_with_snapshot_stats(
                            retry_options.region_backoff.clone(),
                            None,
                            snapshot_variables.clone(),
                        )
                        .snapshot_retry_owner(Arc::clone(&scanner_retry_owner))
                        .merge(CollectScannerPairs)
                        .plan();
                        let raw_batch = plan.execute().await?;
                        if let Some(validator) = &snapshot_visibility_validator {
                            validator.check_visibility(snapshot_version).await?;
                        }
                        if raw_batch.is_empty() {
                            break;
                        }
                        let raw_batch_len = raw_batch.len();
                        let raw_last = raw_batch.last().expect("non-empty raw scan batch");
                        let raw_last_key: Key = if raw_last.key.is_empty() {
                            let error = raw_last.error.as_ref().ok_or_else(|| {
                                Error::StringError(
                                    "scan pair has neither a key nor a key error".to_owned(),
                                )
                            })?;
                            extract_lock_from_key_error(error)?.key.into()
                        } else {
                            raw_last.key.clone().into()
                        };
                        let mut batch = Vec::new();
                        for mut pair in raw_batch {
                            if pair.error.is_none() {
                                batch.push(KvPair::from(pair));
                                continue;
                            }

                            let logical_key: Key = if pair.key.is_empty() {
                                extract_lock_from_key_error(
                                    pair.error.as_ref().expect("pair error checked above"),
                                )?
                                .key
                                .into()
                            } else {
                                std::mem::take(&mut pair.key).into()
                            };
                            let request_key =
                                logical_key.clone().encode_keyspace(keyspace, KeyMode::Txn);
                            let request = new_get_request(request_key, timestamp.clone());
                            let get_resource_group_tag = resource_group_tag.clone().or_else(|| {
                                resource_group_tagger
                                    .as_ref()
                                    .map(|tagger| tagger(SnapshotRequestType::Get))
                            });
                            let get_plan = plan_with_keyspace_name(
                                rpc.clone(),
                                keyspace,
                                keyspace_name.as_deref(),
                                get_rpc_interceptor.clone(),
                                resource_group_name.as_deref(),
                                resource_control.clone(),
                                ru_details.clone(),
                                get_replica_read_config.clone(),
                                request,
                            )
                            .request_source(request_source.clone())
                            .priority(priority)
                            .not_fill_cache(not_fill_cache)
                            .isolation_level(isolation_level)
                            .task_id(task_id)
                            .resource_group_tag(get_resource_group_tag)
                            .snapshot_read_timeout(
                                snapshot_read_timeout,
                                SNAPSHOT_READ_TIMEOUT_SHORT,
                            )
                            .validate_read_timestamp(
                                read_timestamp_validator.clone(),
                                timestamp.version(),
                                get_replica_read_config.stale_read,
                                read_replica_scope.clone(),
                            )
                            .resolve_lock_for_read(
                                timestamp.clone(),
                                retry_options.lock_backoff.clone(),
                                keyspace,
                                read_lock_context.clone(),
                                lock_resolver_context.clone(),
                                snapshot_runtime_stats.clone(),
                                snapshot_variables.clone(),
                            )
                            .without_snapshot_lock_backoff_stats()
                            .max_timestamp_point_get(max_timestamp_point_get)
                            .retry_multi_region_with_snapshot_stats(
                                DEFAULT_REGION_BACKOFF,
                                snapshot_runtime_stats.clone(),
                                snapshot_variables.clone(),
                            )
                            .snapshot_retry_owner(Arc::clone(&scanner_retry_owner))
                            .without_snapshot_region_backoff_stats()
                            .merge(CollectSingle)
                            .post_process_default()
                            .plan();
                            if let Some(value) = get_plan.execute().await? {
                                if !value.is_empty() {
                                    batch.push(KvPair::new(logical_key, value));
                                }
                            }
                        }
                        let mut batch: Vec<_> = batch
                            .into_iter()
                            .map(|pair| pair.encode_keyspace(keyspace, KeyMode::Txn))
                            .collect();
                        batch.sort_unstable_by(|left, right| {
                            if reverse {
                                right.key().cmp(left.key())
                            } else {
                                left.key().cmp(right.key())
                            }
                        });
                        pairs.extend(batch);
                        if raw_batch_len < request_limit as usize {
                            break;
                        }
                        let last_key = raw_last_key.encode_keyspace(keyspace, KeyMode::Txn);
                        if reverse {
                            range.to = Bound::Excluded(last_key);
                        } else {
                            range.from = Bound::Included(last_key.next_key());
                        }
                    }

                    Ok(pairs)
                },
            )
            .await
            .map(move |pairs| pairs.map(move |pair| pair.truncate_keyspace(keyspace)))
    }

    /// Pessimistically lock the keys, and optionally retrieve corresponding values.
    /// If a key does not exist, the corresponding pair will not appear in the result.
    ///
    /// Once resolved it acquires locks on the keys in TiKV.
    /// A lock prevents other transactions from mutating the entry until it is released.
    ///
    /// # Panics
    ///
    /// Only valid for pessimistic transactions, panics if called on an optimistic transaction.
    async fn pessimistic_lock(
        &mut self,
        keys: impl IntoIterator<Item = impl PessimisticLock>,
        need_value: bool,
    ) -> Result<Vec<KvPair>> {
        self.pessimistic_lock_with_options(
            keys,
            need_value,
            0,
            kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal,
        )
        .await
    }

    async fn pessimistic_lock_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl PessimisticLock>,
        need_value: bool,
        wait_timeout: i64,
        wake_up_mode: kvrpcpb::PessimisticLockWakeUpMode,
    ) -> Result<Vec<KvPair>> {
        let locks = keys
            .into_iter()
            .map(|lock| {
                let assertion = lock.assertion();
                (lock.key(), assertion)
            })
            .collect();
        self.pessimistic_lock_impl(
            locks,
            need_value,
            kvrpcpb::Op::PessimisticLock,
            wait_timeout,
            wake_up_mode,
            None,
        )
        .await
    }

    async fn pessimistic_lock_with_context_options(
        &mut self,
        keys: Vec<Key>,
        lock_type: kvrpcpb::Op,
        wake_up_mode: kvrpcpb::PessimisticLockWakeUpMode,
        context: &mut LockContext,
    ) -> Result<Vec<KvPair>> {
        let need_value = context.return_values;
        let locks = keys
            .into_iter()
            .map(|key| (key, kvrpcpb::Assertion::None))
            .collect();
        self.pessimistic_lock_impl(locks, need_value, lock_type, 0, wake_up_mode, Some(context))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn pessimistic_lock_impl(
        &mut self,
        locks: Vec<(Key, kvrpcpb::Assertion)>,
        need_value: bool,
        lock_type: kvrpcpb::Op,
        wait_timeout: i64,
        wake_up_mode: kvrpcpb::PessimisticLockWakeUpMode,
        mut context: Option<&mut LockContext>,
    ) -> Result<Vec<KvPair>> {
        assert!(
            matches!(self.options.kind, TransactionKind::Pessimistic(_)),
            "`pessimistic_lock` is only valid to use with pessimistic transactions"
        );

        if locks.is_empty() {
            return Ok(vec![]);
        }
        debug!(
            "acquiring pessimistic lock, start_ts: {}, keys: {}, need_value: {}",
            self.timestamp.version(),
            locks.len(),
            need_value,
        );

        let first_key = locks[0].0.clone();
        let lock_assertions = locks.iter().cloned().collect::<BTreeMap<_, _>>();
        let keys = locks.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
        // we do not set the primary key here, because pessimistic lock request
        // can fail, in which case the keys may not be part of the transaction.
        let primary_lock = self
            .buffer
            .get_primary_key()
            .unwrap_or_else(|| first_key.clone());
        let for_update_ts = match context.as_deref() {
            Some(context) => Timestamp::from_version(context.for_update_ts),
            None => self.rpc.clone().get_timestamp().await?,
        };
        self.options.push_for_update_ts(for_update_ts.clone());
        let mut request = new_pessimistic_lock_request(
            locks.clone().into_iter(),
            primary_lock,
            self.timestamp.clone(),
            MAX_TTL,
            for_update_ts.clone(),
            need_value,
        );
        for mutation in &mut request.mutations {
            mutation.op = lock_type as i32;
        }
        request.is_first_lock = self.pessimistic_lock_count == 0 && keys.len() == 1;
        request.min_commit_ts = for_update_ts.version().saturating_add(1);
        request.wait_timeout = match context.as_deref_mut() {
            Some(context) => effective_pessimistic_lock_wait_time(context, SystemTime::now())?,
            None => wait_timeout,
        };
        request.wake_up_mode = wake_up_mode as i32;
        if let Some(context) = context.as_deref() {
            request.return_values = context.return_values;
            request.check_existence = context.check_existence;
            request.lock_only_if_exists = context.lock_only_if_exists;
        }
        self.commit_settings
            .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
        if let Some(context) = context.as_deref() {
            let tag = if !context.resource_group_tag.is_empty() {
                Some(context.resource_group_tag.clone())
            } else {
                context
                    .resource_group_tagger
                    .as_ref()
                    .map(|tagger| tagger(&request))
            };
            if let Some(tag) = tag {
                request
                    .context
                    .get_or_insert_with(kvrpcpb::Context::default)
                    .resource_group_tag = tag;
            }
        }
        let plan = self
            .plan(request)
            .priority(self.options.priority)
            .resolve_lock_with_context(
                self.timestamp.clone(),
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
                self.lock_resolver_context.clone(),
            )
            .preserve_shard()
            .retry_multi_region_preserve_results(self.options.retry_options.region_backoff.clone())
            .merge(CollectPessimisticLock)
            .plan();
        let output = plan.execute().await;

        if let Err(err) = output {
            let deadlock = pessimistic_deadlock(&err);
            if let (Some(context), Some(deadlock)) = (context.as_deref(), deadlock.as_ref()) {
                let is_retryable = keys
                    .iter()
                    .any(|key| farmhash::fingerprint64(key.as_ref()) == deadlock.deadlock_key_hash);
                if let Some(on_deadlock) = &context.on_deadlock {
                    on_deadlock(&crate::kv::DeadlockError {
                        deadlock: deadlock.clone(),
                        is_retryable,
                    });
                }
            }
            match err {
                Error::PessimisticLockError {
                    inner,
                    success_keys,
                } if !success_keys.is_empty() => {
                    debug!(
                        "pessimistic lock failed, rolling back {} partially-acquired lock(s), start_ts: {}, for_update_ts: {}",
                        success_keys.len(),
                        self.timestamp.version(),
                        for_update_ts.version(),
                    );
                    let success_keys = success_keys.into_iter().map(Key::from);
                    self.pessimistic_lock_rollback(
                        success_keys,
                        self.timestamp.clone(),
                        for_update_ts.clone(),
                    )
                    .await?;
                    if let Some(deadlock) = deadlock {
                        Err(crate::error::DeadlockError {
                            is_retryable: keys.iter().any(|key| {
                                farmhash::fingerprint64(key.as_ref()) == deadlock.deadlock_key_hash
                            }),
                            deadlock,
                        }
                        .into())
                    } else {
                        Err(*inner)
                    }
                }
                _ => {
                    if let Some(deadlock) = deadlock {
                        Err(crate::error::DeadlockError {
                            is_retryable: keys.iter().any(|key| {
                                farmhash::fingerprint64(key.as_ref()) == deadlock.deadlock_key_hash
                            }),
                            deadlock,
                        }
                        .into())
                    } else {
                        Err(err)
                    }
                }
            }
        } else {
            let output = output.expect("pessimistic lock result checked above");
            if output.max_locked_with_conflict_ts > for_update_ts.version() {
                self.options.push_for_update_ts(Timestamp::from_version(
                    output.max_locked_with_conflict_ts,
                ));
            }
            if let Some(context) = context.as_deref_mut() {
                context.max_locked_with_conflict_ts = context
                    .max_locked_with_conflict_ts
                    .max(output.max_locked_with_conflict_ts);
                for (key, value) in &output.returned_values {
                    let logical_key = key.clone().truncate_keyspace(self.keyspace);
                    context
                        .insert_returned_value(<&[u8]>::from(&logical_key).to_vec(), value.clone());
                }
            }

            let returned_values = output
                .returned_values
                .iter()
                .cloned()
                .collect::<BTreeMap<_, _>>();
            let lock_only_if_exists = context
                .as_deref()
                .is_some_and(|context| context.lock_only_if_exists);
            let locked_keys = keys
                .into_iter()
                .filter(|key| {
                    !lock_only_if_exists
                        || returned_values.get(key).is_none_or(|value| value.exists)
                })
                .collect::<Vec<_>>();
            if !locked_keys.is_empty() && lock_type != kvrpcpb::Op::SharedPessimisticLock {
                self.buffer.primary_key_or(&first_key);
            }

            let aggressive = self.aggressive_locking.is_some()
                && wake_up_mode == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock;
            if !locked_keys.is_empty() {
                self.start_auto_heartbeat().await;
            }
            if aggressive && !locked_keys.is_empty() {
                self.aggressive_locking_dirty = true;
            }
            for key in locked_keys {
                let inferred_value = match lock_assertions.get(&key) {
                    Some(kvrpcpb::Assertion::NotExist) => Some(ReturnedValue {
                        exists: false,
                        ..Default::default()
                    }),
                    Some(kvrpcpb::Assertion::Exist) => Some(ReturnedValue {
                        exists: true,
                        ..Default::default()
                    }),
                    _ => None,
                };
                let returned_value = returned_values.get(&key).or(inferred_value.as_ref());
                if aggressive {
                    let value = returned_values.get(&key).cloned().unwrap_or_default();
                    let actual_timestamp =
                        for_update_ts.version().max(value.locked_with_conflict_ts);
                    self.buffer.unlock(&key);
                    self.aggressive_locking
                        .as_mut()
                        .expect("aggressive locking checked above")
                        .current
                        .insert(
                            key,
                            AggressiveLockEntry {
                                has_return_value: context
                                    .as_deref()
                                    .is_some_and(|context| context.return_values),
                                has_check_existence: context
                                    .as_deref()
                                    .is_some_and(|context| context.check_existence),
                                value,
                                actual_for_update_ts: Timestamp::from_version(actual_timestamp),
                            },
                        );
                } else if lock_type == kvrpcpb::Op::SharedPessimisticLock {
                    self.buffer
                        .lock_with_returned_value(key.clone(), true, returned_value)
                        .map_err(|error| Error::StringError(error.to_owned()))?;
                } else {
                    self.buffer
                        .lock_with_returned_value(key.clone(), false, returned_value)
                        .map_err(|error| Error::StringError(error.to_owned()))?;
                }
                self.pessimistic_lock_count += 1;
            }

            Ok(output.pairs)
        }
    }

    /// Rollback pessimistic lock
    async fn pessimistic_lock_rollback(
        &mut self,
        keys: impl Iterator<Item = Key>,
        start_version: Timestamp,
        for_update_ts: Timestamp,
    ) -> Result<()> {
        let keys: Vec<_> = keys.into_iter().collect();
        if keys.is_empty() {
            return Ok(());
        }
        debug!(
            "rolling back pessimistic lock, start_ts: {}, for_update_ts: {}, keys: {}",
            start_version.version(),
            for_update_ts.version(),
            keys.len(),
        );

        let mut req = new_pessimistic_rollback_request(
            keys.clone().into_iter(),
            start_version.clone(),
            for_update_ts,
        );
        self.commit_settings
            .apply_request(&mut req, SNAPSHOT_READ_TIMEOUT_SHORT);
        let plan = self
            .plan(req)
            .priority(self.options.priority)
            .resolve_lock_with_context(
                start_version,
                self.options.retry_options.lock_backoff.clone(),
                self.keyspace,
                self.lock_resolver_context.clone(),
            )
            .retry_multi_region(self.options.retry_options.region_backoff.clone())
            .extract_error()
            .plan();
        plan.execute().await?;

        for key in keys {
            self.buffer.unlock(&key);
        }
        Ok(())
    }

    /// Checks if the transaction can perform arbitrary operations.
    async fn check_allow_operation(&self) -> Result<()> {
        match self.get_status() {
            TransactionStatus::ReadOnly | TransactionStatus::Active => Ok(()),
            TransactionStatus::Committed
            | TransactionStatus::Rolledback
            | TransactionStatus::StartedCommit
            | TransactionStatus::StartedRollback
            | TransactionStatus::Dropped => Err(Error::OperationAfterCommitError),
        }
    }

    fn is_pessimistic(&self) -> bool {
        matches!(self.options.kind, TransactionKind::Pessimistic(_))
    }

    async fn start_auto_heartbeat(&mut self) {
        if !self.options.heartbeat_option.is_auto_heartbeat() || self.is_heartbeat_started {
            return;
        }
        self.is_heartbeat_started = true;

        let status = self.status.clone();
        let primary_key = self
            .buffer
            .get_primary_key()
            .expect("Primary key should exist");
        let start_ts = self.timestamp.clone();
        let region_backoff = self.options.retry_options.region_backoff.clone();
        let rpc = self.rpc.clone();
        let heartbeat_interval = match self.options.heartbeat_option {
            HeartbeatOption::NoHeartbeat => DEFAULT_HEARTBEAT_INTERVAL,
            HeartbeatOption::FixedTime(heartbeat_interval) => heartbeat_interval,
        };
        let start_instant = self.start_instant;
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let rpc_interceptor = self.rpc_interceptor.clone();
        let resource_group_name = self.resource_group_name.clone();
        let resource_control = self.resource_control.clone();
        let ru_details = self.ru_details.clone();
        let commit_settings = self.commit_settings.clone();
        let lifecycle_hooks = self.commit_settings.lifecycle_hooks.clone();
        debug!(
            "starting auto-heartbeat, start_ts: {}, interval: {:?}",
            self.timestamp.version(),
            heartbeat_interval,
        );

        let heartbeat_task = async move {
            loop {
                tokio::time::sleep(heartbeat_interval).await;
                {
                    let status: TransactionStatus = status.load(atomic::Ordering::Acquire).into();
                    if matches!(
                        status,
                        TransactionStatus::Rolledback
                            | TransactionStatus::Committed
                            | TransactionStatus::Dropped
                    ) {
                        break;
                    }
                }
                let mut request = new_heart_beat_request(
                    start_ts.clone(),
                    primary_key.clone(),
                    start_instant.elapsed().as_millis() as u64 + MAX_TTL,
                );
                commit_settings.apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
                let plan = plan_with_keyspace_name(
                    rpc.clone(),
                    keyspace,
                    keyspace_name.as_deref(),
                    rpc_interceptor.clone(),
                    resource_group_name.as_deref(),
                    resource_control.clone(),
                    ru_details.clone(),
                    ReplicaReadConfig::default(),
                    request,
                )
                .retry_multi_region(region_backoff.clone())
                .merge(CollectSingle)
                .plan();
                plan.execute().await?;
            }
            Ok::<(), Error>(())
        };

        let start_ts_for_log = self.timestamp.version();
        tokio::spawn(async move {
            if let Some(pre) = lifecycle_hooks.pre {
                pre();
            }
            if let Err(err) = heartbeat_task.await {
                log::error!(
                    "auto-heartbeat task terminated, start_ts: {}: {}",
                    start_ts_for_log,
                    err
                );
            }
            if let Some(post) = lifecycle_hooks.post {
                post();
            }
        });
    }

    fn get_status(&self) -> TransactionStatus {
        self.status.load(atomic::Ordering::Acquire).into()
    }

    fn set_status(&self, status: TransactionStatus) {
        self.status.store(status as u8, atomic::Ordering::Release);
    }

    fn transit_status<F>(&self, check_status: F, next: TransactionStatus) -> bool
    where
        F: Fn(TransactionStatus) -> bool,
    {
        let mut current = self.get_status();
        while check_status(current) {
            if current == next {
                return true;
            }
            match self.status.compare_exchange_weak(
                current as u8,
                next as u8,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(x) => current = x.into(),
            }
        }
        false
    }
}

fn plan_with_keyspace_name<PdC: PdClient, Req: KvRequest>(
    rpc: Arc<PdC>,
    keyspace: Keyspace,
    keyspace_name: Option<&str>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<&str>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    replica_read_config: ReplicaReadConfig,
    request: Req,
) -> PlanBuilder<PdC, Dispatch<Req>, NoTarget> {
    PlanBuilder::new(rpc, keyspace, request)
        .keyspace_name_option(keyspace_name)
        .rpc_interceptor_option(rpc_interceptor)
        .resource_group_option(resource_group_name)
        .resource_control_option(resource_control)
        .ru_details_option(ru_details)
        .replica_read(replica_read_config)
}

fn adjusted_replica_read_config(
    stable: &ReplicaReadConfig,
    adjuster: Option<&ReplicaReadAdjuster>,
    item_count: usize,
) -> ReplicaReadConfig {
    let mut config = stable.clone();
    if config.read_type.is_follower_read() {
        if let Some(adjuster) = adjuster {
            config.apply_adjustment(adjuster(item_count));
        }
    }
    config
}

impl<PdC: PdClient> std::fmt::Display for Transaction<PdC> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.timestamp.version())?;
        if let Some(context) = &self.aggressive_locking {
            write!(
                formatter,
                " (aggressiveLocking: prev {} keys, current {} keys)",
                context.previous.len(),
                context.current.len()
            )?;
        }
        Ok(())
    }
}

impl<PdC: PdClient> Drop for Transaction<PdC> {
    fn drop(&mut self) {
        debug!(
            "dropping transaction, start_ts: {}, status: {:?}",
            self.timestamp.version(),
            self.get_status()
        );
        if std::thread::panicking() {
            return;
        }
        if self.get_status() == TransactionStatus::Active {
            let start_ts = self.timestamp.version();
            match self.options.check_level {
                CheckLevel::Panic => {
                    panic!("dropping an active transaction (start_ts: {start_ts}). Consider commit or rollback it.")
                }
                CheckLevel::Warn => {
                    warn!("dropping an active transaction, start_ts: {start_ts}. Consider commit or rollback it.")
                }
                // Even with the drop check disabled, leave a debug breadcrumb so
                // an unfinished transaction is not completely silent.
                CheckLevel::None => {
                    debug!("dropping an active transaction (drop check disabled), start_ts: {start_ts}")
                }
            }
        }
        self.set_status(TransactionStatus::Dropped);
    }
}

/// The default max TTL of a lock in milliseconds. Also called `ManagedLockTTL` in TiDB.
const MAX_TTL: u64 = 20000;
/// The default TTL of a lock in milliseconds.
const DEFAULT_LOCK_TTL: u64 = 3000;
/// The default heartbeat interval
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(MAX_TTL / 2);
/// TiKV recommends each RPC packet should be less than around 1MB. We keep KV size of
/// each request below 16KB.
pub const TXN_COMMIT_BATCH_SIZE: u64 = 16 * 1024;
const TTL_FACTOR: f64 = 6000.0;
static PRE_SPLIT_DETECT_THRESHOLD: atomic::AtomicU64 = atomic::AtomicU64::new(100_000);
static PRE_SPLIT_SIZE_THRESHOLD: atomic::AtomicU64 = atomic::AtomicU64::new(32 << 20);

/// Optimistic or pessimistic transaction.
#[derive(Clone, PartialEq, Debug)]
pub enum TransactionKind {
    Optimistic,
    /// Argument is the transaction's for_update_ts
    Pessimistic(Timestamp),
}

fn ensure_snapshot_commit_ts(return_commit_ts: bool, entry: Option<&ValueEntry>) -> Result<()> {
    if return_commit_ts
        && entry.is_some_and(|entry| !entry.is_value_empty() && entry.commit_ts == 0)
    {
        return Err(Error::StringError(
            "commit timestamp is required but not returned".to_owned(),
        ));
    }
    Ok(())
}

/// Options for configuring a transaction.
///
/// `TransactionOptions` has a builder-style API.
#[derive(Clone, PartialEq, Debug)]
pub struct TransactionOptions {
    /// Optimistic or pessimistic (default) transaction.
    kind: TransactionKind,
    /// Try using 1pc rather than 2pc (default is to always use 2pc).
    try_one_pc: bool,
    /// Try to use async commit (default is not to).
    async_commit: bool,
    /// Is the transaction read only? (Default is no).
    read_only: bool,
    /// How to retry in the event of certain errors.
    retry_options: RetryOptions,
    /// What to do if the transaction is dropped without an attempt to commit or rollback
    check_level: CheckLevel,
    /// Priority carried by read and write request contexts.
    priority: Priority,
    /// Transaction timestamp scope retained by commit timestamp allocation.
    scope: String,
    /// Optional pipelined transaction protocol.
    pipelined: PipelinedTxnOptions,
    #[doc(hidden)]
    heartbeat_option: HeartbeatOption,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum HeartbeatOption {
    NoHeartbeat,
    FixedTime(Duration),
}

impl Default for TransactionOptions {
    fn default() -> TransactionOptions {
        Self::new_pessimistic()
    }
}

impl TransactionOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if !self.pipelined.enable {
            return Ok(());
        }
        if self.pipelined.flush_concurrency == 0 {
            return Err(Error::StringError(
                "pipelined txn flush concurrency should be greater than 0".to_owned(),
            ));
        }
        if self.pipelined.resolve_lock_concurrency == 0 {
            return Err(Error::StringError(
                "pipelined txn resolve lock concurrency should be greater than 0".to_owned(),
            ));
        }
        if self.pipelined.write_throttle_ratio < 0.0 || self.pipelined.write_throttle_ratio >= 1.0 {
            return Err(Error::StringError(format!(
                "invalid write throttle ratio: {}",
                self.pipelined.write_throttle_ratio
            )));
        }
        Ok(())
    }

    /// Default options for an optimistic transaction.
    pub fn new_optimistic() -> TransactionOptions {
        TransactionOptions {
            kind: TransactionKind::Optimistic,
            try_one_pc: false,
            async_commit: false,
            read_only: false,
            retry_options: RetryOptions::default_optimistic(),
            check_level: CheckLevel::Panic,
            priority: Priority::Normal,
            scope: crate::oracle::GLOBAL_TXN_SCOPE.to_owned(),
            pipelined: PipelinedTxnOptions::default(),
            heartbeat_option: HeartbeatOption::FixedTime(DEFAULT_HEARTBEAT_INTERVAL),
        }
    }

    /// Default options for a pessimistic transaction.
    pub fn new_pessimistic() -> TransactionOptions {
        TransactionOptions {
            kind: TransactionKind::Pessimistic(Timestamp::from_version(0)),
            try_one_pc: false,
            async_commit: false,
            read_only: false,
            retry_options: RetryOptions::default_pessimistic(),
            check_level: CheckLevel::Panic,
            priority: Priority::Normal,
            scope: crate::oracle::GLOBAL_TXN_SCOPE.to_owned(),
            pipelined: PipelinedTxnOptions::default(),
            heartbeat_option: HeartbeatOption::FixedTime(DEFAULT_HEARTBEAT_INTERVAL),
        }
    }

    /// Try to use async commit.
    #[must_use]
    pub fn use_async_commit(mut self) -> TransactionOptions {
        self.async_commit = true;
        self
    }

    /// Try to use 1pc.
    #[must_use]
    pub fn try_one_pc(mut self) -> TransactionOptions {
        self.try_one_pc = true;
        self
    }

    /// Make the transaction read only.
    #[must_use]
    pub fn read_only(mut self) -> TransactionOptions {
        self.read_only = true;
        self
    }

    /// Don't automatically resolve locks and retry if keys are locked.
    #[must_use]
    pub fn no_resolve_locks(mut self) -> TransactionOptions {
        self.retry_options.lock_backoff = Backoff::no_backoff();
        self
    }

    /// Don't automatically resolve regions with PD if we have outdated region information.
    #[must_use]
    pub fn no_resolve_regions(mut self) -> TransactionOptions {
        self.retry_options.region_backoff = Backoff::no_backoff();
        self
    }

    /// Set RetryOptions.
    #[must_use]
    pub fn retry_options(mut self, options: RetryOptions) -> TransactionOptions {
        self.retry_options = options;
        self
    }

    /// Set the behavior when dropping a transaction without an attempt to commit or rollback it.
    #[must_use]
    pub fn drop_check(mut self, level: CheckLevel) -> TransactionOptions {
        self.check_level = level;
        self
    }

    /// Set the priority for both read and write requests.
    #[must_use]
    pub fn priority(mut self, priority: Priority) -> TransactionOptions {
        self.priority = priority;
        self
    }

    #[must_use]
    pub fn scope(mut self, scope: impl Into<String>) -> TransactionOptions {
        self.scope = scope.into();
        self
    }

    #[must_use]
    pub fn pipelined(mut self, options: PipelinedTxnOptions) -> TransactionOptions {
        self.pipelined = options;
        self
    }

    fn push_for_update_ts(&mut self, for_update_ts: Timestamp) {
        match &mut self.kind {
            TransactionKind::Optimistic => unreachable!(),
            TransactionKind::Pessimistic(old_for_update_ts) => {
                self.kind = TransactionKind::Pessimistic(Timestamp::from_version(std::cmp::max(
                    old_for_update_ts.version(),
                    for_update_ts.version(),
                )));
            }
        }
    }

    #[must_use]
    pub fn heartbeat_option(mut self, heartbeat_option: HeartbeatOption) -> TransactionOptions {
        self.heartbeat_option = heartbeat_option;
        self
    }

    // Returns true if these options describe a pessimistic transaction.
    pub fn is_pessimistic(&self) -> bool {
        match self.kind {
            TransactionKind::Pessimistic(_) => true,
            TransactionKind::Optimistic => false,
        }
    }
}

/// Determines what happens when a transaction is dropped without being rolled back or committed.
///
/// The default is to panic.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum CheckLevel {
    /// The program will panic.
    ///
    /// Note that if the thread is already panicking, then we will not double-panic and abort, but
    /// just ignore the issue.
    Panic,
    /// Log a warning.
    Warn,
    /// Do nothing
    None,
}

impl HeartbeatOption {
    pub fn is_auto_heartbeat(&self) -> bool {
        !matches!(self, HeartbeatOption::NoHeartbeat)
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Mutation {
    Put(Key, Value),
    Delete(Key),
}

impl Mutation {
    pub fn key(&self) -> &Key {
        match self {
            Mutation::Put(key, _) => key,
            Mutation::Delete(key) => key,
        }
    }
}

/// A struct wrapping the details of two-phase commit protocol (2PC).
///
/// The two phases are `prewrite` and `commit`.
/// Generally, the `prewrite` phase is to send data to all regions and write them.
/// The `commit` phase is to mark all written data as successfully committed.
///
/// The committer implements `prewrite`, `commit` and `rollback` functions.
#[allow(clippy::too_many_arguments)]
#[derive(new)]
struct Committer<PdC: PdClient = PdRpcClient> {
    primary_key: Option<Key>,
    mutations: Vec<kvrpcpb::Mutation>,
    start_version: Timestamp,
    rpc: Arc<PdC>,
    options: TransactionOptions,
    settings: CommitSettings,
    keyspace: Keyspace,
    keyspace_name: Option<String>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<String>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    lock_resolver_context: ResolveLocksContext,
    pipelined_state: PipelinedTransactionState,
    #[new(default)]
    undetermined: bool,
    #[new(default)]
    committed: bool,
    #[new(default)]
    binlog_skipped: bool,
    #[new(default)]
    tried_async_commit: bool,
    #[new(default)]
    tried_one_pc: bool,
    #[new(default)]
    min_commit_ts: MinCommitTsManager,
    #[new(default)]
    max_commit_ts: u64,
    #[new(default)]
    txn_file_chunks: TxnChunkSlice,
    #[new(default)]
    txn_file_commit_timestamp: Option<Timestamp>,
    write_size: u64,
    #[new(default)]
    pessimistic_lock_keys: BTreeSet<Vec<u8>>,
    #[new(default)]
    constraint_check_keys: BTreeSet<Vec<u8>>,
    #[new(default)]
    stashed_assertion: Option<kvrpcpb::AssertionFailed>,
    start_instant: Instant,
}

impl<PdC: PdClient> Clone for Committer<PdC> {
    fn clone(&self) -> Self {
        Self {
            primary_key: self.primary_key.clone(),
            mutations: self.mutations.clone(),
            start_version: self.start_version.clone(),
            rpc: self.rpc.clone(),
            options: self.options.clone(),
            settings: self.settings.clone(),
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
            rpc_interceptor: self.rpc_interceptor.clone(),
            resource_group_name: self.resource_group_name.clone(),
            resource_control: self.resource_control.clone(),
            ru_details: self.ru_details.clone(),
            lock_resolver_context: self.lock_resolver_context.clone(),
            pipelined_state: self.pipelined_state.clone(),
            undetermined: self.undetermined,
            committed: self.committed,
            binlog_skipped: self.binlog_skipped,
            tried_async_commit: self.tried_async_commit,
            tried_one_pc: self.tried_one_pc,
            min_commit_ts: self.min_commit_ts.clone(),
            max_commit_ts: self.max_commit_ts,
            txn_file_chunks: self.txn_file_chunks.clone(),
            txn_file_commit_timestamp: self.txn_file_commit_timestamp.clone(),
            write_size: self.write_size,
            pessimistic_lock_keys: self.pessimistic_lock_keys.clone(),
            constraint_check_keys: self.constraint_check_keys.clone(),
            stashed_assertion: self.stashed_assertion.clone(),
            start_instant: self.start_instant,
        }
    }
}

impl<PdC: PdClient> Committer<PdC> {
    fn with_pessimistic_lock_keys(mut self, keys: BTreeSet<Vec<u8>>) -> Self {
        self.pessimistic_lock_keys = keys;
        self
    }

    fn with_constraint_check_keys(mut self, keys: BTreeSet<Vec<u8>>) -> Self {
        self.constraint_check_keys = keys;
        self
    }

    fn with_stashed_assertion(mut self, assertion: Option<kvrpcpb::AssertionFailed>) -> Self {
        self.stashed_assertion = assertion;
        self
    }

    async fn commit(mut self) -> Result<Option<Timestamp>> {
        let result = self.execute_commit().await;

        if result.is_err() && !self.committed && !self.undetermined {
            if self.txn_file_chunks.is_empty() {
                let cleanup = self.clone();
                let cleanup_start_timestamp = cleanup.start_version.version();
                let hooks = self.settings.lifecycle_hooks.clone();
                tokio::spawn(async move {
                    if let Some(pre) = hooks.pre {
                        pre();
                    }
                    if let Err(error) = cleanup.rollback(true).await {
                        warn!(
                            "failed to clean up transaction after commit error, start_ts: {}, error: {}",
                            cleanup_start_timestamp,
                            error
                        );
                    }
                    if let Some(post) = hooks.post {
                        post();
                    }
                });
            } else {
                let chunks = self.txn_file_chunks.clone();
                if let Err(error) = self
                    .execute_txn_file_action(&chunks, TxnFileAction::Rollback)
                    .await
                {
                    warn!(
                        "failed to clean up transaction after commit error, start_ts: {}, error: {}",
                        self.start_version.version(),
                        error
                    );
                }
            }
        }

        if let Some(binlog) = &self.settings.binlog {
            if self.binlog_skipped {
                binlog.skip();
            } else {
                let commit_timestamp = result
                    .as_ref()
                    .ok()
                    .and_then(|timestamp| timestamp.as_ref())
                    .map_or(0, |timestamp| timestamp.version() as i64);
                binlog
                    .commit(crate::async_util::Cancellation::default(), commit_timestamp)
                    .await;
            }
        }

        if let Some(callback) = &self.settings.commit_callback {
            let commit_mode = if self.options.try_one_pc {
                "1pc"
            } else if self.options.async_commit {
                "async_commit"
            } else {
                "2pc"
            };
            let error = result.as_ref().err().map(ToString::to_string);
            let mut info = serde_json::json!({
                "txn_scope": self.settings.scope,
                "start_ts": self.start_version.version(),
                "commit_ts": result.as_ref().ok().and_then(|timestamp| timestamp.as_ref()).map_or(0, TimestampExt::version),
                "txn_commit_mode": commit_mode,
                "async_commit_fallback": self.tried_async_commit && !self.options.async_commit,
                "one_pc_fallback": self.tried_one_pc && !self.options.try_one_pc,
                "pipelined": self.settings.pipelined.enable,
                "flush_wait_ms": self.pipelined_state.flush_wait_duration.as_millis() as i64,
            });
            if let Some(error) = &error {
                info.as_object_mut()
                    .expect("transaction info is an object")
                    .insert("error".to_owned(), serde_json::Value::String(error.clone()));
            }
            callback(info.to_string(), error);
        }

        result
    }

    async fn execute_commit(&mut self) -> Result<Option<Timestamp>> {
        debug!(
            "committing (2pc), start_ts: {}",
            self.start_version.version()
        );

        if self.settings.pipelined.enable {
            return self.execute_pipelined_commit().await;
        }
        if self.should_use_txn_file() {
            return self.execute_txn_file().await;
        }
        let stashed_assertion_error: Option<Error> =
            if let Some(assertion) = self.stashed_assertion.clone() {
                self.options.async_commit = false;
                self.options.try_one_pc = false;
                Some(match self.get_timestamp_for_commit().await {
                    Ok(timestamp) => match self.check_schema_valid(timestamp.version()) {
                        Ok(()) => crate::error::AssertionFailedError {
                            assertion_failed: assertion,
                        }
                        .into(),
                        Err(error) => error,
                    },
                    Err(error) => error,
                })
            } else {
                None
            };
        self.configure_commit_protocols();
        self.min_commit_ts
            .elevate_write_access(WriteAccessLevel::TwoPc);
        if (self.options.async_commit || self.options.try_one_pc)
            && (!self.settings.causal_consistency || self.settings.commit_wait_until_tso > 0)
        {
            let latest = self.get_timestamp_for_commit().await?.version();
            self.min_commit_ts
                .try_update(latest.saturating_add(1), WriteAccessLevel::TwoPc);
        }
        if self.options.async_commit || self.options.try_one_pc {
            self.calculate_max_commit_ts()?;
        }
        self.pre_split_large_transaction_regions().await;
        let binlog = self.settings.binlog.clone();
        let primary = self.primary_key.as_ref().unwrap().clone();
        let binlog_prewrite = async move {
            match binlog {
                Some(binlog) => Some(
                    binlog
                        .prewrite(crate::async_util::Cancellation::default(), primary.as_ref())
                        .await,
                ),
                None => None,
            }
        };
        let (prewrite_result, binlog_result) = tokio::join!(self.prewrite(), binlog_prewrite);
        if let Some(binlog_result) = binlog_result {
            self.binlog_skipped = binlog_result.skipped();
            if let Some(error) = binlog_result.get_error() {
                return Err(Error::StringError(error.to_string()));
            }
        }
        let min_commit_ts = prewrite_result?;

        fail_point!("after-prewrite", |_| {
            Err(Error::StringError(
                "failpoint: after-prewrite return error".to_owned(),
            ))
        });

        if let Some(error) = stashed_assertion_error {
            return Err(error);
        }

        // `CheckNotExists` verifies delete-your-write constraints during
        // prewrite but creates no lock and must not appear in commit RPCs.
        self.mutations.retain(|mutation| {
            kvrpcpb::Op::try_from(mutation.op) != Ok(kvrpcpb::Op::CheckNotExists)
        });

        // If we didn't use 1pc, prewrite will set `try_one_pc` to false.
        if self.options.try_one_pc {
            self.committed = true;
            return Ok(min_commit_ts);
        }

        let commit_ts = if self.options.async_commit {
            min_commit_ts.expect("async commit prewrite returned a minimum commit timestamp")
        } else if self.mutations.is_empty() {
            let commit_timestamp = self.get_timestamp_for_commit().await?;
            self.validate_commit_timestamp(&commit_timestamp)?;
            self.check_schema_valid(commit_timestamp.version())?;
            commit_timestamp
        } else {
            match self.commit_primary_with_retry().await {
                Ok(commit_ts) => commit_ts,
                Err(e) => {
                    return if self.undetermined {
                        Err(Error::UndeterminedError(Box::new(e)))
                    } else {
                        Err(e)
                    };
                }
            }
        };
        self.committed = true;
        if self.mutations.is_empty() {
            return Ok(Some(commit_ts));
        }
        let secondary = self.clone();
        let secondary_commit_ts = commit_ts.clone();
        let hooks = self.settings.lifecycle_hooks.clone();
        tokio::spawn(async move {
            if let Some(pre) = hooks.pre {
                pre();
            }
            if let Err(error) = secondary.commit_secondary(secondary_commit_ts).await {
                log::warn!("Failed to commit secondary keys: {}", error);
            }
            if let Some(post) = hooks.post {
                post();
            }
        });
        Ok(Some(commit_ts))
    }

    async fn execute_pipelined_commit(&mut self) -> Result<Option<Timestamp>> {
        if self.mutations.iter().any(|mutation| {
            matches!(
                kvrpcpb::Op::try_from(mutation.op),
                Ok(kvrpcpb::Op::SharedLock | kvrpcpb::Op::SharedPessimisticLock)
            )
        }) {
            return Err(Error::StringError(
                "shared lock is not supported in pipelined transaction".to_owned(),
            ));
        }
        let pending = self
            .mutations
            .iter()
            .filter(|mutation| {
                self.pipelined_state.flushed_mutations.get(&mutation.key) != Some(*mutation)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !pending.is_empty() {
            let generation = self.pipelined_state.generation.saturating_add(1);
            self.flush_pipelined_generation(pending, generation).await?;
        }
        if self.pipelined_state.range_start.is_none() || self.pipelined_state.range_end.is_none() {
            return Err(Error::StringError(
                "unexpected empty pipelinedStart or pipelinedEnd".to_owned(),
            ));
        }

        let commit_timestamp = self.get_timestamp_for_commit().await?;
        self.check_schema_valid(commit_timestamp.version())?;
        self.validate_commit_timestamp(&commit_timestamp)?;
        let mut request = new_commit_request(
            std::iter::once(self.primary_key.clone().ok_or(Error::NoPrimaryKey)?),
            self.start_version.clone(),
            commit_timestamp.clone(),
        );
        self.settings
            .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
        request
            .context
            .get_or_insert_with(kvrpcpb::Context::default)
            .request_source = "external_pdml".to_owned();
        plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            self.resource_group_name.as_deref(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
        )
        .priority(self.options.priority)
        .retry_multi_region(self.options.retry_options.region_backoff.clone())
        .extract_error()
        .plan()
        .execute()
        .await?;
        self.committed = true;

        let secondary = self.clone();
        let secondary_commit_timestamp = commit_timestamp.clone();
        let hooks = self.settings.lifecycle_hooks.clone();
        tokio::spawn(async move {
            if let Some(pre) = hooks.pre {
                pre();
            }
            if let Err(error) = secondary
                .finish_pipelined_locks(secondary_commit_timestamp.version())
                .await
            {
                warn!("failed to resolve pipelined transaction locks: {error}");
            }
            if let Some(post) = hooks.post {
                post();
            }
        });
        Ok(Some(commit_timestamp))
    }

    async fn flush_pipelined_generation(
        &mut self,
        mutations: Vec<kvrpcpb::Mutation>,
        generation: u64,
    ) -> Result<()> {
        let primary = self.primary_key.clone().ok_or_else(|| {
            Error::StringError(
                "[pipelined dml] primary key should be set before pipelined flush".to_owned(),
            )
        })?;
        let mut request = new_flush_request(
            mutations.clone(),
            primary,
            self.start_version.clone(),
            Timestamp::from_version(self.start_version.version().saturating_add(1)),
            generation,
            MAX_TTL.max(DEFAULT_LOCK_TTL),
        );
        request.assertion_level = self.settings.assertion_level as i32;
        self.settings
            .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
        request
            .context
            .get_or_insert_with(kvrpcpb::Context::default)
            .request_source = "external_pdml".to_owned();
        let started = Instant::now();
        let size = mutations.iter().fold(0_usize, |total, mutation| {
            total.saturating_add(mutation.key.len() + mutation.value.len())
        });
        plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            self.resource_group_name.as_deref(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
        )
        .priority(self.options.priority)
        .resolve_lock_with_context(
            self.start_version.clone(),
            self.options.retry_options.lock_backoff.clone(),
            self.keyspace,
            self.lock_resolver_context.clone(),
        )
        .retry_multi_region_with_concurrency(
            self.options.retry_options.region_backoff.clone(),
            self.settings.pipelined.flush_concurrency.max(1),
        )
        .merge(CollectError)
        .extract_error()
        .plan()
        .execute()
        .await?;
        crate::stats::observe_pipelined_flush(mutations.len(), size, started.elapsed());
        let first_key = mutations.first().map(|mutation| mutation.key.clone());
        let last_key = mutations.last().map(|mutation| mutation.key.clone());
        for mutation in mutations {
            self.pipelined_state
                .flushed_mutations
                .insert(mutation.key.clone(), mutation);
        }
        self.pipelined_state.generation = generation;
        if let Some(first_key) = first_key {
            if self
                .pipelined_state
                .range_start
                .as_ref()
                .is_none_or(|current| first_key < *current)
            {
                self.pipelined_state.range_start = Some(first_key);
            }
        }
        if let Some(last_key) = last_key {
            if self
                .pipelined_state
                .range_end
                .as_ref()
                .is_none_or(|current| last_key > *current)
            {
                self.pipelined_state.range_end = Some(last_key);
            }
        }
        Ok(())
    }

    async fn resolve_pipelined_locks(&self, commit_version: u64) -> Result<()> {
        let mut next = self
            .pipelined_state
            .range_start
            .clone()
            .ok_or_else(|| Error::StringError("empty pipelined lock range".to_owned()))?;
        let mut exclusive_end = self
            .pipelined_state
            .range_end
            .clone()
            .ok_or_else(|| Error::StringError("empty pipelined lock range".to_owned()))?;
        exclusive_end.push(0);
        let mut ranges = Vec::new();
        while next < exclusive_end {
            let store = self
                .rpc
                .clone()
                .store_for_key(&Key::from(next.clone()))
                .await?;
            let region_end = store.region_with_leader.region.end_key;
            let end = if region_end.is_empty() || region_end >= exclusive_end {
                exclusive_end.clone()
            } else {
                region_end
            };
            if end <= next {
                return Err(Error::StringError(
                    "pipelined resolve-lock region does not advance".to_owned(),
                ));
            }
            ranges.push((next, end.clone()));
            next = end;
        }
        let concurrency = self.settings.pipelined.resolve_lock_concurrency.max(1);
        stream::iter(ranges.into_iter().map(|(start, end)| async move {
            self.resolve_pipelined_lock_range(start, end, commit_version)
                .await
        }))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
        Ok(())
    }

    async fn resolve_pipelined_lock_range(
        &self,
        mut next: Vec<u8>,
        exclusive_end: Vec<u8>,
        commit_version: u64,
    ) -> Result<()> {
        let mut backoff = self.options.retry_options.region_backoff.clone();
        while next < exclusive_end {
            let store = self
                .rpc
                .clone()
                .store_for_key(&Key::from(next.clone()))
                .await?;
            let mut request =
                new_resolve_lock_request(self.start_version.version(), commit_version, false);
            self.settings
                .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
            request
                .context
                .get_or_insert_with(kvrpcpb::Context::default)
                .request_source = "external_pdml".to_owned();
            let response = plan_with_keyspace_name(
                self.rpc.clone(),
                self.keyspace,
                self.keyspace_name.as_deref(),
                self.rpc_interceptor.clone(),
                self.resource_group_name.as_deref(),
                self.resource_control.clone(),
                self.ru_details.clone(),
                ReplicaReadConfig::default(),
                request,
            )
            .priority(self.options.priority)
            .single_region_with_store(store.clone())
            .await?
            .plan()
            .execute()
            .await?;
            if response.region_error.is_some() {
                self.rpc
                    .invalidate_region_cache(store.region_with_leader.ver_id())
                    .await;
                let delay = backoff.next_delay_duration().ok_or_else(|| {
                    Error::StringError("pipelined resolve-lock retry exhausted".to_owned())
                })?;
                tokio::time::sleep(delay).await;
                continue;
            }
            if let Some(error) = response.error {
                return Err(Error::KeyError(Box::new(error)));
            }
            let region_end = store.region_with_leader.region.end_key;
            if region_end.is_empty() || region_end >= exclusive_end {
                return Ok(());
            }
            if region_end <= next {
                return Err(Error::StringError(
                    "pipelined resolve-lock region does not advance".to_owned(),
                ));
            }
            next = region_end;
        }
        Ok(())
    }

    async fn broadcast_pipelined_status(
        &self,
        commit_version: u64,
        is_completed: bool,
    ) -> Result<()> {
        let status = kvrpcpb::TxnStatus {
            start_ts: self.start_version.version(),
            min_commit_ts: (!is_completed && commit_version == 0)
                .then(|| self.min_commit_ts.get())
                .unwrap_or_default(),
            commit_ts: commit_version,
            rolled_back: commit_version == 0,
            is_completed,
        };
        let mut request = kvrpcpb::BroadcastTxnStatusRequest {
            txn_status: vec![status],
            ..Default::default()
        };
        self.settings
            .apply_request(&mut request, Duration::from_secs(5));
        plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            self.resource_group_name.as_deref(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
        )
        .all_stores(DEFAULT_STORE_BACKOFF)
        .merge(Collect)
        .plan()
        .execute()
        .await
    }

    async fn finish_pipelined_locks(&self, commit_version: u64) -> Result<()> {
        if let Err(error) = self.broadcast_pipelined_status(commit_version, false).await {
            warn!("broadcast pipelined transaction status failed: {error}");
        }
        self.resolve_pipelined_locks(commit_version).await?;
        if let Err(error) = self.broadcast_pipelined_status(commit_version, true).await {
            warn!("broadcast completed pipelined transaction status failed: {error}");
        }
        Ok(())
    }

    fn should_use_txn_file(&self) -> bool {
        if self.settings.txn_file_disabled
            || self.settings.variables.disable_txn_file
            || self.settings.binlog.is_some()
            || matches!(self.options.kind, TransactionKind::Pessimistic(_))
            || self.settings.pipelined.enable
        {
            return false;
        }
        let config = crate::config::get_global_config();
        if config.tikv_client.txn_chunk_writer_addr.is_empty() {
            return false;
        }
        let mut minimum_size = self.settings.variables.txn_file_min_mutation_size;
        if minimum_size == 0 {
            minimum_size = config.tikv_client.txn_file_min_mutation_size;
        }
        if self.settings.request_source.internal {
            minimum_size /= 2;
        }
        if self.write_size < minimum_size as u64
            || !request_source_allows_txn_file(
                &self.settings.request_source,
                &config.tikv_client.txn_file_request_source_whitelist,
            )
            || self.mutations.iter().any(|mutation| {
                matches!(
                    kvrpcpb::Op::try_from(mutation.op),
                    Ok(kvrpcpb::Op::SharedLock | kvrpcpb::Op::SharedPessimisticLock)
                )
            })
        {
            return false;
        }
        self.settings.assertion_level == kvrpcpb::AssertionLevel::Off
            || self
                .mutations
                .iter()
                .all(|mutation| mutation.assertion == kvrpcpb::Assertion::None as i32)
    }

    async fn execute_txn_file(&mut self) -> Result<Option<Timestamp>> {
        let chunks = build_txn_chunks(
            &self.mutations,
            self.keyspace,
            crate::async_util::Cancellation::default(),
        )
        .await?;
        if chunks.is_empty() {
            return Err(Error::StringError(
                "txn file: chunk writer returned no chunks".to_owned(),
            ));
        }
        self.txn_file_chunks = chunks.clone();
        self.pre_split_txn_file_regions(&chunks).await?;
        self.execute_txn_file_action(&chunks, TxnFileAction::Prewrite)
            .await?;

        let commit_timestamp = self.get_timestamp_for_commit().await?;
        self.check_schema_valid(commit_timestamp.version())?;
        self.validate_commit_timestamp(&commit_timestamp)?;
        self.txn_file_commit_timestamp = Some(commit_timestamp.clone());
        let commit_result = self
            .execute_txn_file_action(&chunks, TxnFileAction::Commit)
            .await;
        self.normalize_txn_file_commit_result(commit_result)?;
        self.committed = true;
        Ok(Some(commit_timestamp))
    }

    fn normalize_txn_file_commit_result<T>(&self, result: Result<T>) -> Result<T> {
        result.map_err(|error| {
            if self.undetermined {
                Error::UndeterminedError(Box::new(error))
            } else {
                error
            }
        })
    }

    async fn pre_split_txn_file_regions(&self, chunks: &TxnChunkSlice) -> Result<()> {
        let batches = chunks.group_to_batches(&self.rpc, &self.mutations).await?;
        let mut pending = txn_file_pre_split_keys(&batches);
        if pending.is_empty() {
            return Ok(());
        }
        let mut backoff = self.options.retry_options.region_backoff.clone();
        while !pending.is_empty() {
            let mut grouped = BTreeMap::<u64, (crate::store::RegionStore, Vec<Vec<u8>>)>::new();
            for key in pending.drain(..) {
                let store = self
                    .rpc
                    .clone()
                    .store_for_key(&Key::from(key.clone()))
                    .await?;
                // A key equal to the region start was already split by an
                // earlier retry and is deliberately filtered by client-go.
                if store.region_with_leader.region.start_key == key {
                    continue;
                }
                grouped
                    .entry(store.region_with_leader.id())
                    .or_insert_with(|| (store.clone(), Vec::new()))
                    .1
                    .push(key);
            }
            let mut retry_keys = Vec::new();
            for (_, (store, keys)) in grouped {
                let mut request =
                    crate::transaction::requests::new_split_region_request(keys.clone(), false);
                self.settings
                    .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_SHORT);
                let response = plan_with_keyspace_name(
                    self.rpc.clone(),
                    self.keyspace,
                    self.keyspace_name.as_deref(),
                    self.rpc_interceptor.clone(),
                    self.resource_group_name.as_deref(),
                    self.resource_control.clone(),
                    self.ru_details.clone(),
                    ReplicaReadConfig::default(),
                    request,
                )
                .priority(self.options.priority)
                .single_region_with_store(store.clone())
                .await?
                .plan()
                .execute()
                .await?;
                if response.region_error.is_some() {
                    self.rpc
                        .invalidate_region_cache(store.region_with_leader.ver_id())
                        .await;
                    retry_keys.extend(keys);
                    continue;
                }
                if !response.errors.is_empty() {
                    let mut locks = Vec::new();
                    for key_error in response.errors {
                        let extracted =
                            crate::transaction::extract_locks_from_key_error(&key_error)
                                .map_err(|_| Error::KeyError(Box::new(key_error)))?;
                        locks.extend(extracted);
                    }
                    let resolution = crate::transaction::resolve_locks_with_context_result(
                        locks,
                        Timestamp::from_version(u64::MAX),
                        self.rpc.clone(),
                        self.keyspace,
                        self.keyspace_name.as_deref(),
                        self.lock_resolver_context.clone(),
                    )
                    .await?;
                    if resolution.ms_before_expired > 0 {
                        tokio::time::sleep(Duration::from_millis(
                            resolution.ms_before_expired as u64,
                        ))
                        .await;
                    }
                    retry_keys.extend(keys);
                }
            }
            if retry_keys.is_empty() {
                return Ok(());
            }
            let delay = backoff.next_delay_duration().ok_or_else(|| {
                Error::StringError("txn file: pre-split retry exhausted".to_owned())
            })?;
            tokio::time::sleep(delay).await;
            pending = retry_keys;
        }
        Ok(())
    }

    async fn execute_txn_file_action(
        &mut self,
        chunks: &TxnChunkSlice,
        action: TxnFileAction,
    ) -> Result<()> {
        let mut region_backoff = self.options.retry_options.region_backoff.clone();
        loop {
            let mut batches = chunks.group_to_batches(&self.rpc, &self.mutations).await?;
            let primary_index = self.txn_file_primary_batch_index(&batches)?;
            let mut primary_batch = batches.remove(primary_index);
            primary_batch.is_primary = true;
            if self.execute_txn_file_batch(&primary_batch, action).await? {
                self.rpc
                    .invalidate_region_cache(primary_batch.region.ver_id())
                    .await;
                let delay = region_backoff.next_delay_duration().ok_or_else(|| {
                    Error::StringError("txn file: primary region retry exhausted".to_owned())
                })?;
                tokio::time::sleep(delay).await;
                continue;
            }
            if action == TxnFileAction::Commit {
                self.committed = true;
            }
            if batches.is_empty() {
                return Ok(());
            }

            if action == TxnFileAction::Prewrite {
                return self
                    .execute_txn_file_slice_with_retry(chunks.clone(), Some(batches), action)
                    .await;
            }

            let mut secondary = self.clone();
            let secondary_chunks = chunks.clone();
            let hooks = self.settings.lifecycle_hooks.clone();
            tokio::spawn(async move {
                if let Some(pre) = hooks.pre {
                    pre();
                }
                if let Err(error) = secondary
                    .execute_txn_file_slice_with_retry(secondary_chunks, Some(batches), action)
                    .await
                {
                    warn!("txn file secondary failed: {error}");
                }
                if let Some(post) = hooks.post {
                    post();
                }
            });
            return Ok(());
        }
    }

    fn txn_file_primary_batch_index(&self, batches: &[ChunkBatch]) -> Result<usize> {
        let primary = self.primary_key.as_ref().ok_or(Error::NoPrimaryKey)?;
        batches
            .iter()
            .position(|batch| batch.region.contains(primary))
            .ok_or_else(|| Error::StringError("txn file: primary out of batches".to_owned()))
    }

    async fn execute_txn_file_slice_with_retry(
        &mut self,
        mut chunks: TxnChunkSlice,
        mut batches: Option<Vec<ChunkBatch>>,
        action: TxnFileAction,
    ) -> Result<()> {
        let mut region_backoff = self.options.retry_options.region_backoff.clone();
        loop {
            let current_batches = match batches.take() {
                Some(batches) => batches,
                None => chunks.group_to_batches(&self.rpc, &self.mutations).await?,
            };
            if current_batches.is_empty() {
                return Ok(());
            }
            let config = crate::config::get_global_config();
            let concurrency = current_batches
                .len()
                .min(config.committer_concurrency.max(1))
                .min(txn_file_max_chunks_in_parallel(
                    config.tikv_client.txn_chunk_max_size,
                ));
            let template = self.clone();
            let mut results = stream::iter(current_batches.into_iter().map(move |batch| {
                let mut committer = template.clone();
                async move {
                    let retry = committer.execute_txn_file_batch(&batch, action).await;
                    (batch, retry)
                }
            }))
            .buffer_unordered(concurrency);

            let mut region_error_chunks = TxnChunkSlice::default();
            let mut first_error = None;
            while let Some((batch, result)) = results.next().await {
                match result {
                    Ok(false) => {}
                    Ok(true) => {
                        self.rpc
                            .invalidate_region_cache(batch.region.ver_id())
                            .await;
                        region_error_chunks.append(&batch.chunks);
                    }
                    Err(error) if action == TxnFileAction::Prewrite => return Err(error),
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }
            if let Some(error) = first_error {
                return Err(error);
            }
            region_error_chunks.sort_and_dedup();
            if region_error_chunks.is_empty() {
                return Ok(());
            }
            let delay = region_backoff.next_delay_duration().ok_or_else(|| {
                Error::StringError("txn file: secondary region retry exhausted".to_owned())
            })?;
            tokio::time::sleep(delay).await;
            chunks = region_error_chunks;
        }
    }

    async fn execute_txn_file_batch(
        &mut self,
        batch: &ChunkBatch,
        action: TxnFileAction,
    ) -> Result<bool> {
        match action {
            TxnFileAction::Prewrite => self.prewrite_txn_file_batch(batch).await,
            TxnFileAction::Commit => self.commit_txn_file_batch(batch).await,
            TxnFileAction::Rollback => self.rollback_txn_file_batch(batch).await,
        }
    }

    async fn prewrite_txn_file_batch(&mut self, batch: &ChunkBatch) -> Result<bool> {
        loop {
            let mut request = new_prewrite_request(
                Vec::new(),
                self.primary_key.clone().ok_or(Error::NoPrimaryKey)?,
                self.start_version.clone(),
                self.calc_txn_lock_ttl() + self.start_instant.elapsed().as_millis() as u64,
            );
            request.assertion_level = kvrpcpb::AssertionLevel::Off as i32;
            request.txn_file_chunks.clone_from(&batch.chunks.chunk_ids);
            request.txn_size = batch.transaction_size();
            self.settings.apply_txn_file_prewrite(
                &mut request,
                &batch.first_key,
                SNAPSHOT_READ_TIMEOUT_MEDIUM,
            );
            let store = self
                .rpc
                .clone()
                .store_for_key(&Key::from(batch.first_key.clone()))
                .await?;
            let response = plan_with_keyspace_name(
                self.rpc.clone(),
                self.keyspace,
                self.keyspace_name.as_deref(),
                self.rpc_interceptor.clone(),
                self.resource_group_name.as_deref(),
                self.resource_control.clone(),
                self.ru_details.clone(),
                ReplicaReadConfig::default(),
                request,
            )
            .priority(self.options.priority)
            .single_region_with_store(store)
            .await?
            .plan()
            .execute()
            .await?;
            if response.region_error.is_some() {
                return Ok(true);
            }
            if response.errors.is_empty() {
                return Ok(false);
            }
            let mut locks = Vec::new();
            for key_error in response.errors {
                let extracted = crate::transaction::extract_locks_from_key_error(&key_error);
                let Ok(mut extracted) = extracted else {
                    return Err(Error::KeyError(Box::new(key_error)));
                };
                if let Some(lock) = extracted.iter().find(|lock| {
                    self.settings.prewrite_lock_policy == PrewriteEncounterLockPolicy::NoResolve
                        || (lock.lock_version > self.start_version.version()
                            && matches!(self.options.kind, TransactionKind::Optimistic))
                }) {
                    return Err(crate::error::new_write_conflict_with_args(
                        self.start_version.version(),
                        lock.lock_version,
                        0,
                        lock.key.clone(),
                        kvrpcpb::write_conflict::Reason::Optimistic,
                    )
                    .into());
                }
                locks.append(&mut extracted);
            }
            let resolution = crate::transaction::resolve_locks_with_context_result(
                locks,
                self.start_version.clone(),
                self.rpc.clone(),
                self.keyspace,
                self.keyspace_name.as_deref(),
                self.lock_resolver_context.clone(),
            )
            .await?;
            if resolution.ms_before_expired > 0 {
                tokio::time::sleep(Duration::from_millis(resolution.ms_before_expired as u64))
                    .await;
            }
        }
    }

    async fn commit_txn_file_batch(&mut self, batch: &ChunkBatch) -> Result<bool> {
        loop {
            let commit_timestamp = self.txn_file_commit_timestamp.clone().ok_or_else(|| {
                Error::StringError("txn file: commit TS is not prepared".to_owned())
            })?;
            let keys = batch.sample_data_keys.iter().cloned().map(Key::from);
            let mut request =
                new_commit_request(keys, self.start_version.clone(), commit_timestamp);
            request.is_txn_file = true;
            self.settings
                .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
            let store = self
                .rpc
                .clone()
                .store_for_key(&Key::from(batch.first_key.clone()))
                .await?;
            let result = plan_with_keyspace_name(
                self.rpc.clone(),
                self.keyspace,
                self.keyspace_name.as_deref(),
                self.rpc_interceptor.clone(),
                self.resource_group_name.as_deref(),
                self.resource_control.clone(),
                self.ru_details.clone(),
                ReplicaReadConfig::default(),
                request,
            )
            .priority(self.options.priority)
            .single_region_with_store(store)
            .await?
            .plan()
            .execute()
            .await;
            let response = match result {
                Ok(response) => response,
                Err(error) => {
                    if batch.is_primary {
                        self.undetermined = matches!(
                            error,
                            Error::Grpc(_)
                                | Error::GrpcAPI(_)
                                | Error::Connection { .. }
                                | Error::Channel(_)
                        );
                    }
                    return Err(error);
                }
            };
            if let Some(region_error) = response.region_error {
                if batch.is_primary && region_error.undetermined_result.is_some() {
                    self.undetermined = true;
                    return Err(Error::RegionError(Box::new(region_error)));
                }
                return Ok(true);
            }
            if batch.is_primary {
                self.undetermined = false;
            }
            let Some(key_error) = response.error else {
                return Ok(false);
            };
            if let Some(expired) = key_error.commit_ts_expired.as_ref() {
                let primary = self.primary_key.as_ref().ok_or(Error::NoPrimaryKey)?;
                if !batch.is_primary || expired.key.as_slice() != <&[u8]>::from(primary) {
                    return Err(Error::StringError(
                        "2PC commitTS rejected by TiKV, but the key is not the primary key"
                            .to_owned(),
                    ));
                }
                if expired
                    .min_commit_ts
                    .saturating_sub(expired.attempted_commit_ts)
                    > 943_718_400_000
                {
                    return Err(Error::StringError(format!(
                        "2PC min_commit_ts is too large, we got min_commit_ts: {}, and attempted_commit_ts: {}",
                        expired.min_commit_ts, expired.attempted_commit_ts
                    )));
                }
                let commit_timestamp = self.get_timestamp_for_commit().await?;
                self.check_schema_valid(commit_timestamp.version())?;
                self.validate_commit_timestamp(&commit_timestamp)?;
                self.txn_file_commit_timestamp = Some(commit_timestamp);
                continue;
            }
            return Err(Error::KeyError(Box::new(key_error)));
        }
    }

    async fn rollback_txn_file_batch(&mut self, batch: &ChunkBatch) -> Result<bool> {
        let keys = batch.sample_data_keys.iter().cloned().map(Key::from);
        let mut request = new_batch_rollback_request(keys, self.start_version.clone());
        request.is_txn_file = true;
        self.settings
            .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_SHORT);
        let store = self
            .rpc
            .clone()
            .store_for_key(&Key::from(batch.first_key.clone()))
            .await?;
        let response = plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            self.resource_group_name.as_deref(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
        )
        .priority(self.options.priority)
        .single_region_with_store(store)
        .await?
        .plan()
        .execute()
        .await?;
        if response.region_error.is_some() {
            return Ok(true);
        }
        if let Some(error) = response.error {
            return Err(Error::KeyError(Box::new(error)));
        }
        Ok(false)
    }

    fn configure_commit_protocols(&mut self) {
        let global_scope = self.settings.scope == crate::oracle::GLOBAL_TXN_SCOPE;
        let has_commit_upper_bound = self.settings.commit_timestamp_upper_bound.is_some();
        let has_binlog = self.settings.binlog.is_some();
        let has_shared_locks = self.mutations.iter().any(|mutation| {
            matches!(
                kvrpcpb::Op::try_from(mutation.op),
                Ok(kvrpcpb::Op::SharedLock | kvrpcpb::Op::SharedPessimisticLock)
            )
        });
        if !global_scope
            || has_commit_upper_bound
            || has_binlog
            || self.settings.pipelined.enable
            || has_shared_locks
        {
            self.options.async_commit = false;
            self.options.try_one_pc = false;
            return;
        }

        if self.options.async_commit {
            let config = crate::config::get_global_config();
            let async_commit = config.tikv_client.async_commit;
            let key_bytes = self
                .mutations
                .iter()
                .try_fold(0_u64, |total, mutation| {
                    total.checked_add(mutation.key.len() as u64)
                })
                .unwrap_or(u64::MAX);
            if self.mutations.len() as u64 > async_commit.keys_limit
                || key_bytes > async_commit.total_key_size_limit
            {
                self.options.async_commit = false;
            }
        }
        self.tried_async_commit |= self.options.async_commit;
        self.tried_one_pc |= self.options.try_one_pc;
    }

    async fn get_timestamp_for_commit(&self) -> Result<Timestamp> {
        let first = self.rpc.clone().get_timestamp().await?;
        let expected = self.settings.commit_wait_until_tso;
        if expected == 0 || first.version() > expected {
            return Ok(first);
        }
        let timeout = self.settings.commit_wait_until_tso_timeout;
        if timeout.is_zero() {
            return Err(Error::CommitTimestampLag {
                message: format!(
                    "PD TSO '{}' lags the expected timestamp '{}', fail immediately since zero max sleep time is set",
                    first.version(), expected
                ),
                source: crate::error::StaticError::CommitTimestampLag,
            });
        }

        let drift_ms = crate::oracle::extract_physical(expected)
            .saturating_sub(crate::oracle::extract_physical(first.version()));
        if drift_ms > timeout.as_millis().try_into().unwrap_or(i64::MAX) {
            return Err(Error::CommitTimestampLag {
                message: format!(
                    "PD TSO '{}' lags the expected timestamp '{}', clock drift {}ms exceeds maximum allowed timeout {:?}",
                    first.version(), expected, drift_ms, timeout
                ),
                source: crate::error::StaticError::CommitTimestampLag,
            });
        }

        let mut backoffer = RetryBackoffer::new(
            crate::async_util::Cancellation::default(),
            timeout.as_millis().try_into().unwrap_or(u64::MAX),
        );
        let mut attempts = 1_u64;
        let mut last = first.clone();
        while last.version() <= expected {
            if backoffer
                .backoff(BO_COMMIT_TS_LAG, "clock drift from the upstream cluster")
                .await
                .is_err()
            {
                return Err(Error::CommitTimestampLag {
                    message: format!(
                        "PD TSO '{}' lags the expected timestamp '{}', retry timeout: {:?}, attempts: {}, last attempted commit TS: {}",
                        first.version(), expected, timeout, attempts, last.version()
                    ),
                    source: crate::error::StaticError::CommitTimestampLag,
                });
            }
            attempts = attempts.saturating_add(1);
            last = self.rpc.clone().get_timestamp().await?;
        }
        Ok(last)
    }

    fn validate_commit_timestamp(&self, commit_timestamp: &Timestamp) -> Result<()> {
        if self.start_instant.elapsed() > Duration::from_millis(24 * 60 * 60 * 1_000) {
            return Err(Error::StringError(format!(
                "session {} txn takes too much time, txnStartTS: {}, comm: {}",
                self.settings.session_id,
                self.start_version.version(),
                commit_timestamp.version()
            )));
        }
        if self
            .settings
            .commit_timestamp_upper_bound
            .as_ref()
            .is_some_and(|check| !check(commit_timestamp.version()))
        {
            return Err(Error::StringError(format!(
                "session {} check commit ts upper bound fail, txnStartTS: {}, comm: {}",
                self.settings.session_id,
                self.start_version.version(),
                commit_timestamp.version()
            )));
        }
        Ok(())
    }

    fn check_schema_valid(&self, check_timestamp: u64) -> Result<()> {
        let (Some(checker), Some(schema_version)) = (
            &self.settings.schema_lease_checker,
            &self.settings.schema_version,
        ) else {
            return Ok(());
        };
        checker
            .check_by_schema_version(check_timestamp, schema_version.as_ref())
            .map(|_| ())
    }

    fn calculate_max_commit_ts(&mut self) -> Result<()> {
        let current_timestamp =
            self.start_version
                .version()
                .saturating_add(crate::oracle::compose_timestamp(
                    self.start_instant.elapsed().as_millis() as i64,
                    0,
                ));
        self.check_schema_valid(current_timestamp)?;
        let safe_window_ms = crate::config::get_global_config()
            .tikv_client
            .async_commit
            .safe_window
            .as_millis() as i64;
        self.max_commit_ts =
            current_timestamp.saturating_add(crate::oracle::compose_timestamp(safe_window_ms, 0));
        Ok(())
    }

    async fn pre_split_large_transaction_regions(&self) {
        let detect_threshold = PRE_SPLIT_DETECT_THRESHOLD.load(atomic::Ordering::Relaxed) as usize;
        let size_threshold = PRE_SPLIT_SIZE_THRESHOLD.load(atomic::Ordering::Relaxed) as usize;
        if detect_threshold == 0 || size_threshold == 0 || self.mutations.len() < detect_threshold {
            return;
        }
        let mut start = 0;
        while start < self.mutations.len() {
            let region = match self
                .rpc
                .region_for_key(&Key::from(self.mutations[start].key.clone()))
                .await
            {
                Ok(region) => region,
                Err(error) => {
                    warn!("2PC large-transaction pre-split lookup failed: {error}");
                    return;
                }
            };
            let mut end = start + 1;
            while end < self.mutations.len()
                && region.contains(&Key::from(self.mutations[end].key.clone()))
            {
                end += 1;
            }
            if end - start >= detect_threshold {
                let mut accumulated = 0_usize;
                let mut split_keys = Vec::new();
                for mutation in &self.mutations[start..end] {
                    accumulated = accumulated
                        .saturating_add(mutation.key.len())
                        .saturating_add(mutation.value.len());
                    if accumulated >= size_threshold {
                        accumulated = 0;
                        split_keys.push(mutation.key.clone());
                    }
                }
                if !split_keys.is_empty() {
                    match self.rpc.clone().split_regions(split_keys, 3).await {
                        Ok(_) => {
                            self.rpc.invalidate_region_cache(region.ver_id()).await;
                        }
                        Err(error) => {
                            warn!(
                                "2PC large-transaction pre-split failed for region {}: {error}",
                                region.id()
                            );
                        }
                    }
                }
            }
            start = end;
        }
    }

    async fn prewrite(&mut self) -> Result<Option<Timestamp>> {
        debug!(
            "prewriting, start_ts: {}, mutations: {}",
            self.start_version.version(),
            self.mutations.len()
        );
        let primary_lock = self.primary_key.clone().unwrap();
        let elapsed = self.start_instant.elapsed().as_millis() as u64;
        let mut lock_ttl = self.calc_txn_lock_ttl().saturating_add(elapsed);
        if self.options.async_commit && matches!(self.options.kind, TransactionKind::Pessimistic(_))
        {
            let safe_ttl = crate::oracle::extract_physical(self.max_commit_ts)
                .saturating_sub(crate::oracle::extract_physical(
                    self.start_version.version(),
                ))
                .saturating_add(1) as u64;
            lock_ttl = lock_ttl.max(safe_ttl);
        }
        let pessimistic_for_update_ts = match &self.options.kind {
            TransactionKind::Pessimistic(for_update_ts)
                if for_update_ts.version() == 0 && !self.constraint_check_keys.is_empty() =>
            {
                self.start_version.clone()
            }
            TransactionKind::Pessimistic(for_update_ts) => for_update_ts.clone(),
            TransactionKind::Optimistic => Timestamp::from_version(0),
        };
        let mut request = match &self.options.kind {
            TransactionKind::Optimistic => new_prewrite_request(
                self.mutations.clone(),
                primary_lock,
                self.start_version.clone(),
                lock_ttl,
            ),
            TransactionKind::Pessimistic(_) => new_pessimistic_prewrite_request(
                self.mutations.clone(),
                primary_lock,
                self.start_version.clone(),
                lock_ttl,
                pessimistic_for_update_ts.clone(),
            ),
        };

        request.use_async_commit = self.options.async_commit;
        request.try_one_pc = self.options.try_one_pc;
        request.assertion_level = self.settings.assertion_level as i32;
        let mut min_commit_ts = self.min_commit_ts.get();
        if matches!(self.options.kind, TransactionKind::Pessimistic(_)) {
            min_commit_ts =
                min_commit_ts.max(pessimistic_for_update_ts.version().saturating_add(1));
        }
        min_commit_ts = min_commit_ts.max(self.start_version.version().saturating_add(1));
        request.min_commit_ts = min_commit_ts;
        request.max_commit_ts = self.max_commit_ts;
        if matches!(self.options.kind, TransactionKind::Pessimistic(_)) {
            request.pessimistic_actions = self
                .mutations
                .iter()
                .map(|mutation| {
                    if self.pessimistic_lock_keys.contains(&mutation.key) {
                        kvrpcpb::prewrite_request::PessimisticAction::DoPessimisticCheck as i32
                    } else if self.constraint_check_keys.contains(&mutation.key) {
                        kvrpcpb::prewrite_request::PessimisticAction::DoConstraintCheck as i32
                    } else {
                        kvrpcpb::prewrite_request::PessimisticAction::SkipPessimisticCheck as i32
                    }
                })
                .collect();
        }
        self.settings
            .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
        request.secondaries = self
            .mutations
            .iter()
            .filter(|mutation| {
                self.primary_key.as_ref().unwrap() != mutation.key.as_ref()
                    && kvrpcpb::Op::try_from(mutation.op) != Ok(kvrpcpb::Op::CheckNotExists)
            })
            .map(|m| m.key.clone())
            .collect();
        let plan = plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            self.resource_group_name.as_deref(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
        )
        .priority(self.options.priority)
        .resolve_lock_with_context(
            self.start_version.clone(),
            self.options.retry_options.lock_backoff.clone(),
            self.keyspace,
            self.lock_resolver_context.clone(),
        )
        .prewrite_lock_conflict(
            self.start_version.version(),
            self.settings.prewrite_lock_policy == PrewriteEncounterLockPolicy::NoResolve,
            matches!(self.options.kind, TransactionKind::Optimistic),
        )
        .retry_multi_region(self.options.retry_options.region_backoff.clone())
        .merge(CollectError)
        .extract_error()
        .plan();
        let response = plan.execute().await.map_err(normalize_prewrite_error)?;

        if self.options.try_one_pc && response.len() == 1 && response[0].one_pc_commit_ts != 0 {
            return Ok(Timestamp::try_from_version(response[0].one_pc_commit_ts));
        }

        if response
            .iter()
            .any(|response| response.one_pc_commit_ts != 0)
        {
            return Err(Error::StringError(
                "non-1PC transaction committed with the 1PC protocol".to_owned(),
            ));
        }
        self.options.try_one_pc = false;

        if self.options.async_commit {
            if response.iter().any(|response| response.min_commit_ts == 0) {
                self.options.async_commit = false;
            } else if let Some(min_commit_ts) =
                response.iter().map(|response| response.min_commit_ts).max()
            {
                self.min_commit_ts
                    .try_update(min_commit_ts, WriteAccessLevel::TwoPc);
            }
        }

        Ok(self
            .options
            .async_commit
            .then(|| Timestamp::from_version(self.min_commit_ts.get())))
    }

    /// Commits the primary key and returns the commit version
    async fn commit_primary(&mut self) -> Result<Timestamp> {
        debug!(
            "committing primary, start_ts: {}",
            self.start_version.version()
        );
        let primary_key = self.primary_key.clone().into_iter();
        let commit_version = self.get_timestamp_for_commit().await?;
        self.validate_commit_timestamp(&commit_version)?;
        self.check_schema_valid(commit_version.version())?;
        let mut req = new_commit_request(
            primary_key,
            self.start_version.clone(),
            commit_version.clone(),
        );
        self.settings
            .apply_request(&mut req, SNAPSHOT_READ_TIMEOUT_MEDIUM);
        let plan = plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            self.resource_group_name.as_deref(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            req,
        )
        .priority(self.options.priority)
        .resolve_lock_with_context(
            self.start_version.clone(),
            self.options.retry_options.lock_backoff.clone(),
            self.keyspace,
            self.lock_resolver_context.clone(),
        )
        .retry_multi_region(self.options.retry_options.region_backoff.clone())
        .extract_error()
        .plan();
        plan.execute()
            .inspect_err(|e| {
                debug!(
                    "commit primary error: {:?}, start_ts: {}",
                    e,
                    self.start_version.version()
                );
                // We don't know whether the transaction is committed or not if we fail to receive
                // the response. Then, we mark the transaction as undetermined and propagate the
                // error to the user.
                if matches!(
                    e,
                    Error::Grpc(_)
                        | Error::GrpcAPI(_)
                        | Error::Connection { .. }
                        | Error::Channel(_)
                ) {
                    self.undetermined = true;
                }
            })
            .await?;

        Ok(commit_version)
    }

    async fn commit_primary_with_retry(&mut self) -> Result<Timestamp> {
        loop {
            match self.commit_primary().await {
                Ok(commit_version) => return Ok(commit_version),
                Err(Error::ExtractedErrors(mut errors)) => match errors.pop() {
                    Some(Error::KeyError(key_err)) => {
                        if let Some(expired) = key_err.commit_ts_expired {
                            // Ref: https://github.com/tikv/client-go/blob/tidb-8.5/txnkv/transaction/commit.go
                            info!("2PC commit_ts rejected by TiKV, retry with a newer commit_ts, start_ts: {}",
                                self.start_version.version());

                            let primary_key = self.primary_key.as_ref().unwrap();
                            if primary_key != expired.key.as_ref() {
                                error!("2PC commit_ts rejected by TiKV, but the key is not the primary key, start_ts: {}, key: {}, primary: {}",
                                    self.start_version.version(), format_key_for_log(&expired.key), format_key_for_log(primary_key));
                                return Err(Error::StringError("2PC commitTS rejected by TiKV, but the key is not the primary key".to_string()));
                            }

                            // Do not retry for a txn which has a too large min_commit_ts.
                            // 3600000 << 18 = 943718400000
                            if expired
                                .min_commit_ts
                                .saturating_sub(expired.attempted_commit_ts)
                                > 943718400000
                            {
                                let msg = format!("2PC min_commit_ts is too large, we got min_commit_ts: {}, and attempted_commit_ts: {}",
                                                     expired.min_commit_ts, expired.attempted_commit_ts);
                                return Err(Error::StringError(msg));
                            }
                            continue;
                        } else {
                            return Err(Error::KeyError(key_err));
                        }
                    }
                    Some(err) => return Err(err),
                    None => unreachable!(),
                },
                Err(err) => return Err(err),
            }
        }
    }

    async fn commit_secondary(self, commit_version: Timestamp) -> Result<()> {
        debug!(
            "committing secondary keys, start_ts: {}, mutations: {}",
            self.start_version.version(),
            self.mutations.len()
        );
        let start_version = self.start_version.clone();
        let mutations_len = self.mutations.len();
        let primary_only = mutations_len == 1;
        #[cfg(not(feature = "integration-tests"))]
        let mutations = self.mutations.into_iter();

        #[cfg(feature = "integration-tests")]
        let mutations = self.mutations.into_iter().take({
            // Truncate mutation to a new length as `percent/100`.
            // Return error when truncate to zero.
            let fp = || -> Result<usize> {
                #[allow(unused_mut)]
                let mut new_len = mutations_len;
                fail_point!("before-commit-secondary", |percent| {
                    let percent = percent.unwrap().parse::<usize>().unwrap();
                    new_len = mutations_len * percent / 100;
                    if new_len == 0 {
                        Err(Error::StringError(
                            "failpoint: before-commit-secondary return error".to_owned(),
                        ))
                    } else {
                        debug!(
                            "failpoint: before-commit-secondary truncate mutation {} -> {}",
                            mutations_len, new_len
                        );
                        Ok(new_len)
                    }
                });
                Ok(new_len)
            };
            fp()?
        });

        let mut req = if self.options.async_commit {
            let keys = mutations.map(|m| m.key.into());
            new_commit_request(keys, start_version.clone(), commit_version)
        } else if primary_only {
            return Ok(());
        } else {
            let primary_key = self.primary_key.unwrap();
            let keys = mutations
                .map(|m| m.key.into())
                .filter(|key| &primary_key != key);
            new_commit_request(keys, start_version.clone(), commit_version)
        };
        self.settings
            .apply_request(&mut req, SNAPSHOT_READ_TIMEOUT_MEDIUM);
        let lock_resolver_context = self.lock_resolver_context;
        let plan = plan_with_keyspace_name(
            self.rpc,
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor,
            self.resource_group_name.as_deref(),
            self.resource_control,
            self.ru_details,
            ReplicaReadConfig::default(),
            req,
        )
        .priority(self.options.priority)
        .resolve_lock_with_context(
            start_version,
            self.options.retry_options.lock_backoff,
            self.keyspace,
            lock_resolver_context,
        )
        .retry_multi_region(self.options.retry_options.region_backoff)
        .extract_error()
        .plan();
        plan.execute().await?;
        Ok(())
    }

    /// Roll back the transaction.
    ///
    /// `prewritten` must be `true` when the transaction has already started
    /// committing (its prewrite may have placed 2PC locks). A pessimistic
    /// transaction that has been prewritten holds `Put`/`Delete` (2PC) locks,
    /// which `PessimisticRollback` cannot remove — it only clears
    /// `LockType::Pessimistic` locks and would silently leave the prewrite locks
    /// behind. Those are rolled back with `BatchRollback` (as the optimistic
    /// path and client-go's commit cleanup do), which rolls back by `start_ts`
    /// regardless of lock type. Only a pessimistic transaction that has *not*
    /// been prewritten (locks still pessimistic) uses the narrower
    /// `PessimisticRollback`.
    async fn rollback(self, prewritten: bool) -> Result<()> {
        debug!(
            "rolling back (2pc), start_ts: {}, prewritten: {}",
            self.start_version.version(),
            prewritten
        );
        fail_point!("before-rollback", |_| {
            Err(Error::StringError(
                "failpoint: before-rollback return error".to_owned(),
            ))
        });
        if self.settings.pipelined.enable {
            if self.pipelined_state.generation == 0 {
                if let Err(error) = self.broadcast_pipelined_status(0, true).await {
                    warn!("broadcast completed pipelined rollback status failed: {error}");
                }
                return Ok(());
            }
            return self.finish_pipelined_locks(0).await;
        }
        if self.options.kind == TransactionKind::Optimistic && !prewritten {
            return Ok(());
        }
        let keys = self
            .mutations
            .into_iter()
            .map(|mutation| mutation.key.into());
        let start_version = self.start_version.clone();
        let lock_backoff = self.options.retry_options.lock_backoff.clone();
        let region_backoff = self.options.retry_options.region_backoff.clone();
        let rpc = self.rpc;
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name;
        let rpc_interceptor = self.rpc_interceptor;
        let resource_group_name = self.resource_group_name;
        let resource_control = self.resource_control;
        let ru_details = self.ru_details;
        let lock_resolver_context = self.lock_resolver_context;
        let priority = self.options.priority;
        match self.options.kind {
            TransactionKind::Pessimistic(for_update_ts) if !prewritten => {
                let mut req =
                    new_pessimistic_rollback_request(keys, start_version.clone(), for_update_ts);
                self.settings
                    .apply_request(&mut req, SNAPSHOT_READ_TIMEOUT_SHORT);
                let plan = plan_with_keyspace_name(
                    rpc,
                    keyspace,
                    keyspace_name.as_deref(),
                    rpc_interceptor,
                    resource_group_name.as_deref(),
                    resource_control,
                    ru_details,
                    ReplicaReadConfig::default(),
                    req,
                )
                .priority(priority)
                .resolve_lock_with_context(
                    start_version,
                    lock_backoff,
                    keyspace,
                    lock_resolver_context,
                )
                .retry_multi_region(region_backoff)
                .extract_error()
                .plan();
                plan.execute().await?;
            }
            // Optimistic, or pessimistic after prewrite: BatchRollback clears
            // both pessimistic and 2PC locks by start_ts.
            _ => {
                let mut req = new_batch_rollback_request(keys, start_version.clone());
                self.settings
                    .apply_request(&mut req, SNAPSHOT_READ_TIMEOUT_SHORT);
                let plan = plan_with_keyspace_name(
                    rpc,
                    keyspace,
                    keyspace_name.as_deref(),
                    rpc_interceptor,
                    resource_group_name.as_deref(),
                    resource_control,
                    ru_details,
                    ReplicaReadConfig::default(),
                    req,
                )
                .priority(priority)
                .resolve_lock_with_context(
                    start_version,
                    lock_backoff,
                    keyspace,
                    lock_resolver_context,
                )
                .retry_multi_region(region_backoff)
                .extract_error()
                .plan();
                plan.execute().await?;
            }
        }
        Ok(())
    }

    fn calc_txn_lock_ttl(&mut self) -> u64 {
        let mut lock_ttl = DEFAULT_LOCK_TTL;
        if self.write_size >= TXN_COMMIT_BATCH_SIZE {
            let size_mb = self.write_size as f64 / 1024.0 / 1024.0;
            lock_ttl = (TTL_FACTOR * size_mb.sqrt()) as u64;
            lock_ttl = lock_ttl.clamp(DEFAULT_LOCK_TTL, MAX_TTL);
        }
        lock_ttl
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
enum TransactionStatus {
    /// The transaction is read-only [`Snapshot`](super::Snapshot), no need to commit or rollback or panic on drop.
    ReadOnly = 0,
    /// The transaction have not been committed or rolled back.
    Active = 1,
    /// The transaction has committed.
    Committed = 2,
    /// The transaction is currently attempting to commit.
    StartedCommit = 3,
    /// The transaction has rolled back.
    Rolledback = 4,
    /// The transaction is currently attempting to roll back.
    StartedRollback = 5,
    /// The transaction has been dropped.
    Dropped = 6,
}

/// Client-go closes a transaction after the first commit/rollback attempt,
/// including error returns. If the operation did not reach its successful
/// terminal state, mark it dropped and stop transaction-owned background work.
struct TransactionAttemptGuard {
    status: Arc<AtomicU8>,
    in_progress: TransactionStatus,
}

impl TransactionAttemptGuard {
    fn new(status: Arc<AtomicU8>, in_progress: TransactionStatus) -> Self {
        Self {
            status,
            in_progress,
        }
    }
}

impl Drop for TransactionAttemptGuard {
    fn drop(&mut self) {
        let _ = self.status.compare_exchange(
            self.in_progress as u8,
            TransactionStatus::Dropped as u8,
            atomic::Ordering::AcqRel,
            atomic::Ordering::Acquire,
        );
    }
}

impl From<u8> for TransactionStatus {
    fn from(num: u8) -> Self {
        match num {
            0 => TransactionStatus::ReadOnly,
            1 => TransactionStatus::Active,
            2 => TransactionStatus::Committed,
            3 => TransactionStatus::StartedCommit,
            4 => TransactionStatus::Rolledback,
            5 => TransactionStatus::StartedRollback,
            6 => TransactionStatus::Dropped,
            _ => panic!("Unknown transaction status {}", num),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_snapshot_commit_ts;
    use super::CheckLevel;
    use super::CommitSettings;
    use super::Committer;
    use super::MinCommitTsManager;
    use super::PipelinedTransactionState;
    use super::PrewriteEncounterLockPolicy;
    use super::TransactionStatus;
    use super::WriteAccessLevel;
    use crate::transaction::txn_file::{ChunkBatch, TxnChunkRange, TxnChunkSlice};
    use crate::transaction::ResolveLocksContext;
    use std::any::Any;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::Once;
    use std::time::Duration;

    use fail::FailScenario;

    use crate::disable_resource_control;
    use crate::enable_resource_control;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::new_rpc_interceptor;
    use crate::oracle::{OracleError, OracleOption, OracleResult, ReadTimestampValidator};
    use crate::proto::kvrpcpb;
    use crate::proto::pdpb::Timestamp;
    use crate::proto::resource_manager;
    use crate::request::Keyspace;
    use crate::set_resource_control_interceptor;
    use crate::transaction::HeartbeatOption;
    use crate::unset_resource_control_interceptor;
    use crate::Error;
    use crate::GetOption;
    use crate::Key;
    use crate::KvPair;
    use crate::Priority;
    use crate::ReplicaReadAdjustment;
    use crate::ReplicaReadConfig;
    use crate::RequestWaitResult;
    use crate::ResourceControlRequestInfo;
    use crate::ResourceGroupController;
    use crate::ResponseWaitResult;
    use crate::Value;

    #[test]
    fn source_request_source_encoding_and_internal_detection() {
        let mut source = crate::RequestSource {
            internal: true,
            ..Default::default()
        };
        assert_eq!(source.context_value(), "unknown");
        assert!(!source.is_internal());

        source.source_type = "snapshot".to_owned();
        assert_eq!(source.context_value(), "internal_snapshot");
        assert!(source.is_internal());

        source.internal = false;
        source.explicit_source_type = "lightning".to_owned();
        assert_eq!(source.context_value(), "external_snapshot_lightning");
        assert!(!source.is_internal());

        source.source_type.clear();
        assert_eq!(source.context_value(), "external_unknown_lightning");

        source.source_type = "lightning".to_owned();
        assert_eq!(source.context_value(), "external_lightning");
    }

    fn source_test_mutation(key: impl Into<Vec<u8>>, op: kvrpcpb::Op) -> kvrpcpb::Mutation {
        kvrpcpb::Mutation {
            op: op as i32,
            key: key.into(),
            value: b"value".to_vec(),
            ..Default::default()
        }
    }

    fn source_test_committer(
        rpc: Arc<MockPdClient>,
        primary_key: Option<Key>,
        mutations: Vec<kvrpcpb::Mutation>,
        options: crate::TransactionOptions,
        settings: CommitSettings,
    ) -> Committer<MockPdClient> {
        let write_size = mutations.iter().fold(0_u64, |total, mutation| {
            total.saturating_add((mutation.key.len() + mutation.value.len()) as u64)
        });
        Committer::new(
            primary_key,
            mutations,
            Timestamp::from_version(1),
            rpc,
            options,
            settings,
            Keyspace::Disable,
            None,
            None,
            None,
            None,
            None,
            ResolveLocksContext::default(),
            PipelinedTransactionState::default(),
            write_size,
            std::time::Instant::now(),
        )
    }

    fn source_test_chunk_batch(is_primary: bool) -> ChunkBatch {
        let mut chunks = TxnChunkSlice::default();
        chunks.push(7, TxnChunkRange::new(b"k".to_vec(), b"k".to_vec(), 1));
        ChunkBatch {
            chunks,
            region: MockPdClient::region2(),
            first_key: b"k".to_vec(),
            sample_data_keys: vec![b"k".to_vec()],
            is_primary,
        }
    }

    static GLOBAL_RESOURCE_CONTROL_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn source_min_commit_ts_manager_access_and_concurrency() {
        let manager = MinCommitTsManager::default();
        assert_eq!(manager.get(), 0);
        assert_eq!(manager.required_write_access(), WriteAccessLevel::Ttl);
        manager.try_update(10, WriteAccessLevel::Ttl);
        manager.try_update(5, WriteAccessLevel::Ttl);
        assert_eq!(manager.get(), 10);
        assert_eq!(manager.elevate_write_access(WriteAccessLevel::TwoPc), 10);
        assert_eq!(manager.required_write_access(), WriteAccessLevel::TwoPc);
        manager.try_update(20, WriteAccessLevel::Ttl);
        assert_eq!(manager.get(), 10);
        manager.try_update(30, WriteAccessLevel::TwoPc);
        assert_eq!(manager.get(), 30);

        let manager = MinCommitTsManager::default();
        let first = manager.clone();
        let second = manager.clone();
        let first = std::thread::spawn(move || {
            for value in 0..1_000 {
                first.try_update(value, WriteAccessLevel::Ttl);
            }
        });
        let second = std::thread::spawn(move || {
            for value in 1_000..2_000 {
                second.try_update(value, WriteAccessLevel::Ttl);
            }
        });
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(manager.get(), 1_999);
    }

    #[test]
    fn source_transaction_options_and_commit_wait_boundaries() {
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        assert_eq!(
            transaction.commit_wait_until_tso_timeout(),
            Duration::from_secs(1)
        );
        transaction.set_commit_wait_until_tso(20);
        transaction.set_commit_wait_until_tso(10);
        assert_eq!(transaction.commit_wait_until_tso(), 20);

        let invalid_flush = TransactionOptions::new_optimistic().pipelined(
            crate::transaction::PipelinedTxnOptions {
                enable: true,
                flush_concurrency: 0,
                resolve_lock_concurrency: 1,
                write_throttle_ratio: 0.0,
            },
        );
        assert_eq!(
            invalid_flush.validate().unwrap_err().to_string(),
            "pipelined txn flush concurrency should be greater than 0"
        );
        let invalid_resolve = TransactionOptions::new_optimistic().pipelined(
            crate::transaction::PipelinedTxnOptions {
                enable: true,
                flush_concurrency: 1,
                resolve_lock_concurrency: 0,
                write_throttle_ratio: 0.0,
            },
        );
        assert_eq!(
            invalid_resolve.validate().unwrap_err().to_string(),
            "pipelined txn resolve lock concurrency should be greater than 0"
        );
        let invalid_ratio = TransactionOptions::new_optimistic().pipelined(
            crate::transaction::PipelinedTxnOptions {
                enable: true,
                flush_concurrency: 1,
                resolve_lock_concurrency: 1,
                write_throttle_ratio: 1.0,
            },
        );
        assert_eq!(
            invalid_ratio.validate().unwrap_err().to_string(),
            "invalid write throttle ratio: 1"
        );

        let pipelined = TransactionOptions::new_optimistic()
            .pipelined(crate::transaction::PipelinedTxnOptions {
                enable: true,
                flush_concurrency: 1,
                resolve_lock_concurrency: 1,
                write_throttle_ratio: 0.0,
            })
            .drop_check(CheckLevel::None);
        pipelined.validate().unwrap();
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            pipelined,
            Keyspace::Disable,
        );
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            transaction.set_pessimistic(true);
        }))
        .is_err());
    }

    #[tokio::test]
    async fn source_commit_wait_error_class_and_first_attempt_closes_transaction() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    return Ok(Box::new(kvrpcpb::PrewriteResponse {
                        errors: vec![kvrpcpb::KeyError {
                            abort: "source prewrite failure".to_owned(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        assert!(transaction.commit().await.is_err());
        assert!(!transaction.is_valid());
        assert!(matches!(
            transaction.commit().await.unwrap_err(),
            Error::Static(crate::error::StaticError::InvalidTransaction)
        ));
        assert!(matches!(
            transaction.rollback().await.unwrap_err(),
            Error::Static(crate::error::StaticError::InvalidTransaction)
        ));

        let lag_rpc = Arc::new(MockPdClient::default());
        lag_rpc.set_timestamp(Timestamp::from_version(1));
        let mut settings = CommitSettings {
            commit_wait_until_tso: 2,
            commit_wait_until_tso_timeout: Duration::ZERO,
            ..Default::default()
        };
        let committer = source_test_committer(
            lag_rpc,
            Some(Key::from(b"key".to_vec())),
            vec![source_test_mutation(b"key", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            std::mem::take(&mut settings),
        );
        let error = committer.get_timestamp_for_commit().await.unwrap_err();
        assert!(crate::error::is_error_commit_timestamp_lag(&error));
        assert!(error.to_string().contains("fail immediately"));
    }

    #[tokio::test]
    async fn source_check_only_prewrites_without_commit_and_pessimistic_actions_follow_locks() {
        let prewrites = Arc::new(Mutex::new(Vec::new()));
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let captured_prewrites = Arc::clone(&prewrites);
        let captured_commit_calls = Arc::clone(&commit_calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured_prewrites.lock().unwrap().push(request.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if request.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    captured_commit_calls.fetch_add(1, Ordering::Relaxed);
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(10));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .insert(b"check".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        transaction.delete(b"check".to_vec()).await.unwrap();
        assert_eq!(transaction.commit().await.unwrap().unwrap().version(), 10);
        assert_eq!(commit_calls.load(Ordering::Relaxed), 0);
        let prewrites = prewrites.lock().unwrap();
        assert_eq!(prewrites.len(), 1);
        assert_eq!(prewrites[0].primary_lock, b"check");
        assert_eq!(prewrites[0].mutations.len(), 1);
        assert_eq!(
            prewrites[0].mutations[0].op,
            kvrpcpb::Op::CheckNotExists as i32
        );
        assert!(prewrites[0].secondaries.is_empty());

        let actions = Arc::new(Mutex::new(Vec::new()));
        let captured_actions = Arc::clone(&actions);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request.downcast_ref::<kvrpcpb::PrewriteRequest>().unwrap();
                captured_actions
                    .lock()
                    .unwrap()
                    .push(request.pessimistic_actions.clone());
                Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"locked".to_vec())),
            vec![
                source_test_mutation(b"locked", kvrpcpb::Op::Put),
                source_test_mutation(b"constraint", kvrpcpb::Op::Put),
                source_test_mutation(b"unlocked", kvrpcpb::Op::Put),
            ],
            TransactionOptions::new_pessimistic(),
            CommitSettings::default(),
        )
        .with_pessimistic_lock_keys(BTreeSet::from([b"locked".to_vec()]))
        .with_constraint_check_keys(BTreeSet::from([b"constraint".to_vec()]));
        committer.prewrite().await.unwrap();
        assert_eq!(
            actions.lock().unwrap().as_slice(),
            &[vec![
                kvrpcpb::prewrite_request::PessimisticAction::DoPessimisticCheck as i32,
                kvrpcpb::prewrite_request::PessimisticAction::DoConstraintCheck as i32,
                kvrpcpb::prewrite_request::PessimisticAction::SkipPessimisticCheck as i32,
            ]]
        );

        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_| {
                captured_dispatches.fetch_add(1, Ordering::Relaxed);
                Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut invalid = Transaction::new(
            Timestamp::from_version(u64::MAX),
            rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        invalid
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        assert_eq!(
            invalid.commit().await.unwrap_err().to_string(),
            format!("try to commit with invalid txnStartTS: {}", u64::MAX)
        );
        assert_eq!(dispatches.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn source_public_mutation_options_reach_filter_and_prewrite() {
        struct Filter(Arc<Mutex<Vec<super::MutationFlags>>>);
        impl super::KvFilter for Filter {
            fn is_unnecessary_key_value(
                &self,
                _key: &[u8],
                _value: &[u8],
                flags: super::MutationFlags,
            ) -> crate::Result<bool> {
                self.0.lock().unwrap().push(flags);
                Ok(false)
            }
        }

        let prewrites = Arc::new(Mutex::new(Vec::new()));
        let captured_prewrites = Arc::clone(&prewrites);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured_prewrites.lock().unwrap().push(request.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if request.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(10));
        let observed_flags = Arc::new(Mutex::new(Vec::new()));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.set_assertion_level(kvrpcpb::AssertionLevel::Strict);
        transaction.set_kv_filter(Arc::new(Filter(Arc::clone(&observed_flags))));
        transaction
            .put_with_options(
                b"key".to_vec(),
                b"value".to_vec(),
                super::MutationOptions::default()
                    .assertion(super::MutationAssertion::Exist)
                    .need_constraint_check_in_prewrite(true),
            )
            .await
            .unwrap();
        // As in client-go, changing mode after buffering a lazy constraint
        // produces a pessimistic prewrite without an acquired lock.
        transaction.set_pessimistic(true);
        transaction.commit().await.unwrap();

        let flags = observed_flags.lock().unwrap();
        assert_eq!(flags.len(), 1);
        assert_eq!(flags[0].assertion(), super::MutationAssertion::Exist);
        assert!(flags[0].needs_constraint_check_in_prewrite());
        assert!(!flags[0].is_pessimistic_locked());

        let prewrites = prewrites.lock().unwrap();
        assert_eq!(prewrites.len(), 1);
        assert_eq!(
            prewrites[0].mutations[0].assertion,
            kvrpcpb::Assertion::Exist as i32
        );
        assert_eq!(prewrites[0].for_update_ts, 1);
        assert_eq!(
            prewrites[0].pessimistic_actions,
            [kvrpcpb::prewrite_request::PessimisticAction::DoConstraintCheck as i32]
        );
    }

    #[tokio::test]
    async fn source_pessimistic_lock_assertion_is_stashed_until_prewrite_succeeds() {
        let prewrites = Arc::new(Mutex::new(Vec::new()));
        let captured_prewrites = Arc::clone(&prewrites);
        let commit_requests = Arc::new(AtomicUsize::new(0));
        let captured_commit_requests = Arc::clone(&commit_requests);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured_prewrites.lock().unwrap().push(request.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if request.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    captured_commit_requests.fetch_add(1, Ordering::Relaxed);
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(10));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .use_async_commit()
                .try_one_pc()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.set_assertion_level(kvrpcpb::AssertionLevel::Strict);
        let key = Key::from(b"key".to_vec());
        transaction
            .buffer
            .lock_with_returned_value(
                key.clone(),
                false,
                Some(&crate::ReturnedValue {
                    exists: true,
                    ..Default::default()
                }),
            )
            .unwrap();
        transaction.buffer.put(key.clone(), b"value".to_vec());
        transaction
            .buffer
            .set_mutation_options(
                &key,
                super::MutationOptions::default().assertion(super::MutationAssertion::NotExist),
            )
            .unwrap();

        let error = transaction.commit().await.unwrap_err();
        let Error::AssertionFailed(error) = error else {
            panic!("expected typed assertion failure, got {error:?}");
        };
        assert_eq!(error.assertion_failed.start_ts, 1);
        assert_eq!(error.assertion_failed.key, b"key");
        assert_eq!(
            error.assertion_failed.assertion,
            kvrpcpb::Assertion::NotExist as i32
        );
        let prewrites = prewrites.lock().unwrap();
        assert_eq!(prewrites.len(), 1);
        assert!(!prewrites[0].use_async_commit);
        assert!(!prewrites[0].try_one_pc);
        assert_eq!(commit_requests.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn source_prewrite_key_errors_are_typed_and_assertion_has_lower_priority() {
        let assertion: Error = kvrpcpb::KeyError {
            assertion_failed: Some(kvrpcpb::AssertionFailed {
                start_ts: 1,
                key: b"asserted".to_vec(),
                assertion: kvrpcpb::Assertion::Exist as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
        .into();
        assert!(matches!(assertion, Error::AssertionFailed(_)));

        let conflict: Error = kvrpcpb::KeyError {
            conflict: Some(kvrpcpb::WriteConflict {
                start_ts: 1,
                conflict_ts: 2,
                key: b"conflict".to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        }
        .into();
        let selected =
            super::normalize_prewrite_error(Error::MultipleKeyErrors(vec![assertion, conflict]));
        assert!(matches!(selected, Error::WriteConflict(_)));

        let key_exists: Error = kvrpcpb::KeyError {
            already_exist: Some(kvrpcpb::AlreadyExist {
                key: b"existing".to_vec(),
            }),
            ..Default::default()
        }
        .into();
        assert!(matches!(key_exists, Error::KeyExists(_)));

        let retryable: Error = kvrpcpb::KeyError {
            retryable: "retry transaction".to_owned(),
            ..Default::default()
        }
        .into();
        assert!(matches!(retryable, Error::RetryableKey(_)));
    }

    #[tokio::test]
    async fn source_prewrite_lock_policy_returns_typed_conflicts_without_resolving() {
        let resolver_calls = Arc::new(AtomicUsize::new(0));
        let captured_resolver_calls = Arc::clone(&resolver_calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    return Ok(Box::new(kvrpcpb::PrewriteResponse {
                        errors: vec![kvrpcpb::KeyError {
                            locked: Some(kvrpcpb::LockInfo {
                                shared_lock_infos: vec![kvrpcpb::LockInfo {
                                    key: b"shared-holder".to_vec(),
                                    lock_version: 9,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                captured_resolver_calls.fetch_add(1, Ordering::Relaxed);
                Err(Error::StringError(
                    "NoResolvePolicy dispatched a lock-resolver request".to_owned(),
                ))
            },
        )));
        let mut settings = CommitSettings {
            prewrite_lock_policy: PrewriteEncounterLockPolicy::NoResolve,
            ..Default::default()
        };
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"key".to_vec())),
            vec![source_test_mutation(b"key", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            std::mem::take(&mut settings),
        );
        let error = committer.prewrite().await.unwrap_err();
        assert!(crate::error::is_write_conflict(&error), "{error:?}");
        let Error::WriteConflict(conflict) = error else {
            panic!("expected typed write conflict");
        };
        assert_eq!(conflict.conflict.start_ts, 1);
        assert_eq!(conflict.conflict.conflict_ts, 9);
        assert_eq!(conflict.conflict.key, b"shared-holder");
        assert_eq!(resolver_calls.load(Ordering::Relaxed), 0);

        let resolver_calls = Arc::new(AtomicUsize::new(0));
        let captured_resolver_calls = Arc::clone(&resolver_calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    return Ok(Box::new(kvrpcpb::PrewriteResponse {
                        errors: vec![kvrpcpb::KeyError {
                            locked: Some(kvrpcpb::LockInfo {
                                key: b"newer-lock".to_vec(),
                                lock_version: 2,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                captured_resolver_calls.fetch_add(1, Ordering::Relaxed);
                Err(Error::StringError(
                    "newer optimistic lock dispatched a resolver request".to_owned(),
                ))
            },
        )));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"key".to_vec())),
            vec![source_test_mutation(b"key", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        let error = committer.prewrite().await.unwrap_err();
        assert!(crate::error::is_write_conflict(&error), "{error:?}");
        assert_eq!(resolver_calls.load(Ordering::Relaxed), 0);
    }

    struct RecordingReadTimestampValidator {
        calls: Arc<Mutex<Vec<(u64, bool, String)>>>,
        error: Option<&'static str>,
    }

    #[async_trait::async_trait]
    impl ReadTimestampValidator for RecordingReadTimestampValidator {
        async fn validate_read_timestamp(
            &self,
            read_timestamp: u64,
            stale_read: bool,
            option: &OracleOption,
        ) -> OracleResult<()> {
            self.calls
                .lock()
                .unwrap()
                .push((read_timestamp, stale_read, option.txn_scope.clone()));
            match self.error {
                Some(error) => Err(Box::new(io::Error::other(error)) as OracleError),
                None => Ok(()),
            }
        }
    }

    struct RecordingSnapshotVisibilityValidator {
        calls: Arc<Mutex<Vec<u64>>>,
        error: Option<&'static str>,
    }

    #[async_trait::async_trait]
    impl crate::SnapshotVisibilityValidator for RecordingSnapshotVisibilityValidator {
        async fn check_visibility(&self, start_timestamp: u64) -> crate::Result<()> {
            self.calls.lock().unwrap().push(start_timestamp);
            match self.error {
                Some(error) => Err(Error::StringError(error.to_owned())),
                None => Ok(()),
            }
        }
    }

    struct RecordingResourceController {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait::async_trait]
    impl ResourceGroupController for RecordingResourceController {
        async fn on_request_wait(
            &self,
            resource_group_name: &str,
            _: ResourceControlRequestInfo,
        ) -> crate::Result<RequestWaitResult> {
            assert_eq!(resource_group_name, "test-rg");
            self.events.lock().unwrap().push("request");
            Ok(RequestWaitResult {
                consumption: resource_manager::Consumption {
                    r_r_u: 2.0,
                    w_r_u: 3.0,
                    ..Default::default()
                },
                penalty: Some(resource_manager::Consumption {
                    r_r_u: 1.0,
                    ..Default::default()
                }),
                wait_duration: Duration::from_millis(2),
                priority: 7,
            })
        }

        fn on_response_wait(
            &self,
            resource_group_name: &str,
            _: ResourceControlRequestInfo,
            _: crate::ResourceControlResponseInfo,
        ) -> crate::Result<ResponseWaitResult> {
            assert_eq!(resource_group_name, "test-rg");
            self.events.lock().unwrap().push("response");
            Ok(ResponseWaitResult {
                consumption: resource_manager::Consumption {
                    r_r_u: 5.0,
                    w_r_u: 7.0,
                    ..Default::default()
                },
                wait_duration: Duration::from_millis(3),
            })
        }
    }
    use crate::ReplicaReadSelectorOption;
    use crate::ReplicaReadType;
    use crate::SnapshotRequestType;
    use crate::TimestampExt;
    use crate::Transaction;
    use crate::TransactionOptions;
    use crate::ValueEntry;

    #[test]
    fn source_snapshot_timestamp_reset_discards_only_resolved_lock_hints() {
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.read_lock_context.add_resolved(11);
        transaction.read_lock_context.add_committed(12);

        transaction.set_snapshot_timestamp(Timestamp::from_version(2));

        assert_eq!(transaction.start_timestamp().version(), 2);
        assert_eq!(
            transaction.read_lock_context.snapshot(),
            (Vec::new(), vec![12])
        );
    }

    #[test]
    fn source_snapshot_cache_mutation_skips_latest_timestamp_snapshots() {
        let key: Key = b"key".to_vec().into();
        let values = BTreeMap::from([(key.clone(), ValueEntry::new(b"value".to_vec(), 7))]);
        let mut latest = Transaction::new(
            Timestamp::from_version(u64::MAX),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        latest.update_snapshot_cache(vec![key.clone()], values.clone());
        assert!(latest.snapshot_cache().is_empty());

        let mut snapshot = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        snapshot.update_snapshot_cache(vec![key.clone()], values);
        assert_eq!(
            snapshot.snapshot_cache(),
            BTreeMap::from([(key, ValueEntry::new(b"value".to_vec(), 7))])
        );
    }

    #[tokio::test]
    async fn source_snapshot_return_commit_ts_refetches_unknown_cached_entries() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured_calls = Arc::clone(&calls);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    captured_calls
                        .lock()
                        .unwrap()
                        .push(("get", request.need_commit_ts));
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"value".to_vec(),
                        commit_ts: u64::from(request.need_commit_ts) * 42,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    captured_calls
                        .lock()
                        .unwrap()
                        .push(("batch", request.need_commit_ts));
                    return Ok(Box::new(kvrpcpb::BatchGetResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: b"batch".to_vec(),
                            value: b"batch-value".to_vec(),
                            commit_ts: u64::from(request.need_commit_ts) * 43,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected request while testing commit-ts snapshot reads");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        assert_eq!(
            transaction.get("get".to_owned()).await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(
            transaction
                .get_with_options("get".to_owned(), &[GetOption::ReturnCommitTs])
                .await
                .unwrap(),
            Some(ValueEntry::new(b"value".to_vec(), 42))
        );
        assert_eq!(
            transaction
                .get_with_options("get".to_owned(), &[])
                .await
                .unwrap(),
            Some(ValueEntry::new(b"value".to_vec(), 0))
        );
        assert_eq!(
            transaction
                .get_with_options("get".to_owned(), &[GetOption::ReturnCommitTs])
                .await
                .unwrap(),
            Some(ValueEntry::new(b"value".to_vec(), 42))
        );

        let _: Vec<_> = transaction
            .batch_get(vec!["batch".to_owned()])
            .await
            .unwrap()
            .collect();
        assert_eq!(
            transaction
                .batch_get_with_options(vec!["batch".to_owned()], &[GetOption::ReturnCommitTs])
                .await
                .unwrap(),
            BTreeMap::from([(
                Key::from(b"batch".to_vec()),
                ValueEntry::new(b"batch-value".to_vec(), 43),
            )])
        );
        assert_eq!(
            transaction
                .batch_get_with_options(vec!["batch".to_owned()], &[GetOption::ReturnCommitTs])
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            transaction
                .batch_get_with_options(vec!["batch".to_owned()], &[])
                .await
                .unwrap(),
            BTreeMap::from([(
                Key::from(b"batch".to_vec()),
                ValueEntry::new(b"batch-value".to_vec(), 0),
            )])
        );
        assert_eq!(
            *calls.lock().unwrap(),
            [
                ("get", false),
                ("get", true),
                ("batch", false),
                ("batch", true)
            ]
        );
    }

    #[test]
    fn source_snapshot_return_commit_ts_rejects_unknown_nonempty_entries() {
        let error = ensure_snapshot_commit_ts(true, Some(&ValueEntry::new(b"value".to_vec(), 0)))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "commit timestamp is required but not returned"
        );
        assert!(ensure_snapshot_commit_ts(true, Some(&ValueEntry::default())).is_ok());
        assert!(
            ensure_snapshot_commit_ts(false, Some(&ValueEntry::new(b"value".to_vec(), 0))).is_ok()
        );
    }

    #[tokio::test]
    async fn source_snapshot_timestamp_reset_discards_cached_reads() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::GetRequest>());
                let value = if captured_dispatches.fetch_add(1, Ordering::SeqCst) == 0 {
                    b"old".to_vec()
                } else {
                    b"new".to_vec()
                };
                Ok(Box::new(kvrpcpb::GetResponse {
                    value,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        assert_eq!(
            transaction.get("key".to_owned()).await.unwrap(),
            Some(b"old".to_vec())
        );
        transaction.set_snapshot_timestamp(Timestamp::from_version(2));
        assert_eq!(
            transaction.get("key".to_owned()).await.unwrap(),
            Some(b"new".to_vec())
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn source_max_timestamp_snapshot_does_not_cache_gets() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::GetRequest>());
                let value = if captured_dispatches.fetch_add(1, Ordering::SeqCst) == 0 {
                    b"first".to_vec()
                } else {
                    b"second".to_vec()
                };
                Ok(Box::new(kvrpcpb::GetResponse {
                    value,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(u64::MAX),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        assert_eq!(
            transaction.get("key".to_owned()).await.unwrap(),
            Some(b"first".to_vec())
        );
        assert_eq!(
            transaction.get("key".to_owned()).await.unwrap(),
            Some(b"second".to_vec())
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
    }

    #[test]
    #[should_panic(expected = "try to get snapshot with a large ts")]
    fn source_snapshot_constructor_rejects_non_max_u64_large_values() {
        let transaction = Transaction::new(
            Timestamp::from_version(i64::MAX as u64),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let _ = crate::Snapshot::new(transaction);
    }

    #[tokio::test]
    async fn source_max_timestamp_point_get_omits_locks_after_the_first_transaction() {
        let get_attempts = Arc::new(AtomicUsize::new(0));
        let status_checks = Arc::new(AtomicUsize::new(0));
        let captured_get_attempts = Arc::clone(&get_attempts);
        let captured_status_checks = Arc::clone(&status_checks);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    let attempt = captured_get_attempts.fetch_add(1, Ordering::SeqCst);
                    let lock_version = match attempt {
                        0 => 1,
                        1 => 2,
                        2 => {
                            let context = request.context.as_ref().unwrap();
                            assert_eq!(context.committed_locks, [1]);
                            assert_eq!(context.resolved_locks, [2]);
                            return Ok(Box::new(kvrpcpb::GetResponse {
                                value: b"old-value".to_vec(),
                                ..Default::default()
                            }) as Box<dyn Any>);
                        }
                        _ => panic!("unexpected latest point-get attempt"),
                    };
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        error: Some(kvrpcpb::KeyError {
                            locked: Some(kvrpcpb::LockInfo {
                                key: b"key".to_vec(),
                                primary_lock: b"key".to_vec(),
                                lock_version,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    captured_status_checks.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(request.lock_ts, 1);
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 3,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("a different latest point-get lock must not be resolved");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(u64::MAX),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        assert_eq!(
            transaction.get("key".to_owned()).await.unwrap(),
            Some(b"old-value".to_vec())
        );
        assert_eq!(get_attempts.load(Ordering::SeqCst), 3);
        assert_eq!(status_checks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_batch_get_retries_only_pair_locked_keys_and_keeps_clean_pairs() {
        let batch_attempts = Arc::new(Mutex::new(Vec::<Vec<Vec<u8>>>::new()));
        let captured_batch_attempts = Arc::clone(&batch_attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    let mut attempts = captured_batch_attempts.lock().unwrap();
                    attempts.push(request.keys.clone());
                    if attempts.len() == 1 {
                        return Ok(Box::new(kvrpcpb::BatchGetResponse {
                            pairs: vec![
                                kvrpcpb::KvPair {
                                    key: b"clean".to_vec(),
                                    value: b"clean-value".to_vec(),
                                    ..Default::default()
                                },
                                kvrpcpb::KvPair {
                                    key: b"locked".to_vec(),
                                    error: Some(kvrpcpb::KeyError {
                                        locked: Some(kvrpcpb::LockInfo {
                                            key: b"locked".to_vec(),
                                            primary_lock: b"locked".to_vec(),
                                            lock_version: 1,
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ],
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    assert_eq!(request.keys, [b"locked".to_vec()]);
                    assert_eq!(request.context.as_ref().unwrap().committed_locks, [1]);
                    return Ok(Box::new(kvrpcpb::BatchGetResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: b"locked".to_vec(),
                            value: b"locked-value".to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected request while retrying locked batch-get keys");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let result = transaction
            .batch_get([b"clean".to_vec(), b"locked".to_vec()])
            .await
            .unwrap()
            .map(Into::<(Key, Value)>::into)
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            result,
            BTreeMap::from([
                (Key::from(b"clean".to_vec()), b"clean-value".to_vec()),
                (Key::from(b"locked".to_vec()), b"locked-value".to_vec()),
            ])
        );
        assert_eq!(batch_attempts.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn source_async_batch_get_counts_each_initial_concurrent_shard() {
        let mut first_region = MockPdClient::region1();
        first_region.region.start_key.clear();
        first_region.region.end_key = b"m".to_vec();
        let mut second_region = MockPdClient::region2();
        second_region.region.start_key = b"m".to_vec();
        second_region.region.end_key.clear();
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            assert!(request.is::<kvrpcpb::BatchGetRequest>());
            captured_dispatches.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(kvrpcpb::BatchGetResponse::default()) as Box<dyn Any>)
        });
        let pd_client = Arc::new(MockPdClient::with_client_and_regions(
            client,
            vec![first_region, second_region],
        ));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let before = crate::stats::async_batch_get_count("ok");

        let pairs = transaction
            .batch_get([b"a".to_vec(), b"n".to_vec()])
            .await
            .unwrap()
            .collect::<Vec<_>>();

        assert!(pairs.is_empty());
        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
        assert!(crate::stats::async_batch_get_count("ok") >= before + 2);
    }

    #[tokio::test]
    async fn source_snapshot_batch_get_caches_missing_keys() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::BatchGetRequest>());
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::BatchGetResponse::default()) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let first: Vec<_> = transaction
            .batch_get(vec!["missing".to_owned()])
            .await
            .unwrap()
            .collect();
        let second: Vec<_> = transaction
            .batch_get(vec!["missing".to_owned()])
            .await
            .unwrap()
            .collect();
        assert!(first.is_empty());
        assert!(second.is_empty());
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_snapshot_scans_do_not_fill_the_point_read_cache() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::ScanRequest>() {
                    captured_requests.lock().unwrap().push("scan");
                    Ok(Box::new(kvrpcpb::ScanResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: b"key".to_vec(),
                            value: b"scan-value".to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::GetRequest>() {
                    captured_requests.lock().unwrap().push("get");
                    Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"get-value".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>)
                } else {
                    panic!("unexpected request while testing snapshot scan cache behavior");
                }
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let scanned: Vec<_> = transaction
            .scan(b"key".to_vec()..b"keyz".to_vec(), 1)
            .await
            .unwrap()
            .collect();
        assert_eq!(
            scanned,
            [KvPair(b"key".to_vec().into(), b"scan-value".to_vec())]
        );
        assert_eq!(
            transaction.get("key".to_owned()).await.unwrap(),
            Some(b"get-value".to_vec())
        );
        assert_eq!(*requests.lock().unwrap(), ["scan", "get"]);
    }

    #[test]
    #[should_panic(expected = "try to get snapshot with a large ts")]
    fn source_snapshot_timestamp_rejects_non_max_u64_large_values() {
        let mut transaction = Transaction::new(
            Timestamp::default(),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_timestamp(Timestamp::from_version(i64::MAX as u64));
    }

    #[tokio::test]
    async fn source_snapshot_sample_step_reaches_every_scan_request() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::ScanRequest>()
                    .expect("snapshot scan should dispatch ScanRequest");
                assert_eq!(request.sample_step, 3);
                Ok(Box::new(kvrpcpb::ScanResponse::default()) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_sample_step(3);

        let pairs: Vec<_> = transaction
            .scan(b"a".to_vec()..b"b".to_vec(), 1)
            .await
            .unwrap()
            .collect();

        assert!(pairs.is_empty());
    }

    #[tokio::test]
    async fn source_snapshot_scan_batch_size_chunks_forward_scan_requests() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::ScanRequest>()
                    .expect("snapshot scan should dispatch ScanRequest");
                captured_requests.lock().unwrap().push((
                    request.start_key.clone(),
                    request.limit,
                    request.reverse,
                ));
                let pairs = match request.start_key.as_slice() {
                    b"a" => vec![
                        kvrpcpb::KvPair {
                            key: b"a".to_vec(),
                            value: b"a-value".to_vec(),
                            ..Default::default()
                        },
                        kvrpcpb::KvPair {
                            key: b"b".to_vec(),
                            value: b"b-value".to_vec(),
                            ..Default::default()
                        },
                    ],
                    b"b\0" => vec![
                        kvrpcpb::KvPair {
                            key: b"c".to_vec(),
                            value: b"c-value".to_vec(),
                            ..Default::default()
                        },
                        kvrpcpb::KvPair {
                            key: b"d".to_vec(),
                            value: b"d-value".to_vec(),
                            ..Default::default()
                        },
                    ],
                    start => panic!("unexpected scan start key: {start:?}"),
                };
                Ok(Box::new(kvrpcpb::ScanResponse {
                    pairs,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_scan_batch_size(2);

        let pairs: Vec<_> = transaction
            .scan(b"a".to_vec()..b"z".to_vec(), 4)
            .await
            .unwrap()
            .collect();

        assert_eq!(
            pairs,
            [
                KvPair(b"a".to_vec().into(), b"a-value".to_vec()),
                KvPair(b"b".to_vec().into(), b"b-value".to_vec()),
                KvPair(b"c".to_vec().into(), b"c-value".to_vec()),
                KvPair(b"d".to_vec().into(), b"d-value".to_vec()),
            ]
        );
        assert_eq!(
            *requests.lock().unwrap(),
            [(b"a".to_vec(), 2, false), (b"b\0".to_vec(), 2, false)]
        );
    }

    #[tokio::test]
    async fn source_snapshot_iterator_fetches_and_advances_one_batch_at_a_time() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::ScanRequest>()
                    .expect("snapshot iterator should dispatch ScanRequest");
                captured_requests
                    .lock()
                    .unwrap()
                    .push((request.start_key.clone(), request.limit));
                let pairs = match request.start_key.as_slice() {
                    b"a" => vec![
                        kvrpcpb::KvPair {
                            key: b"a".to_vec(),
                            value: b"a-value".to_vec(),
                            ..Default::default()
                        },
                        kvrpcpb::KvPair {
                            key: b"b".to_vec(),
                            value: b"b-value".to_vec(),
                            ..Default::default()
                        },
                    ],
                    b"b\0" => vec![kvrpcpb::KvPair {
                        key: b"c".to_vec(),
                        value: b"c-value".to_vec(),
                        ..Default::default()
                    }],
                    start => panic!("unexpected iterator scan start key: {start:?}"),
                };
                Ok(Box::new(kvrpcpb::ScanResponse {
                    pairs,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_scan_batch_size(2);
        let mut snapshot = crate::Snapshot::new(transaction);

        let mut iterator = snapshot.iter(b"a".to_vec()..b"z".to_vec()).await.unwrap();
        assert!(iterator.is_valid());
        assert_eq!(*requests.lock().unwrap(), [(b"a".to_vec(), 2)]);
        assert_eq!(
            iterator.next().await.unwrap().unwrap().key(),
            &Key::from(b"a".to_vec())
        );
        assert_eq!(
            iterator.next().await.unwrap().unwrap().key(),
            &Key::from(b"b".to_vec())
        );
        assert_eq!(*requests.lock().unwrap(), [(b"a".to_vec(), 2)]);
        assert_eq!(
            iterator.next().await.unwrap().unwrap().key(),
            &Key::from(b"c".to_vec())
        );
        assert!(iterator.next().await.unwrap().is_none());
        assert!(!iterator.is_valid());
        assert_eq!(
            *requests.lock().unwrap(),
            [(b"a".to_vec(), 2), (b"b\0".to_vec(), 2)]
        );
    }

    #[tokio::test]
    async fn source_snapshot_iterator_routes_one_boundary_region_and_crosses_empty_regions() {
        let mut first_region = MockPdClient::region1();
        first_region.region.start_key.clear();
        first_region.region.end_key = b"m".to_vec();
        let mut second_region = MockPdClient::region2();
        second_region.region.start_key = b"m".to_vec();
        second_region.region.end_key.clear();

        let forward_requests = Arc::new(Mutex::new(Vec::new()));
        let captured_forward = Arc::clone(&forward_requests);
        let forward_client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            let request = request
                .downcast_ref::<kvrpcpb::ScanRequest>()
                .expect("forward iterator should dispatch ScanRequest");
            captured_forward.lock().unwrap().push((
                request.start_key.clone(),
                request.end_key.clone(),
                request.reverse,
            ));
            let pairs = if request.start_key == b"a" {
                Vec::new()
            } else {
                assert_eq!(request.start_key, b"m");
                vec![kvrpcpb::KvPair {
                    key: b"n".to_vec(),
                    value: b"n-value".to_vec(),
                    ..Default::default()
                }]
            };
            Ok(Box::new(kvrpcpb::ScanResponse {
                pairs,
                ..Default::default()
            }) as Box<dyn Any>)
        });
        let forward_pd = Arc::new(MockPdClient::with_client_and_regions(
            forward_client,
            vec![first_region.clone(), second_region.clone()],
        ));
        let forward_transaction = Transaction::new(
            Timestamp::from_version(1),
            forward_pd,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let mut forward_snapshot = crate::Snapshot::new(forward_transaction);
        forward_snapshot.set_scan_batch_size(3);
        let mut forward = forward_snapshot
            .iter(b"a".to_vec()..b"z".to_vec())
            .await
            .unwrap();
        assert_eq!(
            forward.next().await.unwrap(),
            Some(KvPair::new(b"n".to_vec(), b"n-value".to_vec()))
        );
        assert!(forward.next().await.unwrap().is_none());
        assert_eq!(
            *forward_requests.lock().unwrap(),
            [
                (b"a".to_vec(), b"m".to_vec(), false),
                (b"m".to_vec(), b"z".to_vec(), false),
            ]
        );

        let reverse_requests = Arc::new(Mutex::new(Vec::new()));
        let captured_reverse = Arc::clone(&reverse_requests);
        let reverse_client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            let request = request
                .downcast_ref::<kvrpcpb::ScanRequest>()
                .expect("reverse iterator should dispatch ScanRequest");
            captured_reverse.lock().unwrap().push((
                request.start_key.clone(),
                request.end_key.clone(),
                request.reverse,
            ));
            let (key, value) = if request.start_key == b"z" {
                (b"n".to_vec(), b"n-value".to_vec())
            } else {
                assert_eq!(request.start_key, b"m");
                (b"b".to_vec(), b"b-value".to_vec())
            };
            Ok(Box::new(kvrpcpb::ScanResponse {
                pairs: vec![kvrpcpb::KvPair {
                    key,
                    value,
                    ..Default::default()
                }],
                ..Default::default()
            }) as Box<dyn Any>)
        });
        let reverse_pd = Arc::new(MockPdClient::with_client_and_regions(
            reverse_client,
            vec![first_region, second_region],
        ));
        let reverse_transaction = Transaction::new(
            Timestamp::from_version(1),
            reverse_pd,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let mut reverse_snapshot = crate::Snapshot::new(reverse_transaction);
        reverse_snapshot.set_scan_batch_size(3);
        let mut reverse = reverse_snapshot
            .iter_reverse(b"a".to_vec()..b"z".to_vec())
            .await
            .unwrap();
        assert_eq!(
            reverse.next().await.unwrap().unwrap().key(),
            &Key::from(b"n".to_vec())
        );
        assert_eq!(
            reverse.next().await.unwrap().unwrap().key(),
            &Key::from(b"b".to_vec())
        );
        assert!(reverse.next().await.unwrap().is_none());
        assert_eq!(
            *reverse_requests.lock().unwrap(),
            [
                (b"z".to_vec(), b"m".to_vec(), true),
                (b"m".to_vec(), b"a".to_vec(), true),
            ]
        );
    }

    #[tokio::test]
    async fn source_snapshot_scanner_point_reads_pair_locks_without_rescanning_clean_pairs() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                    captured_requests.lock().unwrap().push("scan".to_owned());
                    assert_eq!(request.start_key, b"a");
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
                            kvrpcpb::KvPair {
                                key: b"c".to_vec(),
                                value: b"c-value".to_vec(),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    captured_requests
                        .lock()
                        .unwrap()
                        .push(format!("get:{}", String::from_utf8_lossy(&request.key)));
                    assert_eq!(request.key, b"a");
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"a-value".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected scanner pair-lock request")
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_scan_batch_size(3);
        let mut snapshot = crate::Snapshot::new(transaction);

        let mut iterator = snapshot.iter(b"a".to_vec()..b"z".to_vec()).await.unwrap();
        let mut pairs = Vec::new();
        for _ in 0..3 {
            pairs.push(iterator.next().await.unwrap().unwrap());
        }

        assert_eq!(
            pairs,
            [
                KvPair(b"a".to_vec().into(), b"a-value".to_vec()),
                KvPair(b"b".to_vec().into(), b"b-value".to_vec()),
                KvPair(b"c".to_vec().into(), b"c-value".to_vec()),
            ]
        );
        assert_eq!(*requests.lock().unwrap(), ["scan", "get:a"]);
    }

    #[tokio::test]
    async fn source_snapshot_scanner_advances_past_a_missing_locked_pair() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                    captured_requests
                        .lock()
                        .unwrap()
                        .push(format!("scan:{:?}", request.start_key));
                    let pairs = if request.start_key == b"a" {
                        vec![
                            kvrpcpb::KvPair {
                                key: b"a".to_vec(),
                                value: b"a-value".to_vec(),
                                ..Default::default()
                            },
                            kvrpcpb::KvPair {
                                error: Some(kvrpcpb::KeyError {
                                    locked: Some(kvrpcpb::LockInfo {
                                        key: b"b".to_vec(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                        ]
                    } else {
                        assert_eq!(request.start_key, b"b\0");
                        vec![kvrpcpb::KvPair {
                            key: b"c".to_vec(),
                            value: b"c-value".to_vec(),
                            ..Default::default()
                        }]
                    };
                    return Ok(Box::new(kvrpcpb::ScanResponse {
                        pairs,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    captured_requests.lock().unwrap().push("get:b".to_owned());
                    assert_eq!(request.key, b"b");
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected missing scanner pair request")
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_scan_batch_size(2);
        let mut snapshot = crate::Snapshot::new(transaction);

        let pairs: Vec<_> = snapshot
            .scan(b"a".to_vec()..b"z".to_vec(), 2)
            .await
            .unwrap()
            .collect();
        assert_eq!(
            pairs,
            [
                KvPair(b"a".to_vec().into(), b"a-value".to_vec()),
                KvPair(b"c".to_vec().into(), b"c-value".to_vec()),
            ]
        );
        assert_eq!(
            *requests.lock().unwrap(),
            ["scan:[97]", "get:b", "scan:[98, 0]"]
        );
    }

    #[tokio::test]
    async fn source_snapshot_scanner_retries_incomplete_response_after_top_level_lock() {
        let scan_attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = Arc::clone(&scan_attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                    let attempt = captured_attempts.fetch_add(1, Ordering::SeqCst);
                    let context = request.context.as_ref().unwrap();
                    if attempt == 0 {
                        assert!(context.committed_locks.is_empty());
                        return Ok(Box::new(kvrpcpb::ScanResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: b"a".to_vec(),
                                    primary_lock: b"a".to_vec(),
                                    lock_version: 1,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            pairs: vec![kvrpcpb::KvPair {
                                key: b"incomplete".to_vec(),
                                value: b"must-not-escape".to_vec(),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    assert_eq!(context.committed_locks, [1]);
                    return Ok(Box::new(kvrpcpb::ScanResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: b"a".to_vec(),
                            value: b"a-value".to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected scanner response-lock request")
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_scan_batch_size(3);
        let mut snapshot = crate::Snapshot::new(transaction);

        let mut iterator = snapshot.iter(b"a".to_vec()..b"z".to_vec()).await.unwrap();
        assert_eq!(
            iterator.next().await.unwrap(),
            Some(KvPair(b"a".to_vec().into(), b"a-value".to_vec()))
        );
        assert_eq!(scan_attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn source_snapshot_scan_batch_size_chunks_reverse_scan_requests() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::ScanRequest>()
                    .expect("snapshot scan should dispatch ScanRequest");
                captured_requests.lock().unwrap().push((
                    request.start_key.clone(),
                    request.end_key.clone(),
                    request.limit,
                ));
                let pairs = match request.start_key.as_slice() {
                    b"z" => vec![
                        kvrpcpb::KvPair {
                            key: b"d".to_vec(),
                            value: b"d-value".to_vec(),
                            ..Default::default()
                        },
                        kvrpcpb::KvPair {
                            key: b"c".to_vec(),
                            value: b"c-value".to_vec(),
                            ..Default::default()
                        },
                    ],
                    b"c" => vec![
                        kvrpcpb::KvPair {
                            key: b"b".to_vec(),
                            value: b"b-value".to_vec(),
                            ..Default::default()
                        },
                        kvrpcpb::KvPair {
                            key: b"a".to_vec(),
                            value: b"a-value".to_vec(),
                            ..Default::default()
                        },
                    ],
                    start => panic!("unexpected reverse scan start key: {start:?}"),
                };
                Ok(Box::new(kvrpcpb::ScanResponse {
                    pairs,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_scan_batch_size(2);

        let pairs: Vec<_> = transaction
            .scan_reverse(b"a".to_vec()..b"z".to_vec(), 4)
            .await
            .unwrap()
            .collect();

        assert_eq!(
            pairs,
            [
                KvPair(b"d".to_vec().into(), b"d-value".to_vec()),
                KvPair(b"c".to_vec().into(), b"c-value".to_vec()),
                KvPair(b"b".to_vec().into(), b"b-value".to_vec()),
                KvPair(b"a".to_vec().into(), b"a-value".to_vec()),
            ]
        );
        assert_eq!(
            *requests.lock().unwrap(),
            [
                (b"z".to_vec(), b"a".to_vec(), 2),
                (b"c".to_vec(), b"a".to_vec(), 2),
            ]
        );
    }

    #[tokio::test]
    async fn source_snapshot_runtime_stats_exclude_scanner_rpcs() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                if request.is::<kvrpcpb::GetRequest>() {
                    Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"get-value".to_vec(),
                        exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                                wait_wall_time_ns: 1,
                                process_wall_time_ns: 2,
                                process_suspend_wall_time_ns: 3,
                                kv_read_wall_time_ns: 4,
                                total_rpc_wall_time_ns: 5,
                                kv_grpc_process_time_ns: 6,
                                kv_grpc_wait_time_ns: 7,
                            }),
                            scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                                total_versions: 8,
                                processed_versions: 9,
                                processed_versions_size: 10,
                                rocksdb_block_read_nanos: 11,
                                read_index_propose_wait_nanos: 12,
                                read_index_confirm_wait_nanos: 13,
                                read_pool_schedule_wait_nanos: 14,
                                ia_remote_read_segment_nanos: 12,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::BatchGetRequest>() {
                    Ok(Box::new(kvrpcpb::BatchGetResponse::default()) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::ScanRequest>() {
                    Ok(Box::new(kvrpcpb::ScanResponse::default()) as Box<dyn Any>)
                } else {
                    panic!("unexpected request while collecting snapshot runtime stats");
                }
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let stats = Arc::new(crate::SnapshotRuntimeStats::new());
        transaction.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));

        transaction.get("get".to_owned()).await.unwrap();
        transaction
            .batch_get(["batch".to_owned()])
            .await
            .unwrap()
            .for_each(drop);
        transaction
            .scan(b"scan".to_vec()..b"scanz".to_vec(), 1)
            .await
            .unwrap()
            .for_each(drop);

        assert_eq!(stats.rpc_count(crate::SnapshotRpcCommand::Get), 1);
        assert_eq!(stats.rpc_count(crate::SnapshotRpcCommand::BatchGet), 1);
        assert_eq!(stats.rpc_count(crate::SnapshotRpcCommand::Scan), 0);
        assert_eq!(
            stats.rpc_count(crate::SnapshotRpcCommand::BufferBatchGet),
            0
        );
        assert_eq!(stats.time_detail().wait_time, Duration::from_nanos(1));
        assert_eq!(stats.time_detail().process_time, Duration::from_nanos(2));
        assert_eq!(stats.time_detail().suspend_time, Duration::from_nanos(3));
        assert_eq!(
            stats.time_detail().kv_grpc_process_time,
            Duration::from_nanos(6)
        );
        assert_eq!(
            stats.time_detail().kv_grpc_wait_time,
            Duration::from_nanos(7)
        );
        assert_eq!(stats.scan_detail().total_keys, 8);
        assert_eq!(stats.scan_detail().processed_keys, 9);
        assert_eq!(stats.scan_detail().processed_keys_size, 10);
        assert_eq!(
            stats.scan_detail().rocksdb_block_read_duration,
            Duration::from_nanos(11)
        );
        assert_eq!(
            stats.scan_detail().read_index_propose_wait_duration,
            Duration::from_nanos(12)
        );
        assert_eq!(
            stats.scan_detail().read_index_confirm_wait_duration,
            Duration::from_nanos(13)
        );
        assert_eq!(
            stats.scan_detail().read_pool_schedule_wait_duration,
            Duration::from_nanos(14)
        );
        assert_eq!(
            stats.scan_detail().ia_remote_read_segment_duration,
            Duration::from_nanos(12)
        );

        transaction.set_snapshot_runtime_stats(None);
        transaction.get("later".to_owned()).await.unwrap();
        assert_eq!(stats.rpc_count(crate::SnapshotRpcCommand::Get), 1);
    }

    #[tokio::test]
    async fn source_snapshot_read_sli_does_not_require_runtime_stats() {
        let before = crate::stats::snapshot_read_sli_sample_counts();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::GetRequest>());
                Ok(Box::new(kvrpcpb::GetResponse {
                    value: b"v".to_vec(),
                    exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                        time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                            kv_read_wall_time_ns: 1_000_000,
                            ..Default::default()
                        }),
                        scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                            processed_versions_size: 1,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        assert_eq!(
            transaction.get("k".to_owned()).await.unwrap(),
            Some(b"v".to_vec())
        );
        let after = crate::stats::snapshot_read_sli_sample_counts();
        assert!(after.0 >= before.0 + 1);
        assert!(after.1 >= before.1);
    }

    #[tokio::test]
    async fn source_snapshot_runtime_stats_record_region_retry_classes() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = Arc::clone(&attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::GetRequest>());
                if captured_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            server_is_busy: Some(Default::default()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::new(kvrpcpb::GetResponse {
                    value: b"value".to_vec(),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let stats = Arc::new(crate::SnapshotRuntimeStats::new());
        transaction.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));

        assert_eq!(
            transaction.get("key".to_owned()).await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(stats.backoff_count("tikvServerBusy"), 1);
        assert!(stats.backoff_duration("tikvServerBusy") >= Duration::from_millis(1_000));
        assert!(stats.backoff_duration("tikvServerBusy") < Duration::from_millis(2_000));
        assert_eq!(stats.request_error_stats().error_count("server_is_busy"), 1);
        let access = stats.replica_access_stats().access_infos();
        assert_eq!(access.len(), 1);
        assert_eq!(access[0].error, "server_is_busy");
    }

    #[tokio::test]
    async fn source_snapshot_key_only_forces_scan_requests_to_omit_values() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::ScanRequest>()
                    .expect("snapshot scan should dispatch ScanRequest");
                assert!(request.key_only);
                Ok(Box::new(kvrpcpb::ScanResponse::default()) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_key_only(true);

        let pairs: Vec<_> = transaction
            .scan(b"a".to_vec()..b"b".to_vec(), 1)
            .await
            .unwrap()
            .collect();

        assert!(pairs.is_empty());
    }

    #[test]
    fn source_snapshot_pipelined_marks_its_flushed_lock_resolved() {
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        transaction.set_snapshot_pipelined(42);

        assert_eq!(
            transaction.read_lock_context.snapshot(),
            (vec![42], Vec::new())
        );
    }

    #[tokio::test]
    async fn source_snapshot_context_settings_reach_all_read_requests() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured_calls = Arc::clone(&calls);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let (expected_timeout, context) = if let Some(request) =
                    request.downcast_ref::<kvrpcpb::GetRequest>()
                {
                    captured_calls.lock().unwrap().push("get");
                    (17, request.context.as_ref().unwrap())
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    captured_calls.lock().unwrap().push("batch-get");
                    (17, request.context.as_ref().unwrap())
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                    captured_calls.lock().unwrap().push("scan");
                    (60_000, request.context.as_ref().unwrap())
                } else {
                    panic!("unexpected request while testing snapshot context settings");
                };
                assert!(context.not_fill_cache);
                assert_eq!(context.isolation_level, kvrpcpb::IsolationLevel::Rc as i32);
                assert_eq!(context.task_id, 42);
                assert_eq!(context.max_execution_duration_ms, expected_timeout);
                assert_eq!(context.resource_group_tag, b"snapshot-tag");
                assert_eq!(context.request_source, "internal_snapshot_explicit");

                if request.is::<kvrpcpb::GetRequest>() {
                    Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::BatchGetRequest>() {
                    Ok(Box::new(kvrpcpb::BatchGetResponse::default()) as Box<dyn Any>)
                } else {
                    Ok(Box::new(kvrpcpb::ScanResponse::default()) as Box<dyn Any>)
                }
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_not_fill_cache(true);
        transaction.set_isolation_level(kvrpcpb::IsolationLevel::Rc);
        transaction.set_task_id(42);
        transaction.set_snapshot_read_timeout(Duration::from_millis(17));
        transaction.set_resource_group_tag(Some(b"snapshot-tag".to_vec()));
        transaction.set_request_source_internal(true);
        transaction.set_request_source_type("snapshot");
        transaction.set_explicit_request_source_type("explicit");
        assert!(transaction.request_source().is_internal());

        transaction.get("get".to_owned()).await.unwrap();
        let _: Vec<_> = transaction
            .batch_get(vec!["batch".to_owned()])
            .await
            .unwrap()
            .collect();
        let _: Vec<_> = transaction
            .scan(b"scan-a".to_vec()..b"scan-b".to_vec(), 1)
            .await
            .unwrap()
            .collect();

        assert_eq!(*calls.lock().unwrap(), vec!["get", "batch-get", "scan"]);
    }

    #[tokio::test]
    async fn source_snapshot_default_read_timeouts_are_short_for_get_and_medium_for_scans() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured_calls = Arc::clone(&calls);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let (name, expected_timeout, context, response): (
                    &str,
                    u64,
                    &kvrpcpb::Context,
                    Box<dyn Any>,
                ) = if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    (
                        "get",
                        30_000,
                        request.context.as_ref().unwrap(),
                        Box::new(kvrpcpb::GetResponse::default()),
                    )
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    (
                        "batch-get",
                        60_000,
                        request.context.as_ref().unwrap(),
                        Box::new(kvrpcpb::BatchGetResponse::default()),
                    )
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                    (
                        "scan",
                        60_000,
                        request.context.as_ref().unwrap(),
                        Box::new(kvrpcpb::ScanResponse::default()),
                    )
                } else {
                    panic!("unexpected request while testing snapshot default read timeouts");
                };
                assert_eq!(context.max_execution_duration_ms, expected_timeout);
                captured_calls.lock().unwrap().push(name);
                Ok(response)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        transaction.get("get".to_owned()).await.unwrap();
        let _: Vec<_> = transaction
            .batch_get(vec!["batch".to_owned()])
            .await
            .unwrap()
            .collect();
        let _: Vec<_> = transaction
            .scan(b"scan-a".to_vec()..b"scan-b".to_vec(), 1)
            .await
            .unwrap()
            .collect();

        assert_eq!(*calls.lock().unwrap(), vec!["get", "batch-get", "scan"]);
    }

    #[tokio::test]
    async fn source_snapshot_resource_group_tagger_tags_each_read_and_yields_to_static_tag() {
        let tagged_requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&tagged_requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let (expected_tag, response): (&[u8], Box<dyn Any>) = if let Some(request) =
                    request.downcast_ref::<kvrpcpb::GetRequest>()
                {
                    let tag = request
                        .context
                        .as_ref()
                        .unwrap()
                        .resource_group_tag
                        .as_slice();
                    let response: Box<dyn Any> = Box::new(kvrpcpb::GetResponse::default());
                    (tag, response)
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    let tag = request
                        .context
                        .as_ref()
                        .unwrap()
                        .resource_group_tag
                        .as_slice();
                    let response: Box<dyn Any> = Box::new(kvrpcpb::BatchGetResponse::default());
                    (tag, response)
                } else if let Some(request) =
                    request.downcast_ref::<kvrpcpb::BufferBatchGetRequest>()
                {
                    let tag = request
                        .context
                        .as_ref()
                        .unwrap()
                        .resource_group_tag
                        .as_slice();
                    let response: Box<dyn Any> =
                        Box::new(kvrpcpb::BufferBatchGetResponse::default());
                    (tag, response)
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                    let tag = request
                        .context
                        .as_ref()
                        .unwrap()
                        .resource_group_tag
                        .as_slice();
                    let response: Box<dyn Any> = Box::new(kvrpcpb::ScanResponse::default());
                    (tag, response)
                } else {
                    panic!("unexpected request while testing snapshot resource-group tagger");
                };
                captured_requests
                    .lock()
                    .unwrap()
                    .push(expected_tag.to_vec());
                Ok(response)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let tagged_kinds = Arc::new(Mutex::new(Vec::new()));
        let captured_kinds = Arc::clone(&tagged_kinds);
        transaction.set_resource_group_tagger(Some(Arc::new(move |request_type| {
            captured_kinds.lock().unwrap().push(request_type);
            match request_type {
                SnapshotRequestType::Get => b"tag-get".to_vec(),
                SnapshotRequestType::BatchGet => b"tag-batch-get".to_vec(),
                SnapshotRequestType::BufferBatchGet => b"tag-buffer-batch-get".to_vec(),
                SnapshotRequestType::Scan => b"tag-scan".to_vec(),
            }
        })));

        transaction.get("get".to_owned()).await.unwrap();
        let _: Vec<_> = transaction
            .batch_get(vec!["batch".to_owned()])
            .await
            .unwrap()
            .collect();
        transaction.set_snapshot_pipelined(1);
        let _: Vec<_> = transaction
            .batch_get_from_buffer(vec!["buffer-batch".to_owned()])
            .await
            .unwrap()
            .collect();
        let _: Vec<_> = transaction
            .scan(b"scan-a".to_vec()..b"scan-b".to_vec(), 1)
            .await
            .unwrap()
            .collect();

        transaction.set_resource_group_tag(Some(b"static".to_vec()));
        transaction.get("static".to_owned()).await.unwrap();

        assert_eq!(
            *tagged_kinds.lock().unwrap(),
            vec![
                SnapshotRequestType::Get,
                SnapshotRequestType::BatchGet,
                SnapshotRequestType::BufferBatchGet,
                SnapshotRequestType::Scan,
            ]
        );
        assert_eq!(
            *tagged_requests.lock().unwrap(),
            vec![
                b"tag-get".to_vec(),
                b"tag-batch-get".to_vec(),
                b"tag-buffer-batch-get".to_vec(),
                b"tag-scan".to_vec(),
                b"static".to_vec(),
            ]
        );
    }

    #[tokio::test]
    async fn source_snapshot_buffer_batch_get_requires_pipelined_mode() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                let request = request
                    .downcast_ref::<kvrpcpb::BufferBatchGetRequest>()
                    .expect("pipelined buffer reads must use BufferBatchGet");
                assert_eq!(request.version, 1);
                assert_eq!(request.keys, [b"buffer".to_vec()]);
                Ok(Box::new(kvrpcpb::BufferBatchGetResponse {
                    pairs: vec![kvrpcpb::KvPair {
                        key: b"buffer".to_vec(),
                        value: b"value".to_vec(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let error = match transaction
            .batch_get_from_buffer(vec!["buffer".to_owned()])
            .await
        {
            Ok(_) => panic!("unpipelined snapshots must reject buffer-tier reads"),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "only snapshot with pipelined dml can read from buffer"
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);

        transaction.set_snapshot_pipelined(1);
        let pairs: Vec<_> = transaction
            .batch_get_from_buffer(vec!["buffer".to_owned()])
            .await
            .unwrap()
            .collect();
        assert_eq!(
            pairs,
            [KvPair(b"buffer".to_vec().into(), b"value".to_vec())]
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_snapshot_scope_validates_physical_reads_before_dispatch() {
        let validations = Arc::new(Mutex::new(Vec::new()));
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                if request.is::<kvrpcpb::GetRequest>() {
                    Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::BatchGetRequest>() {
                    Ok(Box::new(kvrpcpb::BatchGetResponse::default()) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::ScanRequest>() {
                    Ok(Box::new(kvrpcpb::ScanResponse::default()) as Box<dyn Any>)
                } else {
                    panic!("unexpected request while testing snapshot timestamp validation");
                }
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(7),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_read_timestamp_validator(Arc::new(RecordingReadTimestampValidator {
            calls: Arc::clone(&validations),
            error: None,
        }));
        transaction.set_read_replica_scope("zone-a");
        transaction.set_stale_read(true);

        transaction.get("get".to_owned()).await.unwrap();
        let _: Vec<_> = transaction
            .batch_get(vec!["batch".to_owned()])
            .await
            .unwrap()
            .collect();
        let _: Vec<_> = transaction
            .scan(b"scan-a".to_vec()..b"scan-b".to_vec(), 1)
            .await
            .unwrap()
            .collect();

        assert_eq!(dispatches.load(Ordering::SeqCst), 3);
        assert_eq!(
            *validations.lock().unwrap(),
            vec![
                (7, true, "zone-a".to_owned()),
                (7, true, "zone-a".to_owned()),
                (7, true, String::new()),
            ]
        );
    }

    #[tokio::test]
    async fn source_snapshot_timestamp_validation_failure_prevents_transport_dispatch() {
        let validations = Arc::new(Mutex::new(Vec::new()));
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_request: &dyn Any| {
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                unreachable!("timestamp validation must run before transport dispatch")
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(9),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_read_timestamp_validator(Arc::new(RecordingReadTimestampValidator {
            calls: Arc::clone(&validations),
            error: Some("read timestamp is unsafe"),
        }));
        transaction.set_read_replica_scope("zone-b");

        let error = transaction.get("get".to_owned()).await.unwrap_err();
        assert_eq!(error.to_string(), "read timestamp is unsafe");
        assert_eq!(dispatches.load(Ordering::SeqCst), 0);
        assert_eq!(
            *validations.lock().unwrap(),
            vec![(9, false, "zone-b".to_owned())]
        );
    }

    #[tokio::test]
    async fn source_snapshot_visibility_runs_after_successful_physical_reads() {
        let visibility_calls = Arc::new(Mutex::new(Vec::new()));
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::GetRequest>() {
                    Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"value".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::BatchGetRequest>() {
                    Ok(Box::new(kvrpcpb::BatchGetResponse::default()) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::BufferBatchGetRequest>() {
                    Ok(Box::new(kvrpcpb::BufferBatchGetResponse::default()) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::ScanRequest>() {
                    Ok(Box::new(kvrpcpb::ScanResponse::default()) as Box<dyn Any>)
                } else {
                    panic!("unexpected request while testing snapshot visibility");
                }
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(11),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_visibility_validator(Arc::new(
            RecordingSnapshotVisibilityValidator {
                calls: Arc::clone(&visibility_calls),
                error: None,
            },
        ));

        transaction.get("get".to_owned()).await.unwrap();
        let _: Vec<_> = transaction
            .batch_get(vec!["batch".to_owned()])
            .await
            .unwrap()
            .collect();
        transaction.set_snapshot_pipelined(11);
        let _: Vec<_> = transaction
            .batch_get_from_buffer(vec!["buffer".to_owned()])
            .await
            .unwrap()
            .collect();
        let _: Vec<_> = transaction
            .scan(b"a".to_vec()..b"z".to_vec(), 1)
            .await
            .unwrap()
            .collect();

        assert_eq!(*visibility_calls.lock().unwrap(), [11, 11, 11, 11]);
    }

    #[tokio::test]
    async fn source_snapshot_visibility_failure_is_not_cached() {
        let visibility_calls = Arc::new(Mutex::new(Vec::new()));
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::GetRequest>());
                Ok(Box::new(kvrpcpb::GetResponse {
                    value: b"unsafe".to_vec(),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(12),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_visibility_validator(Arc::new(
            RecordingSnapshotVisibilityValidator {
                calls: Arc::clone(&visibility_calls),
                error: Some("snapshot is older than the transaction safe point"),
            },
        ));

        let error = transaction.get("get".to_owned()).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "snapshot is older than the transaction safe point"
        );
        assert_eq!(*visibility_calls.lock().unwrap(), [12]);
        assert_eq!(transaction.snapshot_cache_size(), 0);
    }

    #[test]
    fn transaction_priority_defaults_to_normal_and_has_a_builder() {
        assert_eq!(
            TransactionOptions::new_optimistic().priority,
            Priority::Normal
        );
        assert_eq!(
            TransactionOptions::new_pessimistic().priority,
            Priority::Normal
        );
        assert_eq!(
            TransactionOptions::new_optimistic()
                .priority(Priority::Low)
                .priority,
            Priority::Low
        );
    }

    #[test]
    fn snapshot_replica_read_configuration_defaults_to_leader_and_is_replaceable() {
        let pd_client = Arc::new(MockPdClient::default());
        let mut transaction = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        assert_eq!(
            transaction.replica_read_config,
            ReplicaReadConfig::default()
        );

        transaction.set_replica_read_config(ReplicaReadConfig {
            read_type: ReplicaReadType::Follower,
            ..Default::default()
        });
        assert_eq!(
            transaction.replica_read_config.read_type,
            ReplicaReadType::Follower
        );

        transaction.set_stale_read(true);
        transaction.set_match_store_labels(vec![crate::proto::metapb::StoreLabel {
            key: "zone".to_owned(),
            value: "east".to_owned(),
        }]);
        assert!(transaction.replica_read_config.stale_read);
        assert_eq!(
            transaction.replica_read_config.labels,
            vec![crate::proto::metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "east".to_owned(),
            }]
        );

        transaction.set_load_based_replica_read_threshold(Duration::from_millis(50));
        assert_eq!(transaction.replica_read_config.busy_threshold_ms, 50);
        transaction.set_load_based_replica_read_threshold(Duration::ZERO);
        assert_eq!(transaction.replica_read_config.busy_threshold_ms, 0);
        transaction
            .set_load_based_replica_read_threshold(Duration::from_millis(u64::from(u32::MAX) + 1));
        assert_eq!(transaction.replica_read_config.busy_threshold_ms, 0);

        transaction.set_replica_read_adjuster(Arc::new(|item_count| {
            assert_eq!(item_count, 3);
            ReplicaReadAdjustment::new(
                Some(ReplicaReadSelectorOption::MatchStores(vec![7, 8])),
                ReplicaReadType::Mixed,
            )
        }));
        let adjusted = transaction.replica_read_config_for_items(3);
        assert_eq!(adjusted.read_type, ReplicaReadType::Mixed);
        assert_eq!(adjusted.stores, vec![7, 8]);
        assert_eq!(
            transaction.replica_read_config.read_type,
            ReplicaReadType::Follower
        );
        assert!(transaction.replica_read_config.stores.is_empty());

        transaction.set_replica_read_config(ReplicaReadConfig::default());
        assert_eq!(
            transaction.replica_read_config_for_items(3),
            ReplicaReadConfig::default()
        );
    }

    #[tokio::test]
    async fn transaction_priority_reaches_read_and_write_request_contexts() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_by_hook = Arc::clone(&observed);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    observed_by_hook
                        .lock()
                        .unwrap()
                        .push(("get", req.context.as_ref().unwrap().priority));
                    Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    observed_by_hook
                        .lock()
                        .unwrap()
                        .push(("prewrite", req.context.as_ref().unwrap().priority));
                    Ok(Box::new(kvrpcpb::PrewriteResponse::default()) as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    observed_by_hook
                        .lock()
                        .unwrap()
                        .push(("commit", req.context.as_ref().unwrap().priority));
                    Ok(Box::new(kvrpcpb::CommitResponse::default()) as Box<dyn Any>)
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>() {
                    observed_by_hook
                        .lock()
                        .unwrap()
                        .push(("heartbeat", req.context.as_ref().unwrap().priority));
                    Ok(Box::new(kvrpcpb::TxnHeartBeatResponse::default()) as Box<dyn Any>)
                } else {
                    panic!("unexpected request while testing transaction priority")
                }
            },
        )));

        let mut txn = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().priority(Priority::Low),
            Keyspace::Disable,
        );
        txn.get("read".to_owned()).await.unwrap();
        txn.set_priority(Priority::High);
        txn.put("write".to_owned(), "value").await.unwrap();
        txn.send_heart_beat().await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                ("get", kvrpcpb::CommandPri::Low as i32),
                ("heartbeat", kvrpcpb::CommandPri::Normal as i32),
                ("prewrite", kvrpcpb::CommandPri::High as i32),
                ("commit", kvrpcpb::CommandPri::High as i32),
            ]
        );
    }

    #[tokio::test]
    async fn transactional_read_retries_with_client_go_lock_hints() {
        let get_attempts = Arc::new(AtomicUsize::new(0));
        let get_attempts_by_hook = Arc::clone(&get_attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let attempt = get_attempts_by_hook.fetch_add(1, Ordering::SeqCst);
                    let context = req.context.as_ref().unwrap();
                    if attempt == 0 {
                        assert!(context.resolved_locks.is_empty());
                        assert!(context.committed_locks.is_empty());
                        return Ok(Box::new(kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: b"read".to_vec(),
                                    primary_lock: b"read".to_vec(),
                                    lock_version: 1,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    assert_eq!(context.committed_locks, [1]);
                    assert!(context.resolved_locks.is_empty());
                    return Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    std::thread::sleep(Duration::from_millis(2));
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request while resolving a transactional read lock");
            },
        )));
        let mut txn = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let stats = Arc::new(crate::SnapshotRuntimeStats::new());
        txn.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));

        assert_eq!(txn.get("read".to_owned()).await.unwrap(), Some(Vec::new()));
        assert_eq!(get_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(stats.rpc_count(crate::SnapshotRpcCommand::Get), 2);
        assert!(stats.resolve_lock_duration() >= Duration::from_millis(2));
    }

    #[tokio::test]
    async fn transactional_read_uses_snapshot_retry_variables_for_txn_lock_fast() {
        let lock_version = Timestamp {
            physical: 10,
            logical: 0,
            ..Default::default()
        }
        .version();
        let get_attempts = Arc::new(AtomicUsize::new(0));
        let get_attempts_by_hook = Arc::clone(&get_attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::GetRequest>() {
                    if get_attempts_by_hook.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Ok(Box::new(kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: b"read".to_vec(),
                                    primary_lock: b"read".to_vec(),
                                    lock_version,
                                    lock_ttl: 5,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    return Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 5,
                        lock_info: Some(kvrpcpb::LockInfo {
                            key: b"read".to_vec(),
                            primary_lock: b"read".to_vec(),
                            lock_version,
                            lock_ttl: 5,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected request while waiting for a snapshot read lock");
            },
        )));
        pd_client.set_timestamp(Timestamp {
            physical: 10,
            logical: 0,
            ..Default::default()
        });
        let mut txn = Transaction::new(
            Timestamp {
                physical: 10,
                logical: 1,
                ..Default::default()
            },
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let mut variables = crate::Variables::default();
        variables.backoff_lock_fast = 2;
        variables.backoff_weight = 1;
        txn.set_snapshot_variables(Arc::new(variables));
        let stats = Arc::new(crate::SnapshotRuntimeStats::new());
        txn.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));

        assert_eq!(txn.get("read".to_owned()).await.unwrap(), Some(Vec::new()));
        assert_eq!(get_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(stats.backoff_count("txnLockFast"), 1);
        assert_eq!(
            stats.backoff_duration("txnLockFast"),
            Duration::from_millis(1)
        );
    }

    #[tokio::test]
    async fn transactional_reads_share_the_client_lock_resolver_status_cache() {
        let get_attempts = Arc::new(Mutex::new(HashMap::<Vec<u8>, usize>::new()));
        let get_attempts_by_hook = Arc::clone(&get_attempts);
        let status_checks = Arc::new(AtomicUsize::new(0));
        let status_checks_by_hook = Arc::clone(&status_checks);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    let first_attempt = {
                        let mut attempts = get_attempts_by_hook.lock().unwrap();
                        let attempt = attempts.entry(req.key.clone()).or_insert(0);
                        *attempt += 1;
                        *attempt == 1
                    };
                    if first_attempt {
                        return Ok(Box::new(kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: req.key.clone(),
                                    primary_lock: b"primary".to_vec(),
                                    lock_version: 1,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    return Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    status_checks_by_hook.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request while testing resolver status sharing");
            },
        )));
        let shared_context = ResolveLocksContext::default();
        let mut txn = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        txn.set_lock_resolver_context(shared_context);

        assert_eq!(txn.get("first".to_owned()).await.unwrap(), Some(Vec::new()));
        assert_eq!(
            txn.get("second".to_owned()).await.unwrap(),
            Some(Vec::new())
        );
        assert_eq!(status_checks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn transactional_write_retries_share_the_client_lock_resolver_status_cache() {
        let heartbeat_attempts = Arc::new(AtomicUsize::new(0));
        let heartbeat_attempts_by_hook = Arc::clone(&heartbeat_attempts);
        let status_checks = Arc::new(AtomicUsize::new(0));
        let status_checks_by_hook = Arc::clone(&status_checks);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::TxnHeartBeatRequest>() {
                    let attempt = heartbeat_attempts_by_hook.fetch_add(1, Ordering::SeqCst);
                    if attempt % 2 == 0 {
                        return Ok(Box::new(kvrpcpb::TxnHeartBeatResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: b"write".to_vec(),
                                    primary_lock: b"write".to_vec(),
                                    lock_version: 1,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    return Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    status_checks_by_hook.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if req.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request while testing write resolver status sharing");
            },
        )));
        let mut txn = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_lock_resolver_context(ResolveLocksContext::default());
        txn.put("write".to_owned(), "value".to_owned())
            .await
            .unwrap();

        txn.send_heart_beat().await.unwrap();
        txn.send_heart_beat().await.unwrap();

        assert_eq!(heartbeat_attempts.load(Ordering::SeqCst), 4);
        assert_eq!(status_checks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn transaction_rpc_interceptor_wraps_each_commit_rpc() {
        let intercepted = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&intercepted);
        let interceptor = new_rpc_interceptor("record", move |target, request, next| {
            let observed = Arc::clone(&observed);
            Box::pin(async move {
                observed
                    .lock()
                    .unwrap()
                    .push((target.to_owned(), request.label()));
                next().await
            })
        });
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                if req.is::<kvrpcpb::PrewriteRequest>() {
                    Ok(Box::new(kvrpcpb::PrewriteResponse::default()) as Box<dyn Any>)
                } else if req.is::<kvrpcpb::CommitRequest>() {
                    Ok(Box::new(kvrpcpb::CommitResponse::default()) as Box<dyn Any>)
                } else {
                    panic!("unexpected request while testing transaction interceptor")
                }
            },
        )));
        let mut txn = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
        );
        txn.set_rpc_interceptor(interceptor);
        txn.put("key".to_owned(), "value").await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(
            *intercepted.lock().unwrap(),
            [(String::new(), "kv_prewrite"), (String::new(), "kv_commit")]
        );
    }

    #[tokio::test]
    async fn transaction_resource_control_charges_and_settles_each_physical_rpc() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let controller = Arc::new(RecordingResourceController {
            events: Arc::clone(&events),
        });
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                let context = if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    req.context.as_ref().unwrap()
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    req.context.as_ref().unwrap()
                } else {
                    panic!("unexpected request while testing resource control")
                };
                let resource_control = context.resource_control_context.as_ref().unwrap();
                assert_eq!(resource_control.resource_group_name, "test-rg");
                assert_eq!(resource_control.override_priority, 7);
                assert_eq!(resource_control.penalty.as_ref().unwrap().r_r_u, 1.0);
                if req.is::<kvrpcpb::PrewriteRequest>() {
                    Ok(Box::new(kvrpcpb::PrewriteResponse::default()) as Box<dyn Any>)
                } else {
                    Ok(Box::new(kvrpcpb::CommitResponse::default()) as Box<dyn Any>)
                }
            },
        )));
        let mut txn = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
        );
        txn.set_resource_group("test-rg");
        txn.set_resource_control(controller);
        let ru_details = Arc::new(crate::RuDetails::new());
        txn.set_ru_details(ru_details.clone());
        txn.put("key".to_owned(), "value").await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(
            *events.lock().unwrap(),
            ["request", "response", "request", "response"]
        );
        assert_eq!(ru_details.read_ru(), 14.0);
        assert_eq!(ru_details.write_ru(), 20.0);
        assert_eq!(ru_details.ru_wait_duration(), Duration::from_millis(10));
    }

    #[tokio::test]
    async fn transaction_resource_control_does_not_settle_transport_failures() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let controller = Arc::new(RecordingResourceController {
            events: Arc::clone(&events),
        });
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::StringError("simulated transport failure".to_owned()))
        })));
        let mut txn = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
        );
        txn.set_resource_group("test-rg");
        txn.set_resource_control(controller);

        assert!(txn.get("key".to_owned()).await.is_err());
        assert_eq!(*events.lock().unwrap(), ["request"]);
        txn.set_status(TransactionStatus::Rolledback);
    }

    #[tokio::test]
    async fn global_resource_control_requires_enable_and_uses_source_group_name() {
        let _global_test_guard = GLOBAL_RESOURCE_CONTROL_TEST_LOCK.lock().unwrap();
        disable_resource_control();
        unset_resource_control_interceptor();
        let events = Arc::new(Mutex::new(Vec::new()));
        set_resource_control_interceptor(Arc::new(RecordingResourceController {
            events: Arc::clone(&events),
        }));
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                let request = req.downcast_ref::<kvrpcpb::GetRequest>().unwrap();
                assert_eq!(
                    request
                        .context
                        .as_ref()
                        .and_then(|context| context.resource_control_context.as_ref())
                        .unwrap()
                        .resource_group_name,
                    "test-rg"
                );
                Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
            },
        )));
        let mut disabled = Transaction::new(
            Timestamp::from_version(1),
            pd_client.clone(),
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
        );
        disabled.set_resource_group_name("test-rg");
        disabled.get("disabled".to_owned()).await.unwrap();
        disabled.set_status(TransactionStatus::Rolledback);
        assert!(events.lock().unwrap().is_empty());

        enable_resource_control();
        let mut enabled = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
        );
        enabled.set_resource_group_name("test-rg");
        enabled.get("enabled".to_owned()).await.unwrap();
        enabled.set_status(TransactionStatus::Rolledback);
        disable_resource_control();
        unset_resource_control_interceptor();

        assert_eq!(*events.lock().unwrap(), ["request", "response"]);
    }

    #[tokio::test]
    async fn transaction_keyspace_name_reaches_read_and_two_pc_request_contexts() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_by_hook = Arc::clone(&observed);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                let context = if let Some(req) = req.downcast_ref::<kvrpcpb::GetRequest>() {
                    observed_by_hook.lock().unwrap().push("get");
                    req.context.as_ref().unwrap()
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    observed_by_hook.lock().unwrap().push("prewrite");
                    req.context.as_ref().unwrap()
                } else if let Some(req) = req.downcast_ref::<kvrpcpb::CommitRequest>() {
                    observed_by_hook.lock().unwrap().push("commit");
                    req.context.as_ref().unwrap()
                } else {
                    panic!("unexpected request while testing transaction keyspace context")
                };
                assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
                assert_eq!(crate::request::context_keyspace_id(context), Some(7));
                assert_eq!(context.keyspace_name, "tenant");

                if req.is::<kvrpcpb::GetRequest>() {
                    Ok(Box::new(kvrpcpb::GetResponse::default()) as Box<dyn Any>)
                } else if req.is::<kvrpcpb::PrewriteRequest>() {
                    Ok(Box::new(kvrpcpb::PrewriteResponse::default()) as Box<dyn Any>)
                } else {
                    Ok(Box::new(kvrpcpb::CommitResponse::default()) as Box<dyn Any>)
                }
            },
        )));
        let mut txn = Transaction::new_with_latches_and_keyspace_name(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic(),
            Keyspace::try_enable(7).unwrap(),
            Some("tenant".to_owned()),
            None,
        );

        txn.get("read".to_owned()).await.unwrap();
        txn.put("write".to_owned(), "value".to_owned())
            .await
            .unwrap();
        txn.commit().await.unwrap();
        assert_eq!(*observed.lock().unwrap(), vec!["get", "prewrite", "commit"]);
    }

    #[tokio::test]
    async fn api_v2_batch_get_and_scan_reencode_only_at_the_physical_buffer_boundary() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured_calls = Arc::clone(&calls);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if let Some(req) = req.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    captured_calls.lock().unwrap().push("batch-get");
                    assert_eq!(req.keys, vec![b"x\0\0\x07a"]);
                    let context = req.context.as_ref().unwrap();
                    assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
                    assert_eq!(crate::request::context_keyspace_id(context), Some(7));
                    return Ok(Box::new(kvrpcpb::BatchGetResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: req.keys[0].clone(),
                            value: b"batch".to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(req) = req.downcast_ref::<kvrpcpb::ScanRequest>() {
                    captured_calls.lock().unwrap().push("scan");
                    assert_eq!(req.start_key, b"x\0\0\x07b");
                    assert_eq!(req.end_key, b"x\0\0\x07c");
                    return Ok(Box::new(kvrpcpb::ScanResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: req.start_key.clone(),
                            value: b"scan".to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected request while testing API-V2 batch/scan decoding")
            },
        )));
        let mut txn = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic(),
            Keyspace::try_enable(7).unwrap(),
        );

        let batch: Vec<_> = txn.batch_get(vec!["a".to_owned()]).await.unwrap().collect();
        let scan: Vec<_> = txn.scan(vec![b'b']..vec![b'c'], 1).await.unwrap().collect();
        assert_eq!(batch, vec![KvPair(vec![b'a'].into(), b"batch".to_vec())]);
        assert_eq!(scan, vec![KvPair(vec![b'b'].into(), b"scan".to_vec())]);
        assert_eq!(*calls.lock().unwrap(), vec!["batch-get", "scan"]);
        txn.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn optimistic_commit_rejects_a_stale_local_latch_before_rpc() {
        let scheduler = crate::transaction::latch::LatchesScheduler::new(8);
        let committed = scheduler.lock(1, vec![b"key".to_vec()]).await;
        committed.set_commit_timestamp(3);
        drop(committed);

        let mut transaction = Transaction::new_with_latches(
            Timestamp::from_version(2),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
            Some(scheduler.clone()),
        );
        transaction
            .put("key".to_owned(), "value".to_owned())
            .await
            .unwrap();
        let error = transaction.commit().await.unwrap_err();
        assert!(matches!(
            error,
            crate::Error::WriteConflictInLatch(crate::error::WriteConflictInLatchError {
                start_timestamp: 2
            })
        ));
        assert_eq!(error.to_string(), "write conflict in latch,startTS: 2");

        // The stale guard is released on the error path, so a restarted
        // transaction with a newer timestamp can acquire the key.
        let restarted = scheduler.lock(4, vec![b"key".to_vec()]).await;
        assert!(!restarted.is_stale());
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    async fn test_optimistic_heartbeat(#[case] keyspace: Keyspace) -> Result<(), io::Error> {
        let scenario = FailScenario::setup();
        fail::cfg("after-prewrite", "sleep(1500)").unwrap();
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let heartbeats_cloned = heartbeats.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>().is_some() {
                    heartbeats_cloned.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                }
            },
        )));
        let key1 = "key1".to_owned();
        let mut heartbeat_txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(1))),
            keyspace,
        );
        heartbeat_txn.put(key1.clone(), "foo").await.unwrap();
        let heartbeat_txn_handle = tokio::task::spawn_blocking(move || {
            assert!(futures::executor::block_on(heartbeat_txn.commit()).is_ok())
        });
        assert_eq!(heartbeats.load(Ordering::SeqCst), 0);
        heartbeat_txn_handle.await.unwrap();
        assert_eq!(heartbeats.load(Ordering::SeqCst), 1);
        scenario.teardown();
        Ok(())
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    async fn test_pessimistic_heartbeat(#[case] keyspace: Keyspace) -> Result<(), io::Error> {
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let heartbeats_cloned = heartbeats.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>().is_some() {
                    heartbeats_cloned.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>)
                } else if req.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else if req
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .is_some()
                {
                    Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>)
                } else {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                }
            },
        )));
        let key1 = "key1".to_owned();
        let mut heartbeat_txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(1))),
            keyspace,
        );
        heartbeat_txn.put(key1.clone(), "foo").await.unwrap();
        assert_eq!(heartbeats.load(Ordering::SeqCst), 0);
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
        assert_eq!(heartbeats.load(Ordering::SeqCst), 1);
        let heartbeat_txn_handle = tokio::spawn(async move {
            assert!(heartbeat_txn.commit().await.is_ok());
        });
        heartbeat_txn_handle.await.unwrap();
        Ok(())
    }

    // A minimal capturing logger (no extra dependency) used to assert that
    // transaction lifecycle logs carry the txn's `start_ts`.
    static CAPTURED_LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static LOGGER: CaptureLogger = CaptureLogger;
    static LOGGER_INIT: Once = Once::new();

    struct CaptureLogger;

    impl log::Log for CaptureLogger {
        fn enabled(&self, _metadata: &log::Metadata) -> bool {
            true
        }

        fn log(&self, record: &log::Record) {
            if let Ok(mut logs) = CAPTURED_LOGS.lock() {
                logs.push(record.args().to_string());
            }
        }

        fn flush(&self) {}
    }

    fn install_capture_logger() {
        LOGGER_INIT.call_once(|| {
            // Ignore the error if another logger is already installed in this
            // process; the assertion below only checks for presence of a unique
            // marker, so foreign records are harmless.
            let _ = log::set_logger(&LOGGER);
            log::set_max_level(log::LevelFilter::Debug);
        });
    }

    #[tokio::test]
    async fn commit_logs_start_ts() {
        install_capture_logger();
        CAPTURED_LOGS.lock().unwrap().clear();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                if req.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                }
            },
        )));

        // A unique start_ts so the assertion cannot be satisfied by any other
        // test's log records if the process is shared (e.g. plain `cargo test`).
        let start_ts = 424242;
        let mut txn = Transaction::new(
            Timestamp::from_version(start_ts),
            pd_client,
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
        );
        txn.put("key1".to_owned(), "value").await.unwrap();
        txn.commit().await.unwrap();

        let logs = CAPTURED_LOGS.lock().unwrap();
        assert!(
            logs.iter()
                .any(|line| line.contains("start_ts") && line.contains(&start_ts.to_string())),
            "expected a lifecycle log carrying start_ts {start_ts}; captured: {logs:?}"
        );
    }

    #[tokio::test]
    async fn source_lock_keys_modes_wait_timeout_and_force_lock_results() {
        let mut optimistic = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        optimistic
            .lock_keys(["exclusive".to_owned()])
            .await
            .unwrap();
        optimistic
            .lock_keys_shared(["shared".to_owned()])
            .await
            .unwrap();
        let optimistic_mutations = optimistic.buffer.to_proto_mutations();
        assert_eq!(optimistic_mutations.len(), 2);
        assert!(optimistic_mutations
            .iter()
            .all(|mutation| mutation.op == kvrpcpb::Op::Lock as i32));

        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("lock_keys only sends pessimistic-lock requests");
                captured.lock().unwrap().push((
                    request.mutations[0].op,
                    request.wait_timeout,
                    request.wake_up_mode,
                ));
                let mut response = kvrpcpb::PessimisticLockResponse::default();
                if request.wake_up_mode
                    == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
                {
                    response.results.push(kvrpcpb::PessimisticLockKeyResult {
                        r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                            as i32,
                        existence: true,
                        locked_with_conflict_ts: 9,
                        ..Default::default()
                    });
                }
                Ok(Box::new(response) as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut pessimistic = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        pessimistic.set_pessimistic(true);

        let error = pessimistic
            .lock_keys_shared_with_wait_time(17, ["shared".to_owned()])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("pessimistic lock in share mode requires primary key to be selected"));

        pessimistic
            .lock_keys_with_wait_time(-1, ["primary".to_owned()])
            .await
            .unwrap();
        pessimistic
            .lock_keys_shared_with_wait_time(17, ["shared".to_owned()])
            .await
            .unwrap();
        assert!(pessimistic
            .buffer
            .is_shared_locked(&Key::from(b"shared".to_vec())));
        let error = pessimistic
            .lock_keys(["shared".to_owned()])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("upgrading a shared lock to an exclusive lock is not supported"));

        pessimistic.start_aggressive_locking();
        let error = pessimistic
            .lock_keys_shared(["another-shared".to_owned()])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("shared lock is not supported in aggressive/fair locking mode"));

        let aggressive_observed = Arc::new(Mutex::new(Vec::new()));
        let aggressive_captured = aggressive_observed.clone();
        let aggressive_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("aggressive lock uses PessimisticLock");
                aggressive_captured
                    .lock()
                    .unwrap()
                    .push(request.wake_up_mode);
                Ok(Box::new(kvrpcpb::PessimisticLockResponse {
                    results: vec![kvrpcpb::PessimisticLockKeyResult {
                        r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                            as i32,
                        existence: true,
                        locked_with_conflict_ts: 9,
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        aggressive_rpc.set_timestamp(Timestamp::from_version(2));
        let mut aggressive = Transaction::new(
            Timestamp::from_version(1),
            aggressive_rpc.clone(),
            TransactionOptions::new_pessimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        aggressive.start_aggressive_locking();
        aggressive.lock_keys(["force".to_owned()]).await.unwrap();
        assert!(aggressive.is_in_aggressive_locking_stage("force".to_owned()));
        assert_eq!(
            aggressive.to_string(),
            "1 (aggressiveLocking: prev 0 keys, current 1 keys)"
        );
        aggressive.retry_aggressive_locking().await.unwrap();
        aggressive_rpc.set_timestamp(Timestamp::from_version(10));
        aggressive.lock_keys(["force".to_owned()]).await.unwrap();
        assert!(aggressive.is_in_aggressive_locking_stage("force".to_owned()));
        aggressive.done_aggressive_locking().await.unwrap();
        assert_eq!(
            *aggressive_observed.lock().unwrap(),
            vec![kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32]
        );
        assert!(matches!(
            aggressive.options.kind,
            super::TransactionKind::Pessimistic(ref timestamp) if timestamp.version() == 9
        ));

        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                (
                    kvrpcpb::Op::PessimisticLock as i32,
                    -1,
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal as i32,
                ),
                (
                    kvrpcpb::Op::SharedPessimisticLock as i32,
                    17,
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal as i32,
                ),
            ]
        );
    }

    #[tokio::test]
    async fn source_lock_context_fields_results_callbacks_and_preflight_errors() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = dispatches.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("LockContext sends PessimisticLock");
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                captured.lock().unwrap().push((
                    request.is_first_lock,
                    request.wait_timeout,
                    request.return_values,
                    request.check_existence,
                    request.lock_only_if_exists,
                    request.min_commit_ts,
                    request.context.as_ref().unwrap().resource_group_tag.clone(),
                ));
                Ok(Box::new(kvrpcpb::PessimisticLockResponse {
                    results: vec![kvrpcpb::PessimisticLockKeyResult {
                        r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                            as i32,
                        value: b"value".to_vec(),
                        existence: true,
                        locked_with_conflict_ts: 9,
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let mut context = crate::LockContext::new(8, 1_000, std::time::SystemTime::now());
        context.init_return_values(1);
        context.init_check_existence(1);
        context.resource_group_tag = b"lock-context".to_vec();
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let captured_callbacks = callback_calls.clone();
        transaction
            .lock_keys_with_context_and_callback(&mut context, ["key".to_owned()], move || {
                captured_callbacks.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .unwrap();
        assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
        assert_eq!(context.max_locked_with_conflict_ts, 9);
        assert_eq!(
            context.value_not_locked(b"key"),
            (Some(b"value".to_vec()), true)
        );
        let request = observed.lock().unwrap()[0].clone();
        assert!(request.0);
        assert!((1..=1_000).contains(&request.1));
        assert_eq!(
            (request.2, request.3, request.4, request.5, request.6),
            (true, true, false, 9, b"lock-context".to_vec())
        );

        let mut duplicate = crate::LockContext::new(10, 0, std::time::SystemTime::now());
        duplicate.init_return_values(1);
        transaction
            .lock_keys_with_context(&mut duplicate, ["key".to_owned()])
            .await
            .unwrap();
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(duplicate.value_not_locked(b"key"), (None, false));

        let mut no_values = crate::LockContext::new(10, 0, std::time::SystemTime::now());
        no_values.lock_only_if_exists = true;
        let error = transaction
            .lock_keys_with_context(&mut no_values, ["other".to_owned()])
            .await
            .unwrap_err();
        assert!(matches!(error, Error::LockOnlyIfExistsNoReturnValue(_)));

        let mut expired = crate::LockContext::new(10, 100, std::time::SystemTime::now());
        expired.max_execution_deadline = Some(
            std::time::SystemTime::now()
                .checked_sub(Duration::from_millis(1))
                .unwrap(),
        );
        let error = transaction
            .lock_keys_with_context(&mut expired, ["expired".to_owned()])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::QueryInterruptedWithSignal(crate::error::QueryInterruptedWithSignalError {
                signal: 2
            })
        ));
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_lock_context_lock_only_if_exists_and_deadlock_callback() {
        let missing_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("lock-only-if-exists sends PessimisticLock");
                assert!(request.lock_only_if_exists);
                Ok(Box::new(kvrpcpb::PessimisticLockResponse {
                    results: vec![kvrpcpb::PessimisticLockKeyResult {
                        existence: false,
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut missing = Transaction::new(
            Timestamp::from_version(1),
            missing_rpc,
            TransactionOptions::new_pessimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let mut context = crate::LockContext::new(2, 0, std::time::SystemTime::now());
        context.init_return_values(1);
        context.lock_only_if_exists = true;
        missing
            .lock_keys_with_context(&mut context, ["missing".to_owned()])
            .await
            .unwrap();
        assert_eq!(
            context.value_not_locked(b"missing"),
            (Some(Vec::new()), true)
        );
        assert!(missing.buffer.get_primary_key().is_none());
        assert!(!missing.buffer.is_locked(&Key::from(b"missing".to_vec())));

        let key = b"deadlock-key".to_vec();
        let key_hash = farmhash::fingerprint64(&key);
        let deadlock_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_| {
                Ok(Box::new(kvrpcpb::PessimisticLockResponse {
                    errors: vec![kvrpcpb::KeyError {
                        deadlock: Some(kvrpcpb::Deadlock {
                            deadlock_key_hash: key_hash,
                            deadlock_key: key.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut deadlocked = Transaction::new(
            Timestamp::from_version(1),
            deadlock_rpc,
            TransactionOptions::new_pessimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let callback = Arc::new(Mutex::new(None));
        let captured = callback.clone();
        let mut context = crate::LockContext::new(2, 0, std::time::SystemTime::now());
        context.on_deadlock = Some(Arc::new(move |error| {
            *captured.lock().unwrap() = Some(error.clone());
        }));
        let error = deadlocked
            .lock_keys_with_context(&mut context, ["deadlock-key".to_owned()])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::Deadlock(crate::error::DeadlockError {
                is_retryable: true,
                ..
            })
        ));
        assert!(callback.lock().unwrap().as_ref().unwrap().is_retryable);
    }

    #[tokio::test]
    async fn source_shared_lock_committer_incompatibilities() {
        let shared = source_test_mutation("shared", kvrpcpb::Op::SharedLock);
        let options = TransactionOptions::new_optimistic()
            .use_async_commit()
            .try_one_pc();
        let mut committer = source_test_committer(
            Arc::new(MockPdClient::default()),
            Some(Key::from(b"shared".to_vec())),
            vec![shared],
            options,
            CommitSettings::default(),
        );
        committer.configure_commit_protocols();
        assert!(!committer.options.async_commit);
        assert!(!committer.options.try_one_pc);
        assert!(!committer.should_use_txn_file());

        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let shared_key = Key::from(b"shared".to_vec());
        transaction.buffer.primary_key_or(&shared_key);
        transaction.buffer.lock_shared(shared_key).unwrap();
        let error = transaction.commit().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("shared lock key cannot be used as transaction primary key"));

        let mut pipelined = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic()
                .pipelined(super::PipelinedTxnOptions {
                    enable: true,
                    flush_concurrency: 1,
                    resolve_lock_concurrency: 1,
                    write_throttle_ratio: 0.0,
                })
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let shared_key = Key::from(b"shared".to_vec());
        pipelined.buffer.primary_key_or(&shared_key);
        pipelined.buffer.lock_shared(shared_key).unwrap();
        let error = pipelined.maybe_flush_pipelined(true).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("shared lock is not supported in pipelined transaction"));
    }

    #[tokio::test]
    async fn source_pipelined_multi_generation_flushes_only_changed_delta() {
        let flushed = Arc::new(Mutex::new(Vec::new()));
        let captured = flushed.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::FlushRequest>()
                    .expect("forced pipelined flush only sends Flush");
                captured.lock().unwrap().push((
                    request.generation,
                    request
                        .mutations
                        .iter()
                        .map(|mutation| mutation.key.clone())
                        .collect::<Vec<_>>(),
                    request.context.as_ref().unwrap().request_source.clone(),
                ));
                Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic()
                .pipelined(super::PipelinedTxnOptions {
                    enable: true,
                    flush_concurrency: 2,
                    resolve_lock_concurrency: 2,
                    write_throttle_ratio: 0.0,
                })
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .put("a".to_owned(), b"one".to_vec())
            .await
            .unwrap();
        assert!(transaction.maybe_flush_pipelined(true).await.unwrap());
        transaction
            .put("b".to_owned(), b"two".to_vec())
            .await
            .unwrap();
        assert!(transaction.maybe_flush_pipelined(true).await.unwrap());
        transaction
            .put("a".to_owned(), b"three".to_vec())
            .await
            .unwrap();
        assert!(transaction.maybe_flush_pipelined(true).await.unwrap());
        assert!(!transaction.maybe_flush_pipelined(true).await.unwrap());

        assert_eq!(
            *flushed.lock().unwrap(),
            vec![
                (1, vec![b"a".to_vec()], "external_pdml".to_owned()),
                (2, vec![b"b".to_vec()], "external_pdml".to_owned()),
                (3, vec![b"a".to_vec()], "external_pdml".to_owned()),
            ]
        );
        assert_eq!(transaction.pipelined_state.generation, 3);
        assert_eq!(
            transaction.pipelined_state.range_start.as_deref(),
            Some(b"a".as_slice())
        );
        assert_eq!(
            transaction.pipelined_state.range_end.as_deref(),
            Some(b"b".as_slice())
        );
    }

    #[tokio::test]
    async fn source_pipelined_flush_owns_resolving_lock_observer_until_retry_finishes() {
        let status_sent = Arc::new(tokio::sync::Notify::new());
        let status_sent_by_hook = status_sent.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::FlushRequest>() {
                    return Ok(Box::new(kvrpcpb::FlushResponse {
                        errors: vec![kvrpcpb::KeyError {
                            locked: Some(kvrpcpb::LockInfo {
                                key: b"a".to_vec(),
                                primary_lock: b"primary".to_vec(),
                                lock_version: 1,
                                lock_ttl: 60_000,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    status_sent_by_hook.notify_one();
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 60_000,
                        lock_info: Some(kvrpcpb::LockInfo {
                            key: b"primary".to_vec(),
                            primary_lock: b"primary".to_vec(),
                            lock_version: 1,
                            lock_ttl: 60_000,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected pipelined-flush resolver request");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic()
                .pipelined(super::PipelinedTxnOptions {
                    enable: true,
                    flush_concurrency: 1,
                    resolve_lock_concurrency: 1,
                    write_throttle_ratio: 0.0,
                })
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .put("a".to_owned(), b"value".to_vec())
            .await
            .unwrap();
        let context = transaction.lock_resolver_context.clone();
        let task = tokio::spawn(async move { transaction.maybe_flush_pipelined(true).await });

        tokio::time::timeout(Duration::from_secs(1), status_sent.notified())
            .await
            .expect("pipelined flush should enter lock resolution");
        assert_eq!(
            context.resolving_locks().await,
            vec![crate::transaction::ResolvingLock {
                txn_id: 1,
                lock_txn_id: 1,
                key: b"a".to_vec(),
                primary: b"primary".to_vec(),
            }]
        );

        task.abort();
        let _ = task.await;
        assert!(context.resolving_locks().await.is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn source_txn_file_admission_exclusions() {
        let restore = crate::config::update_global(|config| {
            config.tikv_client.txn_chunk_writer_addr = "127.0.0.1:1".to_owned();
            config.tikv_client.txn_file_min_mutation_size = 1;
        });
        let put = source_test_mutation("k", kvrpcpb::Op::Put);
        let base = source_test_committer(
            Arc::new(MockPdClient::default()),
            Some(Key::from(b"k".to_vec())),
            vec![put.clone()],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        assert!(base.should_use_txn_file());

        let mut pipelined = base.clone();
        pipelined.settings.pipelined.enable = true;
        assert!(!pipelined.should_use_txn_file());

        let mut shared = base.clone();
        shared.mutations = vec![source_test_mutation("k", kvrpcpb::Op::SharedLock)];
        assert!(!shared.should_use_txn_file());

        let mut asserted = base.clone();
        asserted.settings.assertion_level = kvrpcpb::AssertionLevel::Fast;
        asserted.mutations[0].assertion = kvrpcpb::Assertion::Exist as i32;
        assert!(!asserted.should_use_txn_file());

        let mut pessimistic = base.clone();
        pessimistic.options = TransactionOptions::new_pessimistic();
        assert!(!pessimistic.should_use_txn_file());

        let mut disabled = base.clone();
        disabled.settings.txn_file_disabled = true;
        assert!(!disabled.should_use_txn_file());
        restore();
    }

    #[test]
    fn source_txn_file_tagger_uses_first_key_and_static_tag_wins() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let mut dynamic = CommitSettings::default();
        dynamic.resource_group_tagger =
            Some(Arc::new(move |request: &mut dyn crate::store::Request| {
                let keys = request
                    .as_any()
                    .downcast_ref::<kvrpcpb::PrewriteRequest>()
                    .expect("txn-file prewrite tagger receives Prewrite")
                    .mutations
                    .iter()
                    .map(|mutation| mutation.key.clone())
                    .collect::<Vec<_>>();
                captured.lock().unwrap().push(keys);
                request.set_resource_group_tag(b"dynamic".to_vec());
            }));
        let mut with_first_key = kvrpcpb::PrewriteRequest::default();
        dynamic.apply_txn_file_prewrite(&mut with_first_key, b"first", Duration::from_secs(1));
        assert!(with_first_key.mutations.is_empty());
        assert_eq!(
            with_first_key.context.unwrap().resource_group_tag,
            b"dynamic"
        );

        let mut without_first_key = kvrpcpb::PrewriteRequest::default();
        dynamic.apply_txn_file_prewrite(&mut without_first_key, b"", Duration::from_secs(1));
        assert_eq!(
            without_first_key.context.unwrap().resource_group_tag,
            b"dynamic"
        );
        assert_eq!(
            *observed.lock().unwrap(),
            vec![vec![b"first".to_vec()], Vec::<Vec<u8>>::new()]
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let captured_calls = calls.clone();
        let mut static_settings = CommitSettings::default();
        static_settings.resource_group_tag = Some(b"static".to_vec());
        static_settings.resource_group_tagger = Some(Arc::new(move |_| {
            captured_calls.fetch_add(1, Ordering::SeqCst);
        }));
        let mut request = kvrpcpb::PrewriteRequest::default();
        static_settings.apply_txn_file_prewrite(&mut request, b"ignored", Duration::from_secs(1));
        assert_eq!(request.context.unwrap().resource_group_tag, b"static");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn source_txn_file_actions_apply_dynamic_or_static_resource_group_tag() {
        for (static_tag, expected_tag, expected_tagger_calls) in [
            (None, b"dynamic".to_vec(), 3),
            (Some(b"static".to_vec()), b"static".to_vec(), 0),
        ] {
            let observed = Arc::new(Mutex::new(Vec::new()));
            let captured = observed.clone();
            let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
                move |request: &dyn Any| {
                    if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                        captured.lock().unwrap().push((
                            "prewrite",
                            request.context.as_ref().unwrap().resource_group_tag.clone(),
                        ));
                        return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                    }
                    if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                        captured.lock().unwrap().push((
                            "commit",
                            request.context.as_ref().unwrap().resource_group_tag.clone(),
                        ));
                        return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                    }
                    let request = request
                        .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                        .expect("txn-file cleanup sends BatchRollback");
                    captured.lock().unwrap().push((
                        "rollback",
                        request.context.as_ref().unwrap().resource_group_tag.clone(),
                    ));
                    Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
                },
            )));
            let calls = Arc::new(AtomicUsize::new(0));
            let captured_calls = calls.clone();
            let mut settings = CommitSettings::default();
            settings.resource_group_tag = static_tag;
            settings.resource_group_tagger = Some(Arc::new(move |request| {
                captured_calls.fetch_add(1, Ordering::SeqCst);
                request.set_resource_group_tag(b"dynamic".to_vec());
            }));
            let mut committer = source_test_committer(
                rpc,
                Some(Key::from(b"primary".to_vec())),
                vec![source_test_mutation("k", kvrpcpb::Op::Put)],
                TransactionOptions::new_optimistic(),
                settings,
            );
            committer.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
            let batch = source_test_chunk_batch(true);

            assert!(!committer.prewrite_txn_file_batch(&batch).await.unwrap());
            assert!(!committer.commit_txn_file_batch(&batch).await.unwrap());
            assert!(!committer.rollback_txn_file_batch(&batch).await.unwrap());
            assert_eq!(calls.load(Ordering::SeqCst), expected_tagger_calls);
            assert_eq!(
                *observed.lock().unwrap(),
                vec![
                    ("prewrite", expected_tag.clone()),
                    ("commit", expected_tag.clone()),
                    ("rollback", expected_tag),
                ]
            );
        }
    }

    #[tokio::test]
    async fn source_txn_file_primary_prewrite_cleanup_and_batch_selection() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured.lock().unwrap().push((
                        "prewrite",
                        request.primary_lock.clone(),
                        request.context.as_ref().unwrap().region_id,
                        request.context.as_ref().unwrap().resource_group_tag.clone(),
                    ));
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                let request = request
                    .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                    .expect("cleanup uses BatchRollback");
                captured.lock().unwrap().push((
                    "rollback",
                    Vec::new(),
                    request.context.as_ref().unwrap().region_id,
                    request.context.as_ref().unwrap().resource_group_tag.clone(),
                ));
                Ok(Box::new(kvrpcpb::BatchRollbackResponse {
                    error: Some(kvrpcpb::KeyError {
                        abort: "primary rollback failed".to_owned(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut settings = CommitSettings::default();
        settings.resource_group_tag = Some(b"static".to_vec());
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        let batch = source_test_chunk_batch(true);
        assert!(!committer.prewrite_txn_file_batch(&batch).await.unwrap());
        let error = committer.rollback_txn_file_batch(&batch).await.unwrap_err();
        assert!(error.to_string().contains("primary rollback failed"));
        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                ("prewrite", b"primary".to_vec(), 2, b"static".to_vec()),
                ("rollback", Vec::new(), 2, b"static".to_vec()),
            ]
        );

        let primary = Key::from(vec![10]);
        let selector = source_test_committer(
            Arc::new(MockPdClient::default()),
            Some(primary),
            vec![source_test_mutation(vec![10], kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        let mut first = source_test_chunk_batch(false);
        first.region = MockPdClient::region1();
        let mut second = source_test_chunk_batch(false);
        second.region = MockPdClient::region2();
        assert_eq!(
            selector
                .txn_file_primary_batch_index(&[first, second])
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn source_txn_file_prewrite_expands_shared_lock_holders() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let call = captured_dispatches.fetch_add(1, Ordering::SeqCst);
                if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() && call == 0 {
                    return Ok(Box::new(kvrpcpb::PrewriteResponse {
                        errors: vec![kvrpcpb::KeyError {
                            locked: Some(kvrpcpb::LockInfo {
                                // The wrapper's fields are deliberately empty.
                                // Only the concrete holder proves expansion.
                                shared_lock_infos: vec![kvrpcpb::LockInfo {
                                    key: b"shared-holder".to_vec(),
                                    lock_version: 9,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Err(Error::StringError(
                    "newer shared holder must fail before lock resolution".to_owned(),
                ))
            },
        )));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        let error = committer
            .prewrite_txn_file_batch(&source_test_chunk_batch(true))
            .await
            .unwrap_err();
        assert!(crate::error::is_write_conflict(&error), "{error:?}");
        let Error::WriteConflict(conflict) = error else {
            panic!("expected typed write conflict");
        };
        assert_eq!(conflict.conflict.key, b"shared-holder");
        assert_eq!(conflict.conflict.conflict_ts, 9);
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_txn_file_commit_ambiguity_and_expired_retry() {
        let batch = source_test_chunk_batch(true);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::GrpcAPI(tonic::Status::unavailable(
                "primary unavailable",
            )))
        })));
        let mut primary = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        primary.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        let error = primary.commit_txn_file_batch(&batch).await.unwrap_err();
        assert!(primary.undetermined);
        assert!(matches!(
            primary.normalize_txn_file_commit_result::<()>(Err(error)),
            Err(Error::UndeterminedError(_))
        ));

        let secondary_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::GrpcAPI(tonic::Status::unavailable(
                "secondary unavailable",
            )))
        })));
        let mut secondary = source_test_committer(
            secondary_rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        secondary.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        let mut secondary_batch = batch.clone();
        secondary_batch.is_primary = false;
        let error = secondary
            .commit_txn_file_batch(&secondary_batch)
            .await
            .unwrap_err();
        assert!(!secondary.undetermined);
        assert!(!matches!(
            secondary.normalize_txn_file_commit_result::<()>(Err(error)),
            Err(Error::UndeterminedError(_))
        ));

        let definitive_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
        })));
        let mut definitive = source_test_committer(
            definitive_rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        definitive.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        definitive.undetermined = true;
        assert!(!definitive.commit_txn_file_batch(&batch).await.unwrap());
        assert!(!definitive.undetermined);

        let region_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::new(kvrpcpb::CommitResponse {
                region_error: Some(crate::proto::errorpb::Error {
                    undetermined_result: Some(Default::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }) as Box<dyn Any>)
        })));
        let mut region_undetermined = source_test_committer(
            region_rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        region_undetermined.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        let error = region_undetermined
            .commit_txn_file_batch(&batch)
            .await
            .unwrap_err();
        assert!(region_undetermined.undetermined);
        assert!(matches!(
            region_undetermined.normalize_txn_file_commit_result::<()>(Err(error)),
            Err(Error::UndeterminedError(_))
        ));

        struct Version(i64);
        impl super::SchemaVersion for Version {
            fn schema_meta_version(&self) -> i64 {
                self.0
            }
        }
        struct Checker(Arc<Mutex<Vec<(u64, i64)>>>);
        impl super::SchemaLeaseChecker for Checker {
            fn check_by_schema_version(
                &self,
                timestamp: u64,
                version: &dyn super::SchemaVersion,
            ) -> crate::Result<super::RelatedSchemaChange> {
                self.0
                    .lock()
                    .unwrap()
                    .push((timestamp, version.schema_meta_version()));
                Ok(super::RelatedSchemaChange {
                    physical_table_ids: Vec::new(),
                    action_types: Vec::new(),
                    latest_info_schema: Arc::new(Version(11)),
                })
            }
        }

        let versions = Arc::new(Mutex::new(Vec::new()));
        let captured_versions = versions.clone();
        let request_count = Arc::new(AtomicUsize::new(0));
        let captured_count = request_count.clone();
        let retry_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::CommitRequest>()
                    .expect("retry sends Commit");
                captured_versions
                    .lock()
                    .unwrap()
                    .push(request.commit_version);
                if captured_count.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Ok(Box::new(kvrpcpb::CommitResponse {
                        error: Some(kvrpcpb::KeyError {
                            commit_ts_expired: Some(kvrpcpb::CommitTsExpired {
                                start_ts: 1,
                                attempted_commit_ts: request.commit_version,
                                key: b"k".to_vec(),
                                min_commit_ts: 5,
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        retry_rpc.set_timestamp(Timestamp::from_version(9));
        let schema_checks = Arc::new(Mutex::new(Vec::new()));
        let upper_bound_calls = Arc::new(AtomicUsize::new(0));
        let captured_upper_bound_calls = upper_bound_calls.clone();
        let mut settings = CommitSettings::default();
        settings.schema_version = Some(Arc::new(Version(10)));
        settings.schema_lease_checker = Some(Arc::new(Checker(schema_checks.clone())));
        settings.commit_timestamp_upper_bound = Some(Arc::new(move |timestamp| {
            captured_upper_bound_calls.fetch_add(1, Ordering::SeqCst);
            timestamp == 9
        }));
        let mut retry = source_test_committer(
            retry_rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        retry.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        assert!(!retry.commit_txn_file_batch(&batch).await.unwrap());
        assert_eq!(*versions.lock().unwrap(), vec![2, 9]);
        assert_eq!(*schema_checks.lock().unwrap(), vec![(9, 10)]);
        assert_eq!(upper_bound_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            retry.txn_file_commit_timestamp.as_ref().unwrap().version(),
            9
        );
    }

    #[tokio::test]
    async fn source_txn_file_commit_survives_resource_accounting_response_error() {
        struct FailingResponseController;
        #[async_trait::async_trait]
        impl ResourceGroupController for FailingResponseController {
            async fn on_request_wait(
                &self,
                _resource_group_name: &str,
                _request: ResourceControlRequestInfo,
            ) -> crate::Result<RequestWaitResult> {
                Ok(RequestWaitResult::default())
            }

            fn on_response_wait(
                &self,
                _resource_group_name: &str,
                _request: ResourceControlRequestInfo,
                _response: crate::ResourceControlResponseInfo,
            ) -> crate::Result<ResponseWaitResult> {
                Err(Error::StringError(
                    "resource accounting unavailable after commit".to_owned(),
                ))
            }
        }

        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::CommitRequest>()
                    .expect("txn-file action sends Commit");
                assert!(request.is_txn_file);
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        committer.resource_group_name = Some("test-rg".to_owned());
        committer.resource_control = Some(Arc::new(FailingResponseController));
        committer.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        assert!(!committer
            .commit_txn_file_batch(&source_test_chunk_batch(true))
            .await
            .unwrap());
        assert!(!committer.undetermined);
    }

    #[tokio::test]
    async fn source_prepare_txn_file_commit_timestamp_waits_and_checks_schema_first() {
        struct Version;
        impl super::SchemaVersion for Version {
            fn schema_meta_version(&self) -> i64 {
                10
            }
        }
        struct FailingChecker(Arc<AtomicUsize>);
        impl super::SchemaLeaseChecker for FailingChecker {
            fn check_by_schema_version(
                &self,
                _timestamp: u64,
                _version: &dyn super::SchemaVersion,
            ) -> crate::Result<super::RelatedSchemaChange> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(Error::StringError("schema changed".to_owned()))
            }
        }

        let rpc = Arc::new(MockPdClient::default());
        rpc.set_timestamp(Timestamp::from_version(100));
        let updater = rpc.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(15)).await;
            updater.set_timestamp(Timestamp::from_version(102));
        });
        let mut settings = CommitSettings::default();
        settings.commit_wait_until_tso = 101;
        settings.commit_wait_until_tso_timeout = Duration::from_secs(1);
        let committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        assert_eq!(
            committer
                .get_timestamp_for_commit()
                .await
                .unwrap()
                .version(),
            102
        );

        let checker_calls = Arc::new(AtomicUsize::new(0));
        let upper_bound_calls = Arc::new(AtomicUsize::new(0));
        let captured_upper_bound_calls = upper_bound_calls.clone();
        let mut settings = CommitSettings::default();
        settings.schema_version = Some(Arc::new(Version));
        settings.schema_lease_checker = Some(Arc::new(FailingChecker(checker_calls.clone())));
        settings.commit_timestamp_upper_bound = Some(Arc::new(move |_| {
            captured_upper_bound_calls.fetch_add(1, Ordering::SeqCst);
            true
        }));
        let committer = source_test_committer(
            Arc::new(MockPdClient::default()),
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        let error = committer.check_schema_valid(102).unwrap_err();
        assert!(error.to_string().contains("schema changed"));
        assert_eq!(checker_calls.load(Ordering::SeqCst), 1);
        assert_eq!(upper_bound_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn source_pre_split_txn_file_regions_uses_dedicated_split_path() {
        let split_requests = Arc::new(Mutex::new(Vec::new()));
        let captured = split_requests.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::SplitRegionRequest>()
                    .expect("txn-file pre-split must use the dedicated SplitRegion RPC");
                captured.lock().unwrap().push((
                    request.split_keys.clone(),
                    request.context.as_ref().unwrap().region_id,
                ));
                Ok(Box::<kvrpcpb::SplitRegionResponse>::default() as Box<dyn Any>)
            },
        )));
        let mutations = (20_u8..25)
            .map(|key| source_test_mutation(vec![key], kvrpcpb::Op::Put))
            .collect::<Vec<_>>();
        let mut chunks = TxnChunkSlice::default();
        for (chunk_id, key) in (20_u8..25).enumerate() {
            chunks.push(chunk_id as u64, TxnChunkRange::new(vec![key], vec![key], 1));
        }
        let committer = source_test_committer(
            rpc,
            Some(Key::from(vec![20])),
            mutations,
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        committer.pre_split_txn_file_regions(&chunks).await.unwrap();
        assert_eq!(*split_requests.lock().unwrap(), vec![(vec![vec![24]], 2)]);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn source_large_2pc_group_pre_splits_through_pd_and_invalidates_cache() {
        let old_detect = super::PRE_SPLIT_DETECT_THRESHOLD.swap(5, Ordering::SeqCst);
        let old_size = super::PRE_SPLIT_SIZE_THRESHOLD.swap(10, Ordering::SeqCst);
        let rpc = Arc::new(MockPdClient::default());
        let mutations = (20_u8..25)
            .map(|key| source_test_mutation(vec![key], kvrpcpb::Op::Put))
            .collect::<Vec<_>>();
        let committer = source_test_committer(
            rpc.clone(),
            Some(Key::from(vec![20])),
            mutations,
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        committer.pre_split_large_transaction_regions().await;
        super::PRE_SPLIT_DETECT_THRESHOLD.store(old_detect, Ordering::SeqCst);
        super::PRE_SPLIT_SIZE_THRESHOLD.store(old_size, Ordering::SeqCst);

        assert_eq!(rpc.split_region_keys(), vec![vec![vec![21], vec![23]]]);
        assert_eq!(
            rpc.invalidated_regions(),
            vec![MockPdClient::region2().ver_id()]
        );
    }

    #[tokio::test]
    async fn source_schema_filter_callback_and_memory_contracts() {
        struct Version(i64);
        impl super::SchemaVersion for Version {
            fn schema_meta_version(&self) -> i64 {
                self.0
            }
        }
        struct Checker(Arc<Mutex<Vec<(u64, i64)>>>);
        impl super::SchemaLeaseChecker for Checker {
            fn check_by_schema_version(
                &self,
                timestamp: u64,
                version: &dyn super::SchemaVersion,
            ) -> crate::Result<super::RelatedSchemaChange> {
                self.0
                    .lock()
                    .unwrap()
                    .push((timestamp, version.schema_meta_version()));
                Ok(super::RelatedSchemaChange {
                    physical_table_ids: vec![1],
                    action_types: vec![2],
                    latest_info_schema: Arc::new(Version(11)),
                })
            }
        }
        struct Filter;
        impl super::KvFilter for Filter {
            fn is_unnecessary_key_value(
                &self,
                key: &[u8],
                _value: &[u8],
                _flags: super::MutationFlags,
            ) -> crate::Result<bool> {
                Ok(key == b"drop")
            }
        }

        let prewrite_keys = Arc::new(Mutex::new(Vec::new()));
        let captured_keys = prewrite_keys.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured_keys.lock().unwrap().push(
                        request
                            .mutations
                            .iter()
                            .map(|mutation| mutation.key.clone())
                            .collect::<Vec<_>>(),
                    );
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                assert!(request.downcast_ref::<kvrpcpb::CommitRequest>().is_some());
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(5));
        let schema_checks = Arc::new(Mutex::new(Vec::new()));
        let callbacks = Arc::new(Mutex::new(Vec::new()));
        let captured_callbacks = callbacks.clone();
        let memory = Arc::new(Mutex::new(Vec::new()));
        let captured_memory = memory.clone();
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.set_schema_version(Arc::new(Version(10)));
        transaction.set_schema_lease_checker(Arc::new(Checker(schema_checks.clone())));
        transaction.set_kv_filter(Arc::new(Filter));
        transaction.set_commit_callback(move |info, error| {
            captured_callbacks.lock().unwrap().push((info, error));
        });
        transaction.set_memory_footprint_change_hook(move |bytes| {
            captured_memory.lock().unwrap().push(bytes);
        });
        assert!(transaction.memory_hook_set());
        transaction
            .put("keep".to_owned(), b"value".to_vec())
            .await
            .unwrap();
        transaction
            .put("drop".to_owned(), b"value".to_vec())
            .await
            .unwrap();
        let footprint = transaction.memory_footprint();
        assert!(footprint > 0);
        assert_eq!(transaction.commit().await.unwrap().unwrap().version(), 5);

        assert_eq!(*prewrite_keys.lock().unwrap(), vec![vec![b"keep".to_vec()]]);
        assert_eq!(*schema_checks.lock().unwrap(), vec![(5, 10)]);
        assert_eq!(memory.lock().unwrap().last().copied(), Some(footprint));
        let callbacks = callbacks.lock().unwrap();
        assert_eq!(callbacks.len(), 1);
        assert_eq!(callbacks[0].1, None);
        let info: serde_json::Value = serde_json::from_str(&callbacks[0].0).unwrap();
        assert_eq!(info["txn_scope"], crate::oracle::GLOBAL_TXN_SCOPE);
        assert_eq!(info["start_ts"], 1);
        assert_eq!(info["commit_ts"], 5);
        assert_eq!(info["txn_commit_mode"], "2pc");
        assert_eq!(info["async_commit_fallback"], false);
        assert_eq!(info["one_pc_fallback"], false);
        assert_eq!(info["pipelined"], false);
        assert_eq!(info["flush_wait_ms"], 0);
        assert!(info.get("error").is_none());
    }

    #[tokio::test]
    async fn source_async_commit_max_timestamp_and_zero_minimum_fallback() {
        struct Version;
        impl super::SchemaVersion for Version {
            fn schema_meta_version(&self) -> i64 {
                10
            }
        }
        struct Checker(Arc<Mutex<Vec<u64>>>);
        impl super::SchemaLeaseChecker for Checker {
            fn check_by_schema_version(
                &self,
                timestamp: u64,
                _version: &dyn super::SchemaVersion,
            ) -> crate::Result<super::RelatedSchemaChange> {
                self.0.lock().unwrap().push(timestamp);
                Ok(super::RelatedSchemaChange {
                    physical_table_ids: Vec::new(),
                    action_types: Vec::new(),
                    latest_info_schema: Arc::new(Version),
                })
            }
        }

        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PrewriteRequest>()
                    .expect("async protocol preparation sends Prewrite");
                captured.lock().unwrap().push((
                    request.min_commit_ts,
                    request.max_commit_ts,
                    request.lock_ttl,
                    request.use_async_commit,
                ));
                Ok(Box::new(kvrpcpb::PrewriteResponse {
                    min_commit_ts: 7,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let checks = Arc::new(Mutex::new(Vec::new()));
        let mut settings = CommitSettings::default();
        settings.causal_consistency = true;
        settings.schema_version = Some(Arc::new(Version));
        settings.schema_lease_checker = Some(Arc::new(Checker(checks.clone())));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().use_async_commit(),
            settings,
        );
        committer.configure_commit_protocols();
        committer.calculate_max_commit_ts().unwrap();
        let max_commit_ts = committer.max_commit_ts;
        assert!(max_commit_ts > committer.start_version.version());
        assert_eq!(committer.prewrite().await.unwrap().unwrap().version(), 7);
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert!(observed[0].0 >= 2);
        assert_eq!(observed[0].1, max_commit_ts);
        assert!(observed[0].2 >= super::DEFAULT_LOCK_TTL);
        assert!(observed[0].3);
        assert_eq!(checks.lock().unwrap().len(), 1);

        let fallback_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
        })));
        let mut fallback = source_test_committer(
            fallback_rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().use_async_commit(),
            CommitSettings::default(),
        );
        fallback.configure_commit_protocols();
        fallback.calculate_max_commit_ts().unwrap();
        assert!(fallback.prewrite().await.unwrap().is_none());
        assert!(!fallback.options.async_commit);
    }

    #[tokio::test]
    async fn source_binlog_prewrite_commit_and_skip_lifecycle() {
        struct WriteResult(bool);
        impl super::BinlogWriteResult for WriteResult {
            fn skipped(&self) -> bool {
                self.0
            }

            fn get_error(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
                None
            }
        }
        struct Executor {
            skip_prewrite: bool,
            events: Arc<Mutex<Vec<String>>>,
        }
        #[async_trait::async_trait]
        impl super::BinlogExecutor for Executor {
            async fn prewrite(
                &self,
                _cancellation: crate::async_util::Cancellation,
                primary: &[u8],
            ) -> Box<dyn super::BinlogWriteResult> {
                self.events
                    .lock()
                    .unwrap()
                    .push(format!("prewrite:{}", String::from_utf8_lossy(primary)));
                Box::new(WriteResult(self.skip_prewrite))
            }

            async fn commit(
                &self,
                _cancellation: crate::async_util::Cancellation,
                commit_timestamp: i64,
            ) {
                self.events
                    .lock()
                    .unwrap()
                    .push(format!("commit:{commit_timestamp}"));
            }

            fn skip(&self) {
                self.events.lock().unwrap().push("skip".to_owned());
            }
        }

        for (skip_prewrite, expected_final) in [(false, "commit:5"), (true, "skip")] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
                |request: &dyn Any| {
                    if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                        Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                    } else {
                        assert!(request.downcast_ref::<kvrpcpb::CommitRequest>().is_some());
                        Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                    }
                },
            )));
            rpc.set_timestamp(Timestamp::from_version(5));
            let mut transaction = Transaction::new(
                Timestamp::from_version(1),
                rpc,
                TransactionOptions::new_optimistic()
                    .heartbeat_option(HeartbeatOption::NoHeartbeat)
                    .drop_check(CheckLevel::None),
                Keyspace::Disable,
            );
            transaction.set_binlog_executor(Arc::new(Executor {
                skip_prewrite,
                events: events.clone(),
            }));
            transaction
                .put("primary".to_owned(), b"value".to_vec())
                .await
                .unwrap();
            assert_eq!(transaction.commit().await.unwrap().unwrap().version(), 5);
            assert_eq!(
                *events.lock().unwrap(),
                vec!["prewrite:primary".to_owned(), expected_final.to_owned()]
            );
        }
    }
}

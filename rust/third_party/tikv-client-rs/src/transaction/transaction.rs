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
    FlagsOp, GetOption, GetOptions, LockContext, ReplicaReadAdjuster, ReplicaReadConfig,
    ReturnedValue, ValueEntry, Variables, DEFAULT_VARIABLES, LOCK_ALWAYS_WAIT, LOCK_NO_WAIT,
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
use crate::request::Shardable;
use crate::request::TruncateKeyspace;
use crate::resource_control::{
    RequestInfo as ResourceControlRequestInfo, ResourceGroupControllerHandle,
    ResponseInfo as ResourceControlResponseInfo,
};
use crate::retry::{
    RetryBackoffer, BO_COMMIT_TS_LAG, BO_PD_RPC, BO_REGION_MISS, BO_TIKV_RPC, BO_TXN_LOCK,
};
use crate::store::Request as StoreRequest;
use crate::tikv::MAX_WRITE_EXECUTION_TIME;
use crate::timestamp::TimestampExt;
use crate::transaction::buffer::{memdb_key_bounds, proto_mutations_from_memdb, Buffer};
use crate::transaction::extract_lock_from_key_error;
use crate::transaction::latch::LatchesScheduler;
use crate::transaction::lock::format_key_for_log;
use crate::transaction::lowering::*;
use crate::transaction::requests::{
    new_resolve_lock_request, CollectPessimisticLock, CollectScannerRegionBatch,
    PessimisticLockOutput, PreserveScannerPairErrors,
};
use crate::transaction::snapshot_stats::snapshot_read_sli_interceptor;
use crate::transaction::txn_file::{
    build_txn_chunks, request_source_allows_txn_file, txn_file_max_chunks_in_parallel,
    txn_file_pre_split_keys, ChunkBatch, TxnChunkSlice,
};
use crate::transaction::unionstore::{
    ManagedPipelinedFlushMetadata, ManagedPipelinedFlushOutcome, MemDb, PipelinedError,
};
use crate::transaction::ReadLockContext;
use crate::transaction::ResolveLocksContext;
use crate::transaction::SnapshotRuntimeStats;
use crate::transaction::SnapshotVisibilityValidator;
pub use crate::util::RequestSource;
use crate::BoundRange;

/// Maximum transaction lifetime before commit is refused, in milliseconds.
pub const MAX_TXN_TIME_USE: u64 = 24 * 60 * 60 * 1_000;
/// Process-wide maximum pipelined-transaction TTL, matching client-go's
/// mutable `transaction.MaxPipelinedTxnTTL` integration surface.
pub static MAX_PIPELINED_TXN_TTL: atomic::AtomicU64 = atomic::AtomicU64::new(MAX_TXN_TIME_USE);
/// Process-wide cumulative prewrite retry budget in milliseconds, matching
/// client-go's mutable `transaction.PrewriteMaxBackoff` integration surface.
pub static PREWRITE_MAX_BACKOFF: atomic::AtomicU64 = atomic::AtomicU64::new(40_000);
/// Process-wide cumulative primary-commit retry budget in milliseconds,
/// matching client-go's mutable `transaction.CommitMaxBackoff` surface.
pub static COMMIT_MAX_BACKOFF: atomic::AtomicU64 = atomic::AtomicU64::new(40_000);
const COMMIT_SECONDARY_MAX_BACKOFF: u64 = 41_000;
const PIPELINED_FLUSH_MAX_BACKOFF: u64 = 20_000;
const CLEANUP_MAX_BACKOFF: u64 = 20_000;
#[doc(hidden)]
pub const PESSIMISTIC_LOCK_MAX_BACKOFF: u64 = 20_000;
const MAX_COMMIT_TS_EXPIRED_GAP: u64 = 3_600_000 << 18;

fn commit_ts_expired_gap_is_too_large(expired: &kvrpcpb::CommitTsExpired) -> bool {
    // Go's uint64 subtraction wraps. Preserve that behavior for malformed as
    // well as valid TiKV responses instead of silently accepting a response
    // whose minimum commit timestamp moved backwards.
    expired
        .min_commit_ts
        .wrapping_sub(expired.attempted_commit_ts)
        > MAX_COMMIT_TS_EXPIRED_GAP
}

fn prewrite_min_commit_ts(start_ts: u64, for_update_ts: u64, managed_min_commit_ts: u64) -> u64 {
    if for_update_ts > 0 && for_update_ts >= managed_min_commit_ts {
        for_update_ts.wrapping_add(1)
    } else if start_ts >= managed_min_commit_ts {
        start_ts.wrapping_add(1)
    } else {
        managed_min_commit_ts
    }
}

fn pipelined_broadcast_grace_period() -> Duration {
    if cfg!(test) {
        Duration::from_millis(1)
    } else {
        // Give slow followers time to apply resolved locks before evicting
        // the transaction-status cache entry, matching client-go.
        Duration::from_secs(5)
    }
}

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
const GO_DURATION_MAX_MILLIS: i64 = i64::MAX / 1_000_000;

fn go_system_time_sub_millis(end: SystemTime, start: SystemTime) -> i64 {
    let (negative, duration) = match end.duration_since(start) {
        Ok(duration) => (false, duration),
        Err(error) => (true, error.duration()),
    };
    // time.Time.Sub saturates to time.Duration's signed nanosecond range
    // before Milliseconds truncates it toward zero.
    let millis = duration.as_millis().min(GO_DURATION_MAX_MILLIS as u128) as i64;
    if negative {
        -millis
    } else {
        millis
    }
}

fn effective_pessimistic_lock_wait_time(context: &mut LockContext, now: SystemTime) -> Result<i64> {
    let wait_time = context.lock_wait_time();
    calculate_pessimistic_lock_wait_time(
        context.killed.as_ref(),
        wait_time,
        context.wait_start_time,
        context.max_execution_deadline,
        now,
    )
}

fn calculate_pessimistic_lock_wait_time(
    killed: Option<&Arc<std::sync::atomic::AtomicU32>>,
    wait_time: i64,
    wait_start_time: Option<SystemTime>,
    max_execution_deadline: Option<SystemTime>,
    now: SystemTime,
) -> Result<i64> {
    if let Some(killed) = killed {
        let signal = killed.load(atomic::Ordering::Acquire);
        if signal != 0 {
            return Err(crate::error::QueryInterruptedWithSignalError { signal }.into());
        }
    }
    if max_execution_deadline.is_some_and(|deadline| now > deadline) {
        return Err(crate::error::QueryInterruptedWithSignalError {
            signal: MAX_EXECUTION_TIME_EXCEEDED_SIGNAL,
        }
        .into());
    }

    if wait_time == LOCK_NO_WAIT || wait_time <= 0 {
        return Ok(LOCK_NO_WAIT);
    }
    let mut effective = wait_time;
    if wait_time != LOCK_ALWAYS_WAIT {
        let elapsed = wait_start_time
            .map(|started| go_system_time_sub_millis(now, started))
            .unwrap_or_default();
        effective = wait_time.wrapping_sub(elapsed);
        if effective <= 0 {
            return Ok(LOCK_NO_WAIT);
        }
    }
    if let Some(deadline) = max_execution_deadline {
        let remaining = go_system_time_sub_millis(deadline, now);
        if remaining <= 0 {
            return Ok(LOCK_NO_WAIT);
        }
        effective = effective.min(remaining);
    }
    Ok(effective)
}

#[derive(Clone)]
struct PessimisticLockDispatchTiming {
    start_instant: Instant,
    killed: Option<Arc<std::sync::atomic::AtomicU32>>,
    wait_time: Option<i64>,
    wait_start_time: Option<SystemTime>,
    max_execution_deadline: Option<SystemTime>,
}

impl PessimisticLockDispatchTiming {
    fn prepare(&self, request: &mut kvrpcpb::PessimisticLockRequest) -> Result<()> {
        request.lock_ttl = self.start_instant.elapsed().as_millis() as u64 + managed_lock_ttl();
        if let Some(wait_time) = self.wait_time {
            request.wait_timeout = calculate_pessimistic_lock_wait_time(
                self.killed.as_ref(),
                wait_time,
                self.wait_start_time,
                self.max_execution_deadline,
                SystemTime::now(),
            )?;
        }
        Ok(())
    }
}

struct LockKeysCallbackGuard<F: FnOnce()> {
    callback: Option<F>,
}

impl<F: FnOnce()> LockKeysCallbackGuard<F> {
    fn new(callback: F) -> Self {
        Self {
            callback: Some(callback),
        }
    }
}

impl<F: FnOnce()> Drop for LockKeysCallbackGuard<F> {
    fn drop(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback();
        }
    }
}

fn apply_pessimistic_lock_resource_tag(
    request: &mut kvrpcpb::PessimisticLockRequest,
    resource_group_tag: &[u8],
    resource_group_tagger: Option<&crate::kv::ResourceGroupTagger>,
) {
    let tag = if !resource_group_tag.is_empty() {
        Some(resource_group_tag.to_vec())
    } else {
        resource_group_tagger.map(|tagger| tagger(request))
    };
    if let Some(tag) = tag {
        request
            .context
            .get_or_insert_with(kvrpcpb::Context::default)
            .resource_group_tag = tag;
    }
}

fn pessimistic_key_exists_error(key: &Key, keyspace: Keyspace) -> Error {
    let logical_key = key.clone().truncate_keyspace(keyspace);
    crate::error::KeyExistsError {
        already_exist: kvrpcpb::AlreadyExist {
            key: <&[u8]>::from(&logical_key).to_vec(),
        },
        value: Vec::new(),
    }
    .into()
}

fn apply_transaction_resource_group_tagger<R: StoreRequest>(
    request: &mut R,
    has_static_tag: bool,
    resource_group_tagger: Option<&TransactionResourceGroupTagger>,
) {
    if !has_static_tag {
        if let Some(tagger) = resource_group_tagger {
            tagger(request);
        }
    }
}

fn pessimistic_deadlock(error: &Error) -> Option<kvrpcpb::Deadlock> {
    match error {
        Error::KeyError(error) => error.deadlock.clone().or_else(|| {
            error
                .lock_upgrade_conflict
                .as_ref()
                .map(|conflict| kvrpcpb::Deadlock {
                    lock_ts: conflict.owner_start_ts,
                    lock_key: conflict.key.clone(),
                    ..Default::default()
                })
        }),
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

fn is_transaction_transport_error(error: &Error) -> bool {
    match error {
        Error::Grpc(_) | Error::GrpcAPI(_) | Error::Channel(_) => true,
        Error::StringError(message) if message == "context canceled" => true,
        Error::Connection { source, .. } | Error::UndeterminedError(source) => {
            is_transaction_transport_error(source)
        }
        Error::ExtractedErrors(errors) | Error::MultipleKeyErrors(errors) => {
            errors.iter().any(is_transaction_transport_error)
        }
        Error::PessimisticLockError { inner, .. } => is_transaction_transport_error(inner),
        _ => false,
    }
}

fn is_txn_file_retryable_transport_error(error: &Error) -> bool {
    match error {
        // client-go's RegionRequestSender returns a cancelled request context
        // directly. Retrying it loses the original cause and can duplicate a
        // transaction-file request that the caller has already abandoned.
        Error::GrpcAPI(status) if status.code() == tonic::Code::Cancelled => false,
        Error::Connection { source, .. } | Error::UndeterminedError(source) => {
            is_txn_file_retryable_transport_error(source)
        }
        Error::ExtractedErrors(errors) | Error::MultipleKeyErrors(errors) => {
            errors.iter().any(is_txn_file_retryable_transport_error)
        }
        Error::PessimisticLockError { inner, .. } => is_txn_file_retryable_transport_error(inner),
        _ => is_transaction_transport_error(error),
    }
}

fn heartbeat_error_stops_immediately(error: &Error) -> bool {
    match error {
        Error::KeyError(_)
        | Error::KeyExists(_)
        | Error::WriteConflict(_)
        | Error::RetryableKey(_)
        | Error::AssertionFailed(_)
        | Error::Deadlock(_)
        | Error::TxnNotFound(_) => true,
        Error::Connection { source, .. }
        | Error::UndeterminedError(source)
        | Error::PessimisticLockError { inner: source, .. } => {
            heartbeat_error_stops_immediately(source)
        }
        Error::ExtractedErrors(errors) | Error::MultipleKeyErrors(errors) => {
            errors.iter().any(heartbeat_error_stops_immediately)
        }
        _ => false,
    }
}

fn has_undetermined_region_error(error: &Error) -> bool {
    match error {
        Error::RegionError(error) => error.undetermined_result.is_some(),
        Error::UndeterminedError(_) => true,
        Error::Connection { source, .. } => has_undetermined_region_error(source),
        Error::ExtractedErrors(errors) | Error::MultipleKeyErrors(errors) => {
            errors.iter().any(has_undetermined_region_error)
        }
        Error::PessimisticLockError { inner, .. } => has_undetermined_region_error(inner),
        _ => false,
    }
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
type AutoHeartbeatStarter = Arc<dyn Fn(MinCommitTsManager, bool) + Send + Sync>;
type TxnFileResourceAccounting = (
    ResourceGroupControllerHandle,
    String,
    ResourceControlRequestInfo,
);

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
    fn decorate_request_context(&self, context: &mut kvrpcpb::Context, timeout: Duration) {
        context.sync_log = self.force_sync_log;
        context.disk_full_opt = self.disk_full_option as i32;
        context.txn_source = self.transaction_source;
        context.request_source = self.request_source.context_value();
        context.max_execution_duration_ms = timeout.as_millis().try_into().unwrap_or(u64::MAX);
        if let Some(tag) = &self.resource_group_tag {
            context.resource_group_tag.clone_from(tag);
        }
    }

    fn apply_request_context<R: StoreRequest>(&self, request: &mut R, timeout: Duration) {
        let mut context = request.tikv_context().cloned().unwrap_or_default();
        self.decorate_request_context(&mut context, timeout);
        assert!(request.attach_context(context));
    }

    fn apply_request<R: StoreRequest>(&self, request: &mut R, timeout: Duration) {
        self.apply_request_context(request, timeout);
        apply_transaction_resource_group_tagger(
            request,
            self.resource_group_tag.is_some(),
            self.resource_group_tagger.as_ref(),
        );
    }

    fn apply_cleanup_request_context<R: StoreRequest>(&self, request: &mut R, timeout: Duration) {
        let mut context = request.tikv_context().cloned().unwrap_or_default();
        context.sync_log = self.force_sync_log;
        context.request_source = self.request_source.context_value();
        context.max_execution_duration_ms = timeout.as_millis().try_into().unwrap_or(u64::MAX);
        if let Some(tag) = &self.resource_group_tag {
            context.resource_group_tag.clone_from(tag);
        }
        assert!(request.attach_context(context));
    }

    fn apply_cleanup_request<R: StoreRequest>(&self, request: &mut R, timeout: Duration) {
        self.apply_cleanup_request_context(request, timeout);
        apply_transaction_resource_group_tagger(
            request,
            self.resource_group_tag.is_some(),
            self.resource_group_tagger.as_ref(),
        );
    }

    fn apply_pessimistic_lock_request<R: StoreRequest>(&self, request: &mut R, timeout: Duration) {
        let mut context = request.tikv_context().cloned().unwrap_or_default();
        context.sync_log = self.force_sync_log;
        context.request_source = self.request_source.context_value();
        context.max_execution_duration_ms = timeout.as_millis().try_into().unwrap_or(u64::MAX);
        assert!(request.attach_context(context));
    }

    fn apply_pessimistic_rollback_request<R: StoreRequest>(
        &self,
        request: &mut R,
        timeout: Duration,
    ) {
        let mut context = request.tikv_context().cloned().unwrap_or_default();
        context.request_source = self.request_source.context_value();
        context.max_execution_duration_ms = timeout.as_millis().try_into().unwrap_or(u64::MAX);
        assert!(request.attach_context(context));
    }

    fn apply_heartbeat_request<R: StoreRequest>(&self, request: &mut R, timeout: Duration) {
        let mut context = request.tikv_context().cloned().unwrap_or_default();
        context.max_execution_duration_ms = timeout.as_millis().try_into().unwrap_or(u64::MAX);
        assert!(request.attach_context(context));
    }

    fn apply_pipelined_resolve_request<R: StoreRequest>(&self, request: &mut R) {
        let mut context = request.tikv_context().cloned().unwrap_or_default();
        context.sync_log = self.force_sync_log;
        context.disk_full_opt = self.disk_full_option as i32;
        context.txn_source = self.transaction_source;
        context.request_source = "external_pdml".to_owned();
        if let Some(tag) = &self.resource_group_tag {
            context.resource_group_tag.clone_from(tag);
        }
        assert!(request.attach_context(context));
    }

    fn apply_broadcast_request(
        &self,
        request: &mut kvrpcpb::BroadcastTxnStatusRequest,
        cluster_id: u64,
    ) {
        let context = request
            .context
            .get_or_insert_with(kvrpcpb::Context::default);
        context.cluster_id = cluster_id;
        context.request_source = self.request_source.context_value();
        if let Some(tag) = &self.resource_group_tag {
            context.resource_group_tag.clone_from(tag);
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

#[derive(Clone)]
enum TxnFileRetryBackoff {
    Source(Arc<tokio::sync::Mutex<RetryBackoffer>>),
    Legacy(crate::backoff::Backoff),
}

impl TxnFileRetryBackoff {
    async fn is_retry_request(&self, explicit_retry: bool) -> bool {
        match self {
            Self::Source(owner) => explicit_retry || owner.lock().await.total_sleep_ms() > 0,
            Self::Legacy(_) => explicit_retry,
        }
    }

    async fn backoff_region_error(&mut self, error: &crate::proto::errorpb::Error) -> Result<()> {
        match self {
            Self::Source(owner) => owner
                .lock()
                .await
                .may_backoff_region_error(Some(error))
                .await
                .map_err(|error| Error::StringError(error.to_string())),
            Self::Legacy(backoff) => {
                if error.epoch_not_match.is_some()
                    && !crate::retry::is_fake_region_error(Some(error))
                {
                    return Ok(());
                }
                let delay = backoff.next_delay_duration().ok_or_else(|| {
                    Error::StringError("txn file: region retry exhausted".to_owned())
                })?;
                tokio::time::sleep(delay).await;
                Ok(())
            }
        }
    }

    async fn backoff_region_miss(&mut self, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        match self {
            Self::Source(owner) => owner
                .lock()
                .await
                .backoff(BO_REGION_MISS, reason)
                .await
                .map_err(|error| Error::StringError(error.to_string())),
            Self::Legacy(backoff) => {
                let delay = backoff.next_delay_duration().ok_or_else(|| {
                    Error::StringError("txn file: region retry exhausted".to_owned())
                })?;
                tokio::time::sleep(delay).await;
                Ok(())
            }
        }
    }

    async fn backoff_rpc(&mut self, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        match self {
            Self::Source(owner) => owner
                .lock()
                .await
                .backoff(BO_TIKV_RPC, reason)
                .await
                .map_err(|error| Error::StringError(error.to_string())),
            Self::Legacy(backoff) => {
                let delay = backoff.next_delay_duration().ok_or_else(|| {
                    Error::StringError("txn file: RPC retry exhausted".to_owned())
                })?;
                tokio::time::sleep(delay).await;
                Ok(())
            }
        }
    }

    async fn backoff_lock(&mut self, max_sleep_ms: u64, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        match self {
            Self::Source(owner) => owner
                .lock()
                .await
                .backoff_with_config_and_max_sleep(BO_TXN_LOCK, Some(max_sleep_ms), reason)
                .await
                .map_err(|error| Error::StringError(error.to_string())),
            Self::Legacy(_) => {
                tokio::time::sleep(Duration::from_millis(max_sleep_ms)).await;
                Ok(())
            }
        }
    }

    async fn fork(&self) -> Self {
        match self {
            Self::Source(owner) => {
                let (forked, _) = owner.lock().await.fork();
                Self::Source(Arc::new(tokio::sync::Mutex::new(forked)))
            }
            Self::Legacy(backoff) => Self::Legacy(backoff.clone()),
        }
    }

    async fn detached_clone(&self) -> Self {
        match self {
            Self::Source(owner) => Self::Source(Arc::new(tokio::sync::Mutex::new(
                owner.lock().await.clone(),
            ))),
            Self::Legacy(backoff) => Self::Legacy(backoff.clone()),
        }
    }
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

fn apply_snapshot_resource_group_tag<R: StoreRequest>(
    request: &mut R,
    static_tag: Option<&Vec<u8>>,
    transaction_tagger: Option<&TransactionResourceGroupTagger>,
    snapshot_tagger: Option<&SnapshotResourceGroupTagger>,
    request_type: SnapshotRequestType,
) -> Option<Vec<u8>> {
    if let Some(static_tag) = static_tag {
        return Some(static_tag.clone());
    }
    if let Some(transaction_tagger) = transaction_tagger {
        transaction_tagger(request);
        return None;
    }
    snapshot_tagger.map(|tagger| tagger(request_type))
}

#[derive(Clone)]
struct AggressiveLockEntry {
    has_return_value: bool,
    has_check_existence: bool,
    value: ReturnedValue,
    actual_for_update_ts: Timestamp,
}

impl AggressiveLockEntry {
    fn try_skip_locking_on_retry(&mut self, return_value: bool, check_existence: bool) -> bool {
        if self.value.locked_with_conflict_ts != 0 {
            // ForceLock metadata describes how this lock was acquired, not a
            // conflict that the retried statement should observe again.
            self.value.locked_with_conflict_ts = 0;
        } else {
            if !self.has_return_value && return_value {
                return false;
            }
            if !return_value && !self.has_check_existence && check_existence {
                return false;
            }
        }
        if !return_value {
            self.has_return_value = false;
            self.value.value.clear();
        }
        if !check_existence {
            self.has_check_existence = false;
            self.value.exists = true;
        }
        true
    }
}

struct AggressiveLockingContext {
    current: BTreeMap<Key, AggressiveLockEntry>,
    previous: BTreeMap<Key, AggressiveLockEntry>,
    assigned_primary_key: bool,
    last_assigned_primary_key: bool,
    primary_key: Option<Key>,
    last_primary_key: Option<Key>,
    attempt_start: Instant,
    last_attempt_start: Option<Instant>,
}

impl Default for AggressiveLockingContext {
    fn default() -> Self {
        Self {
            current: BTreeMap::new(),
            previous: BTreeMap::new(),
            assigned_primary_key: false,
            last_assigned_primary_key: false,
            primary_key: None,
            last_primary_key: None,
            attempt_start: Instant::now(),
            last_attempt_start: None,
        }
    }
}

#[derive(Clone, Default)]
struct PipelinedTransactionState {
    generation: u64,
    range_start: Option<Vec<u8>>,
    range_end: Option<Vec<u8>>,
    flush_wait_duration: Duration,
    flush_duration_ewma_ms: f64,
    min_commit_ts: MinCommitTsManager,
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
    /// Immutable transaction identity used by locks, prewrite, commit, and
    /// rollback. client-go keeps this as `KVTxn.startTS`.
    timestamp: Timestamp,
    /// Read version owned by the transaction's snapshot. client-go keeps this
    /// independently as `KVSnapshot.version`, and `SetSnapshotTS` must not
    /// change the transaction start timestamp.
    snapshot_timestamp: Timestamp,
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
    /// Selects client-go's async callback BatchGet accounting path. Both
    /// source paths dispatch all initial region batches concurrently.
    enable_async_batch_get: bool,
    /// Snapshot-only request-context settings retained through physical read
    /// retries, matching client-go `KVSnapshot`.
    not_fill_cache: bool,
    isolation_level: kvrpcpb::IsolationLevel,
    task_id: u64,
    resource_group_tag: Option<Vec<u8>>,
    resource_group_tagger: Option<SnapshotResourceGroupTagger>,
    transaction_resource_group_tagger: Option<TransactionResourceGroupTagger>,
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
    is_heartbeat_started: Arc<atomic::AtomicBool>,
    heartbeat_generation: Arc<atomic::AtomicU64>,
    committer_initialized: bool,
    pessimistic_lock_count: usize,
    /// Set once the transaction enters the commit path (`StartedCommit`), where
    /// prewrite may place 2PC locks. Kept as a dedicated flag because the status
    /// transitions to `StartedRollback` on rollback, losing the fact that commit
    /// had started — which a rollback retry would otherwise need to know.
    prewritten: bool,
    aggressive_locking: Option<AggressiveLockingContext>,
    aggressive_locking_dirty: bool,
    /// Per-key lock timestamps produced by force/aggressive locking. TiKV
    /// verifies these indexed constraints during prewrite so an intervening
    /// lock replacement cannot be committed accidentally.
    for_update_ts_constraints: BTreeMap<Vec<u8>, u64>,
    /// Cancels background pipelined flush retries when the transaction is
    /// committed, rolled back, or dropped.
    pipelined_cancellation: crate::async_util::Cancellation,
    pipelined_heartbeat_started: Arc<atomic::AtomicBool>,
    pipelined_heartbeat_failed: Arc<atomic::AtomicBool>,
    pipelined_state: PipelinedTransactionState,
    start_instant: Instant,
    latches: Option<Arc<LatchesScheduler>>,
}

impl<PdC: PdClient> Transaction<PdC> {
    /// Constructs a transaction over an injected PD/KV client.
    ///
    /// This is the Rust counterpart of client-go's ordinary-build injected
    /// client path. Embedded and in-process stores do not need to enable
    /// crate-internal test behavior to construct a transaction.
    #[doc(hidden)]
    pub fn new(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
    ) -> Transaction<PdC> {
        Self::try_new(timestamp, rpc, options, keyspace)
            .expect("invalid injected transaction options")
    }

    /// Constructs a transaction over an injected PD/KV client and validates
    /// the same pipelined options as client-go's `NewTiKVTxn`.
    ///
    /// `new` remains as the original compatibility surface for downstream
    /// in-process users; new callers that accept untrusted options should use
    /// this fallible constructor.
    pub fn try_new(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        options: TransactionOptions,
        keyspace: Keyspace,
    ) -> Result<Transaction<PdC>> {
        options.validate()?;
        Ok(Self::new_with_latches_and_keyspace_name(
            timestamp, rpc, options, keyspace, None, None,
        ))
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
        let pipelined = options.pipelined.enable;
        let start_version = timestamp.version();
        let mut transaction = Transaction {
            status: Arc::new(AtomicU8::new(status as u8)),
            timestamp: timestamp.clone(),
            snapshot_timestamp: timestamp,
            buffer: Buffer::new_with_keyspace(options.is_pessimistic(), keyspace),
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
            enable_async_batch_get: false,
            not_fill_cache: false,
            isolation_level: kvrpcpb::IsolationLevel::Si,
            task_id: 0,
            resource_group_tag: None,
            resource_group_tagger: None,
            transaction_resource_group_tagger: None,
            snapshot_read_timeout: None,
            read_timestamp_validator: None,
            snapshot_visibility_validator: None,
            read_replica_scope: String::new(),
            read_lock_context: ReadLockContext::default(),
            lock_resolver_context: ResolveLocksContext::default(),
            is_heartbeat_started: Arc::new(atomic::AtomicBool::new(false)),
            heartbeat_generation: Arc::new(atomic::AtomicU64::new(0)),
            committer_initialized: pipelined,
            pessimistic_lock_count: 0,
            prewritten: false,
            aggressive_locking: None,
            aggressive_locking_dirty: false,
            for_update_ts_constraints: BTreeMap::new(),
            pipelined_cancellation: crate::async_util::Cancellation::default(),
            pipelined_heartbeat_started: Arc::new(atomic::AtomicBool::new(false)),
            pipelined_heartbeat_failed: Arc::new(atomic::AtomicBool::new(false)),
            pipelined_state: PipelinedTransactionState::default(),
            start_instant: std::time::Instant::now(),
            latches,
        };
        if pipelined {
            transaction.set_snapshot_pipelined(start_version);
            transaction.configure_pipelined_memdb();
        }
        transaction
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

    /// Configure replica selection for reads through this transaction's
    /// snapshot. This is the direct Rust counterpart of configuring the value
    /// returned by client-go `KVTxn.GetSnapshot`.
    pub fn set_replica_read_config(&mut self, config: ReplicaReadConfig) {
        self.replica_read_config = config;
    }

    pub(crate) fn set_enable_async_batch_get(&mut self, enabled: bool) {
        self.enable_async_batch_get = enabled;
    }

    /// Set the replica-read type for this transaction's snapshot.
    pub fn set_replica_read(&mut self, read_type: crate::ReplicaReadType) {
        self.set_replica_read_config(ReplicaReadConfig {
            read_type,
            ..Default::default()
        });
    }

    pub(crate) fn set_lock_resolver_context(&mut self, context: ResolveLocksContext) {
        self.lock_resolver_context = context;
    }

    /// Reset the transaction-owned snapshot's read timestamp without changing
    /// the transaction start timestamp, and discard retry hints that were
    /// valid only for the previous read version.
    pub fn set_snapshot_timestamp(&mut self, timestamp: Timestamp) {
        let version = timestamp.version();
        assert!(
            version < i64::MAX as u64 || version == u64::MAX,
            "try to get snapshot with a large ts {version}"
        );
        self.snapshot_timestamp = timestamp;
        self.buffer.clear_cached_reads();
        self.read_lock_context.clear_resolved();
    }

    /// Return the number of reads served from the transaction snapshot cache.
    pub fn snapshot_cache_hit_count(&self) -> usize {
        self.buffer.snapshot_cache_hit_count()
    }

    /// Return the number of entries in the transaction snapshot cache.
    pub fn snapshot_cache_size(&self) -> usize {
        self.buffer.snapshot_cache_size()
    }

    /// Return a logical-key copy of the transaction snapshot cache.
    pub fn snapshot_cache(&self) -> BTreeMap<Key, ValueEntry> {
        self.buffer
            .snapshot_cache()
            .into_iter()
            .map(|(key, value)| (key.truncate_keyspace(self.keyspace), value))
            .collect()
    }

    /// Seed entries in the transaction snapshot cache.
    pub fn update_snapshot_cache(
        &mut self,
        keys: impl IntoIterator<Item = Key>,
        values: BTreeMap<Key, ValueEntry>,
    ) {
        if self.snapshot_timestamp.version() == u64::MAX {
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

    /// Remove the supplied logical keys from the transaction snapshot cache.
    pub fn clean_snapshot_cache(&mut self, keys: impl IntoIterator<Item = Key>) {
        self.buffer.clean_snapshot_cache(
            keys.into_iter()
                .map(|key| key.encode_keyspace(self.keyspace, KeyMode::Txn)),
        );
    }

    /// Mark reads through this transaction's snapshot as stale reads.
    pub fn set_stale_read(&mut self, stale_read: bool) {
        self.replica_read_config.stale_read = stale_read;
    }

    /// Replace store-label constraints for this transaction's snapshot.
    pub fn set_match_store_labels(&mut self, labels: Vec<crate::proto::metapb::StoreLabel>) {
        self.replica_read_config.labels = labels;
    }

    /// Set the busy-store threshold used by load-based replica reads.
    pub fn set_load_based_replica_read_threshold(&mut self, busy_threshold: Duration) {
        self.replica_read_config.busy_threshold_ms = u32::try_from(busy_threshold.as_millis())
            .ok()
            .filter(|threshold| *threshold != 0)
            .unwrap_or_default();
    }

    /// Set the per-request replica-read adjustment callback.
    pub fn set_replica_read_adjuster(&mut self, adjuster: ReplicaReadAdjuster) {
        self.replica_read_adjuster = Some(adjuster);
    }

    /// Set TiKV's scan sampling step for this transaction's snapshot.
    pub fn set_sample_step(&mut self, sample_step: u32) {
        self.sample_step = sample_step;
    }

    /// Return keys without values from subsequent transaction snapshot scans.
    pub fn set_snapshot_key_only(&mut self, key_only: bool) {
        self.snapshot_key_only = key_only;
    }

    /// Set the physical scanner batch size for this transaction's snapshot.
    pub fn set_snapshot_scan_batch_size(&mut self, batch_size: u32) {
        self.snapshot_scan_batch_size = batch_size;
    }

    pub(crate) fn snapshot_scan_batch_size(&self) -> u32 {
        if self.snapshot_scan_batch_size <= 1 {
            DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE
        } else {
            self.snapshot_scan_batch_size
        }
    }

    /// Attach runtime statistics to this transaction's snapshot requests.
    pub fn set_snapshot_runtime_stats(&mut self, stats: Option<Arc<SnapshotRuntimeStats>>) {
        self.snapshot_runtime_stats = stats;
    }

    /// Set retry variables only for this transaction's snapshot reads.
    pub fn set_snapshot_variables(&mut self, variables: Arc<Variables>) {
        self.snapshot_variables = variables;
    }

    /// Source pipelined snapshots must read through locks flushed by their
    /// own transaction rather than trying to resolve them.
    pub fn set_snapshot_pipelined(&mut self, timestamp: u64) {
        self.snapshot_pipelined = true;
        self.read_lock_context.add_resolved(timestamp);
    }

    /// Control TiKV cache population for this transaction's snapshot reads.
    pub fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        self.not_fill_cache = not_fill_cache;
    }

    /// Set the isolation level for this transaction's snapshot reads.
    pub fn set_isolation_level(&mut self, isolation_level: kvrpcpb::IsolationLevel) {
        self.isolation_level = isolation_level;
    }

    /// Set TiKV's scheduling task ID for this transaction's snapshot reads.
    pub fn set_task_id(&mut self, task_id: u64) {
        self.task_id = task_id;
    }

    pub fn set_resource_group_tag(&mut self, resource_group_tag: Option<Vec<u8>>) {
        self.commit_settings.resource_group_tag = resource_group_tag.clone();
        self.resource_group_tag = resource_group_tag;
    }

    /// Set the snapshot-only resource-group tagger.
    pub fn set_snapshot_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<SnapshotResourceGroupTagger>,
    ) {
        self.resource_group_tagger = resource_group_tagger;
    }

    /// Set client-go's request-aware resource-group tagger for both reads and
    /// writes. A static tag configured with [`Self::set_resource_group_tag`]
    /// takes precedence.
    pub fn set_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<TransactionResourceGroupTagger>,
    ) {
        self.transaction_resource_group_tagger = resource_group_tagger.clone();
        self.commit_settings.resource_group_tagger = resource_group_tagger;
        self.configure_pipelined_memdb();
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

    /// Set read-timestamp validation scope for transaction snapshot reads.
    pub fn set_read_replica_scope(&mut self, scope: impl Into<String>) {
        self.read_replica_scope = scope.into();
    }

    /// Set the timeout for each physical transaction snapshot read.
    pub fn set_snapshot_read_timeout(&mut self, timeout: Duration) {
        self.snapshot_read_timeout = (!timeout.is_zero()).then_some(timeout);
    }

    /// Return the transaction snapshot read-timeout override.
    pub fn snapshot_read_timeout(&self) -> Option<Duration> {
        self.snapshot_read_timeout
    }

    fn configure_pipelined_memdb(&mut self) {
        if !self.is_pipelined() {
            return;
        }
        let primary_hint = self.buffer.get_primary_key();
        let start_version = self.timestamp.clone();
        let rpc = self.rpc.clone();
        let options = self.options.clone();
        let settings = self.commit_settings.clone();
        let write_throttle_ratio = self.commit_settings.pipelined.write_throttle_ratio;
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let rpc_interceptor = self.rpc_interceptor.clone();
        let resource_group_name = self.resource_group_name.clone();
        let resource_control = self.resource_control.clone();
        let ru_details = self.ru_details.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let pipelined_state = self.pipelined_state.clone();
        let pipelined_cancellation = self.pipelined_cancellation.clone();
        let pipelined_heartbeat_started = self.pipelined_heartbeat_started.clone();
        let pipelined_heartbeat_failed = self.pipelined_heartbeat_failed.clone();
        let start_instant = self.start_instant;
        let is_pessimistic = self.is_pessimistic();
        self.buffer.mem_buffer().configure_managed_pipelined_flush(
            move |generation, memdb, managed_primary| {
                let logical_bounds = memdb_key_bounds(&memdb);
                let mut mutations = proto_mutations_from_memdb(&memdb, keyspace, is_pessimistic);
                if settings.assertion_level == kvrpcpb::AssertionLevel::Off {
                    for mutation in &mut mutations {
                        mutation.assertion = kvrpcpb::Assertion::None as i32;
                    }
                }
                let eligible_primary = |mutation: &&kvrpcpb::Mutation| {
                    !matches!(
                        kvrpcpb::Op::try_from(mutation.op),
                        Ok(kvrpcpb::Op::CheckNotExists
                            | kvrpcpb::Op::SharedLock
                            | kvrpcpb::Op::SharedPessimisticLock)
                    )
                };
                let primary = managed_primary
                    .map(Key::from)
                    .or_else(|| primary_hint.clone())
                    .or_else(|| {
                        mutations
                            .iter()
                            .find(eligible_primary)
                            .map(|mutation| Key::from(mutation.key.clone()))
                    });
                let ttl_manager_closed = pipelined_heartbeat_failed.load(atomic::Ordering::Acquire);
                let metadata = ManagedPipelinedFlushMetadata {
                    generation,
                    primary_key: primary.as_ref().map(|key| <&[u8]>::from(key).to_vec()),
                    range_start: (!ttl_manager_closed)
                        .then(|| logical_bounds.as_ref().map(|bounds| bounds.0.clone()))
                        .flatten(),
                    range_end: (!ttl_manager_closed)
                        .then(|| logical_bounds.as_ref().map(|bounds| bounds.1.clone()))
                        .flatten(),
                };
                let flush_started = Instant::now();
                let result = if ttl_manager_closed {
                    Err(PipelinedError::message("ttl manager is closed"))
                } else if mutations.iter().any(|mutation| {
                    matches!(
                        kvrpcpb::Op::try_from(mutation.op),
                        Ok(kvrpcpb::Op::SharedLock | kvrpcpb::Op::SharedPessimisticLock)
                    )
                }) {
                    Err(PipelinedError::message(
                        "shared lock is not supported in pipelined transaction",
                    ))
                } else if mutations.is_empty() {
                    Ok(())
                } else if primary.is_none() {
                    Err(PipelinedError::message(
                        "[pipelined dml] primary key should be set before pipelined flush",
                    ))
                } else {
                    let mut committer = Committer::new(
                        primary,
                        mutations.clone(),
                        start_version.clone(),
                        rpc.clone(),
                        options.clone(),
                        settings.clone(),
                        keyspace,
                        keyspace_name.clone(),
                        rpc_interceptor.clone(),
                        resource_group_name.clone(),
                        resource_control.clone(),
                        ru_details.clone(),
                        lock_resolver_context.clone(),
                        pipelined_state.clone(),
                        memdb.size() as u64,
                        memdb.size() as u64,
                        start_instant,
                    );
                    let heartbeat_committer = committer.clone();
                    let heartbeat_cancellation = pipelined_cancellation.clone();
                    let heartbeat_started = pipelined_heartbeat_started.clone();
                    let heartbeat_failed = pipelined_heartbeat_failed.clone();
                    let heartbeat_callback: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
                        heartbeat_committer.start_pipelined_heartbeat(
                            heartbeat_cancellation.clone(),
                            heartbeat_started.clone(),
                            heartbeat_failed.clone(),
                        );
                    });
                    let result = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| PipelinedError::message(error.to_string()))
                        .and_then(|runtime| {
                            runtime.block_on(async {
                                tokio::select! {
                                    _ = pipelined_cancellation.cancelled() => {
                                        Err(PipelinedError::message("pipelined flush cancelled"))
                                    }
                                    result = committer.flush_pipelined_generation(
                                        mutations,
                                        generation,
                                        Some(heartbeat_callback),
                                    ) => result.map_err(PipelinedError::from),
                                }
                            })
                        });
                    result
                };
                ManagedPipelinedFlushOutcome {
                    metadata,
                    result,
                    flush_duration: flush_started.elapsed(),
                }
            },
        );
        let remote_rpc = self.rpc.clone();
        let remote_timestamp = self.snapshot_timestamp.clone();
        let remote_keyspace = self.keyspace;
        let remote_keyspace_name = self.keyspace_name.clone();
        let remote_rpc_interceptor = snapshot_runtime_interceptor(
            self.rpc_interceptor.clone(),
            self.snapshot_runtime_stats.clone(),
        );
        let remote_snapshot_runtime_stats = self.snapshot_runtime_stats.clone();
        let remote_snapshot_variables = self.snapshot_variables.clone();
        let remote_resource_group_name = self.resource_group_name.clone();
        let remote_resource_control = self.resource_control.clone();
        let remote_ru_details = self.ru_details.clone();
        let remote_retry_options = self.options.retry_options.clone();
        let remote_priority = self.options.priority;
        let remote_not_fill_cache = self.not_fill_cache;
        let remote_isolation_level = self.isolation_level;
        let remote_task_id = self.task_id;
        let remote_resource_group_tag = self.resource_group_tag.clone();
        let remote_resource_group_tagger = self.resource_group_tagger.clone();
        let remote_transaction_resource_group_tagger =
            self.transaction_resource_group_tagger.clone();
        let remote_snapshot_read_timeout = self.snapshot_read_timeout;
        let remote_read_timestamp_validator = self.read_timestamp_validator.clone();
        let remote_snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let remote_read_replica_scope = self.read_replica_scope.clone();
        let remote_replica_read_config = self.replica_read_config.clone();
        let remote_replica_read_adjuster = self.replica_read_adjuster.clone();
        let remote_read_lock_context = self.read_lock_context.clone();
        let remote_lock_resolver_context = self.lock_resolver_context.clone();
        let remote_request_source = self.commit_settings.request_source.context_value();
        let remote_internal = self.commit_settings.request_source.is_internal();
        let remote_enable_async_batch_get = self.enable_async_batch_get;
        self.buffer
            .mem_buffer()
            .configure_managed_remote_batch_get(move |logical_keys| {
                let logical_keys = logical_keys.to_vec();
                let rpc = remote_rpc.clone();
                let timestamp = remote_timestamp.clone();
                let keyspace_name = remote_keyspace_name.clone();
                let rpc_interceptor = remote_rpc_interceptor.clone();
                let snapshot_runtime_stats = remote_snapshot_runtime_stats.clone();
                let snapshot_variables = remote_snapshot_variables.clone();
                let resource_group_name = remote_resource_group_name.clone();
                let resource_control = remote_resource_control.clone();
                let ru_details = remote_ru_details.clone();
                let retry_options = remote_retry_options.clone();
                let resource_group_tag = remote_resource_group_tag.clone();
                let resource_group_tagger = remote_resource_group_tagger.clone();
                let transaction_resource_group_tagger =
                    remote_transaction_resource_group_tagger.clone();
                let read_timestamp_validator = remote_read_timestamp_validator.clone();
                let snapshot_visibility_validator = remote_snapshot_visibility_validator.clone();
                let read_replica_scope = remote_read_replica_scope.clone();
                let replica_read_config = remote_replica_read_config.clone();
                let replica_read_adjuster = remote_replica_read_adjuster.clone();
                let read_lock_context = remote_read_lock_context.clone();
                let lock_resolver_context = remote_lock_resolver_context.clone();
                let request_source = remote_request_source.clone();
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|_| crate::error::StaticError::Unknown)?;
                    runtime.block_on(async move {
                        let snapshot_version = timestamp.version();
                        let physical_keys = logical_keys
                            .iter()
                            .cloned()
                            .map(|key| {
                                Key::from(key).encode_keyspace(remote_keyspace, KeyMode::Txn)
                            })
                            .collect::<Vec<_>>();
                        let stale_read = replica_read_config.stale_read;
                        let mut request = new_buffer_batch_get_request(
                            physical_keys.into_iter(),
                            timestamp.clone(),
                        );
                        let resource_group_tag = apply_snapshot_resource_group_tag(
                            &mut request,
                            resource_group_tag.as_ref(),
                            transaction_resource_group_tagger.as_ref(),
                            resource_group_tagger.as_ref(),
                            SnapshotRequestType::BufferBatchGet,
                        );
                        let plan = plan_with_keyspace_name(
                            rpc,
                            remote_keyspace,
                            keyspace_name.as_deref(),
                            rpc_interceptor,
                            resource_group_name.as_deref(),
                            resource_control,
                            ru_details,
                            replica_read_config,
                            request,
                        )
                        .snapshot_replica_read_adjuster(replica_read_adjuster)
                        .request_source(request_source)
                        .priority(remote_priority)
                        .not_fill_cache(remote_not_fill_cache)
                        .isolation_level(remote_isolation_level)
                        .task_id(remote_task_id)
                        .resource_group_tag(resource_group_tag)
                        .snapshot_read_timeout(
                            remote_snapshot_read_timeout,
                            SNAPSHOT_READ_TIMEOUT_MEDIUM,
                        )
                        .validate_read_timestamp(
                            read_timestamp_validator,
                            timestamp.version(),
                            stale_read,
                            read_replica_scope,
                        )
                        .resolve_lock_for_read(
                            timestamp,
                            retry_options.lock_backoff,
                            remote_keyspace,
                            read_lock_context,
                            lock_resolver_context,
                            snapshot_runtime_stats.clone(),
                            snapshot_variables.clone(),
                        )
                        .retry_multi_region_with_snapshot_stats(
                            retry_options.region_backoff,
                            snapshot_runtime_stats,
                            snapshot_variables,
                        )
                        .observe_snapshot_regions(remote_internal)
                        .async_batch_get_metrics(remote_enable_async_batch_get)
                        .merge(Collect)
                        .plan();
                        let pairs = plan
                            .execute()
                            .await
                            .map_err(|_| crate::error::StaticError::Unknown)?;
                        if let Some(validator) = snapshot_visibility_validator {
                            validator
                                .check_visibility(snapshot_version)
                                .await
                                .map_err(|_| crate::error::StaticError::Unknown)?;
                        }
                        Ok(pairs
                            .into_iter()
                            .map(|pair| pair.encode_keyspace(remote_keyspace, KeyMode::Txn))
                            .map(|pair| pair.truncate_keyspace(remote_keyspace))
                            .map(|pair| (<&[u8]>::from(&pair.0).to_vec(), pair.1))
                            .collect())
                    })
                })
                .join()
                .map_err(|_| crate::error::StaticError::Unknown)?
            });
        self.buffer
            .mem_buffer()
            .set_managed_write_throttle_ratio(write_throttle_ratio);
    }

    fn sync_pipelined_state_from_memdb(&mut self) {
        let metadata = self.buffer.mem_buffer().managed_pipelined_metadata();
        self.pipelined_state.generation = metadata.generation;
        self.pipelined_state.range_start = metadata.range_start;
        self.pipelined_state.range_end = metadata.range_end;
        self.pipelined_state.flush_wait_duration =
            self.buffer.mem_buffer().metrics().flush_wait_duration;
        self.pipelined_state.flush_duration_ewma_ms =
            self.buffer.mem_buffer().managed_flush_duration_ewma_ms();
    }

    async fn maybe_flush_pipelined(&mut self, force: bool) -> Result<bool> {
        if !self.is_pipelined() {
            return Ok(false);
        }
        self.configure_pipelined_memdb();
        let flushed = self.buffer.mem_buffer().flush(force).map_err(Error::from)?;
        self.sync_pipelined_state_from_memdb();
        Ok(flushed)
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

    fn snapshot_scanner_replica_read_config(&self) -> ReplicaReadConfig {
        ReplicaReadConfig {
            read_type: self.replica_read_config.read_type,
            busy_threshold_ms: self.replica_read_config.busy_threshold_ms,
            ..Default::default()
        }
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
        let key = key.into();
        if self.is_pipelined() {
            let physical_key = key.clone().encode_keyspace(self.keyspace, KeyMode::Txn);
            if let Some(value) = self.buffer.pipelined_value(&physical_key) {
                return Ok(value);
            }
            return Ok(self
                .batch_get_from_buffer(std::iter::once(key))
                .await?
                .next()
                .map(|pair| pair.1));
        }
        let timestamp = self.snapshot_timestamp.clone();
        let snapshot_version = timestamp.version();
        let cache_snapshot_read = timestamp.version() != u64::MAX;
        let rpc = self.rpc.clone();
        let key = key.encode_keyspace(self.keyspace, KeyMode::Txn);
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
        let transaction_resource_group_tagger = self.transaction_resource_group_tagger.clone();
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
                let mut request = new_get_request(key, timestamp.clone());
                let resource_group_tag = apply_snapshot_resource_group_tag(
                    &mut request,
                    resource_group_tag.as_ref(),
                    transaction_resource_group_tagger.as_ref(),
                    resource_group_tagger.as_ref(),
                    SnapshotRequestType::Get,
                );
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
                .force_lite_lock_resolution()
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
        let key = key.into();
        if self.is_pipelined() {
            // PipelinedMemDB intentionally ignores Get options: its remote
            // buffer tier exposes values with commit timestamp zero.
            return self
                .get(key)
                .await
                .map(|value| value.map(|value| ValueEntry::new(value, 0)));
        }
        let _timer = crate::stats::snapshot_command_timer(
            "get",
            self.commit_settings.request_source.is_internal(),
        );
        trace!("invoking transactional get request with options");
        self.check_allow_operation().await?;
        let mut get_options = GetOptions::default();
        get_options.apply(options);
        let return_commit_ts = get_options.return_commit_ts();
        let timestamp = self.snapshot_timestamp.clone();
        let snapshot_version = timestamp.version();
        let cache_snapshot_read = timestamp.version() != u64::MAX;
        let rpc = self.rpc.clone();
        let key = key.encode_keyspace(self.keyspace, KeyMode::Txn);
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
        let transaction_resource_group_tagger = self.transaction_resource_group_tagger.clone();
        let snapshot_read_timeout = self.snapshot_read_timeout;
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let read_replica_scope = self.read_replica_scope.clone();
        let replica_read_config = self.replica_read_config_for_items(1);
        let read_lock_context = self.read_lock_context.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let request_source = self.commit_settings.request_source.context_value();
        let max_timestamp_point_get = timestamp.version() == u64::MAX;

        let entry = self
            .buffer
            .get_snapshot_entry_or_else(
                key,
                return_commit_ts,
                cache_snapshot_read,
                |key| async move {
                    let mut request = new_get_request(key, timestamp.clone());
                    request.need_commit_ts = return_commit_ts;
                    let resource_group_tag = apply_snapshot_resource_group_tag(
                        &mut request,
                        resource_group_tag.as_ref(),
                        transaction_resource_group_tagger.as_ref(),
                        resource_group_tagger.as_ref(),
                        SnapshotRequestType::Get,
                    );
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
                    .force_lite_lock_resolution()
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
                    let entry = (!response.not_found && !response.value.is_empty())
                        .then(|| ValueEntry::new(response.value, response.commit_ts));
                    Ok(entry)
                },
            )
            .await?;
        // client-go inserts a successful point-read result into the snapshot
        // cache before enforcing the ReturnCommitTS assertion. BatchGet keeps
        // its distinct check-before-cache ordering.
        ensure_snapshot_commit_ts(return_commit_ts, entry.as_ref())?;
        Ok(entry)
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
        let keys = keys.into_iter().map(Into::into).collect::<Vec<Key>>();
        if self.is_pipelined() {
            let mut buffered = Vec::new();
            let mut remote_keys = Vec::new();
            let mut remote_physical_keys = Vec::new();
            for key in keys {
                let physical_key = key.clone().encode_keyspace(self.keyspace, KeyMode::Txn);
                match self.buffer.pipelined_batch_value(&physical_key) {
                    Some(Some(value)) => buffered.push(KvPair(key, value)),
                    Some(None) => {}
                    None => {
                        remote_keys.push(key);
                        remote_physical_keys.push(physical_key);
                    }
                }
            }
            if !remote_keys.is_empty() {
                let fetched = self
                    .batch_get_from_buffer(remote_keys)
                    .await?
                    .collect::<Vec<_>>();
                let fetched_by_physical = fetched
                    .iter()
                    .map(|pair| {
                        (
                            pair.0.clone().encode_keyspace(self.keyspace, KeyMode::Txn),
                            pair.1.clone(),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                self.buffer
                    .cache_pipelined_batch_get(remote_physical_keys, &fetched_by_physical);
                buffered.extend(fetched);
            }
            return Ok(buffered.into_iter());
        }
        let timestamp = self.snapshot_timestamp.clone();
        let snapshot_version = timestamp.version();
        let cache_snapshot_read = timestamp.version() != u64::MAX;
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
            .map(move |key| key.encode_keyspace(keyspace, KeyMode::Txn));
        let retry_options = self.options.retry_options.clone();
        let priority = self.options.priority;
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let transaction_resource_group_tagger = self.transaction_resource_group_tagger.clone();
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
        let enable_async_batch_get = self.enable_async_batch_get;

        self.buffer
            .batch_get_or_else_with_cache(keys, cache_snapshot_read, move |keys| async move {
                let _timer = crate::stats::snapshot_command_timer("batch_get", internal);
                let keys = keys.collect::<Vec<_>>();
                let stale_read = replica_read_config.stale_read;
                let mut request = new_batch_get_request(keys.into_iter(), timestamp.clone());
                let resource_group_tag = apply_snapshot_resource_group_tag(
                    &mut request,
                    resource_group_tag.as_ref(),
                    transaction_resource_group_tagger.as_ref(),
                    resource_group_tagger.as_ref(),
                    SnapshotRequestType::BatchGet,
                );
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
                .snapshot_replica_read_adjuster(replica_read_adjuster)
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
                .async_batch_get_metrics(enable_async_batch_get)
                .merge(Collect)
                .plan();
                let pairs = plan.execute().await?;
                if let Some(validator) = snapshot_visibility_validator {
                    validator.check_visibility(snapshot_version).await?;
                }
                Ok(pairs
                    .into_iter()
                    .filter(|pair| !pair.1.is_empty())
                    .map(|pair| pair.encode_keyspace(keyspace, KeyMode::Txn))
                    .collect())
            })
            .await
            .map(move |pairs| {
                pairs
                    .map(move |pair| pair.truncate_keyspace(keyspace))
                    .collect::<Vec<_>>()
                    .into_iter()
            })
    }

    /// Batch-read snapshot entries with source `GetOption` behavior. Missing
    /// keys are omitted and every returned [`ValueEntry`] retains its commit
    /// timestamp only when [`GetOption::ReturnCommitTs`] is requested.
    pub async fn batch_get_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        options: &[GetOption],
    ) -> Result<BTreeMap<Key, ValueEntry>> {
        let keys = keys.into_iter().map(Into::into).collect::<Vec<Key>>();
        if self.is_pipelined() {
            // PipelinedMemDB ignores BatchGet options and reports remote
            // buffer values without snapshot commit timestamps.
            return Ok(self
                .batch_get(keys)
                .await?
                .map(|pair| (pair.0, ValueEntry::new(pair.1, 0)))
                .collect());
        }
        debug!("invoking transactional batch_get request with options");
        self.check_allow_operation().await?;
        let mut get_options = GetOptions::default();
        get_options.apply(options);
        let return_commit_ts = get_options.return_commit_ts();
        let timestamp = self.snapshot_timestamp.clone();
        let snapshot_version = timestamp.version();
        let cache_snapshot_read = timestamp.version() != u64::MAX;
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
            .map(move |key| key.encode_keyspace(keyspace, KeyMode::Txn));
        let retry_options = self.options.retry_options.clone();
        let priority = self.options.priority;
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let transaction_resource_group_tagger = self.transaction_resource_group_tagger.clone();
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
        let enable_async_batch_get = self.enable_async_batch_get;

        self.buffer
            .batch_get_snapshot_entries_or_else(
                keys,
                return_commit_ts,
                cache_snapshot_read,
                move |keys| async move {
                    let _timer = crate::stats::snapshot_command_timer("batch_get", internal);
                    let keys = keys.collect::<Vec<_>>();
                    let stale_read = replica_read_config.stale_read;
                    let mut request = new_batch_get_request(keys.into_iter(), timestamp.clone());
                    request.need_commit_ts = return_commit_ts;
                    let resource_group_tag = apply_snapshot_resource_group_tag(
                        &mut request,
                        resource_group_tag.as_ref(),
                        transaction_resource_group_tagger.as_ref(),
                        resource_group_tagger.as_ref(),
                        SnapshotRequestType::BatchGet,
                    );
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
                    .snapshot_replica_read_adjuster(replica_read_adjuster)
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
                    .async_batch_get_metrics(enable_async_batch_get)
                    .plan();
                    let responses = plan.execute().await?;
                    if let Some(validator) = snapshot_visibility_validator {
                        validator.check_visibility(snapshot_version).await?;
                    }
                    let responses = responses.into_iter().collect::<Result<Vec<_>>>()?;
                    let entries: BTreeMap<_, _> = responses
                        .into_iter()
                        .flat_map(|response| response.pairs)
                        .filter(|pair| !pair.value.is_empty())
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
    pub async fn batch_get_from_buffer(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<impl Iterator<Item = KvPair>> {
        self.batch_get_from_buffer_with_options(keys, &[]).await
    }

    /// Read values from the pipelined transaction buffer tier with point-read
    /// options. [`GetOption::ReturnCommitTs`] is accepted but intentionally
    /// ignored: client-go's buffer-tier values are not committed and therefore
    /// have no commit timestamp.
    pub async fn batch_get_from_buffer_with_options(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        _options: &[GetOption],
    ) -> Result<impl Iterator<Item = KvPair>> {
        let keys = keys.into_iter().map(Into::into).collect::<Vec<Key>>();
        if keys.is_empty() {
            // client-go returns before validating the tier, constructing a
            // backoffer, recording metrics, or checking visibility.
            return Ok(Vec::<KvPair>::new().into_iter());
        }
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
        let timestamp = self.snapshot_timestamp.clone();
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
            .map(move |key| key.encode_keyspace(keyspace, KeyMode::Txn))
            .collect::<Vec<_>>();
        let retry_options = self.options.retry_options.clone();
        let priority = self.options.priority;
        let not_fill_cache = self.not_fill_cache;
        let isolation_level = self.isolation_level;
        let task_id = self.task_id;
        let resource_group_tag = self.resource_group_tag.clone();
        let resource_group_tagger = self.resource_group_tagger.clone();
        let transaction_resource_group_tagger = self.transaction_resource_group_tagger.clone();
        let snapshot_read_timeout = self.snapshot_read_timeout;
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let read_replica_scope = self.read_replica_scope.clone();
        let replica_read_config = self.replica_read_config.clone();
        let replica_read_adjuster = self.replica_read_adjuster.clone();
        let stale_read = replica_read_config.stale_read;
        let read_lock_context = self.read_lock_context.clone();
        let lock_resolver_context = self.lock_resolver_context.clone();
        let request_source = self.commit_settings.request_source.context_value();
        let internal = self.commit_settings.request_source.is_internal();
        let enable_async_batch_get = self.enable_async_batch_get;
        let mut request = new_buffer_batch_get_request(keys.into_iter(), timestamp.clone());
        let resource_group_tag = apply_snapshot_resource_group_tag(
            &mut request,
            resource_group_tag.as_ref(),
            transaction_resource_group_tagger.as_ref(),
            resource_group_tagger.as_ref(),
            SnapshotRequestType::BufferBatchGet,
        );
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
        .snapshot_replica_read_adjuster(replica_read_adjuster)
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
        .async_batch_get_metrics(enable_async_batch_get)
        .merge(Collect)
        .plan();
        let pairs = plan.execute().await?;
        if let Some(validator) = snapshot_visibility_validator {
            validator.check_visibility(snapshot_version).await?;
        }
        Ok(pairs
            .into_iter()
            .map(|pair| pair.encode_keyspace(keyspace, KeyMode::Txn))
            .map(move |pair| pair.truncate_keyspace(keyspace))
            .collect::<Vec<_>>()
            .into_iter())
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
        self.buffer.put(key.clone(), value)?;
        if let Some(options) = options {
            self.buffer.set_mutation_options(&key, options)?;
        }
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
        self.buffer.insert(key.clone(), value)?;
        if let Some(options) = options {
            self.buffer.set_mutation_options(&key, options)?;
        }
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
        self.buffer.delete(key.clone())?;
        if let Some(options) = options {
            self.buffer.set_mutation_options(&key, options)?;
        }
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
        for mutation in mutations {
            self.buffer.mutate(mutation)?;
        }
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
        // client-go's zero-value LockCtx lazily initializes its private wait
        // setting to LockAlwaysWait. Keep the convenience API equivalent;
        // callers that explicitly want TiKV's numeric wait value `0` can use
        // `lock_keys_with_wait_time`.
        self.lock_keys_with_wait_time(LOCK_ALWAYS_WAIT, keys).await
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

    /// Lock keys and invoke `callback` after the source aggressive-lock
    /// applicability preflight, including any later error.
    pub async fn lock_keys_with_context_and_callback(
        &mut self,
        context: &mut LockContext,
        keys: impl IntoIterator<Item = impl Into<Key>>,
        callback: impl FnOnce(),
    ) -> Result<()> {
        self.lock_keys_with_context_inner(
            context,
            keys.into_iter().map(Into::into).collect(),
            callback,
        )
        .await
    }

    async fn lock_keys_with_context_inner<F: FnOnce()>(
        &mut self,
        context: &mut LockContext,
        keys: Vec<Key>,
        callback: F,
    ) -> Result<()> {
        debug!("invoking transactional lock_keys request");
        self.check_allow_operation().await?;
        // client-go decides whether aggressive locking is applicable from the
        // caller's original key list, before deduplication or exclusion of
        // locks already acquired in this stage.
        if self.aggressive_locking.is_some() {
            if context.in_share_mode {
                return Err(Error::StringError(
                    "shared lock is not supported in aggressive/fair locking mode".to_owned(),
                ));
            }
            if keys.len() > 1 {
                self.done_aggressive_locking().await?;
            }
        }
        // client-go registers LockKeysFunc's defer only after
        // exitAggressiveLockingIfInapplicable. Keep the callback alive across
        // every later return (and future cancellation), but do not invoke it
        // when that source preflight rejects shared aggressive locking.
        let _callback = LockKeysCallbackGuard::new(callback);
        if self.aggressive_locking.is_some() && !self.is_pessimistic() {
            return Err(Error::StringError(
                "trying to perform aggressive locking in optimistic transaction".to_owned(),
            ));
        }
        if context.in_share_mode && self.is_pipelined() {
            return Err(Error::StringError(
                "shared lock is not supported in pipelined transaction".to_owned(),
            ));
        }
        let keyspace = self.keyspace;
        let mut keys: Vec<Key> = keys
            .into_iter()
            .map(move |key| key.encode_keyspace(keyspace, KeyMode::Txn))
            .collect();
        keys.sort_unstable();
        keys.dedup();

        let mut pending = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(entry) = self
                .aggressive_locking
                .as_ref()
                .and_then(|aggressive| aggressive.current.get(&key))
            {
                if self.buffer.needs_check_exists(&key) && entry.value.exists {
                    return Err(pessimistic_key_exists_error(&key, self.keyspace));
                }
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
                continue;
            }
            if self.buffer.is_shared_locked(&key) && !context.in_share_mode {
                return Err(Error::StringError(
                    "upgrading a shared lock to an exclusive lock is not supported".to_owned(),
                ));
            }
            if self.buffer.is_locked(&key) {
                if self.is_pessimistic()
                    && self.buffer.needs_check_exists(&key)
                    && self.buffer.locked_value_exists(&key)
                {
                    return Err(pessimistic_key_exists_error(&key, self.keyspace));
                }
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
            self.maybe_flush_pipelined(false).await?;
            return Ok(());
        }
        // client-go advances the transaction's for-update timestamp before
        // filtering aggressive locks. A retry that reuses every lock still
        // carries the newer statement timestamp into commit and rollback.
        self.options
            .push_for_update_ts(Timestamp::from_version(context.for_update_ts));

        // client-go initializes the committer as soon as a real pessimistic
        // lock attempt reaches this point. In particular, it remains
        // initialized when a first shared lock is rejected for lacking an
        // exclusive primary, which controls whether a later SetSessionID call
        // takes effect.
        self.committer_initialized = true;

        if context.in_share_mode && self.buffer.get_primary_key().is_none() {
            return Err(Error::StringError(
                "pessimistic lock in share mode requires primary key to be selected".to_owned(),
            ));
        }
        if self.aggressive_locking.is_some() && pending.len() == 1 {
            let key = pending[0].clone();
            let assigned_primary = if self.buffer.get_primary_key().is_none() {
                let primary = self
                    .aggressive_locking
                    .as_ref()
                    .and_then(|aggressive| aggressive.last_primary_key.as_ref())
                    .filter(|previous| *previous == &key)
                    .cloned()
                    .unwrap_or_else(|| key.clone());
                self.buffer.primary_key_or(&primary);
                let aggressive = self
                    .aggressive_locking
                    .as_mut()
                    .expect("aggressive locking checked above");
                aggressive.assigned_primary_key = true;
                aggressive.primary_key = Some(primary);
                true
            } else {
                false
            };
            if let Some(entry) = self
                .aggressive_locking
                .as_ref()
                .and_then(|aggressive| aggressive.previous.get(&key))
            {
                if context.for_update_ts < entry.value.locked_with_conflict_ts {
                    return Err(Error::StringError(format!(
                        "transaction {} retries aggressive locking with for-update timestamp {} below prior conflict timestamp {}",
                        self.timestamp.version(),
                        context.for_update_ts,
                        entry.value.locked_with_conflict_ts,
                    )));
                }
            }
            let (previous_lock_is_fresh, can_try_skip, reused) = {
                let aggressive = self
                    .aggressive_locking
                    .as_mut()
                    .expect("aggressive locking checked above");
                let previous_lock_is_fresh = aggressive.last_attempt_start.is_some_and(|started| {
                    started.elapsed().as_millis() < managed_lock_ttl() as u128
                });
                let can_try_skip = !aggressive.assigned_primary_key
                    || aggressive.last_primary_key == aggressive.primary_key;
                let reused = aggressive.previous.remove(&key);
                (previous_lock_is_fresh, can_try_skip, reused)
            };
            if let Some(mut entry) = reused {
                if previous_lock_is_fresh
                    && can_try_skip
                    && entry
                        .try_skip_locking_on_retry(context.return_values, context.check_existence)
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
            if assigned_primary {
                let primary_changed = self.aggressive_locking.as_ref().is_some_and(|aggressive| {
                    aggressive.last_primary_key != aggressive.primary_key
                });
                if primary_changed {
                    self.reset_auto_heartbeat();
                }
                // Let pessimistic_lock_impl own this call's assignment so its
                // failure and LockOnlyIfExists cleanup paths can reset it.
                self.buffer.reset_primary_key();
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
        if let Err(error) = self.rollback_aggressive_keys(&context.previous).await {
            warn!(
                "failed to clean up redundant aggressive locks during retry, start_ts: {}, error: {}",
                self.timestamp.version(),
                error
            );
        }
        if context.assigned_primary_key {
            context.assigned_primary_key = false;
            context.last_assigned_primary_key = true;
            // Keep the heartbeat running until the retry selects its primary.
            // If it selects a different key, the next lock call moves the
            // heartbeat before acquiring the new lock, as client-go does.
            self.buffer.reset_primary_key();
        }
        context.last_primary_key = context.primary_key.take();
        context.last_attempt_start = Some(context.attempt_start);
        context.attempt_start = Instant::now();
        context.previous = std::mem::take(&mut context.current);
        self.aggressive_locking = Some(context);
        Ok(())
    }

    pub async fn cancel_aggressive_locking(&mut self) -> Result<()> {
        let context = self
            .aggressive_locking
            .take()
            .expect("trying to cancel aggressive locking while it is not started");
        if context.assigned_primary_key || context.last_assigned_primary_key {
            self.reset_auto_heartbeat();
            self.buffer.reset_primary_key();
        }
        let mut keys = context.previous;
        keys.extend(context.current);
        self.pessimistic_lock_count = self.pessimistic_lock_count.saturating_sub(keys.len());
        if let Err(error) = self.rollback_aggressive_keys(&keys).await {
            warn!(
                "failed to clean up aggressive locks during cancel, start_ts: {}, error: {}",
                self.timestamp.version(),
                error
            );
        }
        Ok(())
    }

    pub async fn done_aggressive_locking(&mut self) -> Result<()> {
        let context = self
            .aggressive_locking
            .take()
            .expect("trying to finish aggressive locking while it is not started");
        if context.last_assigned_primary_key && !context.assigned_primary_key {
            // The primary came only from a previous aggressive attempt and no
            // lock survived the final attempt, so no key remains for the TTL
            // manager to protect.
            self.reset_auto_heartbeat();
            self.buffer.reset_primary_key();
        }
        self.pessimistic_lock_count = self
            .pessimistic_lock_count
            .saturating_sub(context.previous.len());
        if let Err(error) = self.rollback_aggressive_keys(&context.previous).await {
            warn!(
                "failed to clean up redundant aggressive locks while finishing, start_ts: {}, error: {}",
                self.timestamp.version(),
                error
            );
        }
        for (key, mut entry) in context.current {
            if !entry.has_return_value && !entry.has_check_existence {
                entry.value.exists = true;
            }
            self.for_update_ts_constraints
                .entry(<&[u8]>::from(&key).to_vec())
                .or_insert(entry.actual_for_update_ts.version());
            self.buffer
                .lock_with_returned_value(key, false, Some(&entry.value))
                .expect("aggressive exclusive lock cannot violate lock-mode invariants");
        }
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

        // client-go initializes the pipelined committer eagerly, then treats an
        // untouched MemBuffer as an ordinary read-only commit. Do not rotate an
        // empty generation: without a flushed key range there are no locks to
        // commit or resolve.
        if self.is_pipelined() && !self.buffer.mem_buffer().dirty() {
            self.prewritten = false;
            self.commit_timestamp = None;
            self.set_status(TransactionStatus::Committed);
            return Ok(None);
        }

        // Pipelined transactions commit only after the authoritative MemDB's
        // final mutable generation has been handed to the background flush
        // worker and every flush result has been observed.
        if self.is_pipelined() {
            self.configure_pipelined_memdb();
            if let Err(error) = self.buffer.mem_buffer().flush(true).map_err(Error::from) {
                self.pipelined_cancellation.cancel();
                return Err(error);
            }
            if let Err(error) = self.buffer.mem_buffer().flush_wait().map_err(Error::from) {
                self.pipelined_cancellation.cancel();
                return Err(error);
            }
            self.pipelined_cancellation.cancel();
            self.sync_pipelined_state_from_memdb();
            if self.pipelined_state.range_start.is_none()
                || self.pipelined_state.range_end.is_none()
            {
                return Err(Error::StringError(
                    "unexpected empty pipelinedStart or pipelinedEnd".to_owned(),
                ));
            }
        }

        let mut mutations = Vec::new();
        let mut stashed_assertion = None;
        let mut prewrite_only_keys = Vec::new();
        // client-go invokes KvFilter while walking every value-bearing MemDB
        // entry, before operation lowering. The buffer builder also preserves
        // the mutation prefix when the callback fails so pessimistic locks can
        // be rolled back with the same key set as initKeysAndMutations.
        let built_mutations = if let Some(filter) = &self.commit_settings.kv_filter {
            match self.buffer.to_proto_mutations_with_filter(filter.as_ref()) {
                Ok(mutations) => mutations,
                Err((partial_mutations, error)) => {
                    self.prewritten = false;
                    self.spawn_pessimistic_initialization_rollback(partial_mutations);
                    return Err(error);
                }
            }
        } else {
            self.buffer.to_proto_mutations()
        };
        let skip_assertion_check_from_lock =
            crate::util::eval_failpoint("assertionSkipCheckFromLock", |_| ())
                .ok()
                .flatten()
                .is_some();
        for mut mutation in built_mutations {
            if self.commit_settings.assertion_level == kvrpcpb::AssertionLevel::Off {
                mutation.assertion = kvrpcpb::Assertion::None as i32;
            }
            let physical_key = Key::from(mutation.key.clone());
            let logical_key = physical_key.clone().truncate_keyspace(self.keyspace);
            if self.is_pessimistic()
                && self.commit_settings.assertion_level != kvrpcpb::AssertionLevel::Off
                && !skip_assertion_check_from_lock
                && stashed_assertion.is_none()
            {
                stashed_assertion = self
                    .buffer
                    .assertion_failure(&Key::from(mutation.key.clone()), self.timestamp.version());
            }
            if kvrpcpb::Op::try_from(mutation.op) == Ok(kvrpcpb::Op::CheckNotExists) {
                prewrite_only_keys.push(<&[u8]>::from(&logical_key).to_vec());
            }
            mutations.push(mutation);
        }
        for key in prewrite_only_keys {
            self.buffer
                .mem_buffer()
                .update_flags(&key, &[FlagsOp::SetPrewriteOnly]);
        }
        let has_shared_locks = mutations.iter().any(|mutation| {
            matches!(
                kvrpcpb::Op::try_from(mutation.op),
                Ok(kvrpcpb::Op::SharedLock | kvrpcpb::Op::SharedPessimisticLock)
            )
        });
        let has_flushed_pipelined_mutations =
            self.is_pipelined() && self.pipelined_state.generation > 0;
        let primary_key = self
            .buffer
            .get_primary_key()
            .filter(|primary| {
                has_flushed_pipelined_mutations
                    || mutations.iter().any(|mutation| {
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
                            Ok(kvrpcpb::Op::CheckNotExists
                                | kvrpcpb::Op::SharedLock
                                | kvrpcpb::Op::SharedPessimisticLock)
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
        if mutations.is_empty() && !has_flushed_pipelined_mutations {
            assert!(primary_key.is_none());
            self.prewritten = false;
            self.commit_timestamp = None;
            self.set_status(TransactionStatus::Committed);
            return Ok(None);
        }
        if primary_key.is_none() {
            self.prewritten = false;
            if has_shared_locks {
                self.spawn_pessimistic_initialization_rollback(mutations);
                return Err(Error::StringError(
                    "shared lock key cannot be used as transaction primary key".to_owned(),
                ));
            }
            self.spawn_pessimistic_initialization_rollback(mutations);
            return Err(Error::NoPrimaryKey);
        }
        // client-go derives txnSize while pushing the final mutation list:
        // filtered entries and newly-inserted tombstones that lower to no
        // operation do not affect lock TTL or the TTL-manager threshold.
        // Count logical key bytes because API-V2 request encoding is a Rust
        // transport boundary, not part of client-go's MemDB size.
        let write_size = mutations.iter().fold(0_u64, |total, mutation| {
            let logical_key = Key::from(mutation.key.clone()).truncate_keyspace(self.keyspace);
            total
                .saturating_add(<&[u8]>::from(&logical_key).len() as u64)
                .saturating_add(mutation.value.len() as u64)
        });
        let buffer_size = self.buffer.get_write_size() as u64;
        // Direct MemDB writers intentionally do not maintain a second primary
        // field. Once commit selects the first eligible mutation, retain that
        // source primary for heartbeat and rollback ownership.
        self.buffer
            .primary_key_or(primary_key.as_ref().expect("primary checked above"));
        if self.timestamp.version() == u64::MAX {
            self.prewritten = false;
            self.spawn_pessimistic_initialization_rollback(mutations);
            return Err(Error::StringError(format!(
                "try to commit with invalid txnStartTS: {}",
                self.timestamp.version()
            )));
        }

        let latch = if self.is_pipelined() || self.is_pessimistic() {
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

        let trace_context = crate::trace::current_trace_context();
        let commit_details = crate::util::commit_details_from_context(&trace_context).cloned();
        let auto_heartbeat_starter = self.auto_heartbeat_starter(None);
        let presume_key_not_exists_keys = self.buffer.presume_key_not_exists_keys();
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
            write_size,
            buffer_size,
            self.start_instant,
        )
        .with_pessimistic_lock_keys(self.buffer.pessimistic_lock_keys())
        .with_presume_key_not_exists_keys(presume_key_not_exists_keys)
        .with_constraint_check_keys(self.buffer.constraint_check_keys())
        .with_for_update_ts_constraints(self.for_update_ts_constraints.clone())
        .with_stashed_assertion(stashed_assertion)
        .with_auto_heartbeat_starter(auto_heartbeat_starter)
        .with_commit_details(commit_details);
        let res = committer
            .commit_with_value_discard(|| self.buffer.mem_buffer().discard_values())
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

    fn spawn_pessimistic_initialization_rollback(&self, mutations: Vec<kvrpcpb::Mutation>) {
        if !self.is_pessimistic() || mutations.is_empty() {
            return;
        }
        let rollback_keys = mutations
            .iter()
            .map(|mutation| mutation.key.clone())
            .collect();
        let committer = Committer::new(
            None,
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
            PipelinedTransactionState::default(),
            0,
            0,
            self.start_instant,
        )
        .with_pessimistic_lock_keys(rollback_keys);
        let start_timestamp = self.timestamp.version();
        tokio::spawn(async move {
            if let Err(error) = committer.rollback(false).await {
                warn!(
                    "failed to roll back pessimistic locks after mutation initialization error, start_ts: {}, error: {}",
                    start_timestamp, error
                );
            }
        });
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

        if self.is_pipelined() {
            self.configure_pipelined_memdb();
            self.pipelined_cancellation.cancel();
            if let Err(error) = self.buffer.mem_buffer().flush_wait() {
                warn!(
                    "pipelined flush failed before rollback, start_ts: {}, error: {}",
                    self.timestamp.version(),
                    error
                );
            }
            self.sync_pipelined_state_from_memdb();
        }

        let primary_key = self.buffer.get_primary_key();
        let mutations = self.buffer.to_proto_mutations();
        let pessimistic_lock_keys = self.buffer.pessimistic_lock_keys();
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
            self.buffer.get_write_size() as u64,
            self.start_instant,
        )
        .with_pessimistic_lock_keys(pessimistic_lock_keys);
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
        if self.committer_initialized {
            self.commit_settings.session_id = session_id;
        }
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
        self.buffer
            .mem_buffer()
            .set_memory_footprint_change_hook(Arc::new(hook));
    }

    pub fn memory_footprint(&self) -> u64 {
        self.buffer.memory_footprint()
    }

    pub fn memory_hook_set(&self) -> bool {
        self.buffer.memdb_memory_hook_is_set()
    }

    /// Returns the exact staged MemDB used by transaction reads and commit.
    /// Keys on this surface are logical in both API V1 and API V2, matching
    /// client-go's `KVTxn.GetMemBuffer` contract.
    pub fn get_mem_buffer(&mut self) -> &mut MemDb {
        self.configure_pipelined_memdb();
        self.buffer.mem_buffer()
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
        !self.buffer.mem_buffer_readonly().dirty() && !self.aggressive_locking_dirty
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
            self.start_instant.elapsed().as_millis() as u64 + managed_lock_ttl(),
        );
        self.commit_settings
            .apply_heartbeat_request(&mut request, MAX_WRITE_EXECUTION_TIME);
        let plan = plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            None,
            None,
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
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
        let timestamp = self.snapshot_timestamp.clone();
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
        let transaction_resource_group_tagger = self.transaction_resource_group_tagger.clone();
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let replica_read_config = self.snapshot_scanner_replica_read_config();
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
            let mut request = new_scan_request(
                encoded_range.clone(),
                timestamp.clone(),
                batch_size,
                key_only,
                reverse,
                sample_step,
            );
            let scan_resource_group_tag = apply_snapshot_resource_group_tag(
                &mut request,
                resource_group_tag.as_ref(),
                transaction_resource_group_tagger.as_ref(),
                resource_group_tagger.as_ref(),
                SnapshotRequestType::Scan,
            );
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
            .validate_snapshot_visibility(snapshot_visibility_validator.clone(), snapshot_version)
            .resolve_response_lock_for_scan(
                timestamp.clone(),
                retry_options.lock_backoff.clone(),
                keyspace,
                lock_resolver_context.clone(),
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
                let mut request = new_get_request(request_key, timestamp.clone());
                let get_resource_group_tag = apply_snapshot_resource_group_tag(
                    &mut request,
                    resource_group_tag.as_ref(),
                    transaction_resource_group_tagger.as_ref(),
                    resource_group_tagger.as_ref(),
                    SnapshotRequestType::Get,
                );
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
                .force_lite_lock_resolution()
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
        let timestamp = self.snapshot_timestamp.clone();
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
        let transaction_resource_group_tagger = self.transaction_resource_group_tagger.clone();
        let read_timestamp_validator = self.read_timestamp_validator.clone();
        let snapshot_visibility_validator = self.snapshot_visibility_validator.clone();
        let replica_read_config = self.snapshot_scanner_replica_read_config();
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
                    let (scan_start, scan_end) = range.clone().into_keys();
                    let mut pairs = Vec::new();

                    while pairs.len() < new_limit as usize {
                        let remaining = new_limit.saturating_sub(pairs.len() as u32);
                        let request_limit = remaining.min(scan_batch_size);
                        let mut request = new_scan_request(
                            range.clone(),
                            timestamp.clone(),
                            request_limit,
                            key_only,
                            reverse,
                            sample_step,
                        );
                        let scan_resource_group_tag = apply_snapshot_resource_group_tag(
                            &mut request,
                            resource_group_tag.as_ref(),
                            transaction_resource_group_tagger.as_ref(),
                            resource_group_tagger.as_ref(),
                            SnapshotRequestType::Scan,
                        );
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
                        .validate_snapshot_visibility(
                            snapshot_visibility_validator.clone(),
                            snapshot_version,
                        )
                        .resolve_response_lock_for_scan(
                            timestamp.clone(),
                            retry_options.lock_backoff.clone(),
                            keyspace,
                            lock_resolver_context.clone(),
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
                        let region_start: Key = region_batch.range.0.into();
                        let region_end: Key = region_batch.range.1.into();
                        let raw_batch = region_batch.pairs;
                        let raw_batch_len = raw_batch.len();
                        let raw_last_key = raw_batch.last().map(|raw_last| -> Result<Key> {
                            if raw_last.key.is_empty() {
                                let error = raw_last.error.as_ref().ok_or_else(|| {
                                    Error::StringError(
                                        "scan pair has neither a key nor a key error".to_owned(),
                                    )
                                })?;
                                Ok(extract_lock_from_key_error(error)?.key.into())
                            } else {
                                Ok(raw_last.key.clone().into())
                            }
                        });
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
                            let mut request = new_get_request(request_key, timestamp.clone());
                            let get_resource_group_tag = apply_snapshot_resource_group_tag(
                                &mut request,
                                resource_group_tag.as_ref(),
                                transaction_resource_group_tagger.as_ref(),
                                resource_group_tagger.as_ref(),
                                SnapshotRequestType::Get,
                            );
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
                            .force_lite_lock_resolution()
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
                        if pairs.len() >= new_limit as usize {
                            break;
                        }

                        let next_key = if raw_batch_len < request_limit as usize {
                            if reverse {
                                region_start
                            } else {
                                region_end
                            }
                        } else {
                            let raw_last_key = raw_last_key.ok_or_else(|| {
                                Error::StringError("full scanner batch is empty".to_owned())
                            })??;
                            let raw_last_key = raw_last_key.encode_keyspace(keyspace, KeyMode::Txn);
                            if reverse {
                                raw_last_key
                            } else {
                                raw_last_key.next_key()
                            }
                        };
                        let exhausted = if reverse {
                            next_key.is_empty() || next_key <= scan_start
                        } else {
                            next_key.is_empty()
                                || scan_end.as_ref().is_some_and(|end| next_key >= *end)
                        };
                        if exhausted {
                            break;
                        }
                        if reverse {
                            range.to = Bound::Excluded(next_key);
                        } else {
                            range.from = Bound::Included(next_key);
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

    async fn execute_pessimistic_lock_request(
        &self,
        request: kvrpcpb::PessimisticLockRequest,
        timing: PessimisticLockDispatchTiming,
        resource_group_tag: Vec<u8>,
        resource_group_tagger: Option<crate::kv::ResourceGroupTagger>,
        source_retry_owner: Option<Arc<tokio::sync::Mutex<RetryBackoffer>>>,
    ) -> Result<PessimisticLockOutput> {
        let decorated_requests = Arc::new(Mutex::new(BTreeMap::<
            Vec<Vec<u8>>,
            kvrpcpb::PessimisticLockRequest,
        >::new()));
        loop {
            let timing_for_dispatch = timing.clone();
            let resource_group_tag = resource_group_tag.clone();
            let resource_group_tagger = resource_group_tagger.clone();
            let decorated_requests = Arc::clone(&decorated_requests);
            let plan = self
                .plan(request.clone())
                .decorate_shard_request(
                    |request| {
                        request
                            .mutations
                            .iter()
                            .map(|mutation| mutation.key.clone())
                            .collect()
                    },
                    move |request| {
                        let identity = request
                            .mutations
                            .iter()
                            .map(|mutation| mutation.key.clone())
                            .collect::<Vec<_>>();
                        let cached = decorated_requests.lock().unwrap().get(&identity).cloned();
                        if let Some(cached) = cached {
                            *request = cached;
                        } else {
                            apply_pessimistic_lock_resource_tag(
                                request,
                                &resource_group_tag,
                                resource_group_tagger.as_ref(),
                            );
                            decorated_requests
                                .lock()
                                .unwrap()
                                .insert(identity, request.clone());
                        }
                    },
                )
                .prepare_request(move |request| {
                    timing_for_dispatch.prepare(request)?;
                    Ok(())
                })
                .priority(self.options.priority)
                .resolve_lock_with_context_and_pessimistic_region(
                    self.timestamp.clone(),
                    self.options.retry_options.lock_backoff.clone(),
                    self.keyspace,
                    self.lock_resolver_context.clone(),
                )
                .preserve_shard();
            let result = if let Some(owner) = source_retry_owner.as_ref() {
                plan.retry_multi_region_preserve_results_with_source_retry_owner(
                    self.options.retry_options.region_backoff.clone(),
                    Arc::clone(owner),
                )
                .merge(CollectPessimisticLock)
                .plan()
                .execute()
                .await
            } else {
                plan.retry_multi_region_preserve_results(
                    self.options.retry_options.region_backoff.clone(),
                )
                .merge(CollectPessimisticLock)
                .plan()
                .execute()
                .await
            };
            match result {
                Err(Error::PessimisticLockRetry) => continue,
                result => return result,
            }
        }
    }

    fn source_retry_owner(
        &self,
        max_sleep_ms: u64,
    ) -> Option<Arc<tokio::sync::Mutex<RetryBackoffer>>> {
        let defaults = if self.options.is_pessimistic() {
            RetryOptions::default_pessimistic()
        } else {
            RetryOptions::default_optimistic()
        };
        (self.options.retry_options == defaults).then(|| {
            Arc::new(tokio::sync::Mutex::new(RetryBackoffer::with_variables(
                crate::async_util::Cancellation::default(),
                max_sleep_ms,
                self.commit_settings.variables.clone(),
            )))
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn pessimistic_lock_impl(
        &mut self,
        locks: Vec<(Key, kvrpcpb::Assertion)>,
        need_value: bool,
        lock_type: kvrpcpb::Op,
        wait_timeout: i64,
        wake_up_mode: kvrpcpb::PessimisticLockWakeUpMode,
        context: Option<&mut LockContext>,
    ) -> Result<Vec<KvPair>> {
        let source_retry_owner = self.source_retry_owner(PESSIMISTIC_LOCK_MAX_BACKOFF);
        self.pessimistic_lock_impl_with_retry_owner(
            locks,
            need_value,
            lock_type,
            wait_timeout,
            wake_up_mode,
            context,
            source_retry_owner,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn pessimistic_lock_impl_with_retry_owner(
        &mut self,
        mut locks: Vec<(Key, kvrpcpb::Assertion)>,
        need_value: bool,
        lock_type: kvrpcpb::Op,
        wait_timeout: i64,
        wake_up_mode: kvrpcpb::PessimisticLockWakeUpMode,
        mut context: Option<&mut LockContext>,
        source_retry_owner: Option<Arc<tokio::sync::Mutex<RetryBackoffer>>>,
    ) -> Result<Vec<KvPair>> {
        assert!(
            matches!(self.options.kind, TransactionKind::Pessimistic(_)),
            "`pessimistic_lock` is only valid to use with pessimistic transactions"
        );
        if locks.is_empty() {
            return Ok(vec![]);
        }

        // client-go's pessimistic-lock lowering derives Assertion_NotExist
        // from the authoritative MemDB, including when the caller supplied a
        // plain key. This is what makes SetPresumeKeyNotExists reject an
        // existing snapshot value during LockKeys rather than deferring the
        // duplicate check until prewrite.
        for (key, assertion) in &mut locks {
            if self.buffer.presumes_key_not_exists(key) {
                *assertion = kvrpcpb::Assertion::NotExist;
            }
        }
        debug!(
            "acquiring pessimistic lock, start_ts: {}, keys: {}, need_value: {}",
            self.timestamp.version(),
            locks.len(),
            need_value,
        );

        let first_key = locks
            .iter()
            .map(|(key, _)| key)
            .min()
            .expect("non-empty locks checked above")
            .clone();
        let lock_assertions = locks.iter().cloned().collect::<BTreeMap<_, _>>();
        let keys = locks.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
        let existing_primary = self.buffer.get_primary_key();
        let primary_lock = existing_primary
            .clone()
            .unwrap_or_else(|| first_key.clone());
        let assigned_primary =
            existing_primary.is_none() && lock_type != kvrpcpb::Op::SharedPessimisticLock;
        let for_update_ts = match context.as_deref() {
            Some(context) => Timestamp::from_version(context.for_update_ts),
            None => self.rpc.clone().get_timestamp().await?,
        };
        let requested_wait_timeout = match context.as_deref_mut() {
            Some(context) => context.lock_wait_time(),
            None => wait_timeout,
        };
        let no_wait_requested = requested_wait_timeout == LOCK_NO_WAIT;
        self.options.push_for_update_ts(for_update_ts.clone());
        let timing = match context.as_deref_mut() {
            Some(context) => PessimisticLockDispatchTiming {
                start_instant: self.start_instant,
                killed: context.killed.clone(),
                wait_time: Some(requested_wait_timeout),
                wait_start_time: context.wait_start_time,
                max_execution_deadline: context.max_execution_deadline,
            },
            None => PessimisticLockDispatchTiming {
                start_instant: self.start_instant,
                killed: None,
                wait_time: None,
                wait_start_time: None,
                max_execution_deadline: None,
            },
        };
        let lock_ttl = self.start_instant.elapsed().as_millis() as u64 + managed_lock_ttl();
        let mut request = new_pessimistic_lock_request(
            locks.clone().into_iter(),
            primary_lock.clone(),
            self.timestamp.clone(),
            lock_ttl,
            for_update_ts.clone(),
            need_value,
        );
        for mutation in &mut request.mutations {
            mutation.op = lock_type as i32;
        }
        request.is_first_lock = self.pessimistic_lock_count == 0 && keys.len() == 1;
        request.min_commit_ts = for_update_ts.version().saturating_add(1);
        request.wait_timeout = if timing.wait_time.is_some() {
            let mut initial = request.clone();
            timing.prepare(&mut initial)?;
            initial.wait_timeout
        } else {
            wait_timeout
        };
        request.wake_up_mode = wake_up_mode as i32;
        if let Some(context) = context.as_deref() {
            request.return_values = context.return_values;
            request.check_existence = context.check_existence;
            request.lock_only_if_exists = context.lock_only_if_exists;
        }
        let lock_resource_group_tag = context
            .as_deref()
            .map(|context| context.resource_group_tag.clone())
            .unwrap_or_default();
        let lock_resource_group_tagger = context
            .as_deref()
            .and_then(|context| context.resource_group_tagger.clone());
        self.commit_settings
            .apply_pessimistic_lock_request(&mut request, MAX_WRITE_EXECUTION_TIME);

        // client-go routes pessimistic-lock mutations through the same
        // groupMutations pre-split gate as prewrite. Split before locating the
        // request shards so a large first lock operation immediately benefits
        // from the new topology.
        pre_split_large_mutation_regions(self.rpc.clone(), &request.mutations, "pessimistic lock")
            .await;

        // Locate first so the batch containing the primary can complete before
        // any secondary batch starts. This is the ordering contract enforced by
        // client-go's `doActionOnGroupMutations` for pessimistic locks.
        let located_shards = request
            .shards(&self.rpc)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let primary_batch = (located_shards.len() > 1)
            .then(|| {
                located_shards.iter().find_map(|((mutations, _), _)| {
                    mutations
                        .iter()
                        .any(|mutation| mutation.key == <&[u8]>::from(&primary_lock))
                        .then(|| mutations.clone())
                })
            })
            .flatten();

        if assigned_primary {
            self.buffer.primary_key_or(&primary_lock);
        }

        let output = if let Some(primary_mutations) = primary_batch {
            let mut primary_request = request.clone();
            primary_request.mutations = primary_mutations.clone();
            match self
                .execute_pessimistic_lock_request(
                    primary_request,
                    timing.clone(),
                    lock_resource_group_tag.clone(),
                    lock_resource_group_tagger.clone(),
                    source_retry_owner.clone(),
                )
                .await
            {
                Err(error) => Err(error),
                Ok(mut primary_output) => {
                    self.start_auto_heartbeat(context.as_deref()).await;
                    let primary_keys = primary_mutations
                        .iter()
                        .map(|mutation| mutation.key.clone())
                        .collect::<Vec<_>>();
                    let mut secondary_request = request;
                    secondary_request
                        .mutations
                        .retain(|mutation| !primary_keys.contains(&mutation.key));
                    match self
                        .execute_pessimistic_lock_request(
                            secondary_request,
                            timing,
                            lock_resource_group_tag.clone(),
                            lock_resource_group_tagger.clone(),
                            source_retry_owner.clone(),
                        )
                        .await
                    {
                        Ok(mut secondary_output) => {
                            primary_output.pairs.append(&mut secondary_output.pairs);
                            primary_output
                                .returned_values
                                .append(&mut secondary_output.returned_values);
                            primary_output.max_locked_with_conflict_ts = primary_output
                                .max_locked_with_conflict_ts
                                .max(secondary_output.max_locked_with_conflict_ts);
                            Ok(primary_output)
                        }
                        Err(Error::PessimisticLockError {
                            inner,
                            mut success_keys,
                        }) => {
                            success_keys.extend(primary_keys);
                            Err(Error::PessimisticLockError {
                                inner,
                                success_keys,
                            })
                        }
                        Err(error) => Err(Error::PessimisticLockError {
                            inner: Box::new(error),
                            success_keys: primary_keys,
                        }),
                    }
                }
            }
        } else {
            self.execute_pessimistic_lock_request(
                request,
                timing,
                lock_resource_group_tag,
                lock_resource_group_tagger,
                source_retry_owner,
            )
            .await
        };

        if output.is_err() && assigned_primary {
            self.reset_auto_heartbeat();
            self.buffer.reset_primary_key();
        }
        if output.is_err() {
            // A failed client-go pessimistic-lock attempt removes every
            // provisional PresumeKeyNotExists marker in the attempted batch.
            // Keeping it would incorrectly lower a later write to Insert.
            for key in &keys {
                if self.buffer.presumes_key_not_exists(key) {
                    self.buffer.unmark_presume_key_not_exists(key);
                }
            }
        }

        // TiKV receives whole milliseconds, so a wait constrained by
        // max_execution_time can return LockWaitTimeout just before the exact
        // client-side deadline. client-go checks the deadline, loops, and then
        // reports max execution time instead. Wait only that truncation tail
        // when max execution is the tighter constraint; a shorter explicit
        // lock wait must retain LockWaitTimeout.
        if let (Err(error), Some(context)) = (&output, context.as_deref()) {
            if let Some(deadline) = context.max_execution_deadline {
                let now = SystemTime::now();
                let lock_wait_deadline = (requested_wait_timeout > 0
                    && requested_wait_timeout != LOCK_ALWAYS_WAIT)
                    .then(|| {
                        context.wait_start_time.and_then(|started| {
                            started
                                .checked_add(Duration::from_millis(requested_wait_timeout as u64))
                        })
                    })
                    .flatten();
                let max_execution_limits_wait = requested_wait_timeout == LOCK_ALWAYS_WAIT
                    || lock_wait_deadline.is_some_and(|lock_deadline| deadline <= lock_deadline);
                if now >= deadline
                    || (max_execution_limits_wait && crate::error::is_lock_wait_timeout(error))
                {
                    if let Ok(remaining) = deadline.duration_since(now) {
                        tokio::time::sleep(remaining).await;
                    }
                    return Err(crate::error::QueryInterruptedWithSignalError {
                        signal: MAX_EXECUTION_TIME_EXCEEDED_SIGNAL,
                    }
                    .into());
                }
            }
        }

        if let Err(err) = output {
            let deadlock = pessimistic_deadlock(&err);
            let deadlock_is_retryable = deadlock.as_ref().is_some_and(|deadlock| {
                keys.iter()
                    .any(|key| farmhash::fingerprint64(key.as_ref()) == deadlock.deadlock_key_hash)
            });
            if let (Some(context), Some(deadlock)) = (context.as_deref(), deadlock.as_ref()) {
                if let Some(on_deadlock) = &context.on_deadlock {
                    on_deadlock(&crate::kv::DeadlockError {
                        deadlock: deadlock.clone(),
                        is_retryable: deadlock_is_retryable,
                    });
                }
            }
            let err = match err {
                Error::PessimisticLockError { inner, .. } => *inner,
                err => err,
            };
            // The source lock path returns the selected key error directly;
            // its region fan-out does not expose a one-element aggregate to
            // callers. Keep the same selection rule used by prewrite so an
            // assertion-only batch still yields its assertion failure.
            let err = normalize_prewrite_error(err);
            let definitive_single_key_failure = keys.len() == 1
                && (crate::error::is_write_conflict(&err) || crate::error::is_key_exists(&err));
            if !definitive_single_key_failure {
                debug!(
                    "pessimistic lock failed, rolling back {} potentially-acquired lock(s), start_ts: {}, for_update_ts: {}",
                    keys.len(),
                    self.timestamp.version(),
                    for_update_ts.version(),
                );
                if let Err(rollback_error) = self
                    .pessimistic_lock_rollback(
                        keys.iter().cloned(),
                        self.timestamp.clone(),
                        for_update_ts.clone(),
                    )
                    .await
                {
                    warn!(
                        "failed to roll back pessimistic locks after lock error, start_ts: {}, error: {}",
                        self.timestamp.version(),
                        rollback_error
                    );
                }
            }
            if deadlock_is_retryable {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            if let Some(deadlock) = deadlock {
                Err(crate::error::DeadlockError {
                    is_retryable: deadlock_is_retryable,
                    deadlock,
                }
                .into())
            } else if no_wait_requested && crate::error::is_lock_wait_timeout(&err) {
                Err(crate::error::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET.into())
            } else {
                Err(err)
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
                    let expose = value.locked_with_conflict_ts != 0
                        || context.return_values
                        || context.check_existence;
                    if !expose {
                        continue;
                    }
                    let logical_key = key.clone().truncate_keyspace(self.keyspace);
                    let mut value = value.clone();
                    // In ForceLock normal results, CheckExistence populates
                    // only Exists. A conflict result always carries its value
                    // regardless of the load-value options.
                    if value.locked_with_conflict_ts == 0 && !context.return_values {
                        value.value.clear();
                    }
                    context.insert_returned_value(<&[u8]>::from(&logical_key).to_vec(), value);
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
            if assigned_primary && locked_keys.is_empty() {
                self.reset_auto_heartbeat();
                self.buffer.reset_primary_key();
            }
            if !locked_keys.is_empty() && lock_type != kvrpcpb::Op::SharedPessimisticLock {
                self.buffer.primary_key_or(&first_key);
            }

            let aggressive = self.aggressive_locking.is_some()
                && wake_up_mode == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock;
            if !locked_keys.is_empty() {
                self.start_auto_heartbeat(context.as_deref()).await;
            }
            if aggressive && !locked_keys.is_empty() {
                self.aggressive_locking_dirty = true;
            }
            let mut aggressive_lock_error = None;
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
                    if value.locked_with_conflict_ts != 0
                        && value.locked_with_conflict_ts <= for_update_ts.version()
                    {
                        aggressive_lock_error.get_or_insert_with(|| {
                            Error::StringError(format!(
                                "pessimistic lock request to key {:?} returns LockedWithConflictTS({}) not greater than requested ForUpdateTS({})",
                                <&[u8]>::from(&key),
                                value.locked_with_conflict_ts,
                                for_update_ts.version(),
                            ))
                        });
                    }
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

            if let Some(error) = aggressive_lock_error {
                return Err(error);
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
            .apply_pessimistic_rollback_request(&mut req, MAX_WRITE_EXECUTION_TIME);
        let source_retry_owner = self.source_retry_owner(PESSIMISTIC_LOCK_MAX_BACKOFF);
        let plan = plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            None,
            None,
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            req,
        )
        .resolve_lock_with_context(
            start_version,
            self.options.retry_options.lock_backoff.clone(),
            self.keyspace,
            self.lock_resolver_context.clone(),
        );
        if let Some(owner) = source_retry_owner {
            plan.source_retry_owner(Arc::clone(&owner))
                .retry_multi_region_with_source_retry_owner(
                    self.options.retry_options.region_backoff.clone(),
                    owner,
                )
                .extract_error()
                .plan()
                .execute()
                .await?;
        } else {
            plan.retry_multi_region(self.options.retry_options.region_backoff.clone())
                .extract_error()
                .plan()
                .execute()
                .await?;
        }

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

    /// Returns whether this transaction is in pessimistic mode.
    pub fn is_pessimistic(&self) -> bool {
        matches!(self.options.kind, TransactionKind::Pessimistic(_))
    }

    fn auto_heartbeat_starter(
        &self,
        lock_context: Option<&LockContext>,
    ) -> Option<AutoHeartbeatStarter> {
        if self.is_pipelined() || !self.options.heartbeat_option.is_auto_heartbeat() {
            return None;
        }
        let primary_key = self.buffer.get_primary_key()?;
        let started = self.is_heartbeat_started.clone();
        let heartbeat_generation = self.heartbeat_generation.clone();
        let status = self.status.clone();
        let start_ts = self.timestamp.clone();
        let region_backoff = self.options.retry_options.region_backoff.clone();
        let rpc = self.rpc.clone();
        let heartbeat_interval = match self.options.heartbeat_option {
            HeartbeatOption::NoHeartbeat => DEFAULT_HEARTBEAT_INTERVAL,
            HeartbeatOption::Managed => Duration::from_millis(managed_lock_ttl() / 2),
            HeartbeatOption::FixedTime(heartbeat_interval) => heartbeat_interval,
        };
        let keyspace = self.keyspace;
        let keyspace_name = self.keyspace_name.clone();
        let rpc_interceptor = self.rpc_interceptor.clone();
        let ru_details = self.ru_details.clone();
        let commit_settings = self.commit_settings.clone();
        let lifecycle_hooks = self.commit_settings.lifecycle_hooks.clone();
        let killed = lock_context.and_then(|context| context.killed.clone());
        let lock_expired = lock_context.and_then(|context| context.lock_expired.clone());

        Some(Arc::new(move |min_commit_ts, is_txn_file| {
            if started
                .compare_exchange(
                    false,
                    true,
                    atomic::Ordering::AcqRel,
                    atomic::Ordering::Acquire,
                )
                .is_err()
            {
                return;
            }
            let generation = heartbeat_generation
                .fetch_add(1, atomic::Ordering::AcqRel)
                .wrapping_add(1);
            let current_generation = heartbeat_generation.clone();
            let status = status.clone();
            let start_ts = start_ts.clone();
            let primary_key = primary_key.clone();
            let region_backoff = region_backoff.clone();
            let rpc = rpc.clone();
            let keyspace_name = keyspace_name.clone();
            let rpc_interceptor = rpc_interceptor.clone();
            let ru_details = ru_details.clone();
            let commit_settings = commit_settings.clone();
            let lifecycle_hooks = lifecycle_hooks.clone();
            let killed = killed.clone();
            let lock_expired = lock_expired.clone();
            let min_commit_ts = min_commit_ts.clone();
            let start_ts_for_log = start_ts.version();
            debug!(
                "starting auto-heartbeat, start_ts: {}, interval: {:?}",
                start_ts_for_log, heartbeat_interval,
            );
            tokio::spawn(async move {
                if let Some(pre) = lifecycle_hooks.pre {
                    pre();
                }
                let mut consecutive_failures = 0_u32;
                loop {
                    tokio::time::sleep(heartbeat_interval).await;
                    if current_generation.load(atomic::Ordering::Acquire) != generation {
                        break;
                    }
                    let transaction_status: TransactionStatus =
                        status.load(atomic::Ordering::Acquire).into();
                    if matches!(
                        transaction_status,
                        TransactionStatus::Rolledback
                            | TransactionStatus::Committed
                            | TransactionStatus::Dropped
                    ) {
                        break;
                    }
                    if killed
                        .as_ref()
                        .is_some_and(|killed| killed.load(atomic::Ordering::Acquire) != 0)
                    {
                        break;
                    }
                    let now = match rpc.clone().get_timestamp().await {
                        Ok(now) => now,
                        Err(error) => {
                            warn!(
                                "auto-heartbeat get timestamp failed, start_ts: {}: {}",
                                start_ts_for_log, error
                            );
                            break;
                        }
                    };
                    let uptime = crate::oracle::extract_physical(now.version())
                        .saturating_sub(crate::oracle::extract_physical(start_ts.version()))
                        .max(0) as u64;
                    if uptime > crate::config::get_global_config().max_txn_ttl {
                        if let Some(lock_expired) = &lock_expired {
                            lock_expired.store(1, atomic::Ordering::Release);
                        }
                        break;
                    }
                    let mut request = new_heart_beat_request(
                        start_ts.clone(),
                        primary_key.clone(),
                        uptime.saturating_add(managed_lock_ttl()),
                    );
                    request.min_commit_ts = min_commit_ts.get();
                    request.is_txn_file = is_txn_file;
                    commit_settings.apply_heartbeat_request(&mut request, MAX_WRITE_EXECUTION_TIME);
                    let result = plan_with_keyspace_name(
                        rpc.clone(),
                        keyspace,
                        keyspace_name.as_deref(),
                        rpc_interceptor.clone(),
                        None,
                        None,
                        ru_details.clone(),
                        ReplicaReadConfig::default(),
                        request,
                    )
                    .retry_multi_region(region_backoff.clone())
                    .extract_error()
                    .merge(CollectSingle)
                    .post_process_default()
                    .plan()
                    .execute()
                    .await;
                    match result {
                        Ok(_) => consecutive_failures = 0,
                        Err(error) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            if heartbeat_error_stops_immediately(&error)
                                || consecutive_failures > 10
                            {
                                warn!(
                                    "auto-heartbeat stopped, start_ts: {}, consecutive failures: {}: {}",
                                    start_ts_for_log, consecutive_failures, error
                                );
                                break;
                            }
                        }
                    }
                }
                if let Some(post) = lifecycle_hooks.post {
                    post();
                }
            });
        }))
    }

    async fn start_auto_heartbeat(&mut self, lock_context: Option<&LockContext>) {
        if let Some(start) = self.auto_heartbeat_starter(lock_context) {
            start(MinCommitTsManager::default(), false);
        }
    }

    fn reset_auto_heartbeat(&mut self) {
        if self
            .is_heartbeat_started
            .swap(false, atomic::Ordering::AcqRel)
        {
            self.heartbeat_generation
                .fetch_add(1, atomic::Ordering::AcqRel);
        }
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
        self.pipelined_cancellation.cancel();
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

/// The default value of [`MANAGED_LOCK_TTL`], in milliseconds.
const MAX_TTL: u64 = 20000;
/// Process-wide managed lock TTL, matching client-go's mutable
/// `transaction.ManagedLockTTL` integration surface.
pub static MANAGED_LOCK_TTL: atomic::AtomicU64 = atomic::AtomicU64::new(MAX_TTL);

fn managed_lock_ttl() -> u64 {
    MANAGED_LOCK_TTL.load(atomic::Ordering::Relaxed)
}
/// The default TTL of a lock in milliseconds.
pub const DEFAULT_LOCK_TTL: u64 = 3000;
/// The default heartbeat interval
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(MAX_TTL / 2);
#[doc(hidden)]
pub const TTL_FACTOR: f64 = 6000.0;
/// Source-compatible test/integration control corresponding to
/// `ConfigProbe.StorePreSplitDetectThreshold`.
#[doc(hidden)]
pub static PRE_SPLIT_DETECT_THRESHOLD: atomic::AtomicU32 = atomic::AtomicU32::new(100_000);
/// Source-compatible test/integration control corresponding to
/// `ConfigProbe.StorePreSplitSizeThreshold`.
#[doc(hidden)]
pub static PRE_SPLIT_SIZE_THRESHOLD: atomic::AtomicU32 = atomic::AtomicU32::new(32 << 20);
const SPLIT_REGION_BACKOFF_MS: u64 = 20_000;
const MAX_SPLIT_REGIONS_BACKOFF_MS: u64 = 120_000;
const WAIT_SCATTER_REGION_BACKOFF_MS: u64 = 120_000;

async fn scatter_split_regions<PdC: PdClient>(
    rpc: Arc<PdC>,
    region_ids: &[u64],
    budget_ms: u64,
) -> Result<()> {
    let mut retry = RetryBackoffer::new(crate::async_util::Cancellation::default(), budget_ms);
    for &region_id in region_ids {
        loop {
            match rpc.clone().scatter_regions(vec![region_id], None).await {
                Ok(_) => break,
                Err(error) => retry
                    .backoff(
                        BO_PD_RPC,
                        format!("scatter split region {region_id} failed: {error}"),
                    )
                    .await
                    .map_err(|error| Error::StringError(error.to_string()))?,
            }
        }
    }
    Ok(())
}

async fn wait_scatter_region_finish<PdC: PdClient>(rpc: Arc<PdC>, region_id: u64) -> Result<()> {
    let mut retry = RetryBackoffer::new(
        crate::async_util::Cancellation::default(),
        WAIT_SCATTER_REGION_BACKOFF_MS,
    );
    loop {
        let reason = match rpc.clone().get_operator(region_id).await {
            Ok(response)
                if response.desc.as_slice() != b"scatter-region"
                    || response.status != crate::proto::pdpb::OperatorStatus::Running as i32 =>
            {
                return Ok(())
            }
            Ok(response) => {
                if let Some(error) = response
                    .header
                    .as_ref()
                    .and_then(|header| header.error.as_ref())
                {
                    return Err(Error::StringError(format!(
                        "wait scatter region {region_id} failed: {error:?}"
                    )));
                }
                format!("wait scatter region {region_id} timeout")
            }
            Err(error) => format!("wait scatter region {region_id} failed: {error}"),
        };
        retry
            .backoff(BO_REGION_MISS, reason)
            .await
            .map_err(|error| Error::StringError(error.to_string()))?;
    }
}

async fn pre_split_large_mutation_regions<PdC: PdClient>(
    rpc: Arc<PdC>,
    mutations: &[kvrpcpb::Mutation],
    operation: &str,
) {
    let detect_threshold = PRE_SPLIT_DETECT_THRESHOLD.load(atomic::Ordering::Relaxed) as usize;
    let size_threshold = PRE_SPLIT_SIZE_THRESHOLD.load(atomic::Ordering::Relaxed) as usize;
    if mutations.len() < detect_threshold {
        return;
    }
    let mut mutations = mutations.iter().collect::<Vec<_>>();
    mutations.sort_unstable_by(|left, right| left.key.cmp(&right.key));
    let mut start = 0;
    while start < mutations.len() {
        let region = match rpc
            .region_for_key(&Key::from(mutations[start].key.clone()))
            .await
        {
            Ok(region) => region,
            Err(error) => {
                warn!("{operation} pre-split lookup failed: {error}");
                return;
            }
        };
        let mut end = start + 1;
        while end < mutations.len() && region.contains(&Key::from(mutations[end].key.clone())) {
            end += 1;
        }
        if end - start >= detect_threshold {
            let mut accumulated = 0_usize;
            let mut split_keys = Vec::new();
            for mutation in &mutations[start..end] {
                accumulated = accumulated
                    .saturating_add(mutation.key.len())
                    .saturating_add(mutation.value.len());
                if accumulated >= size_threshold {
                    accumulated = 0;
                    split_keys.push(mutation.key.clone());
                }
            }
            if !split_keys.is_empty() {
                let scatter_budget = (split_keys.len() as u64)
                    .saturating_mul(SPLIT_REGION_BACKOFF_MS)
                    .min(MAX_SPLIT_REGIONS_BACKOFF_MS);
                match rpc.clone().split_regions(split_keys, 3).await {
                    Ok(region_ids)
                        if scatter_split_regions(rpc.clone(), &region_ids, scatter_budget)
                            .await
                            .is_ok() =>
                    {
                        for region_id in region_ids {
                            if let Err(error) =
                                wait_scatter_region_finish(rpc.clone(), region_id).await
                            {
                                warn!(
                                    "{operation} wait scatter region failed for region {region_id}: {error}"
                                );
                            }
                        }
                        rpc.invalidate_region_cache(region.ver_id()).await;
                    }
                    Ok(_) => warn!("{operation} scatter failed for region {}", region.id()),
                    Err(error) => warn!(
                        "{operation} pre-split failed for region {}: {error}",
                        region.id()
                    ),
                }
            }
        }
        start = end;
    }
}

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
    /// Optional caller-supplied start timestamp, matching root `tikv.WithStartTS`.
    start_timestamp: Option<Timestamp>,
    /// Optional pipelined transaction protocol.
    pipelined: PipelinedTxnOptions,
    #[doc(hidden)]
    heartbeat_option: HeartbeatOption,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum HeartbeatOption {
    NoHeartbeat,
    Managed,
    FixedTime(Duration),
}

impl Default for TransactionOptions {
    fn default() -> TransactionOptions {
        Self::new_pessimistic()
    }
}

impl TransactionOptions {
    pub(crate) fn with_config_commit_defaults(
        mut self,
        enable_async_commit: bool,
        enable_one_pc: bool,
    ) -> Self {
        self.async_commit |= enable_async_commit;
        self.try_one_pc |= enable_one_pc;
        self
    }

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
            start_timestamp: None,
            pipelined: PipelinedTxnOptions::default(),
            heartbeat_option: HeartbeatOption::Managed,
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
            start_timestamp: None,
            pipelined: PipelinedTxnOptions::default(),
            heartbeat_option: HeartbeatOption::Managed,
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

    /// Begin at an explicit timestamp instead of allocating one from PD.
    #[must_use]
    pub fn start_timestamp(mut self, timestamp: Timestamp) -> TransactionOptions {
        self.start_timestamp = Some(timestamp);
        self
    }

    pub(crate) fn configured_start_timestamp(&self) -> Option<Timestamp> {
        self.start_timestamp.clone()
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
    /// Full MemDB size used by client-go's txn-file eligibility gate. This is
    /// intentionally distinct from `write_size`, which excludes filtered and
    /// omitted mutations for lock-TTL calculations.
    buffer_size: u64,
    #[new(default)]
    pessimistic_lock_keys: BTreeSet<Vec<u8>>,
    /// Authoritative MemDB flag snapshot used to validate KeyExists replies.
    /// `None` keeps synthetic/test committers backward-compatible; real
    /// transactions always install a concrete snapshot, including an empty
    /// one, so an Op_Insert cached before a flag change cannot masquerade as a
    /// current PresumeKeyNotExists mutation.
    #[new(default)]
    presume_key_not_exists_keys: Option<BTreeSet<Vec<u8>>>,
    #[new(default)]
    constraint_check_keys: BTreeSet<Vec<u8>>,
    #[new(default)]
    for_update_ts_constraints: BTreeMap<Vec<u8>, u64>,
    #[new(default)]
    stashed_assertion: Option<kvrpcpb::AssertionFailed>,
    #[new(default)]
    auto_heartbeat_starter: Option<AutoHeartbeatStarter>,
    #[new(default)]
    commit_details: Option<crate::util::SharedCommitDetails>,
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
            buffer_size: self.buffer_size,
            pessimistic_lock_keys: self.pessimistic_lock_keys.clone(),
            presume_key_not_exists_keys: self.presume_key_not_exists_keys.clone(),
            constraint_check_keys: self.constraint_check_keys.clone(),
            for_update_ts_constraints: self.for_update_ts_constraints.clone(),
            stashed_assertion: self.stashed_assertion.clone(),
            auto_heartbeat_starter: self.auto_heartbeat_starter.clone(),
            commit_details: self.commit_details.clone(),
            start_instant: self.start_instant,
        }
    }
}

impl<PdC: PdClient> Committer<PdC> {
    fn attach_resource_group_name<R: StoreRequest>(&self, request: &mut R) {
        if let Some(resource_group_name) = self.resource_group_name.as_deref() {
            request.set_resource_group_name(resource_group_name);
        }
    }

    fn source_retry_owner(
        &self,
        max_sleep_ms: u64,
    ) -> Option<Arc<tokio::sync::Mutex<RetryBackoffer>>> {
        let defaults = if self.options.is_pessimistic() {
            RetryOptions::default_pessimistic()
        } else {
            RetryOptions::default_optimistic()
        };
        (self.options.retry_options == defaults).then(|| {
            Arc::new(tokio::sync::Mutex::new(RetryBackoffer::with_variables(
                crate::async_util::Cancellation::default(),
                max_sleep_ms,
                self.settings.variables.clone(),
            )))
        })
    }

    fn txn_file_retry_backoff(&self, max_sleep_ms: u64) -> TxnFileRetryBackoff {
        self.source_retry_owner(max_sleep_ms)
            .map(TxnFileRetryBackoff::Source)
            .unwrap_or_else(|| {
                TxnFileRetryBackoff::Legacy(self.options.retry_options.region_backoff.clone())
            })
    }

    fn mutation_presumes_key_not_exists(&self, key: &[u8]) -> bool {
        if let Some(keys) = &self.presume_key_not_exists_keys {
            return keys.contains(key);
        }
        self.mutations.iter().any(|mutation| {
            let logical_key = Key::from(mutation.key.clone()).truncate_keyspace(self.keyspace);
            (mutation.key.as_slice() == key || <&[u8]>::from(&logical_key) == key)
                && matches!(
                    kvrpcpb::Op::try_from(mutation.op),
                    Ok(kvrpcpb::Op::Insert | kvrpcpb::Op::CheckNotExists)
                )
        })
    }

    fn validate_key_exists_error(&self, error: Error) -> Error {
        match error {
            Error::KeyExists(error)
                if !self.mutation_presumes_key_not_exists(&error.already_exist.key) =>
            {
                Error::StringError(format!(
                    "session {}, existErr for key:{} should not be nil",
                    self.settings.session_id,
                    format_key_for_log(&error.already_exist.key),
                ))
            }
            Error::ExtractedErrors(errors) => Error::ExtractedErrors(
                errors
                    .into_iter()
                    .map(|error| self.validate_key_exists_error(error))
                    .collect(),
            ),
            Error::MultipleKeyErrors(errors) => Error::MultipleKeyErrors(
                errors
                    .into_iter()
                    .map(|error| self.validate_key_exists_error(error))
                    .collect(),
            ),
            error => error,
        }
    }

    fn with_pessimistic_lock_keys(mut self, keys: BTreeSet<Vec<u8>>) -> Self {
        self.pessimistic_lock_keys = keys;
        self
    }

    fn with_presume_key_not_exists_keys(mut self, keys: BTreeSet<Vec<u8>>) -> Self {
        self.presume_key_not_exists_keys = Some(keys);
        self
    }

    fn with_constraint_check_keys(mut self, keys: BTreeSet<Vec<u8>>) -> Self {
        self.constraint_check_keys = keys;
        self
    }

    fn with_for_update_ts_constraints(mut self, constraints: BTreeMap<Vec<u8>, u64>) -> Self {
        self.for_update_ts_constraints = constraints;
        self
    }

    fn with_stashed_assertion(mut self, assertion: Option<kvrpcpb::AssertionFailed>) -> Self {
        self.stashed_assertion = assertion;
        self
    }

    fn with_auto_heartbeat_starter(mut self, starter: Option<AutoHeartbeatStarter>) -> Self {
        self.auto_heartbeat_starter = starter;
        self
    }

    fn with_commit_details(mut self, details: Option<crate::util::SharedCommitDetails>) -> Self {
        self.commit_details = details;
        self
    }

    fn cleanup_without_wait(self, prewritten: bool) {
        let hooks = self.settings.lifecycle_hooks.clone();
        // client-go starts the cleanup goroutine before returning from
        // CleanupWithoutWait; its lifecycle pre-hook is therefore observable
        // immediately by the caller. Invoke it before handing the action to
        // Tokio, while keeping the rollback and post-hook asynchronous.
        if let Some(pre) = &hooks.pre {
            pre();
        }
        let start_timestamp = self.start_version.version();
        tokio::spawn(async move {
            if let Err(error) = self.rollback(prewritten).await {
                warn!(
                    "failed to clean up transaction after commit error, start_ts: {}, error: {}",
                    start_timestamp, error
                );
            }
            if let Some(post) = hooks.post {
                post();
            }
        });
    }

    async fn commit(self) -> Result<Option<Timestamp>> {
        self.commit_with_value_discard(|| {}).await
    }

    async fn commit_with_value_discard<F>(mut self, discard_values: F) -> Result<Option<Timestamp>>
    where
        F: FnOnce() + Send,
    {
        let mut discard_values = Some(discard_values);
        let result = self.execute_commit(&mut discard_values).await;

        if result.is_err() && !self.committed && !self.undetermined {
            if self.txn_file_chunks.is_empty() {
                // A failed 1PC prewrite is atomic and leaves no 2PC locks for
                // optimistic transactions. Pessimistic transactions may still
                // own their earlier pessimistic locks, which must be released
                // with PessimisticRollback. Once 1PC has fallen back, its flag
                // is false and ordinary BatchRollback cleanup applies.
                let cleanup_prewritten = if self.options.try_one_pc {
                    matches!(self.options.kind, TransactionKind::Pessimistic(_)).then_some(false)
                } else {
                    Some(true)
                };
                if let Some(cleanup_prewritten) = cleanup_prewritten {
                    let mut cleanup = self.clone();
                    if !cleanup_prewritten && self.options.try_one_pc {
                        // client-go rolls back every mutation after a failed
                        // pessimistic 1PC attempt because the response cannot
                        // identify which pre-existing locks were involved.
                        cleanup.pessimistic_lock_keys = cleanup
                            .mutations
                            .iter()
                            .map(|mutation| mutation.key.clone())
                            .collect();
                    }
                    cleanup.cleanup_without_wait(cleanup_prewritten);
                }
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

        let protocol = if self.options.try_one_pc {
            crate::metrics::TxnCommitProtocol::OnePc
        } else if self.options.async_commit {
            crate::metrics::TxnCommitProtocol::AsyncCommit
        } else {
            crate::metrics::TxnCommitProtocol::TwoPc
        };
        let metric_succeeded = match protocol {
            crate::metrics::TxnCommitProtocol::OnePc
            | crate::metrics::TxnCommitProtocol::AsyncCommit => result.is_ok(),
            crate::metrics::TxnCommitProtocol::TwoPc => self.committed || self.undetermined,
        };
        crate::metrics::record_txn_commit(protocol, metric_succeeded);

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

    async fn execute_commit<F>(
        &mut self,
        discard_values: &mut Option<F>,
    ) -> Result<Option<Timestamp>>
    where
        F: FnOnce() + Send,
    {
        debug!(
            "committing (2pc), start_ts: {}",
            self.start_version.version()
        );

        let killed = self
            .settings
            .variables
            .killed
            .load(atomic::Ordering::Acquire);
        if killed != 0 {
            return Err(crate::error::QueryInterruptedWithSignalError { signal: killed }.into());
        }
        if let Some(handler) = &self.settings.variables.kill_signal_handler {
            handler.handle_signal()?;
        }

        if self.settings.pipelined.enable {
            self.options.async_commit = false;
            self.options.try_one_pc = false;
            return self.execute_pipelined_commit().await;
        }
        if self.should_use_txn_file() {
            self.options.async_commit = false;
            self.options.try_one_pc = false;
            return self.execute_txn_file(discard_values).await;
        }
        let stashed_assertion_error: Option<Error> =
            if let Some(assertion) = self.stashed_assertion.clone() {
                self.options.async_commit = false;
                self.options.try_one_pc = false;
                Some(
                    self.check_schema_on_assertion_failure(
                        crate::error::AssertionFailedError {
                            assertion_failed: assertion,
                        }
                        .into(),
                    )
                    .await,
                )
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
        let min_commit_ts = match prewrite_result {
            Ok(min_commit_ts) => min_commit_ts,
            Err(error) => {
                return Err(self.check_schema_on_assertion_failure(error).await);
            }
        };

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
            let commit_timestamp =
                min_commit_ts.expect("async commit prewrite returned a minimum commit timestamp");
            self.validate_commit_timestamp(&commit_timestamp)?;
            commit_timestamp
        } else if self.mutations.is_empty() {
            let commit_timestamp = self.prepare_primary_commit_timestamp().await?;
            if let Some(discard_values) = discard_values.take() {
                discard_values();
            }
            commit_timestamp
        } else {
            match self
                .commit_primary_with_retry_and_value_discard(discard_values)
                .await
            {
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
        if let Some(pre) = &hooks.pre {
            pre();
        }
        tokio::spawn(async move {
            let skip_async_commit = crate::util::eval_failpoint("asyncCommitDoNothing", |_| ())
                .ok()
                .flatten()
                .is_some();
            if !skip_async_commit {
                if let Err(error) = secondary.commit_secondary(secondary_commit_ts).await {
                    log::warn!("Failed to commit secondary keys: {}", error);
                }
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
        let pending = self.mutations.to_vec();
        if !pending.is_empty() {
            let generation = self.pipelined_state.generation.saturating_add(1);
            self.flush_pipelined_generation(pending, generation, None)
                .await?;
        }
        if self.pipelined_state.range_start.is_none() || self.pipelined_state.range_end.is_none() {
            return Err(Error::StringError(
                "unexpected empty pipelinedStart or pipelinedEnd".to_owned(),
            ));
        }

        let commit_timestamp = self.prepare_txn_file_commit_timestamp().await?;
        let primary_key = self.primary_key.clone().ok_or(Error::NoPrimaryKey)?;
        let mut request = new_commit_request(
            std::iter::once(primary_key.clone()),
            self.start_version.clone(),
            commit_timestamp.clone(),
        );
        request.primary_key = primary_key.into();
        request.commit_role = kvrpcpb::CommitRole::Primary as i32;
        self.attach_resource_group_name(&mut request);
        self.settings
            .apply_request(&mut request, MAX_WRITE_EXECUTION_TIME);
        let source_retry_owner =
            self.source_retry_owner(COMMIT_MAX_BACKOFF.load(atomic::Ordering::Relaxed));
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
        .priority(self.options.priority);
        if let Some(owner) = source_retry_owner {
            plan.retry_multi_region_with_source_retry_owner(
                self.options.retry_options.region_backoff.clone(),
                owner,
            )
            .extract_error()
            .plan()
            .execute()
            .await?;
        } else {
            plan.retry_multi_region(self.options.retry_options.region_backoff.clone())
                .extract_error()
                .plan()
                .execute()
                .await?;
        }
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

    fn start_pipelined_heartbeat(
        &self,
        cancellation: crate::async_util::Cancellation,
        started: Arc<atomic::AtomicBool>,
        failed: Arc<atomic::AtomicBool>,
    ) {
        if started
            .compare_exchange(
                false,
                true,
                atomic::Ordering::AcqRel,
                atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let committer = self.clone();
        let interval = match self.options.heartbeat_option {
            HeartbeatOption::NoHeartbeat => DEFAULT_HEARTBEAT_INTERVAL,
            HeartbeatOption::Managed => Duration::from_millis(managed_lock_ttl() / 2),
            HeartbeatOption::FixedTime(interval) => interval,
        };
        let start_ts = self.start_version.version();
        std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    failed.store(true, atomic::Ordering::Release);
                    warn!(
                        "failed to start pipelined heartbeat runtime, start_ts: {start_ts}: {error}"
                    );
                    return;
                }
            };
            runtime.block_on(async move {
                let mut consecutive_failures = 0_u8;
                loop {
                    tokio::select! {
                        _ = cancellation.cancelled() => break,
                        _ = tokio::time::sleep(interval) => {}
                    }
                    match committer.send_pipelined_heartbeat().await {
                        Ok(false) => consecutive_failures = 0,
                        Ok(true) => {
                            failed.store(true, atomic::Ordering::Release);
                            break;
                        }
                        Err(error) => {
                            consecutive_failures = consecutive_failures.saturating_add(1);
                            // sendTxnHeartBeat marks every TiKV key error as a
                            // terminal heartbeat failure. Pipelined DML cannot
                            // keep flushing after any such primary rejection.
                            let terminal = heartbeat_error_stops_immediately(&error);
                            warn!(
                                "pipelined heartbeat failed, start_ts: {start_ts}, consecutive failures: {consecutive_failures}: {error}"
                            );
                            if terminal || consecutive_failures > 10 {
                                failed.store(true, atomic::Ordering::Release);
                                break;
                            }
                        }
                    }
                }
            });
        });
    }

    /// Returns `true` when the source maximum pipelined lifetime has elapsed
    /// and the TTL manager must be closed without sending another heartbeat.
    async fn send_pipelined_heartbeat(&self) -> Result<bool> {
        let primary = self.primary_key.clone().ok_or(Error::NoPrimaryKey)?;
        let now = self.rpc.clone().get_timestamp().await?;
        let uptime = crate::oracle::extract_physical(now.version()).saturating_sub(
            crate::oracle::extract_physical(self.start_version.version()),
        );
        let maximum_lifetime = crate::config::get_global_config()
            .max_txn_ttl
            .max(MAX_PIPELINED_TXN_TTL.load(atomic::Ordering::Relaxed));
        if uptime > maximum_lifetime as i64 {
            info!(
                "pipelined TTL manager reached its maximum lifetime, start_ts: {}, uptime_ms: {}",
                self.start_version.version(),
                uptime
            );
            return Ok(true);
        }
        self.pipelined_state
            .min_commit_ts
            .try_update(now.version(), WriteAccessLevel::Ttl);
        let mut request = new_heart_beat_request(
            self.start_version.clone(),
            primary,
            self.start_instant.elapsed().as_millis() as u64 + managed_lock_ttl(),
        );
        request.min_commit_ts = self.pipelined_state.min_commit_ts.get();
        self.settings
            .apply_heartbeat_request(&mut request, MAX_WRITE_EXECUTION_TIME);
        plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            self.rpc_interceptor.clone(),
            None,
            None,
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
        )
        .retry_multi_region(self.options.retry_options.region_backoff.clone())
        .extract_error()
        .merge(CollectSingle)
        .plan()
        .execute()
        .await?;
        if let Err(error) = self.broadcast_pipelined_status(0, false, false, true).await {
            warn!(
                "broadcast pipelined heartbeat status failed, start_ts: {}: {error}",
                self.start_version.version()
            );
        }
        Ok(false)
    }

    async fn flush_pipelined_generation(
        &mut self,
        mutations: Vec<kvrpcpb::Mutation>,
        generation: u64,
        on_primary_success: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Result<()> {
        let source_retry_owner = self.source_retry_owner(PIPELINED_FLUSH_MAX_BACKOFF);
        self.flush_pipelined_generation_with_retry_owner(
            mutations,
            generation,
            on_primary_success,
            source_retry_owner,
        )
        .await
    }

    async fn flush_pipelined_generation_with_retry_owner(
        &mut self,
        mutations: Vec<kvrpcpb::Mutation>,
        generation: u64,
        on_primary_success: Option<Arc<dyn Fn() + Send + Sync>>,
        source_retry_owner: Option<Arc<tokio::sync::Mutex<RetryBackoffer>>>,
    ) -> Result<()> {
        let primary = self.primary_key.clone().ok_or_else(|| {
            Error::StringError(
                "[pipelined dml] primary key should be set before pipelined flush".to_owned(),
            )
        })?;
        let minimum = self.start_version.version().saturating_add(1);
        let mut request = new_flush_request(
            mutations.clone(),
            primary.clone(),
            self.start_version.clone(),
            Timestamp::from_version(minimum),
            generation,
            managed_lock_ttl().max(DEFAULT_LOCK_TTL),
        );
        request.assertion_level = self.settings.assertion_level as i32;
        self.settings
            .apply_request_context(&mut request, MAX_WRITE_EXECUTION_TIME);
        request
            .context
            .get_or_insert_with(kvrpcpb::Context::default)
            .request_source = "external_pdml".to_owned();
        let started = Instant::now();
        let size = mutations.iter().fold(0_usize, |total, mutation| {
            total.saturating_add(mutation.key.len() + mutation.value.len())
        });
        let rpc_interceptor = if let Some(on_primary_success) = on_primary_success {
            let primary_key = <&[u8]>::from(&primary).to_vec();
            let primary_interceptor = crate::interceptor::new_rpc_interceptor(
                "pipelined-primary-flush-success",
                move |_, request, next| {
                    let is_primary = request
                        .as_any()
                        .downcast_ref::<kvrpcpb::FlushRequest>()
                        .is_some_and(|request| {
                            request
                                .mutations
                                .iter()
                                .any(|mutation| mutation.key == primary_key)
                        });
                    let on_primary_success = on_primary_success.clone();
                    Box::pin(async move {
                        let result = next().await;
                        if is_primary
                            && result.as_ref().ok().is_some_and(|response| {
                                response
                                    .downcast_ref::<kvrpcpb::FlushResponse>()
                                    .is_some_and(|response| {
                                        response.region_error.is_none()
                                            && response.errors.is_empty()
                                    })
                            })
                        {
                            on_primary_success();
                        }
                        result
                    })
                        as futures::future::BoxFuture<'_, crate::interceptor::RpcDispatchResult>
                },
            );
            let mut chain = RpcInterceptorChain::new();
            chain.link(primary_interceptor);
            if let Some(existing) = self.rpc_interceptor.clone() {
                chain.link(Arc::new(existing));
            }
            Some(chain)
        } else {
            self.rpc_interceptor.clone()
        };
        let has_static_resource_group_tag = self.settings.resource_group_tag.is_some();
        let resource_group_tagger = self.settings.resource_group_tagger.clone();
        let plan = plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            rpc_interceptor,
            self.resource_group_name.as_deref(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
        )
        .decorate_shard_request(
            |request| {
                request
                    .mutations
                    .iter()
                    .map(|mutation| mutation.key.clone())
                    .collect()
            },
            move |request| {
                apply_transaction_resource_group_tagger(
                    request,
                    has_static_resource_group_tag,
                    resource_group_tagger.as_ref(),
                );
            },
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
            false,
            matches!(self.options.kind, TransactionKind::Optimistic),
        );
        if let Some(owner) = source_retry_owner {
            plan.source_retry_owner(Arc::clone(&owner))
                .retry_multi_region_with_source_retry_owner_and_concurrency(
                    self.options.retry_options.region_backoff.clone(),
                    owner,
                    self.settings.pipelined.flush_concurrency.max(1),
                )
                .merge(CollectError)
                .extract_error()
                .plan()
                .execute()
                .await?;
        } else {
            plan.retry_multi_region_with_concurrency(
                self.options.retry_options.region_backoff.clone(),
                self.settings.pipelined.flush_concurrency.max(1),
            )
            .merge(CollectError)
            .extract_error()
            .plan()
            .execute()
            .await?;
        }
        crate::stats::observe_pipelined_flush(mutations.len(), size, started.elapsed());
        let logical_key = |key: Vec<u8>| {
            let key = Key::from(key).truncate_keyspace(self.keyspace);
            <&[u8]>::from(&key).to_vec()
        };
        let first_key = mutations
            .first()
            .map(|mutation| logical_key(mutation.key.clone()));
        let last_key = mutations
            .last()
            .map(|mutation| logical_key(mutation.key.clone()));
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
        next: Vec<u8>,
        exclusive_end: Vec<u8>,
        commit_version: u64,
    ) -> Result<()> {
        let max_sleep_ms = if commit_version == 0 {
            CLEANUP_MAX_BACKOFF
        } else {
            COMMIT_SECONDARY_MAX_BACKOFF
        };
        let mut retry_backoff = self.txn_file_retry_backoff(max_sleep_ms);
        self.resolve_pipelined_lock_range_with_backoff(
            next,
            exclusive_end,
            commit_version,
            &mut retry_backoff,
        )
        .await
    }

    async fn resolve_pipelined_lock_range_with_backoff(
        &self,
        mut next: Vec<u8>,
        exclusive_end: Vec<u8>,
        commit_version: u64,
        retry_backoff: &mut TxnFileRetryBackoff,
    ) -> Result<()> {
        while next < exclusive_end {
            let store = self
                .rpc
                .clone()
                .store_for_key(&Key::from(next.clone()))
                .await?;
            let mut request =
                new_resolve_lock_request(self.start_version.version(), commit_version, false);
            self.settings.apply_pipelined_resolve_request(&mut request);
            if retry_backoff.is_retry_request(false).await {
                request.set_is_retry_request();
            }
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
            .await;
            let response = match response {
                Ok(response) => response,
                Err(error) if is_transaction_transport_error(&error) => {
                    self.rpc
                        .invalidate_region_cache(store.region_with_leader.ver_id())
                        .await;
                    retry_backoff
                        .backoff_region_miss(format!(
                            "send pipelined resolve lock request error: {error}"
                        ))
                        .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if let Some(region_error) = response.region_error {
                self.rpc
                    .invalidate_region_cache(store.region_with_leader.ver_id())
                    .await;
                retry_backoff
                    .backoff_region_miss(format!(
                        "send pipelined resolve lock get region error: {region_error:?}"
                    ))
                    .await?;
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
        rolled_back: bool,
        include_min_commit_ts: bool,
    ) -> Result<()> {
        let status = kvrpcpb::TxnStatus {
            start_ts: self.start_version.version(),
            min_commit_ts: include_min_commit_ts
                .then(|| self.pipelined_state.min_commit_ts.get())
                .unwrap_or_default(),
            commit_ts: commit_version,
            rolled_back,
            is_completed,
        };
        let mut request = kvrpcpb::BroadcastTxnStatusRequest {
            txn_status: vec![status],
            ..Default::default()
        };
        let cluster_id = self.rpc.cluster_id().await;
        self.settings
            .apply_broadcast_request(&mut request, cluster_id);
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
        let rolled_back = commit_version == 0;
        if let Err(error) = self
            .broadcast_pipelined_status(commit_version, false, rolled_back, true)
            .await
        {
            warn!("broadcast pipelined transaction status failed: {error}");
        }
        self.resolve_pipelined_locks(commit_version).await?;
        tokio::time::sleep(pipelined_broadcast_grace_period()).await;
        if let Err(error) = self
            .broadcast_pipelined_status(commit_version, true, rolled_back, false)
            .await
        {
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
        if self.buffer_size < minimum_size as u64
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

    async fn execute_txn_file<F>(
        &mut self,
        discard_values: &mut Option<F>,
    ) -> Result<Option<Timestamp>>
    where
        F: FnOnce() + Send,
    {
        let resource_accounting = self.before_execute_txn_file_resource_control().await?;
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

        let commit_timestamp = self.prepare_txn_file_commit_timestamp().await?;
        self.txn_file_commit_timestamp = Some(commit_timestamp.clone());
        if let Some(discard_values) = discard_values.take() {
            discard_values();
        }
        let commit_result = self
            .execute_txn_file_action(&chunks, TxnFileAction::Commit)
            .await;
        self.normalize_txn_file_commit_result(commit_result)?;
        self.committed = true;
        self.after_execute_txn_file_resource_control(resource_accounting);
        Ok(Some(commit_timestamp))
    }

    async fn before_execute_txn_file_resource_control(
        &self,
    ) -> Result<Option<TxnFileResourceAccounting>> {
        let Some(controller) = self
            .resource_control
            .clone()
            .or_else(crate::resource_control::global_controller)
        else {
            return Ok(None);
        };
        let Some(resource_group_name) = self.resource_group_name.clone() else {
            return Ok(None);
        };
        let primary = self.primary_key.as_ref().ok_or(Error::NoPrimaryKey)?;
        let store = self.rpc.clone().store_for_key(primary).await?;
        let leader_store_id = store
            .region_with_leader
            .leader
            .as_ref()
            .map_or(0, |leader| leader.store_id);
        let replica_number = store
            .region_with_leader
            .region
            .peers
            .iter()
            .filter(|peer| {
                crate::proto::metapb::PeerRole::try_from(peer.role)
                    == Ok(crate::proto::metapb::PeerRole::Voter)
            })
            .count() as i64;
        let ratio = crate::config::get_global_config()
            .tikv_client
            .txn_file_ru_discount_ratio;
        let mut write_bytes = self.buffer_size;
        if ratio > 0.0 && ratio < 1.0 {
            write_bytes = (write_bytes as f64 * ratio) as u64;
        }
        let request = ResourceControlRequestInfo::new(
            Some(write_bytes),
            leader_store_id,
            replica_number,
            false,
        );
        let result = controller
            .on_request_wait(&resource_group_name, request)
            .await?;
        if let Some(ru_details) = &self.ru_details {
            ru_details.update(&result.consumption, result.wait_duration);
        }
        Ok(Some((controller, resource_group_name, request)))
    }

    fn after_execute_txn_file_resource_control(
        &self,
        accounting: Option<TxnFileResourceAccounting>,
    ) {
        let Some((controller, resource_group_name, request)) = accounting else {
            return;
        };
        match controller.on_response_wait(
            &resource_group_name,
            request,
            ResourceControlResponseInfo::default(),
        ) {
            Ok(result) => {
                if let Some(ru_details) = &self.ru_details {
                    // client-go's txn-file settlement adds no wait time.
                    ru_details.update(&result.consumption, Duration::ZERO);
                }
            }
            Err(error) => {
                crate::stats::increment_txn_file_error("accounting");
                warn!(
                    "txn file: resource control accounting failed after commit, start_ts: {}, error: {}",
                    self.start_version.version(),
                    error
                );
            }
        }
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
                self.attach_resource_group_name(&mut request);
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
                    let mut lock_resolver_context = self.lock_resolver_context.clone();
                    lock_resolver_context.pessimistic_region_resolve = true;
                    let resolution = crate::transaction::resolve_locks_with_context_result(
                        locks,
                        Timestamp::from_version(u64::MAX),
                        self.rpc.clone(),
                        self.keyspace,
                        self.keyspace_name.as_deref(),
                        lock_resolver_context,
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
        let max_sleep_ms = match action {
            TxnFileAction::Prewrite => PREWRITE_MAX_BACKOFF.load(atomic::Ordering::Relaxed),
            TxnFileAction::Commit | TxnFileAction::Rollback => {
                COMMIT_MAX_BACKOFF.load(atomic::Ordering::Relaxed)
            }
        };
        let mut retry_backoff = self.txn_file_retry_backoff(max_sleep_ms);
        let mut is_retry_request = false;
        loop {
            let mut batches = chunks.group_to_batches(&self.rpc, &self.mutations).await?;
            let primary_index = self.txn_file_primary_batch_index(&batches)?;
            let mut primary_batch = batches.remove(primary_index);
            primary_batch.is_primary = true;
            if self
                .execute_txn_file_batch(
                    &primary_batch,
                    action,
                    &mut retry_backoff,
                    is_retry_request,
                )
                .await?
            {
                self.rpc
                    .invalidate_region_cache(primary_batch.region.ver_id())
                    .await;
                retry_backoff
                    .backoff_region_miss("txn file: execute primary batch failed")
                    .await?;
                is_retry_request = true;
                continue;
            }
            if action == TxnFileAction::Commit {
                self.committed = true;
            } else if action == TxnFileAction::Prewrite {
                if let Some(start_heartbeat) = &self.auto_heartbeat_starter {
                    start_heartbeat(self.min_commit_ts.clone(), true);
                }
            }
            if batches.is_empty() {
                return Ok(());
            }

            if action == TxnFileAction::Prewrite {
                return self
                    .execute_txn_file_slice_with_retry(
                        chunks.clone(),
                        Some(batches),
                        action,
                        &mut retry_backoff,
                        is_retry_request,
                    )
                    .await;
            }

            let mut secondary = self.clone();
            let secondary_chunks = chunks.clone();
            let hooks = self.settings.lifecycle_hooks.clone();
            tokio::spawn(async move {
                let mut retry_backoff =
                    secondary.txn_file_retry_backoff(COMMIT_SECONDARY_MAX_BACKOFF);
                if let Some(pre) = hooks.pre {
                    pre();
                }
                if let Err(error) = secondary
                    .execute_txn_file_slice_with_retry(
                        secondary_chunks,
                        Some(batches),
                        action,
                        &mut retry_backoff,
                        is_retry_request,
                    )
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
        retry_backoff: &mut TxnFileRetryBackoff,
        mut is_retry_request: bool,
    ) -> Result<()> {
        loop {
            let mut current_batches = match batches.take() {
                Some(batches) => batches,
                None => chunks.group_to_batches(&self.rpc, &self.mutations).await?,
            };
            if current_batches.is_empty() {
                return Ok(());
            }
            let mut region_error_chunks = TxnChunkSlice::default();
            let mut first_error = None;
            if current_batches.len() == 1 {
                let batch = current_batches.pop().unwrap();
                let result = self
                    .execute_txn_file_batch(&batch, action, retry_backoff, is_retry_request)
                    .await;
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
                        first_error = Some(error);
                    }
                }
            } else {
                // client-go forks once for the concurrent slice, then gives
                // each batch a detached Clone. Child sleeps intentionally do
                // not update the parent action budget.
                let forked = retry_backoff.fork().await;
                let mut work = Vec::with_capacity(current_batches.len());
                for batch in current_batches {
                    work.push((batch, forked.detached_clone().await));
                }
                let config = crate::config::get_global_config();
                let mut concurrency = work.len().min(config.committer_concurrency.max(1));
                let max_chunks_in_parallel =
                    txn_file_max_chunks_in_parallel(config.tikv_client.txn_chunk_max_size);
                // Source applies the chunk-size limiter only when this slice
                // itself contains more chunks than the computed threshold.
                if chunks.len() > max_chunks_in_parallel {
                    concurrency = max_chunks_in_parallel;
                }
                let template = self.clone();
                let mut results = stream::iter(work.into_iter().map(
                    move |(batch, mut batch_retry_backoff)| {
                        let mut committer = template.clone();
                        async move {
                            let result = committer
                                .execute_txn_file_batch(
                                    &batch,
                                    action,
                                    &mut batch_retry_backoff,
                                    is_retry_request,
                                )
                                .await;
                            (batch, result)
                        }
                    },
                ))
                .buffer_unordered(concurrency);
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
            }
            if let Some(error) = first_error {
                return Err(error);
            }
            region_error_chunks.sort_and_dedup();
            if region_error_chunks.is_empty() {
                return Ok(());
            }
            retry_backoff
                .backoff_region_miss("txn file: execute failed, region miss")
                .await?;
            chunks = region_error_chunks;
            is_retry_request = true;
        }
    }

    async fn execute_txn_file_batch(
        &mut self,
        batch: &ChunkBatch,
        action: TxnFileAction,
        retry_backoff: &mut TxnFileRetryBackoff,
        is_retry_request: bool,
    ) -> Result<bool> {
        match action {
            TxnFileAction::Prewrite => {
                self.prewrite_txn_file_batch_with_backoff(batch, retry_backoff, is_retry_request)
                    .await
            }
            TxnFileAction::Commit => {
                self.commit_txn_file_batch_with_backoff(batch, retry_backoff, is_retry_request)
                    .await
            }
            TxnFileAction::Rollback => {
                self.rollback_txn_file_batch_with_backoff(batch, retry_backoff, is_retry_request)
                    .await
            }
        }
    }

    #[cfg(test)]
    async fn prewrite_txn_file_batch(&mut self, batch: &ChunkBatch) -> Result<bool> {
        self.prewrite_txn_file_batch_with_retry(batch, false).await
    }

    async fn prewrite_txn_file_batch_with_retry(
        &mut self,
        batch: &ChunkBatch,
        is_retry_request: bool,
    ) -> Result<bool> {
        let mut retry_backoff =
            self.txn_file_retry_backoff(PREWRITE_MAX_BACKOFF.load(atomic::Ordering::Relaxed));
        self.prewrite_txn_file_batch_with_backoff(batch, &mut retry_backoff, is_retry_request)
            .await
    }

    async fn prewrite_txn_file_batch_with_backoff(
        &mut self,
        batch: &ChunkBatch,
        retry_backoff: &mut TxnFileRetryBackoff,
        is_retry_request: bool,
    ) -> Result<bool> {
        let mut request = new_prewrite_request(
            Vec::new(),
            self.primary_key.clone().ok_or(Error::NoPrimaryKey)?,
            self.start_version.clone(),
            self.calc_txn_lock_ttl() + self.start_instant.elapsed().as_millis() as u64,
        );
        request.assertion_level = kvrpcpb::AssertionLevel::Off as i32;
        request.txn_file_chunks.clone_from(&batch.chunks.chunk_ids);
        request.txn_size = batch.transaction_size();
        self.attach_resource_group_name(&mut request);
        self.settings.apply_txn_file_prewrite(
            &mut request,
            &batch.first_key,
            SNAPSHOT_READ_TIMEOUT_MEDIUM,
        );
        loop {
            if batch.is_primary {
                request.lock_ttl =
                    self.calc_txn_lock_ttl() + self.start_instant.elapsed().as_millis() as u64;
            }
            if retry_backoff.is_retry_request(is_retry_request).await {
                request.set_is_retry_request();
            }
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
                request.clone(),
            )
            .priority(self.options.priority)
            .single_region_with_store(store)
            .await?
            .plan()
            .execute()
            .await;
            let response = match response {
                Ok(response) => response,
                Err(error) if is_txn_file_retryable_transport_error(&error) => {
                    retry_backoff
                        .backoff_rpc(format!("txn file prewrite request failed: {error}"))
                        .await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if let Some(region_error) = response.region_error {
                if region_error.disk_full.is_some() {
                    return Err(Error::RegionError(Box::new(region_error)));
                }
                let real_epoch_not_match = region_error.epoch_not_match.is_some()
                    && !crate::retry::is_fake_region_error(Some(&region_error));
                retry_backoff.backoff_region_error(&region_error).await?;
                if real_epoch_not_match {
                    return Ok(true);
                }
                let relocated = self
                    .rpc
                    .clone()
                    .store_for_key(&Key::from(batch.first_key.clone()))
                    .await?;
                if relocated.region_with_leader.ver_id() == batch.region.ver_id() {
                    continue;
                }
                return Ok(true);
            }
            if response.errors.is_empty() {
                return Ok(false);
            }
            let mut locks = Vec::new();
            for key_error in response.errors {
                if key_error.already_exist.is_some() {
                    let error: Error = key_error.into();
                    return Err(self.validate_key_exists_error(error));
                }
                let extracted = crate::transaction::extract_locks_from_key_error(&key_error);
                let Ok(mut extracted) = extracted else {
                    let error: Error = key_error.into();
                    return Err(self.validate_key_exists_error(error));
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
            let lock_count = locks.len();
            let mut lock_resolver_context = self.lock_resolver_context.clone();
            lock_resolver_context.pessimistic_region_resolve = true;
            let resolution = crate::transaction::resolve_locks_with_context_result(
                locks,
                self.start_version.clone(),
                self.rpc.clone(),
                self.keyspace,
                self.keyspace_name.as_deref(),
                lock_resolver_context,
            )
            .await?;
            if resolution.ms_before_expired > 0 {
                retry_backoff
                    .backoff_lock(
                        resolution.ms_before_expired as u64,
                        format!("2PC txn file prewrite lockedKeys: {lock_count}"),
                    )
                    .await?;
            }
        }
    }

    #[cfg(test)]
    async fn commit_txn_file_batch(&mut self, batch: &ChunkBatch) -> Result<bool> {
        self.commit_txn_file_batch_with_retry(batch, false).await
    }

    async fn commit_txn_file_batch_with_retry(
        &mut self,
        batch: &ChunkBatch,
        is_retry_request: bool,
    ) -> Result<bool> {
        let mut retry_backoff =
            self.txn_file_retry_backoff(COMMIT_MAX_BACKOFF.load(atomic::Ordering::Relaxed));
        self.commit_txn_file_batch_with_backoff(batch, &mut retry_backoff, is_retry_request)
            .await
    }

    async fn commit_txn_file_batch_with_backoff(
        &mut self,
        batch: &ChunkBatch,
        retry_backoff: &mut TxnFileRetryBackoff,
        is_retry_request: bool,
    ) -> Result<bool> {
        let commit_timestamp = self
            .txn_file_commit_timestamp
            .clone()
            .ok_or_else(|| Error::StringError("txn file: commit TS is not prepared".to_owned()))?;
        let keys = batch.sample_data_keys.iter().cloned().map(Key::from);
        let mut request = new_commit_request(keys, self.start_version.clone(), commit_timestamp);
        request.is_txn_file = true;
        self.attach_resource_group_name(&mut request);
        self.settings
            .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_MEDIUM);
        loop {
            if retry_backoff.is_retry_request(is_retry_request).await {
                request.set_is_retry_request();
            }
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
                request.clone(),
            )
            .priority(self.options.priority)
            .single_region_with_store(store)
            .await?
            .plan()
            .execute()
            .await;
            let response = match result {
                Ok(response) => response,
                Err(error) if is_transaction_transport_error(&error) => {
                    if batch.is_primary {
                        // Keep prior ambiguity until TiKV returns a definitive
                        // primary response. A later local/non-transport error
                        // cannot prove that an earlier request was not applied.
                        self.undetermined = true;
                    }
                    if !is_txn_file_retryable_transport_error(&error) {
                        return Err(error);
                    }
                    retry_backoff
                        .backoff_rpc(format!("txn file commit request failed: {error}"))
                        .await?;
                    continue;
                }
                Err(error) => return Err(error),
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
                info!(
                    "2PC commitTS rejected by TiKV, retry with a newer commitTS, txnStartTS: {}, info: {}",
                    self.start_version.version(),
                    crate::logutil::hex(expired)
                );
                let primary = self.primary_key.as_ref().ok_or(Error::NoPrimaryKey)?;
                if !batch.is_primary || expired.key.as_slice() != <&[u8]>::from(primary) {
                    return Err(Error::StringError(
                        "2PC commitTS rejected by TiKV, but the key is not the primary key"
                            .to_owned(),
                    ));
                }
                if commit_ts_expired_gap_is_too_large(expired) {
                    return Err(Error::StringError(format!(
                        "2PC min_commit_ts is too large, we got min_commit_ts: {}, and attempted_commit_ts: {}",
                        expired.min_commit_ts, expired.attempted_commit_ts
                    )));
                }
                let commit_timestamp = self.prepare_txn_file_commit_timestamp().await?;
                request.commit_version = commit_timestamp.version();
                self.txn_file_commit_timestamp = Some(commit_timestamp);
                continue;
            }
            return Err(key_error.into());
        }
    }

    #[cfg(test)]
    async fn rollback_txn_file_batch(&mut self, batch: &ChunkBatch) -> Result<bool> {
        self.rollback_txn_file_batch_with_retry(batch, false).await
    }

    async fn rollback_txn_file_batch_with_retry(
        &mut self,
        batch: &ChunkBatch,
        is_retry_request: bool,
    ) -> Result<bool> {
        let mut retry_backoff =
            self.txn_file_retry_backoff(COMMIT_MAX_BACKOFF.load(atomic::Ordering::Relaxed));
        self.rollback_txn_file_batch_with_backoff(batch, &mut retry_backoff, is_retry_request)
            .await
    }

    async fn rollback_txn_file_batch_with_backoff(
        &mut self,
        batch: &ChunkBatch,
        retry_backoff: &mut TxnFileRetryBackoff,
        is_retry_request: bool,
    ) -> Result<bool> {
        let keys = batch.sample_data_keys.iter().cloned().map(Key::from);
        let mut request = new_batch_rollback_request(keys, self.start_version.clone());
        request.is_txn_file = true;
        self.attach_resource_group_name(&mut request);
        self.settings
            .apply_request(&mut request, SNAPSHOT_READ_TIMEOUT_SHORT);
        if retry_backoff.is_retry_request(is_retry_request).await {
            request.set_is_retry_request();
        }
        let store = self
            .rpc
            .clone()
            .store_for_key(&Key::from(batch.first_key.clone()))
            .await?;
        let response = loop {
            match plan_with_keyspace_name(
                self.rpc.clone(),
                self.keyspace,
                self.keyspace_name.as_deref(),
                self.rpc_interceptor.clone(),
                self.resource_group_name.as_deref(),
                self.resource_control.clone(),
                self.ru_details.clone(),
                ReplicaReadConfig::default(),
                request.clone(),
            )
            .priority(self.options.priority)
            .single_region_with_store(store.clone())
            .await?
            .plan()
            .execute()
            .await
            {
                Ok(response) => break response,
                Err(error) if is_txn_file_retryable_transport_error(&error) => {
                    retry_backoff
                        .backoff_rpc(format!("txn file rollback request failed: {error}"))
                        .await?;
                    request.set_is_retry_request();
                }
                Err(error) => return Err(error),
            }
        };
        if response.region_error.is_some() {
            return Ok(true);
        }
        if let Some(error) = response.error {
            return Err(Error::StringError(format!(
                "session {} txn file cleanup failed: {:?}",
                self.settings.session_id, error
            )));
        }
        Ok(false)
    }

    fn check_async_commit(&self) -> bool {
        let global_scope = self.settings.scope == crate::oracle::GLOBAL_TXN_SCOPE;
        let has_commit_upper_bound = self.settings.commit_timestamp_upper_bound.is_some();
        let has_binlog = self.settings.binlog.is_some();
        let has_shared_locks = self.mutations.iter().any(|mutation| {
            matches!(
                kvrpcpb::Op::try_from(mutation.op),
                Ok(kvrpcpb::Op::SharedLock | kvrpcpb::Op::SharedPessimisticLock)
            )
        });
        if !self.options.async_commit
            || !global_scope
            || has_commit_upper_bound
            || has_binlog
            || self.settings.pipelined.enable
            || has_shared_locks
        {
            return false;
        }

        let config = crate::config::get_global_config();
        let async_commit = config.tikv_client.async_commit;
        let key_bytes = self
            .mutations
            .iter()
            .try_fold(0_u64, |total, mutation| {
                total.checked_add(mutation.key.len() as u64)
            })
            .unwrap_or(u64::MAX);
        self.mutations.len() as u64 <= async_commit.keys_limit
            && key_bytes <= async_commit.total_key_size_limit
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

        if self.options.async_commit && !self.check_async_commit() {
            self.options.async_commit = false;
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
        let started = Instant::now();
        let mut attempts = 1_u64;
        let timeout = self.settings.commit_wait_until_tso_timeout;
        let result = async {
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
        .await;

        let wait_time = started.elapsed();
        crate::stats::observe_commit_ts_lag(wait_time, attempts, result.is_ok());
        if let Some(details) = &self.commit_details {
            details.lock().unwrap().lag_details = crate::util::CommitTsLagDetails {
                wait_time,
                backoff_count: attempts.saturating_sub(1).try_into().unwrap_or(i32::MAX),
                first_lag_ts: first.version(),
                wait_until_ts: expected,
            };
        }
        result
    }

    fn validate_commit_timestamp(&self, commit_timestamp: &Timestamp) -> Result<()> {
        if self.start_instant.elapsed() > Duration::from_millis(MAX_TXN_TIME_USE) {
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

    async fn check_schema_on_assertion_failure(&self, error: Error) -> Error {
        if !matches!(error, Error::AssertionFailed(_)) {
            return error;
        }
        let timestamp = match self.get_timestamp_for_commit().await {
            Ok(timestamp) => timestamp,
            Err(timestamp_error) => return timestamp_error,
        };
        match self.check_schema_valid(timestamp.version()) {
            Ok(()) => error,
            Err(schema_error) => schema_error,
        }
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
        pre_split_large_mutation_regions(
            self.rpc.clone(),
            &self.mutations,
            "2PC large transaction",
        )
        .await;
    }

    async fn prewrite(&mut self) -> Result<Option<Timestamp>> {
        let source_retry_owner =
            self.source_retry_owner(PREWRITE_MAX_BACKOFF.load(atomic::Ordering::Relaxed));
        self.prewrite_with_retry_owner(source_retry_owner).await
    }

    async fn prewrite_with_retry_owner(
        &mut self,
        source_retry_owner: Option<Arc<tokio::sync::Mutex<RetryBackoffer>>>,
    ) -> Result<Option<Timestamp>> {
        let undetermined_retry_owner = source_retry_owner.clone();
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
        let skip_assertion_check_from_prewrite =
            crate::util::eval_failpoint("assertionSkipCheckFromPrewrite", |_| ())
                .ok()
                .flatten()
                .is_some();
        request.assertion_level = if skip_assertion_check_from_prewrite {
            kvrpcpb::AssertionLevel::Off as i32
        } else {
            self.settings.assertion_level as i32
        };
        request.min_commit_ts = prewrite_min_commit_ts(
            self.start_version.version(),
            pessimistic_for_update_ts.version(),
            self.min_commit_ts.get(),
        );
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
        request.for_update_ts_constraints = self
            .mutations
            .iter()
            .enumerate()
            .filter_map(|(index, mutation)| {
                self.for_update_ts_constraints
                    .get(&mutation.key)
                    .copied()
                    .map(|expected_for_update_ts| {
                        kvrpcpb::prewrite_request::ForUpdateTsConstraint {
                            index: index as u32,
                            expected_for_update_ts,
                        }
                    })
            })
            .collect();
        self.settings
            .apply_request_context(&mut request, MAX_WRITE_EXECUTION_TIME);
        request.secondaries = if self.options.async_commit {
            self.mutations
                .iter()
                .filter(|mutation| {
                    self.primary_key.as_ref().unwrap() != mutation.key.as_ref()
                        && kvrpcpb::Op::try_from(mutation.op) != Ok(kvrpcpb::Op::CheckNotExists)
                })
                .map(|mutation| mutation.key.clone())
                .collect()
        } else {
            Vec::new()
        };

        // A transport failure for any async-commit/1PC prewrite shard makes
        // the transaction result ambiguous unless that same shard later
        // receives a definitive successful response. Track physical attempts
        // by key so region re-sharding can clear only the recovered subset.
        let ambiguous_prewrite_keys = Arc::new(Mutex::new(BTreeSet::<Vec<u8>>::new()));
        let tracked_keys = Arc::clone(&ambiguous_prewrite_keys);
        let ambiguity_interceptor =
            crate::interceptor::new_rpc_interceptor(
                "tikv-client-transaction-prewrite-ambiguity",
                move |_, request, next| {
                    let keys = request
                        .as_any()
                        .downcast_ref::<kvrpcpb::PrewriteRequest>()
                        .filter(|request| request.use_async_commit || request.try_one_pc)
                        .map(|request| {
                            request
                                .mutations
                                .iter()
                                .map(|mutation| mutation.key.clone())
                                .collect::<Vec<_>>()
                        });
                    let tracked_keys = Arc::clone(&tracked_keys);
                    Box::pin(async move {
                        let result = next().await;
                        if let Some(keys) = keys {
                            let mut ambiguous = tracked_keys.lock().unwrap();
                            match &result {
                                Err(error) if is_transaction_transport_error(error) => {
                                    ambiguous.extend(keys);
                                }
                                Ok(response) => {
                                    if let Some(response) =
                                        response.downcast_ref::<kvrpcpb::PrewriteResponse>()
                                    {
                                        if response.region_error.as_ref().is_some_and(|error| {
                                            error.undetermined_result.is_some()
                                        }) {
                                            ambiguous.extend(keys);
                                        } else if response.region_error.is_none()
                                            && response.errors.is_empty()
                                        {
                                            for key in keys {
                                                ambiguous.remove(&key);
                                            }
                                        }
                                    }
                                }
                                Err(_) => {}
                            }
                        }
                        result
                    })
                        as futures::future::BoxFuture<'_, crate::interceptor::RpcDispatchResult>
                },
            );
        let mut rpc_interceptor = RpcInterceptorChain::new();
        if self.write_size
            > crate::config::get_global_config()
                .tikv_client
                .ttl_refreshed_txn_size
                .max(0) as u64
        {
            if let Some(start_heartbeat) = self.auto_heartbeat_starter.clone() {
                let primary_key = <&[u8]>::from(&self.primary_key.clone().unwrap()).to_vec();
                let min_commit_ts = self.min_commit_ts.clone();
                rpc_interceptor.link(crate::interceptor::new_rpc_interceptor(
                    "tikv-client-large-transaction-primary-prewrite-heartbeat",
                    move |_, request, next| {
                        let contains_primary = request
                            .as_any()
                            .downcast_ref::<kvrpcpb::PrewriteRequest>()
                            .is_some_and(|request| {
                                request
                                    .mutations
                                    .iter()
                                    .any(|mutation| mutation.key == primary_key)
                            });
                        let start_heartbeat = start_heartbeat.clone();
                        let min_commit_ts = min_commit_ts.clone();
                        Box::pin(async move {
                            let result = next().await;
                            if contains_primary {
                                if let Ok(response) = &result {
                                    if response
                                        .downcast_ref::<kvrpcpb::PrewriteResponse>()
                                        .is_some_and(|response| {
                                            response.region_error.is_none()
                                                && response.errors.is_empty()
                                                && response.one_pc_commit_ts == 0
                                        })
                                    {
                                        start_heartbeat(min_commit_ts, false);
                                    }
                                }
                            }
                            result
                        })
                            as futures::future::BoxFuture<'_, crate::interceptor::RpcDispatchResult>
                    },
                ));
            }
        }
        rpc_interceptor.link(ambiguity_interceptor);
        if let Some(existing) = self.rpc_interceptor.clone() {
            rpc_interceptor.link(Arc::new(existing));
        }
        let has_static_resource_group_tag = self.settings.resource_group_tag.is_some();
        let resource_group_tagger = self.settings.resource_group_tagger.clone();
        let plan = plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            Some(rpc_interceptor),
            self.resource_group_name.as_deref(),
            self.resource_control.clone(),
            self.ru_details.clone(),
            ReplicaReadConfig::default(),
            request,
        )
        .decorate_shard_request(
            |request| {
                request
                    .mutations
                    .iter()
                    .map(|mutation| mutation.key.clone())
                    .collect()
            },
            move |request| {
                apply_transaction_resource_group_tagger(
                    request,
                    has_static_resource_group_tag,
                    resource_group_tagger.as_ref(),
                );
            },
        )
        .priority(self.options.priority)
        .resolve_lock_with_context_and_pessimistic_region(
            self.start_version.clone(),
            self.options.retry_options.lock_backoff.clone(),
            self.keyspace,
            self.lock_resolver_context.clone(),
        )
        .prewrite_lock_conflict(
            self.start_version.version(),
            self.settings.prewrite_lock_policy == PrewriteEncounterLockPolicy::NoResolve,
            matches!(self.options.kind, TransactionKind::Optimistic),
        );
        let response_result = if let Some(owner) = source_retry_owner {
            plan.source_retry_owner(Arc::clone(&owner))
                .retry_multi_region_with_source_retry_owner(
                    self.options.retry_options.region_backoff.clone(),
                    owner,
                )
                .merge(CollectError)
                .extract_error()
                .plan()
                .execute()
                .await
        } else {
            plan.retry_multi_region(self.options.retry_options.region_backoff.clone())
                .merge(CollectError)
                .extract_error()
                .plan()
                .execute()
                .await
        };
        let response = match response_result {
            Ok(response) => response,
            Err(error) => {
                let ambiguous = !ambiguous_prewrite_keys.lock().unwrap().is_empty()
                    || has_undetermined_region_error(&error);
                if !self.options.async_commit
                    && !self.options.try_one_pc
                    && has_undetermined_region_error(&error)
                {
                    if let Some(owner) = undetermined_retry_owner {
                        owner
                            .lock()
                            .await
                            .backoff(
                                BO_REGION_MISS,
                                format!("standard 2PC prewrite result undetermined: {error}"),
                            )
                            .await
                            .map_err(|error| Error::StringError(error.to_string()))?;
                        return Box::pin(self.prewrite_with_retry_owner(Some(owner))).await;
                    }
                }
                let error = normalize_prewrite_error(self.validate_key_exists_error(error));
                if (self.options.async_commit || self.options.try_one_pc) && ambiguous {
                    self.undetermined = true;
                    return Err(Error::UndeterminedError(Box::new(error)));
                }
                return Err(error);
            }
        };

        let attempted_one_pc = self.options.try_one_pc && response.len() == 1;
        if attempted_one_pc && response[0].one_pc_commit_ts != 0 {
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
        if attempted_one_pc {
            if response[0].min_commit_ts != 0 {
                return Err(Error::StringError(
                    "MinCommitTs must be 0 when 1pc falls back to 2pc".to_owned(),
                ));
            }
            // A real one-batch 1PC fallback disables the async bit as well.
            // Multi-batch transactions never send try_one_pc and may still
            // proceed with async commit, matching checkOnePCFallBack.
            self.options.async_commit = false;
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

    async fn prepare_primary_commit_timestamp(&self) -> Result<Timestamp> {
        let commit_version = self.get_timestamp_for_commit().await?;
        // Standard 2PC validates schema before transaction lifetime and the
        // optional commit-TS upper bound. This ordering is observable when
        // more than one check would reject the same timestamp.
        self.check_schema_valid(commit_version.version())?;
        self.validate_commit_timestamp(&commit_version)?;
        Ok(commit_version)
    }

    async fn prepare_txn_file_commit_timestamp(&self) -> Result<Timestamp> {
        let commit_version = self.get_timestamp_for_commit().await?;
        // Txn-file source order is observable: schema rejection prevents both
        // lifetime and upper-bound checks from running.
        self.check_schema_valid(commit_version.version())?;
        self.validate_commit_timestamp(&commit_version)?;
        Ok(commit_version)
    }

    /// Commits the primary key with a prepared commit version.
    async fn commit_primary_at_version<F>(
        &mut self,
        commit_version: Timestamp,
        discard_values: &mut Option<F>,
        source_retry_owner: Option<Arc<tokio::sync::Mutex<RetryBackoffer>>>,
        retained_request: &mut Option<kvrpcpb::CommitRequest>,
    ) -> Result<Timestamp>
    where
        F: FnOnce() + Send,
    {
        debug!(
            "committing primary, start_ts: {}, commit_ts: {}",
            self.start_version.version(),
            commit_version.version(),
        );
        let primary_key = self.primary_key.clone().ok_or(Error::NoPrimaryKey)?;
        if let Some(discard_values) = discard_values.take() {
            discard_values();
        }
        let req = match retained_request.take() {
            Some(mut request) => {
                // CommitTsExpired retries mutate only CommitVersion on the
                // request that was already decorated for this physical batch.
                // In particular, a dynamic tagger is not called again and any
                // other request edits it made remain intact.
                request.commit_version = commit_version.version();
                request
            }
            None => {
                let mut request = new_commit_request(
                    std::iter::once(primary_key.clone()),
                    self.start_version.clone(),
                    commit_version.clone(),
                );
                request.primary_key = primary_key.into();
                request.commit_role = kvrpcpb::CommitRole::Primary as i32;
                self.attach_resource_group_name(&mut request);
                self.settings
                    .apply_request(&mut request, MAX_WRITE_EXECUTION_TIME);
                request
            }
        };
        *retained_request = Some(req.clone());
        let primary_undetermined = Arc::new(atomic::AtomicBool::new(self.undetermined));
        let tracked_undetermined = Arc::clone(&primary_undetermined);
        let ambiguity_interceptor = crate::interceptor::new_rpc_interceptor(
            "tikv-client-transaction-primary-ambiguity",
            move |_, request, next| {
                let tracks_primary = request
                    .as_any()
                    .downcast_ref::<kvrpcpb::CommitRequest>()
                    .is_some();
                let tracked_undetermined = Arc::clone(&tracked_undetermined);
                Box::pin(async move {
                    let result = next().await;
                    if tracks_primary {
                        match &result {
                            Err(error) if is_transaction_transport_error(error) => {
                                tracked_undetermined.store(true, atomic::Ordering::SeqCst);
                            }
                            Ok(response) => {
                                if let Some(response) =
                                    response.downcast_ref::<kvrpcpb::CommitResponse>()
                                {
                                    if response
                                        .region_error
                                        .as_ref()
                                        .is_some_and(|error| error.undetermined_result.is_some())
                                    {
                                        tracked_undetermined.store(true, atomic::Ordering::SeqCst);
                                    } else if response.region_error.is_none() {
                                        // A response evaluated by TiKV is definitive even
                                        // when it carries a key error such as CommitTsExpired.
                                        tracked_undetermined.store(false, atomic::Ordering::SeqCst);
                                    }
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    result
                })
                    as futures::future::BoxFuture<'_, crate::interceptor::RpcDispatchResult>
            },
        );
        let mut rpc_interceptor = RpcInterceptorChain::new();
        rpc_interceptor.link(ambiguity_interceptor);
        if let Some(existing) = self.rpc_interceptor.clone() {
            rpc_interceptor.link(Arc::new(existing));
        }
        let plan = plan_with_keyspace_name(
            self.rpc.clone(),
            self.keyspace,
            self.keyspace_name.as_deref(),
            Some(rpc_interceptor),
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
        );
        let result = if let Some(owner) = source_retry_owner {
            plan.source_retry_owner(Arc::clone(&owner))
                .retry_multi_region_with_source_retry_owner(
                    self.options.retry_options.region_backoff.clone(),
                    owner,
                )
                .extract_error()
                .plan()
                .execute()
                .await
        } else {
            plan.retry_multi_region(self.options.retry_options.region_backoff.clone())
                .extract_error()
                .plan()
                .execute()
                .await
        };
        self.undetermined = primary_undetermined.load(atomic::Ordering::SeqCst);
        if let Err(error) = &result {
            debug!(
                "commit primary error: {:?}, start_ts: {}",
                error,
                self.start_version.version()
            );
            if has_undetermined_region_error(error) {
                self.undetermined = true;
            }
        }
        result?;

        Ok(commit_version)
    }

    /// Prepares and commits the primary key, returning the commit version.
    async fn commit_primary_and_value_discard<F>(
        &mut self,
        discard_values: &mut Option<F>,
    ) -> Result<Timestamp>
    where
        F: FnOnce() + Send,
    {
        let commit_version = self.prepare_primary_commit_timestamp().await?;
        let source_retry_owner =
            self.source_retry_owner(COMMIT_MAX_BACKOFF.load(atomic::Ordering::Relaxed));
        let mut retained_request = None;
        self.commit_primary_at_version(
            commit_version,
            discard_values,
            source_retry_owner,
            &mut retained_request,
        )
        .await
    }

    #[cfg(test)]
    async fn commit_primary(&mut self) -> Result<Timestamp> {
        let mut discard_values: Option<fn()> = None;
        self.commit_primary_and_value_discard(&mut discard_values)
            .await
    }

    #[cfg(test)]
    async fn commit_primary_with_retry(&mut self) -> Result<Timestamp> {
        let mut discard_values: Option<fn()> = None;
        self.commit_primary_with_retry_and_value_discard(&mut discard_values)
            .await
    }

    async fn commit_primary_with_retry_and_value_discard<F>(
        &mut self,
        discard_values: &mut Option<F>,
    ) -> Result<Timestamp>
    where
        F: FnOnce() + Send,
    {
        let mut commit_version = self.prepare_primary_commit_timestamp().await?;
        let source_retry_owner =
            self.source_retry_owner(COMMIT_MAX_BACKOFF.load(atomic::Ordering::Relaxed));
        let mut retained_request = None;
        loop {
            match self
                .commit_primary_at_version(
                    commit_version.clone(),
                    discard_values,
                    source_retry_owner.clone(),
                    &mut retained_request,
                )
                .await
            {
                Ok(commit_version) => return Ok(commit_version),
                Err(Error::ExtractedErrors(mut errors)) => match errors.pop() {
                    Some(Error::KeyError(key_err)) => {
                        if let Some(expired) = key_err.commit_ts_expired {
                            // Ref: https://github.com/tikv/client-go/blob/tidb-8.5/txnkv/transaction/commit.go
                            info!(
                                "2PC commit_ts rejected by TiKV, retry with a newer commit_ts, start_ts: {}, info: {}",
                                self.start_version.version(),
                                crate::logutil::hex(&expired)
                            );

                            let primary_key = self.primary_key.as_ref().unwrap();
                            if primary_key != expired.key.as_ref() {
                                error!("2PC commit_ts rejected by TiKV, but the key is not the primary key, start_ts: {}, key: {}, primary: {}",
                                    self.start_version.version(), format_key_for_log(&expired.key), format_key_for_log(primary_key));
                                return Err(Error::StringError("2PC commitTS rejected by TiKV, but the key is not the primary key".to_string()));
                            }

                            // Do not retry for a txn which has a too large min_commit_ts.
                            // 3600000 << 18 = 943718400000
                            if commit_ts_expired_gap_is_too_large(&expired) {
                                let msg = format!("2PC min_commit_ts is too large, we got min_commit_ts: {}, and attempted_commit_ts: {}",
                                                     expired.min_commit_ts, expired.attempted_commit_ts);
                                return Err(Error::StringError(msg));
                            }
                            // client-go's actionCommit retry allocates a fresh
                            // timestamp and updates CommitVersion in place. The
                            // initial schema, lifetime, and upper-bound checks
                            // are deliberately not repeated at this boundary.
                            commit_version = self.get_timestamp_for_commit().await?;
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
        let primary_key = self.primary_key.clone().ok_or(Error::NoPrimaryKey)?;
        let source_retry_owner = self.source_retry_owner(COMMIT_SECONDARY_MAX_BACKOFF);
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
            let keys = mutations
                .map(|m| m.key.into())
                .filter(|key| &primary_key != key);
            new_commit_request(keys, start_version.clone(), commit_version)
        };
        req.primary_key = primary_key.clone().into();
        req.use_async_commit = self.options.async_commit;
        req.commit_role = if req
            .keys
            .iter()
            .any(|key| key == <&[u8]>::from(&primary_key))
        {
            kvrpcpb::CommitRole::Primary
        } else {
            kvrpcpb::CommitRole::Secondary
        } as i32;
        self.settings
            .apply_request_context(&mut req, MAX_WRITE_EXECUTION_TIME);
        let has_static_resource_group_tag = self.settings.resource_group_tag.is_some();
        let resource_group_tagger = self.settings.resource_group_tagger.clone();
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
        .decorate_shard_request(
            |request| request.keys.clone(),
            move |request| {
                apply_transaction_resource_group_tagger(
                    request,
                    has_static_resource_group_tag,
                    resource_group_tagger.as_ref(),
                );
            },
        )
        .priority(self.options.priority)
        .resolve_lock_with_context(
            start_version,
            self.options.retry_options.lock_backoff,
            self.keyspace,
            lock_resolver_context,
        );
        if let Some(owner) = source_retry_owner {
            plan.source_retry_owner(Arc::clone(&owner))
                .retry_multi_region_with_source_retry_owner(
                    self.options.retry_options.region_backoff,
                    owner,
                )
                .extract_error()
                .plan()
                .execute()
                .await?;
        } else {
            plan.retry_multi_region(self.options.retry_options.region_backoff)
                .extract_error()
                .plan()
                .execute()
                .await?;
        }
        Ok(())
    }

    async fn execute_cleanup_request(
        &self,
        mut request: kvrpcpb::BatchRollbackRequest,
        source_retry_owner: Option<Arc<tokio::sync::Mutex<RetryBackoffer>>>,
    ) -> Result<()> {
        self.settings
            .apply_cleanup_request_context(&mut request, MAX_WRITE_EXECUTION_TIME);
        let has_static_resource_group_tag = self.settings.resource_group_tag.is_some();
        let resource_group_tagger = self.settings.resource_group_tagger.clone();
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
        .decorate_shard_request(
            |request| request.keys.clone(),
            move |request| {
                apply_transaction_resource_group_tagger(
                    request,
                    has_static_resource_group_tag,
                    resource_group_tagger.as_ref(),
                );
            },
        )
        .priority(self.options.priority)
        .resolve_lock_with_context(
            self.start_version.clone(),
            self.options.retry_options.lock_backoff.clone(),
            self.keyspace,
            self.lock_resolver_context.clone(),
        );
        if let Some(owner) = source_retry_owner {
            plan.source_retry_owner(Arc::clone(&owner))
                .retry_multi_region_with_source_retry_owner(
                    self.options.retry_options.region_backoff.clone(),
                    owner,
                )
                .extract_error()
                .plan()
                .execute()
                .await?;
        } else {
            plan.retry_multi_region(self.options.retry_options.region_backoff.clone())
                .extract_error()
                .plan()
                .execute()
                .await?;
        }
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
        let source_retry_owner = self.source_retry_owner(CLEANUP_MAX_BACKOFF);
        self.rollback_with_retry_owner(prewritten, source_retry_owner)
            .await
    }

    async fn rollback_with_retry_owner(
        self,
        prewritten: bool,
        source_retry_owner: Option<Arc<tokio::sync::Mutex<RetryBackoffer>>>,
    ) -> Result<()> {
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
                if let Err(error) = self.broadcast_pipelined_status(0, true, true, true).await {
                    warn!("broadcast completed pipelined rollback status failed: {error}");
                }
                return Ok(());
            }
            return self.finish_pipelined_locks(0).await;
        }
        if self.options.kind == TransactionKind::Optimistic && !prewritten {
            return Ok(());
        }
        let pessimistic_only =
            matches!(self.options.kind, TransactionKind::Pessimistic(_)) && !prewritten;
        let pessimistic_lock_keys = self.pessimistic_lock_keys.clone();
        let keys = self
            .mutations
            .iter()
            .filter(move |mutation| {
                !pessimistic_only || pessimistic_lock_keys.contains(&mutation.key)
            })
            .map(|mutation| Key::from(mutation.key.clone()))
            .collect::<Vec<_>>();
        match self.options.kind.clone() {
            TransactionKind::Pessimistic(for_update_ts) if !prewritten => {
                let mut req = new_pessimistic_rollback_request(
                    keys.into_iter(),
                    self.start_version.clone(),
                    for_update_ts,
                );
                self.settings
                    .apply_pessimistic_rollback_request(&mut req, MAX_WRITE_EXECUTION_TIME);
                let plan = plan_with_keyspace_name(
                    self.rpc.clone(),
                    self.keyspace,
                    self.keyspace_name.as_deref(),
                    self.rpc_interceptor.clone(),
                    None,
                    None,
                    self.ru_details.clone(),
                    ReplicaReadConfig::default(),
                    req,
                )
                .resolve_lock_with_context(
                    self.start_version.clone(),
                    self.options.retry_options.lock_backoff.clone(),
                    self.keyspace,
                    self.lock_resolver_context.clone(),
                );
                if let Some(owner) = source_retry_owner {
                    plan.source_retry_owner(Arc::clone(&owner))
                        .retry_multi_region_with_source_retry_owner(
                            self.options.retry_options.region_backoff.clone(),
                            owner,
                        )
                        .extract_error()
                        .plan()
                        .execute()
                        .await?;
                } else {
                    plan.retry_multi_region(self.options.retry_options.region_backoff.clone())
                        .extract_error()
                        .plan()
                        .execute()
                        .await?;
                }
            }
            // Optimistic, or pessimistic after prewrite: BatchRollback clears
            // both pessimistic and 2PC locks by start_ts.
            _ => {
                let request =
                    new_batch_rollback_request(keys.into_iter(), self.start_version.clone());
                let located_shards = request
                    .shards(&self.rpc)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                let primary_key = self.primary_key.as_ref().map(<&[u8]>::from);
                let primary_batch = (located_shards.len() > 1)
                    .then(|| {
                        primary_key.and_then(|primary_key| {
                            located_shards.iter().find_map(|(keys, _)| {
                                keys.iter()
                                    .any(|key| key.as_slice() == primary_key)
                                    .then(|| keys.clone())
                            })
                        })
                    })
                    .flatten();
                if let Some(primary_keys) = primary_batch {
                    let mut primary_request = request.clone();
                    primary_request.keys = primary_keys.clone();
                    self.execute_cleanup_request(primary_request, source_retry_owner.clone())
                        .await?;
                    let mut secondary_request = request;
                    secondary_request
                        .keys
                        .retain(|key| !primary_keys.contains(key));
                    if !secondary_request.keys.is_empty() {
                        self.execute_cleanup_request(secondary_request, source_retry_owner)
                            .await?;
                    }
                } else {
                    self.execute_cleanup_request(request, source_retry_owner)
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn calc_txn_lock_ttl(&mut self) -> u64 {
        let mut lock_ttl = DEFAULT_LOCK_TTL;
        if self.write_size >= crate::kv::TXN_COMMIT_BATCH_SIZE.load(atomic::Ordering::Relaxed) {
            let size_mb = self.write_size as f64 / 1024.0 / 1024.0;
            lock_ttl = (TTL_FACTOR * size_mb.sqrt()) as u64;
            lock_ttl = lock_ttl.max(DEFAULT_LOCK_TTL);
            let managed_lock_ttl = managed_lock_ttl();
            if lock_ttl > managed_lock_ttl {
                lock_ttl = managed_lock_ttl;
            }
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
    use super::LockContext;
    use super::MinCommitTsManager;
    use super::PipelinedTransactionState;
    use super::PrewriteEncounterLockPolicy;
    use super::TransactionKind;
    use super::TransactionStatus;
    use super::TxnFileAction;
    use super::WriteAccessLevel;
    use super::DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE;
    use super::MAX_TTL;
    use crate::transaction::txn_file::{ChunkBatch, TxnChunkRange, TxnChunkSlice};
    use crate::transaction::unionstore::PipelinedError;
    use crate::transaction::ResolveLocksContext;
    use std::any::Any;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::io;
    use std::sync::atomic::Ordering;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::Once;
    use std::time::{Duration, Instant, SystemTime};

    use fail::FailScenario;

    #[test]
    fn source_go_store_driver_txn_lock_upgrade_error_mapping() {
        let error = Error::KeyError(Box::new(kvrpcpb::KeyError {
            lock_upgrade_conflict: Some(kvrpcpb::LockUpgradeConflict {
                key: b"key".to_vec(),
                start_ts: 101,
                owner_start_ts: 202,
                reason: kvrpcpb::lock_upgrade_conflict::Reason::SecondUpgrader as i32,
            }),
            ..Default::default()
        }));
        let deadlock = super::pessimistic_deadlock(&error).expect("upgrade maps to deadlock");
        assert_eq!(deadlock.lock_ts, 202);
        assert_eq!(deadlock.lock_key, b"key");
        assert_eq!(deadlock.deadlock_key_hash, 0);
    }

    #[test]
    fn source_go_store_driver_txn_shared_lock_lost_stays_typed() {
        let error = Error::from(kvrpcpb::KeyError {
            shared_lock_lost: Some(kvrpcpb::SharedLockLost {
                key: b"key".to_vec(),
                start_ts: 101,
            }),
            ..Default::default()
        });
        let Error::SharedLockLost(error) = error else {
            panic!("shared lock loss must not become a generic key error")
        };
        assert_eq!(error.shared_lock_lost.start_ts, 101);
        assert_eq!(error.shared_lock_lost.key, b"key");
    }

    #[test]
    fn source_uncovered_effective_wait_preserves_future_start_time() {
        let now = std::time::UNIX_EPOCH + Duration::from_secs(1);
        let wait_start = now + Duration::from_millis(50);

        assert_eq!(
            super::calculate_pessimistic_lock_wait_time(None, 100, Some(wait_start), None, now,)
                .unwrap(),
            150,
        );

        let far_deadline = now
            .checked_add(Duration::from_millis(
                (super::GO_DURATION_MAX_MILLIS + 1) as u64,
            ))
            .unwrap();
        assert_eq!(
            super::calculate_pessimistic_lock_wait_time(
                None,
                crate::kv::LOCK_ALWAYS_WAIT,
                Some(now),
                Some(far_deadline),
                now,
            )
            .unwrap(),
            super::GO_DURATION_MAX_MILLIS,
        );
    }

    use crate::disable_resource_control;
    use crate::enable_resource_control;
    use crate::mock::MockKvClient;
    use crate::mock::MockPdClient;
    use crate::oracle::{OracleError, OracleOption, OracleResult, ReadTimestampValidator};
    use crate::proto::kvrpcpb;
    use crate::proto::pdpb::Timestamp;
    use crate::proto::resource_manager;
    use crate::request::Keyspace;
    use crate::request::RetryOptions;
    use crate::set_resource_control_interceptor;
    use crate::store::Request as _;
    use crate::transaction::HeartbeatOption;
    use crate::unset_resource_control_interceptor;
    use crate::Backoff;
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
    fn source_config_commit_defaults_apply_without_disabling_explicit_options() {
        let configured =
            TransactionOptions::new_optimistic().with_config_commit_defaults(true, true);
        assert!(configured.async_commit);
        assert!(configured.try_one_pc);

        let explicit = TransactionOptions::new_optimistic()
            .use_async_commit()
            .try_one_pc()
            .with_config_commit_defaults(false, false);
        assert!(explicit.async_commit);
        assert!(explicit.try_one_pc);

        let disabled =
            TransactionOptions::new_optimistic().with_config_commit_defaults(false, false);
        assert!(!disabled.async_commit);
        assert!(!disabled.try_one_pc);
    }

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

    #[test]
    fn source_is_read_only_uses_memdb_dirty_state_not_entry_count() {
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        assert!(transaction.is_read_only());

        let stage = transaction.get_mem_buffer().staging();
        transaction
            .get_mem_buffer()
            .set(b"staged", b"value")
            .unwrap();
        assert_eq!(transaction.len(), 1);
        assert!(transaction.is_read_only());

        transaction.get_mem_buffer().release(stage);
        assert!(!transaction.is_read_only());

        let stage = transaction.get_mem_buffer().staging();
        transaction
            .get_mem_buffer()
            .set(b"cleaned", b"value")
            .unwrap();
        transaction.get_mem_buffer().cleanup(stage);
        assert!(!transaction.is_read_only());
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

    async fn source_test_txn_chunk_writer() -> (String, tokio::task::JoinHandle<Vec<u8>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let uploaded_chunk = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let (header_end, content_length) = loop {
                let mut part = [0_u8; 1024];
                let read = stream.read(&mut part).await.unwrap();
                assert!(read > 0);
                received.extend_from_slice(&part[..read]);
                if let Some(header_end) = received.windows(4).position(|part| part == b"\r\n\r\n") {
                    let header_end = header_end + 4;
                    let headers = std::str::from_utf8(&received[..header_end]).unwrap();
                    assert!(headers.starts_with(&format!(
                        "POST /txn_chunk?keyspace_id={} HTTP/1.1\r\n",
                        crate::request::NULL_KEYSPACE_ID
                    )));
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>())
                        })
                        .unwrap()
                        .unwrap();
                    break (header_end, content_length);
                }
            };
            while received.len() < header_end + content_length {
                let mut part = [0_u8; 1024];
                let read = stream.read(&mut part).await.unwrap();
                assert!(read > 0);
                received.extend_from_slice(&part[..read]);
            }
            let response = b"{\"chunk_id\":1}";
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(response).await.unwrap();
            received[header_end..header_end + content_length].to_vec()
        });
        (address, uploaded_chunk)
    }

    static GLOBAL_RESOURCE_CONTROL_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    #[allow(non_snake_case)]
    fn source_go_txnkv_transaction_TestMinCommitTsManager() {
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
    #[serial_test::serial]
    fn source_config_probe_values_and_presplit_controls() {
        struct AtomicU32Restore(&'static std::sync::atomic::AtomicU32, u32);
        impl Drop for AtomicU32Restore {
            fn drop(&mut self) {
                self.0.store(self.1, Ordering::SeqCst);
            }
        }

        assert_eq!(
            crate::kv::TXN_COMMIT_BATCH_SIZE.load(Ordering::SeqCst),
            16 * 1024
        );
        assert_eq!(super::PESSIMISTIC_LOCK_MAX_BACKOFF, 20_000);
        assert_eq!(super::DEFAULT_LOCK_TTL, 3_000);
        assert_eq!(super::TTL_FACTOR, 6_000.0);

        let old_detect = super::PRE_SPLIT_DETECT_THRESHOLD.swap(17, Ordering::SeqCst);
        let _detect_restore = AtomicU32Restore(&super::PRE_SPLIT_DETECT_THRESHOLD, old_detect);
        let old_size = super::PRE_SPLIT_SIZE_THRESHOLD.swap(23, Ordering::SeqCst);
        let _size_restore = AtomicU32Restore(&super::PRE_SPLIT_SIZE_THRESHOLD, old_size);
        assert_eq!(super::PRE_SPLIT_DETECT_THRESHOLD.load(Ordering::SeqCst), 17);
        assert_eq!(super::PRE_SPLIT_SIZE_THRESHOLD.load(Ordering::SeqCst), 23);
    }

    #[test]
    #[serial_test::serial]
    fn source_transaction_lock_ttl_uses_shared_commit_batch_threshold() {
        struct AtomicRestore(&'static std::sync::atomic::AtomicU64, u64);
        impl Drop for AtomicRestore {
            fn drop(&mut self) {
                self.0.store(self.1, Ordering::SeqCst);
            }
        }

        let old_threshold = crate::kv::TXN_COMMIT_BATCH_SIZE.swap(u64::MAX, Ordering::SeqCst);
        let _threshold_restore = AtomicRestore(&crate::kv::TXN_COMMIT_BATCH_SIZE, old_threshold);
        let old_managed_ttl = super::MANAGED_LOCK_TTL.swap(20_000, Ordering::SeqCst);
        let _managed_ttl_restore = AtomicRestore(&super::MANAGED_LOCK_TTL, old_managed_ttl);
        let rpc = Arc::new(MockPdClient::default());
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        committer.write_size = 1024 * 1024;

        assert_eq!(committer.calc_txn_lock_ttl(), 3_000);
        crate::kv::TXN_COMMIT_BATCH_SIZE.store(1, Ordering::SeqCst);
        assert_eq!(committer.calc_txn_lock_ttl(), 6_000);
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
        assert_eq!(
            Transaction::try_new(
                Timestamp::from_version(1),
                Arc::new(MockPdClient::default()),
                invalid_flush,
                Keyspace::Disable,
            )
            .err()
            .expect("invalid injected options must be rejected")
            .to_string(),
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
        let configured_start = Timestamp::from_version(42);
        assert_eq!(
            TransactionOptions::new_optimistic()
                .start_timestamp(configured_start.clone())
                .configured_start_timestamp(),
            Some(configured_start)
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

    #[test]
    #[serial_test::serial]
    #[allow(non_snake_case)]
    fn source_go_integration_tests_option_test_TestSetCommitWaitUntilTSO() {
        std::thread::Builder::new()
            .name("client-go-TestSetCommitWaitUntilTSO".to_owned())
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_stack_size(16 * 1024 * 1024)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        #[derive(Clone, Copy)]
                        struct Case {
                            name: &'static str,
                            commit_wait_offset: u64,
                            timestamp_offsets: &'static [u64],
                            timeout: Option<Duration>,
                            one_pc: bool,
                            causal_consistency: bool,
                            error: bool,
                        }

                        let cases = [
                            Case {
                                name: "no lag commit ts",
                                commit_wait_offset: 1,
                                timestamp_offsets: &[100],
                                timeout: None,
                                one_pc: false,
                                causal_consistency: false,
                                error: false,
                            },
                            Case {
                                name: "lag, retry once and success",
                                commit_wait_offset: 200,
                                timestamp_offsets: &[100, 201],
                                timeout: None,
                                one_pc: false,
                                causal_consistency: false,
                                error: false,
                            },
                            Case {
                                name: "no wait",
                                commit_wait_offset: 200,
                                timestamp_offsets: &[100],
                                timeout: Some(Duration::ZERO),
                                one_pc: false,
                                causal_consistency: false,
                                error: true,
                            },
                            Case {
                                name: "lag, retry twice and success",
                                commit_wait_offset: 300,
                                timestamp_offsets: &[100, 200, 301],
                                timeout: None,
                                one_pc: false,
                                causal_consistency: false,
                                error: false,
                            },
                            Case {
                                name: "lag too much, fail directly",
                                commit_wait_offset: crate::oracle::compose_timestamp(10_000, 0),
                                timestamp_offsets: &[100],
                                timeout: None,
                                one_pc: false,
                                causal_consistency: false,
                                error: true,
                            },
                            Case {
                                name: "lag, retry but timeout",
                                commit_wait_offset: 100,
                                timestamp_offsets: &[10, 20],
                                timeout: Some(Duration::from_millis(1)),
                                one_pc: false,
                                causal_consistency: false,
                                error: true,
                            },
                            Case {
                                name: "should also check for 1pc",
                                commit_wait_offset: crate::oracle::compose_timestamp(10_000, 0),
                                timestamp_offsets: &[100],
                                timeout: None,
                                one_pc: true,
                                causal_consistency: false,
                                error: true,
                            },
                            Case {
                                name: "should also check for causal consistency",
                                commit_wait_offset: crate::oracle::compose_timestamp(10_000, 0),
                                timestamp_offsets: &[100],
                                timeout: None,
                                one_pc: true,
                                causal_consistency: true,
                                error: true,
                            },
                        ];

                        for case in cases {
                            let start = crate::oracle::compose_timestamp(1_000, 0);
                            let rpc = Arc::new(MockPdClient::new(
                                MockKvClient::with_dispatch_hook(|request: &dyn Any| {
                                    if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some()
                                    {
                                        return Ok(Box::<kvrpcpb::PrewriteResponse>::default()
                                            as Box<dyn Any>);
                                    }
                                    if request.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                                        return Ok(Box::<kvrpcpb::CommitResponse>::default()
                                            as Box<dyn Any>);
                                    }
                                    if request
                                        .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                                        .is_some()
                                    {
                                        return Ok(
                                            Box::<kvrpcpb::BatchRollbackResponse>::default()
                                                as Box<dyn Any>,
                                        );
                                    }
                                    panic!("unexpected option-test request")
                                }),
                            ));
                            rpc.set_timestamp_sequence(case.timestamp_offsets.iter().map(
                                |offset| Timestamp::from_version(start.saturating_add(*offset)),
                            ));
                            let mut transaction = Transaction::new(
                                Timestamp::from_version(start),
                                rpc.clone(),
                                TransactionOptions::new_optimistic()
                                    .heartbeat_option(HeartbeatOption::NoHeartbeat)
                                    .drop_check(CheckLevel::None),
                                Keyspace::Disable,
                            );
                            transaction
                                .put(
                                    format!("~option:{}", case.name).into_bytes(),
                                    b"somevalue".to_vec(),
                                )
                                .await
                                .unwrap();
                            transaction.set_enable_async_commit(case.one_pc);
                            transaction.set_enable_one_pc(case.one_pc);
                            transaction.set_causal_consistency(case.causal_consistency);
                            transaction.set_commit_wait_until_tso(
                                start.saturating_add(case.commit_wait_offset),
                            );
                            if let Some(timeout) = case.timeout {
                                transaction.set_commit_wait_until_tso_timeout(timeout);
                            }

                            let details =
                                Arc::new(Mutex::new(crate::util::CommitDetails::default()));
                            let context = crate::util::context_with_commit_details(
                                &crate::trace::TraceContext::new(),
                                details.clone(),
                            );
                            let before = crate::stats::commit_ts_lag_sample_counts();
                            let result =
                                crate::trace::with_trace_context(context, transaction.commit())
                                    .await;
                            rpc.clear_timestamp_sequence();
                            let after = crate::stats::commit_ts_lag_sample_counts();
                            let increments = std::array::from_fn::<_, 4, _>(|index| {
                                after[index] - before[index]
                            });

                            assert_eq!(
                                transaction.commit_wait_until_tso(),
                                start.saturating_add(case.commit_wait_offset),
                                "{}",
                                case.name
                            );
                            if case.error {
                                let error = result.expect_err(case.name);
                                assert!(
                                    crate::error::is_error_commit_timestamp_lag(&error),
                                    "{}: {error}",
                                    case.name
                                );
                                assert_eq!(increments, [0, 0, 1, 1], "{}", case.name);
                            } else {
                                result.unwrap_or_else(|error| panic!("{}: {error}", case.name));
                                assert_eq!(
                                    transaction.commit_timestamp().unwrap().version(),
                                    start
                                        + case.timestamp_offsets[case.timestamp_offsets.len() - 1],
                                    "{}",
                                    case.name
                                );
                                let details = details.lock().unwrap().clone();
                                if case.timestamp_offsets.len() == 1 {
                                    assert_eq!(increments, [0, 0, 0, 0], "{}", case.name);
                                    assert_eq!(
                                        details.lag_details,
                                        crate::util::CommitTsLagDetails::default(),
                                        "{}",
                                        case.name
                                    );
                                } else {
                                    assert_eq!(increments, [1, 1, 0, 0], "{}", case.name);
                                    assert!(details.lag_details.wait_time > Duration::ZERO);
                                    assert_eq!(
                                        details.lag_details.backoff_count,
                                        (case.timestamp_offsets.len() - 1) as i32,
                                        "{}",
                                        case.name
                                    );
                                    assert_eq!(
                                        details.lag_details.first_lag_ts,
                                        start + case.timestamp_offsets[0],
                                        "{}",
                                        case.name
                                    );
                                    assert_eq!(
                                        details.lag_details.wait_until_ts,
                                        transaction.commit_wait_until_tso(),
                                        "{}",
                                        case.name
                                    );
                                }
                            }
                        }
                    });
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_integration_tests_option_test_TestSetCommitWaitUntilTSOTimeout() {
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
        transaction.set_commit_wait_until_tso_timeout(Duration::from_secs(2));
        assert_eq!(
            transaction.commit_wait_until_tso_timeout(),
            Duration::from_secs(2)
        );
    }

    #[tokio::test]
    async fn source_session_id_only_applies_after_committer_initialization() {
        let mut ordinary = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        ordinary.set_session_id(11);
        assert_eq!(ordinary.commit_settings.session_id, 0);

        let rpc = Arc::new(MockPdClient::default());
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut pessimistic = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        assert_eq!(
            pessimistic
                .lock_keys_shared(["shared".to_owned()])
                .await
                .unwrap_err()
                .to_string(),
            "pessimistic lock in share mode requires primary key to be selected"
        );
        pessimistic.set_session_id(22);
        assert_eq!(pessimistic.commit_settings.session_id, 22);

        let mut pipelined = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic()
                .pipelined(crate::transaction::PipelinedTxnOptions {
                    enable: true,
                    flush_concurrency: 1,
                    resolve_lock_concurrency: 1,
                    write_throttle_ratio: 0.0,
                })
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        pipelined.set_session_id(33);
        assert_eq!(pipelined.commit_settings.session_id, 33);
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
                kvrpcpb::prewrite_request::PessimisticAction::DoConstraintCheck as i32,
                kvrpcpb::prewrite_request::PessimisticAction::DoPessimisticCheck as i32,
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
    async fn source_standard_prewrite_omits_async_secondaries() {
        let prewrites = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&prewrites);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PessimisticRollbackRequest>() {
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                let request = request
                    .downcast_ref::<kvrpcpb::PrewriteRequest>()
                    .expect("test dispatches one prewrite request");
                assert_eq!(
                    request
                        .context
                        .as_ref()
                        .expect("prewrite carries a request context")
                        .max_execution_duration_ms,
                    20_000
                );
                captured.lock().unwrap().push(request.clone());
                Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"a".to_vec())),
            vec![
                source_test_mutation("a", kvrpcpb::Op::Put),
                source_test_mutation("b", kvrpcpb::Op::Put),
            ],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );

        committer.prewrite().await.unwrap();
        assert!(prewrites.lock().unwrap()[0].secondaries.is_empty());
    }

    #[tokio::test]
    async fn source_single_batch_one_pc_fallback_requires_zero_min_commit_ts() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::new(kvrpcpb::PrewriteResponse {
                min_commit_ts: 9,
                ..Default::default()
            }) as Box<dyn Any>)
        })));
        let options = TransactionOptions::new_optimistic()
            .use_async_commit()
            .try_one_pc();
        let mut invalid = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            options.clone(),
            CommitSettings::default(),
        );
        assert_eq!(
            invalid.prewrite().await.unwrap_err().to_string(),
            "MinCommitTs must be 0 when 1pc falls back to 2pc"
        );

        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
        })));
        let mut fallback = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            options,
            CommitSettings::default(),
        );
        assert_eq!(fallback.prewrite().await.unwrap(), None);
        assert!(!fallback.options.try_one_pc);
        assert!(!fallback.options.async_commit);
    }

    #[tokio::test]
    async fn source_one_pc_failure_cleanup_depends_on_transaction_kind() {
        let optimistic_cleanups = Arc::new(AtomicUsize::new(0));
        let captured_optimistic_cleanups = Arc::clone(&optimistic_cleanups);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PrewriteRequest>() {
                    return Ok(Box::new(kvrpcpb::PrewriteResponse {
                        errors: vec![kvrpcpb::KeyError {
                            abort: "reject 1pc".to_owned(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                captured_optimistic_cleanups.fetch_add(1, Ordering::SeqCst);
                if request.is::<kvrpcpb::BatchRollbackRequest>() {
                    return Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        let optimistic = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().try_one_pc(),
            CommitSettings::default(),
        );
        optimistic.commit().await.unwrap_err();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(optimistic_cleanups.load(Ordering::SeqCst), 0);

        let pessimistic_rollbacks = Arc::new(Mutex::new(Vec::new()));
        let captured_pessimistic_rollbacks = Arc::clone(&pessimistic_rollbacks);
        let rollback_sent = Arc::new(tokio::sync::Notify::new());
        let captured_rollback_sent = Arc::clone(&rollback_sent);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PrewriteRequest>() {
                    return Ok(Box::new(kvrpcpb::PrewriteResponse {
                        errors: vec![kvrpcpb::KeyError {
                            abort: "reject pessimistic 1pc".to_owned(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                    .expect("failed pessimistic 1PC uses PessimisticRollback");
                captured_pessimistic_rollbacks
                    .lock()
                    .unwrap()
                    .push(request.keys.clone());
                captured_rollback_sent.notify_one();
                Ok(Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        let pessimistic = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_pessimistic().try_one_pc(),
            CommitSettings::default(),
        );
        pessimistic.commit().await.unwrap_err();
        tokio::time::timeout(Duration::from_secs(1), rollback_sent.notified())
            .await
            .expect("pessimistic 1PC cleanup is dispatched");
        assert_eq!(
            *pessimistic_rollbacks.lock().unwrap(),
            [vec![b"k".to_vec()]]
        );
    }

    #[tokio::test]
    async fn source_cleanup_actions_use_one_cumulative_retry_owner() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observed);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchRollbackRequest>() {
                    captured.lock().unwrap().push((
                        "cleanup",
                        request.context.as_ref().unwrap().is_retry_request,
                    ));
                    return Ok(Box::new(kvrpcpb::BatchRollbackResponse {
                        region_error: Some(crate::proto::errorpb::Error {
                            not_leader: Some(crate::proto::errorpb::NotLeader {
                                region_id: 2,
                                leader: None,
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                    .expect("pessimistic cleanup sends PessimisticRollback");
                captured.lock().unwrap().push((
                    "pessimistic",
                    request.context.as_ref().unwrap().is_retry_request,
                ));
                Ok(Box::new(kvrpcpb::PessimisticRollbackResponse {
                    region_error: Some(crate::proto::errorpb::Error {
                        not_leader: Some(crate::proto::errorpb::NotLeader {
                            region_id: 2,
                            leader: None,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));

        let optimistic = source_test_committer(
            Arc::clone(&rpc),
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        let cleanup_owner = Arc::new(tokio::sync::Mutex::new(super::RetryBackoffer::new(
            crate::async_util::Cancellation::default(),
            1,
        )));
        optimistic
            .rollback_with_retry_owner(true, Some(Arc::clone(&cleanup_owner)))
            .await
            .unwrap_err();

        let pessimistic = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_pessimistic(),
            CommitSettings::default(),
        )
        .with_pessimistic_lock_keys(BTreeSet::from([b"k".to_vec()]));
        let pessimistic_owner = Arc::new(tokio::sync::Mutex::new(super::RetryBackoffer::new(
            crate::async_util::Cancellation::default(),
            1,
        )));
        pessimistic
            .rollback_with_retry_owner(false, Some(Arc::clone(&pessimistic_owner)))
            .await
            .unwrap_err();

        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                ("cleanup", false),
                ("cleanup", true),
                ("pessimistic", false),
                ("pessimistic", true),
            ]
        );
        assert!(cleanup_owner.lock().await.total_sleep_ms() >= 1);
        assert!(pessimistic_owner.lock().await.total_sleep_ms() >= 1);
    }

    #[tokio::test]
    async fn source_standard_actions_tag_each_physical_batch_and_cleanup_primary_first() {
        let tagger_calls = Arc::new(Mutex::new(Vec::new()));
        let captured_tagger_calls = Arc::clone(&tagger_calls);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let rpc = Arc::new(MockPdClient::with_client_and_regions(
            MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
                let (action, keys, tag) =
                    if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                        (
                            "prewrite",
                            request
                                .mutations
                                .iter()
                                .map(|mutation| mutation.key.clone())
                                .collect::<Vec<_>>(),
                            request.context.as_ref().unwrap().resource_group_tag.clone(),
                        )
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                        (
                            "commit",
                            request.keys.clone(),
                            request.context.as_ref().unwrap().resource_group_tag.clone(),
                        )
                    } else {
                        let request = request
                            .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                            .expect("standard cleanup sends BatchRollback");
                        (
                            "cleanup",
                            request.keys.clone(),
                            request.context.as_ref().unwrap().resource_group_tag.clone(),
                        )
                    };
                captured_requests
                    .lock()
                    .unwrap()
                    .push((action, keys.clone(), tag));
                match action {
                    "prewrite" => Ok(Box::new(kvrpcpb::PrewriteResponse {
                        min_commit_ts: 2,
                        ..Default::default()
                    }) as Box<dyn Any>),
                    "commit" => Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>),
                    _ => Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>),
                }
            }),
            vec![MockPdClient::region1(), MockPdClient::region2()],
        ));
        let mut settings = CommitSettings::default();
        settings.resource_group_tagger = Some(Arc::new(move |request| {
            let (action, keys) = if let Some(request) =
                request.as_any().downcast_ref::<kvrpcpb::PrewriteRequest>()
            {
                (
                    "prewrite",
                    request
                        .mutations
                        .iter()
                        .map(|mutation| mutation.key.clone())
                        .collect::<Vec<_>>(),
                )
            } else if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::CommitRequest>()
            {
                ("commit", request.keys.clone())
            } else {
                let request = request
                    .as_any()
                    .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                    .expect("tagger receives a physical transaction action");
                ("cleanup", request.keys.clone())
            };
            captured_tagger_calls
                .lock()
                .unwrap()
                .push((action, keys.clone()));
            request.set_resource_group_tag(keys[0].clone());
        }));
        let mutations = vec![
            source_test_mutation(vec![1], kvrpcpb::Op::Put),
            source_test_mutation(vec![20], kvrpcpb::Op::Put),
        ];
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(vec![20])),
            mutations,
            TransactionOptions::new_optimistic().use_async_commit(),
            settings,
        );

        assert_eq!(committer.prewrite().await.unwrap().unwrap().version(), 2);
        committer
            .clone()
            .commit_secondary(Timestamp::from_version(3))
            .await
            .unwrap();
        committer.rollback(true).await.unwrap();

        let tagger_calls = tagger_calls.lock().unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(tagger_calls.len(), 6);
        assert_eq!(requests.len(), 6);
        for ((tag_action, tag_keys), (request_action, request_keys, tag)) in
            tagger_calls.iter().zip(requests.iter())
        {
            assert_eq!(tag_action, request_action);
            assert_eq!(tag_keys, request_keys);
            assert_eq!(tag, &request_keys[0]);
        }
        assert_eq!(requests[4].0, "cleanup");
        assert_eq!(requests[4].1, vec![vec![20]]);
        assert_eq!(requests[5].0, "cleanup");
        assert_eq!(requests[5].1, vec![vec![1]]);
    }

    #[tokio::test]
    async fn source_async_commit_validates_maximum_lifetime_after_prewrite() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request| {
                assert!(request.is::<kvrpcpb::PrewriteRequest>());
                Ok(Box::new(kvrpcpb::PrewriteResponse {
                    min_commit_ts: 2,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut settings = CommitSettings::default();
        settings.causal_consistency = true;
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().use_async_commit(),
            settings,
        );
        committer.start_instant =
            Instant::now() - Duration::from_millis(super::MAX_TXN_TIME_USE.saturating_add(1));

        let mut discard_values: Option<fn()> = None;
        let error = committer
            .execute_commit(&mut discard_values)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("txn takes too much time"));
        assert!(!committer.committed);
    }

    #[tokio::test]
    async fn source_async_prewrite_transport_failure_is_ambiguous() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Err(Error::GrpcAPI(tonic::Status::unavailable(
                "prewrite response lost",
            )))
        })));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic()
                .use_async_commit()
                .retry_options(RetryOptions::none()),
            CommitSettings::default(),
        );

        let error = committer.prewrite().await.unwrap_err();
        assert!(committer.undetermined);
        assert!(matches!(error, Error::UndeterminedError(_)));
    }

    #[tokio::test]
    async fn source_aggressive_lock_commit_sends_expected_for_update_ts() {
        let prewrites = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&prewrites);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    let response = if request.wake_up_mode
                        == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
                    {
                        kvrpcpb::PessimisticLockResponse {
                            results: vec![kvrpcpb::PessimisticLockKeyResult {
                                r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                                    as i32,
                                existence: true,
                                locked_with_conflict_ts: 9,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }
                    } else {
                        kvrpcpb::PessimisticLockResponse::default()
                    };
                    return Ok(Box::new(response) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured.lock().unwrap().push(request.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if request.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request in aggressive-lock commit test");
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc.clone(),
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.start_aggressive_locking();
        transaction.lock_keys(["k".to_owned()]).await.unwrap();
        transaction.done_aggressive_locking().await.unwrap();
        transaction.put("k".to_owned(), "value").await.unwrap();
        rpc.set_timestamp(Timestamp::from_version(10));

        transaction.commit().await.unwrap();
        let prewrites = prewrites.lock().unwrap();
        assert_eq!(prewrites.len(), 1);
        assert_eq!(
            prewrites[0].for_update_ts_constraints,
            [kvrpcpb::prewrite_request::ForUpdateTsConstraint {
                index: 0,
                expected_for_update_ts: 9,
            }]
        );
    }

    #[tokio::test]
    async fn source_aggressive_retry_relocks_after_the_managed_ttl_window() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = dispatches.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PessimisticRollbackRequest>() {
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("aggressive retry sends PessimisticLock");
                assert_eq!(
                    request.wake_up_mode,
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
                );
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::PessimisticLockResponse {
                    results: vec![kvrpcpb::PessimisticLockKeyResult {
                        r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultNormal as i32,
                        existence: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.start_aggressive_locking();
        transaction.lock_keys(["key".to_owned()]).await.unwrap();
        transaction.retry_aggressive_locking().await.unwrap();
        transaction
            .aggressive_locking
            .as_mut()
            .unwrap()
            .last_attempt_start = Some(Instant::now() - Duration::from_millis(MAX_TTL + 1));

        transaction.lock_keys(["key".to_owned()]).await.unwrap();

        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
        transaction.cancel_aggressive_locking().await.unwrap();
    }

    #[tokio::test]
    async fn source_aggressive_retry_reselects_primary_when_the_key_changes() {
        let primaries = Arc::new(Mutex::new(Vec::new()));
        let captured_primaries = primaries.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PessimisticRollbackRequest>() {
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("aggressive locking sends PessimisticLock");
                captured_primaries
                    .lock()
                    .unwrap()
                    .push(request.primary_lock.clone());
                Ok(Box::new(kvrpcpb::PessimisticLockResponse {
                    results: vec![kvrpcpb::PessimisticLockKeyResult {
                        r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultNormal as i32,
                        existence: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.start_aggressive_locking();
        transaction.lock_keys(["b".to_owned()]).await.unwrap();
        transaction.retry_aggressive_locking().await.unwrap();
        transaction.lock_keys(["a".to_owned()]).await.unwrap();

        assert_eq!(*primaries.lock().unwrap(), [b"b".to_vec(), b"a".to_vec()]);
        transaction.cancel_aggressive_locking().await.unwrap();
    }

    #[tokio::test]
    async fn source_aggressive_retry_normalizes_reused_force_lock_metadata() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = dispatches.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("reused aggressive lock avoids every other RPC");
                assert_eq!(
                    request.wake_up_mode,
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
                );
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::PessimisticLockResponse {
                    results: vec![kvrpcpb::PessimisticLockKeyResult {
                        r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                            as i32,
                        value: b"saved".to_vec(),
                        existence: true,
                        locked_with_conflict_ts: 9,
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.start_aggressive_locking();
        transaction.lock_keys(["key".to_owned()]).await.unwrap();
        transaction.retry_aggressive_locking().await.unwrap();
        let mut retry_context = LockContext::new(10, 0, SystemTime::now());
        retry_context.init_return_values(1);
        transaction
            .lock_keys_with_context(&mut retry_context, ["key".to_owned()])
            .await
            .unwrap();

        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(
            retry_context.value_not_locked(b"key"),
            (Some(b"saved".to_vec()), true)
        );
        let entry = transaction
            .aggressive_locking
            .as_ref()
            .unwrap()
            .current
            .get(&Key::from(b"key".to_vec()))
            .unwrap();
        assert_eq!(entry.value.locked_with_conflict_ts, 0);
        assert_eq!(entry.actual_for_update_ts.version(), 9);
        assert!(matches!(
            transaction.options.kind,
            super::TransactionKind::Pessimistic(ref timestamp) if timestamp.version() == 10
        ));
        transaction.done_aggressive_locking().await.unwrap();
    }

    #[tokio::test]
    async fn source_aggressive_same_stage_and_original_multi_key_exit_semantics() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = requests.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("test only acquires pessimistic locks");
                captured_requests.lock().unwrap().push((
                    request.wake_up_mode,
                    request
                        .mutations
                        .iter()
                        .map(|mutation| mutation.key.clone())
                        .collect::<Vec<_>>(),
                ));
                let response = if request.wake_up_mode
                    == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
                {
                    kvrpcpb::PessimisticLockResponse {
                        results: vec![kvrpcpb::PessimisticLockKeyResult {
                            r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultNormal as i32,
                            existence: true,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }
                } else {
                    kvrpcpb::PessimisticLockResponse::default()
                };
                Ok(Box::new(response) as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.start_aggressive_locking();
        transaction.lock_keys(["a".to_owned()]).await.unwrap();
        let mut repeated = LockContext::new(3, 0, SystemTime::now());
        repeated.init_return_values(1);
        transaction
            .lock_keys_with_context(&mut repeated, ["a".to_owned()])
            .await
            .unwrap();
        assert_eq!(repeated.value_not_locked(b"a"), (None, false));

        transaction
            .lock_keys(["a".to_owned(), "b".to_owned()])
            .await
            .unwrap();

        assert!(!transaction.is_in_aggressive_locking_mode());
        assert!(transaction.buffer.is_locked(&Key::from(b"a".to_vec())));
        assert!(transaction.buffer.is_locked(&Key::from(b"b".to_vec())));
        assert_eq!(
            *requests.lock().unwrap(),
            [
                (
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32,
                    vec![b"a".to_vec()],
                ),
                (
                    kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal as i32,
                    vec![b"b".to_vec()],
                ),
            ]
        );
    }

    #[tokio::test]
    async fn source_aggressive_invalid_conflict_is_retained_for_cancel_cleanup() {
        let rollbacks = Arc::new(Mutex::new(Vec::new()));
        let captured_rollbacks = rollbacks.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                {
                    captured_rollbacks
                        .lock()
                        .unwrap()
                        .push((request.keys.clone(), request.for_update_ts));
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("test only locks or rolls back one key");
                Ok(Box::new(kvrpcpb::PessimisticLockResponse {
                    results: vec![kvrpcpb::PessimisticLockKeyResult {
                        r#type: kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict
                            as i32,
                        existence: true,
                        locked_with_conflict_ts: request.for_update_ts,
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.start_aggressive_locking();
        let error = transaction.lock_keys(["key".to_owned()]).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("LockedWithConflictTS(2) not greater than requested ForUpdateTS(2)"));
        assert!(transaction.is_in_aggressive_locking_stage("key".to_owned()));
        transaction.cancel_aggressive_locking().await.unwrap();
        assert_eq!(*rollbacks.lock().unwrap(), [(vec![b"key".to_vec()], 2)]);
    }

    #[tokio::test]
    async fn source_force_lock_failed_result_retries_the_same_mutation() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = attempts.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("test only sends ForceLock requests");
                assert_eq!(request.mutations[0].key, b"key");
                let attempt = captured_attempts.fetch_add(1, Ordering::SeqCst);
                let result_type = if attempt == 0 {
                    kvrpcpb::PessimisticLockKeyResultType::LockResultFailed
                } else {
                    kvrpcpb::PessimisticLockKeyResultType::LockResultNormal
                };
                Ok(Box::new(kvrpcpb::PessimisticLockResponse {
                    results: vec![kvrpcpb::PessimisticLockKeyResult {
                        r#type: result_type as i32,
                        existence: true,
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.start_aggressive_locking();
        transaction.lock_keys(["key".to_owned()]).await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        transaction.done_aggressive_locking().await.unwrap();
    }

    #[tokio::test]
    async fn source_single_pessimistic_lock_transport_error_rolls_back_the_key() {
        let rollbacks = Arc::new(Mutex::new(Vec::new()));
        let captured_rollbacks = rollbacks.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                {
                    captured_rollbacks
                        .lock()
                        .unwrap()
                        .push(request.keys.clone());
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                assert!(request.is::<kvrpcpb::PessimisticLockRequest>());
                Err(Error::GrpcAPI(tonic::Status::unavailable(
                    "lock response lost",
                )))
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .retry_options(RetryOptions::none())
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        let error = transaction.lock_keys(["key".to_owned()]).await.unwrap_err();
        assert!(matches!(error, Error::GrpcAPI(_)));
        assert_eq!(*rollbacks.lock().unwrap(), [vec![b"key".to_vec()]]);
        assert!(transaction.buffer.get_primary_key().is_none());
    }

    #[tokio::test]
    async fn source_set_and_delete_do_not_implicitly_acquire_pessimistic_locks() {
        let lock_requests = Arc::new(AtomicUsize::new(0));
        let captured_lock_requests = lock_requests.clone();
        let prewrites = Arc::new(Mutex::new(Vec::new()));
        let captured_prewrites = prewrites.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PessimisticLockRequest>() {
                    captured_lock_requests.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured_prewrites.lock().unwrap().push(request.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.put("put".to_owned(), "value").await.unwrap();
        transaction.delete("delete".to_owned()).await.unwrap();
        assert_eq!(lock_requests.load(Ordering::SeqCst), 0);
        transaction.commit().await.unwrap();

        assert!(prewrites.lock().unwrap().iter().all(|request| request
            .pessimistic_actions
            .iter()
            .all(|action| *action
                == kvrpcpb::prewrite_request::PessimisticAction::SkipPessimisticCheck as i32)));
    }

    #[tokio::test]
    async fn source_explicit_pessimistic_rollback_sends_only_acquired_locks() {
        let rollbacks = Arc::new(Mutex::new(Vec::new()));
        let captured_rollbacks = Arc::clone(&rollbacks);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PessimisticLockRequest>() {
                    return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
                }
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                    .expect("explicit rollback sends only PessimisticRollback");
                assert_eq!(
                    request
                        .context
                        .as_ref()
                        .expect("rollback carries a request context")
                        .max_execution_duration_ms,
                    20_000
                );
                captured_rollbacks
                    .lock()
                    .unwrap()
                    .push(request.keys.clone());
                Ok(Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.lock_keys(["locked".to_owned()]).await.unwrap();
        transaction
            .put("unlocked-write".to_owned(), "value")
            .await
            .unwrap();
        transaction.rollback().await.unwrap();

        assert_eq!(*rollbacks.lock().unwrap(), [vec![b"locked".to_vec()]]);
    }

    #[tokio::test]
    async fn source_buffered_writes_select_the_first_sorted_commit_primary() {
        let primary = Arc::new(Mutex::new(None));
        let captured_primary = primary.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    *captured_primary.lock().unwrap() = Some(request.primary_lock.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.put("b".to_owned(), "value").await.unwrap();
        transaction.put("a".to_owned(), "value").await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(*primary.lock().unwrap(), Some(b"a".to_vec()));
    }

    #[tokio::test]
    async fn source_optimistic_local_lock_does_not_select_commit_primary() {
        let primary = Arc::new(Mutex::new(None));
        let captured_primary = primary.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    *captured_primary.lock().unwrap() = Some(request.primary_lock.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.lock_keys(["b".to_owned()]).await.unwrap();
        transaction.put("a".to_owned(), "value").await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(*primary.lock().unwrap(), Some(b"a".to_vec()));
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
    async fn source_mutation_initialization_error_rolls_back_pessimistic_prefix() {
        struct FailOnSecondKey;
        impl super::KvFilter for FailOnSecondKey {
            fn is_unnecessary_key_value(
                &self,
                key: &[u8],
                _value: &[u8],
                _flags: super::MutationFlags,
            ) -> crate::Result<bool> {
                if key == b"b" {
                    Err(crate::Error::StringError("filter failed".to_owned()))
                } else {
                    Ok(false)
                }
            }
        }

        let rollbacks = Arc::new(Mutex::new(Vec::new()));
        let captured_rollbacks = Arc::clone(&rollbacks);
        let batch_rollbacks = Arc::new(AtomicUsize::new(0));
        let captured_batch_rollbacks = Arc::clone(&batch_rollbacks);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                {
                    captured_rollbacks
                        .lock()
                        .unwrap()
                        .push((request.keys.clone(), request.for_update_ts));
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                if request
                    .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                    .is_some()
                {
                    captured_batch_rollbacks.fetch_add(1, Ordering::Relaxed);
                    return Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>);
                }
                panic!("mutation initialization failure must not dispatch prewrite or commit");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .options
            .push_for_update_ts(Timestamp::from_version(9));
        for key in [b"a".as_slice(), b"b".as_slice()] {
            let key = Key::from(key.to_vec());
            transaction
                .buffer
                .put(key.clone(), b"value".to_vec())
                .unwrap();
            transaction.buffer.lock(key);
        }
        transaction.set_kv_filter(Arc::new(FailOnSecondKey));

        assert_eq!(
            transaction.commit().await.unwrap_err().to_string(),
            "filter failed"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !rollbacks.lock().unwrap().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("client-go starts pessimistic rollback after initKeysAndMutations fails");

        assert_eq!(*rollbacks.lock().unwrap(), vec![(vec![b"a".to_vec()], 9)]);
        assert_eq!(batch_rollbacks.load(Ordering::Relaxed), 0);
        assert!(!transaction.prewritten);
    }

    #[tokio::test]
    async fn source_check_not_exists_marks_authoritative_memdb_prewrite_only() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PrewriteRequest>()
                    .expect("check-only transaction sends only Prewrite");
                assert_eq!(request.mutations.len(), 1);
                assert_eq!(request.mutations[0].op, kvrpcpb::Op::CheckNotExists as i32);
                Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
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
            .insert("check".to_owned(), "value")
            .await
            .unwrap();
        transaction.delete("check".to_owned()).await.unwrap();

        // The check-only mutation creates no commit lock, but client-go still
        // allocates and records a commit timestamp after successful prewrite.
        assert_eq!(
            transaction.commit().await.unwrap(),
            Some(Timestamp::from_version(0))
        );
        assert!(transaction
            .get_mem_buffer()
            .get_flags_readonly(b"check")
            .unwrap()
            .has_prewrite_only());
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
        transaction
            .buffer
            .put(key.clone(), b"value".to_vec())
            .unwrap();
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

    #[tokio::test]
    async fn source_server_assertion_failure_rechecks_schema_before_returning() {
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

        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    return Ok(Box::new(kvrpcpb::PrewriteResponse {
                        errors: vec![kvrpcpb::KeyError {
                            assertion_failed: Some(kvrpcpb::AssertionFailed {
                                start_ts: 1,
                                key: b"key".to_vec(),
                                assertion: kvrpcpb::Assertion::Exist as i32,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(50));
        let checker_calls = Arc::new(AtomicUsize::new(0));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.set_schema_version(Arc::new(Version));
        transaction.set_schema_lease_checker(Arc::new(FailingChecker(checker_calls.clone())));
        transaction.put("key".to_owned(), "value").await.unwrap();

        let error = transaction.commit().await.unwrap_err();
        assert!(error.to_string().contains("schema changed"));
        assert_eq!(checker_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_commit_discards_memdb_values_only_after_prewrite_succeeds() {
        let success_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if request.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request while testing successful value discard");
            },
        )));
        success_rpc.set_timestamp(Timestamp::from_version(10));
        let mut committed = Transaction::new(
            Timestamp::from_version(1),
            success_rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        committed.put("key".to_owned(), "value").await.unwrap();
        committed.commit().await.unwrap();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = committed.get_mem_buffer().get(b"key");
        }))
        .is_err());

        let failure_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
                    return Ok(Box::new(kvrpcpb::PrewriteResponse {
                        errors: vec![kvrpcpb::KeyError {
                            retryable: "retry transaction".to_owned(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut failed = Transaction::new(
            Timestamp::from_version(1),
            failure_rpc,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        failed.put("key".to_owned(), "value").await.unwrap();
        assert!(matches!(
            failed.commit().await.unwrap_err(),
            Error::RetryableKey(_)
        ));
        assert_eq!(failed.get_mem_buffer().get(b"key").unwrap(), b"value");
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
    fn source_snapshot_timestamp_reset_keeps_start_and_discards_only_resolved_lock_hints() {
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.read_lock_context.add_resolved(11);
        transaction.read_lock_context.add_committed(12);

        transaction.set_snapshot_timestamp(Timestamp::from_version(2));

        assert_eq!(transaction.start_timestamp().version(), 1);
        assert_eq!(transaction.snapshot_timestamp.version(), 2);
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
    #[cfg_attr(
        feature = "nextgen",
        ignore = "client-go skips return-commit-TS snapshot reads in NextGen"
    )]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_test_TestGetAndBatchGetWithReturnCommitTS() {
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

    fn assert_snapshot_return_commit_ts_rejects_unknown_nonempty_entries() {
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

    async fn assert_point_get_caches_value_before_missing_commit_ts_error() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::GetRequest>());
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::GetResponse {
                    value: b"value".to_vec(),
                    commit_ts: 0,
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
            transaction
                .get_with_options("key".to_owned(), &[GetOption::ReturnCommitTs])
                .await
                .unwrap_err()
                .to_string(),
            "commit timestamp is required but not returned"
        );
        assert_eq!(
            transaction.get("key".to_owned()).await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    }

    async fn assert_batch_get_does_not_cache_a_missing_commit_ts_response() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::BatchGetRequest>()
                    .expect("commit-ts batch test dispatches BatchGet");
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::BatchGetResponse {
                    pairs: vec![kvrpcpb::KvPair {
                        key: request.keys[0].clone(),
                        value: b"value".to_vec(),
                        commit_ts: 0,
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

        assert_eq!(
            transaction
                .batch_get_with_options(vec!["key".to_owned()], &[GetOption::ReturnCommitTs])
                .await
                .unwrap_err()
                .to_string(),
            "commit timestamp is required but not returned"
        );
        assert_eq!(
            transaction
                .batch_get(vec!["key".to_owned()])
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            [KvPair(Key::from("key".to_owned()), b"value".to_vec())]
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_fail_test_TestResetSnapshotTS() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let versions = Arc::new(Mutex::new(Vec::new()));
        let captured_versions = Arc::clone(&versions);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request.downcast_ref::<kvrpcpb::GetRequest>().unwrap();
                captured_versions.lock().unwrap().push(request.version);
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
        assert_eq!(transaction.start_timestamp().version(), 1);
        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
        assert_eq!(*versions.lock().unwrap(), [1, 2]);
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

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_test_TestSnapshotCacheBypassMaxUint64() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let sequence = captured_dispatches.fetch_add(1, Ordering::SeqCst) + 1;
                if request.is::<kvrpcpb::GetRequest>() {
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: vec![sequence as u8],
                        commit_ts: sequence as u64,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    return Ok(Box::new(kvrpcpb::BatchGetResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: request.keys[0].clone(),
                            value: vec![sequence as u8],
                            commit_ts: sequence as u64,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected latest-snapshot request")
            },
        )));
        let transaction = Transaction::new(
            Timestamp::from_version(u64::MAX),
            pd_client,
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let mut snapshot = crate::Snapshot::new(transaction);

        assert_eq!(snapshot.get("key".to_owned()).await.unwrap(), Some(vec![1]));
        assert_eq!(snapshot.get("key".to_owned()).await.unwrap(), Some(vec![2]));
        assert_eq!(
            snapshot
                .get_with_options("get-options".to_owned(), &[GetOption::ReturnCommitTs])
                .await
                .unwrap(),
            Some(ValueEntry::new(vec![3], 3))
        );
        assert_eq!(
            snapshot
                .get_with_options("get-options".to_owned(), &[GetOption::ReturnCommitTs])
                .await
                .unwrap(),
            Some(ValueEntry::new(vec![4], 4))
        );
        assert_eq!(
            snapshot
                .batch_get(vec!["batch".to_owned()])
                .await
                .unwrap()
                .next()
                .unwrap()
                .1,
            vec![5]
        );
        assert_eq!(
            snapshot
                .batch_get(vec!["batch".to_owned()])
                .await
                .unwrap()
                .next()
                .unwrap()
                .1,
            vec![6]
        );
        assert_eq!(
            snapshot
                .batch_get_with_options(
                    vec!["batch-options".to_owned()],
                    &[GetOption::ReturnCommitTs],
                )
                .await
                .unwrap()
                .values()
                .next(),
            Some(&ValueEntry::new(vec![7], 7))
        );
        assert_eq!(
            snapshot
                .batch_get_with_options(
                    vec!["batch-options".to_owned()],
                    &[GetOption::ReturnCommitTs],
                )
                .await
                .unwrap()
                .values()
                .next(),
            Some(&ValueEntry::new(vec![8], 8))
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_test_TestSnapshotThreadSafe() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<crate::Snapshot<MockPdClient>>();

        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: (request.key == b"key")
                            .then(|| b"x".to_vec())
                            .unwrap_or_default(),
                        not_found: request.key != b"key",
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    let pairs = request
                        .keys
                        .iter()
                        .filter(|key| key.as_slice() == b"key")
                        .map(|key| kvrpcpb::KvPair {
                            key: key.clone(),
                            value: b"x".to_vec(),
                            ..Default::default()
                        })
                        .collect();
                    return Ok(Box::new(kvrpcpb::BatchGetResponse {
                        pairs,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("snapshot thread-safety workload sent an unexpected request");
            },
        )));
        let transaction = Transaction::new(
            Timestamp::from_version(u64::MAX),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let snapshot = Arc::new(tokio::sync::Mutex::new(crate::Snapshot::new(transaction)));

        let tasks = (0..5)
            .map(|_| {
                let snapshot = Arc::clone(&snapshot);
                tokio::spawn(async move {
                    for _ in 0..30 {
                        assert_eq!(
                            snapshot.lock().await.get(b"key".to_vec()).await?,
                            Some(b"x".to_vec())
                        );
                        let entries = snapshot
                            .lock()
                            .await
                            .batch_get([b"key".to_vec(), b"missing".to_vec()])
                            .await?
                            .map(Into::<(Key, Value)>::into)
                            .collect::<BTreeMap<_, _>>();
                        assert_eq!(
                            entries,
                            BTreeMap::from([(Key::from(b"key".to_vec()), b"x".to_vec())])
                        );
                    }
                    Ok::<(), Error>(())
                })
            })
            .collect::<Vec<_>>();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
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
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_fail_test_TestRetryMaxTsPointGetSkipLock() {
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
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_test_TestPointGetSkipTxnLock() {
        let get_attempts = Arc::new(AtomicUsize::new(0));
        let status_checks = Arc::new(AtomicUsize::new(0));
        let captured_get_attempts = Arc::clone(&get_attempts);
        let captured_status_checks = Arc::clone(&status_checks);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    if captured_get_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        assert!(request.context.as_ref().unwrap().committed_locks.is_empty());
                        return Ok(Box::new(kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: b"secondary".to_vec(),
                                    primary_lock: b"primary".to_vec(),
                                    lock_version: 1,
                                    lock_ttl: 3_000,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    assert_eq!(request.context.as_ref().unwrap().committed_locks, [1]);
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"y".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    captured_status_checks.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(request.lock_ts, 1);
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected point-get lock-skip request");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(u64::MAX),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        assert_eq!(
            transaction.get(b"secondary".to_vec()).await.unwrap(),
            Some(b"y".to_vec())
        );
        assert_eq!(get_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(status_checks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_fail_test_TestRetryPointGetResolveTS() {
        let get_attempts = Arc::new(AtomicUsize::new(0));
        let captured_get_attempts = Arc::clone(&get_attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    if captured_get_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Ok(Box::new(kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: b"k2".to_vec(),
                                    primary_lock: b"k1".to_vec(),
                                    lock_version: 5,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    assert_eq!(request.context.as_ref().unwrap().committed_locks, [5]);
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"v2".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    assert_eq!(request.lock_ts, 5);
                    assert_eq!(request.caller_start_ts, u64::MAX);
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 6,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected resolve-TS point-get request");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(u64::MAX),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        assert_eq!(
            transaction.get(b"k2".to_vec()).await.unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(get_attempts.load(Ordering::SeqCst), 2);
    }

    #[cfg(not(feature = "nextgen"))]
    #[tokio::test]
    async fn source_batch_get_retries_only_pair_locked_keys_and_keeps_clean_pairs() {
        let batch_attempts = Arc::new(Mutex::new(Vec::<Vec<Vec<u8>>>::new()));
        let captured_batch_attempts = Arc::clone(&batch_attempts);
        let adjustment_counts = Arc::new(Mutex::new(Vec::new()));
        let client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                let mut attempts = captured_batch_attempts.lock().unwrap();
                attempts.push(request.keys.clone());
                if attempts.len() == 1 {
                    assert_eq!(
                        request
                            .context
                            .as_ref()
                            .unwrap()
                            .peer
                            .as_ref()
                            .unwrap()
                            .store_id,
                        42
                    );
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
                assert_eq!(
                    request
                        .context
                        .as_ref()
                        .unwrap()
                        .peer
                        .as_ref()
                        .unwrap()
                        .store_id,
                    41
                );
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
        });
        let mut region = MockPdClient::region1();
        let leader = crate::proto::metapb::Peer {
            id: 1,
            store_id: 41,
            ..Default::default()
        };
        let follower = crate::proto::metapb::Peer {
            id: 2,
            store_id: 42,
            ..Default::default()
        };
        region.leader = Some(leader.clone());
        region.region.peers = vec![leader, follower];
        region.region.end_key.clear();
        let pd_client = Arc::new(MockPdClient::with_client_and_regions(client, vec![region]));
        let mut transaction = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_replica_read_config(ReplicaReadConfig {
            read_type: ReplicaReadType::Follower,
            ..Default::default()
        });
        let captured_adjustment_counts = Arc::clone(&adjustment_counts);
        transaction.set_replica_read_adjuster(Arc::new(move |item_count| {
            captured_adjustment_counts.lock().unwrap().push(item_count);
            ReplicaReadAdjustment::new(
                None,
                if item_count == 2 {
                    ReplicaReadType::Mixed
                } else {
                    ReplicaReadType::Leader
                },
            )
        }));

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
        assert_eq!(*adjustment_counts.lock().unwrap(), [2, 1]);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_fail_test_TestBatchGetResponseKeyError() {
        let attempts = Arc::new(Mutex::new(Vec::<Vec<Vec<u8>>>::new()));
        let captured_attempts = Arc::clone(&attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    let mut attempts = captured_attempts.lock().unwrap();
                    attempts.push(request.keys.clone());
                    if attempts.len() == 1 {
                        return Ok(Box::new(kvrpcpb::BatchGetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: b"locked".to_vec(),
                                    primary_lock: b"locked".to_vec(),
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
                    assert_eq!(request.keys, [b"clean".to_vec(), b"locked".to_vec()]);
                    assert_eq!(request.context.as_ref().unwrap().committed_locks, [1]);
                    return Ok(Box::new(kvrpcpb::BatchGetResponse {
                        pairs: vec![
                            kvrpcpb::KvPair {
                                key: b"clean".to_vec(),
                                value: b"clean-value".to_vec(),
                                ..Default::default()
                            },
                            kvrpcpb::KvPair {
                                key: b"locked".to_vec(),
                                value: b"locked-value".to_vec(),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected response-level BatchGet retry request");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        let entries = transaction
            .batch_get([b"clean".to_vec(), b"locked".to_vec()])
            .await
            .unwrap()
            .map(Into::<(Key, Value)>::into)
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            entries,
            BTreeMap::from([
                (Key::from(b"clean".to_vec()), b"clean-value".to_vec()),
                (Key::from(b"locked".to_vec()), b"locked-value".to_vec()),
            ])
        );
        assert_eq!(attempts.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_split_test_TestBatchGetUsingAsyncAPI() {
        let mut first_region = MockPdClient::region1();
        first_region.region.start_key.clear();
        first_region.region.end_key = b"m".to_vec();
        let mut second_region = MockPdClient::region2();
        second_region.region.start_key = b"m".to_vec();
        second_region.region.end_key.clear();
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            let request = request
                .downcast_ref::<kvrpcpb::BatchGetRequest>()
                .expect("multi-region BatchGet sends BatchGetRequest");
            captured_dispatches.fetch_add(1, Ordering::SeqCst);
            let pairs = request
                .keys
                .iter()
                .filter(|key| key.as_slice() == b"a" || key.as_slice() == b"b")
                .map(|key| kvrpcpb::KvPair {
                    key: key.clone(),
                    value: [key.as_slice(), b"-value"].concat(),
                    ..Default::default()
                })
                .collect();
            Ok(Box::new(kvrpcpb::BatchGetResponse {
                pairs,
                ..Default::default()
            }) as Box<dyn Any>)
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
            .map(Into::<(Key, Value)>::into)
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            pairs,
            BTreeMap::from([(Key::from(b"a".to_vec()), b"a-value".to_vec())])
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 2);
        assert_eq!(crate::stats::async_batch_get_count("ok"), before);

        transaction.set_enable_async_batch_get(true);
        let pairs = transaction
            .batch_get([b"b".to_vec(), b"o".to_vec()])
            .await
            .unwrap()
            .map(Into::<(Key, Value)>::into)
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            pairs,
            BTreeMap::from([(Key::from(b"b".to_vec()), b"b-value".to_vec())])
        );
        assert_eq!(dispatches.load(Ordering::SeqCst), 4);
        assert!(crate::stats::async_batch_get_count("ok") >= before + 2);
    }

    #[cfg(not(feature = "nextgen"))]
    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_test_TestReplicaReadAdjuster() {
        let mut first_region = MockPdClient::region1();
        first_region.region.start_key.clear();
        first_region.region.end_key = b"m".to_vec();
        let mut second_region = MockPdClient::region2();
        second_region.region.start_key = b"m".to_vec();
        second_region.region.end_key.clear();
        let client = MockKvClient::with_dispatch_hook(|request: &dyn Any| {
            assert!(request.is::<kvrpcpb::BatchGetRequest>());
            Ok(Box::new(kvrpcpb::BatchGetResponse::default()) as Box<dyn Any>)
        });
        let pd_client = Arc::new(MockPdClient::with_client_and_regions(
            client,
            vec![first_region, second_region],
        ));
        let adjustment_counts = Arc::new(Mutex::new(Vec::new()));
        let captured_counts = Arc::clone(&adjustment_counts);
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_replica_read_config(ReplicaReadConfig {
            read_type: ReplicaReadType::Follower,
            ..Default::default()
        });
        transaction.set_replica_read_adjuster(Arc::new(move |item_count| {
            captured_counts.lock().unwrap().push(item_count);
            ReplicaReadAdjustment::new(None, ReplicaReadType::Leader)
        }));

        assert!(transaction
            .batch_get([b"a".to_vec(), b"n".to_vec()])
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .is_empty());
        let mut adjustment_counts = adjustment_counts.lock().unwrap().clone();
        adjustment_counts.sort_unstable();
        assert_eq!(adjustment_counts, [1, 1]);
    }

    #[cfg(feature = "nextgen")]
    #[tokio::test]
    #[ignore = "client-go skips replica-read adjustment in NextGen"]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_test_TestReplicaReadAdjuster() {}

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
    async fn source_empty_snapshot_values_are_missing_but_buffer_deletes_are_retained() {
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                if request.is::<kvrpcpb::GetRequest>() {
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: Vec::new(),
                        not_found: false,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    return Ok(Box::new(kvrpcpb::BatchGetResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: request.keys[0].clone(),
                            value: Vec::new(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BufferBatchGetRequest>() {
                    return Ok(Box::new(kvrpcpb::BufferBatchGetResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: request.keys[0].clone(),
                            value: Vec::new(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected request while testing empty snapshot values")
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );

        assert_eq!(transaction.get("get-empty".to_owned()).await.unwrap(), None);
        assert!(transaction
            .batch_get(["batch-empty".to_owned()])
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .is_empty());

        transaction.set_snapshot_pipelined(1);
        assert_eq!(
            transaction
                .batch_get_from_buffer(["buffer-delete".to_owned()])
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            [KvPair(Key::from("buffer-delete".to_owned()), Vec::new())]
        );
        assert_eq!(
            transaction
                .batch_get_from_buffer_with_options(
                    ["buffer-delete-with-options".to_owned()],
                    &[GetOption::ReturnCommitTs],
                )
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            [KvPair(
                Key::from("buffer-delete-with-options".to_owned()),
                Vec::new(),
            )]
        );
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
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_scan_test_TestScan() {
        fn key(index: usize) -> Vec<u8> {
            format!("k{index:04}").into_bytes()
        }

        for row_count in [
            1,
            DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE as usize,
            DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE as usize + 1,
            DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE as usize * 3,
        ] {
            let client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::ScanRequest>()
                    .expect("the external scan matrix only sends ScanRequest");
                assert!(request.limit <= DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE);
                let mut pairs = (0..row_count)
                    .filter_map(|index| {
                        let key = key(index);
                        let in_range = if request.reverse {
                            key < request.start_key
                                && (request.end_key.is_empty() || key >= request.end_key)
                        } else {
                            key >= request.start_key
                                && (request.end_key.is_empty() || key < request.end_key)
                        };
                        in_range.then(|| kvrpcpb::KvPair {
                            key,
                            value: (!request.key_only)
                                .then(|| index.to_string().into_bytes())
                                .unwrap_or_default(),
                            ..Default::default()
                        })
                    })
                    .collect::<Vec<_>>();
                if request.reverse {
                    pairs.reverse();
                }
                pairs.truncate(request.limit as usize);
                Ok(Box::new(kvrpcpb::ScanResponse {
                    pairs,
                    ..Default::default()
                }) as Box<dyn Any>)
            });

            let mut first = MockPdClient::region1();
            first.region.start_key.clear();
            first.region.end_key = (row_count > 123).then(|| key(123)).unwrap_or_default();
            let mut regions = vec![first];
            if row_count > 123 {
                let mut second = MockPdClient::region2();
                second.region.start_key = key(123);
                second.region.end_key = (row_count > 456).then(|| key(456)).unwrap_or_default();
                regions.push(second);
            }
            if row_count > 456 {
                let mut third = MockPdClient::region3();
                third.region.start_key = key(456);
                third.region.end_key.clear();
                regions.push(third);
            }
            let pd_client = Arc::new(MockPdClient::with_client_and_regions(client, regions));
            let transaction = Transaction::new(
                Timestamp::from_version(1),
                pd_client,
                TransactionOptions::new_optimistic().read_only(),
                Keyspace::Disable,
            );
            let mut snapshot = crate::Snapshot::new(transaction);
            snapshot.set_scan_batch_size(DEFAULT_SNAPSHOT_SCAN_BATCH_SIZE);

            let pairs = snapshot
                .scan(key(0)..b"z".to_vec(), row_count as u32)
                .await
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(pairs.len(), row_count);
            for (index, pair) in pairs.iter().enumerate() {
                assert_eq!(pair.key(), &Key::from(key(index)));
                assert_eq!(pair.value(), index.to_string().as_bytes());
            }

            let upper = row_count / 2;
            let pairs = snapshot
                .scan(key(0)..key(upper), row_count as u32)
                .await
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(pairs.len(), upper);

            snapshot.set_key_only(true);
            let pairs = snapshot
                .scan(key(0)..b"z".to_vec(), row_count as u32)
                .await
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(pairs.len(), row_count);
            assert!(pairs.iter().all(|pair| pair.value().is_empty()));

            snapshot.set_key_only(false);
            let pair = snapshot
                .scan(key(0)..key(1), 1)
                .await
                .unwrap()
                .next()
                .expect("restoring key-only must restore values");
            assert_eq!(pair.value(), b"0");

            snapshot.set_scan_batch_size(10);
            let pairs = snapshot
                .scan_reverse(key(0)..b"z".to_vec(), row_count as u32)
                .await
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(pairs.len(), row_count);
            for (pair, index) in pairs.iter().zip((0..row_count).rev()) {
                assert_eq!(pair.key(), &Key::from(key(index)));
                assert_eq!(pair.value(), index.to_string().as_bytes());
            }
        }
    }

    fn source_alphabet_snapshot() -> crate::Snapshot<MockPdClient> {
        let client = MockKvClient::with_dispatch_hook(|request: &dyn Any| {
            let request = request
                .downcast_ref::<kvrpcpb::ScanRequest>()
                .expect("alphabet fixture only dispatches ScanRequest");
            let mut pairs = (b'a'..=b'z')
                .filter_map(|character| {
                    let key = vec![character];
                    let in_range = if request.reverse {
                        key < request.start_key
                            && (request.end_key.is_empty() || key >= request.end_key)
                    } else {
                        key >= request.start_key
                            && (request.end_key.is_empty() || key < request.end_key)
                    };
                    in_range.then(|| kvrpcpb::KvPair {
                        key,
                        value: vec![character],
                        ..Default::default()
                    })
                })
                .collect::<Vec<_>>();
            if request.reverse {
                pairs.reverse();
            }
            pairs.truncate(request.limit as usize);
            Ok(Box::new(kvrpcpb::ScanResponse {
                pairs,
                ..Default::default()
            }) as Box<dyn Any>)
        });
        let mut first = MockPdClient::region1();
        first.region.start_key.clear();
        first.region.end_key = b"m".to_vec();
        let mut second = MockPdClient::region2();
        second.region.start_key = b"m".to_vec();
        second.region.end_key.clear();
        let transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::with_client_and_regions(
                client,
                vec![first, second],
            )),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        let mut snapshot = crate::Snapshot::new(transaction);
        snapshot.set_scan_batch_size(10);
        snapshot
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_scan_mock_test_TestScanMultipleRegions() {
        let mut snapshot = source_alphabet_snapshot();
        let pairs = snapshot
            .scan(b"a".to_vec()..Vec::<u8>::new(), 26)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            (b'a'..=b'z')
                .map(|character| KvPair(vec![character].into(), vec![character]))
                .collect::<Vec<_>>()
        );

        let pairs = snapshot
            .scan(b"a".to_vec()..b"i".to_vec(), 26)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            (b'a'..b'i')
                .map(|character| KvPair(vec![character].into(), vec![character]))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_scan_mock_test_TestReverseScan() {
        let mut snapshot = source_alphabet_snapshot();
        let pairs = snapshot
            .scan_reverse(Vec::<u8>::new()..b"z".to_vec(), 26)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            (b'a'..b'z')
                .rev()
                .map(|character| KvPair(vec![character].into(), vec![character]))
                .collect::<Vec<_>>()
        );

        let pairs = snapshot
            .scan_reverse(b"a".to_vec()..b"i".to_vec(), 26)
            .await
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(
            pairs,
            (b'a'..b'i')
                .rev()
                .map(|character| KvPair(vec![character].into(), vec![character]))
                .collect::<Vec<_>>()
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
            iterator.next().await.unwrap_err().to_string(),
            "scanner iterator is invalid"
        );
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
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_fail_test_TestScanResponseKeyError() {
        let scan_attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = Arc::clone(&scan_attempts);
        let resolve_requests = Arc::new(AtomicUsize::new(0));
        let captured_resolve_requests = Arc::clone(&resolve_requests);
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
                                    primary_lock: b"primary".to_vec(),
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
                    assert!(context.committed_locks.is_empty());
                    assert!(context.resolved_locks.is_empty());
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
                if request.is::<kvrpcpb::ResolveLockRequest>() {
                    captured_resolve_requests.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()) as Box<dyn Any>);
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
        assert_eq!(resolve_requests.load(Ordering::SeqCst), 1);
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
    fn source_snapshot_scanner_inherits_only_read_type_and_busy_threshold() {
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_replica_read_config(ReplicaReadConfig {
            read_type: ReplicaReadType::Mixed,
            leader_only: true,
            prefer_leader: true,
            stale_read: true,
            labels: vec![crate::proto::metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "east".to_owned(),
            }],
            stores: vec![7, 8],
            busy_threshold_ms: 73,
        });

        assert_eq!(
            transaction.snapshot_scanner_replica_read_config(),
            ReplicaReadConfig {
                read_type: ReplicaReadType::Mixed,
                busy_threshold_ms: 73,
                ..Default::default()
            }
        );
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
    async fn source_lock_retries_drop_the_configurable_snapshot_read_timeout() {
        let get_attempts = Arc::new(AtomicUsize::new(0));
        let batch_attempts = Arc::new(AtomicUsize::new(0));
        let captured_get_attempts = Arc::clone(&get_attempts);
        let captured_batch_attempts = Arc::clone(&batch_attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    let attempt = captured_get_attempts.fetch_add(1, Ordering::SeqCst);
                    let context = request.context.as_ref().unwrap();
                    if attempt == 0 {
                        assert_eq!(context.max_execution_duration_ms, 17);
                        assert!(!context.is_retry_request);
                        return Ok(Box::new(kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: b"get".to_vec(),
                                    primary_lock: b"get".to_vec(),
                                    lock_version: 1,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    assert_eq!(context.max_execution_duration_ms, 30_000);
                    assert!(context.is_retry_request);
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    let attempt = captured_batch_attempts.fetch_add(1, Ordering::SeqCst);
                    let context = request.context.as_ref().unwrap();
                    if attempt == 0 {
                        assert_eq!(context.max_execution_duration_ms, 17);
                        assert!(!context.is_retry_request);
                        return Ok(Box::new(kvrpcpb::BatchGetResponse {
                            pairs: vec![kvrpcpb::KvPair {
                                key: b"batch".to_vec(),
                                error: Some(kvrpcpb::KeyError {
                                    locked: Some(kvrpcpb::LockInfo {
                                        key: b"batch".to_vec(),
                                        primary_lock: b"batch".to_vec(),
                                        lock_version: 2,
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    assert_eq!(context.max_execution_duration_ms, 60_000);
                    assert!(context.is_retry_request);
                    return Ok(Box::new(kvrpcpb::BatchGetResponse::default()) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: request.lock_ts + 10,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected request while retrying snapshot locks");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(20),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_snapshot_read_timeout(Duration::from_millis(17));

        assert_eq!(transaction.get("get".to_owned()).await.unwrap(), None);
        assert!(transaction
            .batch_get(vec!["batch".to_owned()])
            .await
            .unwrap()
            .next()
            .is_none());
        assert_eq!(get_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(batch_attempts.load(Ordering::SeqCst), 2);
    }

    #[cfg(not(feature = "nextgen"))]
    #[tokio::test]
    async fn source_stale_snapshot_lock_retry_is_a_threshold_free_leader_read() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = Arc::clone(&attempts);
        let client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                let context = request.context.as_ref().unwrap();
                if captured_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    assert!(context.stale_read);
                    assert_eq!(context.busy_threshold_ms, 50);
                    assert_eq!(context.peer.as_ref().unwrap().store_id, 42);
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        error: Some(kvrpcpb::KeyError {
                            locked: Some(kvrpcpb::LockInfo {
                                key: b"key".to_vec(),
                                primary_lock: b"key".to_vec(),
                                lock_version: 1,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                assert!(!context.stale_read);
                assert_eq!(context.busy_threshold_ms, 0);
                assert_eq!(context.peer.as_ref().unwrap().store_id, 41);
                return Ok(Box::new(kvrpcpb::GetResponse {
                    not_found: true,
                    ..Default::default()
                }) as Box<dyn Any>);
            }
            if request.is::<kvrpcpb::CheckTxnStatusRequest>() {
                return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                    commit_version: 2,
                    ..Default::default()
                }) as Box<dyn Any>);
            }
            panic!("unexpected request while retrying a stale snapshot lock");
        });
        let mut region = MockPdClient::region1();
        let leader = crate::proto::metapb::Peer {
            id: 1,
            store_id: 41,
            ..Default::default()
        };
        let follower = crate::proto::metapb::Peer {
            id: 2,
            store_id: 42,
            ..Default::default()
        };
        region.leader = Some(leader.clone());
        region.region.peers = vec![leader, follower];
        region.region.end_key.clear();
        let pd_client = Arc::new(MockPdClient::with_client_and_regions(client, vec![region]));
        let mut transaction = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        transaction.set_replica_read_config(ReplicaReadConfig {
            read_type: ReplicaReadType::Mixed,
            stale_read: true,
            busy_threshold_ms: 50,
            ..Default::default()
        });

        assert_eq!(transaction.get("key".to_owned()).await.unwrap(), None);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
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
        transaction.set_snapshot_resource_group_tagger(Some(Arc::new(move |request_type| {
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
    async fn source_transaction_resource_group_tagger_covers_reads_writes_and_static_precedence() {
        let observed_tags = Arc::new(Mutex::new(Vec::new()));
        let captured_tags = observed_tags.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let (name, context, response): (&str, &kvrpcpb::Context, Box<dyn Any>) =
                    if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                        (
                            "get",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::GetResponse::default()),
                        )
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>()
                    {
                        (
                            "batch-get",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::BatchGetResponse::default()),
                        )
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                        (
                            "scan",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::ScanResponse::default()),
                        )
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>()
                    {
                        (
                            "prewrite",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::PrewriteResponse::default()),
                        )
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                        (
                            "commit",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::CommitResponse::default()),
                        )
                    } else {
                        panic!(
                            "unexpected request while testing transaction resource-group tagger"
                        );
                    };
                captured_tags
                    .lock()
                    .unwrap()
                    .push((name, context.resource_group_tag.clone()));
                Ok(response)
            },
        )));
        pd_client.set_timestamp(Timestamp::from_version(10));

        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let captured_tagger_calls = tagger_calls.clone();
        let tagger: super::TransactionResourceGroupTagger = Arc::new(move |request| {
            captured_tagger_calls.fetch_add(1, Ordering::SeqCst);
            request.set_resource_group_tag(b"dynamic".to_vec());
        });
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client.clone(),
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.set_resource_group_tagger(Some(tagger.clone()));
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
        transaction.put("write".to_owned(), "value").await.unwrap();
        transaction.commit().await.unwrap();

        let mut static_transaction = Transaction::new(
            Timestamp::from_version(2),
            pd_client,
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        static_transaction.set_resource_group_tag(Some(b"static".to_vec()));
        static_transaction.set_resource_group_tagger(Some(tagger));
        static_transaction.get("static".to_owned()).await.unwrap();

        assert_eq!(tagger_calls.load(Ordering::SeqCst), 5);
        assert_eq!(
            *observed_tags.lock().unwrap(),
            vec![
                ("get", b"dynamic".to_vec()),
                ("batch-get", b"dynamic".to_vec()),
                ("scan", b"dynamic".to_vec()),
                ("prewrite", b"dynamic".to_vec()),
                ("commit", b"dynamic".to_vec()),
                ("get", b"static".to_vec()),
            ]
        );
    }

    #[test]
    fn source_transaction_resource_group_tagger_recomputes_after_reshard() {
        let calls = Arc::new(AtomicUsize::new(0));
        let captured_calls = Arc::clone(&calls);
        let tagger: super::TransactionResourceGroupTagger = Arc::new(move |request| {
            captured_calls.fetch_add(1, Ordering::SeqCst);
            let request = request
                .as_any_mut()
                .downcast_mut::<kvrpcpb::PrewriteRequest>()
                .expect("resharded transaction request remains a Prewrite");
            let tag = request.mutations[0].key.clone();
            request
                .context
                .get_or_insert_with(kvrpcpb::Context::default)
                .resource_group_tag = tag;
        });
        let mut request = kvrpcpb::PrewriteRequest {
            context: Some(kvrpcpb::Context::default()),
            mutations: vec![source_test_mutation(vec![1], kvrpcpb::Op::Put)],
            ..Default::default()
        };

        super::apply_transaction_resource_group_tagger(&mut request, false, Some(&tagger));
        assert_eq!(
            request.context.as_ref().unwrap().resource_group_tag,
            vec![1]
        );

        // An EpochNotMatch re-shard clones the already decorated physical
        // request before replacing its mutations. The new physical batch must
        // not inherit the old batch's dynamic tag.
        request.mutations = vec![source_test_mutation(vec![20], kvrpcpb::Op::Put)];
        super::apply_transaction_resource_group_tagger(&mut request, false, Some(&tagger));
        assert_eq!(
            request.context.as_ref().unwrap().resource_group_tag,
            vec![20]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        request.context.as_mut().unwrap().resource_group_tag = b"static".to_vec();
        super::apply_transaction_resource_group_tagger(&mut request, true, Some(&tagger));
        assert_eq!(
            request.context.as_ref().unwrap().resource_group_tag,
            b"static"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    async fn assert_snapshot_buffer_batch_get_requires_pipelined_mode() {
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

        assert!(transaction
            .batch_get_from_buffer(Vec::<String>::new())
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .is_empty());
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
                (7, false, String::new()),
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

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_safepoint_test_TestSafePoint() {
        struct SafePointValidator(Arc<crate::tikv::TxnSafePointCache>);

        #[async_trait::async_trait]
        impl crate::SnapshotVisibilityValidator for SafePointValidator {
            async fn check_visibility(&self, start_timestamp: u64) -> crate::Result<()> {
                self.0.check_visibility(start_timestamp)
            }
        }

        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        value: format!("value-{}", String::from_utf8_lossy(&request.key))
                            .into_bytes(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if request.downcast_ref::<kvrpcpb::ScanRequest>().is_some() {
                    return Ok(Box::<kvrpcpb::ScanResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    return Ok(Box::new(kvrpcpb::BatchGetResponse {
                        pairs: request
                            .keys
                            .iter()
                            .map(|key| kvrpcpb::KvPair {
                                key: key.clone(),
                                value: b"value".to_vec(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected safe-point request")
            },
        )));
        let cache = Arc::new(crate::tikv::TxnSafePointCache::new(0, SystemTime::now()));
        let make_transaction = |start_timestamp| {
            let mut transaction = Transaction::new(
                Timestamp::from_version(start_timestamp),
                rpc.clone(),
                TransactionOptions::new_optimistic()
                    .read_only()
                    .drop_check(CheckLevel::None),
                Keyspace::Disable,
            );
            transaction
                .set_snapshot_visibility_validator(Arc::new(SafePointValidator(cache.clone())));
            transaction
        };

        let key = b"~safepoint/key00000000".to_vec();
        let mut get_transaction = make_transaction(100);
        assert!(get_transaction.get(key.clone()).await.unwrap().is_some());
        cache.update(110, SystemTime::now());
        get_transaction.clean_snapshot_cache([Key::from(key.clone())]);
        assert!(matches!(
            get_transaction.get(key.clone()).await.unwrap_err(),
            Error::TransactionAbortedByGc(_)
        ));

        let mut scan_transaction = make_transaction(200);
        cache.update(210, SystemTime::now());
        let scan_error = match scan_transaction.scan(b"~safepoint/".to_vec().., 10).await {
            Ok(_) => panic!("scan older than the safe point must fail"),
            Err(error) => error,
        };
        assert!(matches!(scan_error, Error::TransactionAbortedByGc(_)));

        let mut batch_transaction = make_transaction(300);
        cache.update(310, SystemTime::now());
        let batch_error = match batch_transaction
            .batch_get((0..10).map(|index| format!("~safepoint/key{index:08}")))
            .await
        {
            Ok(_) => panic!("batch get older than the safe point must fail"),
            Err(error) => error,
        };
        assert!(matches!(batch_error, Error::TransactionAbortedByGc(_)));
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

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_store_test_TestRequestPriority() {
        let expected = Arc::new(std::sync::atomic::AtomicI32::new(
            kvrpcpb::CommandPri::High as i32,
        ));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_expected = expected.clone();
        let captured_requests = requests.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let (kind, priority) =
                    if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                        ("prewrite", request.context.as_ref().unwrap().priority)
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                        ("commit", request.context.as_ref().unwrap().priority)
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                        ("get", request.context.as_ref().unwrap().priority)
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                        ("scan", request.context.as_ref().unwrap().priority)
                    } else {
                        panic!("unexpected priority-test request")
                    };
                captured_requests.lock().unwrap().push((kind, priority));
                let expected = captured_expected.load(Ordering::SeqCst);
                if kind == "get" && priority != expected {
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        error: Some(kvrpcpb::KeyError {
                            abort: "request check error".to_owned(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                assert_eq!(priority, expected, "{kind}");
                match kind {
                    "prewrite" => Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>),
                    "commit" => Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>),
                    "get" => Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"value".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>),
                    "scan" => Ok(Box::<kvrpcpb::ScanResponse>::default() as Box<dyn Any>),
                    _ => unreachable!(),
                }
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(10));
        let key = b"~store/request_priority_key".to_vec();

        let mut write = Transaction::new(
            Timestamp::from_version(1),
            rpc.clone(),
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        write.set_priority(Priority::High);
        write.put(key.clone(), b"value".to_vec()).await.unwrap();
        Box::pin(write.commit()).await.unwrap();

        let mut read = Transaction::new(
            Timestamp::from_version(2),
            rpc,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        expected.store(kvrpcpb::CommandPri::Low as i32, Ordering::SeqCst);
        read.set_priority(Priority::Low);
        assert_eq!(
            read.get(key.clone()).await.unwrap(),
            Some(b"value".to_vec())
        );

        read.clean_snapshot_cache([Key::from(key.clone())]);
        read.set_priority(Priority::Normal);
        assert!(read.get(key.clone()).await.is_err());

        expected.store(kvrpcpb::CommandPri::High as i32, Ordering::SeqCst);
        read.set_priority(Priority::High);
        let _: Vec<_> = read.scan(key.., 10).await.unwrap().collect();

        let requests = requests.lock().unwrap();
        assert!(requests.contains(&("prewrite", kvrpcpb::CommandPri::High as i32)));
        assert!(requests.contains(&("commit", kvrpcpb::CommandPri::High as i32)));
        assert!(requests.contains(&("get", kvrpcpb::CommandPri::Low as i32)));
        assert!(requests.contains(&("get", kvrpcpb::CommandPri::Normal as i32)));
        assert!(requests.contains(&("scan", kvrpcpb::CommandPri::High as i32)));
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_store_test_TestFailBusyServerKV() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = attempts.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.downcast_ref::<kvrpcpb::GetRequest>().is_some());
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
            rpc,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        assert_eq!(
            transaction
                .get(b"~store/fail_busy_server_key".to_vec())
                .await
                .unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
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
        txn.set_priority(Priority::from_i32(99));
        txn.put("write".to_owned(), "value").await.unwrap();
        // client-go selects the commit primary while initializing its
        // committer, not while staging a MemDB write. This test invokes the
        // hidden heartbeat directly, so establish that post-init state.
        txn.buffer.primary_key_or(&Key::from(b"write".to_vec()));
        txn.send_heart_beat().await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                ("get", kvrpcpb::CommandPri::Low as i32),
                ("heartbeat", kvrpcpb::CommandPri::Normal as i32),
                ("prewrite", 99),
                ("commit", 99),
            ]
        );
    }

    #[tokio::test]
    async fn source_write_actions_use_their_own_request_context_contracts() {
        let observed = Arc::new(Mutex::new(Vec::<(&'static str, kvrpcpb::Context)>::new()));
        let observed_by_hook = Arc::clone(&observed);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    observed_by_hook
                        .lock()
                        .unwrap()
                        .push(("pessimistic-lock", request.context.clone().unwrap()));
                    return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>() {
                    observed_by_hook
                        .lock()
                        .unwrap()
                        .push(("heartbeat", request.context.clone().unwrap()));
                    return Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::PessimisticRollbackRequest>()
                {
                    observed_by_hook
                        .lock()
                        .unwrap()
                        .push(("pessimistic-rollback", request.context.clone().unwrap()));
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                let request = request
                    .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                    .expect("the remaining action is cleanup");
                observed_by_hook
                    .lock()
                    .unwrap()
                    .push(("cleanup", request.context.clone().unwrap()));
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        pd_client.set_timestamp(Timestamp::from_version(2));

        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client.clone(),
            TransactionOptions::new_pessimistic()
                .priority(Priority::High)
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.enable_force_sync_log();
        transaction.set_disk_full_option(kvrpcpb::DiskFullOpt::AllowedOnAlmostFull);
        transaction.set_transaction_source(42);
        transaction.set_request_source_type("sql");
        transaction.set_resource_group_tag(Some(b"transaction-tag".to_vec()));
        transaction.set_resource_group_name("test-rg");
        let expected_request_source = transaction.request_source().context_value();
        let mut broadcast = kvrpcpb::BroadcastTxnStatusRequest::default();
        transaction
            .commit_settings
            .apply_broadcast_request(&mut broadcast, 7);
        let broadcast_context = broadcast.context.unwrap();
        assert_eq!(broadcast_context.cluster_id, 7);
        assert_eq!(broadcast_context.request_source, expected_request_source);
        assert_eq!(broadcast_context.resource_group_tag, b"transaction-tag");
        assert_eq!(
            broadcast_context.priority,
            kvrpcpb::CommandPri::Normal as i32
        );
        assert!(!broadcast_context.sync_log);
        assert_eq!(broadcast_context.disk_full_opt, 0);
        assert_eq!(broadcast_context.txn_source, 0);
        assert_eq!(broadcast_context.max_execution_duration_ms, 0);
        let mut lock_context = LockContext::new(2, 0, SystemTime::now());
        lock_context.resource_group_tag = b"lock-tag".to_vec();

        transaction
            .lock_keys_with_context(&mut lock_context, ["key".to_owned()])
            .await
            .unwrap();
        transaction.send_heart_beat().await.unwrap();
        transaction.rollback().await.unwrap();

        let mut cleanup = source_test_committer(
            pd_client,
            Some(Key::from(b"key".to_vec())),
            vec![source_test_mutation("key", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().priority(Priority::High),
            transaction.commit_settings.clone(),
        );
        cleanup.resource_group_name = Some("test-rg".to_owned());
        cleanup.rollback(true).await.unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 4);
        for (label, context) in observed.iter() {
            assert_eq!(context.max_execution_duration_ms, 20_000, "{label}");
        }

        let lock = &observed[0];
        assert_eq!(lock.0, "pessimistic-lock");
        assert_eq!(lock.1.priority, kvrpcpb::CommandPri::High as i32);
        assert!(lock.1.sync_log);
        assert_eq!(lock.1.resource_group_tag, b"lock-tag");
        assert_eq!(
            lock.1.disk_full_opt,
            kvrpcpb::DiskFullOpt::NotAllowedOnFull as i32
        );
        assert_eq!(lock.1.txn_source, 0);
        assert_eq!(lock.1.request_source, expected_request_source);
        assert_eq!(
            lock.1
                .resource_control_context
                .as_ref()
                .unwrap()
                .resource_group_name,
            "test-rg"
        );

        let heartbeat = &observed[1];
        assert_eq!(heartbeat.0, "heartbeat");
        assert_eq!(heartbeat.1.priority, kvrpcpb::CommandPri::Normal as i32);
        assert!(!heartbeat.1.sync_log);
        assert!(heartbeat.1.resource_group_tag.is_empty());
        assert_eq!(heartbeat.1.disk_full_opt, 0);
        assert_eq!(heartbeat.1.txn_source, 0);
        assert!(heartbeat.1.request_source.is_empty());
        assert!(heartbeat.1.resource_control_context.is_none());

        let pessimistic_rollback = &observed[2];
        assert_eq!(pessimistic_rollback.0, "pessimistic-rollback");
        assert_eq!(
            pessimistic_rollback.1.priority,
            kvrpcpb::CommandPri::Normal as i32
        );
        assert!(!pessimistic_rollback.1.sync_log);
        assert!(pessimistic_rollback.1.resource_group_tag.is_empty());
        assert_eq!(pessimistic_rollback.1.disk_full_opt, 0);
        assert_eq!(pessimistic_rollback.1.txn_source, 0);
        assert_eq!(
            pessimistic_rollback.1.request_source,
            expected_request_source
        );
        assert!(pessimistic_rollback.1.resource_control_context.is_none());

        let cleanup = &observed[3];
        assert_eq!(cleanup.0, "cleanup");
        assert_eq!(cleanup.1.priority, kvrpcpb::CommandPri::High as i32);
        assert!(cleanup.1.sync_log);
        assert_eq!(cleanup.1.resource_group_tag, b"transaction-tag");
        assert_eq!(cleanup.1.disk_full_opt, 0);
        assert_eq!(cleanup.1.txn_source, 0);
        assert_eq!(cleanup.1.request_source, expected_request_source);
        assert_eq!(
            cleanup
                .1
                .resource_control_context
                .as_ref()
                .unwrap()
                .resource_group_name,
            "test-rg"
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
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any>);
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

        assert_eq!(txn.get("read".to_owned()).await.unwrap(), None);
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
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any>);
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

        assert_eq!(txn.get("read".to_owned()).await.unwrap(), None);
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
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any>);
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

        assert_eq!(txn.get("first".to_owned()).await.unwrap(), None);
        assert_eq!(txn.get("second".to_owned()).await.unwrap(), None);
        assert_eq!(status_checks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_transaction_heartbeat_key_errors_do_not_resolve_locks() {
        let heartbeat_attempts = Arc::new(AtomicUsize::new(0));
        let heartbeat_attempts_by_hook = Arc::clone(&heartbeat_attempts);
        let status_checks = Arc::new(AtomicUsize::new(0));
        let status_checks_by_hook = Arc::clone(&status_checks);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |req: &dyn Any| {
                if req.is::<kvrpcpb::TxnHeartBeatRequest>() {
                    assert_eq!(
                        req.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>()
                            .unwrap()
                            .context
                            .as_ref()
                            .expect("heartbeat carries a request context")
                            .max_execution_duration_ms,
                        20_000
                    );
                    heartbeat_attempts_by_hook.fetch_add(1, Ordering::SeqCst);
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
                if req.is::<kvrpcpb::PessimisticLockRequest>() {
                    return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
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
        pd_client.set_timestamp(Timestamp::from_version(4));
        let mut txn = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        txn.set_lock_resolver_context(ResolveLocksContext::default());
        txn.lock_keys(["write".to_owned()]).await.unwrap();

        assert!(matches!(
            txn.send_heart_beat().await.unwrap_err(),
            Error::ExtractedErrors(_)
        ));

        assert_eq!(heartbeat_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(status_checks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_interceptor_test_TestInterceptor() {
        let manager = crate::MockInterceptorManager::new();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |req: &dyn Any| {
                if req.is::<kvrpcpb::PrewriteRequest>() {
                    Ok(Box::new(kvrpcpb::PrewriteResponse::default()) as Box<dyn Any>)
                } else if req.is::<kvrpcpb::CommitRequest>() {
                    Ok(Box::new(kvrpcpb::CommitResponse::default()) as Box<dyn Any>)
                } else if req.is::<kvrpcpb::GetRequest>() {
                    Ok(Box::new(kvrpcpb::GetResponse {
                        value: b"value".to_vec(),
                        ..Default::default()
                    }) as Box<dyn Any>)
                } else {
                    panic!("unexpected request while testing transaction interceptor")
                }
            },
        )));
        let mut txn = Transaction::new(
            Timestamp::from_version(1),
            pd_client.clone(),
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
        );
        txn.set_rpc_interceptor(manager.create_mock_interceptor("INTERCEPTOR-1"));
        txn.put("key".to_owned(), "value").await.unwrap();
        txn.commit().await.unwrap();

        assert_eq!(manager.begin_count(), 2);
        assert_eq!(manager.end_count(), 2);
        assert_eq!(manager.exec_log(), ["INTERCEPTOR-1", "INTERCEPTOR-1"]);
        manager.reset();

        let mut txn = Transaction::new(
            Timestamp::from_version(3),
            pd_client,
            TransactionOptions::new_optimistic(),
            Keyspace::Disable,
        );
        txn.set_rpc_interceptor(manager.create_mock_interceptor("INTERCEPTOR-2"));
        assert_eq!(
            txn.get("key".to_owned()).await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(manager.begin_count(), 1);
        assert_eq!(manager.end_count(), 1);
        assert_eq!(manager.exec_log(), ["INTERCEPTOR-2"]);
        txn.rollback().await.unwrap();
        manager.reset();
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_resource_group_test_TestResourceGroupName() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observed);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let (kind, context, response): (&str, &kvrpcpb::Context, Box<dyn Any>) =
                    if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                        (
                            "get",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::GetResponse {
                                not_found: true,
                                ..Default::default()
                            }),
                        )
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>()
                    {
                        (
                            "batch-get",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::BatchGetResponse::default()),
                        )
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                        (
                            "scan",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::ScanResponse::default()),
                        )
                    } else {
                        panic!("resource-group-name test received an unexpected request");
                    };
                captured.lock().unwrap().push((
                    kind,
                    context
                        .resource_control_context
                        .as_ref()
                        .expect("resource-group name creates resource-control context")
                        .resource_group_name
                        .clone(),
                ));
                Ok(response)
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.set_resource_group_name("test");
        transaction.get(Vec::new()).await.unwrap();
        let _: Vec<_> = transaction
            .batch_get(vec![b"batch".to_vec()])
            .await
            .unwrap()
            .collect();
        let _: Vec<_> = transaction
            .scan(b"abc".to_vec()..b"def".to_vec(), 1)
            .await
            .unwrap()
            .collect();

        assert_eq!(
            *observed.lock().unwrap(),
            [
                ("get", "test".to_owned()),
                ("batch-get", "test".to_owned()),
                ("scan", "test".to_owned()),
            ]
        );
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_resource_tag_test_TestResourceGroupTag() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observed);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let (kind, context, response): (&str, &kvrpcpb::Context, Box<dyn Any>) =
                    if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                        (
                            "get",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::GetResponse {
                                not_found: true,
                                ..Default::default()
                            }),
                        )
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>()
                    {
                        (
                            "batch-get",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::BatchGetResponse::default()),
                        )
                    } else if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                        (
                            "scan",
                            request.context.as_ref().unwrap(),
                            Box::new(kvrpcpb::ScanResponse::default()),
                        )
                    } else {
                        panic!("resource-group-tag test received an unexpected request");
                    };
                captured
                    .lock()
                    .unwrap()
                    .push((kind, context.resource_group_tag.clone()));
                Ok(response)
            },
        )));

        for kind in ["get", "batch-get", "scan"] {
            for (static_tag, dynamic_tag) in [(true, false), (false, true), (true, true)] {
                let mut transaction = Transaction::new(
                    Timestamp::from_version(1),
                    Arc::clone(&pd_client),
                    TransactionOptions::new_optimistic()
                        .read_only()
                        .drop_check(CheckLevel::None),
                    Keyspace::Disable,
                );
                if static_tag {
                    transaction.set_resource_group_tag(Some(b"TEST-TAG-1".to_vec()));
                }
                if dynamic_tag {
                    transaction.set_resource_group_tagger(Some(Arc::new(|request| {
                        request.set_resource_group_tag(b"TEST-TAG-2".to_vec());
                    })));
                }
                match kind {
                    "get" => {
                        transaction.get(Vec::new()).await.unwrap();
                    }
                    "batch-get" => {
                        let _: Vec<_> = transaction
                            .batch_get(vec![b"batch".to_vec()])
                            .await
                            .unwrap()
                            .collect();
                    }
                    "scan" => {
                        let _: Vec<_> = transaction
                            .scan(b"abc".to_vec()..b"def".to_vec(), 1)
                            .await
                            .unwrap()
                            .collect();
                    }
                    _ => unreachable!(),
                }
            }
        }

        assert_eq!(
            *observed.lock().unwrap(),
            [
                ("get", b"TEST-TAG-1".to_vec()),
                ("get", b"TEST-TAG-2".to_vec()),
                ("get", b"TEST-TAG-1".to_vec()),
                ("batch-get", b"TEST-TAG-1".to_vec()),
                ("batch-get", b"TEST-TAG-2".to_vec()),
                ("batch-get", b"TEST-TAG-1".to_vec()),
                ("scan", b"TEST-TAG-1".to_vec()),
                ("scan", b"TEST-TAG-2".to_vec()),
                ("scan", b"TEST-TAG-1".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn source_test_send_request_settles_on_success() {
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
    async fn source_test_send_request_does_not_settle_and_keeps_ru_details_on_transport_failure() {
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
    async fn source_test_send_request_async_does_not_settle_and_keeps_ru_details_on_transport_failure(
    ) {
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
    async fn source_api_v2_direct_memdb_commit_filters_logical_and_dispatches_physical_keys() {
        struct LogicalKeyFilter(Arc<Mutex<Vec<Vec<u8>>>>);
        impl super::KvFilter for LogicalKeyFilter {
            fn is_unnecessary_key_value(
                &self,
                key: &[u8],
                _value: &[u8],
                _flags: super::MutationFlags,
            ) -> crate::Result<bool> {
                self.0.lock().unwrap().push(key.to_vec());
                Ok(key == b"skipped")
            }
        }

        let dispatched = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&dispatched);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    assert_eq!(request.mutations.len(), 1);
                    assert_eq!(request.mutations[0].key, b"x\0\0\x07direct");
                    assert_eq!(request.mutations[0].value, b"value");
                    captured.lock().unwrap().push("prewrite");
                    Ok(Box::new(kvrpcpb::PrewriteResponse::default()) as Box<dyn Any>)
                } else if request.is::<kvrpcpb::CommitRequest>() {
                    captured.lock().unwrap().push("commit");
                    Ok(Box::new(kvrpcpb::CommitResponse::default()) as Box<dyn Any>)
                } else {
                    panic!("unexpected request while committing direct API-V2 MemDB write")
                }
            },
        )));
        let filtered = Arc::new(Mutex::new(Vec::new()));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic().heartbeat_option(HeartbeatOption::NoHeartbeat),
            Keyspace::try_enable(7).unwrap(),
        );
        transaction.set_kv_filter(Arc::new(LogicalKeyFilter(Arc::clone(&filtered))));
        transaction
            .get_mem_buffer()
            .set(b"direct", b"value")
            .unwrap();
        transaction.get_mem_buffer().delete(b"skipped").unwrap();

        assert_eq!(
            transaction.get("direct".to_owned()).await.unwrap(),
            Some(b"value".to_vec())
        );
        transaction.commit().await.unwrap();
        assert_eq!(
            *filtered.lock().unwrap(),
            vec![b"direct".to_vec(), b"skipped".to_vec()]
        );
        assert_eq!(*dispatched.lock().unwrap(), vec!["prewrite", "commit"]);
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
    #[serial_test::serial]
    async fn source_small_optimistic_transaction_does_not_start_ttl_manager(
        #[case] keyspace: Keyspace,
    ) -> Result<(), io::Error> {
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
        assert_eq!(heartbeats.load(Ordering::SeqCst), 0);
        scenario.teardown();
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn source_large_primary_prewrite_success_starts_ttl_manager() {
        let restore = crate::config::update_global(|config| {
            config.tikv_client.ttl_refreshed_txn_size = 0;
        });
        let scenario = FailScenario::setup();
        fail::cfg("after-prewrite", "sleep(50)").unwrap();
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let heartbeats_cloned = heartbeats.clone();
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::TxnHeartBeatRequest>() {
                    heartbeats_cloned.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>)
                } else if request.is::<kvrpcpb::PrewriteRequest>() {
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                }
            },
        )));
        pd_client.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(5))),
            Keyspace::Disable,
        );
        transaction.put("key".to_owned(), "value").await.unwrap();

        tokio::task::spawn_blocking(move || futures::executor::block_on(transaction.commit()))
            .await
            .unwrap()
            .unwrap();

        assert!(heartbeats.load(Ordering::SeqCst) > 0);
        scenario.teardown();
        restore();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn source_filtered_entries_do_not_inflate_ttl_manager_txn_size() {
        struct SkipFiltered;
        impl super::KvFilter for SkipFiltered {
            fn is_unnecessary_key_value(
                &self,
                key: &[u8],
                _value: &[u8],
                _flags: super::MutationFlags,
            ) -> crate::Result<bool> {
                Ok(key == b"filtered")
            }
        }

        let restore = crate::config::update_global(|config| {
            config.tikv_client.ttl_refreshed_txn_size = 10;
        });
        let scenario = FailScenario::setup();
        fail::cfg("after-prewrite", "sleep(50)").unwrap();
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let captured_heartbeats = Arc::clone(&heartbeats);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::TxnHeartBeatRequest>() {
                    captured_heartbeats.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>)
                } else if request.is::<kvrpcpb::PrewriteRequest>() {
                    Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
                } else {
                    Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
                }
            },
        )));
        pd_client.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(5))),
            Keyspace::Disable,
        );
        transaction.set_kv_filter(Arc::new(SkipFiltered));
        transaction.put("a".to_owned(), "v").await.unwrap();
        transaction
            .put("filtered".to_owned(), vec![b'x'; 64])
            .await
            .unwrap();

        transaction.commit().await.unwrap();

        assert_eq!(heartbeats.load(Ordering::SeqCst), 0);
        scenario.teardown();
        restore();
    }

    #[tokio::test]
    async fn source_filtered_pessimistic_write_retains_value_on_lock_mutation() {
        struct FilterLockedWrite;
        impl super::KvFilter for FilterLockedWrite {
            fn is_unnecessary_key_value(
                &self,
                key: &[u8],
                _value: &[u8],
                _flags: super::MutationFlags,
            ) -> crate::Result<bool> {
                Ok(key == b"locked")
            }
        }

        let prewrites = Arc::new(Mutex::new(Vec::new()));
        let captured_prewrites = Arc::clone(&prewrites);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PessimisticLockRequest>() {
                    return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured_prewrites
                        .lock()
                        .unwrap()
                        .extend(request.mutations.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                assert!(request.is::<kvrpcpb::CommitRequest>());
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .put("locked".to_owned(), "retained-value")
            .await
            .unwrap();
        transaction.lock_keys(["locked".to_owned()]).await.unwrap();
        transaction.set_kv_filter(Arc::new(FilterLockedWrite));

        transaction.commit().await.unwrap();

        let prewrites = prewrites.lock().unwrap();
        assert_eq!(prewrites.len(), 1);
        assert_eq!(prewrites[0].op, kvrpcpb::Op::Lock as i32);
        assert_eq!(prewrites[0].key, b"locked");
        assert_eq!(prewrites[0].value, b"retained-value");
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable)]
    #[case(Keyspace::Enable { keyspace_id: 0 })]
    #[tokio::test]
    #[serial_test::serial]
    async fn source_pessimistic_primary_lock_starts_ttl_manager(
        #[case] keyspace: Keyspace,
    ) -> Result<(), io::Error> {
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
        // A zero for-update timestamp intentionally takes the local lock-only
        // path, so allocate a real statement timestamp as client-go callers do.
        pd_client.set_timestamp(Timestamp::from_version(1));
        let key1 = "key1".to_owned();
        let mut heartbeat_txn = Transaction::new(
            Timestamp::default(),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(1))),
            keyspace,
        );
        heartbeat_txn.lock_keys([key1.clone()]).await.unwrap();
        assert!(heartbeat_txn.buffer.get_primary_key().is_some());
        assert!(heartbeat_txn.is_heartbeat_started.load(Ordering::Acquire));
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

    #[tokio::test]
    async fn source_standard_ttl_manager_stops_after_first_key_error() {
        let heartbeat_attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = Arc::clone(&heartbeat_attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PessimisticLockRequest>() {
                    return Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::TxnHeartBeatRequest>() {
                    captured_attempts.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::TxnHeartBeatResponse {
                        error: Some(kvrpcpb::KeyError {
                            txn_not_found: Some(kvrpcpb::TxnNotFound {
                                start_ts: 1,
                                primary_key: b"primary".to_vec(),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected request while testing terminal standard heartbeat error");
            },
        )));
        pd_client.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(1)))
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.lock_keys(["primary".to_owned()]).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while heartbeat_attempts.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the TTL manager sends its first heartbeat");
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(heartbeat_attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_ttl_manager_sends_live_min_commit_ts_and_txn_file_marker() {
        let observed = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&observed);
        let sent = Arc::new(tokio::sync::Notify::new());
        let sent_by_hook = Arc::clone(&sent);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>() {
                    assert_eq!(
                        request
                            .context
                            .as_ref()
                            .expect("managed heartbeat carries a request context")
                            .max_execution_duration_ms,
                        20_000
                    );
                    *captured.lock().unwrap() = Some((request.min_commit_ts, request.is_txn_file));
                    sent_by_hook.notify_one();
                    return Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request while testing heartbeat metadata");
            },
        )));
        pd_client.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            pd_client,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(1)))
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .buffer
            .primary_key_or(&Key::from(b"primary".to_vec()));
        let min_commit_ts = MinCommitTsManager::default();
        min_commit_ts.try_update(77, WriteAccessLevel::Ttl);

        transaction
            .auto_heartbeat_starter(None)
            .expect("a selected primary enables heartbeat")(min_commit_ts, true);
        tokio::time::timeout(Duration::from_secs(1), sent.notified())
            .await
            .expect("heartbeat metadata request was sent");
        transaction.set_status(TransactionStatus::Committed);

        assert_eq!(*observed.lock().unwrap(), Some((77, true)));
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
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestLockKeys() {
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
        let optimistic_exclusive = Key::from(b"exclusive".to_vec());
        let optimistic_shared = Key::from(b"shared".to_vec());
        assert!(optimistic.buffer.is_locked(&optimistic_exclusive));
        assert!(!optimistic.buffer.is_shared_locked(&optimistic_exclusive));
        assert!(optimistic.buffer.is_locked(&optimistic_shared));
        assert!(!optimistic.buffer.is_shared_locked(&optimistic_shared));

        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("lock_keys only sends pessimistic-lock requests");
                assert_eq!(
                    request
                        .context
                        .as_ref()
                        .expect("pessimistic lock carries a request context")
                        .max_execution_duration_ms,
                    20_000
                );
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
        let primary = Key::from(b"primary".to_vec());
        assert!(pessimistic.buffer.is_locked(&primary));
        assert!(!pessimistic.buffer.is_shared_locked(&primary));
        pessimistic
            .lock_keys_shared_with_wait_time(17, ["shared".to_owned()])
            .await
            .unwrap();
        assert!(pessimistic
            .buffer
            .is_shared_locked(&Key::from(b"shared".to_vec())));
        pessimistic
            .buffer
            .lock_with_returned_value(Key::from(b"shared".to_vec()), false, None)
            .unwrap();
        assert!(pessimistic.buffer.is_locked(&Key::from(b"shared".to_vec())));
        assert!(!pessimistic
            .buffer
            .is_shared_locked(&Key::from(b"shared".to_vec())));
        pessimistic
            .buffer
            .delete(Key::from(b"shared".to_vec()))
            .unwrap();

        let mut pipelined = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_pessimistic()
                .pipelined(super::PipelinedTxnOptions {
                    enable: true,
                    flush_concurrency: 1,
                    resolve_lock_concurrency: 1,
                    write_throttle_ratio: 0.0,
                })
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let error = pipelined
            .lock_keys_shared(["shared".to_owned()])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("shared lock is not supported in pipelined transaction"));
        pipelined.pipelined_cancellation.cancel();

        pessimistic.start_aggressive_locking();
        let error = pessimistic
            .lock_keys_shared(["another-shared".to_owned()])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("shared lock is not supported in aggressive/fair locking mode"));
        assert!(pessimistic.is_in_aggressive_locking_mode());
        pessimistic.cancel_aggressive_locking().await.unwrap();

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
            super::TransactionKind::Pessimistic(ref timestamp) if timestamp.version() == 10
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
    async fn source_uncovered_lock_keys_callback_starts_after_aggressive_preflight() {
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_pessimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.start_aggressive_locking();

        let mut context = LockContext::new(2, crate::kv::LOCK_NO_WAIT, SystemTime::now());
        context.in_share_mode = true;
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&callback_calls);
        let error = transaction
            .lock_keys_with_context_and_callback(&mut context, ["shared".to_owned()], move || {
                captured.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("shared lock is not supported in aggressive/fair locking mode"));
        assert_eq!(callback_calls.load(Ordering::SeqCst), 0);
        assert!(transaction.is_in_aggressive_locking_mode());

        let mut optimistic = Transaction::new(
            Timestamp::from_version(1),
            Arc::new(MockPdClient::default()),
            TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        optimistic.start_aggressive_locking();
        let mut context = LockContext::new(2, crate::kv::LOCK_NO_WAIT, SystemTime::now());
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&callback_calls);
        let error = optimistic
            .lock_keys_with_context_and_callback(&mut context, ["key".to_owned()], move || {
                captured.fetch_add(1, Ordering::SeqCst);
            })
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("trying to perform aggressive locking in optimistic transaction"));
        assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_pessimistic_lock_selects_and_dispatches_primary_first_with_elapsed_ttl() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let rpc = Arc::new(MockPdClient::with_client_and_regions(
            MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("lock_keys only sends pessimistic-lock requests");
                captured.lock().unwrap().push((
                    request.primary_lock.clone(),
                    request
                        .mutations
                        .iter()
                        .map(|mutation| mutation.key.clone())
                        .collect::<Vec<_>>(),
                    request.lock_ttl,
                ));
                Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>)
            }),
            vec![MockPdClient::region1(), MockPdClient::region2()],
        ));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.start_instant = Instant::now() - Duration::from_millis(125);

        // client-go deduplicates and sorts before selecting the first primary,
        // independent of caller order.
        transaction
            .lock_keys_with_wait_time(1_000, [vec![20], vec![1]])
            .await
            .unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert!(observed.iter().all(|(primary, _, _)| primary == &[1]));
        assert_eq!(observed[0].1, vec![vec![1]]);
        assert!(observed.iter().all(|(_, _, ttl)| *ttl >= MAX_TTL + 100));
    }

    #[tokio::test]
    async fn source_pessimistic_primary_and_secondary_share_one_retry_owner() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observed);
        let tagged_batches = Arc::new(Mutex::new(Vec::new()));
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let captured_primary_calls = Arc::clone(&primary_calls);
        let rpc = Arc::new(MockPdClient::with_client_and_regions(
            MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
                    let keys = request
                        .mutations
                        .iter()
                        .map(|mutation| mutation.key.clone())
                        .collect::<Vec<_>>();
                    captured.lock().unwrap().push((
                        keys.clone(),
                        request.context.as_ref().unwrap().is_retry_request,
                        request.context.as_ref().unwrap().resource_group_tag.clone(),
                    ));
                    if keys == vec![vec![1]]
                        && captured_primary_calls.fetch_add(1, Ordering::SeqCst) > 0
                    {
                        return Ok(
                            Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>
                        );
                    }
                    return Err(Error::GrpcAPI(tonic::Status::unavailable(
                        "pessimistic lock response lost",
                    )));
                }
                assert!(request.is::<kvrpcpb::PessimisticRollbackRequest>());
                Ok(Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>)
            }),
            vec![MockPdClient::region1(), MockPdClient::region2()],
        ));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let owner = Arc::new(tokio::sync::Mutex::new(super::RetryBackoffer::new(
            crate::async_util::Cancellation::default(),
            3,
        )));
        let mut context = LockContext::new(2, 0, SystemTime::now());
        let captured_tagged_batches = Arc::clone(&tagged_batches);
        context.resource_group_tagger = Some(Arc::new(move |request| {
            let keys = request
                .mutations
                .iter()
                .map(|mutation| mutation.key.clone())
                .collect::<Vec<_>>();
            captured_tagged_batches.lock().unwrap().push(keys.clone());
            keys.first().cloned().unwrap_or_default()
        }));

        transaction
            .pessimistic_lock_impl_with_retry_owner(
                vec![
                    (Key::from(vec![1]), kvrpcpb::Assertion::None),
                    (Key::from(vec![20]), kvrpcpb::Assertion::None),
                ],
                false,
                kvrpcpb::Op::PessimisticLock,
                0,
                kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeNormal,
                Some(&mut context),
                Some(Arc::clone(&owner)),
            )
            .await
            .unwrap_err();

        assert_eq!(
            *observed.lock().unwrap(),
            vec![
                (vec![vec![1]], false, vec![1]),
                (vec![vec![1]], true, vec![1]),
                (vec![vec![20]], true, vec![20]),
            ]
        );
        assert_eq!(
            *tagged_batches.lock().unwrap(),
            vec![vec![vec![1]], vec![vec![20]]]
        );
        assert!(owner.lock().await.total_sleep_ms() > 0);
    }

    #[tokio::test]
    async fn source_pessimistic_lock_tagger_runs_for_each_physical_region_batch() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let rpc = Arc::new(MockPdClient::with_client_and_regions(
            MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("lock_keys only sends pessimistic-lock requests");
                captured.lock().unwrap().push((
                    request
                        .mutations
                        .iter()
                        .map(|mutation| mutation.key.clone())
                        .collect::<Vec<_>>(),
                    request.context.as_ref().unwrap().resource_group_tag.clone(),
                ));
                Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>)
            }),
            vec![
                MockPdClient::region1(),
                MockPdClient::region2(),
                MockPdClient::region3(),
            ],
        ));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction.set_resource_group_tag(Some(b"transaction-tag".to_vec()));
        let mut context = LockContext::new(2, 0, SystemTime::now());
        context.resource_group_tagger = Some(Arc::new(|request| {
            request
                .mutations
                .first()
                .expect("each physical lock batch is non-empty")
                .key
                .clone()
        }));

        transaction
            .lock_keys_with_context(&mut context, [vec![250, 250], vec![20], vec![1]])
            .await
            .unwrap();
        transaction.set_status(TransactionStatus::Rolledback);

        let mut observed = observed.lock().unwrap().clone();
        observed.sort();
        assert_eq!(
            observed,
            vec![
                (vec![vec![1]], vec![1]),
                (vec![vec![20]], vec![20]),
                (vec![vec![250, 250]], vec![250, 250]),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_pessimistic_secondaries_wait_for_primary_success() {
        let primary_completed = Arc::new(AtomicBool::new(false));
        let secondary_started_early = Arc::new(AtomicBool::new(false));
        let captured_primary_completed = primary_completed.clone();
        let captured_secondary_started_early = secondary_started_early.clone();
        let rpc = Arc::new(MockPdClient::with_client_and_regions(
            MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                    .expect("lock_keys only sends pessimistic-lock requests");
                if request.mutations.iter().any(|mutation| mutation.key == [1]) {
                    std::thread::sleep(Duration::from_millis(50));
                    captured_primary_completed.store(true, Ordering::SeqCst);
                } else if !captured_primary_completed.load(Ordering::SeqCst) {
                    captured_secondary_started_early.store(true, Ordering::SeqCst);
                }
                Ok(Box::<kvrpcpb::PessimisticLockResponse>::default() as Box<dyn Any>)
            }),
            vec![MockPdClient::region1(), MockPdClient::region2()],
        ));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut transaction = Transaction::new(
            Timestamp::from_version(1),
            rpc,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );

        transaction.lock_keys([vec![20], vec![1]]).await.unwrap();

        assert!(primary_completed.load(Ordering::SeqCst));
        assert!(!secondary_started_early.load(Ordering::SeqCst));
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
                    values: vec![b"value".to_vec()],
                    not_founds: vec![false],
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
        assert_eq!(context.max_locked_with_conflict_ts, 0);
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
                    values: vec![Vec::new()],
                    not_founds: vec![true],
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
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::PessimisticRollbackRequest>() {
                    return Ok(
                        Box::<kvrpcpb::PessimisticRollbackResponse>::default() as Box<dyn Any>
                    );
                }
                assert!(request.is::<kvrpcpb::PessimisticLockRequest>());
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
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestSharedLockCommitterIncompatibilities() {
        let options = TransactionOptions::new_optimistic()
            .use_async_commit()
            .try_one_pc();
        let mut plain = source_test_committer(
            Arc::new(MockPdClient::default()),
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("primary", kvrpcpb::Op::Lock)],
            options.clone(),
            CommitSettings::default(),
        );
        plain.configure_commit_protocols();
        assert!(plain.options.async_commit);
        assert!(plain.options.try_one_pc);

        let shared = source_test_mutation("shared", kvrpcpb::Op::SharedLock);
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
        assert!(pipelined.maybe_flush_pipelined(true).await.unwrap());
        let error = pipelined.buffer.mem_buffer().flush_wait().unwrap_err();
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
                assert_eq!(
                    request
                        .context
                        .as_ref()
                        .expect("pipelined flush carries a request context")
                        .max_execution_duration_ms,
                    20_000
                );
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
        // Source `Flush(true)` rotates an empty final generation; the flush
        // callback observes it but deliberately sends no TiKV RPC.
        assert!(transaction.maybe_flush_pipelined(true).await.unwrap());
        transaction.buffer.mem_buffer().flush_wait().unwrap();
        transaction.sync_pipelined_state_from_memdb();

        assert_eq!(
            *flushed.lock().unwrap(),
            vec![
                (1, vec![b"a".to_vec()], "external_pdml".to_owned()),
                (2, vec![b"b".to_vec()], "external_pdml".to_owned()),
                (3, vec![b"a".to_vec()], "external_pdml".to_owned()),
            ]
        );
        assert_eq!(transaction.pipelined_state.generation, 4);
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
    async fn source_pipelined_flush_uses_one_cumulative_retry_owner() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request| {
                let request = request
                    .downcast_ref::<kvrpcpb::FlushRequest>()
                    .expect("pipelined flush sends Flush");
                captured_requests.lock().unwrap().push(
                    request
                        .context
                        .as_ref()
                        .expect("flush carries a request context")
                        .is_retry_request,
                );
                Ok(Box::new(kvrpcpb::FlushResponse {
                    region_error: Some(crate::proto::errorpb::Error {
                        not_leader: Some(crate::proto::errorpb::NotLeader {
                            region_id: 2,
                            leader: None,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let captured_tagger_calls = Arc::clone(&tagger_calls);
        let mut settings = CommitSettings::default();
        settings.resource_group_tagger = Some(Arc::new(move |request| {
            captured_tagger_calls.fetch_add(1, Ordering::SeqCst);
            request.set_resource_group_tag(b"flush".to_vec());
        }));
        settings.pipelined = super::PipelinedTxnOptions {
            enable: true,
            flush_concurrency: 1,
            resolve_lock_concurrency: 1,
            write_throttle_ratio: 0.0,
        };
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        let owner = Arc::new(tokio::sync::Mutex::new(super::RetryBackoffer::new(
            crate::async_util::Cancellation::default(),
            1,
        )));

        committer
            .flush_pipelined_generation_with_retry_owner(
                vec![source_test_mutation("k", kvrpcpb::Op::Put)],
                1,
                None,
                Some(Arc::clone(&owner)),
            )
            .await
            .unwrap_err();

        assert_eq!(*requests.lock().unwrap(), vec![false, true]);
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 1);
        assert!(owner.lock().await.total_sleep_ms() >= 1);
    }

    #[tokio::test]
    async fn source_pipelined_resolve_retries_transport_with_its_action_context() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured_requests = Arc::clone(&requests);
        let calls = Arc::new(AtomicUsize::new(0));
        let captured_calls = Arc::clone(&calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request| {
                let request = request
                    .downcast_ref::<kvrpcpb::ResolveLockRequest>()
                    .expect("pipelined cleanup sends ResolveLock");
                captured_requests
                    .lock()
                    .unwrap()
                    .push((request.clone(), request.context.clone().unwrap()));
                if captured_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(Error::GrpcAPI(tonic::Status::unavailable(
                        "resolve response lost",
                    )));
                }
                Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
            },
        )));
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let captured_tagger_calls = Arc::clone(&tagger_calls);
        let mut settings = CommitSettings::default();
        settings.force_sync_log = true;
        settings.transaction_source = 77;
        settings.request_source.source_type = "sql".to_owned();
        settings.resource_group_tagger = Some(Arc::new(move |request| {
            captured_tagger_calls.fetch_add(1, Ordering::SeqCst);
            request.set_resource_group_tag(b"dynamic".to_vec());
        }));
        let committer = source_test_committer(
            rpc,
            Some(Key::from(b"a".to_vec())),
            vec![source_test_mutation("a", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().priority(Priority::High),
            settings,
        );
        let owner = Arc::new(tokio::sync::Mutex::new(super::RetryBackoffer::new(
            crate::async_util::Cancellation::default(),
            1,
        )));
        let mut retry_backoff = super::TxnFileRetryBackoff::Source(Arc::clone(&owner));

        committer
            .resolve_pipelined_lock_range_with_backoff(
                b"a".to_vec(),
                b"z".to_vec(),
                9,
                &mut retry_backoff,
            )
            .await
            .unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].0.start_version, 1);
        assert_eq!(requests[0].0.commit_version, 9);
        assert!(!requests[0].1.is_retry_request);
        assert!(requests[1].1.is_retry_request);
        for (_, context) in requests.iter() {
            assert_eq!(context.priority, kvrpcpb::CommandPri::High as i32);
            assert!(context.sync_log);
            assert_eq!(context.txn_source, 77);
            assert_eq!(context.request_source, "external_pdml");
            assert!(context.resource_group_tag.is_empty());
            assert_eq!(context.max_execution_duration_ms, 0);
        }
        drop(requests);
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 0);
        assert!(owner.lock().await.total_sleep_ms() >= 1);
    }

    #[rstest::rstest]
    #[case(Keyspace::Disable, b"a".to_vec(), b"b".to_vec())]
    #[case(
        Keyspace::try_enable(7).unwrap(),
        b"x\0\0\x07a".to_vec(),
        b"x\0\0\x07b".to_vec()
    )]
    #[tokio::test]
    async fn source_direct_mem_buffer_flush_rotates_the_authoritative_generation(
        #[case] keyspace: Keyspace,
        #[case] physical_a: Vec<u8>,
        #[case] physical_b: Vec<u8>,
    ) {
        let flushed = Arc::new(Mutex::new(Vec::new()));
        let flushed_by_hook = flushed.clone();
        let committed = Arc::new(Mutex::new(Vec::new()));
        let committed_by_hook = committed.clone();
        let buffer_reads = Arc::new(AtomicUsize::new(0));
        let buffer_reads_by_hook = buffer_reads.clone();
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let statuses_by_hook = statuses.clone();
        let physical_a_by_hook = physical_a.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::FlushRequest>() {
                    assert_eq!(
                        request.context.as_ref().unwrap().request_source,
                        "external_pdml"
                    );
                    flushed_by_hook.lock().unwrap().push((
                        request.generation,
                        request.primary_key.clone(),
                        request
                            .mutations
                            .iter()
                            .map(|mutation| mutation.key.clone())
                            .collect::<Vec<_>>(),
                    ));
                    std::thread::sleep(Duration::from_millis(5));
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BufferBatchGetRequest>() {
                    assert_eq!(request.context.as_ref().unwrap().resolved_locks, vec![1]);
                    buffer_reads_by_hook.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::BufferBatchGetResponse {
                        pairs: request
                            .keys
                            .iter()
                            .filter(|key| key.as_slice() == physical_a_by_hook.as_slice())
                            .map(|key| kvrpcpb::KvPair {
                                key: key.clone(),
                                value: b"one".to_vec(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                    committed_by_hook.lock().unwrap().push(request.clone());
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>()
                {
                    statuses_by_hook
                        .lock()
                        .unwrap()
                        .extend(request.txn_status.clone());
                    return Ok(
                        Box::<kvrpcpb::BroadcastTxnStatusResponse>::default() as Box<dyn Any>
                    );
                }
                panic!("unexpected direct-MemBuffer request");
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
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
            keyspace,
        );
        transaction.set_request_source_type("sql");
        let footprints = Arc::new(Mutex::new(Vec::new()));
        let footprints_by_hook = footprints.clone();
        transaction.set_memory_footprint_change_hook(move |bytes| {
            footprints_by_hook.lock().unwrap().push(bytes);
        });
        let scan_error = match transaction.scan(b"a".to_vec()..b"z".to_vec(), 1).await {
            Ok(_) => panic!("pipelined transaction scans must reject MemDB iteration"),
            Err(error) => error,
        };
        assert!(scan_error
            .to_string()
            .contains("pipelined memdb does not support Iter"));

        {
            let memdb = transaction.get_mem_buffer();
            let mut iterator = memdb.iter(None, None);
            assert_eq!(
                iterator.next(),
                Err("pipelined memdb does not support Iter")
            );
            let mut snapshot_iterator = memdb.snapshot_iter(None, None);
            assert_eq!(
                snapshot_iterator.next(),
                Err("SnapshotIter is not supported for PipelinedMemDB")
            );
            assert_eq!(
                memdb.for_each_in_snapshot_range(None, None, false, |_, _| Ok(false)),
                Err("pipelined memdb does not support ForEachInSnapshotRange")
            );
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                memdb.checkpoint();
            }))
            .is_err());

            memdb.set(b"a", b"one").unwrap();
            assert!(memdb.flush(true).unwrap());
            assert_eq!(memdb.get_readonly(b"a").unwrap(), b"one");
            memdb.flush_wait().unwrap();

            memdb.set_managed_flush_thresholds(1, 0, u64::MAX);
            memdb.set(b"b", b"two").unwrap();
            assert!(memdb.flush(false).unwrap());
            assert_eq!(memdb.get_readonly(b"b").unwrap(), b"two");
            memdb.flush_wait().unwrap();
        }
        transaction.sync_pipelined_state_from_memdb();

        assert_eq!(
            *flushed.lock().unwrap(),
            vec![
                (1, physical_a.clone(), vec![physical_a.clone()]),
                (2, physical_a.clone(), vec![physical_b]),
            ]
        );
        assert_eq!(transaction.pipelined_state.generation, 2);
        assert_eq!(
            transaction.pipelined_state.range_start.as_deref(),
            Some(b"a".as_slice())
        );
        assert_eq!(
            transaction.pipelined_state.range_end.as_deref(),
            Some(b"b".as_slice())
        );
        assert_eq!(transaction.memory_footprint(), 0);
        assert_eq!(footprints.lock().unwrap().last().copied(), Some(0));
        assert!(transaction.get_mem_buffer().metrics().flush_wait_duration > Duration::ZERO);
        assert_eq!(transaction.get_mem_buffer().get(b"a").unwrap(), b"one");
        assert_eq!(buffer_reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            transaction
                .get_mem_buffer()
                .batch_get(&[b"a".to_vec(), b"missing".to_vec()])
                .unwrap(),
            BTreeMap::from([(b"a".to_vec(), b"one".to_vec())])
        );
        assert_eq!(buffer_reads.load(Ordering::SeqCst), 2);
        assert_eq!(transaction.get_mem_buffer().get(b"a").unwrap(), b"one");
        assert_eq!(buffer_reads.load(Ordering::SeqCst), 2);
        assert_eq!(
            transaction.get("a".to_owned()).await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(buffer_reads.load(Ordering::SeqCst), 2);
        assert_eq!(
            transaction
                .batch_get(["a".to_owned(), "missing".to_owned()])
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            vec![KvPair(Key::from("a".to_owned()), b"one".to_vec())]
        );
        assert_eq!(buffer_reads.load(Ordering::SeqCst), 3);
        assert_eq!(
            transaction
                .get_with_options("a".to_owned(), &[GetOption::ReturnCommitTs])
                .await
                .unwrap()
                .unwrap()
                .commit_ts,
            0
        );
        assert_eq!(buffer_reads.load(Ordering::SeqCst), 3);
        assert_eq!(
            transaction
                .batch_get_with_options(
                    ["a".to_owned(), "missing".to_owned()],
                    &[GetOption::ReturnCommitTs],
                )
                .await
                .unwrap()
                .get(&Key::from("a".to_owned()))
                .unwrap()
                .commit_ts,
            0
        );
        assert_eq!(buffer_reads.load(Ordering::SeqCst), 4);
        assert_eq!(
            transaction.get("a".to_owned()).await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(buffer_reads.load(Ordering::SeqCst), 4);
        assert!(!transaction.get_mem_buffer().flush(false).unwrap());
        assert_eq!(
            transaction.get("a".to_owned()).await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(buffer_reads.load(Ordering::SeqCst), 5);
        assert_eq!(transaction.commit().await.unwrap().unwrap().version(), 2);
        let committed = committed.lock().unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].keys, vec![physical_a.clone()]);
        assert_eq!(committed[0].primary_key, physical_a);
        assert_eq!(
            committed[0].commit_role,
            kvrpcpb::CommitRole::Primary as i32
        );
        assert_eq!(
            committed[0].context.as_ref().unwrap().request_source,
            "external_sql"
        );
        drop(committed);
        tokio::time::timeout(Duration::from_secs(1), async {
            while statuses.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("pipelined secondary resolution broadcasts both statuses");
        let statuses = statuses.lock().unwrap();
        let committing = statuses
            .iter()
            .find(|status| status.commit_ts == 2 && !status.is_completed)
            .expect("commit broadcasts the non-completed transaction status");
        assert_eq!(committing.min_commit_ts, 0);
        let completed = statuses
            .iter()
            .find(|status| status.commit_ts == 2 && status.is_completed)
            .expect("resolved locks broadcast the completed transaction status");
        assert_eq!(completed.min_commit_ts, 0);
    }

    #[tokio::test]
    async fn source_managed_pipelined_flush_enriches_key_exists_value() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::FlushRequest>());
                Ok(Box::new(kvrpcpb::FlushResponse {
                    errors: vec![kvrpcpb::KeyError {
                        already_exist: Some(kvrpcpb::AlreadyExist {
                            key: b"key".to_vec(),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }) as Box<dyn Any>)
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
        let memdb = transaction.get_mem_buffer();
        memdb.set(b"key", b"buffered-value").unwrap();
        assert!(memdb.flush(true).unwrap());
        match memdb.flush_wait().unwrap_err() {
            PipelinedError::KeyExists(error) => {
                assert_eq!(error.already_exist.key, b"key");
                assert_eq!(error.value, b"buffered-value");
            }
            error => panic!("expected typed key-exists error, got {error}"),
        }
    }

    #[tokio::test]
    async fn source_empty_pipelined_commit_is_a_read_only_noop() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let observed_by_hook = observed.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::FlushRequest>()
                    .expect("empty pipelined commit only flushes its final generation");
                observed_by_hook
                    .lock()
                    .unwrap()
                    .push((request.generation, request.mutations.len()));
                Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>)
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

        assert_eq!(transaction.commit().await.unwrap(), None);
        assert!(observed.lock().unwrap().is_empty());
        assert_eq!(transaction.pipelined_state.generation, 0);
        assert_eq!(transaction.get_status(), TransactionStatus::Committed);
    }

    #[tokio::test]
    async fn source_empty_pipelined_rollback_broadcasts_completed_status_without_flushing() {
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let statuses_by_hook = statuses.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>()
                    .expect("empty pipelined rollback only broadcasts transaction status");
                statuses_by_hook
                    .lock()
                    .unwrap()
                    .extend(request.txn_status.clone());
                Ok(Box::<kvrpcpb::BroadcastTxnStatusResponse>::default() as Box<dyn Any>)
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

        transaction.rollback().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while statuses.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("empty pipelined rollback broadcasts its status asynchronously");
        let statuses = statuses.lock().unwrap();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].start_ts, 1);
        assert_eq!(statuses[0].min_commit_ts, 0);
        assert_eq!(statuses[0].commit_ts, 0);
        assert!(statuses[0].rolled_back);
        assert!(statuses[0].is_completed);
    }

    #[cfg(not(feature = "nextgen"))]
    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_pipelined_memdb_test_TestResolveLockRace() {
        let status_sent = Arc::new(tokio::sync::Notify::new());
        let status_sent_by_hook = status_sent.clone();
        let allow_status_finish = Arc::new(std::sync::Barrier::new(2));
        let allow_status_finish_by_hook = allow_status_finish.clone();
        let first_flush = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let first_flush_by_hook = first_flush.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::FlushRequest>() {
                    if !first_flush_by_hook.swap(false, std::sync::atomic::Ordering::SeqCst) {
                        return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                    }
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
                    allow_status_finish_by_hook.wait();
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
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
        assert!(transaction.maybe_flush_pipelined(true).await.unwrap());

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

        allow_status_finish.wait();
        transaction.buffer.mem_buffer().flush_wait().unwrap();
        assert!(context.resolving_locks().await.is_empty());
    }

    #[cfg(not(feature = "nextgen"))]
    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_pipelined_memdb_test_TestPipelinedRollback() {
        let status_sent = Arc::new(tokio::sync::Notify::new());
        let status_sent_by_hook = status_sent.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::FlushRequest>() {
                    return Ok(Box::new(kvrpcpb::FlushResponse {
                        errors: vec![kvrpcpb::KeyError {
                            locked: Some(kvrpcpb::LockInfo {
                                key: b"locked".to_vec(),
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
                if request.is::<kvrpcpb::ResolveLockRequest>() {
                    return Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::BroadcastTxnStatusRequest>() {
                    return Ok(
                        Box::<kvrpcpb::BroadcastTxnStatusResponse>::default() as Box<dyn Any>
                    );
                }
                panic!("unexpected pipelined-cancellation request");
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
            .put("key".to_owned(), "value".to_owned())
            .await
            .unwrap();
        let context = transaction.lock_resolver_context.clone();
        assert!(transaction.maybe_flush_pipelined(true).await.unwrap());
        tokio::time::timeout(Duration::from_secs(1), status_sent.notified())
            .await
            .expect("flush reaches live-lock backoff");

        tokio::time::timeout(Duration::from_secs(1), transaction.rollback())
            .await
            .expect("rollback cancellation interrupts the flush retry")
            .unwrap();
        assert!(context.resolving_locks().await.is_empty());
    }

    #[cfg(not(feature = "nextgen"))]
    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_pipelined_memdb_test_TestPipelinedCommit() {
        let heartbeats = Arc::new(Mutex::new(Vec::new()));
        let heartbeats_by_hook = heartbeats.clone();
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let statuses_by_hook = statuses.clone();
        let flush_min_commit_ts = Arc::new(Mutex::new(Vec::new()));
        let flush_min_commit_ts_by_hook = flush_min_commit_ts.clone();
        let status_sent = Arc::new(tokio::sync::Notify::new());
        let status_sent_by_hook = status_sent.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::FlushRequest>() {
                    flush_min_commit_ts_by_hook
                        .lock()
                        .unwrap()
                        .push((request.generation, request.min_commit_ts));
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::TxnHeartBeatRequest>() {
                    heartbeats_by_hook.lock().unwrap().push((
                        request.primary_lock.clone(),
                        request.start_version,
                        request.advise_lock_ttl,
                        request.min_commit_ts,
                    ));
                    return Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::BroadcastTxnStatusRequest>()
                {
                    statuses_by_hook
                        .lock()
                        .unwrap()
                        .extend(request.txn_status.clone());
                    status_sent_by_hook.notify_one();
                    return Ok(
                        Box::<kvrpcpb::BroadcastTxnStatusResponse>::default() as Box<dyn Any>
                    );
                }
                panic!("unexpected pipelined-heartbeat request");
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(10));
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
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(1)))
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .put("primary".to_owned(), "value".to_owned())
            .await
            .unwrap();
        assert!(transaction.maybe_flush_pipelined(true).await.unwrap());
        transaction.buffer.mem_buffer().flush_wait().unwrap();
        tokio::time::timeout(Duration::from_secs(1), status_sent.notified())
            .await
            .expect("successful primary flush starts heartbeat and broadcast");

        let heartbeat = heartbeats.lock().unwrap()[0].clone();
        assert_eq!(heartbeat.0, b"primary");
        assert_eq!(heartbeat.1, 1);
        assert!(heartbeat.2 >= super::MAX_TTL);
        assert!(heartbeat.3 >= 10);
        let status = statuses.lock().unwrap()[0].clone();
        assert_eq!(status.start_ts, 1);
        assert_eq!(status.min_commit_ts, heartbeat.3);
        assert_eq!(status.commit_ts, 0);
        assert!(!status.rolled_back);
        assert!(!status.is_completed);
        {
            let memdb = transaction.get_mem_buffer();
            memdb.set(b"secondary", b"value").unwrap();
            assert!(memdb.flush(true).unwrap());
            memdb.flush_wait().unwrap();
        }
        let flush_min_commit_ts = flush_min_commit_ts.lock().unwrap().clone();
        assert_eq!(flush_min_commit_ts[0], (1, 2));
        assert_eq!(flush_min_commit_ts[1], (2, 2));
        transaction.pipelined_cancellation.cancel();
        assert!(transaction
            .pipelined_heartbeat_started
            .load(Ordering::Acquire));
    }

    #[cfg(not(feature = "nextgen"))]
    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_pipelined_memdb_test_TestPipelinedDMLFailedByPKRollback() {
        let flushes = Arc::new(AtomicUsize::new(0));
        let flushes_by_hook = flushes.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if request.is::<kvrpcpb::FlushRequest>() {
                    flushes_by_hook.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::TxnHeartBeatRequest>() {
                    return Ok(Box::new(kvrpcpb::TxnHeartBeatResponse {
                        error: Some(kvrpcpb::KeyError {
                            abort: "terminal primary rejection".to_owned(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("unexpected terminal-heartbeat request");
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
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(1)))
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .put("primary".to_owned(), "value".to_owned())
            .await
            .unwrap();
        assert!(transaction.maybe_flush_pipelined(true).await.unwrap());
        transaction.buffer.mem_buffer().flush_wait().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !transaction
                .pipelined_heartbeat_failed
                .load(Ordering::Acquire)
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("terminal heartbeat error closes the TTL manager");

        let memdb = transaction.get_mem_buffer();
        memdb.set(b"secondary", b"value").unwrap();
        assert!(memdb.flush(true).unwrap());
        assert_eq!(
            memdb.flush_wait().unwrap_err().to_string(),
            "ttl manager is closed"
        );
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        transaction.pipelined_cancellation.cancel();
    }

    #[cfg(not(feature = "nextgen"))]
    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_pipelined_memdb_test_TestPipelinedDMLFailedByPKMaxTTLExceeded(
    ) {
        let flushes = Arc::new(AtomicUsize::new(0));
        let flushes_by_hook = flushes.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::FlushRequest>());
                flushes_by_hook.fetch_add(1, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::FlushResponse>::default() as Box<dyn Any>)
            },
        )));
        let start_physical = 1_000_i64;
        rpc.set_timestamp(Timestamp {
            physical: start_physical + super::MAX_TXN_TIME_USE as i64 + 1,
            logical: 0,
            ..Default::default()
        });
        let mut transaction = Transaction::new(
            Timestamp {
                physical: start_physical,
                logical: 0,
                ..Default::default()
            },
            rpc,
            TransactionOptions::new_optimistic()
                .pipelined(super::PipelinedTxnOptions {
                    enable: true,
                    flush_concurrency: 1,
                    resolve_lock_concurrency: 1,
                    write_throttle_ratio: 0.0,
                })
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(1)))
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        transaction
            .put("primary".to_owned(), "value".to_owned())
            .await
            .unwrap();
        assert!(transaction.maybe_flush_pipelined(true).await.unwrap());
        transaction.buffer.mem_buffer().flush_wait().unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !transaction
                .pipelined_heartbeat_failed
                .load(Ordering::Acquire)
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("maximum pipelined lifetime closes the TTL manager");

        let memdb = transaction.get_mem_buffer();
        memdb.set(b"secondary", b"value").unwrap();
        assert!(memdb.flush(true).unwrap());
        assert_eq!(
            memdb.flush_wait().unwrap_err().to_string(),
            "ttl manager is closed"
        );
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        transaction.pipelined_cancellation.cancel();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn source_pipelined_heartbeat_honors_larger_configured_maximum_lifetime() {
        let restore = crate::config::update_global(|config| {
            config.max_txn_ttl = super::MAX_TXN_TIME_USE + 1_000;
        });
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let captured_heartbeats = Arc::clone(&heartbeats);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                assert!(request.is::<kvrpcpb::TxnHeartBeatRequest>());
                captured_heartbeats.fetch_add(1, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::TxnHeartBeatResponse>::default() as Box<dyn Any>)
            },
        )));
        let start_physical = 1_000_i64;
        rpc.set_timestamp(Timestamp {
            physical: start_physical + super::MAX_TXN_TIME_USE as i64 + 1,
            logical: 0,
            ..Default::default()
        });
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("primary", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().pipelined(super::PipelinedTxnOptions {
                enable: true,
                flush_concurrency: 1,
                resolve_lock_concurrency: 1,
                write_throttle_ratio: 0.0,
            }),
            CommitSettings::default(),
        );
        committer.start_version = Timestamp {
            physical: start_physical,
            logical: 0,
            ..Default::default()
        };

        assert!(!committer.send_pipelined_heartbeat().await.unwrap());
        assert_eq!(heartbeats.load(Ordering::SeqCst), 1);
        restore();
    }

    fn source_txn_file_admission_committer() -> Committer<MockPdClient> {
        source_test_committer(
            Arc::new(MockPdClient::default()),
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        )
    }

    #[test]
    #[serial_test::serial]
    #[allow(non_snake_case)]
    fn source_go_txnkv_transaction_TestUseTxnFileExcludesPipelinedTxn() {
        let restore = crate::config::update_global(|config| {
            config.tikv_client.txn_chunk_writer_addr = "127.0.0.1".to_owned();
            config.tikv_client.txn_file_min_mutation_size = 0;
        });
        let mut committer = source_txn_file_admission_committer();
        committer.settings.assertion_level = kvrpcpb::AssertionLevel::Strict;
        assert!(committer.should_use_txn_file());

        committer.settings.pipelined.enable = true;

        assert!(!committer.should_use_txn_file());
        restore();
    }

    #[test]
    #[serial_test::serial]
    #[allow(non_snake_case)]
    fn source_go_txnkv_transaction_TestUseTxnFileExcludesSharedLockTxn() {
        let restore = crate::config::update_global(|config| {
            config.tikv_client.txn_chunk_writer_addr = "127.0.0.1".to_owned();
            config.tikv_client.txn_file_min_mutation_size = 0;
        });
        let mut committer = source_txn_file_admission_committer();
        assert!(committer.should_use_txn_file());

        committer.mutations = vec![source_test_mutation("key", kvrpcpb::Op::SharedLock)];

        assert!(!committer.should_use_txn_file());
        restore();
    }

    #[test]
    #[serial_test::serial]
    #[allow(non_snake_case)]
    fn source_go_txnkv_transaction_TestUseTxnFileExcludesMutationAssertions() {
        let restore = crate::config::update_global(|config| {
            config.tikv_client.txn_chunk_writer_addr = "127.0.0.1".to_owned();
            config.tikv_client.txn_file_min_mutation_size = 0;
        });
        for (assertion_level, assertion, expected) in [
            (
                kvrpcpb::AssertionLevel::Strict,
                kvrpcpb::Assertion::Exist,
                false,
            ),
            (
                kvrpcpb::AssertionLevel::Strict,
                kvrpcpb::Assertion::NotExist,
                false,
            ),
            (
                kvrpcpb::AssertionLevel::Strict,
                kvrpcpb::Assertion::None,
                true,
            ),
            (
                kvrpcpb::AssertionLevel::Off,
                kvrpcpb::Assertion::Exist,
                true,
            ),
        ] {
            let mut committer = source_txn_file_admission_committer();
            committer.settings.assertion_level = assertion_level;
            committer.mutations[0].assertion = assertion as i32;

            assert_eq!(committer.should_use_txn_file(), expected);
        }
        restore();
    }

    #[test]
    #[serial_test::serial]
    fn source_uncovered_txn_file_admission_native_exclusions() {
        let restore = crate::config::update_global(|config| {
            config.tikv_client.txn_chunk_writer_addr = "127.0.0.1".to_owned();
            config.tikv_client.txn_file_min_mutation_size = 0;
        });
        let base = source_txn_file_admission_committer();

        let mut filtered_size = base.clone();
        filtered_size.write_size = 0;
        filtered_size.buffer_size = 1;
        assert!(
            filtered_size.should_use_txn_file(),
            "txn-file admission uses full MemDB size, not filtered prewrite size"
        );

        let mut pessimistic = base.clone();
        pessimistic.options = TransactionOptions::new_pessimistic();
        assert!(!pessimistic.should_use_txn_file());

        let mut disabled = base;
        disabled.settings.txn_file_disabled = true;
        assert!(!disabled.should_use_txn_file());
        restore();
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFilePrewriteTaggerUsesFirstKeyWithoutSampleDataKeys(
    ) {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PrewriteRequest>()
                    .expect("txn-file prewrite dispatch");
                assert!(request.mutations.is_empty());
                assert!(request
                    .context
                    .as_ref()
                    .unwrap()
                    .resource_group_tag
                    .is_empty());
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
            },
        )));
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let captured_tagger_calls = Arc::clone(&tagger_calls);
        let mut settings = CommitSettings::default();
        settings.resource_group_tagger = Some(Arc::new(move |request| {
            let request = request
                .as_any()
                .downcast_ref::<kvrpcpb::PrewriteRequest>()
                .expect("txn-file prewrite tagger receives Prewrite");
            assert_eq!(request.mutations.len(), 1);
            assert_eq!(request.mutations[0].key, b"k");
            captured_tagger_calls.fetch_add(1, Ordering::SeqCst);
        }));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        let mut batch = source_test_chunk_batch(true);
        batch.sample_data_keys.clear();

        assert!(!committer.prewrite_txn_file_batch(&batch).await.unwrap());
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 1);
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFilePrewriteTaggerAppliesWithoutFirstKey() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PrewriteRequest>()
                    .expect("txn-file prewrite dispatch");
                assert!(request.mutations.is_empty());
                assert_eq!(
                    request.context.as_ref().unwrap().resource_group_tag,
                    b"metadata-tag"
                );
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
            },
        )));
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let captured_tagger_calls = Arc::clone(&tagger_calls);
        let mut settings = CommitSettings::default();
        settings.resource_group_tagger = Some(Arc::new(move |request| {
            let prewrite = request
                .as_any()
                .downcast_ref::<kvrpcpb::PrewriteRequest>()
                .expect("txn-file prewrite tagger receives Prewrite");
            assert!(prewrite.mutations.is_empty());
            captured_tagger_calls.fetch_add(1, Ordering::SeqCst);
            request.set_resource_group_tag(b"metadata-tag".to_vec());
        }));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        let mut batch = source_test_chunk_batch(true);
        batch.first_key.clear();
        batch.sample_data_keys.clear();

        assert!(!committer.prewrite_txn_file_batch(&batch).await.unwrap());
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 1);
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn source_uncovered_txn_file_tagger_static_tag_wins() {
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

    type SourceTxnFileActionObservation = (&'static str, Vec<u8>, u64);

    fn source_txn_file_action_fixture(
        static_tag: Option<Vec<u8>>,
    ) -> (
        Committer<MockPdClient>,
        ChunkBatch,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<SourceTxnFileActionObservation>>>,
    ) {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    assert!(request.mutations.is_empty());
                    assert_eq!(request.txn_file_chunks, [1]);
                    assert_eq!(request.primary_lock, b"k");
                    assert_eq!(
                        request
                            .context
                            .as_ref()
                            .unwrap()
                            .resource_control_context
                            .as_ref()
                            .unwrap()
                            .resource_group_name,
                        "txn-file-test"
                    );
                    captured.lock().unwrap().push((
                        "prewrite",
                        request.context.as_ref().unwrap().resource_group_tag.clone(),
                        request.context.as_ref().unwrap().max_execution_duration_ms,
                    ));
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                    assert_eq!(request.keys, [b"k".to_vec()]);
                    assert_eq!(
                        request
                            .context
                            .as_ref()
                            .unwrap()
                            .resource_control_context
                            .as_ref()
                            .unwrap()
                            .resource_group_name,
                        "txn-file-test"
                    );
                    captured.lock().unwrap().push((
                        "commit",
                        request.context.as_ref().unwrap().resource_group_tag.clone(),
                        request.context.as_ref().unwrap().max_execution_duration_ms,
                    ));
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                let request = request
                    .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                    .expect("txn-file cleanup sends BatchRollback");
                assert_eq!(request.keys, [b"k".to_vec()]);
                assert_eq!(
                    request
                        .context
                        .as_ref()
                        .unwrap()
                        .resource_control_context
                        .as_ref()
                        .unwrap()
                        .resource_group_name,
                    "txn-file-test"
                );
                captured.lock().unwrap().push((
                    "rollback",
                    request.context.as_ref().unwrap().resource_group_tag.clone(),
                    request.context.as_ref().unwrap().max_execution_duration_ms,
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
            assert_eq!(request.resource_group_name(), Some("txn-file-test"));
            if let Some(request) = request
                .as_any_mut()
                .downcast_mut::<kvrpcpb::PrewriteRequest>()
            {
                assert_eq!(request.mutations.len(), 1);
                assert_eq!(request.mutations[0].key, b"k");
                request.primary_lock[0] = b'x';
                request.txn_file_chunks[0] = 99;
                request.set_resource_group_name("tagger-mutated");
            } else if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::CommitRequest>()
            {
                assert_eq!(request.keys, [b"k".to_vec()]);
            } else {
                let request = request
                    .as_any()
                    .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                    .expect("tagger receives a txn-file action");
                assert_eq!(request.keys, [b"k".to_vec()]);
            }
            request.set_resource_group_tag(b"dynamic".to_vec());
        }));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        committer.resource_group_name = Some("txn-file-test".to_owned());
        committer.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        let mut chunks = TxnChunkSlice::default();
        chunks.push(1, TxnChunkRange::new(b"k".to_vec(), b"k".to_vec(), 1));
        let batch = ChunkBatch {
            chunks,
            region: MockPdClient::region2(),
            first_key: b"k".to_vec(),
            sample_data_keys: vec![b"k".to_vec()],
            is_primary: true,
        };

        (committer, batch, calls, observed)
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileActionsApplyResourceGroupTagger() {
        for (action, expected_label, expected_timeout) in [
            (TxnFileAction::Prewrite, "prewrite", 60_000),
            (TxnFileAction::Commit, "commit", 60_000),
            (TxnFileAction::Rollback, "rollback", 30_000),
        ] {
            let (mut committer, batch, tagger_calls, observed) =
                source_txn_file_action_fixture(None);

            let retry = match action {
                TxnFileAction::Prewrite => committer.prewrite_txn_file_batch(&batch).await,
                TxnFileAction::Commit => committer.commit_txn_file_batch(&batch).await,
                TxnFileAction::Rollback => committer.rollback_txn_file_batch(&batch).await,
            }
            .unwrap();

            assert!(!retry);
            assert_eq!(tagger_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                *observed.lock().unwrap(),
                vec![(expected_label, b"dynamic".to_vec(), expected_timeout)]
            );
        }
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileActionsPreserveStaticResourceGroupTag() {
        for (action, expected_label, expected_timeout) in [
            (TxnFileAction::Prewrite, "prewrite", 60_000),
            (TxnFileAction::Commit, "commit", 60_000),
            (TxnFileAction::Rollback, "rollback", 30_000),
        ] {
            let (mut committer, batch, tagger_calls, observed) =
                source_txn_file_action_fixture(Some(b"static".to_vec()));

            let retry = match action {
                TxnFileAction::Prewrite => committer.prewrite_txn_file_batch(&batch).await,
                TxnFileAction::Commit => committer.commit_txn_file_batch(&batch).await,
                TxnFileAction::Rollback => committer.rollback_txn_file_batch(&batch).await,
            }
            .unwrap();

            assert!(!retry);
            assert_eq!(tagger_calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                *observed.lock().unwrap(),
                vec![(expected_label, b"static".to_vec(), expected_timeout)]
            );
        }
    }

    #[tokio::test]
    async fn source_txn_file_actions_mark_requests_after_prior_backoff() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured.lock().unwrap().push((
                        "prewrite",
                        request.context.as_ref().unwrap().is_retry_request,
                    ));
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                    captured
                        .lock()
                        .unwrap()
                        .push(("commit", request.context.as_ref().unwrap().is_retry_request));
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                let request = request
                    .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                    .expect("txn-file cleanup sends BatchRollback");
                captured.lock().unwrap().push((
                    "rollback",
                    request.context.as_ref().unwrap().is_retry_request,
                ));
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        committer.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        let batch = source_test_chunk_batch(true);

        assert!(!committer
            .prewrite_txn_file_batch_with_retry(&batch, true)
            .await
            .unwrap());
        assert!(!committer
            .commit_txn_file_batch_with_retry(&batch, true)
            .await
            .unwrap());
        assert!(!committer
            .rollback_txn_file_batch_with_retry(&batch, true)
            .await
            .unwrap());
        assert_eq!(
            *observed.lock().unwrap(),
            vec![("prewrite", true), ("commit", true), ("rollback", true)]
        );
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileCleanupContextUsesStoreContext() {
        let caller = crate::async_util::Cancellation::default();
        caller.cancel();
        assert!(caller.is_cancelled());
        let observed_start_ts = Arc::new(Mutex::new(Vec::new()));
        let captured_start_ts = Arc::clone(&observed_start_ts);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                    .expect("txn-file cleanup sends BatchRollback");
                captured_start_ts
                    .lock()
                    .unwrap()
                    .push(request.start_version);
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        // Rust does not accept a caller context on transaction methods; its
        // txn-file cleanup owner is detached by construction. The explicit
        // request field is the native counterpart of Go's retained TxnStartKey.
        committer.start_version = Timestamp::from_version(42);
        assert!(!committer
            .rollback_txn_file_batch(&source_test_chunk_batch(true))
            .await
            .unwrap());
        assert_eq!(*observed_start_ts.lock().unwrap(), vec![42]);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFilePrewriteUsesPrimaryKey() {
        let dispatches = Arc::new(AtomicUsize::new(0));
        let captured_dispatches = Arc::clone(&dispatches);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PrewriteRequest>()
                    .expect("txn-file prewrite dispatch");
                assert_eq!(request.primary_lock, b"primary");
                captured_dispatches.fetch_add(1, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
            },
        )));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        let batch = source_test_chunk_batch(true);
        assert!(!committer.prewrite_txn_file_batch(&batch).await.unwrap());
        assert_eq!(dispatches.load(Ordering::SeqCst), 1);
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_txnkv_transaction_TestTxnFilePrimaryBatchIndexFindsPrimaryRegion() {
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
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFilePrimaryRollbackPropagatesKeyError() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                request
                    .downcast_ref::<kvrpcpb::BatchRollbackRequest>()
                    .expect("cleanup uses BatchRollback");
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
        settings.session_id = 7;
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        let error = committer
            .rollback_txn_file_batch(&source_test_chunk_batch(true))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("session 7 txn file cleanup failed"),
            "{error}"
        );
        assert!(error.to_string().contains("primary rollback failed"));
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFilePrewriteExpandsSharedLockHolders() {
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
    async fn source_txn_file_prewrite_keeps_disk_full_terminal_and_key_exists_typed() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::new(kvrpcpb::PrewriteResponse {
                region_error: Some(crate::proto::errorpb::Error {
                    disk_full: Some(Default::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }) as Box<dyn Any>)
        })));
        let mut disk_full = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        assert!(matches!(
            disk_full
                .prewrite_txn_file_batch(&source_test_chunk_batch(true))
                .await
                .unwrap_err(),
            Error::RegionError(error) if error.disk_full.is_some()
        ));

        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::new(kvrpcpb::PrewriteResponse {
                errors: vec![kvrpcpb::KeyError {
                    already_exist: Some(kvrpcpb::AlreadyExist { key: b"k".to_vec() }),
                    ..Default::default()
                }],
                ..Default::default()
            }) as Box<dyn Any>)
        })));
        let mut insert = source_test_committer(
            rpc.clone(),
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Insert)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        assert!(matches!(
            insert
                .prewrite_txn_file_batch(&source_test_chunk_batch(true))
                .await
                .unwrap_err(),
            Error::KeyExists(_)
        ));

        let mut impossible = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        assert!(impossible
            .prewrite_txn_file_batch(&source_test_chunk_batch(true))
            .await
            .unwrap_err()
            .to_string()
            .contains("existErr for key"));
    }

    #[tokio::test]
    async fn source_txn_file_commit_extracts_typed_errors_and_cleanup_wraps_key_errors() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request: &dyn Any| {
                if request.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
                    return Ok(Box::new(kvrpcpb::CommitResponse {
                        error: Some(kvrpcpb::KeyError {
                            retryable: "retry transaction".to_owned(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::new(kvrpcpb::BatchRollbackResponse {
                    error: Some(kvrpcpb::KeyError {
                        abort: "cleanup rejected".to_owned(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut settings = CommitSettings::default();
        settings.session_id = 42;
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"primary".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        committer.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        let batch = source_test_chunk_batch(true);

        assert!(matches!(
            committer.commit_txn_file_batch(&batch).await.unwrap_err(),
            Error::RetryableKey(_)
        ));
        let cleanup_error = committer.rollback_txn_file_batch(&batch).await.unwrap_err();
        assert!(cleanup_error
            .to_string()
            .contains("session 42 txn file cleanup failed"));
    }

    #[tokio::test]
    async fn source_standard_commit_primary_region_undetermined_is_ambiguous() {
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            |request| {
                assert!(request.downcast_ref::<kvrpcpb::CommitRequest>().is_some());
                Ok(Box::new(kvrpcpb::CommitResponse {
                    region_error: Some(crate::proto::errorpb::Error {
                        undetermined_result: Some(Default::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );

        let error = committer.commit_primary_with_retry().await.unwrap_err();
        assert!(matches!(
            error,
            Error::RegionError(ref region_error)
                if region_error.undetermined_result.is_some()
        ));
        assert!(committer.undetermined);
    }

    #[tokio::test]
    async fn source_standard_commit_request_marks_primary_role_and_key() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request| {
                let request = request
                    .downcast_ref::<kvrpcpb::CommitRequest>()
                    .expect("primary commit sends Commit");
                captured.lock().unwrap().push(request.clone());
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );

        committer.commit_primary().await.unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].commit_role, kvrpcpb::CommitRole::Primary as i32);
        assert_eq!(requests[0].primary_key, b"k");
        assert!(!requests[0].use_async_commit);
        assert_eq!(
            requests[0]
                .context
                .as_ref()
                .expect("commit carries a request context")
                .max_execution_duration_ms,
            20_000
        );
    }

    #[tokio::test]
    async fn source_prewrite_and_primary_commit_use_cumulative_retry_owners() {
        fn retry_owner(max_sleep_ms: u64) -> Arc<tokio::sync::Mutex<super::RetryBackoffer>> {
            Arc::new(tokio::sync::Mutex::new(super::RetryBackoffer::new(
                crate::async_util::Cancellation::default(),
                max_sleep_ms,
            )))
        }

        let prewrite_calls = Arc::new(AtomicUsize::new(0));
        let captured_prewrite_calls = Arc::clone(&prewrite_calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request| {
                assert!(request.is::<kvrpcpb::PrewriteRequest>());
                captured_prewrite_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::PrewriteResponse {
                    region_error: Some(crate::proto::errorpb::Error {
                        not_leader: Some(crate::proto::errorpb::NotLeader {
                            region_id: 2,
                            leader: None,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut prewrite = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        let prewrite_owner = retry_owner(1);
        prewrite
            .prewrite_with_retry_owner(Some(Arc::clone(&prewrite_owner)))
            .await
            .unwrap_err();
        assert_eq!(prewrite_calls.load(Ordering::SeqCst), 2);
        assert!(prewrite_owner.lock().await.total_sleep_ms() >= 1);

        let commit_calls = Arc::new(AtomicUsize::new(0));
        let captured_commit_calls = Arc::clone(&commit_calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request| {
                assert!(request.is::<kvrpcpb::CommitRequest>());
                captured_commit_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::CommitResponse {
                    region_error: Some(crate::proto::errorpb::Error {
                        not_leader: Some(crate::proto::errorpb::NotLeader {
                            region_id: 2,
                            leader: None,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let mut commit = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        let commit_owner = retry_owner(1);
        let mut discard_values: Option<fn()> = None;
        let mut retained_request = None;
        commit
            .commit_primary_at_version(
                Timestamp::from_version(2),
                &mut discard_values,
                Some(Arc::clone(&commit_owner)),
                &mut retained_request,
            )
            .await
            .unwrap_err();
        assert_eq!(commit_calls.load(Ordering::SeqCst), 2);
        assert!(commit_owner.lock().await.total_sleep_ms() >= 1);

        let custom = source_test_committer(
            Arc::new(MockPdClient::default()),
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().retry_options(RetryOptions::none()),
            CommitSettings::default(),
        );
        assert!(custom.source_retry_owner(1).is_none());
    }

    #[test]
    fn source_commit_ts_expired_gap_uses_go_uint64_wrapping() {
        let expired = |min_commit_ts, attempted_commit_ts| kvrpcpb::CommitTsExpired {
            min_commit_ts,
            attempted_commit_ts,
            ..Default::default()
        };

        assert!(!super::commit_ts_expired_gap_is_too_large(&expired(2, 1)));
        assert!(!super::commit_ts_expired_gap_is_too_large(&expired(
            super::MAX_COMMIT_TS_EXPIRED_GAP + 1,
            1,
        )));
        assert!(super::commit_ts_expired_gap_is_too_large(&expired(
            super::MAX_COMMIT_TS_EXPIRED_GAP + 2,
            1,
        )));
        assert!(super::commit_ts_expired_gap_is_too_large(&expired(1, 2)));
    }

    #[tokio::test]
    async fn source_standard_commit_ts_expired_reallocates_without_revalidating() {
        struct Version;
        impl super::SchemaVersion for Version {
            fn schema_meta_version(&self) -> i64 {
                10
            }
        }

        struct Checker(Arc<AtomicUsize>);
        impl super::SchemaLeaseChecker for Checker {
            fn check_by_schema_version(
                &self,
                _timestamp: u64,
                _version: &dyn super::SchemaVersion,
            ) -> crate::Result<super::RelatedSchemaChange> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(super::RelatedSchemaChange {
                    physical_table_ids: Vec::new(),
                    action_types: Vec::new(),
                    latest_info_schema: Arc::new(Version),
                })
            }
        }

        let commit_versions = Arc::new(Mutex::new(Vec::new()));
        let captured_versions = Arc::clone(&commit_versions);
        let primary_keys = Arc::new(Mutex::new(Vec::new()));
        let captured_primary_keys = Arc::clone(&primary_keys);
        let requests = Arc::new(AtomicUsize::new(0));
        let captured_requests = Arc::clone(&requests);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request| {
                let request = request
                    .downcast_ref::<kvrpcpb::CommitRequest>()
                    .expect("primary commit sends Commit");
                captured_versions
                    .lock()
                    .unwrap()
                    .push(request.commit_version);
                captured_primary_keys
                    .lock()
                    .unwrap()
                    .push(request.primary_key.clone());
                if captured_requests.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Ok(Box::new(kvrpcpb::CommitResponse {
                        error: Some(kvrpcpb::KeyError {
                            commit_ts_expired: Some(kvrpcpb::CommitTsExpired {
                                start_ts: 1,
                                attempted_commit_ts: request.commit_version,
                                key: b"k".to_vec(),
                                min_commit_ts: request.commit_version + 1,
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(9));
        let schema_checks = Arc::new(AtomicUsize::new(0));
        let upper_bound_checks = Arc::new(AtomicUsize::new(0));
        let captured_upper_bound_checks = Arc::clone(&upper_bound_checks);
        let mut settings = CommitSettings::default();
        settings.schema_version = Some(Arc::new(Version));
        settings.schema_lease_checker = Some(Arc::new(Checker(Arc::clone(&schema_checks))));
        settings.commit_timestamp_upper_bound = Some(Arc::new(move |_| {
            captured_upper_bound_checks.fetch_add(1, Ordering::SeqCst);
            true
        }));
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let captured_tagger_calls = Arc::clone(&tagger_calls);
        settings.resource_group_tagger = Some(Arc::new(move |request| {
            let call = captured_tagger_calls.fetch_add(1, Ordering::SeqCst) + 1;
            let request = request
                .as_any_mut()
                .downcast_mut::<kvrpcpb::CommitRequest>()
                .expect("primary Commit is taggable as its concrete request");
            request.primary_key = format!("tagged-primary-{call}").into_bytes();
            request
                .context
                .get_or_insert_with(kvrpcpb::Context::default)
                .resource_group_tag = format!("dynamic-tag-{call}").into_bytes();
        }));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );

        assert_eq!(
            committer
                .commit_primary_with_retry()
                .await
                .unwrap()
                .version(),
            9
        );
        assert_eq!(*commit_versions.lock().unwrap(), vec![9, 9]);
        assert_eq!(
            *primary_keys.lock().unwrap(),
            vec![b"tagged-primary-1".to_vec(), b"tagged-primary-1".to_vec()]
        );
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 1);
        assert_eq!(schema_checks.load(Ordering::SeqCst), 1);
        assert_eq!(upper_bound_checks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_standard_commit_timestamp_checks_schema_before_lifetime_and_upper_bound() {
        struct Version;
        impl super::SchemaVersion for Version {
            fn schema_meta_version(&self) -> i64 {
                10
            }
        }

        struct Checker {
            events: Arc<Mutex<Vec<&'static str>>>,
            reject: bool,
        }
        impl super::SchemaLeaseChecker for Checker {
            fn check_by_schema_version(
                &self,
                _timestamp: u64,
                _version: &dyn super::SchemaVersion,
            ) -> crate::Result<super::RelatedSchemaChange> {
                self.events.lock().unwrap().push("schema");
                if self.reject {
                    return Err(Error::StringError("schema changed".to_owned()));
                }
                Ok(super::RelatedSchemaChange {
                    physical_table_ids: Vec::new(),
                    action_types: Vec::new(),
                    latest_info_schema: Arc::new(Version),
                })
            }
        }

        let committer = |events: Arc<Mutex<Vec<&'static str>>>, reject_schema| {
            let mut settings = CommitSettings::default();
            settings.schema_version = Some(Arc::new(Version));
            settings.schema_lease_checker = Some(Arc::new(Checker {
                events: Arc::clone(&events),
                reject: reject_schema,
            }));
            settings.commit_timestamp_upper_bound = Some(Arc::new(move |_| {
                events.lock().unwrap().push("upper-bound");
                false
            }));
            let rpc = Arc::new(MockPdClient::default());
            rpc.set_timestamp(Timestamp::from_version(100));
            source_test_committer(
                rpc,
                Some(Key::from(b"k".to_vec())),
                vec![source_test_mutation("k", kvrpcpb::Op::Put)],
                TransactionOptions::new_optimistic(),
                settings,
            )
        };

        let events = Arc::new(Mutex::new(Vec::new()));
        let schema_error = committer(Arc::clone(&events), true)
            .prepare_primary_commit_timestamp()
            .await
            .unwrap_err();
        assert!(schema_error.to_string().contains("schema changed"));
        assert_eq!(*events.lock().unwrap(), vec!["schema"]);

        let events = Arc::new(Mutex::new(Vec::new()));
        let mut expired = committer(Arc::clone(&events), false);
        expired.start_instant = Instant::now() - Duration::from_millis(super::MAX_TXN_TIME_USE + 1);
        let lifetime_error = expired
            .prepare_primary_commit_timestamp()
            .await
            .unwrap_err();
        assert!(lifetime_error
            .to_string()
            .contains("txn takes too much time"));
        assert_eq!(*events.lock().unwrap(), vec!["schema"]);

        let events = Arc::new(Mutex::new(Vec::new()));
        let upper_bound_error = committer(Arc::clone(&events), false)
            .prepare_primary_commit_timestamp()
            .await
            .unwrap_err();
        assert!(upper_bound_error
            .to_string()
            .contains("check commit ts upper bound fail"));
        assert_eq!(*events.lock().unwrap(), vec!["schema", "upper-bound"]);
    }

    #[tokio::test]
    async fn source_standard_commit_retains_transport_ambiguity_until_a_definitive_response() {
        let calls = Arc::new(AtomicUsize::new(0));
        let captured_calls = Arc::clone(&calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_| {
                if captured_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(Error::GrpcAPI(tonic::Status::unavailable(
                        "primary response lost",
                    )));
                }
                Ok(Box::new(kvrpcpb::CommitResponse {
                    region_error: Some(crate::proto::errorpb::Error {
                        server_is_busy: Some(Default::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let options = TransactionOptions::new_optimistic().retry_options(RetryOptions::new(
            Backoff::no_jitter_backoff(0, 0, 1),
            Backoff::no_backoff(),
        ));
        let mut ambiguous = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            options,
            CommitSettings::default(),
        );
        ambiguous.commit_primary_with_retry().await.unwrap_err();
        assert!(calls.load(Ordering::SeqCst) >= 2);
        assert!(ambiguous.undetermined);

        let calls = Arc::new(AtomicUsize::new(0));
        let captured_calls = Arc::clone(&calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_| {
                if captured_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(Error::GrpcAPI(tonic::Status::unavailable(
                        "primary response lost",
                    )));
                }
                Ok(Box::new(kvrpcpb::CommitResponse {
                    error: Some(kvrpcpb::KeyError {
                        abort: "definitive primary rejection".to_owned(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let options = TransactionOptions::new_optimistic().retry_options(RetryOptions::new(
            Backoff::no_jitter_backoff(0, 0, 1),
            Backoff::no_backoff(),
        ));
        let mut definitive = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            options,
            CommitSettings::default(),
        );
        definitive.commit_primary_with_retry().await.unwrap_err();
        assert!(calls.load(Ordering::SeqCst) >= 2);
        assert!(!definitive.undetermined);
    }

    fn source_txn_file_retry_backoff(max_sleep_ms: u64) -> super::TxnFileRetryBackoff {
        super::TxnFileRetryBackoff::Source(Arc::new(tokio::sync::Mutex::new(
            super::RetryBackoffer::new(crate::async_util::Cancellation::default(), max_sleep_ms),
        )))
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileCommitPrimaryRPCErrorMarksResultUndetermined() {
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
        let error = primary
            .commit_txn_file_batch_with_backoff(
                &batch,
                &mut source_txn_file_retry_backoff(1),
                false,
            )
            .await
            .unwrap_err();
        assert!(primary.undetermined);
        assert!(matches!(
            primary.normalize_txn_file_commit_result::<()>(Err(error)),
            Err(Error::UndeterminedError(_))
        ));
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileCommitSecondaryRPCErrorIsNotResultUndetermined()
    {
        let batch = source_test_chunk_batch(true);
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
            .commit_txn_file_batch_with_backoff(
                &secondary_batch,
                &mut source_txn_file_retry_backoff(1),
                false,
            )
            .await
            .unwrap_err();
        assert!(!secondary.undetermined);
        assert!(!matches!(
            secondary.normalize_txn_file_commit_result::<()>(Err(error)),
            Err(Error::UndeterminedError(_))
        ));
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileCommitClearsUndeterminedErrOnDefinitivePrimaryResponse(
    ) {
        let batch = source_test_chunk_batch(true);
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

        let key_error_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(|_| {
            Ok(Box::new(kvrpcpb::CommitResponse {
                error: Some(kvrpcpb::KeyError {
                    abort: "aborted".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }) as Box<dyn Any>)
        })));
        let mut definitive_key_error = source_test_committer(
            key_error_rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        definitive_key_error.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
        definitive_key_error.undetermined = true;
        definitive_key_error
            .commit_txn_file_batch(&batch)
            .await
            .unwrap_err();
        assert!(!definitive_key_error.undetermined);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileCommitPrimaryUndeterminedRegionError() {
        let batch = source_test_chunk_batch(true);
        let request_count = Arc::new(AtomicUsize::new(0));
        let captured_request_count = Arc::clone(&request_count);
        let region_rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |_| {
                captured_request_count.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(kvrpcpb::CommitResponse {
                    region_error: Some(crate::proto::errorpb::Error {
                        undetermined_result: Some(Default::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
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
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileCommitTSExpiredRetryUsesPreparedTimestamp() {
        let batch = source_test_chunk_batch(true);
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
        let tagger_calls = Arc::new(AtomicUsize::new(0));
        let captured_tagger_calls = Arc::clone(&tagger_calls);
        let mut settings = CommitSettings::default();
        settings.schema_version = Some(Arc::new(Version(10)));
        settings.schema_lease_checker = Some(Arc::new(Checker(schema_checks.clone())));
        settings.commit_timestamp_upper_bound = Some(Arc::new(move |timestamp| {
            captured_upper_bound_calls.fetch_add(1, Ordering::SeqCst);
            timestamp == 9
        }));
        settings.resource_group_tagger = Some(Arc::new(move |request| {
            captured_tagger_calls.fetch_add(1, Ordering::SeqCst);
            request.set_resource_group_tag(b"txn-file".to_vec());
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
        assert_eq!(tagger_calls.load(Ordering::SeqCst), 1);
        assert_eq!(request_count.load(Ordering::SeqCst), 2);
        assert_eq!(
            retry.txn_file_commit_timestamp.as_ref().unwrap().version(),
            9
        );

        for (is_primary, expired_key) in [(false, b"k".to_vec()), (true, b"not-primary".to_vec())] {
            let request_count = Arc::new(AtomicUsize::new(0));
            let captured_request_count = Arc::clone(&request_count);
            let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
                move |request: &dyn Any| {
                    captured_request_count.fetch_add(1, Ordering::SeqCst);
                    let request = request
                        .downcast_ref::<kvrpcpb::CommitRequest>()
                        .expect("expired retry sends Commit");
                    Ok(Box::new(kvrpcpb::CommitResponse {
                        error: Some(kvrpcpb::KeyError {
                            commit_ts_expired: Some(kvrpcpb::CommitTsExpired {
                                start_ts: 1,
                                attempted_commit_ts: request.commit_version,
                                key: expired_key.clone(),
                                min_commit_ts: 5,
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>)
                },
            )));
            rpc.set_timestamp(Timestamp::from_version(11));
            let schema_checks = Arc::new(Mutex::new(Vec::new()));
            let upper_bound_calls = Arc::new(AtomicUsize::new(0));
            let captured_upper_bound_calls = Arc::clone(&upper_bound_calls);
            let mut settings = CommitSettings::default();
            settings.schema_version = Some(Arc::new(Version(10)));
            settings.schema_lease_checker = Some(Arc::new(Checker(Arc::clone(&schema_checks))));
            settings.commit_timestamp_upper_bound = Some(Arc::new(move |_| {
                captured_upper_bound_calls.fetch_add(1, Ordering::SeqCst);
                true
            }));
            let mut rejected = source_test_committer(
                rpc,
                Some(Key::from(b"k".to_vec())),
                vec![source_test_mutation("k", kvrpcpb::Op::Put)],
                TransactionOptions::new_optimistic(),
                settings,
            );
            rejected.txn_file_commit_timestamp = Some(Timestamp::from_version(2));
            let mut rejected_batch = batch.clone();
            rejected_batch.is_primary = is_primary;
            let error = rejected
                .commit_txn_file_batch(&rejected_batch)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("key is not the primary key"));
            assert_eq!(
                rejected
                    .txn_file_commit_timestamp
                    .as_ref()
                    .unwrap()
                    .version(),
                2
            );
            assert_eq!(request_count.load(Ordering::SeqCst), 1);
            assert!(schema_checks.lock().unwrap().is_empty());
            assert_eq!(upper_bound_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileCommitPrimaryRPCErrorIsNormalized() {
        crate::transaction::close_txn_file_idle_connections();
        let (address, uploaded_chunk) = source_test_txn_chunk_writer().await;
        let restore = crate::config::update_global(|config| {
            config.tikv_client.txn_chunk_writer_addr = address;
        });
        let prewrite_calls = Arc::new(AtomicUsize::new(0));
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let rollback_calls = Arc::new(AtomicUsize::new(0));
        let captured_prewrites = Arc::clone(&prewrite_calls);
        let captured_commits = Arc::clone(&commit_calls);
        let captured_rollbacks = Arc::clone(&rollback_calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    assert_eq!(request.txn_file_chunks, [1]);
                    captured_prewrites.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                    assert!(request.is_txn_file);
                    captured_commits.fetch_add(1, Ordering::SeqCst);
                    return Err(Error::GrpcAPI(tonic::Status::cancelled(
                        "primary response lost",
                    )));
                }
                captured_rollbacks.fetch_add(1, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let options = TransactionOptions::new_optimistic().retry_options(RetryOptions::new(
            Backoff::no_jitter_backoff(0, 0, 1),
            Backoff::no_backoff(),
        ));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            }],
            options,
            CommitSettings::default(),
        );
        let mut discard_values: Option<fn()> = None;
        let error = committer
            .execute_txn_file(&mut discard_values)
            .await
            .unwrap_err();
        assert!(
            matches!(
                error,
                Error::UndeterminedError(ref source)
                    if matches!(source.as_ref(), Error::GrpcAPI(status) if status.code() == tonic::Code::Cancelled)
            ),
            "unexpected normalized error: {error:?}"
        );
        restore();
        crate::transaction::close_txn_file_idle_connections();
        assert!(!uploaded_chunk.await.unwrap().is_empty());
        assert_eq!(prewrite_calls.load(Ordering::SeqCst), 1);
        assert_eq!(commit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(rollback_calls.load(Ordering::SeqCst), 0);
        assert!(committer.undetermined);
        assert!(!committer.committed);
    }

    #[tokio::test]
    #[serial_test::serial]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestTxnFileCommitPreservesCommitOnResourceControlResponseError(
    ) {
        struct FailingResponseController(Arc<AtomicUsize>);
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
                request: ResourceControlRequestInfo,
                _response: crate::ResourceControlResponseInfo,
            ) -> crate::Result<ResponseWaitResult> {
                if request.write_bytes() == 2 {
                    self.0.fetch_add(1, Ordering::SeqCst);
                    Err(Error::StringError(
                        "resource accounting unavailable after commit".to_owned(),
                    ))
                } else {
                    Ok(ResponseWaitResult::default())
                }
            }
        }

        crate::transaction::close_txn_file_idle_connections();
        let accounting_errors = crate::metrics::global_metrics()
            .counter_vec("TiKVTxnFileErrorCounter")
            .expect("txn-file error metric is registered")
            .with_label_values(&["accounting"]);
        let accounting_errors_before = accounting_errors.get();
        let (address, uploaded_chunk) = source_test_txn_chunk_writer().await;
        let restore = crate::config::update_global(|config| {
            config.tikv_client.txn_chunk_writer_addr = address;
            config.tikv_client.txn_file_min_mutation_size = 1;
            config.tikv_client.txn_file_ru_discount_ratio = 1.0;
        });

        let prewrite_calls = Arc::new(AtomicUsize::new(0));
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let rollback_calls = Arc::new(AtomicUsize::new(0));
        let captured_prewrites = Arc::clone(&prewrite_calls);
        let captured_commits = Arc::clone(&commit_calls);
        let captured_rollbacks = Arc::clone(&rollback_calls);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    assert_eq!(request.txn_file_chunks, [1]);
                    captured_prewrites.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                    assert!(request.is_txn_file);
                    captured_commits.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                captured_rollbacks.fetch_add(1, Ordering::SeqCst);
                Ok(Box::<kvrpcpb::BatchRollbackResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(2));
        let response_calls = Arc::new(AtomicUsize::new(0));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            }],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        committer.resource_group_name = Some("test-rg".to_owned());
        committer.resource_control = Some(Arc::new(FailingResponseController(Arc::clone(
            &response_calls,
        ))));
        let values_discarded = Arc::new(AtomicBool::new(false));
        let captured_discard = Arc::clone(&values_discarded);
        let mut discard_values = Some(move || {
            captured_discard.store(true, Ordering::SeqCst);
        });

        assert_eq!(
            committer.execute_commit(&mut discard_values).await.unwrap(),
            Some(Timestamp::from_version(2))
        );
        restore();
        crate::transaction::close_txn_file_idle_connections();

        let payload = uploaded_chunk.await.unwrap();
        let (serialized, checksum) = payload.split_at(payload.len() - 4);
        assert_eq!(
            serialized,
            [
                1_u16.to_le_bytes().as_slice(),
                b"k",
                &[kvrpcpb::Op::Put as u8],
                1_u32.to_le_bytes().as_slice(),
                b"v",
            ]
            .concat()
        );
        assert_eq!(
            u32::from_le_bytes(checksum.try_into().unwrap()),
            crc32fast::hash(serialized)
        );
        assert_eq!(prewrite_calls.load(Ordering::SeqCst), 1);
        assert_eq!(commit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(rollback_calls.load(Ordering::SeqCst), 0);
        assert_eq!(response_calls.load(Ordering::SeqCst), 1);
        assert_eq!(accounting_errors.get(), accounting_errors_before + 1.0);
        assert!(committer.committed);
        assert!(!committer.undetermined);
        assert!(values_discarded.load(Ordering::SeqCst));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn source_txn_file_bulk_resource_accounting_uses_discounted_buffer_size() {
        struct RecordingController {
            requests: Arc<Mutex<Vec<(String, ResourceControlRequestInfo)>>>,
            responses: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl ResourceGroupController for RecordingController {
            async fn on_request_wait(
                &self,
                resource_group_name: &str,
                request: ResourceControlRequestInfo,
            ) -> crate::Result<RequestWaitResult> {
                self.requests
                    .lock()
                    .unwrap()
                    .push((resource_group_name.to_owned(), request));
                Ok(RequestWaitResult {
                    consumption: resource_manager::Consumption {
                        w_r_u: 2.0,
                        ..Default::default()
                    },
                    wait_duration: Duration::from_millis(7),
                    ..Default::default()
                })
            }

            fn on_response_wait(
                &self,
                resource_group_name: &str,
                request: ResourceControlRequestInfo,
                response: crate::ResourceControlResponseInfo,
            ) -> crate::Result<ResponseWaitResult> {
                assert_eq!(resource_group_name, "test-rg");
                assert_eq!(request.write_bytes(), 200);
                assert_eq!(response, crate::ResourceControlResponseInfo::default());
                self.responses.fetch_add(1, Ordering::SeqCst);
                Ok(ResponseWaitResult {
                    consumption: resource_manager::Consumption {
                        w_r_u: 3.0,
                        ..Default::default()
                    },
                    // client-go deliberately does not add response wait time.
                    wait_duration: Duration::from_secs(1),
                })
            }
        }

        let restore = crate::config::update_global(|config| {
            config.tikv_client.txn_file_ru_discount_ratio = 0.25;
        });
        let mut region = MockPdClient::region2();
        region.region.peers = vec![
            crate::proto::metapb::Peer {
                role: crate::proto::metapb::PeerRole::Voter as i32,
                ..Default::default()
            },
            crate::proto::metapb::Peer {
                role: crate::proto::metapb::PeerRole::Learner as i32,
                ..Default::default()
            },
            crate::proto::metapb::Peer {
                role: crate::proto::metapb::PeerRole::Voter as i32,
                ..Default::default()
            },
        ];
        let rpc = Arc::new(MockPdClient::with_regions(vec![region]));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let responses = Arc::new(AtomicUsize::new(0));
        let controller = Arc::new(RecordingController {
            requests: Arc::clone(&requests),
            responses: Arc::clone(&responses),
        });
        let ru_details = Arc::new(crate::RuDetails::new());
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        committer.buffer_size = 800;
        committer.resource_group_name = Some("test-rg".to_owned());
        committer.resource_control = Some(controller);
        committer.ru_details = Some(Arc::clone(&ru_details));

        let accounting = committer
            .before_execute_txn_file_resource_control()
            .await
            .unwrap();
        assert_eq!(
            *requests.lock().unwrap(),
            vec![(
                "test-rg".to_owned(),
                ResourceControlRequestInfo::new(Some(200), 42, 2, false)
            )]
        );
        assert_eq!(ru_details.write_ru(), 2.0);
        assert_eq!(ru_details.ru_wait_duration(), Duration::from_millis(7));

        committer.after_execute_txn_file_resource_control(accounting);
        restore();
        assert_eq!(responses.load(Ordering::SeqCst), 1);
        assert_eq!(ru_details.write_ru(), 5.0);
        assert_eq!(ru_details.ru_wait_duration(), Duration::from_millis(7));
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestPrepareTxnFileCommitTS() {
        struct Version;
        impl super::SchemaVersion for Version {
            fn schema_meta_version(&self) -> i64 {
                10
            }
        }
        struct Checker {
            calls: Arc<Mutex<Vec<(u64, i64)>>>,
            failure: Option<&'static str>,
        }
        impl super::SchemaLeaseChecker for Checker {
            fn check_by_schema_version(
                &self,
                timestamp: u64,
                version: &dyn super::SchemaVersion,
            ) -> crate::Result<super::RelatedSchemaChange> {
                self.calls
                    .lock()
                    .unwrap()
                    .push((timestamp, version.schema_meta_version()));
                if let Some(failure) = self.failure {
                    return Err(Error::StringError(failure.to_owned()));
                }
                Ok(super::RelatedSchemaChange {
                    physical_table_ids: Vec::new(),
                    action_types: Vec::new(),
                    latest_info_schema: Arc::new(Version),
                })
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
        let checker_calls = Arc::new(Mutex::new(Vec::new()));
        settings.schema_version = Some(Arc::new(Version));
        settings.schema_lease_checker = Some(Arc::new(Checker {
            calls: Arc::clone(&checker_calls),
            failure: None,
        }));
        let upper_bound_calls = Arc::new(AtomicUsize::new(0));
        let captured_upper_bound_calls = Arc::clone(&upper_bound_calls);
        settings.commit_timestamp_upper_bound = Some(Arc::new(move |timestamp| {
            captured_upper_bound_calls.fetch_add(1, Ordering::SeqCst);
            timestamp == 102
        }));
        let committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        assert_eq!(
            committer
                .prepare_txn_file_commit_timestamp()
                .await
                .unwrap()
                .version(),
            102
        );
        assert_eq!(*checker_calls.lock().unwrap(), vec![(102, 10)]);
        assert_eq!(upper_bound_calls.load(Ordering::SeqCst), 1);

        let checker_calls = Arc::new(Mutex::new(Vec::new()));
        let upper_bound_calls = Arc::new(AtomicUsize::new(0));
        let captured_upper_bound_calls = upper_bound_calls.clone();
        let mut settings = CommitSettings::default();
        settings.schema_version = Some(Arc::new(Version));
        settings.schema_lease_checker = Some(Arc::new(Checker {
            calls: Arc::clone(&checker_calls),
            failure: Some("schema changed"),
        }));
        settings.commit_timestamp_upper_bound = Some(Arc::new(move |_| {
            captured_upper_bound_calls.fetch_add(1, Ordering::SeqCst);
            true
        }));
        let rpc = Arc::new(MockPdClient::default());
        rpc.set_timestamp(Timestamp::from_version(100));
        let committer = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        let error = committer
            .prepare_txn_file_commit_timestamp()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("schema changed"));
        assert_eq!(*checker_calls.lock().unwrap(), vec![(100, 10)]);
        assert_eq!(upper_bound_calls.load(Ordering::SeqCst), 0);

        let checker_calls = Arc::new(Mutex::new(Vec::new()));
        let upper_bound_calls = Arc::new(AtomicUsize::new(0));
        let captured_upper_bound_calls = Arc::clone(&upper_bound_calls);
        let mut settings = CommitSettings::default();
        settings.schema_version = Some(Arc::new(Version));
        settings.schema_lease_checker = Some(Arc::new(Checker {
            calls: Arc::clone(&checker_calls),
            failure: None,
        }));
        settings.commit_timestamp_upper_bound = Some(Arc::new(move |_| {
            captured_upper_bound_calls.fetch_add(1, Ordering::SeqCst);
            true
        }));
        let rpc = Arc::new(MockPdClient::default());
        rpc.set_timestamp(Timestamp::from_version(100));
        let mut expired = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        expired.start_instant = Instant::now() - Duration::from_millis(super::MAX_TXN_TIME_USE + 1);
        let error = expired
            .prepare_txn_file_commit_timestamp()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("txn takes too much time"));
        assert_eq!(*checker_calls.lock().unwrap(), vec![(100, 10)]);
        assert_eq!(upper_bound_calls.load(Ordering::SeqCst), 0);

        let checker_calls = Arc::new(Mutex::new(Vec::new()));
        let upper_bound_calls = Arc::new(AtomicUsize::new(0));
        let captured_upper_bound_calls = Arc::clone(&upper_bound_calls);
        let mut settings = CommitSettings::default();
        settings.schema_version = Some(Arc::new(Version));
        settings.schema_lease_checker = Some(Arc::new(Checker {
            calls: Arc::clone(&checker_calls),
            failure: None,
        }));
        settings.commit_timestamp_upper_bound = Some(Arc::new(move |_| {
            captured_upper_bound_calls.fetch_add(1, Ordering::SeqCst);
            false
        }));
        let rpc = Arc::new(MockPdClient::default());
        rpc.set_timestamp(Timestamp::from_version(100));
        let upper_bound = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic(),
            settings,
        );
        let error = upper_bound
            .prepare_txn_file_commit_timestamp()
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("check commit ts upper bound fail"));
        assert_eq!(*checker_calls.lock().unwrap(), vec![(100, 10)]);
        assert_eq!(upper_bound_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_transaction_TestPreSplitTxnFileRegionsUsesDedicatedSplitPath() {
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
        rpc.set_split_region_ids(vec![201, 202]);
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
        assert_eq!(rpc.scattered_region_ids(), vec![vec![201], vec![202]]);
        assert_eq!(rpc.operator_region_ids(), vec![201, 202]);
        assert_eq!(
            rpc.invalidated_regions(),
            vec![MockPdClient::region2().ver_id()]
        );

        let old_detect = super::PRE_SPLIT_DETECT_THRESHOLD.swap(0, Ordering::SeqCst);
        let old_size = super::PRE_SPLIT_SIZE_THRESHOLD.swap(0, Ordering::SeqCst);
        let rpc = Arc::new(MockPdClient::default());
        let mutations = (20_u8..23)
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

        assert_eq!(
            rpc.split_region_keys(),
            vec![vec![vec![20], vec![21], vec![22]]]
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
        let post_commit_footprint = transaction.memory_footprint();
        assert!(post_commit_footprint < footprint);

        assert_eq!(*prewrite_keys.lock().unwrap(), vec![vec![b"keep".to_vec()]]);
        assert_eq!(*schema_checks.lock().unwrap(), vec![(5, 10)]);
        assert_eq!(
            memory.lock().unwrap().last().copied(),
            Some(post_commit_footprint)
        );
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

    #[test]
    #[allow(non_snake_case)]
    fn source_go_txnkv_txnsnapshot_snapshot_test_TestBatchGet() {
        // The pinned source declares rowNums but never populates it, so its
        // loop has no cases. BatchGet behavior is covered by the direct tests
        // above and by the split-suite identities.
        let row_nums: [usize; 0] = [];
        assert!(row_nums.is_empty());
    }

    #[test]
    #[allow(non_snake_case)]
    fn source_go_txnkv_txnsnapshot_snapshot_test_TestBatchGetNotExist() {
        // This source test uses the same unpopulated rowNums slice.
        let row_nums: [usize; 0] = [];
        assert!(row_nums.is_empty());
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_test_TestSkipLargeTxnLock() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = Arc::clone(&attempts);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    if captured_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Ok(Box::new(kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: request.key.clone(),
                                    primary_lock: b"primary".to_vec(),
                                    lock_version: 1,
                                    txn_size: 10_000,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    assert_eq!(request.context.as_ref().unwrap().committed_locks, [1]);
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        not_found: true,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                panic!("large-lock point read must not dispatch cleanup");
            },
        )));
        let mut transaction = Transaction::new(
            Timestamp::from_version(u64::MAX),
            pd_client,
            TransactionOptions::new_optimistic().read_only(),
            Keyspace::Disable,
        );
        assert_eq!(transaction.get(b"secondary".to_vec()).await.unwrap(), None);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    #[ignore = "client-go TestRCRead unconditionally skips in the pinned source"]
    #[allow(non_snake_case)]
    fn source_go_txnkv_txnsnapshot_snapshot_test_TestRCRead() {}

    #[tokio::test]
    #[cfg_attr(
        feature = "nextgen",
        ignore = "client-go skips commit-TS assertions in NextGen"
    )]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_fail_test_TestCommitTSRequiredAssertion() {
        #[cfg(not(feature = "nextgen"))]
        {
            assert_snapshot_return_commit_ts_rejects_unknown_nonempty_entries();
            assert_point_get_caches_value_before_missing_commit_ts_error().await;
            assert_batch_get_does_not_cache_a_missing_commit_ts_response().await;
            assert_snapshot_buffer_batch_get_requires_pipelined_mode().await;
        }
    }

    #[tokio::test]
    #[cfg_attr(
        feature = "nextgen",
        ignore = "client-go skips read-through-lock snapshot tests in NextGen"
    )]
    #[allow(non_snake_case)]
    async fn source_go_txnkv_txnsnapshot_snapshot_fail_test_TestSnapshotUseResolveForRead() {
        #[cfg(not(feature = "nextgen"))]
        {
            let attempts = Arc::new(AtomicUsize::new(0));
            let captured_attempts = Arc::clone(&attempts);
            let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
                move |request: &dyn Any| {
                    if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                        if captured_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            return Ok(Box::new(kvrpcpb::GetResponse {
                                error: Some(kvrpcpb::KeyError {
                                    locked: Some(kvrpcpb::LockInfo {
                                        key: request.key.clone(),
                                        primary_lock: b"primary".to_vec(),
                                        lock_version: 5,
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }) as Box<dyn Any>);
                        }
                        assert_eq!(request.context.as_ref().unwrap().committed_locks, [5]);
                        return Ok(Box::new(kvrpcpb::GetResponse {
                            value: b"y".to_vec(),
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    if request.is::<kvrpcpb::CheckTxnStatusRequest>() {
                        return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                            commit_version: 6,
                            ..Default::default()
                        }) as Box<dyn Any>);
                    }
                    panic!("read-through lock must not synchronously clean the secondary");
                },
            )));
            let mut transaction = Transaction::new(
                Timestamp::from_version(u64::MAX),
                pd_client,
                TransactionOptions::new_optimistic().read_only(),
                Keyspace::Disable,
            );
            assert_eq!(
                transaction.get(b"secondary".to_vec()).await.unwrap(),
                Some(b"y".to_vec())
            );
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_prewrite_test_TestSetMinCommitTSInAsyncCommit() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observed);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::PrewriteRequest>()
                    .expect("min-commit-TS test only sends prewrite requests");
                captured.lock().unwrap().push(request.min_commit_ts);
                Ok(Box::new(kvrpcpb::PrewriteResponse {
                    min_commit_ts: request.min_commit_ts,
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));

        let mut without_for_update = source_test_committer(
            Arc::clone(&rpc),
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().use_async_commit(),
            CommitSettings::default(),
        );
        without_for_update.prewrite().await.unwrap();

        let for_update_ts = 1 + (5 << 18);
        let mut pessimistic_options = TransactionOptions::new_pessimistic().use_async_commit();
        pessimistic_options.kind =
            TransactionKind::Pessimistic(Timestamp::from_version(for_update_ts));
        let mut with_for_update = source_test_committer(
            Arc::clone(&rpc),
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            pessimistic_options,
            CommitSettings::default(),
        );
        with_for_update.prewrite().await.unwrap();

        let explicit_min_commit_ts = 1 + (10 << 18);
        let mut with_explicit_minimum = source_test_committer(
            rpc,
            Some(Key::from(b"k".to_vec())),
            vec![source_test_mutation("k", kvrpcpb::Op::Put)],
            TransactionOptions::new_optimistic().use_async_commit(),
            CommitSettings::default(),
        );
        with_explicit_minimum
            .min_commit_ts
            .try_update(explicit_min_commit_ts, WriteAccessLevel::TwoPc);
        with_explicit_minimum.prewrite().await.unwrap();

        assert_eq!(
            *observed.lock().unwrap(),
            [2, for_update_ts + 1, explicit_min_commit_ts]
        );
        assert_eq!(
            super::prewrite_min_commit_ts(u64::MAX, 0, 0),
            0,
            "client-go's uint64 increment wraps"
        );
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_prewrite_test_TestIsRetryRequestFlagWithRegionError() {
        let mut full_region = MockPdClient::region1();
        full_region.region.end_key.clear();
        let split_regions = vec![MockPdClient::region1(), MockPdClient::region2()];
        let split_regions_for_hook = split_regions.clone();
        let pd_slot = Arc::new(Mutex::new(None::<Arc<MockPdClient>>));
        let captured_pd_slot = Arc::clone(&pd_slot);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observed);
        let attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = Arc::clone(&attempts);
        let client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            let request = request
                .downcast_ref::<kvrpcpb::PrewriteRequest>()
                .expect("prewrite retry test only sends prewrite requests");
            let attempt = captured_attempts.fetch_add(1, Ordering::SeqCst);
            captured.lock().unwrap().push((
                request
                    .mutations
                    .iter()
                    .map(|mutation| mutation.key.clone())
                    .collect::<Vec<_>>(),
                request.context.as_ref().unwrap().is_retry_request,
            ));
            if attempt == 0 {
                return Err(Error::GrpcAPI(tonic::Status::unavailable(
                    "prewrite response lost",
                )));
            }
            if attempt == 1 {
                captured_pd_slot
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .replace_regions(split_regions_for_hook.clone());
                return Ok(Box::new(kvrpcpb::PrewriteResponse {
                    region_error: Some(crate::proto::errorpb::Error {
                        epoch_not_match: Some(crate::proto::errorpb::EpochNotMatch {
                            current_regions: split_regions_for_hook
                                .iter()
                                .map(|region| {
                                    let mut region = region.region.clone();
                                    let codec = crate::request::ApiV1Codec::new(
                                        crate::request::KeyMode::Txn,
                                    );
                                    (region.start_key, region.end_key) = codec
                                        .encode_region_range(&region.start_key, &region.end_key);
                                    region
                                })
                                .collect(),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>);
            }
            Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
        });
        let rpc = Arc::new(MockPdClient::with_client_and_regions(
            client,
            vec![full_region],
        ));
        *pd_slot.lock().unwrap() = Some(Arc::clone(&rpc));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(vec![1])),
            vec![
                source_test_mutation(vec![1], kvrpcpb::Op::Put),
                source_test_mutation(vec![20], kvrpcpb::Op::Put),
            ],
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        let retry_owner = Arc::new(tokio::sync::Mutex::new(super::RetryBackoffer::new(
            crate::async_util::Cancellation::default(),
            100,
        )));
        committer
            .prewrite_with_retry_owner(Some(retry_owner))
            .await
            .unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 4);
        assert_eq!(
            observed.iter().map(|(_, retry)| *retry).collect::<Vec<_>>(),
            [false, true, true, true]
        );
        assert_eq!(observed[0].0, [vec![1], vec![20]]);
        assert_eq!(observed[1].0, [vec![1], vec![20]]);
        let mut regrouped = observed[2..]
            .iter()
            .map(|(keys, _)| keys.clone())
            .collect::<Vec<_>>();
        regrouped.sort();
        assert_eq!(regrouped, [vec![vec![1]], vec![vec![20]]]);
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_txn_file_test_TestTxnFilePrewriteTxnSize() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&captured);
        let client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            let request = request
                .downcast_ref::<kvrpcpb::PrewriteRequest>()
                .expect("txn-file size test only sends prewrite requests");
            observed.lock().unwrap().push((
                request.txn_file_chunks.clone(),
                request.txn_size,
                request.context.as_ref().unwrap().is_retry_request,
            ));
            Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
        });
        let mut full_region = MockPdClient::region1();
        full_region.region.end_key.clear();
        let rpc = Arc::new(MockPdClient::with_client_and_regions(
            client,
            vec![full_region],
        ));
        let mutations = [b"a", b"b", b"x", b"y", b"z"]
            .into_iter()
            .map(|key| source_test_mutation(key.to_vec(), kvrpcpb::Op::Put))
            .collect::<Vec<_>>();
        let mut chunks = TxnChunkSlice::default();
        chunks.push(7, TxnChunkRange::new(b"a".to_vec(), b"z".to_vec(), 5));
        let mut committer = source_test_committer(
            Arc::clone(&rpc),
            Some(Key::from(b"a".to_vec())),
            mutations.clone(),
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );

        let mut batches = chunks.group_to_batches(&rpc, &mutations).await.unwrap();
        assert_eq!(batches.len(), 1);
        batches[0].is_primary = true;
        assert!(!committer
            .prewrite_txn_file_batch(&batches[0])
            .await
            .unwrap());

        let mut left = MockPdClient::region1();
        left.region.end_key = b"m".to_vec();
        let mut right = MockPdClient::region2();
        right.region.start_key = b"m".to_vec();
        right.region.end_key.clear();
        rpc.replace_regions(vec![left, right]);
        let batches = chunks.group_to_batches(&rpc, &mutations).await.unwrap();
        assert_eq!(batches.len(), 2);
        for batch in &batches {
            assert!(!committer.prewrite_txn_file_batch(batch).await.unwrap());
        }

        assert_eq!(
            *captured.lock().unwrap(),
            [
                (vec![7], 5, false),
                (vec![7], 5, false),
                (vec![7], 5, false),
            ]
        );
    }

    #[tokio::test]
    #[allow(non_snake_case)]
    async fn source_go_integration_tests_txn_file_test_TestTxnFilePrewriteTxnSizeAfterRegionRegroup(
    ) {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observed);
        let attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = Arc::clone(&attempts);
        let pd_slot = Arc::new(Mutex::new(None::<Arc<MockPdClient>>));
        let captured_pd_slot = Arc::clone(&pd_slot);
        let mut left = MockPdClient::region1();
        left.region.end_key = b"m".to_vec();
        let mut right = MockPdClient::region2();
        right.region.start_key = b"m".to_vec();
        right.region.end_key.clear();
        let split_regions = vec![left, right];
        let split_regions_for_hook = split_regions.clone();
        let client = MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            let request = request
                .downcast_ref::<kvrpcpb::PrewriteRequest>()
                .expect("txn-file regroup test only sends prewrite requests");
            let attempt = captured_attempts.fetch_add(1, Ordering::SeqCst);
            captured.lock().unwrap().push((
                request.txn_file_chunks.clone(),
                request.txn_size,
                request.context.as_ref().unwrap().is_retry_request,
                attempt == 1,
            ));
            if attempt == 0 {
                return Err(Error::GrpcAPI(tonic::Status::unavailable(
                    "txn-file prewrite response lost",
                )));
            }
            if attempt == 1 {
                captured_pd_slot
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .replace_regions(split_regions_for_hook.clone());
                return Ok(Box::new(kvrpcpb::PrewriteResponse {
                    region_error: Some(crate::proto::errorpb::Error {
                        epoch_not_match: Some(crate::proto::errorpb::EpochNotMatch {
                            current_regions: split_regions_for_hook
                                .iter()
                                .map(|region| {
                                    let mut region = region.region.clone();
                                    let codec = crate::request::ApiV1Codec::new(
                                        crate::request::KeyMode::Txn,
                                    );
                                    (region.start_key, region.end_key) = codec
                                        .encode_region_range(&region.start_key, &region.end_key);
                                    region
                                })
                                .collect(),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }) as Box<dyn Any>);
            }
            Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>)
        });
        let mut full_region = MockPdClient::region1();
        full_region.region.end_key.clear();
        let rpc = Arc::new(MockPdClient::with_client_and_regions(
            client,
            vec![full_region],
        ));
        *pd_slot.lock().unwrap() = Some(Arc::clone(&rpc));
        let mutations = vec![
            source_test_mutation(b"a".to_vec(), kvrpcpb::Op::Put),
            source_test_mutation(b"z".to_vec(), kvrpcpb::Op::Put),
        ];
        let mut chunks = TxnChunkSlice::default();
        chunks.push(9, TxnChunkRange::new(b"a".to_vec(), b"z".to_vec(), 2));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(b"a".to_vec())),
            mutations,
            TransactionOptions::new_optimistic(),
            CommitSettings::default(),
        );
        committer
            .execute_txn_file_action(&chunks, TxnFileAction::Prewrite)
            .await
            .unwrap();

        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 4);
        assert!(observed
            .iter()
            .all(|(chunk_ids, txn_size, _, _)| chunk_ids == &[9] && *txn_size == 2));
        assert_eq!(
            observed
                .iter()
                .map(|(_, _, retry, _)| *retry)
                .collect::<Vec<_>>(),
            [false, true, true, true]
        );
        assert_eq!(
            observed
                .iter()
                .filter(|(_, _, _, region_error)| *region_error)
                .count(),
            1
        );
    }

    include!("integration_source_tests.rs");
    include!("integration_lock_source_tests.rs");
    include!("integration_2pc_source_tests.rs");
}

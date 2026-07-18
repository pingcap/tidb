// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! One address-directed unary TiKV read path with an injected client.
//!
//! The injected [`RegionCache`] is the sole topology authority. Query binding
//! discovers every required region, while peer/context selection stays lazy
//! until immediately before each dispatch. Region and send failures are
//! classified through the sole [`RegionCache`] authority. This response owns
//! only request-scoped selection history, bounded resend/rebuild, per-region
//! backoff budgets, cancellation-aware sleep, and ordered replacement of
//! unconsumed work.

use std::cell::{RefCell, RefMut};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::query_runtime::{
    QueryDispatch, QueryOperation, QueryResponse, QueryResponseError, QueryResultSubset,
    QueryTransport,
};
use crate::{
    CancelHandle, CoprCache, CoprCacheConfig, ResponseChannelEvent, TransportRequest,
    TransportRequestError,
};
use prost::Message;
use tidb_txnkv::region::{
    KeyRange as RegionKeyRange, LeaderRequest, ReadPolicy, RegionAttempt, RegionBackoffBudget,
    RegionBackoffKind, RegionCache, RegionErrorDisposition, RegionLoader, RegionLocation,
    RegionQueryLoader, RegionRecoveryError, RegionRecoveryLoader, RegionRouteError, RegionVerId,
    ReplicaHealthPolicy, ReplicaReadMode, RequestSelection, RequestSelector, StoreFailureOutcome,
    StoreLabel as RoutingStoreLabel,
};
use tidb_txnkv::{
    rpc::{AsyncRequestDispatcher, CompletionError, PendingRequest},
    EndpointType, SharedReadRuntime, TraceInfo, UnaryCallContext, UnaryCancellation,
    DEFAULT_STORE_LIVENESS_TIMEOUT,
};
pub use tidb_txnkv::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryRequest, DirectUnaryResponse,
};

use super::{
    build_tikv_unary_request_for_dispatch, classify_transport_failure, decode_tikv_unary_response,
    CopPagingState, CopReadTaskError, CopReadTaskRuntime, ReadEngineGeneration,
    TransportFailureAction,
};
use crate::{RegionTaskEpoch, RegionTaskTopology, ReplicaReadType};

pub use super::forwarding::UnaryNetworkMetrics;
use super::forwarding::{UnaryRouteDispatch, UnaryTrafficLocation};

/// Deterministic policy owned by the direct unary runtime.
#[derive(Clone)]
pub struct DirectUnaryRuntimeConfig {
    /// Default used when the task has no request-local read timeout.
    pub default_timeout: Duration,
    /// Storage generation used for paging read-byte accounting.
    pub read_engine_generation: ReadEngineGeneration,
    /// Initial shared read-byte prediction for one request runtime.
    pub seed_read_bytes: u64,
    /// Optional cache configuration. Each returned query response owns one
    /// cache instance for all of its logical tasks and paging attempts.
    pub cache: Option<CoprCacheConfig>,
    /// Optional source-statement trace data injected before dispatch.
    pub trace: Option<TraceInfo>,
    /// Absolute observation clock used by the shared read-byte EMA.
    ///
    /// The default is wall-clock time since the Unix epoch. Keeping the
    /// epoch distance is essential: Go observes with `time.Now()` against a
    /// zero `time.Time`, so the first real response replaces the seed. A
    /// function pointer keeps that contract deterministic in focused tests.
    pub observation_time: fn() -> Duration,
    /// Injectable wait implementation for deterministic retry tests.
    /// Cancellation always comes from the request-owned carrier passed here.
    pub region_retry_waiter: Rc<dyn RegionRetryWaiter>,
    /// Effective sleep budget shared by both retry levels for one region ID.
    pub region_retry_max_sleep: Duration,
    /// Whether leader requests may use a cache-selected physical proxy.
    pub enable_forwarding: bool,
    /// TiDB's configured local `zone` label for exact traffic classification.
    pub local_zone_label: Option<String>,
}

impl std::fmt::Debug for DirectUnaryRuntimeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectUnaryRuntimeConfig")
            .field("default_timeout", &self.default_timeout)
            .field("read_engine_generation", &self.read_engine_generation)
            .field("seed_read_bytes", &self.seed_read_bytes)
            .field("cache", &self.cache)
            .field("trace", &self.trace)
            .field("observation_time", &"fn() -> Duration")
            .field("region_retry_waiter", &self.region_retry_waiter)
            .field("region_retry_max_sleep", &self.region_retry_max_sleep)
            .field("enable_forwarding", &self.enable_forwarding)
            .field("local_zone_label", &self.local_zone_label)
            .finish()
    }
}

impl Default for DirectUnaryRuntimeConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(60),
            read_engine_generation: ReadEngineGeneration::Classic,
            seed_read_bytes: 0,
            cache: None,
            trace: None,
            observation_time: system_observation_time,
            region_retry_waiter: Rc::new(ExactRegionRetryWaiter),
            region_retry_max_sleep: Duration::from_secs(20),
            enable_forwarding: false,
            local_zone_label: None,
        }
    }
}

/// Immutable locked-response fact passed after route success and before the
/// task can mutate paging, EMA, cache admission, or response publication.
#[derive(Clone, Debug)]
pub struct LockedResponseObservation {
    /// Address that returned the valid locked response.
    pub address: String,
    /// Exact request context attached at the send boundary.
    pub request_context: tidb_proto::KvrpcContext,
    /// Exact optimistic lock fact returned by TiKV.
    pub lock: tidb_proto::KvrpcLockInfo,
    /// Caller transaction start timestamp from the original Cop request.
    pub caller_start_ts: u64,
    /// Exact call context used by the Cop request. Lock-status, resolve, and
    /// TTL waiting must reuse its deadline and cancellation carrier.
    pub call: UnaryCallContext,
}

/// Immutable selection, cache-observation, and transport facts for one failed
/// dispatch. Keeping them together prevents retry policy from mixing facts
/// from different attempts or route generations.
struct ObservedTransportFailure {
    selected: LeaderRequest,
    observation: tidb_txnkv::region::RegionAttemptObservation,
    observation_current: bool,
    feedback: tidb_txnkv::region::RouteFeedback,
    error: DirectUnaryClientError,
}

/// Only continuation admitted after bounded lock handling succeeds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockedResponseAction {
    /// Retry the same unconsumed logical task.
    RetrySameTask,
}

/// Fail-closed locked-response policy boundary.
pub trait LockedResponseDelegate<C, L>: std::fmt::Debug {
    /// Handles one lock using the same client and region-cache authority.
    fn handle_locked_response(
        &self,
        runtime: &SharedReadRuntime<C, L>,
        observation: LockedResponseObservation,
    ) -> Result<LockedResponseAction, String>;
}

/// Injectable wait mechanism for response-owned region recovery.
///
/// The request-owned carrier remains the sole cancellation authority. Test
/// waiters may avoid wall-clock delay, but cannot substitute another token.
pub trait RegionRetryWaiter: std::fmt::Debug {
    /// Returns `true` when the supplied canonical carrier was cancelled.
    fn wait(&self, cancellation: &UnaryCancellation, delay: Duration) -> bool;
}

#[derive(Debug)]
struct ExactRegionRetryWaiter;

impl RegionRetryWaiter for ExactRegionRetryWaiter {
    fn wait(&self, cancellation: &UnaryCancellation, delay: Duration) -> bool {
        cancellation.wait_timeout(delay)
    }
}

fn system_observation_time() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
}

/// Fail-closed construction and response errors for the direct unary seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectUnaryTransportError {
    /// PD-backed region discovery or leader selection failed.
    Route(RegionRouteError),
    /// Another response is borrowing the shared region cache.
    RegionCacheLifecycle,
    /// Only the executor's `SelectWithRuntimeStats` DAG path is admitted.
    UnsupportedOperation(QueryOperation),
    /// The request was not bound by `InjectedQueryRuntime`.
    Request(TransportRequestError),
    /// Cache configuration is invalid.
    Cache(String),
    /// The checked read-task coordinator rejected the request or response.
    Coordinator(String),
    /// The injected client failed before returning a raw response.
    Client(DirectUnaryClientError),
    /// Another response is already dispatching through the shared client.
    ClientLifecycle,
    /// The raw protobuf response was malformed.
    Decode(String),
    /// The coordinator produced a response-channel state this synchronous
    /// owner cannot consume.
    ResponseState(&'static str),
    /// Region recovery rejected stale or malformed topology.
    RegionRecovery(String),
    /// Pinned client-go classified the region error as terminal.
    RegionTerminal(String),
    /// Caller cancellation won before any further query mutation.
    CallerCancelled,
    /// The bind-anchored query deadline cannot admit another retry wait.
    DeadlineExceeded,
    /// A locked response could not be handled by the bounded lock delegate.
    LockRecovery(String),
}

impl DirectUnaryTransportError {
    /// Stable category for source-shaped tests and callers.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Route(_) => "route",
            Self::RegionCacheLifecycle => "region_cache_lifecycle",
            Self::UnsupportedOperation(_) => "unsupported_operation",
            Self::Request(_) => "request",
            Self::Cache(_) => "cache",
            Self::Coordinator(_) => "coordinator",
            Self::Client(_) => "client",
            Self::ClientLifecycle => "client_lifecycle",
            Self::Decode(_) => "decode",
            Self::ResponseState(_) => "response_state",
            Self::RegionRecovery(_) => "region_recovery",
            Self::RegionTerminal(_) => "region_terminal",
            Self::CallerCancelled => "caller_cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::LockRecovery(_) => "lock_recovery",
        }
    }
}

impl std::fmt::Display for DirectUnaryTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Route(error) => write!(formatter, "region route failed: {error}"),
            Self::RegionCacheLifecycle => {
                formatter.write_str("region cache is already in use by another response")
            }
            Self::UnsupportedOperation(operation) => {
                write!(
                    formatter,
                    "unsupported direct unary operation {operation:?}"
                )
            }
            Self::Request(error) => write!(formatter, "request is not sendable: {error:?}"),
            Self::Cache(message) => write!(formatter, "invalid coprocessor cache: {message}"),
            Self::Coordinator(message) => write!(formatter, "cop read task failed: {message}"),
            Self::Client(error) => write!(formatter, "unary client failed: {error}"),
            Self::ClientLifecycle => {
                formatter.write_str("unary client is already in use by another response")
            }
            Self::Decode(message) => write!(formatter, "invalid unary response: {message}"),
            Self::ResponseState(state) => {
                write!(formatter, "invalid unary response state: {state}")
            }
            Self::RegionRecovery(message) => write!(formatter, "region recovery failed: {message}"),
            Self::RegionTerminal(message) => write!(formatter, "terminal region error: {message}"),
            Self::CallerCancelled => formatter.write_str("query cancelled by caller"),
            Self::DeadlineExceeded => formatter.write_str("query deadline exceeded"),
            Self::LockRecovery(message) => write!(formatter, "lock recovery failed: {message}"),
        }
    }
}

impl std::error::Error for DirectUnaryTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Route(error) => Some(error),
            Self::Client(error) => Some(error),
            _ => None,
        }
    }
}

impl From<RegionRouteError> for DirectUnaryTransportError {
    fn from(error: RegionRouteError) -> Self {
        Self::Route(error)
    }
}

impl From<CopReadTaskError> for DirectUnaryTransportError {
    fn from(error: CopReadTaskError) -> Self {
        Self::Coordinator(error.to_string())
    }
}

/// Injected transport that creates lazy, response-owned unary runtimes.
pub struct DirectUnaryQueryTransport<C, L> {
    shared_runtime: SharedReadRuntime<C, L>,
    locked_response_delegate: Rc<dyn LockedResponseDelegate<C, L>>,
    async_begin: Option<AsyncBegin<C>>,
    replica_read_seed: ReplicaReadSeed,
    config: DirectUnaryRuntimeConfig,
}

type AsyncBegin<C> = fn(
    &RefCell<C>,
    &LeaderRequest,
    &DirectUnaryRequest,
    &UnaryCallContext,
) -> Result<
    Result<Box<dyn PendingRequest>, DirectUnaryClientError>,
    DirectUnaryTransportError,
>;

fn begin_async_request<C>(
    client: &RefCell<C>,
    selected: &LeaderRequest,
    request: &DirectUnaryRequest,
    call: &UnaryCallContext,
) -> Result<Result<Box<dyn PendingRequest>, DirectUnaryClientError>, DirectUnaryTransportError>
where
    C: AsyncRequestDispatcher,
    C::Pending: 'static,
{
    Ok(try_borrow_client(client)?
        .begin(
            selected.dispatch_address(),
            selected.forwarded_host(),
            request,
            call,
        )
        .map(|pending| Box::new(pending) as Box<dyn PendingRequest>))
}

/// One transport-owned rotating seed source sampled once per lazy response.
///
/// One `send` owns one immutable seed across all of its region tasks and region
/// reloads. Only binding a fresh query advances the transport source.
#[derive(Debug)]
struct ReplicaReadSeed {
    current: u32,
}

impl ReplicaReadSeed {
    fn new() -> Self {
        Self { current: 0 }
    }

    fn next(&mut self) -> u32 {
        self.current = self.current.wrapping_add(1);
        self.current
    }
}

impl<C, L: RegionLoader> DirectUnaryQueryTransport<C, L> {
    /// Constructs an explicitly injected, workerless transport runtime.
    /// Production PD-backed callers use [`Self::new_production`].
    pub fn new_injected<S>(
        client: C,
        region_cache: RegionCache<L>,
        config: DirectUnaryRuntimeConfig,
        timestamp_source: S,
    ) -> Result<Self, DirectUnaryTransportError>
    where
        C: tidb_txnkv::lock::LockRecoveryClient,
        L: RegionRecoveryLoader,
        S: tidb_txnkv::lock::TimestampSource + 'static,
    {
        Self::with_shared_runtime(
            SharedReadRuntime::new_injected(client, region_cache),
            config,
            timestamp_source,
        )
    }

    /// Constructs an injected transport whose first physical attempt for each
    /// logical request uses BatchCommands. Injected clients which do not opt
    /// into this constructor stay on the synchronous path without consuming
    /// selector or backoff state.
    pub fn new_injected_batch_first<S>(
        client: C,
        region_cache: RegionCache<L>,
        config: DirectUnaryRuntimeConfig,
        timestamp_source: S,
    ) -> Result<Self, DirectUnaryTransportError>
    where
        C: tidb_txnkv::lock::LockRecoveryClient + AsyncRequestDispatcher,
        C::Pending: 'static,
        L: RegionRecoveryLoader,
        S: tidb_txnkv::lock::TimestampSource + 'static,
    {
        Self::with_shared_runtime_batch_first(
            SharedReadRuntime::new_injected(client, region_cache),
            config,
            timestamp_source,
        )
    }

    /// Retains already-shared client/cache handles and installs required
    /// production lock recovery without creating another runtime authority.
    pub fn with_shared_runtime<S>(
        shared_runtime: SharedReadRuntime<C, L>,
        config: DirectUnaryRuntimeConfig,
        timestamp_source: S,
    ) -> Result<Self, DirectUnaryTransportError>
    where
        C: tidb_txnkv::lock::LockRecoveryClient,
        L: RegionRecoveryLoader,
        S: tidb_txnkv::lock::TimestampSource + 'static,
    {
        Self::with_locked_response_delegate(
            shared_runtime,
            config,
            Rc::new(super::OptimisticLockRecovery::new(timestamp_source)),
        )
    }

    /// Retains shared runtime ownership and enables one BatchCommands attempt
    /// before the response-owned synchronous retry loop.
    pub fn with_shared_runtime_batch_first<S>(
        shared_runtime: SharedReadRuntime<C, L>,
        config: DirectUnaryRuntimeConfig,
        timestamp_source: S,
    ) -> Result<Self, DirectUnaryTransportError>
    where
        C: tidb_txnkv::lock::LockRecoveryClient + AsyncRequestDispatcher,
        C::Pending: 'static,
        L: RegionRecoveryLoader,
        S: tidb_txnkv::lock::TimestampSource + 'static,
    {
        let mut transport = Self::with_locked_response_delegate(
            shared_runtime,
            config,
            Rc::new(super::OptimisticLockRecovery::new(timestamp_source)),
        )?;
        transport.async_begin = Some(begin_async_request::<C>);
        Ok(transport)
    }

    /// Installs the bounded lock policy over the same shared read runtime.
    pub fn with_locked_response_delegate(
        shared_runtime: SharedReadRuntime<C, L>,
        config: DirectUnaryRuntimeConfig,
        locked_response_delegate: Rc<dyn LockedResponseDelegate<C, L>>,
    ) -> Result<Self, DirectUnaryTransportError> {
        if shared_runtime.cluster_id() == 0 {
            return Err(RegionRouteError::MissingClusterId.into());
        }
        Ok(Self {
            shared_runtime,
            locked_response_delegate,
            async_begin: None,
            replica_read_seed: ReplicaReadSeed::new(),
            config,
        })
    }
}

impl<C, L> DirectUnaryQueryTransport<C, L>
where
    L: RegionQueryLoader + RegionRecoveryLoader + Send + 'static,
{
    /// Starts the sole store-maintenance and cache-GC worker over the same
    /// cache authority consumed by foreground reads and lock recovery.
    pub fn new_production<S>(
        client: C,
        region_cache: RegionCache<L>,
        config: DirectUnaryRuntimeConfig,
        timestamp_source: S,
    ) -> Result<Self, DirectUnaryTransportError>
    where
        C: tidb_txnkv::lock::LockRecoveryClient + AsyncRequestDispatcher,
        C::Pending: 'static,
        S: tidb_txnkv::lock::TimestampSource + 'static,
    {
        let runtime = SharedReadRuntime::new_with_maintenance(client, region_cache)
            .map_err(|_| DirectUnaryTransportError::RegionCacheLifecycle)?;
        debug_assert!(runtime.is_maintained());
        Self::with_shared_runtime_batch_first(runtime, config, timestamp_source)
    }
}

impl<C: DirectUnaryClient + 'static, L: RegionRecoveryLoader + 'static> QueryTransport
    for DirectUnaryQueryTransport<C, L>
{
    type Response = DirectUnaryQueryResponse<C, L>;

    fn send(
        &mut self,
        request: &TransportRequest,
        dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        let cancellation = Arc::clone(
            request
                .request_cancellation()
                .map_err(|error| DirectUnaryTransportError::Request(error).to_string())?,
        );
        if cancellation.is_cancelled() {
            return Err(DirectUnaryTransportError::CallerCancelled.to_string());
        }
        if dispatch.operation != QueryOperation::SelectWithRuntimeStats {
            return Err(
                DirectUnaryTransportError::UnsupportedOperation(dispatch.operation).to_string(),
            );
        }
        let metadata = request
            .metadata_for_send()
            .map_err(|error| DirectUnaryTransportError::Request(error).to_string())?;
        let mut read_policy =
            read_policy_from_metadata(metadata).map_err(|error| error.to_string())?;
        read_policy.forwarding = self.config.enable_forwarding;
        CopPagingState::validate_read_request(metadata)
            .map_err(|error| DirectUnaryTransportError::from(error).to_string())?;
        let requested_ranges =
            metadata_region_ranges(metadata).map_err(|error| error.to_string())?;
        let cluster_id = self.shared_runtime.cluster_id();
        let locations = self
            .shared_runtime
            .locate_ranges(&requested_ranges)
            .map_err(|_| DirectUnaryTransportError::RegionCacheLifecycle.to_string())?
            .map_err(|error| DirectUnaryTransportError::Route(error).to_string())?;
        let topology = topology_from_locations(locations);
        let cache = CoprCache::from_optional_config(self.config.cache.as_ref())
            .map_err(|error| DirectUnaryTransportError::Cache(error.to_string()).to_string())?;
        let runtime = CopPagingState::prepare_read_tasks(
            metadata,
            &topology,
            cache,
            self.config.read_engine_generation,
            self.config.seed_read_bytes,
        )
        .map_err(|error| DirectUnaryTransportError::from(error).to_string())?;

        let mut logical_order = Vec::new();
        let mut active_attempts = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for prepared in runtime.prepared_attempts() {
            task_region_ver_id(prepared.task()).map_err(|error| error.to_string())?;
            if seen.insert(prepared.logical_task_id()) {
                logical_order.push(prepared.logical_task_id());
            }
            active_attempts.insert(prepared.logical_task_id(), prepared.attempt_id());
        }
        if logical_order.is_empty() {
            return Err(DirectUnaryTransportError::Coordinator(
                "region discovery produced no logical task".to_owned(),
            )
            .to_string());
        }
        let selection_seed = self.replica_read_seed.next();
        let timeout = if metadata.session.tikv_client_read_timeout_ms > 0 {
            Duration::from_millis(metadata.session.tikv_client_read_timeout_ms)
        } else {
            self.config.default_timeout
        };
        let bound_at = request
            .bound_at()
            .map_err(|error| DirectUnaryTransportError::Request(error).to_string())?;
        let call =
            UnaryCallContext::with_deadline(bound_at + timeout, cancellation.unary_cancellation());

        Ok(Some(DirectUnaryQueryResponse {
            shared_runtime: self.shared_runtime.clone(),
            locked_response_delegate: Rc::clone(&self.locked_response_delegate),
            async_begin: self.async_begin,
            cancellation,
            call,
            selection_seed,
            read_policy,
            metadata: metadata.clone(),
            cluster_id,
            config: self.config.clone(),
            runtime,
            logical_order,
            active_attempts,
            logical_index: 0,
            closed: false,
            region_backoffs: BTreeMap::new(),
            request_selectors: BTreeMap::new(),
            sync_only_chains: BTreeSet::new(),
            pending_batch: None,
            network_metrics: UnaryNetworkMetrics::default(),
        }))
    }
}

fn read_policy_from_metadata(
    metadata: &crate::KvRequestMetadata,
) -> Result<ReadPolicy, DirectUnaryTransportError> {
    let mode = match metadata.session.replica_read {
        ReplicaReadType::Leader => ReplicaReadMode::Leader,
        ReplicaReadType::Follower => ReplicaReadMode::Follower,
        ReplicaReadType::Mixed => ReplicaReadMode::Mixed,
        ReplicaReadType::Learner => ReplicaReadMode::Learner,
        ReplicaReadType::PreferLeader => ReplicaReadMode::PreferLeader,
        // Pinned client-go treats the closest variants as mixed selection
        // plus labels/load inputs. Those inputs are rejected above until the
        // corresponding store metadata exists; the bare mode is exact Mixed.
        ReplicaReadType::Closest | ReplicaReadType::ClosestAdaptive => ReplicaReadMode::Mixed,
    };
    Ok(ReadPolicy {
        // EnableStaleWithMixedReplicaRead overrides the session mode.
        mode: if metadata.is_staleness {
            ReplicaReadMode::Mixed
        } else {
            mode
        },
        stale_read: metadata.is_staleness,
        forwarding: false,
        // Replaced by the query-scoped seed sampled once in `send` before any
        // logical selector is created.
        selection_seed: 0,
    })
}

fn metadata_region_ranges(
    metadata: &crate::KvRequestMetadata,
) -> Result<Vec<RegionKeyRange>, DirectUnaryTransportError> {
    let ranges = metadata
        .key_ranges
        .as_ref()
        .ok_or_else(|| DirectUnaryTransportError::Coordinator("missing_ranges".to_owned()))?;
    if !ranges.is_non_partitioned() || ranges.partitions.len() != 1 {
        return Err(DirectUnaryTransportError::Coordinator(
            "partitioned_ranges".to_owned(),
        ));
    }
    let ranges = ranges
        .partitions
        .first()
        .ok_or_else(|| DirectUnaryTransportError::Coordinator("missing_ranges".to_owned()))?;
    if ranges.is_empty() {
        return Err(DirectUnaryTransportError::Coordinator(
            "missing_ranges".to_owned(),
        ));
    }
    Ok(ranges
        .iter()
        .map(|range| RegionKeyRange::new(range.start_key.clone(), range.end_key.clone()))
        .collect())
}

fn topology_from_locations(locations: Vec<RegionLocation>) -> Vec<RegionTaskTopology> {
    let mut topology = Vec::with_capacity(locations.len());
    for location in locations {
        topology.push(RegionTaskTopology {
            region_id: location.region.id,
            region_epoch: Some(RegionTaskEpoch {
                conf_ver: location.region.epoch.conf_ver,
                version: location.region.epoch.version,
            }),
            peer: None,
            start_key: location.start_key,
            end_key: location.end_key,
            ..RegionTaskTopology::default()
        });
    }
    topology
}

fn task_region_ver_id(
    task: &crate::RegionTaskEnvelope,
) -> Result<RegionVerId, DirectUnaryTransportError> {
    let epoch = task.region_epoch.ok_or_else(|| {
        DirectUnaryTransportError::Coordinator(
            "prepared PD-backed task has no region epoch".to_owned(),
        )
    })?;
    Ok(RegionVerId::new(
        task.region_id,
        epoch.conf_ver,
        epoch.version,
    ))
}

/// Lazy response owner returned by [`DirectUnaryQueryTransport`].
pub struct DirectUnaryQueryResponse<C, L> {
    shared_runtime: SharedReadRuntime<C, L>,
    locked_response_delegate: Rc<dyn LockedResponseDelegate<C, L>>,
    async_begin: Option<AsyncBegin<C>>,
    cancellation: Arc<CancelHandle>,
    call: UnaryCallContext,
    selection_seed: u32,
    read_policy: ReadPolicy,
    metadata: crate::KvRequestMetadata,
    cluster_id: u64,
    config: DirectUnaryRuntimeConfig,
    runtime: CopReadTaskRuntime,
    logical_order: Vec<u64>,
    active_attempts: BTreeMap<u64, u64>,
    logical_index: usize,
    closed: bool,
    region_backoffs: BTreeMap<u64, RegionBackoffBudget>,
    request_selectors: BTreeMap<u64, RequestSelector>,
    // Region/send failure keeps this logical request chain on the existing
    // synchronous loop. Terminal success clears it so the next page can begin
    // a fresh BatchCommands attempt, matching SendReqAsync-per-call behavior.
    sync_only_chains: BTreeSet<u64>,
    pending_batch: Option<PendingBatchAttempt>,
    network_metrics: UnaryNetworkMetrics,
}

struct PreparedRegionDispatch {
    logical_task_id: u64,
    attempt_id: u64,
    selected: LeaderRequest,
    observation: tidb_txnkv::region::RegionAttemptObservation,
    client_request: DirectUnaryRequest,
    request_bytes: usize,
    traffic_location: UnaryTrafficLocation,
    batch_attempt: bool,
}

struct PendingBatchAttempt {
    dispatch: PreparedRegionDispatch,
    pending: Box<dyn PendingRequest>,
    started_at: Instant,
}

impl<C, L> Drop for DirectUnaryQueryResponse<C, L> {
    fn drop(&mut self) {
        if let Some(attempt) = self.pending_batch.as_mut() {
            attempt.pending.cancel();
        }
    }
}

fn try_borrow_client<C>(client: &RefCell<C>) -> Result<RefMut<'_, C>, DirectUnaryTransportError> {
    client
        .try_borrow_mut()
        .map_err(|_| DirectUnaryTransportError::ClientLifecycle)
}

fn cache_operation<C, L: RegionLoader, R>(
    runtime: &SharedReadRuntime<C, L>,
    operation: impl FnOnce(&mut RegionCache<L>) -> R,
) -> Result<R, DirectUnaryTransportError> {
    runtime
        .with_region_cache(operation)
        .map_err(|_| DirectUnaryTransportError::RegionCacheLifecycle)
}

impl<C: DirectUnaryClient, L: RegionRecoveryLoader> DirectUnaryQueryResponse<C, L> {
    /// Request-local network observations accumulated before publication.
    #[must_use]
    pub const fn network_metrics(&self) -> &UnaryNetworkMetrics {
        &self.network_metrics
    }

    fn pull(
        &mut self,
        _required_rows: usize,
    ) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        if self.closed {
            return Ok(None);
        }
        loop {
            let Some(&logical_task_id) = self.logical_order.get(self.logical_index) else {
                self.closed = true;
                return Ok(None);
            };

            match self.runtime.next_response(logical_task_id) {
                Some(ResponseChannelEvent::Result(data)) => {
                    return Ok(Some(QueryResultSubset {
                        data,
                        runtime: None,
                    }));
                }
                Some(ResponseChannelEvent::Closed) => {
                    self.active_attempts.remove(&logical_task_id);
                    self.request_selectors.remove(&logical_task_id);
                    self.logical_index += 1;
                    continue;
                }
                Some(ResponseChannelEvent::Error(message)) => {
                    return self.fail(DirectUnaryTransportError::Coordinator(message));
                }
                Some(ResponseChannelEvent::Warning(_)) => {
                    return self.fail(DirectUnaryTransportError::ResponseState(
                        "unexpected warning event",
                    ));
                }
                Some(ResponseChannelEvent::ResultWithRuntime { .. }) => {
                    return self.fail(DirectUnaryTransportError::ResponseState(
                        "unexpected runtime event",
                    ));
                }
                None => {}
            }

            let Some(&attempt_id) = self.active_attempts.get(&logical_task_id) else {
                return self.fail(DirectUnaryTransportError::ResponseState(
                    "open logical task has no active attempt",
                ));
            };
            let dispatch_result = if self.pending_batch.is_some() {
                self.complete_batch_attempt()
            } else {
                self.dispatch_attempt(logical_task_id, attempt_id)
            };
            if let Err(error) = dispatch_result {
                return self.fail(error);
            }
        }
    }

    fn dispatch_attempt(
        &mut self,
        logical_task_id: u64,
        attempt_id: u64,
    ) -> Result<(), DirectUnaryTransportError> {
        self.check_retry_active()?;
        let prepared = self.runtime.prepared_attempt(attempt_id).cloned().ok_or(
            DirectUnaryTransportError::ResponseState("active attempt is not prepared"),
        )?;
        let region = task_region_ver_id(prepared.task())?;
        let replace_selector = self
            .request_selectors
            .get(&logical_task_id)
            .is_none_or(|selector| selector.region() != region);
        if replace_selector {
            let mut read_policy = self.read_policy;
            read_policy.selection_seed = self.selection_seed;
            let mut selector = cache_operation(&self.shared_runtime, |region_cache| {
                region_cache.request_selector(region, read_policy)
            })??;
            selector.set_health_policy(ReplicaHealthPolicy {
                try_leader: matches!(
                    read_policy.mode,
                    ReplicaReadMode::Mixed | ReplicaReadMode::PreferLeader
                ),
                prefer_leader: read_policy.mode == ReplicaReadMode::PreferLeader,
                learner_only: read_policy.mode == ReplicaReadMode::Learner,
                labels: self
                    .metadata
                    .match_store_labels
                    .iter()
                    .map(|label| RoutingStoreLabel {
                        key: label.key.clone(),
                        value: label.value.clone(),
                    })
                    .collect(),
                stores: Vec::new(),
                busy_threshold: Duration::from_millis(
                    self.metadata.session.store_busy_threshold_ms,
                ),
            });
            self.request_selectors.insert(logical_task_id, selector);
        }
        let (selection, observation, effective_busy_threshold) = {
            let selector = self.request_selectors.get_mut(&logical_task_id).ok_or(
                DirectUnaryTransportError::ResponseState("request selector was not installed"),
            )?;
            let (selection, observation) =
                cache_operation(&self.shared_runtime, |region_cache| {
                    let selection = region_cache
                        .select_request(selector)
                        .map_err(DirectUnaryTransportError::Route)?;
                    let observation = match &selection {
                        RequestSelection::Attempt(selected) => Some(
                            region_cache
                                .observe_attempt(selected.dispatch_attempt())
                                .map_err(|error| {
                                    DirectUnaryTransportError::RegionRecovery(error.to_string())
                                })?,
                        ),
                        RequestSelection::ReloadRegion { .. } => None,
                    };
                    Ok::<_, DirectUnaryTransportError>((selection, observation))
                })??;
            (selection, observation, selector.busy_threshold())
        };
        let (selected, observation) = match (selection, observation) {
            (RequestSelection::Attempt(selected), Some(observation)) => (selected, observation),
            (RequestSelection::ReloadRegion { region }, None) => {
                let failed = self.runtime.consume_failed_attempt(attempt_id)?;
                self.request_selectors.remove(&logical_task_id);
                return self.rebuild_exhausted_region(failed, region.id);
            }
            _ => {
                return Err(DirectUnaryTransportError::ResponseState(
                    "selected route has no cache-issued observation",
                ));
            }
        };
        let predicted_read_bytes = self
            .runtime
            .task_predicted_read_bytes(logical_task_id)
            .ok_or(DirectUnaryTransportError::ResponseState(
                "active task has no read-byte prediction",
            ))?;
        let mut request = build_tikv_unary_request_for_dispatch(
            &prepared,
            &self.metadata,
            predicted_read_bytes,
            self.config.trace.as_ref(),
            self.cluster_id,
            &selected,
        );
        request.context.busy_threshold_ms =
            u32::try_from(effective_busy_threshold.as_millis()).unwrap_or(u32::MAX);
        if request.endpoint != EndpointType::TiKv {
            return Err(DirectUnaryTransportError::ResponseState(
                "direct unary request selected a non-TiKV endpoint",
            ));
        }
        let client_request = DirectUnaryRequest {
            endpoint: request.endpoint,
            replica_read_type: request.replica_read_type,
            replica_read: request.replica_read,
            stale_read: request.stale_read,
            input_request_source: request.input_request_source,
            predicted_read_bytes: request.predicted_read_bytes,
            read_replica_scope: request.read_replica_scope,
            txn_scope: request.txn_scope,
            context: request.context,
            encoded_request: request.encoded_request,
        };
        let request_bytes = client_request.encoded_request.len();
        let target_zone = cache_operation(&self.shared_runtime, |region_cache| {
            region_cache
                .store_label(selected.target().store_id, "zone")
                .map(str::to_owned)
        })?;
        let cross_zone = match (
            self.config.local_zone_label.as_deref(),
            target_zone.as_deref(),
        ) {
            (Some(local), Some(target)) if !local.is_empty() && !target.is_empty() => {
                Some(local != target)
            }
            _ => None,
        };
        let traffic_location = UnaryTrafficLocation::from_cross_zone(cross_zone);
        let prepared_dispatch = PreparedRegionDispatch {
            logical_task_id,
            attempt_id,
            selected,
            observation,
            client_request,
            request_bytes,
            traffic_location,
            batch_attempt: false,
        };
        self.check_retry_active()?;
        let dispatch_started = Instant::now();
        let call = self.call.clone();
        if let Some(begin) = self.async_begin.filter(|_| {
            !self
                .sync_only_chains
                .contains(&prepared_dispatch.logical_task_id)
        }) {
            let mut prepared_dispatch = prepared_dispatch;
            prepared_dispatch.batch_attempt = true;
            let mut begin_result = begin(
                self.shared_runtime.client(),
                &prepared_dispatch.selected,
                &prepared_dispatch.client_request,
                &call,
            )?;
            if let Err(error) = self.check_retry_active() {
                if let Ok(pending) = &mut begin_result {
                    pending.cancel();
                }
                return Err(error);
            }
            self.network_metrics.on_request(
                prepared_dispatch.request_bytes,
                prepared_dispatch.selected.stale_read,
                prepared_dispatch.traffic_location,
            );
            match begin_result {
                Ok(pending) => {
                    self.pending_batch = Some(PendingBatchAttempt {
                        dispatch: prepared_dispatch,
                        pending,
                        started_at: dispatch_started,
                    });
                    return Ok(());
                }
                Err(error) => {
                    self.sync_only_chains
                        .insert(prepared_dispatch.logical_task_id);
                    return self.settle_dispatch(
                        prepared_dispatch,
                        Err(error),
                        dispatch_started.elapsed(),
                    );
                }
            }
        }
        self.network_metrics.on_request(
            prepared_dispatch.request_bytes,
            prepared_dispatch.selected.stale_read,
            prepared_dispatch.traffic_location,
        );
        let dispatch = UnaryRouteDispatch::from_request(&prepared_dispatch.selected);
        let send_result = try_borrow_client(self.shared_runtime.client())?.send_request_with_route(
            dispatch.physical_address(),
            dispatch.forwarded_host(),
            &prepared_dispatch.client_request,
            &call,
        );
        let dispatch_duration = dispatch_started.elapsed();
        self.settle_dispatch(prepared_dispatch, send_result, dispatch_duration)
    }

    fn complete_batch_attempt(&mut self) -> Result<(), DirectUnaryTransportError> {
        if let Err(error) = self.check_retry_active() {
            if let Some(attempt) = self.pending_batch.as_mut() {
                attempt.pending.cancel();
            }
            return Err(error);
        }
        let completion = {
            let attempt =
                self.pending_batch
                    .as_mut()
                    .ok_or(DirectUnaryTransportError::ResponseState(
                        "missing pending BatchCommands attempt",
                    ))?;
            attempt.pending.complete(&self.call)
        };
        if let Err(error) = self.check_retry_active() {
            if let Some(attempt) = self.pending_batch.as_mut() {
                attempt.pending.cancel();
            }
            return Err(error);
        }
        let send_result = match completion {
            Ok(result) => result,
            Err(CompletionError::Cancelled) => {
                if let Some(attempt) = self.pending_batch.as_mut() {
                    attempt.pending.cancel();
                }
                return Err(DirectUnaryTransportError::CallerCancelled);
            }
            Err(CompletionError::DeadlineExceeded) => {
                if let Some(attempt) = self.pending_batch.as_mut() {
                    attempt.pending.cancel();
                }
                return Err(DirectUnaryTransportError::DeadlineExceeded);
            }
            Err(error) => Err(DirectUnaryClientError::Runtime(error.to_string())),
        };
        let pending = self
            .pending_batch
            .take()
            .ok_or(DirectUnaryTransportError::ResponseState(
                "completed BatchCommands attempt vanished",
            ))?;
        self.settle_dispatch(pending.dispatch, send_result, pending.started_at.elapsed())
    }

    fn settle_dispatch(
        &mut self,
        dispatch: PreparedRegionDispatch,
        send_result: Result<DirectUnaryResponse, DirectUnaryClientError>,
        dispatch_duration: Duration,
    ) -> Result<(), DirectUnaryTransportError> {
        let PreparedRegionDispatch {
            logical_task_id,
            attempt_id,
            selected,
            observation,
            client_request,
            request_bytes,
            traffic_location,
            batch_attempt,
        } = dispatch;
        // Go checks ctx.Err after SendRequest returns. Caller cancellation has
        // precedence over a simultaneous transport error or successful reply.
        if self.cancellation.is_cancelled()
            || self.call.timeout().is_zero()
            || matches!(&send_result, Err(DirectUnaryClientError::CallerCancelled))
        {
            return if self.cancellation.is_cancelled()
                || matches!(&send_result, Err(DirectUnaryClientError::CallerCancelled))
            {
                Err(DirectUnaryTransportError::CallerCancelled)
            } else {
                Err(DirectUnaryTransportError::DeadlineExceeded)
            };
        }
        let raw_response = match send_result {
            Ok(response) => response,
            Err(error) => {
                if batch_attempt {
                    self.sync_only_chains.insert(logical_task_id);
                }
                let feedback = UnaryRouteDispatch::from_request(&selected)
                    .feedback(&selected, tidb_txnkv::region::RouteOutcome::Failure);
                let observation_current = match cache_operation(&self.shared_runtime, |cache| {
                    cache.validate_route_observation(&selected, &observation)
                })? {
                    Ok(()) => true,
                    Err(RegionRecoveryError::StaleObservation(_)) => false,
                    Err(error) => {
                        return Err(DirectUnaryTransportError::RegionRecovery(error.to_string()));
                    }
                };
                self.record_attempt_result(logical_task_id, &selected, dispatch_duration)?;
                return self.recover_transport_failure(
                    logical_task_id,
                    attempt_id,
                    ObservedTransportFailure {
                        selected,
                        observation,
                        observation_current,
                        feedback,
                        error,
                    },
                );
            }
        };
        self.network_metrics.on_response(
            request_bytes,
            raw_response.encoded_response.len(),
            selected.replica_read,
            selected.stale_read,
            traffic_location,
        );
        let locked =
            tidb_proto::CoprocessorResponse::decode(raw_response.encoded_response.as_slice())
                .map_err(|error| DirectUnaryTransportError::Decode(error.to_string()))?
                .locked;
        let response = decode_tikv_unary_response(&raw_response.encoded_response)
            .map_err(|error| DirectUnaryTransportError::Decode(error.to_string()))?;
        if let Some(region_error) = response.region_error_ref().cloned() {
            if batch_attempt {
                self.sync_only_chains.insert(logical_task_id);
            }
            if selected.stale_read && region_error.data_is_not_ready.is_some() {
                self.network_metrics.on_stale_read_result(false);
            }
            self.record_attempt_result(logical_task_id, &selected, dispatch_duration)?;
            let failed = self.runtime.consume_region_error(attempt_id)?;
            return self.recover_region_error(
                logical_task_id,
                failed,
                selected.attempt,
                region_error,
            );
        }
        if selected.stale_read {
            self.network_metrics.on_stale_read_result(true);
        }
        self.record_attempt_result(logical_task_id, &selected, dispatch_duration)?;
        cache_operation(&self.shared_runtime, |region_cache| {
            region_cache
                .on_route_success(&selected)
                .map_err(|error| DirectUnaryTransportError::RegionRecovery(error.to_string()))?;
            region_cache
                .promote_successful_request(&selected)
                .map_err(|error| DirectUnaryTransportError::RegionRecovery(error.to_string()))?;
            Ok::<_, DirectUnaryTransportError>(())
        })??;
        if let Some(lock) = locked {
            let action = self
                .locked_response_delegate
                .handle_locked_response(
                    &self.shared_runtime,
                    LockedResponseObservation {
                        address: selected.attempt.address.clone(),
                        request_context: client_request.context.clone(),
                        lock,
                        caller_start_ts: self.metadata.start_ts,
                        call: self.call.clone(),
                    },
                )
                .map_err(|error| {
                    if self.cancellation.is_cancelled() {
                        DirectUnaryTransportError::CallerCancelled
                    } else {
                        DirectUnaryTransportError::LockRecovery(error)
                    }
                })?;
            self.check_retry_active()?;
            match action {
                LockedResponseAction::RetrySameTask => {
                    let failed = self.runtime.consume_failed_attempt(attempt_id)?;
                    let replacement = self.runtime.retry_transport_attempt(failed)?;
                    self.install_same_task_retry(replacement)?;
                    return Ok(());
                }
            }
        }
        let accepted = self.runtime.accept_response(
            attempt_id,
            response,
            None,
            (self.config.observation_time)(),
        )?;
        self.sync_only_chains.remove(&logical_task_id);
        self.request_selectors.remove(&logical_task_id);
        match accepted.next_attempt_id {
            Some(next_attempt_id) => {
                self.active_attempts
                    .insert(logical_task_id, next_attempt_id);
            }
            None => {
                self.active_attempts.remove(&logical_task_id);
            }
        }
        Ok(())
    }

    fn record_attempt_result(
        &mut self,
        logical_task_id: u64,
        selected: &LeaderRequest,
        dispatch_duration: Duration,
    ) -> Result<(), DirectUnaryTransportError> {
        if !self
            .request_selectors
            .get_mut(&logical_task_id)
            .ok_or(DirectUnaryTransportError::ResponseState(
                "request selector disappeared during dispatch",
            ))?
            .record_attempt_result(&selected.attempt, dispatch_duration)
        {
            return Err(DirectUnaryTransportError::RegionRecovery(
                "RPC completion did not match the selector's pending attempt".to_owned(),
            ));
        }
        Ok(())
    }

    fn recover_transport_failure(
        &mut self,
        logical_task_id: u64,
        attempt_id: u64,
        failure: ObservedTransportFailure,
    ) -> Result<(), DirectUnaryTransportError> {
        let ObservedTransportFailure {
            selected,
            observation,
            observation_current,
            feedback,
            error,
        } = failure;
        self.check_retry_active()?;
        let action = classify_transport_failure(&error);
        let (connection, close_generation) = match action {
            TransportFailureAction::Terminal => {
                return Err(DirectUnaryTransportError::Client(error));
            }
            TransportFailureAction::RetryConnection {
                connection,
                close_generation,
            } => (connection, close_generation),
        };
        if connection.address() != feedback.dispatch_attempt().address {
            return Err(DirectUnaryTransportError::RegionRecovery(
                "transport failure address disagrees with selected attempt".to_owned(),
            ));
        }
        let failed = self.runtime.consume_failed_attempt(attempt_id)?;
        if close_generation {
            self.check_retry_active()?;
            try_borrow_client(self.shared_runtime.client())?
                .close_address_version(connection.address(), connection.version())
                .map_err(DirectUnaryTransportError::Client)?;
        }
        self.check_retry_active()?;
        let liveness = try_borrow_client(self.shared_runtime.client())?
            .liveness(connection.address(), DEFAULT_STORE_LIVENESS_TIMEOUT)
            .map_err(DirectUnaryTransportError::Client)?;
        self.check_retry_active()?;
        let failure_outcome = if observation_current {
            match cache_operation(&self.shared_runtime, |region_cache| {
                region_cache.on_route_send_failure_observed(&selected, &observation, liveness)
            })? {
                Ok(outcome) => Some(outcome),
                Err(RegionRecoveryError::StaleObservation(_)) => None,
                Err(error) => {
                    return Err(DirectUnaryTransportError::RegionRecovery(error.to_string()));
                }
            }
        } else {
            None
        };
        if matches!(
            failure_outcome,
            Some(StoreFailureOutcome::Invalidated { .. })
        ) {
            self.shared_runtime
                .trigger_store_check()
                .map_err(|_| DirectUnaryTransportError::RegionCacheLifecycle)?;
        }
        let delay = self
            .region_backoffs
            .entry(selected.attempt.region.id)
            .or_insert_with(|| RegionBackoffBudget::new(self.config.region_retry_max_sleep))
            .next_delay(RegionBackoffKind::TikvRpc)
            .map_err(|error| DirectUnaryTransportError::RegionTerminal(format!("{error:?}")))?;
        self.sleep_retry(delay)?;
        let replacement = self.runtime.retry_transport_attempt(failed)?;
        self.install_same_task_retry(replacement)?;
        debug_assert!(self.request_selectors.contains_key(&logical_task_id));
        Ok(())
    }

    fn recover_region_error(
        &mut self,
        logical_task_id: u64,
        failed: super::FailedCopReadAttempt,
        observed_attempt: RegionAttempt,
        region_error: tidb_proto::RegionError,
    ) -> Result<(), DirectUnaryTransportError> {
        self.check_retry_active()?;
        if let Some(server_busy) = &region_error.server_is_busy {
            let fast_retry = {
                let selector = self.request_selectors.get_mut(&logical_task_id).ok_or(
                    DirectUnaryTransportError::ResponseState(
                        "missing request selector for ServerIsBusy",
                    ),
                )?;
                let fast_retry =
                    server_busy.estimated_wait_ms > 0 && !selector.busy_threshold().is_zero();
                cache_operation(&self.shared_runtime, |region_cache| {
                    region_cache.on_server_busy(
                        selector,
                        &observed_attempt,
                        server_busy.estimated_wait_ms,
                        (self.config.observation_time)(),
                    )
                })?
                .map_err(|error| DirectUnaryTransportError::RegionRecovery(error.to_string()))?;
                if fast_retry && !selector.acknowledge_server_busy(&observed_attempt) {
                    return Err(DirectUnaryTransportError::RegionRecovery(
                        "ServerIsBusy did not match the completed selector attempt".to_owned(),
                    ));
                }
                fast_retry
            };
            if fast_retry {
                let replacement = self.runtime.retry_transport_attempt(failed)?;
                return self.install_same_task_retry(replacement);
            }
        }
        if region_error.store_not_match.is_some() {
            try_borrow_client(self.shared_runtime.client())?
                .close_address(&observed_attempt.address)
                .map_err(DirectUnaryTransportError::Client)?;
        }
        let region_id = observed_attempt.region.id;
        let region_retry_max_sleep = self.config.region_retry_max_sleep;
        let disposition = {
            let budget = self
                .region_backoffs
                .entry(region_id)
                .or_insert_with(|| RegionBackoffBudget::new(region_retry_max_sleep));
            cache_operation(&self.shared_runtime, |region_cache| {
                region_cache.on_region_error(&region_error, observed_attempt, budget)
            })?
            .map_err(|error| DirectUnaryTransportError::RegionRecovery(error.to_string()))?
        };

        match disposition {
            RegionErrorDisposition::RetrySelector {
                attempt,
                transition,
                delay,
            } => {
                if !self
                    .request_selectors
                    .get_mut(&logical_task_id)
                    .ok_or(DirectUnaryTransportError::ResponseState(
                        "missing request selector for typed recovery",
                    ))?
                    .apply_recovery(&attempt, transition)
                {
                    return Err(DirectUnaryTransportError::RegionRecovery(
                        "typed recovery did not match the selector's completed attempt".to_owned(),
                    ));
                }
                self.sleep_retry(delay)?;
                let replacement = self.runtime.retry_transport_attempt(failed)?;
                self.install_same_task_retry(replacement)
            }
            RegionErrorDisposition::RetryRoute { delay, .. } => {
                self.sleep_retry(delay)?;
                let ranges = failed.ranges().to_vec();
                let topology = self.locate_retry_ranges(&ranges)?;
                let replacement = self.runtime.retry_region_attempt(failed, &topology)?;
                self.request_selectors.remove(&logical_task_id);
                self.install_same_task_retry(replacement)
            }
            RegionErrorDisposition::RebuildRanges { delay, action } => {
                self.sleep_retry(delay)?;
                self.check_retry_active()?;
                cache_operation(&self.shared_runtime, |region_cache| {
                    region_cache.apply_rebuild_action(action)
                })?
                .map_err(|error| DirectUnaryTransportError::RegionRecovery(error.to_string()))?;
                let outer_delay = self
                    .region_backoffs
                    .get_mut(&region_id)
                    .expect("region budget is created before disposition")
                    .next_delay(RegionBackoffKind::RegionMiss)
                    .map_err(|error| {
                        DirectUnaryTransportError::RegionTerminal(format!("{error:?}"))
                    })?;
                self.sleep_retry(outer_delay)?;
                let ranges = self.runtime.retry_ranges_from(&failed)?;
                let topology = self.locate_retry_ranges(&ranges)?;
                let replacement = self.runtime.rebuild_region_attempts(failed, &topology)?;
                self.request_selectors.remove(&logical_task_id);
                self.install_rebuilt_tail(replacement)
            }
            RegionErrorDisposition::ReturnRegionError => Err(
                DirectUnaryTransportError::Coordinator(CopReadTaskError::RegionError.to_string()),
            ),
            RegionErrorDisposition::Terminal(error) => Err(
                DirectUnaryTransportError::RegionTerminal(format!("{error:?}")),
            ),
        }
    }

    fn locate_retry_ranges(
        &mut self,
        ranges: &[crate::RequestKeyRange],
    ) -> Result<Vec<RegionTaskTopology>, DirectUnaryTransportError> {
        self.check_retry_active()?;
        let region_ranges: Vec<_> = ranges
            .iter()
            .map(|range| RegionKeyRange::new(range.start_key.clone(), range.end_key.clone()))
            .collect();
        let locations = self
            .shared_runtime
            .locate_ranges(&region_ranges)
            .map_err(|_| DirectUnaryTransportError::RegionCacheLifecycle)?
            .map_err(DirectUnaryTransportError::Route)?;
        Ok(topology_from_locations(locations))
    }

    fn install_same_task_retry(
        &mut self,
        replacement: super::CopReadTaskReplacement,
    ) -> Result<(), DirectUnaryTransportError> {
        if replacement.logical_task_ids.len() != 1 || replacement.active_attempt_ids.len() != 1 {
            return Err(DirectUnaryTransportError::ResponseState(
                "known-leader retry did not produce one attempt",
            ));
        }
        self.active_attempts.insert(
            replacement.logical_task_ids[0],
            replacement.active_attempt_ids[0],
        );
        Ok(())
    }

    fn install_rebuilt_tail(
        &mut self,
        replacement: super::CopReadTaskReplacement,
    ) -> Result<(), DirectUnaryTransportError> {
        if replacement.logical_task_ids.len() != replacement.active_attempt_ids.len()
            || replacement.logical_task_ids.is_empty()
        {
            return Err(DirectUnaryTransportError::ResponseState(
                "region rebuild produced an invalid active tail",
            ));
        }
        let failed_logical_task_id = self.logical_order.get(self.logical_index).copied().ok_or(
            DirectUnaryTransportError::ResponseState("region rebuild has no current logical task"),
        )?;
        if replacement.logical_task_ids[0] != failed_logical_task_id {
            return Err(DirectUnaryTransportError::ResponseState(
                "region rebuild changed the failed logical task identity",
            ));
        }
        let sync_only_chain = self.sync_only_chains.contains(&failed_logical_task_id);
        self.active_attempts.remove(&failed_logical_task_id);
        self.request_selectors.remove(&failed_logical_task_id);
        self.logical_order.splice(
            self.logical_index..=self.logical_index,
            replacement.logical_task_ids.iter().copied(),
        );
        for (logical_task_id, attempt_id) in replacement
            .logical_task_ids
            .into_iter()
            .zip(replacement.active_attempt_ids)
        {
            if sync_only_chain {
                self.sync_only_chains.insert(logical_task_id);
            }
            self.active_attempts.insert(logical_task_id, attempt_id);
        }
        Ok(())
    }

    fn rebuild_exhausted_region(
        &mut self,
        failed: super::FailedCopReadAttempt,
        region_id: u64,
    ) -> Result<(), DirectUnaryTransportError> {
        let delay = self
            .region_backoffs
            .entry(region_id)
            .or_insert_with(|| RegionBackoffBudget::new(self.config.region_retry_max_sleep))
            .next_delay(RegionBackoffKind::RegionMiss)
            .map_err(|error| DirectUnaryTransportError::RegionTerminal(format!("{error:?}")))?;
        self.sleep_retry(delay)?;
        let ranges = self.runtime.retry_ranges_from(&failed)?;
        let topology = self.locate_retry_ranges(&ranges)?;
        let replacement = self.runtime.rebuild_region_attempts(failed, &topology)?;
        self.install_rebuilt_tail(replacement)
    }

    fn check_retry_active(&self) -> Result<(), DirectUnaryTransportError> {
        if self.closed || self.cancellation.is_cancelled() {
            return Err(DirectUnaryTransportError::CallerCancelled);
        }
        if self.call.timeout().is_zero() {
            return Err(DirectUnaryTransportError::DeadlineExceeded);
        }
        Ok(())
    }

    fn sleep_retry(&self, delay: Duration) -> Result<(), DirectUnaryTransportError> {
        self.check_retry_active()?;
        if delay.is_zero() {
            return Ok(());
        }
        let remaining = self.call.timeout();
        if remaining.is_zero() || delay >= remaining {
            return Err(DirectUnaryTransportError::DeadlineExceeded);
        }
        if self
            .config
            .region_retry_waiter
            .wait(&self.cancellation.unary_cancellation(), delay)
        {
            return Err(DirectUnaryTransportError::CallerCancelled);
        }
        self.check_retry_active()
    }

    fn fail<T>(&mut self, error: DirectUnaryTransportError) -> Result<T, QueryResponseError> {
        self.closed = true;
        if matches!(error, DirectUnaryTransportError::CallerCancelled) {
            Err(QueryResponseError::Cancelled)
        } else {
            Err(QueryResponseError::Source(error.to_string()))
        }
    }
}

impl<C: DirectUnaryClient, L: RegionRecoveryLoader> QueryResponse
    for DirectUnaryQueryResponse<C, L>
{
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.pull(usize::MAX)
    }

    fn next_with_required_rows(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.pull(required_rows)
    }

    fn close(&mut self) {
        self.cancellation.cancel();
        self.closed = true;
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::{try_borrow_client, DirectUnaryTransportError};

    #[test]
    fn shared_client_borrow_conflict_is_a_typed_lifecycle_error() {
        let client = RefCell::new(());
        let active_dispatch = client.borrow_mut();

        let error = match try_borrow_client(&client) {
            Ok(_) => panic!("a second mutable client owner must be rejected"),
            Err(error) => error,
        };
        assert_eq!(error, DirectUnaryTransportError::ClientLifecycle);
        assert_eq!(error.kind(), "client_lifecycle");

        drop(active_dispatch);
        assert!(try_borrow_client(&client).is_ok());
    }
}

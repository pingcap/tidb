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
//! discovers every required region and selects its PD-declared leader, while
//! TiKV dispatch remains lazy until the consumer pulls the returned response.
//! Region errors are classified through the sole [`RegionCache`] authority.
//! This response then owns the bounded resend/rebuild loop, per-region
//! backoff budgets, cancellation-aware sleep, and ordered replacement of only
//! unconsumed work.

use std::cell::{RefCell, RefMut};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::query_runtime::{
    QueryDispatch, QueryOperation, QueryResponse, QueryResponseError, QueryResultSubset,
    QueryTransport,
};
use crate::{
    CoprCache, CoprCacheConfig, ResponseChannelEvent, TransportRequest, TransportRequestError,
};
use tidb_txnkv::region::{
    KeyRange as RegionKeyRange, OwnedLeaderRoute, ReadPolicy, RegionAttempt, RegionBackoffBudget,
    RegionBackoffKind, RegionCache, RegionErrorDisposition, RegionLoader, RegionLocation,
    RegionRecoveryLoader, RegionRouteError, RegionVerId, ReplicaSelector,
};
pub use tidb_txnkv::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryRequest, DirectUnaryResponse,
};
use tidb_txnkv::{EndpointType, TraceInfo};

use super::{
    build_tikv_unary_request, decode_tikv_unary_response, CopPagingState, CopReadTaskError,
    CopReadTaskRuntime, ReadEngineGeneration,
};
use crate::{RegionTaskEpoch, RegionTaskPeer, RegionTaskTopology, ReplicaReadType};

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
    /// Cancellation and sleeping owner for region-recovery delays.
    pub region_retry_control: Rc<dyn RegionRetryControl>,
    /// Effective sleep budget shared by both retry levels for one region ID.
    pub region_retry_max_sleep: Duration,
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
            .field("region_retry_control", &self.region_retry_control)
            .field("region_retry_max_sleep", &self.region_retry_max_sleep)
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
            region_retry_control: Rc::new(ThreadRegionRetryControl),
            region_retry_max_sleep: Duration::from_secs(20),
        }
    }
}

/// Cancellation result returned before retry-side mutation or dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionRetryCancelled;

impl std::fmt::Display for RegionRetryCancelled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("region retry cancelled")
    }
}

impl std::error::Error for RegionRetryCancelled {}

/// Injected cancellation-aware sleeper for response-owned region recovery.
///
/// This seam deliberately does not claim cancellation of an active blocking
/// `DirectUnaryClient::send_request` call.
pub trait RegionRetryControl: std::fmt::Debug {
    /// Rejects work before cache mutation, PD lookup, or TiKV dispatch.
    fn check_cancelled(&self) -> Result<(), RegionRetryCancelled>;

    /// Sleeps one already-reserved delay, returning early on cancellation.
    fn sleep(&self, delay: Duration) -> Result<(), RegionRetryCancelled>;
}

#[derive(Debug)]
struct ThreadRegionRetryControl;

impl RegionRetryControl for ThreadRegionRetryControl {
    fn check_cancelled(&self) -> Result<(), RegionRetryCancelled> {
        Ok(())
    }

    fn sleep(&self, delay: Duration) -> Result<(), RegionRetryCancelled> {
        std::thread::sleep(delay);
        Ok(())
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
    /// Cancellation won before retry-side mutation, sleep, PD, or dispatch.
    RetryCancelled,
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
            Self::RetryCancelled => "retry_cancelled",
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
            Self::RetryCancelled => formatter.write_str("region retry cancelled"),
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectedRegionRoute {
    region: RegionVerId,
    peer_id: u64,
    store_id: u64,
    address: String,
    store_epoch: u64,
}

/// Injected transport that creates lazy, response-owned unary runtimes.
pub struct DirectUnaryQueryTransport<C, L> {
    client: Rc<RefCell<C>>,
    region_cache: Rc<RefCell<RegionCache<L>>>,
    config: DirectUnaryRuntimeConfig,
}

impl<C, L: RegionLoader> DirectUnaryQueryTransport<C, L> {
    /// Retains one client and one PD-backed region-cache authority.
    pub fn new(
        client: C,
        region_cache: RegionCache<L>,
        config: DirectUnaryRuntimeConfig,
    ) -> Result<Self, DirectUnaryTransportError> {
        if region_cache.cluster_id() == 0 {
            return Err(RegionRouteError::MissingClusterId.into());
        }
        Ok(Self {
            client: Rc::new(RefCell::new(client)),
            region_cache: Rc::new(RefCell::new(region_cache)),
            config,
        })
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
        if dispatch.operation != QueryOperation::SelectWithRuntimeStats {
            return Err(
                DirectUnaryTransportError::UnsupportedOperation(dispatch.operation).to_string(),
            );
        }
        let metadata = request
            .metadata_for_send()
            .map_err(|error| DirectUnaryTransportError::Request(error).to_string())?;
        if metadata.session.replica_read != ReplicaReadType::Leader || metadata.is_staleness {
            return Err(
                DirectUnaryTransportError::Route(RegionRouteError::UnsupportedReadPolicy)
                    .to_string(),
            );
        }
        CopPagingState::validate_read_request(metadata)
            .map_err(|error| DirectUnaryTransportError::from(error).to_string())?;
        let requested_ranges =
            metadata_region_ranges(metadata).map_err(|error| error.to_string())?;
        let (cluster_id, locations) = {
            let mut region_cache = self
                .region_cache
                .try_borrow_mut()
                .map_err(|_| DirectUnaryTransportError::RegionCacheLifecycle.to_string())?;
            let cluster_id = region_cache.cluster_id();
            let locations = region_cache
                .locate_ranges(&requested_ranges)
                .map_err(|error| DirectUnaryTransportError::Route(error).to_string())?;
            (cluster_id, locations)
        };
        let (topology, routes) = topology_from_locations(locations)
            .map_err(|error| DirectUnaryTransportError::Route(error).to_string())?;
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
            let region = task_region_ver_id(prepared.task()).map_err(|error| error.to_string())?;
            if !routes.contains_key(&region) {
                return Err(DirectUnaryTransportError::Coordinator(
                    "prepared task has no PD-backed route".to_owned(),
                )
                .to_string());
            }
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

        Ok(Some(DirectUnaryQueryResponse {
            client: Rc::clone(&self.client),
            region_cache: Rc::clone(&self.region_cache),
            metadata: metadata.clone(),
            cluster_id,
            routes,
            config: self.config.clone(),
            runtime,
            logical_order,
            active_attempts,
            logical_index: 0,
            closed: false,
            region_backoffs: BTreeMap::new(),
        }))
    }
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

fn topology_from_locations(
    locations: Vec<RegionLocation>,
) -> Result<
    (
        Vec<RegionTaskTopology>,
        BTreeMap<RegionVerId, SelectedRegionRoute>,
    ),
    RegionRouteError,
> {
    let mut topology = Vec::with_capacity(locations.len());
    let mut routes = BTreeMap::new();
    for location in locations {
        let selected = ReplicaSelector::select_leader(&location, ReadPolicy::default())?;
        let peer = RegionTaskPeer {
            id: selected.peer.id,
            store_id: selected.peer.store_id,
            role: selected.peer.role.as_i32(),
            is_witness: selected.peer.is_witness,
        };
        routes.insert(
            location.region,
            SelectedRegionRoute {
                region: location.region,
                peer_id: selected.peer.id,
                store_id: selected.peer.store_id,
                address: selected.store.address.clone(),
                store_epoch: selected.store.epoch,
            },
        );
        topology.push(RegionTaskTopology {
            region_id: location.region.id,
            region_epoch: Some(RegionTaskEpoch {
                conf_ver: location.region.epoch.conf_ver,
                version: location.region.epoch.version,
            }),
            peer: Some(peer),
            start_key: location.start_key,
            end_key: location.end_key,
            ..RegionTaskTopology::default()
        });
    }
    Ok((topology, routes))
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
    client: Rc<RefCell<C>>,
    region_cache: Rc<RefCell<RegionCache<L>>>,
    metadata: crate::KvRequestMetadata,
    cluster_id: u64,
    routes: BTreeMap<RegionVerId, SelectedRegionRoute>,
    config: DirectUnaryRuntimeConfig,
    runtime: CopReadTaskRuntime,
    logical_order: Vec<u64>,
    active_attempts: BTreeMap<u64, u64>,
    logical_index: usize,
    closed: bool,
    region_backoffs: BTreeMap<u64, RegionBackoffBudget>,
}

fn try_borrow_client<C>(client: &RefCell<C>) -> Result<RefMut<'_, C>, DirectUnaryTransportError> {
    client
        .try_borrow_mut()
        .map_err(|_| DirectUnaryTransportError::ClientLifecycle)
}

impl<C: DirectUnaryClient, L: RegionRecoveryLoader> DirectUnaryQueryResponse<C, L> {
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
            if let Err(error) = self.dispatch_attempt(logical_task_id, attempt_id) {
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
        let route = self.routes.get(&region).ok_or_else(|| {
            DirectUnaryTransportError::Coordinator(
                "prepared task has no PD-backed route".to_owned(),
            )
        })?;
        let observed_attempt = RegionAttempt {
            region: route.region,
            peer_id: route.peer_id,
            store_id: route.store_id,
            address: route.address.clone(),
            store_epoch: route.store_epoch,
        };
        let predicted_read_bytes = self
            .runtime
            .task_predicted_read_bytes(logical_task_id)
            .ok_or(DirectUnaryTransportError::ResponseState(
                "active task has no read-byte prediction",
            ))?;
        let request = build_tikv_unary_request(
            &prepared,
            &self.metadata,
            predicted_read_bytes,
            self.config.trace.as_ref(),
            self.cluster_id,
        );
        let timeout = request
            .timeout_override_ms
            .map(Duration::from_millis)
            .unwrap_or(self.config.default_timeout);
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
        let raw_response = try_borrow_client(self.client.as_ref())?
            .send_request(&route.address, &client_request, timeout)
            .map_err(DirectUnaryTransportError::Client)?;
        let response = decode_tikv_unary_response(&raw_response.encoded_response)
            .map_err(|error| DirectUnaryTransportError::Decode(error.to_string()))?;
        if let Some(region_error) = response.region_error_ref().cloned() {
            let failed = self.runtime.consume_region_error(attempt_id)?;
            return self.recover_region_error(failed, observed_attempt, region_error);
        }
        let accepted = match self.runtime.accept_response(
            attempt_id,
            response,
            None,
            (self.config.observation_time)(),
        ) {
            Ok(accepted) => accepted,
            Err(error) => return Err(error.into()),
        };
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

    fn recover_region_error(
        &mut self,
        failed: super::FailedCopReadAttempt,
        observed_attempt: RegionAttempt,
        region_error: tidb_proto::RegionError,
    ) -> Result<(), DirectUnaryTransportError> {
        self.check_retry_active()?;
        let region_id = observed_attempt.region.id;
        let region_retry_max_sleep = self.config.region_retry_max_sleep;
        let disposition = {
            let budget = self
                .region_backoffs
                .entry(region_id)
                .or_insert_with(|| RegionBackoffBudget::new(region_retry_max_sleep));
            self.region_cache
                .try_borrow_mut()
                .map_err(|_| DirectUnaryTransportError::RegionCacheLifecycle)?
                .on_region_error(&region_error, observed_attempt, budget)
                .map_err(|error| DirectUnaryTransportError::RegionRecovery(error.to_string()))?
        };

        match disposition {
            RegionErrorDisposition::RetryRoute { route, delay } => {
                self.sleep_retry(delay)?;
                let ranges = failed.ranges().to_vec();
                let (topology, routes) = self.locate_retry_ranges(&ranges)?;
                validate_retry_route(&route, &routes)?;
                let replacement = self.runtime.retry_region_attempt(failed, &topology)?;
                self.install_same_task_retry(replacement, routes)
            }
            RegionErrorDisposition::RebuildRanges { delay, action } => {
                self.sleep_retry(delay)?;
                self.check_retry_active()?;
                self.region_cache
                    .try_borrow_mut()
                    .map_err(|_| DirectUnaryTransportError::RegionCacheLifecycle)?
                    .apply_rebuild_action(action)
                    .map_err(|error| {
                        DirectUnaryTransportError::RegionRecovery(error.to_string())
                    })?;
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
                let (topology, routes) = self.locate_retry_ranges(&ranges)?;
                let replacement = self.runtime.rebuild_region_attempts(failed, &topology)?;
                self.install_rebuilt_tail(replacement, routes)
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
    ) -> Result<
        (
            Vec<RegionTaskTopology>,
            BTreeMap<RegionVerId, SelectedRegionRoute>,
        ),
        DirectUnaryTransportError,
    > {
        self.check_retry_active()?;
        let region_ranges: Vec<_> = ranges
            .iter()
            .map(|range| RegionKeyRange::new(range.start_key.clone(), range.end_key.clone()))
            .collect();
        let locations = self
            .region_cache
            .try_borrow_mut()
            .map_err(|_| DirectUnaryTransportError::RegionCacheLifecycle)?
            .locate_ranges(&region_ranges)?;
        topology_from_locations(locations).map_err(Into::into)
    }

    fn install_same_task_retry(
        &mut self,
        replacement: super::CopReadTaskReplacement,
        routes: BTreeMap<RegionVerId, SelectedRegionRoute>,
    ) -> Result<(), DirectUnaryTransportError> {
        if replacement.logical_task_ids.len() != 1 || replacement.active_attempt_ids.len() != 1 {
            return Err(DirectUnaryTransportError::ResponseState(
                "known-leader retry did not produce one attempt",
            ));
        }
        self.routes.extend(routes);
        self.active_attempts.insert(
            replacement.logical_task_ids[0],
            replacement.active_attempt_ids[0],
        );
        Ok(())
    }

    fn install_rebuilt_tail(
        &mut self,
        replacement: super::CopReadTaskReplacement,
        routes: BTreeMap<RegionVerId, SelectedRegionRoute>,
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
        self.active_attempts.remove(&failed_logical_task_id);
        self.logical_order.splice(
            self.logical_index..=self.logical_index,
            replacement.logical_task_ids.iter().copied(),
        );
        for (logical_task_id, attempt_id) in replacement
            .logical_task_ids
            .into_iter()
            .zip(replacement.active_attempt_ids)
        {
            self.active_attempts.insert(logical_task_id, attempt_id);
        }
        self.routes.extend(routes);
        Ok(())
    }

    fn check_retry_active(&self) -> Result<(), DirectUnaryTransportError> {
        if self.closed {
            return Err(DirectUnaryTransportError::RetryCancelled);
        }
        self.config
            .region_retry_control
            .check_cancelled()
            .map_err(|_| DirectUnaryTransportError::RetryCancelled)
    }

    fn sleep_retry(&self, delay: Duration) -> Result<(), DirectUnaryTransportError> {
        self.check_retry_active()?;
        if delay.is_zero() {
            return Ok(());
        }
        self.config
            .region_retry_control
            .sleep(delay)
            .map_err(|_| DirectUnaryTransportError::RetryCancelled)?;
        self.check_retry_active()
    }

    fn fail<T>(&mut self, error: DirectUnaryTransportError) -> Result<T, QueryResponseError> {
        self.closed = true;
        Err(QueryResponseError::Source(error.to_string()))
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
        self.closed = true;
    }
}

fn validate_retry_route(
    expected: &OwnedLeaderRoute,
    routes: &BTreeMap<RegionVerId, SelectedRegionRoute>,
) -> Result<(), DirectUnaryTransportError> {
    let actual = routes.get(&expected.region).ok_or_else(|| {
        DirectUnaryTransportError::Coordinator(
            "updated leader route is absent from the region cache".to_owned(),
        )
    })?;
    if actual.peer_id != expected.peer_id
        || actual.store_id != expected.store_id
        || actual.address != expected.address
        || actual.store_epoch != expected.store_epoch
    {
        return Err(DirectUnaryTransportError::Coordinator(
            "updated leader route disagrees with the region cache".to_owned(),
        ));
    }
    Ok(())
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

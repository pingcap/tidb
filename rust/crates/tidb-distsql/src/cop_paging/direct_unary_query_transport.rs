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
//! The caller supplies checked region snapshots, a nonzero PD cluster identity,
//! and the exact address selected for each snapshot. The injected client may be
//! the real KV-owned tonic Coprocessor transport. A returned response owns the
//! coordinator and dispatches lazily, in logical-task order, only when its
//! consumer pulls. Region errors, locks, batch responses, and every other retry
//! case stop the response instead of manufacturing retry behavior.

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
pub use tidb_txnkv::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryRequest, DirectUnaryResponse,
};
use tidb_txnkv::{EndpointType, TraceInfo};

use super::{
    build_tikv_unary_request, decode_tikv_unary_response, CopPagingState, CopReadTaskError,
    CopReadTaskRuntime, ReadEngineGeneration,
};
use crate::RegionTaskTopology;

/// One checked region snapshot coupled to its selected store address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedRegionRoute {
    /// Region and bucket metadata used to build the cop task.
    pub topology: RegionTaskTopology,
    /// Non-empty address selected for this exact region snapshot.
    pub address: String,
}

/// Deterministic policy owned by the direct unary runtime.
#[derive(Clone, Debug)]
pub struct DirectUnaryRuntimeConfig {
    /// Default used when the task has no request-local read timeout.
    pub default_timeout: Duration,
    /// Storage generation used for paging read-byte accounting.
    pub read_engine_generation: ReadEngineGeneration,
    /// Initial shared read-byte prediction for one request runtime.
    pub seed_read_bytes: u64,
    /// PD cluster identity attached to every checked request context.
    /// The constructor rejects the default zero value before retaining the
    /// client or any route.
    pub cluster_id: u64,
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
}

impl Default for DirectUnaryRuntimeConfig {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(60),
            read_engine_generation: ReadEngineGeneration::Classic,
            seed_read_bytes: 0,
            cluster_id: 0,
            cache: None,
            trace: None,
            observation_time: system_observation_time,
        }
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
    /// Live TiKV contexts require the nonzero identity published by PD.
    MissingClusterId,
    /// Two supplied routes claim the same region ID.
    DuplicateRoute(u64),
    /// A supplied route has no usable address.
    MissingAddress(u64),
    /// No supplied region snapshot covers the built request.
    MissingRoute,
    /// Only the executor's `SelectWithRuntimeStats` DAG path is admitted.
    UnsupportedOperation(QueryOperation),
    /// The request was not bound by `InjectedQueryRuntime`.
    Request(TransportRequestError),
    /// Cache configuration is invalid.
    Cache(String),
    /// The checked read-task coordinator rejected the request or response.
    Coordinator(String),
    /// The injected client failed before returning a raw response.
    Client(String),
    /// Another response is already dispatching through the shared client.
    ClientLifecycle,
    /// The raw protobuf response was malformed.
    Decode(String),
    /// The coordinator produced a response-channel state this synchronous
    /// owner cannot consume.
    ResponseState(&'static str),
}

impl DirectUnaryTransportError {
    /// Stable category for source-shaped tests and callers.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::MissingClusterId => "missing_cluster_id",
            Self::DuplicateRoute(_) => "duplicate_route",
            Self::MissingAddress(_) => "missing_address",
            Self::MissingRoute => "missing_route",
            Self::UnsupportedOperation(_) => "unsupported_operation",
            Self::Request(_) => "request",
            Self::Cache(_) => "cache",
            Self::Coordinator(_) => "coordinator",
            Self::Client(_) => "client",
            Self::ClientLifecycle => "client_lifecycle",
            Self::Decode(_) => "decode",
            Self::ResponseState(_) => "response_state",
        }
    }
}

impl std::fmt::Display for DirectUnaryTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingClusterId => {
                formatter.write_str("direct unary runtime has no PD cluster identity")
            }
            Self::DuplicateRoute(region_id) => {
                write!(formatter, "duplicate route for region {region_id}")
            }
            Self::MissingAddress(region_id) => {
                write!(formatter, "missing address for region {region_id}")
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
            Self::Client(message) => write!(formatter, "unary client failed: {message}"),
            Self::ClientLifecycle => {
                formatter.write_str("unary client is already in use by another response")
            }
            Self::Decode(message) => write!(formatter, "invalid unary response: {message}"),
            Self::ResponseState(state) => {
                write!(formatter, "invalid unary response state: {state}")
            }
            Self::MissingRoute => formatter.write_str("request ranges have no exact region route"),
        }
    }
}

impl std::error::Error for DirectUnaryTransportError {}

impl From<CopReadTaskError> for DirectUnaryTransportError {
    fn from(error: CopReadTaskError) -> Self {
        Self::Coordinator(error.to_string())
    }
}

/// Injected transport that creates lazy, response-owned unary runtimes.
pub struct DirectUnaryQueryTransport<C> {
    client: Rc<RefCell<C>>,
    topology: Vec<RegionTaskTopology>,
    addresses: BTreeMap<u64, String>,
    config: DirectUnaryRuntimeConfig,
}

impl<C> DirectUnaryQueryTransport<C> {
    /// Validates a complete route set without opening a connection or sending.
    pub fn new(
        client: C,
        routes: impl IntoIterator<Item = ResolvedRegionRoute>,
        config: DirectUnaryRuntimeConfig,
    ) -> Result<Self, DirectUnaryTransportError> {
        if config.cluster_id == 0 {
            return Err(DirectUnaryTransportError::MissingClusterId);
        }
        let mut topology = Vec::new();
        let mut addresses = BTreeMap::new();
        for route in routes {
            let region_id = route.topology.region_id;
            if route.address.is_empty() {
                return Err(DirectUnaryTransportError::MissingAddress(region_id));
            }
            if addresses.insert(region_id, route.address).is_some() {
                return Err(DirectUnaryTransportError::DuplicateRoute(region_id));
            }
            topology.push(route.topology);
        }
        Ok(Self {
            client: Rc::new(RefCell::new(client)),
            topology,
            addresses,
            config,
        })
    }
}

impl<C: DirectUnaryClient + 'static> QueryTransport for DirectUnaryQueryTransport<C> {
    type Response = DirectUnaryQueryResponse<C>;

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
        let cache = CoprCache::from_optional_config(self.config.cache.as_ref())
            .map_err(|error| DirectUnaryTransportError::Cache(error.to_string()).to_string())?;
        let runtime = CopPagingState::prepare_read_tasks(
            metadata,
            &self.topology,
            cache,
            self.config.read_engine_generation,
            self.config.seed_read_bytes,
        )
        .map_err(|error| {
            let mapped = if matches!(error, CopReadTaskError::InvalidTopology) {
                DirectUnaryTransportError::MissingRoute
            } else {
                DirectUnaryTransportError::from(error)
            };
            mapped.to_string()
        })?;

        let mut logical_order = Vec::new();
        let mut active_attempts = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for prepared in runtime.prepared_attempts() {
            let region_id = prepared.task().region_id;
            if !self.addresses.contains_key(&region_id) {
                return Err(DirectUnaryTransportError::MissingRoute.to_string());
            }
            if seen.insert(prepared.logical_task_id()) {
                logical_order.push(prepared.logical_task_id());
            }
            active_attempts.insert(prepared.logical_task_id(), prepared.attempt_id());
        }
        if logical_order.is_empty() {
            return Err(DirectUnaryTransportError::MissingRoute.to_string());
        }

        Ok(Some(DirectUnaryQueryResponse {
            client: Rc::clone(&self.client),
            metadata: metadata.clone(),
            addresses: self.addresses.clone(),
            config: self.config.clone(),
            runtime,
            logical_order,
            active_attempts,
            logical_index: 0,
            closed: false,
        }))
    }
}

/// Lazy response owner returned by [`DirectUnaryQueryTransport`].
pub struct DirectUnaryQueryResponse<C> {
    client: Rc<RefCell<C>>,
    metadata: crate::KvRequestMetadata,
    addresses: BTreeMap<u64, String>,
    config: DirectUnaryRuntimeConfig,
    runtime: CopReadTaskRuntime,
    logical_order: Vec<u64>,
    active_attempts: BTreeMap<u64, u64>,
    logical_index: usize,
    closed: bool,
}

fn try_borrow_client<C>(client: &RefCell<C>) -> Result<RefMut<'_, C>, DirectUnaryTransportError> {
    client
        .try_borrow_mut()
        .map_err(|_| DirectUnaryTransportError::ClientLifecycle)
}

impl<C: DirectUnaryClient> DirectUnaryQueryResponse<C> {
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
                    return self.fail(DirectUnaryTransportError::Client(message));
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
        let prepared = self.runtime.prepared_attempt(attempt_id).cloned().ok_or(
            DirectUnaryTransportError::ResponseState("active attempt is not prepared"),
        )?;
        let address = self
            .addresses
            .get(&prepared.task().region_id)
            .ok_or(DirectUnaryTransportError::MissingRoute)?;
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
            self.config.cluster_id,
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
            .send_request(address, &client_request, timeout)
            .map_err(|error| DirectUnaryTransportError::Client(error.to_string()))?;
        let response = decode_tikv_unary_response(&raw_response.encoded_response)
            .map_err(|error| DirectUnaryTransportError::Decode(error.to_string()))?;
        let accepted = self.runtime.accept_response(
            attempt_id,
            response,
            None,
            (self.config.observation_time)(),
        )?;
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

    fn fail<T>(&mut self, error: DirectUnaryTransportError) -> Result<T, QueryResponseError> {
        self.closed = true;
        Err(QueryResponseError::Source(error.to_string()))
    }
}

impl<C: DirectUnaryClient> QueryResponse for DirectUnaryQueryResponse<C> {
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

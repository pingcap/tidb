// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Injected execution boundary for `pkg/distsql/distsql.go`.
//!
//! Request construction and response decoding already have single owners in
//! this crate. This module connects them at Go's `kv.Client.Send` boundary:
//! callers provide one immutable built request and one transport, and receive
//! a Go-shaped result that owns the raw response. Select consumption transfers
//! that owner once into the existing decoder; Analyze and Checksum keep raw
//! pulls. Concrete TiKV routing, RPC, retries, cancellation, memory accounting,
//! and asynchronous production stay outside this dependency-closed runtime.

pub mod query_response;

pub use query_response::{CopInfo, QueryResponse, QueryResponseError, QueryResultSubset, QuerySelectResult};

use tidb_datatype::{FieldType, SessionTimeZone};
use tidb_txnkv::EventCallback;

use crate::{
    analyze_request_source, analyze_result_metadata, checksum_result_metadata,
    select_result_metadata, select_with_runtime_stats, RequestSource, RequestType, SelectInput,
    SelectResultMetadata, TransportBinding, TransportRequest, TransportRequestError,
    WarningCollector,
};

/// Source operation sent through [`QueryTransport`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryOperation {
    /// `Select` without runtime-plan binding.
    Select,
    /// `SelectWithRuntimeStats`.
    SelectWithRuntimeStats,
    /// Statistics analyze request.
    Analyze,
    /// Checksum request.
    Checksum,
}

/// Immutable metadata presented to the injected transport for one send.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryDispatch {
    /// Source operation being executed.
    pub operation: QueryOperation,
    /// Result metadata created by the corresponding Go constructor.
    pub result: SelectResultMetadata,
    /// Request-source replacement made by `Analyze`, when applicable.
    pub request_source_override: Option<RequestSource>,
}

/// Result-decoding state retained by one Go-shaped `selectResult`.
///
/// Field layout and warning publication travel together from the DistSQL
/// context into the sole response owner. Bundling them prevents Select and
/// SelectWithRuntimeStats from growing parallel argument lists for state that
/// has the same lifetime and consumer.
#[derive(Clone)]
pub struct QueryResultContext {
    final_field_types: Vec<FieldType>,
    time_zone: SessionTimeZone,
    warnings: WarningCollector,
    event_callback: Option<EventCallback>,
}

impl QueryResultContext {
    /// Creates the decoding context for one result owner.
    #[must_use]
    pub fn new(final_field_types: Vec<FieldType>, warnings: WarningCollector) -> Self {
        Self {
            final_field_types,
            time_zone: SessionTimeZone::utc(),
            warnings,
            event_callback: None,
        }
    }

    /// Uses the statement location for schema-aware timestamp decoding.
    #[must_use]
    pub fn with_time_zone(mut self, time_zone: SessionTimeZone) -> Self {
        self.time_zone = time_zone;
        self
    }

    /// Installs the transaction-event callback passed through Go's
    /// `kv.ClientSendOption` boundary.
    #[must_use]
    pub fn with_event_callback(mut self, callback: EventCallback) -> Self {
        self.event_callback = Some(callback);
        self
    }
}

impl std::fmt::Debug for QueryResultContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryResultContext")
            .field("final_field_types", &self.final_field_types)
            .field("warnings", &self.warnings)
            .field("event_callback", &self.event_callback.is_some())
            .finish()
    }
}

/// Caller-supplied owner of the `kv.Client.Send` capability.
pub trait QueryTransport {
    /// Raw response owner returned by this transport.
    type Response: QueryResponse;

    /// Installs the callback from this send's client options.
    ///
    /// Transports without transaction events keep the default no-op. A
    /// concrete transport must copy the callback into its response owner
    /// during `send`; the runtime clears this transient slot immediately
    /// afterward.
    fn set_event_callback(&mut self, _callback: Option<EventCallback>) {}

    /// Sends one already-built request and returns its raw response owner.
    ///
    /// `Ok(None)` is kept distinct because Go treats a nil response as an
    /// explicit error rather than an empty result.
    fn send(
        &mut self,
        request: &TransportRequest,
        dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String>;
}

impl<T: QueryTransport + ?Sized> QueryTransport for &mut T {
    type Response = T::Response;

    fn set_event_callback(&mut self, callback: Option<EventCallback>) {
        (**self).set_event_callback(callback);
    }

    fn send(
        &mut self,
        request: &TransportRequest,
        dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        (**self).send(request, dispatch)
    }
}

/// Errors returned by the injected query runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryRuntimeError {
    /// The immutable request could not be exclusively bound to this transport.
    Request(TransportRequestError),
    /// The injected transport returned Go's nil-response case.
    NilResponse,
    /// The injected transport failed before yielding a response iterator.
    Transport(String),
    /// The canonical query cancellation won before a response was returned.
    Cancelled,
}

impl std::fmt::Display for QueryRuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(error) => write!(formatter, "DistSQL request is not sendable: {error:?}"),
            Self::NilResponse => formatter.write_str("client returns nil response"),
            Self::Transport(message) => formatter.write_str(message),
            Self::Cancelled => formatter.write_str("query cancelled by caller"),
        }
    }
}

impl std::error::Error for QueryRuntimeError {}

/// Live DistSQL send boundary backed by one injected transport.
pub struct InjectedQueryRuntime<T> {
    transport: T,
}

impl<T: QueryTransport> InjectedQueryRuntime<T> {
    /// Binds a caller-owned transport to this runtime.
    #[must_use]
    pub const fn new(transport: T) -> Self {
        Self { transport }
    }

    /// Returns the transport after the runtime is no longer needed.
    #[must_use]
    pub fn into_transport(self) -> T {
        self.transport
    }

    /// Executes Go's `Select` path.
    pub fn select(
        &mut self,
        request: &TransportRequest,
        mut input: SelectInput,
        result_context: QueryResultContext,
    ) -> Result<QuerySelectResult<T::Response>, QueryRuntimeError> {
        let _region = tidb_util::tracing::start_thread_region_ex("distsql.Select");
        input.row_len = result_context.final_field_types.len();
        self.execute(
            request,
            QueryDispatch {
                operation: QueryOperation::Select,
                result: select_result_metadata(select_input_from_request(request, input)),
                request_source_override: None,
            },
            result_context,
            None,
        )
    }

    /// Executes `SelectWithRuntimeStats` and binds plan metadata to the sole
    /// response iterator before returning it to the executor.
    pub fn select_with_runtime_stats(
        &mut self,
        request: &TransportRequest,
        mut input: SelectInput,
        result_context: QueryResultContext,
        cop_plan_ids: Vec<isize>,
        root_plan_id: isize,
        collector_enabled: bool,
    ) -> Result<QuerySelectResult<T::Response>, QueryRuntimeError> {
        // Go `SelectWithRuntimeStats` sends through `Select`.
        let _region = tidb_util::tracing::start_thread_region_ex("distsql.Select");
        input.row_len = result_context.final_field_types.len();
        self.execute(
            request,
            QueryDispatch {
                operation: QueryOperation::SelectWithRuntimeStats,
                result: select_with_runtime_stats(
                    select_input_from_request(request, input),
                    cop_plan_ids,
                    root_plan_id,
                ),
                request_source_override: None,
            },
            result_context,
            Some(collector_enabled),
        )
    }

    /// Executes Go's `Analyze` path with the internal statistics source
    /// override visible to the transport owner.
    pub fn analyze(
        &mut self,
        request: &TransportRequest,
        in_restricted_sql: bool,
    ) -> Result<QuerySelectResult<T::Response>, QueryRuntimeError> {
        self.execute(
            request,
            QueryDispatch {
                operation: QueryOperation::Analyze,
                result: analyze_result_metadata(request.metadata().store_type, in_restricted_sql),
                request_source_override: Some(analyze_request_source()),
            },
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            None,
        )
    }

    /// Executes Go's `Checksum` path.
    pub fn checksum(
        &mut self,
        request: &TransportRequest,
    ) -> Result<QuerySelectResult<T::Response>, QueryRuntimeError> {
        self.execute(
            request,
            QueryDispatch {
                operation: QueryOperation::Checksum,
                result: checksum_result_metadata(request.metadata().store_type),
                request_source_override: None,
            },
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            None,
        )
    }

    fn execute(
        &mut self,
        request: &TransportRequest,
        dispatch: QueryDispatch,
        result_context: QueryResultContext,
        runtime_stats_collector_enabled: Option<bool>,
    ) -> Result<QuerySelectResult<T::Response>, QueryRuntimeError> {
        let response = send_request(
            &mut self.transport,
            request,
            &dispatch,
            result_context.event_callback.clone(),
        )?;
        Ok(QuerySelectResult::new(
            response,
            dispatch.result,
            result_context.final_field_types,
            result_context.time_zone,
            result_context.warnings,
            runtime_stats_collector_enabled,
        ))
    }
}

/// Go `distsql.SelectWithRuntimeStats` over `DistSQLContext.Client`:
/// `dctx.Client.Send(kvReq)` and the select result over its response, with
/// the reader's plan ids bound for runtime statistics. The client binds the
/// request and owns its cancellation, as Go's `CopClient.Send` does.
pub fn select_with_runtime_stats_over_client(
    client: &dyn tidb_txnkv::Client,
    request: &TransportRequest,
    mut input: SelectInput,
    result_context: QueryResultContext,
    cop_plan_ids: Vec<isize>,
    root_plan_id: isize,
    collector_enabled: bool,
) -> Result<QuerySelectResult<Box<dyn tidb_txnkv::Response + Send>>, QueryRuntimeError> {
    let _region = tidb_util::tracing::start_thread_region_ex("distsql.Select");
    input.row_len = result_context.final_field_types.len();
    let metadata = select_with_runtime_stats(
        select_input_from_request(request, input),
        cop_plan_ids,
        root_plan_id,
    );
    let response = client.send(request).map_err(QueryRuntimeError::Transport)?;
    Ok(QuerySelectResult::new(
        response,
        metadata,
        result_context.final_field_types,
        result_context.time_zone,
        result_context.warnings,
        Some(collector_enabled),
    ))
}

/// The `kv.Client.Send` boundary over one injected transport: claims the
/// built request for this send (minting its request-local cancellation),
/// sends it, and returns the raw response owner.
pub fn send_request<T: QueryTransport>(
    transport: &mut T,
    request: &TransportRequest,
    dispatch: &QueryDispatch,
    event_callback: Option<EventCallback>,
) -> Result<T::Response, QueryRuntimeError> {
    let bound = match dispatch.request_source_override.clone() {
        Some(request_source) => request
            .bind_with_request_source(TransportBinding::new(), request_source)
            .map_err(QueryRuntimeError::Request)?,
        None => request
            .bind(TransportBinding::new())
            .map_err(QueryRuntimeError::Request)?,
    };
    if bound
        .request_cancellation()
        .map_err(QueryRuntimeError::Request)?
        .is_cancelled()
    {
        return Err(QueryRuntimeError::Cancelled);
    }
    transport.set_event_callback(event_callback);
    let response = transport.send(&bound, dispatch);
    transport.set_event_callback(None);
    // The canonical carrier has Go's post-Send ctx.Err precedence over a
    // simultaneous transport failure or response.
    if bound
        .request_cancellation()
        .map_err(QueryRuntimeError::Request)?
        .is_cancelled()
    {
        return Err(QueryRuntimeError::Cancelled);
    }
    response
        .map_err(QueryRuntimeError::Transport)?
        .ok_or(QueryRuntimeError::NilResponse)
}

impl QueryDispatch {
    /// The dispatch a `kv.Client.Send` presents to its transport for a
    /// request DistSQL's `Select`, `Analyze` or `Checksum` built: the
    /// operation its request type names, with that constructor's result
    /// metadata. `None` for a request type no DistSQL operation sends.
    #[must_use]
    pub fn for_request(request: &TransportRequest) -> Option<Self> {
        let metadata = request.metadata();
        let (operation, result) = match metadata.request_type {
            RequestType::Dag => (
                QueryOperation::Select,
                select_result_metadata(select_input_from_request(
                    request,
                    SelectInput {
                        in_restricted_sql: metadata.request_source.internal,
                        ..SelectInput::default()
                    },
                )),
            ),
            RequestType::Analyze => (
                QueryOperation::Analyze,
                analyze_result_metadata(metadata.store_type, metadata.request_source.internal),
            ),
            RequestType::Checksum => (
                QueryOperation::Checksum,
                checksum_result_metadata(metadata.store_type),
            ),
            _ => return None,
        };
        Some(Self {
            operation,
            result,
            request_source_override: None,
        })
    }
}

/// Go `distsql.Checksum` (`pkg/distsql/distsql.go`): sends one built
/// `kv.ReqTypeChecksum` request through `client` and returns the result its
/// region responses are read from with `NextRaw`. An `Err` is the client's
/// rendered failure.
pub fn checksum(
    client: &dyn tidb_txnkv::Client,
    request: &TransportRequest,
) -> Result<QuerySelectResult<Box<dyn tidb_txnkv::Response + Send>>, String> {
    let response = client.send(request)?;
    let context = QueryResultContext::new(Vec::new(), WarningCollector::new());
    Ok(QuerySelectResult::new(
        response,
        checksum_result_metadata(request.metadata().store_type),
        context.final_field_types,
        context.time_zone,
        context.warnings,
        None,
    ))
}

/// Retains caller-owned context and field metadata while taking request-owned
/// store, paging, and concurrency from the immutable built request. This
/// prevents a second transport-facing copy from silently drifting from what
/// the request builder produced.
fn select_input_from_request(request: &TransportRequest, mut input: SelectInput) -> SelectInput {
    let metadata = request.metadata();
    input.store_type = metadata.store_type;
    input.paging_enabled = metadata.paging.enabled;
    input.paging_size_bytes = metadata.paging.size_bytes;
    input.dist_sql_concurrency = u64::try_from(metadata.concurrency).unwrap_or(0);
    input
}

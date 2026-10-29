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

//! Go `pkg/store/copr`'s read path over the node's client-rust store.
//!
//! The coprocessor keeps what Go's keeps: building region tasks over the
//! store's region cache (`buildCopTasks` over `SplitKeyRangesByLocations`),
//! paging, the coprocessor cache, rebuilding a task's ranges when the sender
//! returns a region error (`handleCopResponse`), and resolving the locks a
//! read meets (`handleLockErr`). Sending is the store's: client-go's
//! `RegionRequestSender.SendReq`, which picks the replica and retries the
//! failures and region errors it owns, charging the task's `Backoffer`. The
//! `Backoffer` carries the request's ctx interceptors, so TopSQL's KV-exec
//! counter sees every RPC, retries included (Go
//! `distsql.WithSQLKvExecCounterInterceptor`).
//!
//! Tasks split at the cached region's buckets (Go `SplitKeyRangesByBuckets`),
//! a newer bucket version reported by TiKV refreshes them in the background
//! (`UpdateBucketsIfNeeded`), and "Request range exceeds bound" rebuilds a
//! task without buckets. A request that `checkStoreBatchCopr` admits batches
//! tasks by store; the children's responses are served through the parent's
//! channel and the children it did not serve run as tasks of their own
//! (`handleBatchCopResponse`).
//!
//! # Narrowings, by name
//!
//! * Go's `copIterator.GetStoreBatchInfo` counters (batched and fallback
//!   task counts) are not kept: nothing in this node reports them.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as _;

use crate::query_runtime::{QueryDispatch, QueryOperation, QueryTransport};
use tidb_txnkv::QueryResponseError;
use tidb_txnkv::QueryResultSubset;
use crate::{
    CancelHandle, CoprCache, RegionTaskEpoch, RegionTaskPeer, RegionTaskTopology,
    ResponseChannelEvent, TransportRequest,
};
use tidb_store_driver_txn::driver::cop_store::{CopBackoffer, NativeCopSend, NativeCopStore};
use tidb_txnkv::lock::SnapshotLockSet;
use tidb_txnkv::UnaryCallContext;

use super::{
    build_tikv_unary_request, decode_tikv_unary_response, BatchedRemain, BatchedRemainCause,
    CopPagingState, CopReadTaskError, CopReadTaskResponse, CopReadTaskRuntime, CopRuntimeConfig,
};

/// Why a read stopped: the task state machine's own errors keep their type,
/// so the statement sees Go's error for them (`ErrMaxKeysReadExceeded`).
#[derive(Debug)]
enum NativeCopError {
    Task(CopReadTaskError),
    Client(tikv_client::Error),
    /// Go `copIterator.Next`'s `CheckVisibility` failure.
    Visibility(tidb_txnkv::VisibilityError),
    Other(String),
}

impl From<tikv_client::Error> for NativeCopError {
    fn from(error: tikv_client::Error) -> Self {
        Self::Client(error)
    }
}

impl From<CopReadTaskError> for NativeCopError {
    fn from(error: CopReadTaskError) -> Self {
        Self::Task(error)
    }
}

impl From<String> for NativeCopError {
    fn from(error: String) -> Self {
        Self::Other(error)
    }
}

impl From<NativeCopError> for QueryResponseError {
    fn from(error: NativeCopError) -> Self {
        match error {
            NativeCopError::Task(CopReadTaskError::MaxKeysRead) => Self::Sql {
                code: tidb_errno::errcode::ErrMaxKeysReadExceeded,
                message: "tidb_max_keys_read limit exceeded".to_owned(),
            },
            NativeCopError::Task(error) => Self::Source(format!("cop read task failed: {error}")),
            // Go `ToTiDBErr`: an exhausted backoff is its class's storage error.
            NativeCopError::Client(error) => {
                match tidb_store_driver_txn::driver::tikv_transaction::client_backoff_kind(&error) {
                    Some(kind) => {
                        let source =
                            tidb_txnkv::StorageDriverError::from_backoff(kind, &error.to_string());
                        match tidb_txnkv::to_tidb_driver_error(&source) {
                            tidb_txnkv::ConvertedDriverError::Terror(converted) => Self::Sql {
                                code: u16::try_from(converted.code().value())
                                    .expect("registered storage error fits the MySQL protocol"),
                                // The client reads Go's `terror.ToSQLError`
                                // message (`GetMsg()`), not `Error()`'s
                                // `[class:code]` form.
                                message: converted.message().to_owned(),
                            },
                            other => Self::Source(other.to_string()),
                        }
                    }
                    None => Self::Source(error.to_string()),
                }
            }
            // Go `kvStore.CheckVisibility` -> `ToTiDBErr`: `ErrTxnAbortedByGC`
            // (9006) or `ErrPDServerTimeout`.
            NativeCopError::Visibility(error) => {
                match tidb_txnkv::to_tidb_driver_error(&error.into_storage_driver_error()) {
                    tidb_txnkv::ConvertedDriverError::Terror(converted) => Self::Sql {
                        code: u16::try_from(converted.code().value())
                            .expect("registered storage error fits the MySQL protocol"),
                        message: converted.message().to_owned(),
                    },
                    other => Self::Source(other.to_string()),
                }
            }
            NativeCopError::Other(message) => Self::Source(message),
        }
    }
}

/// Go `copNextMaxBackoff`: a task's backoff budget, shared by its sends,
/// its region-miss rebuilds and its lock resolution.
const COP_NEXT_MAX_BACKOFF_MS: u64 = 20_000;

/// Go `maxExceedsBoundRetries`: rebuilds without buckets after a
/// bucket-less task still met "Request range exceeds bound".
const MAX_EXCEEDS_BOUND_RETRIES: u32 = 3;

/// Go `buildCopTasks`' locations: the regions covering `ranges`, as task
/// topology.
fn locate<S: NativeCopStore + ?Sized>(
    store: &S,
    ranges: &[(Vec<u8>, Vec<u8>)],
    kv_context: &tidb_txnkv::StatementKvContext,
) -> Result<Vec<RegionTaskTopology>, tikv_client::Error> {
    Ok(store
        .locate(ranges, kv_context)?
        .iter()
        .map(region_topology)
        .collect())
}

/// Go `buildCopTasks`' view of one located region: its epoch, bounds and the
/// leader peer the first send goes to.
fn region_topology(region: &tikv_client::RegionWithLeader) -> RegionTaskTopology {
    let epoch = region.region.region_epoch.clone().unwrap_or_default();
    let peer = region
        .leader
        .as_ref()
        .or_else(|| region.region.peers.first())
        .map(|peer| RegionTaskPeer {
            id: peer.id,
            store_id: peer.store_id,
            role: peer.role,
            is_witness: peer.is_witness,
        });
    // Go `SplitKeyRangesByBuckets` over the cached region's buckets
    // (`loc.Buckets`, whose keys include the region's own bounds).
    let (bucket_keys, buckets_version) = region.buckets.as_ref().map_or_else(
        || (Vec::new(), 0),
        |buckets| {
            let keys = buckets
                .keys
                .iter()
                .filter(|key| {
                    key.as_slice() > region.region.start_key.as_slice()
                        && (region.region.end_key.is_empty()
                            || key.as_slice() < region.region.end_key.as_slice())
                })
                .cloned()
                .collect();
            (keys, buckets.version)
        },
    );
    RegionTaskTopology {
        region_id: region.region.id,
        region_epoch: Some(RegionTaskEpoch {
            conf_ver: epoch.conf_ver,
            version: epoch.version,
        }),
        store_batch_eligible: region.leader.is_some(),
        peer,
        start_key: region.region.start_key.clone(),
        end_key: region.region.end_key.clone(),
        bucket_keys,
        buckets_version,
    }
}

fn observation_time() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

/// Go `copr.CopClient` over the node's client-rust store.
pub struct NativeCopTransport<S: ?Sized> {
    store: Arc<S>,
    config: CopRuntimeConfig,
    event_callback: Option<tidb_trxevents::EventCallback>,
}

impl<S: NativeCopStore + ?Sized> NativeCopTransport<S> {
    /// A transport over `store` with `config`'s timeout, cache and paging
    /// policy.
    #[must_use]
    pub fn new(store: Arc<S>, config: CopRuntimeConfig) -> Self {
        Self {
            store,
            config,
            event_callback: None,
        }
    }
}

impl<S: NativeCopStore + ?Sized> QueryTransport for NativeCopTransport<S> {
    type Response = super::CopIterator<NativeCopResponse<S>>;

    fn set_event_callback(&mut self, callback: Option<tidb_trxevents::EventCallback>) {
        self.event_callback = callback;
    }

    fn send(
        &mut self,
        request: &TransportRequest,
        dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        let cancellation = Arc::clone(
            request
                .request_cancellation()
                .map_err(|e| format!("{e:?}"))?,
        );
        if cancellation.is_cancelled() {
            return Err("query cancelled by caller".to_owned());
        }
        let metadata = request.metadata_for_send().map_err(|e| format!("{e:?}"))?;
        let supported = match dispatch.operation {
            QueryOperation::Select | QueryOperation::SelectWithRuntimeStats => {
                metadata.request_type == crate::RequestType::Dag
            }
            QueryOperation::Analyze => metadata.request_type == crate::RequestType::Analyze,
            QueryOperation::Checksum => metadata.request_type == crate::RequestType::Checksum,
        };
        if !supported {
            return Err(format!(
                "unsupported coprocessor operation {:?}",
                dispatch.operation
            ));
        }
        // Each task source answers its own tasks in order; the worker pool
        // above it keeps or relaxes the order across tasks.
        let mut coordinator_metadata = metadata.clone();
        coordinator_metadata.keep_order = true;
        CopPagingState::validate_read_request(&coordinator_metadata)
            .map_err(|e| format!("{e:?}"))?;
        let ranges = request_ranges(metadata)?;
        // Go `CopClient.Send`: `ctx = context.WithValue(ctx,
        // tikv.TxnStartKey(), req.StartTs)` for the whole iterator, its task
        // building, sends, backoffers and lock resolution.
        let kv_context = request.kv_context().with_txn_start_ts(metadata.start_ts);
        let build_cop_tasks = tidb_tracing::start_thread_region("copr.buildCopTasks");
        let build_started = std::time::Instant::now();
        let topology =
            locate(self.store.as_ref(), &ranges, &kv_context).map_err(|error| error.to_string())?;
        let cache = match self.config.shared_cache.clone() {
            Some(cache) => Some(cache),
            None => CoprCache::from_optional_config(self.config.cache.as_ref())
                .map_err(|error| error.to_string())?,
        };
        let runtime = CopReadTaskRuntime::prepare_for_transport(
            &coordinator_metadata,
            &topology,
            cache,
            self.config.read_engine_generation,
            self.config.seed_read_bytes,
        )
        .map_err(|error| error.to_string())?;
        build_cop_tasks.end();
        let build_task_elapsed = build_started.elapsed();
        let keep_order = metadata.keep_order;
        let mut metadata = runtime.request_metadata().clone();
        metadata.keep_order = keep_order;
        let mut logical_order = Vec::new();
        let mut active_attempts = BTreeMap::new();
        for (attempt_id, task) in runtime.initial_tasks() {
            if !active_attempts.contains_key(&task.task_id) {
                logical_order.push(task.task_id);
            }
            active_attempts.insert(task.task_id, attempt_id);
        }
        if logical_order.is_empty() {
            return Err("region discovery produced no logical task".to_owned());
        }
        let timeout = if metadata.tikv_client_read_timeout_ms > 0 {
            Duration::from_millis(metadata.tikv_client_read_timeout_ms)
        } else {
            self.config.default_timeout
        };
        let bound_at = request.bound_at().map_err(|e| format!("{e:?}"))?;
        let query_deadline = (metadata.max_execution_time_ms > 0)
            .then(|| bound_at + Duration::from_millis(metadata.max_execution_time_ms));
        let call = UnaryCallContext::with_optional_deadline(
            query_deadline,
            cancellation.unary_cancellation(),
        );
        // Go's copr workers inherit `distsql.Select`'s ctx and its span.
        let trace_parent = tidb_tracing::current_span();
        let source = NativeCopResponse {
            store: Arc::clone(&self.store),
            cluster_id: self.store.cluster_id(),
            trace: self.config.trace.clone(),
            trace_parent,
            event_callback: self.event_callback.clone(),
            cancellation,
            call,
            kv_exec_counter: request.kv_exec_counter(),
            kv_context,
            timeout,
            metadata: Arc::new(metadata),
            runtime,
            logical_order,
            active_attempts,
            logical_index: 0,
            closed: false,
            backoffers: BTreeMap::new(),
            exceeds_bound: BTreeMap::new(),
            snapshot_locks: Arc::default(),
            build_task_elapsed,
        };
        // Go's `copIteratorWorker`s run on the query worker runtime.
        tidb_txnkv::rpc::query_worker_runtime()?;
        Ok(Some(super::CopIterator::new(
            source,
            Some(super::cop_iterator::start_concurrent::<NativeCopResponse<S>>),
            request.cop_lite_worker(),
        )))
    }
}

/// The request's key ranges, one partition (Go's coprocessor does not
/// accept partitioned ranges on this path either).
fn request_ranges(metadata: &crate::KvRequestMetadata) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
    let ranges = metadata
        .key_ranges
        .as_ref()
        .ok_or_else(|| "missing_ranges".to_owned())?;
    if !ranges.is_non_partitioned() || ranges.partitions().len() != 1 {
        return Err("partitioned_ranges".to_owned());
    }
    let ranges = ranges
        .partitions()
        .first()
        .filter(|ranges| !ranges.is_empty())
        .ok_or_else(|| "missing_ranges".to_owned())?;
    Ok(ranges
        .iter()
        .map(|range| {
            (
                Vec::from(range.start_key.clone()),
                Vec::from(range.end_key.clone()),
            )
        })
        .collect())
}

/// One coprocessor read's response owner (Go `copIterator`).
pub struct NativeCopResponse<S: ?Sized> {
    store: Arc<S>,
    cluster_id: u64,
    trace: Option<tidb_txnkv::TraceInfo>,
    /// A traced statement's `distsql.Select` span: Go's copr workers run
    /// with its ctx, and each task's backoffer takes it as its `bo.ctx`, so
    /// the client spans of the request's sends report under it.
    trace_parent: Option<tidb_tracing::Span>,
    event_callback: Option<tidb_trxevents::EventCallback>,
    cancellation: Arc<CancelHandle>,
    call: UnaryCallContext,
    kv_exec_counter: tidb_topsql_stmtstats::KvExecCounterHandle,
    /// Go `bo.GetCtx()`'s statement values: the `RUDetails` and
    /// `ExecDetails` charged for every send and lock resolution of the read.
    kv_context: tidb_txnkv::StatementKvContext,
    timeout: Duration,
    metadata: Arc<crate::KvRequestMetadata>,
    runtime: CopReadTaskRuntime,
    logical_order: Vec<u64>,
    active_attempts: BTreeMap<u64, u64>,
    logical_index: usize,
    closed: bool,
    /// Go's per-task `bo`, by logical task.
    backoffers: BTreeMap<u64, CopBackoffer>,
    /// Go `copTask.skipBuckets` and `exceedsBoundRetry`, by logical task: a
    /// task present here was rebuilt without bucket splitting that many
    /// times.
    exceeds_bound: BTreeMap<u64, u32>,
    /// Go `ClientHelper`'s resolved and committed locks for this snapshot,
    /// shared by its tasks.
    snapshot_locks: Arc<std::sync::Mutex<SnapshotLockSet>>,
    /// Go `copIterator.buildTaskElapsed`.
    build_task_elapsed: Duration,
}

impl<S: NativeCopStore + ?Sized> NativeCopResponse<S> {
    fn fail(&mut self, message: String) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.closed = true;
        Err(QueryResponseError::Source(message))
    }

    /// Go `copIterator.Next`: every response is handed back only after
    /// `it.store.CheckVisibility(it.req.StartTs)` proves GC has not passed the
    /// read's timestamp; checking after the read is what makes it sound.
    fn check_visibility(&mut self) -> Result<(), QueryResponseError> {
        match self.store.check_visibility(self.metadata.start_ts) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.closed = true;
                Err(
                    NativeCopError::Visibility(tidb_txnkv::VisibilityError::from_client_error(
                        error,
                    ))
                    .into(),
                )
            }
        }
    }

    fn check_active(&self) -> Result<(), NativeCopError> {
        if self.closed || self.cancellation.is_cancelled() {
            return Err("query cancelled by caller".to_owned().into());
        }
        if self.call.timeout().is_zero() {
            return Err("query deadline exceeded".to_owned().into());
        }
        Ok(())
    }

    fn backoffer(&mut self, logical_task_id: u64) -> CopBackoffer {
        let counter = self.kv_exec_counter.clone();
        let trace_parent = self.trace_parent.clone();
        let statement_ctx = self.kv_context.trace_context();
        Arc::clone(self.backoffers.entry(logical_task_id).or_insert_with(|| {
            // Go `distsql.WithSQLKvExecCounterInterceptor` on the request's
            // ctx: TopSQL's counter sees every RPC the task's sends issue,
            // the sender's retries and lock resolution included.
            let mut interceptors = tikv_client::RpcInterceptorChain::new();
            interceptors.link(tikv_client::new_rpc_interceptor(
                "kv-exec-counter",
                move |target, _request, next| {
                    counter.mark(target);
                    Box::pin(next())
                        as std::pin::Pin<
                            Box<
                                dyn std::future::Future<Output = tikv_client::RpcDispatchResult>
                                    + Send
                                    + '_,
                            >,
                        >
                },
            ));
            // Go `backoff.NewBackofferWithVars(ctx, ...)` on the worker's
            // ctx: the backoffer's `bo.ctx` is `distsql.Select`'s span and
            // the statement ctx, whose `ExecDetails` its sleeps charge and
            // whose `TxnStartKey` its `retry later` lines report.
            let backoffer =
                tidb_txnkv::rpc::trace::build_in_ctx(trace_parent, statement_ctx, || {
                    tikv_client::tikv::Backoffer::new(
                        tikv_client::async_util::Cancellation::default(),
                        COP_NEXT_MAX_BACKOFF_MS,
                    )
                    .with_rpc_interceptor(interceptors)
                });
            Arc::new(tokio::sync::Mutex::new(backoffer))
        }))
    }

    /// Go `copIteratorWorker.handleTaskOnce` for the task's active attempt.
    fn dispatch(&mut self, logical_task_id: u64, attempt_id: u64) -> Result<(), NativeCopError> {
        // The worker runs the task with `distsql.Select`'s ctx, whichever
        // thread it is on: the store calls below report their client spans
        // under that span (or the backoffer's ctx) and bring back execution
        // details.
        let _ctx = self
            .trace_parent
            .clone()
            .map(|span| tidb_tracing::with_thread_span(Some(span)));
        self.check_active()?;
        let prepared = self.runtime.activate_attempt(attempt_id)?;
        let task = prepared.task();
        let epoch = task
            .region_epoch
            .ok_or_else(|| "prepared task has no region epoch".to_owned())?;
        let region = (task.region_id, epoch.conf_ver, epoch.version);
        let predicted_read_bytes = self
            .runtime
            .task_predicted_read_bytes(logical_task_id)
            .ok_or_else(|| "active task has no read-byte prediction".to_owned())?;
        let mut request = build_tikv_unary_request(
            &prepared,
            &self.metadata,
            predicted_read_bytes,
            self.trace.as_ref(),
            self.cluster_id,
        );
        // Go `ClientHelper.SendReqCtx` stamps both lock sets on every send.
        self.snapshot_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stamp(&mut request.context);
        let mut wire = tidb_proto::CoprocessorRequest::decode(request.encoded_request.as_slice())
            .map_err(|error| error.to_string())?;
        wire.context = Some(request.context.clone());
        let backoffer = self.backoffer(logical_task_id);
        // Go `handleTaskOnce`: `worker.kvclient.Stats` collects the task's
        // physical RPCs and `costTime` is the send's wall time.
        let req_stats = Arc::new(tikv_client::RegionRequestRuntimeStats::new());
        let send_started = std::time::Instant::now();
        let sent = self.store.send(
            &backoffer,
            &wire.encode_to_vec(),
            region,
            self.timeout.min(self.call.timeout()),
            &self.kv_context,
            Some(Arc::clone(&req_stats)),
        )?;
        let cost_time = send_started.elapsed();
        let (encoded, address) = match sent {
            NativeCopSend::Response(encoded, address) => (encoded, address.unwrap_or_default()),
            NativeCopSend::RegionError(error) => {
                // No response reached the store: every batched child remains
                // (Go `handleBatchRemainsOnErr` with an empty response).
                let remains = self.runtime.accept_batch_responses(
                    attempt_id,
                    &tidb_proto::CoprocessorResponse::default(),
                    "",
                )?;
                self.rebuild_after_region_error(logical_task_id, attempt_id, &backoffer, error)?;
                return self.schedule_batched_remains(
                    logical_task_id,
                    &backoffer,
                    &request.context,
                    remains,
                );
            }
        };
        let response =
            decode_tikv_unary_response(encoded.as_slice()).map_err(|error| error.to_string())?;
        // Go `handleBatchCopResponse` / `handleBatchRemainsOnErr`: the batched
        // children are served, or remain, whatever the parent's own outcome.
        let remains =
            self.runtime
                .accept_batch_responses(attempt_id, response.response_ref(), &address)?;
        self.handle_parent_response(
            logical_task_id,
            attempt_id,
            &backoffer,
            &request.context,
            region,
            address,
            response,
            (cost_time, req_stats),
        )?;
        self.schedule_batched_remains(logical_task_id, &backoffer, &request.context, remains)
    }

    /// Go `handleCopResponse` for the task's own response.
    #[allow(clippy::too_many_arguments)]
    fn handle_parent_response(
        &mut self,
        logical_task_id: u64,
        attempt_id: u64,
        backoffer: &CopBackoffer,
        context: &tidb_proto::KvrpcContext,
        region: (u64, u64, u64),
        address: String,
        response: CopReadTaskResponse,
        (cost_time, req_stats): (std::time::Duration, Arc<tikv_client::RegionRequestRuntimeStats>),
    ) -> Result<(), NativeCopError> {
        if let Some(region_error) = response.region_error_ref() {
            let error = format!("{region_error:?}");
            return self.rebuild_after_region_error(logical_task_id, attempt_id, backoffer, error);
        }
        if let Some(lock) = response.locked_ref().cloned() {
            return self.retry_after_lock(logical_task_id, attempt_id, backoffer, context, &lock);
        }
        // Go `handleCopResponse`: stale bucket metadata can send ranges past
        // the region's bounds; TiKV answers this other error, and the task
        // is rebuilt without bucket splitting.
        if let Some(message) = response
            .other_error_ref()
            .filter(|message| message.contains("Request range exceeds bound"))
            .map(str::to_owned)
        {
            return self.rebuild_after_exceeds_bound(
                logical_task_id,
                attempt_id,
                backoffer,
                region,
                message,
            );
        }
        let process_time_nanos = response.process_time_nanos();
        let mut runtime_sample = response.runtime_sample(&address);
        runtime_sample.resp_time = cost_time;
        runtime_sample.request_rpc_stats_present = req_stats.rpc_type_count() > 0;
        runtime_sample.req_stats = Some(crate::RequestStatsSample(req_stats));
        let accepted = self.runtime.accept_response_with_runtime(
            attempt_id,
            response,
            process_time_nanos,
            Some(runtime_sample),
            observation_time(),
        )?;
        if let Some(update) = accepted.buckets_update {
            self.store.update_buckets_if_needed(
                (update.region_id, update.conf_ver, update.version),
                update.request_version,
                update.latest_version,
            );
        }
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

    /// Go `handleCopResponse`'s region-error arm: back off `BoRegionMiss`,
    /// locate the task's remaining ranges again and rebuild its tasks.
    fn rebuild_after_region_error(
        &mut self,
        logical_task_id: u64,
        attempt_id: u64,
        backoffer: &CopBackoffer,
        error: String,
    ) -> Result<(), NativeCopError> {
        let failed = self.runtime.consume_region_error(attempt_id)?;
        self.store.backoff_region_miss(backoffer, error)?;
        self.check_active()?;
        let ranges: Vec<_> = self
            .runtime
            .retry_ranges_from(&failed)?
            .into_iter()
            .map(|range| (Vec::from(range.start_key), Vec::from(range.end_key)))
            .collect();
        let topology = locate(self.store.as_ref(), &ranges, &self.kv_context)?;
        let replacement = self.runtime.rebuild_region_attempts(failed, &topology)?;
        if replacement.logical_task_ids.len() != replacement.active_attempt_ids.len()
            || replacement.logical_task_ids.first() != Some(&logical_task_id)
        {
            return Err("region rebuild produced an invalid active tail"
                .to_owned()
                .into());
        }
        // The rebuilt tasks share the failed task's backoffer (Go keeps one
        // `bo` across `handleTaskOnce`'s remaining tasks).
        let backoffer = self.backoffers.remove(&logical_task_id);
        self.active_attempts.remove(&logical_task_id);
        self.logical_order.splice(
            self.logical_index..=self.logical_index,
            replacement.logical_task_ids.iter().copied(),
        );
        for (logical_task_id, attempt_id) in replacement
            .logical_task_ids
            .into_iter()
            .zip(replacement.active_attempt_ids)
        {
            if let Some(backoffer) = &backoffer {
                self.backoffers
                    .insert(logical_task_id, Arc::clone(backoffer));
            }
            self.active_attempts.insert(logical_task_id, attempt_id);
        }
        Ok(())
    }

    /// Go `handleCopResponse`'s "Request range exceeds bound" arm: invalidate
    /// the region, back off `BoRegionMiss` and rebuild the task's ranges
    /// without bucket splitting -- at most [`MAX_EXCEEDS_BOUND_RETRIES`] times
    /// once a bucket-less task still meets it.
    fn rebuild_after_exceeds_bound(
        &mut self,
        logical_task_id: u64,
        attempt_id: u64,
        backoffer: &CopBackoffer,
        region: (u64, u64, u64),
        message: String,
    ) -> Result<(), NativeCopError> {
        let retry = match self.exceeds_bound.get(&logical_task_id) {
            Some(&retry) if retry >= MAX_EXCEEDS_BOUND_RETRIES => {
                return Err(format!(
                    "request range exceeds bound persists after bucket-less retry and exceeded \
                     retry budget, region_id:{}, region_ver:{}, retry:{retry}/{}, error:{message}",
                    region.0, region.2, MAX_EXCEEDS_BOUND_RETRIES
                )
                .into());
            }
            Some(&retry) => retry + 1,
            None => 1,
        };
        self.store.invalidate_region(region);
        let failed = self.runtime.consume_region_error(attempt_id)?;
        self.store.backoff_region_miss(
            backoffer,
            format!(
                "Request range exceeds bound: region_id:{}, region_ver:{}, error:{message}",
                region.0, region.2
            ),
        )?;
        self.check_active()?;
        let ranges: Vec<_> = self
            .runtime
            .retry_ranges_from(&failed)?
            .into_iter()
            .map(|range| (Vec::from(range.start_key), Vec::from(range.end_key)))
            .collect();
        // Go `buildCopTasks` with `skipBuckets`: split by region only.
        let topology: Vec<_> = locate(self.store.as_ref(), &ranges, &self.kv_context)?
            .into_iter()
            .map(|mut region| {
                region.bucket_keys.clear();
                region
            })
            .collect();
        let replacement = self.runtime.rebuild_region_attempts(failed, &topology)?;
        if replacement.logical_task_ids.len() != replacement.active_attempt_ids.len()
            || replacement.logical_task_ids.first() != Some(&logical_task_id)
        {
            return Err("region rebuild produced an invalid active tail"
                .to_owned()
                .into());
        }
        let backoffer = self.backoffers.remove(&logical_task_id);
        self.active_attempts.remove(&logical_task_id);
        self.logical_order.splice(
            self.logical_index..=self.logical_index,
            replacement.logical_task_ids.iter().copied(),
        );
        for (logical_task_id, attempt_id) in replacement
            .logical_task_ids
            .into_iter()
            .zip(replacement.active_attempt_ids)
        {
            if let Some(backoffer) = &backoffer {
                self.backoffers
                    .insert(logical_task_id, Arc::clone(backoffer));
            }
            self.exceeds_bound.insert(logical_task_id, retry);
            self.active_attempts.insert(logical_task_id, attempt_id);
        }
        Ok(())
    }

    /// Go `handleLockErr`: resolve the lock, remember what was resolved for
    /// this snapshot's later sends, back off while its owner is alive, and
    /// send the same task again.
    fn retry_after_lock(
        &mut self,
        logical_task_id: u64,
        attempt_id: u64,
        backoffer: &CopBackoffer,
        context: &tidb_proto::KvrpcContext,
        lock: &tidb_proto::KvrpcLockInfo,
    ) -> Result<(), NativeCopError> {
        self.handle_lock_err(backoffer, context, lock)?;
        let failed = self.runtime.consume_failed_attempt(attempt_id)?;
        let replacement = self.runtime.retry_transport_attempt(failed)?;
        match (
            replacement.logical_task_ids.as_slice(),
            replacement.active_attempt_ids.as_slice(),
        ) {
            ([logical], [attempt]) if *logical == logical_task_id => {
                self.active_attempts.insert(*logical, *attempt);
                Ok(())
            }
            _ => Err("lock retry did not produce one attempt".to_owned().into()),
        }
    }
}

impl<S: NativeCopStore + ?Sized> NativeCopResponse<S> {
    /// Go `handleLockErr`'s resolution: resolve the lock, remember what was
    /// resolved for this snapshot's later sends, and back off while its
    /// owner is alive.
    fn handle_lock_err(
        &mut self,
        backoffer: &CopBackoffer,
        context: &tidb_proto::KvrpcContext,
        lock: &tidb_proto::KvrpcLockInfo,
    ) -> Result<(), NativeCopError> {
        if let Some(callback) = &self.event_callback {
            callback(tidb_trxevents::wrap_cop_meet_lock(Some(Arc::new(
                tidb_trxevents::CopMeetLock {
                    lock_info: Some(Arc::new(lock.clone())),
                },
            ))));
        }
        let recovered = self.store.resolve_lock(
            backoffer,
            context,
            lock,
            self.metadata.start_ts,
            &self.kv_context,
        )?;
        self.snapshot_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .absorb(&recovered);
        if !recovered.ttl.is_zero() {
            let max_sleep_ms = u64::try_from(recovered.ttl.as_millis()).unwrap_or(u64::MAX);
            self.store
                .backoff_lock_fast(backoffer, max_sleep_ms, format!("{lock:?}"))?;
        }
        self.check_active()?;
        Ok(())
    }

    /// Go `handleBatchCopResponse`'s remains: a child whose region moved is
    /// located again, a locked child waits for its lock, and each runs as a
    /// task of its own after its parent, on the parent's `bo`.
    fn schedule_batched_remains(
        &mut self,
        parent_logical_task_id: u64,
        backoffer: &CopBackoffer,
        context: &tidb_proto::KvrpcContext,
        remains: Vec<BatchedRemain>,
    ) -> Result<(), NativeCopError> {
        if remains.is_empty() {
            return Ok(());
        }
        let mut tasks = Vec::new();
        for remain in remains {
            match remain.cause {
                BatchedRemainCause::RegionError(error) => {
                    self.store.backoff_region_miss(backoffer, error)?;
                    let ranges: Vec<_> = remain
                        .task
                        .ranges
                        .iter()
                        .map(|range| {
                            (
                                Vec::from(range.start_key.clone()),
                                Vec::from(range.end_key.clone()),
                            )
                        })
                        .collect();
                    let topology = locate(self.store.as_ref(), &ranges, &self.kv_context)?;
                    tasks.extend(self.runtime.child_retry_tasks(&remain.task, &topology)?);
                }
                BatchedRemainCause::Locked(lock) => {
                    self.handle_lock_err(backoffer, context, &lock)?;
                    tasks.push(remain.task);
                }
                BatchedRemainCause::Unanswered => tasks.push(remain.task),
            }
        }
        self.check_active()?;
        let added = self
            .runtime
            .add_tasks_after(parent_logical_task_id, tasks)?;
        let position = self
            .logical_order
            .iter()
            .position(|id| *id == parent_logical_task_id)
            .map_or(self.logical_order.len(), |position| position + 1);
        self.logical_order
            .splice(position..position, added.logical_task_ids.iter().copied());
        for (logical_task_id, attempt_id) in added
            .logical_task_ids
            .into_iter()
            .zip(added.active_attempt_ids)
        {
            self.backoffers
                .insert(logical_task_id, Arc::clone(backoffer));
            self.active_attempts.insert(logical_task_id, attempt_id);
        }
        Ok(())
    }
}

impl<S: NativeCopStore + ?Sized> tidb_txnkv::Response for NativeCopResponse<S> {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        loop {
            if self.closed {
                return Ok(None);
            }
            let Some(&logical_task_id) = self.logical_order.get(self.logical_index) else {
                self.closed = true;
                return Ok(None);
            };
            match self.runtime.next_response(logical_task_id) {
                Some(ResponseChannelEvent::Result(data)) => {
                    self.check_visibility()?;
                    return Ok(Some(QueryResultSubset {
                        data,
                        runtime: None,
                    }));
                }
                Some(ResponseChannelEvent::ResultWithRuntime {
                    result,
                    runtime_stats,
                }) => {
                    self.check_visibility()?;
                    return Ok(Some(QueryResultSubset {
                        data: result,
                        runtime: Some(runtime_stats),
                    }));
                }
                Some(ResponseChannelEvent::Closed) => {
                    self.active_attempts.remove(&logical_task_id);
                    self.backoffers.remove(&logical_task_id);
                    self.logical_index += 1;
                    continue;
                }
                Some(ResponseChannelEvent::Error(message)) => return self.fail(message),
                Some(ResponseChannelEvent::Warning(_)) => {
                    return self.fail("unexpected warning event".to_owned());
                }
                None => {}
            }
            let Some(&attempt_id) = self.active_attempts.get(&logical_task_id) else {
                return self.fail("open logical task has no active attempt".to_owned());
            };
            if let Err(error) = self.dispatch(logical_task_id, attempt_id) {
                self.closed = true;
                if self.cancellation.is_cancelled() {
                    return Err(QueryResponseError::Cancelled);
                }
                return Err(error.into());
            }
        }
    }

    fn close(&mut self) {
        self.closed = true;
    }

    /// Go `copClient.Send`'s concurrency: the request's, capped by the task
    /// count and by the tasks that are not small, with the extra workers the
    /// small tasks get (`smallTaskConcurrency`).
    fn cop_info(&self) -> Option<tidb_txnkv::CopInfo> {
        let tasks = self.logical_order.len();
        let use_row_hints = !self.metadata.request_source.internal;
        let small = if use_row_hints {
            self.runtime.small_task_count()
        } else {
            0
        };
        let mut extra = crate::RegionTaskEnvelope::small_concurrency(
            small,
            tidb_txnkv::rpc::go_max_procs(),
        );
        let mut concurrency = usize::try_from(self.metadata.concurrency)
            .unwrap_or(1)
            .min(tasks);
        if tasks - small.min(tasks) < concurrency {
            concurrency = tasks - small.min(tasks);
        }
        concurrency = concurrency.max(1);
        if self.metadata.keep_order {
            extra = extra.min(20);
        }
        Some(tidb_txnkv::CopInfo {
            concurrency,
            extra_concurrency: extra,
            build_task_elapsed: self.build_task_elapsed,
        })
    }
}


impl<S: NativeCopStore + ?Sized> super::cop_iterator::CopTaskSource for NativeCopResponse<S> {
    fn try_next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        tidb_txnkv::Response::next(self)
    }

    fn task_count(&self) -> usize {
        self.logical_order.len()
    }

    fn has_remaining_work(&self) -> bool {
        !self.closed && self.logical_index < self.logical_order.len()
    }

    fn call(&self) -> UnaryCallContext {
        self.call.clone()
    }

    fn use_task_worker(&mut self) {}

    fn set_waker(&mut self, _waker: std::task::Waker) {}
}

impl<S: NativeCopStore + ?Sized> super::cop_iterator::CopWorkerSource for NativeCopResponse<S> {
    fn keep_order(&self) -> bool {
        self.metadata.keep_order
    }

    fn concurrency(&self) -> usize {
        usize::try_from(self.metadata.concurrency)
            .unwrap_or(1)
            .max(1)
    }

    fn task(&self) -> &crate::RegionTaskEnvelope {
        self.runtime.first_task()
    }

    fn use_row_hints(&self) -> bool {
        !self.metadata.request_source.internal
    }

    fn start_worker(&mut self) {}

    /// Go `copIterator.open`: one source per task for the worker pool, unless
    /// one worker would take them all in order anyway.
    fn into_tasks(mut self) -> Vec<Self> {
        if self.metadata.concurrency <= 1
            && (self.metadata.request_source.internal || !self.runtime.has_small_tasks())
        {
            return vec![self];
        }
        self.runtime
            .take_tasks()
            .into_iter()
            .map(|(logical_task_id, runtime)| Self {
                store: Arc::clone(&self.store),
                cluster_id: self.cluster_id,
                trace: self.trace.clone(),
                trace_parent: self.trace_parent.clone(),
                event_callback: self.event_callback.clone(),
                cancellation: Arc::clone(&self.cancellation),
                call: self.call.clone(),
                kv_exec_counter: self.kv_exec_counter.clone(),
                kv_context: self.kv_context.clone(),
                timeout: self.timeout,
                metadata: Arc::clone(&self.metadata),
                runtime,
                logical_order: vec![logical_task_id],
                active_attempts: self
                    .active_attempts
                    .remove(&logical_task_id)
                    .map(|attempt_id| (logical_task_id, attempt_id))
                    .into_iter()
                    .collect(),
                logical_index: 0,
                closed: false,
                backoffers: BTreeMap::new(),
                exceeds_bound: self
                    .exceeds_bound
                    .remove(&logical_task_id)
                    .map(|retry| (logical_task_id, retry))
                    .into_iter()
                    .collect(),
                snapshot_locks: Arc::clone(&self.snapshot_locks),
                build_task_elapsed: self.build_task_elapsed,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::NativeCopError;
    use tidb_txnkv::QueryResponseError;

    /// Go `ToTiDBErr`: a read whose region-miss budget ran out fails with
    /// `ErrRegionUnavailable` (9005), and the task state machine's
    /// `ErrMaxKeysReadExceeded` keeps its code.
    #[test]
    fn exhausted_backoffs_and_task_errors_keep_gos_codes() {
        let region_unavailable = QueryResponseError::from(NativeCopError::Client(
            tikv_client::Error::Static(tikv_client::error::StaticError::RegionUnavailable),
        ));
        assert!(
            matches!(
                region_unavailable,
                QueryResponseError::Sql { code: 9005, .. }
            ),
            "{region_unavailable:?}"
        );
        let max_keys_read =
            QueryResponseError::from(NativeCopError::Task(super::CopReadTaskError::MaxKeysRead));
        assert!(
            matches!(max_keys_read, QueryResponseError::Sql { code: 8274, .. }),
            "{max_keys_read:?}"
        );
    }

    /// Go `copIterator.Next`'s `CheckVisibility` goes through `ToTiDBErr`:
    /// a read GC has overtaken is `ErrTxnAbortedByGC` (9006) with client-go's
    /// text, a stale safe-point cache `ErrPDServerTimeout` (9001).
    #[test]
    fn visibility_failures_keep_gos_codes() {
        let aborted = tidb_txnkv::VisibilityError::TxnAbortedByGc {
            txn_start_ts: 449,
            txn_safe_point: 450,
        };
        let expected = aborted.to_string();
        let converted = QueryResponseError::from(NativeCopError::Visibility(aborted));
        assert!(
            matches!(
                &converted,
                QueryResponseError::Sql { code: 9006, message } if *message == expected
            ),
            "{converted:?}"
        );
        let stale = QueryResponseError::from(NativeCopError::Visibility(
            tidb_txnkv::VisibilityError::StaleGcStateCache,
        ));
        assert!(
            matches!(stale, QueryResponseError::Sql { code: 9001, .. }),
            "{stale:?}"
        );
    }
}

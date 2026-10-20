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
//! failures and region errors it owns, charging the task's `Backoffer`.
//!
//! # Narrowings, by name
//!
//! * Buckets and `StoreBatchCoprocessor` are not used yet (plan Step 6e-6).
//! * The answering store's address is not reported to the per-store
//!   runtime statistics or the TopSQL KV-exec counter.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as _;

use crate::query_runtime::{
    QueryDispatch, QueryOperation, QueryResponse, QueryResponseError, QueryResultSubset,
    QueryTransport,
};
use crate::{
    CancelHandle, CoprCache, RegionTaskEpoch, RegionTaskPeer, RegionTaskTopology,
    ResponseChannelEvent, TransportRequest,
};
use tidb_txnkv::lock::{LockRecoveryResult, SnapshotLockSet};
use tidb_txnkv::UnaryCallContext;

use super::{
    build_tikv_unary_request, decode_tikv_unary_response, CopPagingState, CopReadTaskError,
    CopReadTaskRuntime, DirectUnaryRuntimeConfig, DirectUnaryTransportError,
};

/// Why a read stopped: the task state machine's own errors keep their type,
/// so the statement sees Go's error for them (`ErrMaxKeysReadExceeded`).
#[derive(Debug)]
enum NativeCopError {
    Task(CopReadTaskError),
    Other(String),
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
            NativeCopError::Task(error) => DirectUnaryTransportError::from(error).into(),
            NativeCopError::Other(message) => Self::Source(message),
        }
    }
}

/// Go `copNextMaxBackoff`: a task's backoff budget, shared by its sends,
/// its region-miss rebuilds and its lock resolution.
const COP_NEXT_MAX_BACKOFF_MS: u64 = 20_000;

/// One task's `Backoffer` (Go `bo`), shared with the store's sender and lock
/// resolver.
pub type CopBackoffer = Arc<tokio::sync::Mutex<tikv_client::tikv::Backoffer>>;

/// What the store's sender answered for one task.
#[derive(Debug)]
pub enum NativeCopSend {
    /// The response, encoded (`coprocessor.Response`).
    Response(Vec<u8>),
    /// A region error the caller handles: the task's ranges are located again.
    RegionError(String),
}

/// The client-go store surface Go's coprocessor uses: the region cache, the
/// region sender, the lock resolver and the backoffer.
pub trait NativeCopStore: Send + Sync + 'static {
    /// Go `KVStore.GetClusterID`.
    fn cluster_id(&self) -> u64;

    /// Go `RegionCache.SplitKeyRangesByLocations`' locations: the regions
    /// covering `ranges`, in key order.
    fn locate(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<RegionTaskTopology>, String>;

    /// Go `KVStore.SendReq(bo, req, region, timeout)`, `request` encoded.
    fn send(
        &self,
        backoffer: &CopBackoffer,
        request: &[u8],
        region: (u64, u64, u64),
        timeout: Duration,
    ) -> Result<NativeCopSend, String>;

    /// Go `ResolveLocksWithOpts` for a read, from `handleLockErr`.
    fn resolve_lock(
        &self,
        backoffer: &CopBackoffer,
        context: &tidb_proto::KvrpcContext,
        lock: &tidb_proto::KvrpcLockInfo,
        caller_start_ts: u64,
    ) -> Result<LockRecoveryResult, String>;

    /// Go `bo.Backoff(BoRegionMiss, err)`.
    fn backoff_region_miss(&self, backoffer: &CopBackoffer, reason: String) -> Result<(), String>;

    /// Go `bo.BackoffWithMaxSleepTxnLockFast(msBeforeExpired, err)`.
    fn backoff_lock_fast(
        &self,
        backoffer: &CopBackoffer,
        max_sleep_ms: u64,
        reason: String,
    ) -> Result<(), String>;
}

impl<PdC: tikv_client::PdClient> NativeCopStore for tidb_txnkv::driver::tikv_store::TikvStore<PdC> {
    fn cluster_id(&self) -> u64 {
        self.block_on(self.store().pd_client().cluster_id())
    }

    fn locate(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<Vec<RegionTaskTopology>, String> {
        self.block_on(async {
            let pd = self.store().pd_client();
            let mut topology: Vec<RegionTaskTopology> = Vec::new();
            for (start, end) in ranges {
                let mut next = start.clone();
                loop {
                    let region = pd
                        .region_for_key(&next.clone().into())
                        .await
                        .map_err(|error| error.to_string())?;
                    let region_end = region.region.end_key.clone();
                    if topology
                        .last()
                        .is_none_or(|last| last.region_id != region.region.id)
                    {
                        topology.push(region_topology(&region));
                    }
                    if region_end.is_empty() || (!end.is_empty() && region_end >= *end) {
                        break;
                    }
                    next = region_end;
                }
            }
            Ok(topology)
        })
    }

    fn send(
        &self,
        backoffer: &CopBackoffer,
        request: &[u8],
        region: (u64, u64, u64),
        timeout: Duration,
    ) -> Result<NativeCopSend, String> {
        let request = tikv_client::proto::coprocessor::Request::decode(request)
            .map_err(|error| error.to_string())?;
        let region = tikv_client::RegionVerId {
            id: region.0,
            conf_ver: region.1,
            ver: region.2,
        };
        let sent =
            self.block_on(
                self.store()
                    .send_req(Arc::clone(backoffer), request, &region, timeout),
            );
        match sent {
            Ok(response) => Ok(NativeCopSend::Response(response.encode_to_vec())),
            Err(tikv_client::Error::RegionError(error)) => {
                Ok(NativeCopSend::RegionError(format!("{error:?}")))
            }
            Err(error) => Err(error.to_string()),
        }
    }

    fn resolve_lock(
        &self,
        backoffer: &CopBackoffer,
        context: &tidb_proto::KvrpcContext,
        lock: &tidb_proto::KvrpcLockInfo,
        caller_start_ts: u64,
    ) -> Result<LockRecoveryResult, String> {
        use tikv_client::txnkv::txnlock::{LockHintsInRequest, ResolveLocksOptions};
        let lock = tikv_client::proto::kvrpcpb::LockInfo::decode(lock.encode_to_vec().as_slice())
            .map_err(|error| error.to_string())?;
        let native_context =
            tikv_client::proto::kvrpcpb::Context::decode(context.encode_to_vec().as_slice())
                .map_err(|error| error.to_string())?;
        let opts = ResolveLocksOptions {
            caller_start_ts,
            locks: vec![lock],
            for_read: true,
            lock_hints_in_request: LockHintsInRequest::new(
                &context.resolved_locks,
                &context.committed_locks,
            ),
            ..Default::default()
        };
        let result = self
            .block_on(self.store().resolve_locks_with_opts(
                &native_context,
                Arc::clone(backoffer),
                opts,
            ))
            .map_err(|error| error.to_string())?;
        Ok(LockRecoveryResult {
            ttl: Duration::from_millis(u64::try_from(result.ttl).unwrap_or(0)),
            ignore_locks: result.ignore_locks,
            access_locks: result.access_locks,
        })
    }

    fn backoff_region_miss(&self, backoffer: &CopBackoffer, reason: String) -> Result<(), String> {
        self.block_on(async {
            backoffer
                .lock()
                .await
                .backoff(tikv_client::tikv::BO_REGION_MISS, reason)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn backoff_lock_fast(
        &self,
        backoffer: &CopBackoffer,
        max_sleep_ms: u64,
        reason: String,
    ) -> Result<(), String> {
        self.block_on(async {
            backoffer
                .lock()
                .await
                .backoff_with_max_sleep_txn_lock_fast(max_sleep_ms, reason)
                .await
                .map_err(|error| error.to_string())
        })
    }
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
        ..RegionTaskTopology::default()
    }
}

fn observation_time() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

/// Go `copr.CopClient` over the node's client-rust store.
pub struct NativeCopTransport<S> {
    store: Arc<S>,
    config: DirectUnaryRuntimeConfig,
    event_callback: Option<tidb_txnkv::EventCallback>,
}

impl<S: NativeCopStore> NativeCopTransport<S> {
    /// A transport over `store` with `config`'s timeout, cache and paging
    /// policy.
    #[must_use]
    pub fn new(store: Arc<S>, config: DirectUnaryRuntimeConfig) -> Self {
        Self {
            store,
            config,
            event_callback: None,
        }
    }
}

impl<S: NativeCopStore> QueryTransport for NativeCopTransport<S> {
    type Response = super::CopIterator<NativeCopResponse<S>>;

    fn set_event_callback(&mut self, callback: Option<tidb_txnkv::EventCallback>) {
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
            _ => false,
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
        let topology = self.store.locate(&ranges)?;
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
        let source = NativeCopResponse {
            store: Arc::clone(&self.store),
            cluster_id: self.store.cluster_id(),
            trace: self.config.trace.clone(),
            event_callback: self.event_callback.clone(),
            cancellation,
            call,
            timeout,
            metadata: Arc::new(metadata),
            runtime,
            logical_order,
            active_attempts,
            logical_index: 0,
            closed: false,
            backoffers: BTreeMap::new(),
            snapshot_locks: Arc::default(),
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
pub struct NativeCopResponse<S> {
    store: Arc<S>,
    cluster_id: u64,
    trace: Option<tidb_txnkv::TraceInfo>,
    event_callback: Option<tidb_txnkv::EventCallback>,
    cancellation: Arc<CancelHandle>,
    call: UnaryCallContext,
    timeout: Duration,
    metadata: Arc<crate::KvRequestMetadata>,
    runtime: CopReadTaskRuntime,
    logical_order: Vec<u64>,
    active_attempts: BTreeMap<u64, u64>,
    logical_index: usize,
    closed: bool,
    /// Go's per-task `bo`, by logical task.
    backoffers: BTreeMap<u64, CopBackoffer>,
    /// Go `ClientHelper`'s resolved and committed locks for this snapshot,
    /// shared by its tasks.
    snapshot_locks: Arc<std::sync::Mutex<SnapshotLockSet>>,
}

impl<S: NativeCopStore> NativeCopResponse<S> {
    fn fail(&mut self, message: String) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.closed = true;
        Err(QueryResponseError::Source(message))
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
        Arc::clone(self.backoffers.entry(logical_task_id).or_insert_with(|| {
            Arc::new(tokio::sync::Mutex::new(tikv_client::tikv::Backoffer::new(
                tikv_client::async_util::Cancellation::default(),
                COP_NEXT_MAX_BACKOFF_MS,
            )))
        }))
    }

    /// Go `copIteratorWorker.handleTaskOnce` for the task's active attempt.
    fn dispatch(&mut self, logical_task_id: u64, attempt_id: u64) -> Result<(), NativeCopError> {
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
        let sent = self.store.send(
            &backoffer,
            &wire.encode_to_vec(),
            region,
            self.timeout.min(self.call.timeout()),
        )?;
        let encoded = match sent {
            NativeCopSend::Response(encoded) => encoded,
            NativeCopSend::RegionError(error) => {
                return self.rebuild_after_region_error(
                    logical_task_id,
                    attempt_id,
                    &backoffer,
                    error,
                );
            }
        };
        let response =
            decode_tikv_unary_response(encoded.as_slice()).map_err(|error| error.to_string())?;
        if let Some(region_error) = response.region_error_ref() {
            let error = format!("{region_error:?}");
            return self.rebuild_after_region_error(logical_task_id, attempt_id, &backoffer, error);
        }
        if let Some(lock) = response.locked_ref().cloned() {
            return self.retry_after_lock(
                logical_task_id,
                attempt_id,
                &backoffer,
                &request.context,
                &lock,
            );
        }
        let process_time_nanos = response.process_time_nanos();
        let runtime_sample = response.runtime_sample("");
        let accepted = self.runtime.accept_response_with_runtime(
            attempt_id,
            response,
            process_time_nanos,
            Some(runtime_sample),
            observation_time(),
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
        let topology = self.store.locate(&ranges)?;
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
        if let Some(callback) = &self.event_callback {
            callback(tidb_txnkv::wrap_cop_meet_lock(Some(Arc::new(
                tidb_txnkv::CopMeetLock {
                    lock_info: Some(Arc::new(lock.clone())),
                },
            ))));
        }
        let recovered =
            self.store
                .resolve_lock(backoffer, context, lock, self.metadata.start_ts)?;
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

impl<S: NativeCopStore> QueryResponse for NativeCopResponse<S> {
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
                    return Ok(Some(QueryResultSubset {
                        data,
                        runtime: None,
                    }));
                }
                Some(ResponseChannelEvent::ResultWithRuntime {
                    result,
                    runtime_stats,
                }) => {
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
}

impl<S: NativeCopStore> super::cop_iterator::CopTaskSource for NativeCopResponse<S> {
    fn try_next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.next()
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

impl<S: NativeCopStore> super::cop_iterator::CopWorkerSource for NativeCopResponse<S> {
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
                event_callback: self.event_callback.clone(),
                cancellation: Arc::clone(&self.cancellation),
                call: self.call.clone(),
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
                snapshot_locks: Arc::clone(&self.snapshot_locks),
            })
            .collect()
    }
}

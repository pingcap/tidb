// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::prelude::*;
use log::debug;
use log::error;
use log::info;
use log::warn;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::async_util::Cancellation;
use crate::backoff::Backoff;
use crate::interceptor::RpcInterceptorChain;
use crate::kv::Variables;
use crate::kv::{AccessLocationType, ReplicaReadConfig};
use crate::locate::ReplicaSelectorState;
use crate::oracle::{OracleOption, ReadTimestampValidator};
use crate::pd::PdClient;
use crate::proto::errorpb;
use crate::proto::errorpb::EpochNotMatch;
use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::region::StoreId;
use crate::region::{RegionVerId, RegionWithLeader};
use crate::region_request::{region_error_access_message, region_error_label};
use crate::request::shard::HasNextBatch;
use crate::request::NextBatch;
use crate::request::Shardable;
use crate::request::{KvRequest, StoreRequest};
use crate::resource_control::ResourceGroupControllerHandle;
use crate::retry::{
    RetryBackoffer, RetryConfig, BO_IS_WITNESS, BO_MAX_REGION_NOT_INITIALIZED,
    BO_MAX_TS_NOT_SYNCED, BO_REGION_MISS, BO_REGION_RECOVERY_IN_PROGRESS, BO_REGION_SCHEDULING,
    BO_STALE_CMD, BO_TIFLASH_RPC, BO_TIFLASH_SERVER_BUSY, BO_TIKV_DISK_FULL, BO_TIKV_RPC,
    BO_TIKV_SERVER_BUSY,
};
use crate::stats::tikv_stats;
use crate::store::CommandType;
use crate::store::HasRegionError;
use crate::store::HasRegionErrors;
use crate::store::KvClient;
use crate::store::RegionStore;
use crate::store::{HasKeyErrors, Store};
use crate::timestamp::TimestampExt;
use crate::transaction::resolve_locks_for_read_with_context_result;
use crate::transaction::resolve_locks_with_context_result;
use crate::transaction::HasLocks;
use crate::transaction::ReadLockContext;
use crate::transaction::ResolveLocksContext;
use crate::transaction::ResolveLocksOptions;
use crate::transaction::ResolvingLocksGuard;
use crate::transaction::SnapshotRuntimeStats;
use crate::util::iter::FlatMapOkIterExt;
use crate::Error;
use crate::Result;

use super::keyspace::{EncodeKeyspace, KeyMode, Keyspace};

/// A plan for how to execute a request. A user builds up a plan with various
/// options, then exectutes it.
#[async_trait]
pub trait Plan: Sized + Clone + Sync + Send + 'static {
    /// The ultimate result of executing the plan (should be a high-level type, not a GRPC response).
    type Result: Send;

    /// Execute the plan.
    async fn execute(&self) -> Result<Self::Result>;

    /// Attach the source snapshot's lock-resolution hints to an imminent
    /// physical read. Plans that do not end in a TiKV context request retain
    /// the no-op default.
    fn set_read_lock_context(&mut self, _resolved_locks: Vec<u64>, _committed_locks: Vec<u64>) {}
}

/// The simplest plan which just dispatches a request to a specific kv server.
#[derive(Clone)]
pub struct Dispatch<Req: KvRequest> {
    pub request: Req,
    pub kv_client: Option<Arc<dyn KvClient + Send + Sync>>,
    /// Optional caller-specific physical RPC deadline. `None` retains the
    /// client-wide transport timeout.
    pub request_timeout: Option<Duration>,
    /// Source snapshot reads use an operation-specific timeout after the
    /// first retry, even when their initial send used `SetKVReadTimeout`.
    pub(crate) retry_request_timeout: Option<Duration>,
    /// Optional client-go read-timestamp validation run before every physical
    /// dispatch, including sender retries.
    pub(crate) read_timestamp_validation: Option<ReadTimestampValidation>,
    /// Address of the current TiKV target, set when the request is assigned to a store.
    pub target: String,
    /// Logical TiKV target used only when this request is physically sent to
    /// a proxy store. Empty means direct transport.
    pub forwarded_host: String,
    /// Stable source replica-read selector settings for this sharded plan.
    pub replica_read_config: ReplicaReadConfig,
    /// Per-request source selector state. This remains internal so public
    /// replica-read configuration stays stable across independent requests.
    pub(crate) replica_selector_state: ReplicaSelectorState,
    pub(crate) store_health: Option<Arc<crate::locate::StoreHealthStatus>>,
    pub(crate) record_client_side_slow_score: bool,
    /// Physical endpoint classification used by transport-side accounting.
    /// Client-go only charges completed RU-v2 RPC counts to ordinary TiKV.
    pub(crate) physical_endpoint_type: crate::store::EndpointType,
    pub(crate) resource_control_replica_number: i64,
    pub(crate) resource_control_access_location: AccessLocationType,
    pub(crate) predicted_read_bytes: u64,
    pub(crate) ru_details: Option<Arc<crate::RuDetails>>,
    pub(crate) store_token_count: Arc<AtomicI64>,
    pub(crate) store_token_store_id: StoreId,
    /// Optional source request-sender statistics shared by every shard and
    /// retry owned by this logical request.
    pub(crate) region_request_runtime_stats: Option<Arc<crate::RegionRequestRuntimeStats>>,
    pub(crate) logical_peer_id: Option<u64>,
    pub(crate) logical_store_id: Option<StoreId>,
    pub(crate) request_stale_read: bool,
    pub(crate) request_replica_read: bool,
    /// Optional transaction-level decorator for this physical RPC.
    pub interceptor: Option<RpcInterceptorChain>,
    /// Task-scoped execution-detail trace sink captured before this dispatch
    /// may move into a fan-out task.
    pub(crate) execution_details_trace_handler: Option<crate::trace::ExecutionDetailsTraceHandler>,
    pub(crate) network_traffic_details: Option<Arc<crate::traffic::NetworkTrafficDetails>>,
    /// Original request invariant retained when a stale read falls back to a
    /// normal leader read after meeting a lock.
    pub(crate) network_stale_read: bool,
    /// Optional client-go-compatible resource-group controller applied before
    /// the user interceptor and settled after a successful response.
    pub resource_control: Option<ResourceGroupControllerHandle>,
    pub response_codec: Option<super::keyspace::ApiV2Codec>,
    pub v1_response_codec: Option<super::keyspace::ApiV1Codec>,
}

#[derive(Clone)]
pub(crate) struct ReadTimestampValidation {
    pub(crate) validator: Arc<dyn ReadTimestampValidator>,
    pub(crate) read_timestamp: u64,
    pub(crate) stale_read: bool,
    pub(crate) option: OracleOption,
}

#[async_trait]
impl<Req: KvRequest> Plan for Dispatch<Req> {
    type Result = Req::Response;

    async fn execute(&self) -> Result<Self::Result> {
        if let Some(validation) = &self.read_timestamp_validation {
            validation
                .validator
                .validate_read_timestamp(
                    validation.read_timestamp,
                    validation.stale_read,
                    &validation.option,
                )
                .await
                .map_err(|error| Error::StringError(error.to_string()))?;
        }
        let store_token_limit = crate::kv::STORE_LIMIT.load(std::sync::atomic::Ordering::Relaxed);
        let store_token_addr = if self.forwarded_host.is_empty() {
            self.target.as_str()
        } else {
            self.forwarded_host.as_str()
        };
        let _store_token = (store_token_limit > 0)
            .then(|| {
                crate::store::StoreToken::acquire(
                    self.store_token_count.clone(),
                    self.store_token_store_id,
                    store_token_addr,
                    store_token_limit,
                )
            })
            .transpose()?;
        let mut request = self.request.clone();
        let resource_control = self
            .resource_control
            .clone()
            .or_else(crate::resource_control::global_controller);
        let selected_resource_control = resource_control.as_ref().and_then(|controller| {
            crate::resource_control::select(
                controller,
                &request,
                self.resource_control_replica_number,
                self.resource_control_access_location,
                self.predicted_read_bytes,
            )
        });
        if let Some(selected) = &selected_resource_control {
            let result = selected
                .controller
                .on_request_wait(&selected.resource_group_name, selected.request)
                .await?;
            if let Some(ru_details) = &self.ru_details {
                ru_details.update(&result.consumption, result.wait_duration);
            }
            request.set_resource_control_penalty(result.penalty);
            request.set_resource_control_priority_if_unset(result.priority);
        }
        let stats = tikv_stats(self.request.label());
        let client = self
            .kv_client
            .as_ref()
            .expect("Unreachable: kv_client has not been initialised in Dispatch")
            .clone();
        let execution_details_trace_handler = self
            .execution_details_trace_handler
            .clone()
            .or_else(crate::trace::current_execution_details_trace_handler);
        let next = Box::new(|| {
            Box::pin(async {
                let dispatch = client.dispatch_with_timeout_and_forwarded_host(
                    &request,
                    self.request_timeout,
                    &self.forwarded_host,
                );
                match execution_details_trace_handler.clone() {
                    Some(handler) => crate::trace::with_trace_exec_details(handler, dispatch).await,
                    None => dispatch.await,
                }
            }) as futures::future::BoxFuture<'_, crate::interceptor::RpcDispatchResult>
        });
        let started_at = Instant::now();
        let result = match &self.interceptor {
            Some(interceptor) => interceptor.dispatch(&self.target, &request, next).await,
            None => next().await,
        };
        if let Some(runtime_stats) = &self.region_request_runtime_stats {
            if let Some(command) = CommandType::from_request_label(self.request.label()) {
                runtime_stats.record_rpc(command, started_at.elapsed());
            }
            if let Err(error) = &result {
                let error = request_error_message(error);
                runtime_stats.record_error(error.clone());
                if let (Some(peer_id), Some(store_id)) =
                    (self.logical_peer_id, self.logical_store_id)
                {
                    runtime_stats.record_replica_access(
                        self.request_stale_read,
                        self.request_replica_read,
                        peer_id,
                        store_id,
                        error,
                    );
                }
            }
        }
        let network_collector = crate::traffic::NetworkCollector {
            stale_read: self.network_stale_read || self.replica_read_config.stale_read,
            access_location: self.resource_control_access_location,
            endpoint_type: self.physical_endpoint_type,
            details: self
                .network_traffic_details
                .clone()
                .or_else(crate::traffic::current_network_traffic_details),
        };
        network_collector.on_request(&request);
        if let Ok(response) = &result {
            network_collector.on_response(&request, response.as_ref());
        }
        let result = match result {
            Ok(response) => {
                if let Some(selected) = selected_resource_control {
                    let response_info =
                        crate::resource_control::ResponseInfo::from_dispatch_response(
                            response.as_ref(),
                        );
                    let settlement = selected.controller.on_response_wait(
                        &selected.resource_group_name,
                        selected.request,
                        response_info,
                    );
                    match settlement {
                        Ok(settlement) => {
                            if let Some(ru_details) = &self.ru_details {
                                ru_details
                                    .update(&settlement.consumption, settlement.wait_duration);
                            }
                        }
                        Err(error)
                            if request
                                .as_any()
                                .downcast_ref::<kvrpcpb::CommitRequest>()
                                .is_some_and(|request| request.is_txn_file) =>
                        {
                            log::warn!(
                                "txn file: resource control accounting failed after commit: {}",
                                error
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
                Ok(response)
            }
            Err(error) => Err(error),
        };
        if self.record_client_side_slow_score {
            if let Some(health_status) = &self.store_health {
                health_status.record_client_side_latency(started_at.elapsed());
            }
        }
        let result = stats.done(result);
        result.and_then(|r| {
            let mut response = *r
                .downcast()
                .expect("Downcast failed: request and response type mismatch");
            let request_info =
                crate::resource_control::RequestInfo::from_store_request(&self.request);
            if !request_info.bypass
                && self.physical_endpoint_type == crate::store::EndpointType::TiKv
            {
                let (read_rpc_count, write_rpc_count) = if request_info.is_write() {
                    (0, 1)
                } else {
                    (1, 0)
                };
                crate::config::update_tikv_ru_v2_from_exec_details_v2(
                    crate::store::exec_details_v2_mut(&mut response),
                    read_rpc_count,
                    write_rpc_count,
                    self.ru_details.as_deref(),
                );
            }
            self.request
                .decode_response(&mut response, self.response_codec.as_ref())?;
            self.request
                .decode_v1_response(&mut response, self.v1_response_codec.as_ref())?;
            Ok(response)
        })
    }

    fn set_read_lock_context(&mut self, resolved_locks: Vec<u64>, committed_locks: Vec<u64>) {
        self.request.set_resolved_locks(resolved_locks);
        self.request.set_committed_locks(committed_locks);
    }
}

impl<Req: KvRequest + StoreRequest> StoreRequest for Dispatch<Req> {
    fn apply_store(&mut self, store: &Store) {
        self.kv_client = Some(store.client.clone());
        self.target = store.target.clone();
        self.forwarded_host.clear();
        self.store_health = None;
        self.record_client_side_slow_score = false;
        self.physical_endpoint_type = crate::store::EndpointType::TiKv;
        self.logical_peer_id = None;
        self.logical_store_id = None;
        self.request_stale_read = false;
        self.request_replica_read = false;
        self.request.apply_store(store);
    }
}

pub(crate) const MULTI_REGION_CONCURRENCY: usize = 16;
const MULTI_STORES_CONCURRENCY: usize = 16;

pub(crate) fn is_grpc_error(e: &Error) -> bool {
    matches!(e, Error::GrpcAPI(_) | Error::Grpc(_))
        || matches!(e, Error::Connection { source, .. } if is_grpc_error(source))
}

fn request_error_message(error: &Error) -> String {
    match error {
        Error::Connection { source, .. } => request_error_message(source),
        _ => error.to_string(),
    }
}

fn is_grpc_deadline_exceeded(e: &Error) -> bool {
    matches!(e, Error::GrpcAPI(status) if status.code() == tonic::Code::DeadlineExceeded)
        || matches!(e, Error::Connection { source, .. } if is_grpc_deadline_exceeded(source))
}

pub(crate) async fn invalidate_connection_for_error<PdC: PdClient>(
    pd_client: &PdC,
    error: &Error,
    store_id: Option<StoreId>,
) {
    if let Some((address, version)) = error.connection_info() {
        pd_client.close_kv_client_addr_ver(address, version).await;
    } else if let Some(store_id) = store_id {
        pd_client.invalidate_store_cache(store_id).await;
    }
}

/// Await every task in `join_set`, reassembling the results in spawn order.
///
/// Contract: on any join failure (a panicked or cancelled task) the remaining tasks
/// are aborted via [`JoinSet::shutdown`] before the error is returned — an error from
/// the surrounding handler therefore means *no further effects from this call*. The
/// previous `try_join_all`-over-`JoinHandle`s code instead **detached** in-flight
/// tasks on early return, which let them race on after the caller had already
/// observed the failure and panicked the runtime's timer driver when a short-lived
/// runtime shut down underneath them (#534).
async fn collect_join_set_results<T>(
    mut join_set: JoinSet<(usize, T)>,
    task_count: usize,
    handler_name: &str,
) -> Result<Vec<T>>
where
    T: Send + 'static,
{
    let mut results = (0..task_count).map(|_| None).collect::<Vec<_>>();
    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok((idx, val)) => results[idx] = Some(val),
            Err(e) => {
                error!(
                    "{}: failed to join task ({} tasks): {}",
                    handler_name, task_count, e
                );
                join_set.shutdown().await;
                return Err(Error::JoinError(e));
            }
        }
    }

    Ok(results
        .into_iter()
        .map(|result| result.expect("all spawned tasks should return a result"))
        .collect())
}

/// Retry state used by multi-region plans. The legacy [`Backoff`] remains the
/// public default; source-owned paths such as RawKV can opt into the
/// cumulative `RetryBackoffer` state instead.
#[async_trait]
pub(crate) trait RegionRetryState: Clone + Send + Sync + 'static {
    /// Wait for a source retry class. `Ok(false)` means the legacy retry
    /// strategy is exhausted and the caller should return its triggering
    /// error unchanged.
    async fn backoff(&mut self, config: RetryConfig, reason: String) -> Result<bool>;

    /// Create a child retry state for a concurrently dispatched shard. The
    /// cancellation handle is shared by sibling children for first-error
    /// cancellation, as in RawKV's `Backoffer.Fork` topology.
    fn fork(&self) -> (Self, Cancellation);

    /// Merge the accounting from the final completed child once it is no
    /// longer used. Legacy `Backoff` intentionally keeps its old independent
    /// per-shard behavior.
    fn update_using_forked(&mut self, forked: &Self);

    /// RawKV owns an outer source retry loop: after `RegionRequestSender`
    /// returns a terminal region error, RawKV charges `BoRegionMiss`, locates
    /// again, and resends. Ordinary request plans leave that error visible to
    /// their callers.
    fn retries_terminal_region_errors(&self) -> bool {
        false
    }

    /// Whether the source request context was cancelled. Implementations
    /// without a cancellable context retain the legacy `false` default.
    fn is_cancelled(&self) -> bool {
        false
    }

    /// Shared snapshot retry owner, when region and lock retries charge one
    /// client-go Backoffer budget.
    fn snapshot_retry_owner(&self) -> Option<Arc<Mutex<RetryBackoffer>>> {
        None
    }
}

#[async_trait]
impl RegionRetryState for Backoff {
    async fn backoff(&mut self, _config: RetryConfig, _reason: String) -> Result<bool> {
        match self.next_delay_duration() {
            Some(duration) => {
                sleep(duration).await;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn fork(&self) -> (Self, Cancellation) {
        (self.clone(), Cancellation::default())
    }

    fn update_using_forked(&mut self, _forked: &Self) {}
}

/// client-go's `getMaxBackoff`, `batchGetMaxBackoff`, and scanner retry
/// budget. `RetryBackoffer` applies the configured backoff weight.
const SNAPSHOT_MAX_BACKOFF_MS: u64 = crate::transaction::GET_MAX_BACKOFF_MS;

pub(crate) fn new_snapshot_retry_owner(variables: Arc<Variables>) -> Arc<Mutex<RetryBackoffer>> {
    Arc::new(Mutex::new(RetryBackoffer::with_variables(
        Cancellation::default(),
        SNAPSHOT_MAX_BACKOFF_MS,
        variables,
    )))
}

/// Snapshot-read retry state that owns client-go's cumulative retry budget
/// while reporting retry-class sleep totals to an optional collector.
#[derive(Clone)]
pub(crate) struct SnapshotRegionBackoff {
    backoff: Arc<Mutex<RetryBackoffer>>,
    stats: Option<Arc<SnapshotRuntimeStats>>,
    disabled: bool,
}

impl SnapshotRegionBackoff {
    pub(crate) fn new(
        legacy_backoff: Backoff,
        stats: Option<Arc<SnapshotRuntimeStats>>,
        variables: Arc<Variables>,
    ) -> Self {
        Self {
            backoff: new_snapshot_retry_owner(variables),
            stats,
            disabled: legacy_backoff.is_none(),
        }
    }

    pub(crate) fn owner(&self) -> Arc<Mutex<RetryBackoffer>> {
        Arc::clone(&self.backoff)
    }

    pub(crate) fn set_owner(&mut self, owner: Arc<Mutex<RetryBackoffer>>) {
        self.backoff = owner;
    }

    pub(crate) fn clear_stats(&mut self) {
        self.stats = None;
    }
}

#[async_trait]
impl RegionRetryState for SnapshotRegionBackoff {
    async fn backoff(&mut self, config: RetryConfig, _reason: String) -> Result<bool> {
        if self.disabled {
            return Ok(false);
        }
        let mut backoff = self.backoff.lock().await;
        let before_count = backoff
            .times_by_type()
            .get(config.name)
            .copied()
            .unwrap_or_default();
        let before_sleep = backoff
            .sleep_by_type()
            .get(config.name)
            .copied()
            .unwrap_or_default();
        let result = backoff.backoff(config, _reason).await;
        let after_count = backoff
            .times_by_type()
            .get(config.name)
            .copied()
            .unwrap_or_default();
        let after_sleep = backoff
            .sleep_by_type()
            .get(config.name)
            .copied()
            .unwrap_or_default();
        if after_count > before_count {
            if let Some(stats) = &self.stats {
                stats.record_backoff(
                    config.name,
                    Duration::from_millis(after_sleep.saturating_sub(before_sleep)),
                );
            }
        }
        result
            .map(|_| true)
            .map_err(|error| Error::StringError(error.to_string()))
    }

    fn fork(&self) -> (Self, Cancellation) {
        let backoff = self
            .backoff
            .try_lock()
            .expect("snapshot retry owner must be idle while it is forked");
        let (backoff, cancellation) = backoff.fork();
        (
            Self {
                backoff: Arc::new(Mutex::new(backoff)),
                stats: self.stats.clone(),
                disabled: self.disabled,
            },
            cancellation,
        )
    }

    fn update_using_forked(&mut self, forked: &Self) {
        let forked = forked
            .backoff
            .try_lock()
            .expect("completed snapshot retry child must be idle");
        self.backoff
            .try_lock()
            .expect("snapshot retry parent must be idle while it is updated")
            .update_using_forked(&forked);
    }

    fn is_cancelled(&self) -> bool {
        self.backoff
            .try_lock()
            .expect("snapshot retry owner must be idle before dispatch")
            .is_cancelled()
    }

    fn snapshot_retry_owner(&self) -> Option<Arc<Mutex<RetryBackoffer>>> {
        Some(self.owner())
    }
}

/// Snapshot lock waits use a source-shaped cumulative backoffer. client-go
/// caps each `txnLockFast` sleep at the remaining lock TTL. Mutations retain
/// their legacy [`Backoff`] handling in [`ResolveLock`].
#[derive(Clone)]
pub(crate) struct SnapshotLockBackoff {
    backoff: Arc<Mutex<RetryBackoffer>>,
    stats: Option<Arc<SnapshotRuntimeStats>>,
}

impl SnapshotLockBackoff {
    pub(crate) fn new(stats: Option<Arc<SnapshotRuntimeStats>>, variables: Arc<Variables>) -> Self {
        Self {
            backoff: new_snapshot_retry_owner(variables),
            stats,
        }
    }

    pub(crate) fn set_owner(&mut self, owner: Arc<Mutex<RetryBackoffer>>) {
        self.backoff = owner;
    }

    pub(crate) fn clear_stats(&mut self) {
        self.stats = None;
    }

    async fn backoff_with_max_sleep_txn_lock_fast(
        &mut self,
        max_sleep_ms: u64,
        reason: String,
    ) -> Result<()> {
        let mut backoff = self.backoff.lock().await;
        let before_count = backoff
            .times_by_type()
            .get("txnLockFast")
            .copied()
            .unwrap_or_default();
        let before_sleep = backoff
            .sleep_by_type()
            .get("txnLockFast")
            .copied()
            .unwrap_or_default();
        let result = backoff
            .backoff_with_max_sleep_txn_lock_fast(max_sleep_ms, reason)
            .await;
        let after_count = backoff
            .times_by_type()
            .get("txnLockFast")
            .copied()
            .unwrap_or_default();
        let after_sleep = backoff
            .sleep_by_type()
            .get("txnLockFast")
            .copied()
            .unwrap_or_default();
        if after_count > before_count {
            if let Some(stats) = &self.stats {
                stats.record_backoff(
                    "txnLockFast",
                    Duration::from_millis(after_sleep.saturating_sub(before_sleep)),
                );
            }
        }
        result.map_err(|error| Error::StringError(error.to_string()))
    }
}

#[async_trait]
impl RegionRetryState for RetryBackoffer {
    async fn backoff(&mut self, config: RetryConfig, reason: String) -> Result<bool> {
        RetryBackoffer::backoff(self, config, reason)
            .await
            .map(|_| true)
            .map_err(|error| Error::StringError(error.to_string()))
    }

    fn fork(&self) -> (Self, Cancellation) {
        RetryBackoffer::fork(self)
    }

    fn update_using_forked(&mut self, forked: &Self) {
        RetryBackoffer::update_using_forked(self, forked);
    }

    fn retries_terminal_region_errors(&self) -> bool {
        true
    }

    fn is_cancelled(&self) -> bool {
        RetryBackoffer::is_cancelled(self)
    }
}

#[allow(private_bounds)]
pub struct RetryableMultiRegion<P: Plan, PdC: PdClient, R: RegionRetryState = Backoff> {
    pub(super) inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: R,

    /// Preserve all regions' results for other downstream plans to handle.
    /// If true, return Ok and preserve all regions' results, even if some of them are Err.
    /// Otherwise, return the first Err if there is any.
    pub preserve_region_results: bool,
    /// Maximum number of region shards dispatched concurrently.
    pub concurrency: usize,
    /// Snapshot scanner refills select exactly one boundary region. `true`
    /// selects the last (reverse) shard and `false` the first (forward) shard.
    pub(crate) one_region: Option<bool>,
    /// Initial batch-get sharding reports the number of distinct regions for
    /// client-go's snapshot metric. Retries deliberately clear this field.
    pub(crate) snapshot_region_scope: Option<bool>,
}

#[allow(private_bounds)]
impl<P: Plan + Shardable, PdC: PdClient, R: RegionRetryState> RetryableMultiRegion<P, PdC, R>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    // A plan may involve multiple shards
    #[async_recursion]
    async fn single_plan_handler(
        pd_client: Arc<PdC>,
        current_plan: P,
        mut backoff: R,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        one_region: Option<bool>,
        snapshot_region_scope: Option<bool>,
    ) -> (Result<<Self as Plan>::Result>, R) {
        let mut shards = current_plan.shards(&pd_client).collect::<Vec<_>>().await;
        if let Some(internal) = snapshot_region_scope {
            if shards.iter().all(Result::is_ok) {
                let regions = shards
                    .iter()
                    .filter_map(|shard| shard.as_ref().ok().map(|(_, region)| region.ver_id()))
                    .collect::<std::collections::HashSet<_>>()
                    .len();
                crate::stats::observe_snapshot_regions(internal, regions);
            }
        }
        if let Some(reverse) = one_region {
            let selected = if reverse {
                shards.pop()
            } else if shards.is_empty() {
                None
            } else {
                Some(shards.remove(0))
            };
            shards = selected.into_iter().collect();
        }
        let shards_len = shards.len();
        let record_async_batch_get_metric = snapshot_region_scope.is_some() && shards_len > 1;
        debug!("single_plan_handler, shards: {}", shards_len);
        let (forked_backoff, cancel) = backoff.fork();
        let mut join_set = JoinSet::new();
        for (idx, shard) in shards.into_iter().enumerate() {
            let (shard, region) = match shard {
                Ok(shard) => shard,
                Err(e) => {
                    join_set.shutdown().await;
                    return (Err(e), backoff);
                }
            };
            let mut clone = current_plan.clone_then_apply_shard(shard);
            clone.set_async_batch_get_metrics(record_async_batch_get_metric);
            let pd_client = pd_client.clone();
            let (backoff, _) = forked_backoff.fork();
            if let Some(owner) = backoff.snapshot_retry_owner() {
                clone.set_snapshot_retry_owner(owner);
            }
            let permits = permits.clone();
            join_set.spawn(async move {
                (
                    idx,
                    Self::single_shard_handler(
                        pd_client,
                        clone,
                        region,
                        backoff,
                        permits,
                        preserve_region_results,
                        one_region,
                    )
                    .await,
                )
            });
        }

        let mut results = std::iter::repeat_with(|| None)
            .take(shards_len)
            .collect::<Vec<Option<Result<<Self as Plan>::Result>>>>();
        let mut has_error = false;
        let mut last_forked = None;
        while let Some(joined) = join_set.join_next().await {
            let (index, (result, forked)) = match joined {
                Ok(joined) => joined,
                Err(error) => return (Err(error.into()), backoff),
            };
            if result.is_err() && !has_error {
                cancel.cancel();
                has_error = true;
            }
            last_forked = Some(forked);
            results[index] = Some(result);
        }
        if let Some(forked) = last_forked.as_ref() {
            backoff.update_using_forked(forked);
        }
        let results = results
            .into_iter()
            .map(|result| result.expect("successful shard task must produce a result"))
            .collect::<Vec<_>>();

        if !has_error {
            cancel.cancel();
        }
        if preserve_region_results {
            (
                Ok(results
                    .into_iter()
                    .flat_map_ok(|results| results)
                    .map(|result| result.and_then(|result| result))
                    .collect()),
                backoff,
            )
        } else {
            (
                results
                    .into_iter()
                    .collect::<Result<Vec<_>>>()
                    .map(|results| results.into_iter().flatten().collect()),
                backoff,
            )
        }
    }

    #[async_recursion]
    async fn single_shard_handler(
        pd_client: Arc<PdC>,
        mut plan: P,
        region: RegionWithLeader,
        mut backoff: R,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        one_region: Option<bool>,
    ) -> (Result<<Self as Plan>::Result>, R) {
        if backoff.is_cancelled() {
            return (
                Err(Error::StringError("context canceled".to_owned())),
                backoff,
            );
        }
        let region_ver_id = region.ver_id();
        let store_id = region.get_store_id().ok();
        debug!(
            "single_shard_handler, region: {:?}, store: {:?}",
            region_ver_id, store_id
        );
        let replica_read_config = plan.replica_read_config();
        let replica_selector_state = plan.replica_selector_state();
        let is_read_request = plan.is_read_request();
        let region_store = match pd_client
            .clone()
            .map_region_to_store_with_replica(
                region,
                replica_read_config,
                replica_selector_state,
                is_read_request,
            )
            .await
            .and_then(|region_store| {
                plan.apply_store(&region_store)?;
                Ok(region_store)
            }) {
            Ok(region_store) => region_store,
            Err(err) if is_selector_exhausted_error(&err) => {
                if let Some((config, reason)) = plan.largest_pending_backoff() {
                    match backoff.backoff(config, reason).await {
                        Ok(_) => {}
                        Err(backoff_error) => return (Err(backoff_error), backoff),
                    }
                }
                return (Err(err), backoff);
            }
            Err(err) => {
                debug!("single_shard_handler::sharding, error: {:?}", err);
                return Self::handle_other_error(
                    pd_client,
                    plan,
                    region_ver_id,
                    store_id,
                    None,
                    backoff,
                    permits,
                    preserve_region_results,
                    one_region,
                    err,
                )
                .await;
            }
        };
        if let Some(peer) = region_store.target_peer.as_ref() {
            plan.record_replica_attempt(peer.id);
        }
        let proxy_peer_id = region_store
            .physical_store_id
            .and_then(|physical_store_id| {
                region_store
                    .region_with_leader
                    .region
                    .peers
                    .iter()
                    .find(|peer| peer.store_id == physical_store_id)
                    .map(|peer| peer.id)
            });
        let proxy_peer_id = proxy_peer_id.filter(|proxy_peer_id| {
            region_store
                .target_peer
                .as_ref()
                .is_none_or(|target| target.id != *proxy_peer_id)
        });
        if let Some(proxy_peer_id) = proxy_peer_id {
            plan.record_replica_attempt(proxy_peer_id);
        }

        // A fast ServerIsBusy retry defers its delay until this selector
        // returns to the same logical store. Context construction increments
        // the source attempt counter before this wait, so preserve that order.
        if let Some(store_id) = region_store.target_peer.as_ref().map(|peer| peer.store_id) {
            if let Some((config, reason)) = plan.take_pending_backoff(store_id) {
                match backoff.backoff(config, reason.clone()).await {
                    Ok(true) => {}
                    Ok(false) => return (Err(Error::StringError(reason)), backoff),
                    Err(error) => return (Err(error), backoff),
                }
            }
        }

        // limit concurrent requests
        let permit = permits.acquire().await.unwrap();
        let rpc_started_at = Instant::now();
        let res = plan.execute().await;
        let rpc_duration = rpc_started_at.elapsed();
        drop(permit);

        if let Some(peer) = region_store.target_peer.as_ref() {
            plan.record_replica_attempted_time(peer.id, rpc_duration);
        }
        if let Some(proxy_peer_id) = proxy_peer_id {
            plan.record_replica_attempted_time(proxy_peer_id, rpc_duration);
        }

        let mut resp = match res {
            Ok(resp) => resp,
            Err(e) if is_grpc_deadline_exceeded(&e) && source_configurable_read_timeout(&plan) => {
                if let Some(peer) = region_store.target_peer.as_ref() {
                    plan.mark_replica_deadline_exceeded(peer.id);
                }
                debug!(
                    "single_shard_handler: configurable read timeout, reselection without backoff: {:?}",
                    e
                );
                plan.mark_retry_request();
                return Self::single_shard_handler(
                    pd_client,
                    plan,
                    region_store.region_with_leader,
                    backoff,
                    permits,
                    preserve_region_results,
                    one_region,
                )
                .await;
            }
            Err(e) if is_grpc_error(&e) => {
                debug!("single_shard_handler:execute: grpc error: {:?}", e);
                return Self::handle_other_error(
                    pd_client,
                    plan,
                    region_store.region_with_leader.ver_id(),
                    region_store
                        .physical_store_id
                        .or_else(|| region_store.region_with_leader.get_store_id().ok()),
                    Some(region_store.clone()),
                    backoff,
                    permits,
                    preserve_region_results,
                    one_region,
                    e,
                )
                .await;
            }
            Err(e) => {
                debug!("single_shard_handler:execute: error: {:?}", e);
                return (Err(e), backoff);
            }
        };

        if let Some(e) = resp.key_errors() {
            debug!("single_shard_handler:execute: key errors: {:?}", e);
            (Ok(vec![Err(Error::MultipleKeyErrors(e))]), backoff)
        } else if let Some(e) = resp.region_error() {
            debug!(
                "single_shard_handler:execute: region error: {:?}, region: {:?}",
                e, region_ver_id
            );
            let region_error_label = region_error_label(&e);
            if region_error_label == "unknown" {
                info!("unknown region error: {e:?}");
            }
            crate::stats::increment_region_error(
                region_error_label,
                region_store.target_peer.as_ref().map(|peer| peer.store_id),
            );
            if let Some(runtime_stats) = plan.region_request_runtime_stats() {
                runtime_stats.record_error(region_error_label);
                if let Some(peer) = region_store.target_peer.as_ref() {
                    runtime_stats.record_replica_access(
                        region_store.stale_read,
                        region_store.is_replica_read(),
                        peer.id,
                        peer.store_id,
                        region_error_access_message(&e, region_error_label),
                    );
                }
            }
            // client-go returns an indeterminate result before every typed
            // region-error branch. Do not mutate selector/cache state or let
            // RawKV's generic region-miss retry resend a possibly committed
            // write when a malformed response also carries another field.
            if e.undetermined_result.is_some() {
                return (Err(Error::RegionError(Box::new(e))), backoff);
            }
            if source_configurable_server_busy_timeout(&plan, &e) {
                if let Some(peer) = region_store.target_peer.as_ref() {
                    plan.mark_replica_deadline_exceeded(peer.id);
                }
                debug!(
                    "single_shard_handler: configurable server-busy deadline, reselection without backoff: {:?}",
                    e
                );
                plan.mark_retry_request();
                return Self::single_shard_handler(
                    pd_client,
                    plan,
                    region_store.region_with_leader,
                    backoff,
                    permits,
                    preserve_region_results,
                    one_region,
                )
                .await;
            }
            if let (Some(busy), Some(target_peer)) =
                (e.server_is_busy.as_ref(), region_store.target_peer.as_ref())
            {
                if source_batched_coprocessor_busy_is_terminal(
                    &plan.replica_read_config(),
                    is_read_request,
                    plan.is_batched_coprocessor_read(),
                    busy.estimated_wait_ms,
                ) {
                    // `onServerIsBusy` updates the load estimate, then leaves
                    // a batched Cop response to its task owner. Retrying it as
                    // a single replica request would manufacture region misses.
                    pd_client.record_server_load(target_peer.store_id, busy.estimated_wait_ms);
                    return (Ok(vec![Ok(resp)]), backoff);
                }
            }
            if e.data_is_not_ready.is_some() {
                if let Some(peer) = region_store.target_peer.as_ref() {
                    plan.mark_replica_data_not_ready(peer.id);
                }
            }
            if let (Some(busy), Some(target_peer), Some(leader)) = (
                e.server_is_busy.as_ref(),
                region_store.target_peer.as_ref(),
                region_store.region_with_leader.leader.as_ref(),
            ) {
                plan.record_busy_leader(target_peer.id, leader.id, busy.estimated_wait_ms);
            }
            if let (Some(busy), Some(target_peer)) =
                (e.server_is_busy.as_ref(), region_store.target_peer.as_ref())
            {
                if !plan.is_batched_coprocessor_read() {
                    let config = plan.replica_read_config();
                    if busy.estimated_wait_ms != 0
                        && config.busy_threshold_ms != 0
                        && is_read_request
                    {
                        plan.record_server_busy(target_peer.id);
                    }
                    if busy.estimated_wait_ms != 0 {
                        pd_client.record_server_load(target_peer.store_id, busy.estimated_wait_ms);
                    }
                }
            }
            if let (Some(not_leader), Some(target_peer)) =
                (e.not_leader.as_ref(), region_store.target_peer.as_ref())
            {
                if let Some(leader) = not_leader.leader.as_ref() {
                    plan.record_not_leader(target_peer.id, leader.id);
                } else {
                    plan.mark_replica_no_leader(target_peer.id);
                }
            }
            let retry_flashback_through_leader =
                e.flashback_in_progress.is_some() && region_store.is_replica_read();
            if retry_flashback_through_leader {
                plan.force_leader_after_flashback();
            }
            let retry_region_not_found_at_leader = e.region_not_found.is_some()
                && region_store
                    .region_with_leader
                    .leader
                    .as_ref()
                    .is_some_and(|leader| plan.force_leader_after_region_not_found(leader.id));
            let fast_server_busy_retry = e.server_is_busy.as_ref().is_some_and(|busy| {
                source_fast_server_busy_retry(
                    &plan.replica_read_config(),
                    &plan.replica_selector_state(),
                    &region_store,
                    is_read_request,
                    plan.is_batched_coprocessor_read(),
                    busy.estimated_wait_ms,
                )
            });
            if fast_server_busy_retry {
                if let Some(target_peer) = region_store.target_peer.as_ref() {
                    plan.record_server_busy(target_peer.id);
                    plan.add_pending_backoff(
                        target_peer.store_id,
                        source_server_busy_backoff_config(&region_store),
                        format!("server is busy: {e:?}"),
                    );
                }
            }
            let configurable_region_error_timeout =
                source_configurable_region_error_timeout(&plan, &e);
            if configurable_region_error_timeout {
                if let Some(peer) = region_store.target_peer.as_ref() {
                    plan.mark_replica_deadline_exceeded(peer.id);
                }
            }
            let region_error_action = if retry_flashback_through_leader {
                Ok(RegionErrorRetry::Immediate)
            } else if retry_region_not_found_at_leader {
                // Source `onRegionNotFound` hard-invalidates the cache so
                // concurrent users refresh from PD, while this request gets
                // one immediate retry against its previously untried leader.
                pd_client
                    .invalidate_region_cache(region_ver_id.clone())
                    .await;
                Ok(RegionErrorRetry::Immediate)
            } else if configurable_region_error_timeout {
                // `onRegionError` recognizes this text-only protobuf form of
                // deadline exhaustion after its typed region-error branches.
                Ok(RegionErrorRetry::Immediate)
            } else {
                handle_region_error(pd_client.clone(), e.clone(), region_store.clone()).await
            };
            let mut region_error_action = match region_error_action {
                Ok(action) => action,
                Err(error)
                    if matches!(error, Error::RegionError(_))
                        && backoff.retries_terminal_region_errors() =>
                {
                    match backoff
                        .backoff(BO_REGION_MISS, format!("raw region error: {error:?}"))
                        .await
                    {
                        Ok(true) => {
                            plan.mark_retry_request();
                            return Self::single_plan_handler(
                                pd_client,
                                plan,
                                backoff,
                                permits,
                                preserve_region_results,
                                one_region,
                                None,
                            )
                            .await;
                        }
                        Ok(false) => return (Err(error), backoff),
                        Err(backoff_error) => return (Err(backoff_error), backoff),
                    }
                }
                Err(error) => return (Err(error), backoff),
            };
            if source_fast_selector_retry(&e, fast_server_busy_retry)
                && matches!(
                    region_error_action,
                    RegionErrorRetry::Backoff(config)
                        if config == BO_TIKV_SERVER_BUSY || config == BO_STALE_CMD
                )
            {
                region_error_action = RegionErrorRetry::Immediate;
            }
            match region_error_action {
                RegionErrorRetry::Immediate => {
                    // Source `RegionRequestSender` retains its `KeyLocation`
                    // and advances the replica selector. Do not re-enter the
                    // Rust sharding layer here: split/merge outcomes are
                    // already terminal and are rebuilt by their owning
                    // caller path.
                    plan.mark_retry_request();
                    return Self::single_shard_handler(
                        pd_client,
                        plan,
                        region_store.region_with_leader,
                        backoff,
                        permits,
                        preserve_region_results,
                        one_region,
                    )
                    .await;
                }
                RegionErrorRetry::Backoff(config) => {
                    match backoff
                        .backoff(config, format!("region error: {e:?}"))
                        .await
                    {
                        Ok(true) => {
                            // Source performs its per-store pending backoff
                            // before the next `replicaSelector.next`, without
                            // rebuilding the original key-to-region shard.
                            plan.mark_retry_request();
                            return Self::single_shard_handler(
                                pd_client,
                                plan,
                                region_store.region_with_leader,
                                backoff,
                                permits,
                                preserve_region_results,
                                one_region,
                            )
                            .await;
                        }
                        Ok(false) => {
                            warn!(
                                "giving up after exhausting retries on region error, region: {:?}",
                                region_ver_id
                            );
                            return (Err(Error::RegionError(Box::new(e))), backoff);
                        }
                        Err(error) => return (Err(error), backoff),
                    }
                }
                RegionErrorRetry::TerminalAfterBackoff(config) => {
                    match backoff
                        .backoff(config, format!("region error: {e:?}"))
                        .await
                    {
                        Ok(_) => return (Err(Error::RegionError(Box::new(e))), backoff),
                        Err(error) => return (Err(error), backoff),
                    }
                }
            }
        } else {
            if !region_store.forwarded_host.is_empty() {
                if let Some(proxy_store_id) = region_store.physical_store_id {
                    pd_client
                        .record_forwarding_proxy(
                            region_store.region_with_leader.ver_id(),
                            proxy_store_id,
                        )
                        .await;
                }
            }
            if let Some(leader) = region_store.successful_forced_leader_peer() {
                // Source `onSendSuccess` updates its cached working leader
                // after a forced follower leader-read succeeds. Cache update
                // failure is advisory and must not turn a successful user RPC
                // into an error.
                if let Err(error) = pd_client
                    .update_leader(region_store.region_with_leader.ver_id(), leader)
                    .await
                {
                    warn!(
                        "failed to update cached leader after successful forced leader read: {:?}",
                        error
                    );
                }
            }
            (Ok(vec![Ok(resp)]), backoff)
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_other_error(
        pd_client: Arc<PdC>,
        mut plan: P,
        region: RegionVerId,
        store: Option<StoreId>,
        route: Option<RegionStore>,
        mut backoff: R,
        permits: Arc<Semaphore>,
        preserve_region_results: bool,
        one_region: Option<bool>,
        e: Error,
    ) -> (Result<<Self as Plan>::Result>, R) {
        debug!("handle_other_error: {:?}", e);
        // A cancelled caller does not say anything about TiKV liveness. The
        // source returns immediately without invalidating store/region state
        // or charging a transport backoff.
        if is_request_cancelled_error(&e, backoff.is_cancelled()) {
            return (Err(e), backoff);
        }
        let invalidate_region = pd_client.clone().on_send_failure(route.as_ref()).await;
        let transport_backoff = source_transport_backoff_config(route.as_ref());
        let retained_region = (!invalidate_region)
            .then(|| route.as_ref().map(|route| route.region_with_leader.clone()))
            .flatten();
        if invalidate_region {
            pd_client.invalidate_region_cache(region).await;
        }
        if is_grpc_error(&e) && invalidate_region {
            invalidate_connection_for_error(pd_client.as_ref(), &e, store).await;
        }
        match backoff
            .backoff(transport_backoff, format!("send store request error: {e}"))
            .await
        {
            Ok(true) => {
                plan.mark_retry_request();
                if let Some(region) = retained_region {
                    // `replicaSelector.onSendFailure` retains source routing
                    // state and its KeyLocation; the following TiKV-RPC
                    // backoff is not a request re-sharding boundary.
                    Self::single_shard_handler(
                        pd_client,
                        plan,
                        region,
                        backoff,
                        permits,
                        preserve_region_results,
                        one_region,
                    )
                    .await
                } else {
                    Self::single_plan_handler(
                        pd_client,
                        plan,
                        backoff,
                        permits,
                        preserve_region_results,
                        one_region,
                        None,
                    )
                    .await
                }
            }
            Ok(false) => (Err(e), backoff),
            Err(error) => (Err(error), backoff),
        }
    }
}

/// The source selector performs a zero-delay retry through another eligible
/// replica when a busy response has already made the ordinary leader path
/// unsuitable. A healthy leader with `ServerIsBusy(0)` deliberately still
/// uses the server-busy backoff; its later suspect-leader probe is separate.
fn source_fast_server_busy_retry(
    config: &ReplicaReadConfig,
    selector_state: &ReplicaSelectorState,
    region_store: &RegionStore,
    is_read_request: bool,
    is_batched_coprocessor_read: bool,
    estimated_wait_ms: u32,
) -> bool {
    if estimated_wait_ms != 0 && is_batched_coprocessor_read {
        return false;
    }
    if !matches!(config.read_type, crate::kv::ReplicaReadType::Leader)
        || region_store.force_leader_read
    {
        return true;
    }
    let threshold_redirect =
        estimated_wait_ms != 0 && config.busy_threshold_ms != 0 && is_read_request;
    let Some(leader) = region_store.region_with_leader.leader.as_ref() else {
        return true;
    };
    threshold_redirect
        || !selector_state.is_leader_selectable(leader.id)
        || selector_state.is_server_busy(leader.id)
}

fn source_batched_coprocessor_busy_is_terminal(
    config: &ReplicaReadConfig,
    is_read_request: bool,
    is_batched_coprocessor_read: bool,
    estimated_wait_ms: u32,
) -> bool {
    estimated_wait_ms != 0
        && config.busy_threshold_ms != 0
        && is_read_request
        && is_batched_coprocessor_read
}

fn is_selector_exhausted_error(error: &Error) -> bool {
    matches!(error, Error::RegionError(region_error) if region_error.epoch_not_match.is_some())
}

fn is_request_cancelled_error(error: &Error, request_context_cancelled: bool) -> bool {
    match error {
        Error::GrpcAPI(status) => {
            status.code() == tonic::Code::Cancelled && request_context_cancelled
        }
        Error::Connection { source, .. } => {
            is_request_cancelled_error(source, request_context_cancelled)
        }
        Error::StringError(message) => message == "context canceled",
        _ => false,
    }
}

/// Source `onSendFail` uses TiFlash's distinct terminal timeout/backoff class
/// for both TiFlash and TiFlash-compute physical endpoints.
fn source_transport_backoff_config(route: Option<&RegionStore>) -> RetryConfig {
    route
        .is_some_and(|route| route.physical_endpoint_type.is_tiflash_related())
        .then_some(BO_TIFLASH_RPC)
        .unwrap_or(BO_TIKV_RPC)
}

/// Source `onRegionError` also distinguishes a TiFlash physical destination
/// for a server-busy reply. This path has no route absence because it runs
/// after the request has been dispatched to a concrete region store.
fn source_server_busy_backoff_config(route: &RegionStore) -> RetryConfig {
    route
        .physical_endpoint_type
        .is_tiflash_related()
        .then_some(BO_TIFLASH_SERVER_BUSY)
        .unwrap_or(BO_TIKV_SERVER_BUSY)
}

/// `replicaSelector.onRegionError` handles stale-command replies by selecting
/// again without waiting. Server-busy uses the narrower policy above.
fn source_fast_selector_retry(error: &errorpb::Error, fast_server_busy_retry: bool) -> bool {
    error.stale_command.is_some() || fast_server_busy_retry
}

/// `replicaSelector.onReadReqConfigurableTimeout`: a deadline on a read
/// request using a caller-configured duration below `ReadTimeoutShort` (30 s)
/// reselects immediately. `record_replica_attempt` has already recorded that
/// unsuitable peer before dispatch, so the next route observes the same
/// exhaustion boundary as client-go's selector flag.
fn source_configurable_read_timeout<P: Shardable>(plan: &P) -> bool {
    plan.is_read_request() && plan.max_execution_duration_ms() < 30_000
}

/// Source `replicaSelector.onReadReqConfigurableTimeout` is also selected by
/// TiKV's `ServerIsBusy` message when its reason reports the read deadline.
fn source_configurable_server_busy_timeout<P: Shardable>(plan: &P, error: &errorpb::Error) -> bool {
    source_configurable_read_timeout(plan)
        && error
            .server_is_busy
            .as_ref()
            .is_some_and(|busy| busy.reason.contains("deadline is exceeded"))
}

/// Source `isDeadlineExceeded`: a few TiKV region errors expose deadline
/// exhaustion only through their message instead of a gRPC status or typed
/// `ServerIsBusy` reason.
fn source_configurable_region_error_timeout<P: Shardable>(
    plan: &P,
    error: &errorpb::Error,
) -> bool {
    source_configurable_read_timeout(plan)
        && error.message.contains("Deadline is exceeded")
        // `onRegionError` reaches `isDeadlineExceeded` only after every
        // preceding typed branch. Preserve that ordering for malformed or
        // mixed protobufs too.
        && error.not_leader.is_none()
        && error.disk_full.is_none()
        && error.recovery_in_progress.is_none()
        && error.is_witness.is_none()
        && error.flashback_in_progress.is_none()
        && error.flashback_not_prepared.is_none()
        && error.undetermined_result.is_none()
        && error.region_not_found.is_none()
        && error.key_not_in_region.is_none()
        && error.epoch_not_match.is_none()
        && error.bucket_version_not_match.is_none()
        && error.server_is_busy.is_none()
        && error.stale_command.is_none()
        && error.store_not_match.is_none()
        && error.raft_entry_too_large.is_none()
        && error.max_timestamp_not_synced.is_none()
        && error.region_not_initialized.is_none()
        && error.read_index_not_ready.is_none()
        && error.proposal_in_merging_mode.is_none()
        && error.data_is_not_ready.is_none()
}

/// How the source region-request sender continues after processing a region
/// error.  Keeping the error class here ensures every plan surface uses the
/// same source retry budget rather than reducing all errors to `regionMiss`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionErrorRetry {
    /// The selector/cache change itself makes another attempt safe now.
    Immediate,
    /// The attempt must consume this client-go retry class before retrying.
    Backoff(RetryConfig),
    /// Source consumes this retry class for accounting/throttling, but ends
    /// the current send loop afterward and returns the region error.
    TerminalAfterBackoff(RetryConfig),
}

/// Source `OnRegionEpochNotMatch` distinguishes a stale local epoch—which
/// needs one region-miss backoff before retrying—from the ordinary
/// stop-and-resplit outcome after cache replacement/invalidation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EpochNotMatchOutcome {
    RetryAfterBackoff,
    Stop,
}

// Returns a source-compatible retry action, or a terminal request error.
pub(crate) async fn handle_region_error<PdC: PdClient>(
    pd_client: Arc<PdC>,
    e: errorpb::Error,
    region_store: RegionStore,
) -> Result<RegionErrorRetry> {
    let ver_id = region_store.region_with_leader.ver_id();
    let store_id = region_store.region_with_leader.get_store_id();
    debug!("handling region error: {:?}, region: {:?}", e, ver_id);
    // Source checks this flag before the ordered errorpb branch chain. A
    // malformed response can carry several optional errors, so preserving the
    // precedence prevents an indeterminate write from being retried.
    if e.undetermined_result.is_some() {
        return Err(Error::RegionError(Box::new(e)));
    }
    if let Some(not_leader) = e.not_leader {
        if let Some(leader) = not_leader.leader {
            match pd_client
                .update_leader(region_store.region_with_leader.ver_id(), leader)
                .await
            {
                Ok(_) => Ok(RegionErrorRetry::Immediate),
                Err(e) => {
                    pd_client.invalidate_region_cache(ver_id).await;
                    Err(e)
                }
            }
        } else {
            // The peer doesn't know who is the current leader. Generally it's because
            // the Raft group is in an election, but it's possible that the peer is
            // isolated and removed from the Raft group. So it's necessary to reload
            // the region from PD.
            pd_client.invalidate_region_cache(ver_id).await;
            Ok(RegionErrorRetry::Backoff(BO_REGION_SCHEDULING))
        }
    } else if e.disk_full.is_some() {
        Ok(RegionErrorRetry::Backoff(BO_TIKV_DISK_FULL))
    } else if e.recovery_in_progress.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        Ok(RegionErrorRetry::TerminalAfterBackoff(
            BO_REGION_RECOVERY_IN_PROGRESS,
        ))
    } else if e.is_witness.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        Ok(RegionErrorRetry::TerminalAfterBackoff(BO_IS_WITNESS))
    } else if let Some(flashback) = e.flashback_in_progress.as_ref() {
        // With no replica-read fallback, source returns a direct terminal
        // error rather than propagating the raw region-error wrapper.
        Err(Error::StringError(format!(
            "region {} is in flashback progress, FlashbackStartTS is {}",
            flashback.region_id, flashback.flashback_start_ts
        )))
    } else if let Some(flashback) = e.flashback_not_prepared.as_ref() {
        Err(Error::StringError(format!(
            "region {} is not prepared for the flashback",
            flashback.region_id
        )))
    } else if e.region_not_found.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        // The one source retry is handled by the caller before entering this
        // common branch: a follower failure may hand the current selector to
        // an untried leader. Once that is unavailable, stop so the caller can
        // rebuild/resplit instead of retrying the stale region in place.
        Err(Error::RegionError(Box::new(e)))
    } else if e.key_not_in_region.is_some() {
        pd_client.invalidate_region_cache(ver_id).await;
        Err(Error::RegionError(Box::new(e)))
    } else if let Some(epoch_not_match) = e.epoch_not_match.clone() {
        match on_region_epoch_not_match(pd_client.clone(), region_store, epoch_not_match).await? {
            EpochNotMatchOutcome::RetryAfterBackoff => {
                Ok(RegionErrorRetry::Backoff(BO_REGION_MISS))
            }
            EpochNotMatchOutcome::Stop => Err(Error::RegionError(Box::new(e))),
        }
    } else if let Some(bucket_mismatch) = e.bucket_version_not_match.as_ref() {
        pd_client
            .update_buckets(
                ver_id,
                bucket_mismatch.version,
                bucket_mismatch.keys.clone(),
            )
            .await;
        // client-go updates the bucket cache but deliberately returns this
        // region error to its bucket-aware caller, which must reschedule the
        // original work using the new boundaries.
        Err(Error::RegionError(Box::new(e)))
    } else if let Some(server_is_busy) = e.server_is_busy.as_ref() {
        if server_is_busy.estimated_wait_ms == 0 {
            if let Some(health_status) = region_store.health_status.as_ref() {
                health_status.mark_already_slow();
            }
        }
        Ok(RegionErrorRetry::Backoff(
            source_server_busy_backoff_config(&region_store),
        ))
    } else if e.stale_command.is_some() {
        Ok(RegionErrorRetry::Backoff(BO_STALE_CMD))
    } else if e.store_not_match.is_some() {
        // client-go marks the store for re-resolution and invalidates this
        // region, then stops the current send loop (`retry == false`). Do not
        // consume the generic region-miss budget and hide the error here.
        pd_client.invalidate_region_cache(ver_id).await;
        if let Ok(store_id) = store_id {
            pd_client.invalidate_store_cache(store_id).await;
        }
        // `RegionRequestSender.onRegionError` closes `RPCContext.Addr`, which
        // is the logical destination. A forwarding proxy has a separate
        // transport address and must not be retired for this error.
        let store_address = if region_store.forwarded_host.is_empty() {
            &region_store.target
        } else {
            &region_store.forwarded_host
        };
        if !store_address.is_empty() {
            pd_client
                .close_kv_client_addr_ver(store_address, u64::MAX)
                .await;
        }
        Err(Error::RegionError(Box::new(e)))
    } else if e.raft_entry_too_large.is_some() {
        // `onRegionError` returns `errors.New(regionErr.String())`: preserve
        // the direct terminal boundary so outer RawKV region-error recovery
        // cannot resend an oversized write.
        Err(Error::StringError(format!("{e:?}")))
    } else if e.max_timestamp_not_synced.is_some() {
        Ok(RegionErrorRetry::Backoff(BO_MAX_TS_NOT_SYNCED))
    } else if e.region_not_initialized.is_some() {
        Ok(RegionErrorRetry::Backoff(BO_MAX_REGION_NOT_INITIALIZED))
    } else if e.read_index_not_ready.is_some() || e.proposal_in_merging_mode.is_some() {
        Ok(RegionErrorRetry::Backoff(BO_REGION_SCHEDULING))
    } else if e.data_is_not_ready.is_some() {
        // client-go retries stale reads without a delay after marking this
        // replica unsuitable.  A fresh Rust mapping performs the equivalent
        // candidate selection on the next attempt.
        Ok(RegionErrorRetry::Immediate)
    } else if e.mismatch_peer_id.is_some() {
        // Like StoreNotMatch, source invalidates selector/region state and
        // terminates this send loop so the caller can rebuild its request.
        pd_client.invalidate_region_cache(ver_id).await;
        Err(Error::RegionError(Box::new(e)))
    } else if e.message.contains("invalid max_ts update") {
        // Source `isInvalidMaxTsUpdate` is a direct terminal sender error,
        // not an unknown-error replica retry.
        Err(Error::StringError(format!("{e:?}")))
    } else {
        // TiKV sends every source request through `replicaSelector`; its
        // fallback returns true here so `next` can select another replica
        // without cache eviction or backoff. Rust routes the next attempt
        // through the retained `ReplicaSelectorState` in `Dispatch`.
        debug!(
            "unknown region error, retrying source replica selection, region: {:?}: {:?}",
            ver_id, e
        );
        Ok(RegionErrorRetry::Immediate)
    }
}

// Mirrors source `RegionCache.OnRegionEpochNotMatch` retry semantics.
pub(crate) async fn on_region_epoch_not_match<PdC: PdClient>(
    pd_client: Arc<PdC>,
    region_store: RegionStore,
    error: EpochNotMatch,
) -> Result<EpochNotMatchOutcome> {
    let ver_id = region_store.region_with_leader.ver_id();
    if error.current_regions.is_empty() {
        pd_client.invalidate_region_cache(ver_id).await;
        return Ok(EpochNotMatchOutcome::Stop);
    }

    for r in &error.current_regions {
        if r.id == region_store.region_with_leader.id() {
            let region_epoch = r.region_epoch.as_ref().unwrap();
            let returned_conf_ver = region_epoch.conf_ver;
            let returned_version = region_epoch.version;
            let current_region_epoch = region_store
                .region_with_leader
                .region
                .region_epoch
                .clone()
                .unwrap();
            let current_conf_ver = current_region_epoch.conf_ver;
            let current_version = current_region_epoch.version;

            // Find whether the current region is ahead of TiKV's. If so, backoff.
            if returned_conf_ver < current_conf_ver || returned_version < current_version {
                return Ok(EpochNotMatchOutcome::RetryAfterBackoff);
            }
        }
    }

    // client-go installs every replacement region from the error and seeds
    // its working leader from the responding store. A following route lookup
    // can therefore use split/merged metadata without an avoidable PD round
    // trip. The old entry remains only when TiKV returned that exact version.
    let responding_store_id = region_store
        .target_peer
        .as_ref()
        .or(region_store.region_with_leader.leader.as_ref())
        .map(|peer| peer.store_id);
    // `OnRegionEpochNotMatch` carries the previous bucket metadata into each
    // replacement until PD provides a newer version. The source treats it as
    // a cache hint, so split/merged entries may temporarily share it.
    let inherited_buckets = region_store.region_with_leader.buckets.clone();
    let preserves_old_region = error.current_regions.iter().any(|region| {
        region.id == ver_id.id
            && region.region_epoch.as_ref().is_some_and(|epoch| {
                epoch.conf_ver == ver_id.conf_ver && epoch.version == ver_id.ver
            })
    });
    if !preserves_old_region {
        pd_client.invalidate_region_cache(ver_id).await;
    }
    let replacements = error
        .current_regions
        .into_iter()
        .map(|region| {
            let leader = responding_store_id.and_then(|store_id| {
                region
                    .peers
                    .iter()
                    .find(|peer| peer.store_id == store_id)
                    .cloned()
            });
            let mut replacement = RegionWithLeader::new(region, leader);
            replacement.buckets = inherited_buckets.clone();
            replacement
        })
        .collect();
    pd_client.update_region_cache(replacements).await?;
    Ok(EpochNotMatchOutcome::Stop)
}

#[allow(private_bounds)]
impl<P: Plan, PdC: PdClient, R: RegionRetryState> Clone for RetryableMultiRegion<P, PdC, R> {
    fn clone(&self) -> Self {
        RetryableMultiRegion {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            preserve_region_results: self.preserve_region_results,
            concurrency: self.concurrency,
            one_region: self.one_region,
            snapshot_region_scope: self.snapshot_region_scope,
        }
    }
}

#[async_trait]
#[allow(private_bounds)]
impl<P: Plan + Shardable, PdC: PdClient, R: RegionRetryState> Plan
    for RetryableMultiRegion<P, PdC, R>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    type Result = Vec<Result<P::Result>>;

    async fn execute(&self) -> Result<Self::Result> {
        // Limit the maximum concurrency of multi-region request. If there are
        // too many concurrent requests, TiKV is more likely to return a "TiKV
        // is busy" error
        let concurrency_permits = Arc::new(Semaphore::new(self.concurrency.max(1)));
        Self::single_plan_handler(
            self.pd_client.clone(),
            self.inner.clone(),
            self.backoff.clone(),
            concurrency_permits.clone(),
            self.preserve_region_results,
            self.one_region,
            self.snapshot_region_scope,
        )
        .await
        .0
    }
}

pub struct RetryableAllStores<P: Plan, PdC: PdClient> {
    pub(super) inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
    pub tikv_only: bool,
}

impl<P: Plan, PdC: PdClient> Clone for RetryableAllStores<P, PdC> {
    fn clone(&self) -> Self {
        RetryableAllStores {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            tikv_only: self.tikv_only,
        }
    }
}

// About `HasRegionError`:
// Store requests should be return region errors.
// But as the response of only store request by now (UnsafeDestroyRangeResponse) has the `region_error` field,
// we require `HasRegionError` to check whether there is region error returned from TiKV.
#[async_trait]
impl<P: Plan + StoreRequest, PdC: PdClient> Plan for RetryableAllStores<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    type Result = Vec<Result<P::Result>>;

    async fn execute(&self) -> Result<Self::Result> {
        let concurrency_permits = Arc::new(Semaphore::new(MULTI_STORES_CONCURRENCY));
        let mut stores = match self.pd_client.clone().all_stores().await {
            Ok(stores) => stores,
            Err(error) => {
                if self.tikv_only {
                    crate::stats::increment_unsafe_destroy_range_failure("get_stores");
                }
                return Err(error);
            }
        };
        if self.tikv_only {
            stores.retain(|store| store.endpoint_type == crate::store::EndpointType::TiKv);
        }
        let stores_len = stores.len();
        let mut join_set = JoinSet::new();
        for (idx, store) in stores.into_iter().enumerate() {
            let mut clone = self.inner.clone();
            clone.apply_store(&store);
            let backoff = self.backoff.clone();
            let concurrency_permits = concurrency_permits.clone();
            join_set.spawn(async move {
                (
                    idx,
                    Self::single_store_handler(clone, backoff, concurrency_permits).await,
                )
            });
        }

        let results =
            match collect_join_set_results(join_set, stores_len, "single_store_handler").await {
                Ok(results) => results,
                Err(error) => {
                    if self.tikv_only {
                        crate::stats::increment_unsafe_destroy_range_failure("send");
                    }
                    return Err(error);
                }
            };
        if self.tikv_only {
            for _ in results.iter().filter(|result| result.is_err()) {
                crate::stats::increment_unsafe_destroy_range_failure("send");
            }
        }
        Ok(results)
    }
}

impl<P: Plan, PdC: PdClient> RetryableAllStores<P, PdC>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    async fn single_store_handler(
        plan: P,
        mut backoff: Backoff,
        permits: Arc<Semaphore>,
    ) -> Result<P::Result> {
        loop {
            let permit = permits.acquire().await.unwrap();
            let res = plan.execute().await;
            drop(permit);

            match res {
                Ok(mut resp) => {
                    if let Some(e) = resp.key_errors() {
                        return Err(Error::MultipleKeyErrors(e));
                    } else if let Some(e) = resp.region_error() {
                        // Store request should not return region error.
                        return Err(Error::RegionError(Box::new(e)));
                    } else {
                        return Ok(resp);
                    }
                }
                Err(e) if is_grpc_error(&e) => match backoff.next_delay_duration() {
                    Some(duration) => {
                        sleep(duration).await;
                        continue;
                    }
                    None => return Err(e),
                },
                Err(e) => return Err(e),
            }
        }
    }
}

/// A technique for merging responses into a single result (with type `Out`).
pub trait Merge<In>: Sized + Clone + Send + Sync + 'static {
    type Out: Send;

    fn merge(&self, input: Vec<Result<In>>) -> Result<Self::Out>;
}

#[derive(Clone)]
pub struct MergeResponse<P: Plan, In, M: Merge<In>> {
    pub inner: P,
    pub merge: M,
    pub phantom: PhantomData<In>,
}

#[async_trait]
impl<In: Clone + Send + Sync + 'static, P: Plan<Result = Vec<Result<In>>>, M: Merge<In>> Plan
    for MergeResponse<P, In, M>
{
    type Result = M::Out;

    async fn execute(&self) -> Result<Self::Result> {
        self.merge.merge(self.inner.execute().await?)
    }
}

/// A merge strategy which collects data from a response into a single type.
#[derive(Clone, Copy)]
pub struct Collect;

/// A merge strategy that only takes the first element. It's used for requests
/// that should have exactly one response, e.g. a get request.
#[derive(Clone, Copy)]
pub struct CollectSingle;

#[doc(hidden)]
#[macro_export]
macro_rules! collect_single {
    ($type_: ty) => {
        impl Merge<$type_> for CollectSingle {
            type Out = $type_;

            fn merge(&self, mut input: Vec<Result<$type_>>) -> Result<Self::Out> {
                assert!(input.len() == 1);
                input.pop().unwrap()
            }
        }
    };
}

/// A merge strategy to be used with
/// [`preserve_shard`](super::plan_builder::PlanBuilder::preserve_shard).
/// It matches the shards preserved before and the values returned in the response.
#[derive(Clone, Debug)]
pub struct CollectWithShard;

/// A merge strategy which returns an error if any response is an error and
/// otherwise returns a Vec of the results.
#[derive(Clone, Copy)]
pub struct CollectError;

impl<T: Send> Merge<T> for CollectError {
    type Out = Vec<T>;

    fn merge(&self, input: Vec<Result<T>>) -> Result<Self::Out> {
        input.into_iter().collect()
    }
}

/// Process data into another kind of data.
pub trait Process<In>: Sized + Clone + Send + Sync + 'static {
    type Out: Send;

    fn process(&self, input: Result<In>) -> Result<Self::Out>;
}

#[derive(Clone)]
pub struct ProcessResponse<P: Plan, Pr: Process<P::Result>> {
    pub inner: P,
    pub processor: Pr,
}

/// Increment one lock-resolver action immediately before each physical shard
/// attempt. Keeping this inside the shardable plan preserves client-go's
/// pre-send metric timing across region regrouping and retries.
pub(crate) struct CountLockResolverAction<P: Plan> {
    pub(crate) inner: P,
    pub(crate) action: &'static str,
}

impl<P: Plan> Clone for CountLockResolverAction<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            action: self.action,
        }
    }
}

#[async_trait]
impl<P: Plan> Plan for CountLockResolverAction<P> {
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        crate::stats::increment_lock_resolver_action(self.action);
        self.inner.execute().await
    }
}

#[async_trait]
impl<P: Plan, Pr: Process<P::Result>> Plan for ProcessResponse<P, Pr> {
    type Result = Pr::Out;

    async fn execute(&self) -> Result<Self::Result> {
        self.processor.process(self.inner.execute().await)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct DefaultProcessor;

pub struct ResolveLock<P: Plan, PdC: PdClient> {
    pub inner: P,
    pub timestamp: Timestamp,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
    pub keyspace: Keyspace,
    pub keyspace_name: Option<String>,
    pub rpc_interceptor: Option<RpcInterceptorChain>,
    pub resource_group_name: Option<String>,
    pub resource_control: Option<ResourceGroupControllerHandle>,
    pub ru_details: Option<Arc<crate::RuDetails>>,
    pub(crate) resolve_locks_context: ResolveLocksContext,
    pub(crate) read_lock_context: Option<ReadLockContext>,
    /// Present only for snapshot reads that enabled runtime statistics.
    pub(crate) snapshot_runtime_stats: Option<Arc<SnapshotRuntimeStats>>,
    /// Snapshot reads own client-go's cumulative `txnLockFast` state.
    pub(crate) snapshot_lock_backoff: Option<SnapshotLockBackoff>,
    /// Leave pair-level locks in the response for scanner-owned point reads.
    pub(crate) response_locks_only: bool,
    /// Prewrite-specific early-conflict behavior from client-go. Normal lock
    /// resolution plans leave this unset.
    pub(crate) prewrite_lock_conflict: Option<PrewriteLockConflict>,
    /// A latest-version point get is blocked only by the first lock it sees;
    /// later locks from different transactions are sent as resolved hints.
    pub(crate) max_timestamp_point_get: bool,
    /// Count the first response of a native concurrent BatchGet shard using
    /// client-go's async callback result labels.
    pub(crate) record_async_batch_get_metric: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PrewriteLockConflict {
    pub(crate) caller_start_ts: u64,
    pub(crate) no_resolve: bool,
    pub(crate) optimistic: bool,
}

impl PrewriteLockConflict {
    fn rejects(self, lock: &kvrpcpb::LockInfo) -> bool {
        self.no_resolve || (self.optimistic && lock.lock_version > self.caller_start_ts)
    }
}

impl<P: Plan, PdC: PdClient> Clone for ResolveLock<P, PdC> {
    fn clone(&self) -> Self {
        ResolveLock {
            inner: self.inner.clone(),
            timestamp: self.timestamp.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
            rpc_interceptor: self.rpc_interceptor.clone(),
            resource_group_name: self.resource_group_name.clone(),
            resource_control: self.resource_control.clone(),
            ru_details: self.ru_details.clone(),
            resolve_locks_context: self.resolve_locks_context.clone(),
            read_lock_context: self.read_lock_context.clone(),
            snapshot_runtime_stats: self.snapshot_runtime_stats.clone(),
            snapshot_lock_backoff: self.snapshot_lock_backoff.clone(),
            response_locks_only: self.response_locks_only,
            prewrite_lock_conflict: self.prewrite_lock_conflict,
            max_timestamp_point_get: self.max_timestamp_point_get,
            record_async_batch_get_metric: self.record_async_batch_get_metric,
        }
    }
}

fn async_batch_get_result(response: &dyn std::any::Any) -> Option<&'static str> {
    let (region_error, key_error, pairs) =
        if let Some(response) = response.downcast_ref::<kvrpcpb::BatchGetResponse>() {
            (&response.region_error, &response.error, &response.pairs)
        } else if let Some(response) = response.downcast_ref::<kvrpcpb::BufferBatchGetResponse>() {
            (&response.region_error, &response.error, &response.pairs)
        } else {
            return None;
        };
    let result = if region_error.is_some() {
        "region_error"
    } else if let Some(error) = key_error {
        if error.locked.is_some() {
            "lock_error"
        } else {
            "other_error"
        }
    } else {
        let mut locked = false;
        let mut other = false;
        for error in pairs.iter().filter_map(|pair| pair.error.as_ref()) {
            if error.locked.is_some() {
                locked = true;
            } else {
                other = true;
                break;
            }
        }
        if other {
            "other_error"
        } else if locked {
            "lock_error"
        } else {
            "ok"
        }
    };
    Some(result)
}

fn record_async_batch_get_result(response: &dyn std::any::Any) {
    if let Some(result) = async_batch_get_result(response) {
        crate::stats::increment_async_batch_get(result);
    }
}

#[async_trait]
impl<P: Plan + Shardable, PdC: PdClient> Plan for ResolveLock<P, PdC>
where
    P::Result: HasLocks + 'static,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut clone = self.clone();
        let mut resolving_locks_guard: Option<ResolvingLocksGuard> = None;
        let mut first_lock_txn_id = None;
        let mut clean_lock_retry_result = None;
        let mut result = match clone.execute_inner().await {
            Ok(result) => result,
            Err(error) => {
                if clone.record_async_batch_get_metric {
                    crate::stats::increment_async_batch_get("other_error");
                }
                return Err(error);
            }
        };
        if clone.record_async_batch_get_metric {
            record_async_batch_get_result(&result);
        }
        loop {
            let mut locks = if clone.response_locks_only {
                result.take_response_locks()
            } else {
                result.take_locks()
            };
            if locks.is_empty() {
                if let Some(clean) = clean_lock_retry_result {
                    result.merge_clean_lock_retry_result(clean);
                }
                return Ok(result);
            }
            // Response keys are logical after API V2 transport decoding, but
            // Rust's sharding and request objects retain physical keys.
            let resolver_locks = locks.clone().encode_keyspace(self.keyspace, KeyMode::Txn);

            if !clone.response_locks_only {
                if let Some(clean) = result.take_clean_result_for_lock_retry() {
                    if clone.inner.retry_only_lock_keys(&resolver_locks) {
                        if let Some(retained) = clean_lock_retry_result.as_mut() {
                            retained.merge_clean_lock_retry_result(clean);
                        } else {
                            clean_lock_retry_result = Some(clean);
                        }
                    }
                }
            }

            if clone.max_timestamp_point_get {
                if let Some(first_lock_txn_id) = first_lock_txn_id {
                    if let Some(read_lock_context) = &clone.read_lock_context {
                        locks.retain(|lock| {
                            if lock.lock_version == first_lock_txn_id {
                                true
                            } else {
                                read_lock_context.add_resolved(lock.lock_version);
                                false
                            }
                        });
                    }
                    if locks.is_empty() {
                        result = clone.execute_inner().await?;
                        continue;
                    }
                } else {
                    first_lock_txn_id = locks.first().map(|lock| lock.lock_version);
                }
            }

            if let Some((policy, lock)) = self.prewrite_lock_conflict.and_then(|policy| {
                locks
                    .iter()
                    .find(|lock| policy.rejects(lock))
                    .map(|lock| (policy, lock))
            }) {
                return Err(crate::error::new_write_conflict_with_args(
                    policy.caller_start_ts,
                    lock.lock_version,
                    0,
                    lock.key.clone(),
                    kvrpcpb::write_conflict::Reason::Optimistic,
                )
                .into());
            }

            if self.backoff.is_none() {
                return Err(Error::ResolveLockError(locks));
            }

            if let Some(guard) = &resolving_locks_guard {
                guard.update(&locks);
            } else {
                resolving_locks_guard = Some(ResolvingLocksGuard::new(
                    self.resolve_locks_context.clone(),
                    &locks,
                    self.timestamp.version(),
                ));
            }

            // Source `KVSnapshot.get` turns a stale read that met a lock
            // into a threshold-free leader read before resolving/retrying it.
            // The clone is the retry owner, so this cannot alter a sibling
            // plan that shares the original immutable `ResolveLock` wrapper.
            clone.disable_stale_read_after_lock();

            let pd_client = self.pd_client.clone();
            let started = self.snapshot_runtime_stats.as_ref().map(|_| Instant::now());
            let lock_result = match &self.read_lock_context {
                Some(read_lock_context) => {
                    resolve_locks_for_read_with_context_result(
                        resolver_locks,
                        self.timestamp.clone(),
                        pd_client.clone(),
                        self.keyspace,
                        self.keyspace_name.as_deref(),
                        self.resolve_locks_context.clone(),
                        read_lock_context,
                    )
                    .await
                }
                None => {
                    resolve_locks_with_context_result(
                        resolver_locks,
                        self.timestamp.clone(),
                        pd_client.clone(),
                        self.keyspace,
                        self.keyspace_name.as_deref(),
                        self.resolve_locks_context.clone(),
                    )
                    .await
                }
            };
            if let (Some(stats), Some(started)) = (&self.snapshot_runtime_stats, started) {
                stats.record_resolve_lock(started.elapsed());
            }
            let lock_result = lock_result?;
            let live_locks = lock_result.live_locks;
            if live_locks.is_empty() {
                result = clone.execute_inner().await?;
            } else if let Some(snapshot_lock_backoff) = clone.snapshot_lock_backoff.as_mut() {
                // client-go only waits when the resolver reports a positive
                // remaining TTL. A zero TTL is retried immediately.
                if lock_result.ms_before_expired > 0 {
                    crate::stats::increment_lock_resolver_action("wait_expired");
                    snapshot_lock_backoff
                        .backoff_with_max_sleep_txn_lock_fast(
                            lock_result.ms_before_expired as u64,
                            "key is locked during snapshot read".to_owned(),
                        )
                        .await?;
                }
                result = clone.execute_inner().await?;
            } else {
                match clone.backoff.next_delay_duration() {
                    None => return Err(Error::ResolveLockError(live_locks)),
                    Some(delay_duration) => {
                        let delay_duration = u64::try_from(lock_result.ms_before_expired)
                            .ok()
                            .map(Duration::from_millis)
                            .map_or(delay_duration, |ttl| delay_duration.min(ttl));
                        if lock_result.ms_before_expired > 0 {
                            crate::stats::increment_lock_resolver_action("wait_expired");
                        }
                        sleep(delay_duration).await;
                        if let Some(stats) = &self.snapshot_runtime_stats {
                            stats.record_backoff("txnLockFast", delay_duration);
                        }
                        result = clone.execute_inner().await?;
                    }
                }
            }
        }
    }
}

impl<P: Plan, PdC: PdClient> ResolveLock<P, PdC> {
    async fn execute_inner(&self) -> Result<P::Result> {
        let mut inner = self.inner.clone();
        if let Some(read_lock_context) = &self.read_lock_context {
            let (resolved_locks, committed_locks) = read_lock_context.snapshot();
            inner.set_read_lock_context(resolved_locks, committed_locks);
        }
        inner.execute().await
    }
}

#[derive(Debug, Default)]
pub struct CleanupLocksResult {
    pub region_error: Option<errorpb::Error>,
    pub key_error: Option<Vec<Error>>,
    pub resolved_locks: usize,
}

impl Clone for CleanupLocksResult {
    fn clone(&self) -> Self {
        Self {
            resolved_locks: self.resolved_locks,
            ..Default::default() // Ignore errors, which should be extracted by `extract_error()`.
        }
    }
}

impl HasRegionError for CleanupLocksResult {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.region_error.take()
    }
}

impl HasKeyErrors for CleanupLocksResult {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.key_error.take()
    }
}

impl Merge<CleanupLocksResult> for Collect {
    type Out = CleanupLocksResult;

    fn merge(&self, input: Vec<Result<CleanupLocksResult>>) -> Result<Self::Out> {
        input
            .into_iter()
            .try_fold(CleanupLocksResult::default(), |acc, x| {
                Ok(CleanupLocksResult {
                    resolved_locks: acc.resolved_locks + x?.resolved_locks,
                    ..Default::default()
                })
            })
    }
}

pub struct CleanupLocks<P: Plan, PdC: PdClient> {
    pub inner: P,
    pub ctx: ResolveLocksContext,
    pub options: ResolveLocksOptions,
    pub store: Option<RegionStore>,
    pub pd_client: Arc<PdC>,
    pub keyspace: Keyspace,
    pub keyspace_name: Option<String>,
    pub rpc_interceptor: Option<RpcInterceptorChain>,
    pub resource_group_name: Option<String>,
    pub resource_control: Option<ResourceGroupControllerHandle>,
    pub backoff: Backoff,
}

impl<P: Plan, PdC: PdClient> Clone for CleanupLocks<P, PdC> {
    fn clone(&self) -> Self {
        CleanupLocks {
            inner: self.inner.clone(),
            ctx: self.ctx.clone(),
            options: self.options,
            store: None,
            pd_client: self.pd_client.clone(),
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
            rpc_interceptor: self.rpc_interceptor.clone(),
            resource_group_name: self.resource_group_name.clone(),
            resource_control: self.resource_control.clone(),
            backoff: self.backoff.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable + NextBatch, PdC: PdClient> Plan for CleanupLocks<P, PdC>
where
    P::Result: HasLocks + HasNextBatch + HasKeyErrors + HasRegionError,
{
    type Result = CleanupLocksResult;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = CleanupLocksResult::default();
        let mut inner = self.inner.clone();
        let mut context = self.ctx.clone();
        context.rpc_interceptor = self.rpc_interceptor.clone();
        context.resource_group_name = self.resource_group_name.clone();
        context.resource_control = self.resource_control.clone();
        let mut lock_resolver = crate::transaction::LockResolver::new(context);
        let region = &self.store.as_ref().unwrap().region_with_leader;
        let mut has_more_batch = true;

        while has_more_batch {
            let mut scan_lock_resp = inner.execute().await?;

            // Propagate errors to `retry_multi_region` for retry.
            if let Some(e) = scan_lock_resp.key_errors() {
                info!("CleanupLocks::execute, inner key errors:{:?}", e);
                result.key_error = Some(e);
                return Ok(result);
            } else if let Some(e) = scan_lock_resp.region_error() {
                info!("CleanupLocks::execute, inner region error:{}", e.message);
                result.region_error = Some(e);
                return Ok(result);
            }

            // Iterate to next batch of inner.
            match scan_lock_resp.has_next_batch() {
                Some((start, end)) => {
                    let start: Vec<u8> = crate::Key::from(start)
                        .encode_keyspace(self.keyspace, KeyMode::Txn)
                        .into();
                    if region.contains(start.as_ref()) {
                        debug!("CleanupLocks::execute, next range:{:?}", (&start, &end));
                        inner.next_batch((start, end));
                    } else {
                        has_more_batch = false;
                    }
                }
                _ => has_more_batch = false,
            }

            let mut locks = scan_lock_resp.take_locks();
            if locks.is_empty() {
                break;
            }
            if locks.len() < self.options.batch_size as usize {
                has_more_batch = false;
            }

            // BEFORE any filter: a shared-lock wrapper's fields (including
            // `use_async_commit`) must not be read — filtering on them would silently
            // drop the real member locks. Refuse instead; see `reject_shared_locks`.
            crate::transaction::reject_shared_locks(&locks)?;
            if self.options.async_commit_only {
                locks = locks
                    .into_iter()
                    .filter(|l| l.use_async_commit)
                    .collect::<Vec<_>>();
            }
            locks = locks.encode_keyspace(self.keyspace, KeyMode::Txn);
            debug!("CleanupLocks::execute, meet locks:{}", locks.len());

            let lock_size = locks.len();
            match lock_resolver
                .cleanup_locks(
                    self.store.clone().unwrap(),
                    locks,
                    self.pd_client.clone(),
                    self.keyspace,
                    self.keyspace_name.as_deref(),
                )
                .await
            {
                Ok(()) => {
                    result.resolved_locks += lock_size;
                }
                Err(Error::ExtractedErrors(mut errors)) => {
                    // Propagate errors to `retry_multi_region` for retry.
                    if let Error::RegionError(e) = errors.pop().unwrap() {
                        result.region_error = Some(*e);
                    } else {
                        result.key_error = Some(errors);
                    }
                    return Ok(result);
                }
                Err(e) => {
                    return Err(e);
                }
            }

            // TODO: improve backoff
            // if self.backoff.is_none() {
            //     return Err(Error::ResolveLockError);
            // }
        }

        Ok(result)
    }
}

/// When executed, the plan extracts errors from its inner plan, and returns an
/// `Err` wrapping the error.
///
/// We usually need to apply this plan if (and only if) the output of the inner
/// plan is of a response type.
///
/// The errors come from two places: `Err` from inner plans, and `Ok(response)`
/// where `response` contains unresolved errors (`error` and `region_error`).
pub struct ExtractError<P: Plan> {
    pub inner: P,
}

impl<P: Plan> Clone for ExtractError<P> {
    fn clone(&self) -> Self {
        ExtractError {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<P: Plan> Plan for ExtractError<P>
where
    P::Result: HasKeyErrors + HasRegionErrors,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = self.inner.execute().await?;
        if let Some(errors) = result.key_errors() {
            Err(Error::ExtractedErrors(errors))
        } else if let Some(errors) = result.region_errors() {
            Err(Error::ExtractedErrors(
                errors
                    .into_iter()
                    .map(|e| Error::RegionError(Box::new(e)))
                    .collect(),
            ))
        } else {
            Ok(result)
        }
    }
}

/// When executed, the plan clones the shard and execute its inner plan, then
/// returns `(shard, response)`.
///
/// It's useful when the information of shard are lost in the response but needed
/// for processing.
pub struct PreserveShard<P: Plan + Shardable> {
    pub inner: P,
    pub shard: Option<P::Shard>,
}

impl<P: Plan + Shardable> Clone for PreserveShard<P> {
    fn clone(&self) -> Self {
        PreserveShard {
            inner: self.inner.clone(),
            shard: None,
        }
    }
}

#[async_trait]
impl<P> Plan for PreserveShard<P>
where
    P: Plan + Shardable,
{
    type Result = ResponseWithShard<P::Result, P::Shard>;

    async fn execute(&self) -> Result<Self::Result> {
        let res = self.inner.execute().await?;
        let shard = self
            .shard
            .as_ref()
            .expect("Unreachable: Shardable::apply_shard() is not called before executing PreserveShard")
            .clone();
        Ok(ResponseWithShard(res, shard))
    }
}

// contains a response and the corresponding shards
#[derive(Debug, Clone)]
pub struct ResponseWithShard<Resp, Shard>(pub Resp, pub Shard);

impl<Resp: HasKeyErrors, Shard> HasKeyErrors for ResponseWithShard<Resp, Shard> {
    fn key_errors(&mut self) -> Option<Vec<Error>> {
        self.0.key_errors()
    }
}

impl<Resp: HasLocks, Shard> HasLocks for ResponseWithShard<Resp, Shard> {
    fn take_locks(&mut self) -> Vec<kvrpcpb::LockInfo> {
        self.0.take_locks()
    }
}

impl<Resp: HasRegionError, Shard> HasRegionError for ResponseWithShard<Resp, Shard> {
    fn region_error(&mut self) -> Option<errorpb::Error> {
        self.0.region_error()
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures::stream::BoxStream;
    use futures::stream::{self};
    use tokio::sync::Barrier;

    use super::*;

    #[test]
    fn source_async_batch_get_result_labels_match_callback_branch() {
        assert_eq!(
            async_batch_get_result(&kvrpcpb::BatchGetResponse::default()),
            Some("ok")
        );
        assert_eq!(
            async_batch_get_result(&kvrpcpb::BatchGetResponse {
                region_error: Some(crate::proto::errorpb::Error::default()),
                ..Default::default()
            }),
            Some("region_error")
        );
        assert_eq!(
            async_batch_get_result(&kvrpcpb::BatchGetResponse {
                pairs: vec![kvrpcpb::KvPair {
                    error: Some(kvrpcpb::KeyError {
                        locked: Some(kvrpcpb::LockInfo::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Some("lock_error")
        );
        assert_eq!(
            async_batch_get_result(&kvrpcpb::BufferBatchGetResponse {
                error: Some(kvrpcpb::KeyError {
                    abort: "abort".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            Some("other_error")
        );
    }
    use crate::backoff::Backoff;
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::proto::kvrpcpb;
    use crate::proto::kvrpcpb::BatchGetResponse;
    use crate::request::PlanBuilder;
    use crate::store::Request;

    fn region_store() -> RegionStore {
        RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()))
    }

    #[tokio::test]
    async fn physical_dispatch_collects_task_scoped_network_traffic() {
        let observed_request_size = Arc::new(AtomicU64::new(0));
        let hook_size = observed_request_size.clone();
        let client = MockKvClient::with_dispatch_hook(move |request| {
            let request = request.downcast_ref::<kvrpcpb::GetRequest>().unwrap();
            hook_size.store(request.network_request_size(), Ordering::SeqCst);
            Ok(Box::new(kvrpcpb::GetResponse {
                value: b"value".to_vec(),
                ..Default::default()
            }))
        });
        let details = Arc::new(crate::traffic::NetworkTrafficDetails::default());
        let captured = details.clone();
        crate::traffic::with_network_traffic_details(details.clone(), async move {
            let route = RegionStore::new(MockPdClient::region1(), Arc::new(client))
                .with_resource_control_access_location(
                    "zone-a",
                    &crate::proto::metapb::Store {
                        labels: vec![crate::proto::metapb::StoreLabel {
                            key: "zone".to_owned(),
                            value: "zone-b".to_owned(),
                        }],
                        ..Default::default()
                    },
                )
                .with_stale_read(true);
            PlanBuilder::new(
                Arc::new(MockPdClient::default()),
                Keyspace::Disable,
                kvrpcpb::GetRequest {
                    key: b"key".to_vec(),
                    ..Default::default()
                },
            )
            .replica_read(crate::kv::ReplicaReadConfig {
                stale_read: true,
                ..Default::default()
            })
            .single_region_with_store(route)
            .await
            .unwrap()
            .plan()
            .execute()
            .await
            .unwrap();
        })
        .await;

        let snapshot = captured.snapshot();
        assert_eq!(
            snapshot.sent_kv_total,
            observed_request_size.load(Ordering::SeqCst) as i64
        );
        assert_eq!(snapshot.sent_kv_cross_zone, snapshot.sent_kv_total);
        assert_eq!(snapshot.received_kv_total, 7);
        assert_eq!(snapshot.received_kv_cross_zone, 7);
    }

    #[tokio::test]
    async fn successful_physical_dispatch_updates_source_ru_v2_rpc_counts() {
        let client = MockKvClient::with_dispatch_hook(|request| {
            assert!(request.is::<kvrpcpb::GetRequest>());
            Ok(Box::new(kvrpcpb::GetResponse {
                exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                    ru_v2: Some(kvrpcpb::Ruv2 {
                        storage_processed_keys_get: 3,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });
        let details = Arc::new(crate::RuDetails::new());
        let response = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::GetRequest::default(),
        )
        .ru_details(details.clone())
        .single_region_with_store(RegionStore::new(MockPdClient::region1(), Arc::new(client)))
        .await
        .unwrap()
        .plan()
        .execute()
        .await
        .unwrap();

        let response_ru = response.exec_details_v2.unwrap().ru_v2.unwrap();
        assert_eq!(response_ru.read_rpc_count, 1);
        assert_eq!(response_ru.write_rpc_count, 0);
        let accumulated = details.drain_ru_v2().unwrap();
        assert_eq!(accumulated.read_rpc_count, 1);
        assert_eq!(accumulated.storage_processed_keys_get, 3);

        let client = MockKvClient::with_dispatch_hook(|request| {
            assert!(request.is::<kvrpcpb::PrewriteRequest>());
            Ok(Box::new(kvrpcpb::PrewriteResponse {
                exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                    ru_v2: Some(kvrpcpb::Ruv2::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });
        let details = Arc::new(crate::RuDetails::new());
        let response = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::PrewriteRequest::default(),
        )
        .ru_details(details.clone())
        .single_region_with_store(RegionStore::new(MockPdClient::region1(), Arc::new(client)))
        .await
        .unwrap()
        .plan()
        .execute()
        .await
        .unwrap();
        let response_ru = response.exec_details_v2.unwrap().ru_v2.unwrap();
        assert_eq!(response_ru.read_rpc_count, 0);
        assert_eq!(response_ru.write_rpc_count, 1);
        assert_eq!(details.drain_ru_v2().unwrap().write_rpc_count, 1);
    }

    #[tokio::test]
    async fn source_ru_v2_skips_non_tikv_endpoints() {
        let client = MockKvClient::with_dispatch_hook(|request| {
            assert!(request.is::<kvrpcpb::GetRequest>());
            Ok(Box::new(kvrpcpb::GetResponse {
                exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                    ru_v2: Some(kvrpcpb::Ruv2::default()),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });
        let details = Arc::new(crate::RuDetails::new());
        let store = RegionStore::new(MockPdClient::region1(), Arc::new(client))
            .with_physical_store(41, crate::store::EndpointType::TiFlash);
        let response = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::GetRequest::default(),
        )
        .ru_details(details.clone())
        .single_region_with_store(store)
        .await
        .unwrap()
        .plan()
        .execute()
        .await
        .unwrap();

        assert_eq!(
            response
                .exec_details_v2
                .unwrap()
                .ru_v2
                .unwrap()
                .read_rpc_count,
            0
        );
        assert!(details.drain_ru_v2().is_none());
    }

    #[tokio::test]
    async fn source_ru_v2_skips_internal_bypass_requests() {
        let client = MockKvClient::with_dispatch_hook(|request| {
            assert!(request.is::<kvrpcpb::GetRequest>());
            Ok(Box::new(kvrpcpb::GetResponse {
                exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                    ru_v2: Some(kvrpcpb::Ruv2 {
                        storage_processed_keys_get: 3,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }))
        });
        let details = Arc::new(crate::RuDetails::new());
        let request = kvrpcpb::GetRequest {
            context: Some(kvrpcpb::Context {
                request_source: "internal_others".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            request,
        )
        .ru_details(details.clone())
        .single_region_with_store(RegionStore::new(MockPdClient::region1(), Arc::new(client)))
        .await
        .unwrap()
        .plan()
        .execute()
        .await
        .unwrap();

        assert_eq!(
            response
                .exec_details_v2
                .unwrap()
                .ru_v2
                .unwrap()
                .read_rpc_count,
            0
        );
        assert!(details.drain_ru_v2().is_none());
    }

    #[tokio::test]
    async fn resolve_lock_observer_is_removed_when_the_retry_future_is_cancelled() {
        let check_status_sent = Arc::new(tokio::sync::Notify::new());
        let check_status_sent_by_hook = Arc::clone(&check_status_sent);
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn std::any::Any| {
                if request.is::<kvrpcpb::GetRequest>() {
                    return Ok(Box::new(kvrpcpb::GetResponse {
                        error: Some(kvrpcpb::KeyError {
                            locked: Some(kvrpcpb::LockInfo {
                                key: b"locked".to_vec(),
                                primary_lock: b"primary".to_vec(),
                                lock_version: 1,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn std::any::Any>);
                }
                if request.is::<kvrpcpb::CheckTxnStatusRequest>() {
                    check_status_sent_by_hook.notify_one();
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        lock_ttl: 60_000,
                        lock_info: Some(kvrpcpb::LockInfo {
                            key: b"locked".to_vec(),
                            primary_lock: b"primary".to_vec(),
                            lock_version: 1,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn std::any::Any>);
                }
                panic!("unexpected request while observing lock retry");
            },
        )));
        let context = ResolveLocksContext::default();
        let plan = PlanBuilder::new(
            client,
            Keyspace::Disable,
            kvrpcpb::GetRequest {
                key: b"locked".to_vec(),
                ..Default::default()
            },
        )
        .resolve_lock_with_context(
            Timestamp::from_version(7),
            Backoff::no_jitter_backoff(1_000, 1_000, 2),
            Keyspace::Disable,
            context.clone(),
        )
        .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 1))
        .plan();
        let task = tokio::spawn(async move { plan.execute().await });

        tokio::time::timeout(Duration::from_secs(1), check_status_sent.notified())
            .await
            .expect("lock resolver should begin status lookup");
        assert_eq!(
            context.resolving_locks().await,
            vec![crate::transaction::ResolvingLock {
                txn_id: 7,
                lock_txn_id: 1,
                key: b"locked".to_vec(),
                primary: b"primary".to_vec(),
            }]
        );

        task.abort();
        let _ = task.await;
        assert!(context.resolving_locks().await.is_empty());
    }

    #[tokio::test]
    async fn api_v2_decoded_lock_is_reencoded_before_status_lookup() {
        let codec = crate::request::ApiV2Codec::new(crate::request::KeyMode::Txn, 0).unwrap();
        let get_count = Arc::new(AtomicUsize::new(0));
        let get_count_by_hook = Arc::clone(&get_count);
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn std::any::Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    assert_eq!(request.key, codec.encode_key(b"locked"));
                    if get_count_by_hook.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Ok(Box::new(kvrpcpb::GetResponse {
                            error: Some(kvrpcpb::KeyError {
                                locked: Some(kvrpcpb::LockInfo {
                                    key: codec.encode_key(b"locked"),
                                    primary_lock: codec.encode_key(b"locked"),
                                    lock_version: 1,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }) as Box<dyn std::any::Any>);
                    }
                    return Ok(Box::<kvrpcpb::GetResponse>::default() as Box<dyn std::any::Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    assert_eq!(request.primary_key, codec.encode_key(b"locked"));
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn std::any::Any>);
                }
                panic!("unexpected API V2 lock-resolution request");
            },
        )));
        let keyspace = Keyspace::Enable { keyspace_id: 0 };
        let response = PlanBuilder::new(
            client,
            keyspace,
            kvrpcpb::GetRequest {
                key: codec.encode_key(b"locked"),
                ..Default::default()
            },
        )
        .resolve_lock(
            Timestamp::from_version(7),
            Backoff::no_jitter_backoff(0, 0, 1),
            keyspace,
        )
        .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 1))
        .plan()
        .execute()
        .await
        .unwrap();

        assert_eq!(response.len(), 1);
        assert!(response
            .into_iter()
            .next()
            .unwrap()
            .unwrap()
            .error
            .is_none());
        assert_eq!(get_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn api_v2_batch_get_retries_decoded_lock_keys_in_physical_form() {
        let codec = crate::request::ApiV2Codec::new(crate::request::KeyMode::Txn, 0).unwrap();
        let batch_count = Arc::new(AtomicUsize::new(0));
        let batch_count_by_hook = Arc::clone(&batch_count);
        let client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn std::any::Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    let attempt = batch_count_by_hook.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        assert_eq!(
                            request.keys,
                            [codec.encode_key(b"bar"), codec.encode_key(b"locked")]
                        );
                        return Ok(Box::new(kvrpcpb::BatchGetResponse {
                            pairs: vec![
                                kvrpcpb::KvPair {
                                    key: codec.encode_key(b"bar"),
                                    value: b"clean".to_vec(),
                                    ..Default::default()
                                },
                                kvrpcpb::KvPair {
                                    key: codec.encode_key(b"locked"),
                                    error: Some(kvrpcpb::KeyError {
                                        locked: Some(kvrpcpb::LockInfo {
                                            key: codec.encode_key(b"locked"),
                                            primary_lock: codec.encode_key(b"locked"),
                                            lock_version: 1,
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ],
                            ..Default::default()
                        }) as Box<dyn std::any::Any>);
                    }
                    assert_eq!(request.keys, [codec.encode_key(b"locked")]);
                    return Ok(Box::new(kvrpcpb::BatchGetResponse {
                        pairs: vec![kvrpcpb::KvPair {
                            key: codec.encode_key(b"locked"),
                            value: b"resolved".to_vec(),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }) as Box<dyn std::any::Any>);
                }
                if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                    assert_eq!(request.primary_key, codec.encode_key(b"locked"));
                    return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                        commit_version: 2,
                        ..Default::default()
                    }) as Box<dyn std::any::Any>);
                }
                panic!("unexpected API V2 batch-get lock-resolution request");
            },
        )));
        let keyspace = Keyspace::Enable { keyspace_id: 0 };
        let response = PlanBuilder::new(
            client,
            keyspace,
            kvrpcpb::BatchGetRequest {
                keys: vec![codec.encode_key(b"bar"), codec.encode_key(b"locked")],
                ..Default::default()
            },
        )
        .resolve_lock(
            Timestamp::from_version(7),
            Backoff::no_jitter_backoff(0, 0, 1),
            keyspace,
        )
        .retry_multi_region(Backoff::no_jitter_backoff(0, 0, 1))
        .plan()
        .execute()
        .await
        .unwrap();

        assert_eq!(response.len(), 1);
        let response = response.into_iter().next().unwrap().unwrap();
        assert_eq!(
            response
                .pairs
                .into_iter()
                .map(|pair| (pair.key, pair.value))
                .collect::<Vec<_>>(),
            [
                (b"bar".to_vec(), b"clean".to_vec()),
                (b"locked".to_vec(), b"resolved".to_vec()),
            ]
        );
        assert_eq!(batch_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn region_error_actions_preserve_client_go_retry_classes() {
        let pd_client = Arc::new(MockPdClient::default());

        let busy = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                server_is_busy: Some(Default::default()),
                ..Default::default()
            },
            region_store(),
        )
        .await
        .unwrap();
        assert_eq!(busy, RegionErrorRetry::Backoff(BO_TIKV_SERVER_BUSY));

        let health = Arc::new(crate::locate::StoreHealthStatus::default());
        let estimated_busy = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                server_is_busy: Some(errorpb::ServerIsBusy {
                    estimated_wait_ms: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
            region_store().with_health_status(health.clone()),
        )
        .await
        .unwrap();
        assert_eq!(
            estimated_busy,
            RegionErrorRetry::Backoff(BO_TIKV_SERVER_BUSY)
        );
        assert!(!health.is_slow());

        handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                server_is_busy: Some(Default::default()),
                ..Default::default()
            },
            region_store().with_health_status(health.clone()),
        )
        .await
        .unwrap();
        assert!(health.is_slow());

        let stale = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                stale_command: Some(Default::default()),
                ..Default::default()
            },
            region_store(),
        )
        .await
        .unwrap();
        assert_eq!(stale, RegionErrorRetry::Backoff(BO_STALE_CMD));

        let flashback = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                flashback_in_progress: Some(errorpb::FlashbackInProgress {
                    region_id: 42,
                    flashback_start_ts: 99,
                }),
                ..Default::default()
            },
            region_store(),
        )
        .await;
        assert!(matches!(
            flashback,
            Err(Error::StringError(message))
                if message == "region 42 is in flashback progress, FlashbackStartTS is 99"
        ));

        let not_prepared = handle_region_error(
            pd_client,
            errorpb::Error {
                flashback_not_prepared: Some(errorpb::FlashbackNotPrepared { region_id: 43 }),
                ..Default::default()
            },
            region_store(),
        )
        .await;
        assert!(matches!(
            not_prepared,
            Err(Error::StringError(message)) if message == "region 43 is not prepared for the flashback"
        ));

        let raft_entry = handle_region_error(
            Arc::new(MockPdClient::default()),
            errorpb::Error {
                raft_entry_too_large: Some(Default::default()),
                ..Default::default()
            },
            region_store(),
        )
        .await;
        assert!(matches!(raft_entry, Err(Error::StringError(_))));

        let invalid_max_ts = handle_region_error(
            Arc::new(MockPdClient::default()),
            errorpb::Error {
                message: "invalid max_ts update from peer".to_owned(),
                ..Default::default()
            },
            region_store(),
        )
        .await;
        assert!(matches!(invalid_max_ts, Err(Error::StringError(_))));
    }

    #[tokio::test]
    async fn source_region_error_handler_preserves_mixed_field_precedence() {
        let pd_client = Arc::new(MockPdClient::default());
        let undetermined = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                undetermined_result: Some(Default::default()),
                not_leader: Some(errorpb::NotLeader {
                    leader: Some(crate::proto::metapb::Peer {
                        id: 99,
                        store_id: 99,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            region_store(),
        )
        .await;
        assert!(
            matches!(undetermined, Err(Error::RegionError(error)) if error.undetermined_result.is_some())
        );
        assert!(pd_client.invalidated_regions().is_empty());

        let busy_before_stale = handle_region_error(
            Arc::new(MockPdClient::default()),
            errorpb::Error {
                server_is_busy: Some(Default::default()),
                stale_command: Some(Default::default()),
                ..Default::default()
            },
            region_store(),
        )
        .await
        .unwrap();
        assert_eq!(
            busy_before_stale,
            RegionErrorRetry::Backoff(BO_TIKV_SERVER_BUSY)
        );

        let pd_client = Arc::new(MockPdClient::default());
        let epoch_before_store = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                epoch_not_match: Some(Default::default()),
                store_not_match: Some(Default::default()),
                ..Default::default()
            },
            region_store().with_target("tikv-a"),
        )
        .await;
        assert!(
            matches!(epoch_before_store, Err(Error::RegionError(error)) if error.epoch_not_match.is_some())
        );
        assert!(pd_client.closed_client_addresses().is_empty());
    }

    #[tokio::test]
    async fn source_store_identity_errors_stop_the_current_send_loop() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = region_store().with_target("tikv-a");
        let ver_id = store.region_with_leader.ver_id();
        let result = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                store_not_match: Some(Default::default()),
                ..Default::default()
            },
            store,
        )
        .await;
        assert!(matches!(result, Err(Error::RegionError(_))));
        assert_eq!(pd_client.invalidated_regions(), vec![ver_id]);
        assert_eq!(pd_client.closed_client_addresses(), vec!["tikv-a"]);

        let pd_client = Arc::new(MockPdClient::default());
        let store = region_store()
            .with_target("proxy-a")
            .with_forwarded_host("tikv-a");
        let result = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                store_not_match: Some(Default::default()),
                ..Default::default()
            },
            store,
        )
        .await;
        assert!(matches!(result, Err(Error::RegionError(_))));
        assert_eq!(pd_client.closed_client_addresses(), vec!["tikv-a"]);

        let pd_client = Arc::new(MockPdClient::default());
        let store = region_store().with_target("tikv-a");
        let ver_id = store.region_with_leader.ver_id();
        let result = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                mismatch_peer_id: Some(Default::default()),
                ..Default::default()
            },
            store,
        )
        .await;
        assert!(matches!(result, Err(Error::RegionError(_))));
        assert_eq!(pd_client.invalidated_regions(), vec![ver_id]);
        assert!(pd_client.closed_client_addresses().is_empty());
    }

    #[tokio::test]
    async fn source_terminal_region_errors_do_not_retry_the_send_loop() {
        for (error, expected) in [
            (
                errorpb::Error {
                    recovery_in_progress: Some(Default::default()),
                    ..Default::default()
                },
                Some(RegionErrorRetry::TerminalAfterBackoff(
                    BO_REGION_RECOVERY_IN_PROGRESS,
                )),
            ),
            (
                errorpb::Error {
                    is_witness: Some(Default::default()),
                    ..Default::default()
                },
                Some(RegionErrorRetry::TerminalAfterBackoff(BO_IS_WITNESS)),
            ),
            (
                errorpb::Error {
                    key_not_in_region: Some(Default::default()),
                    ..Default::default()
                },
                None,
            ),
            (
                errorpb::Error {
                    region_not_found: Some(Default::default()),
                    ..Default::default()
                },
                None,
            ),
            // TiKV creates a replica selector for every TiKV request. An
            // unclassified error therefore immediately advances selection,
            // without invalidating the current cache entry.
            (errorpb::Error::default(), Some(RegionErrorRetry::Immediate)),
        ] {
            let pd_client = Arc::new(MockPdClient::default());
            let store = region_store();
            let ver_id = store.region_with_leader.ver_id();
            let is_unknown = error == errorpb::Error::default();
            let result = handle_region_error(pd_client.clone(), error, store).await;
            match expected {
                Some(expected) => assert_eq!(result.unwrap(), expected),
                None => assert!(matches!(result, Err(Error::RegionError(_)))),
            }
            if is_unknown {
                assert!(pd_client.invalidated_regions().is_empty());
            } else {
                assert_eq!(pd_client.invalidated_regions(), vec![ver_id]);
            }
        }
    }

    #[test]
    fn source_server_busy_fast_retry_keeps_healthy_leader_backoff() {
        let region = region_store();
        let mut config = ReplicaReadConfig::default();
        let state = ReplicaSelectorState::default();
        assert!(!source_fast_server_busy_retry(
            &config, &state, &region, true, false, 0
        ));

        config.read_type = crate::kv::ReplicaReadType::Follower;
        assert!(source_fast_server_busy_retry(
            &config, &state, &region, true, false, 0
        ));
        config.read_type = crate::kv::ReplicaReadType::Mixed;
        assert!(source_fast_server_busy_retry(
            &config, &state, &region, true, false, 0
        ));
        config.read_type = crate::kv::ReplicaReadType::PreferLeader;
        assert!(source_fast_server_busy_retry(
            &config, &state, &region, true, false, 0
        ));

        config.read_type = crate::kv::ReplicaReadType::Leader;
        config.busy_threshold_ms = 10;
        assert!(source_fast_server_busy_retry(
            &config, &state, &region, true, false, 1
        ));
        assert!(!source_fast_server_busy_retry(
            &config, &state, &region, true, true, 1
        ));
        assert!(source_batched_coprocessor_busy_is_terminal(
            &config, true, true, 1
        ));
        assert!(!source_batched_coprocessor_busy_is_terminal(
            &config, true, false, 1
        ));
        assert!(source_fast_server_busy_retry(
            &config,
            &state,
            &region.clone().with_force_leader_read(),
            true,
            false,
            0
        ));

        let leader_id = region.region_with_leader.leader.as_ref().unwrap().id;
        let mut exhausted = ReplicaSelectorState::default();
        for _ in 0..10 {
            exhausted.record_attempt(leader_id);
        }
        config.busy_threshold_ms = 0;
        assert!(source_fast_server_busy_retry(
            &config, &exhausted, &region, true, false, 0
        ));

        let mut suspect = ReplicaSelectorState::default();
        suspect.record_busy_leader(leader_id);
        suspect.record_busy_leader(leader_id);
        assert!(source_fast_server_busy_retry(
            &config, &suspect, &region, true, false, 0
        ));

        assert!(source_fast_selector_retry(
            &errorpb::Error {
                stale_command: Some(Default::default()),
                ..Default::default()
            },
            false
        ));
        assert!(!source_fast_selector_retry(
            &errorpb::Error::default(),
            false
        ));
    }

    #[test]
    fn source_request_cancellation_is_terminal_without_store_failure_handling() {
        let cancelled = Error::GrpcAPI(tonic::Status::cancelled("context canceled"));
        assert!(!is_request_cancelled_error(&cancelled, false));
        assert!(is_request_cancelled_error(&cancelled, true));
        assert!(is_request_cancelled_error(
            &Error::Connection {
                source: Box::new(cancelled),
                address: "store-1".to_owned(),
                version: 7,
            },
            true
        ));
        assert!(is_request_cancelled_error(
            &Error::StringError("context canceled".to_owned()),
            false
        ));
        assert!(!is_request_cancelled_error(
            &Error::GrpcAPI(tonic::Status::deadline_exceeded("deadline")),
            true
        ));

        let cancellation = Cancellation::default();
        let state = RetryBackoffer::new(cancellation.clone(), 100);
        assert!(!RegionRetryState::is_cancelled(&state));
        cancellation.cancel();
        assert!(RegionRetryState::is_cancelled(&state));
    }

    #[test]
    fn source_transport_failure_uses_tiflash_retry_class_only_for_tiflash_endpoints() {
        let region = region_store();
        assert_eq!(source_transport_backoff_config(None), BO_TIKV_RPC);
        assert_eq!(source_transport_backoff_config(Some(&region)), BO_TIKV_RPC);
        let tiflash = region.with_physical_store(1, crate::store::EndpointType::TiFlash);
        assert_eq!(
            source_transport_backoff_config(Some(&tiflash)),
            BO_TIFLASH_RPC
        );
        let tiflash_compute =
            region_store().with_physical_store(1, crate::store::EndpointType::TiFlashCompute);
        assert_eq!(
            source_transport_backoff_config(Some(&tiflash_compute)),
            BO_TIFLASH_RPC
        );
    }

    #[test]
    fn source_server_busy_uses_tiflash_retry_class_only_for_tiflash_endpoints() {
        let region = region_store();
        assert_eq!(
            source_server_busy_backoff_config(&region),
            BO_TIKV_SERVER_BUSY
        );
        let tiflash = region.with_physical_store(1, crate::store::EndpointType::TiFlash);
        assert_eq!(
            source_server_busy_backoff_config(&tiflash),
            BO_TIFLASH_SERVER_BUSY
        );
        let tiflash_compute =
            region_store().with_physical_store(1, crate::store::EndpointType::TiFlashCompute);
        assert_eq!(
            source_server_busy_backoff_config(&tiflash_compute),
            BO_TIFLASH_SERVER_BUSY
        );
    }

    #[tokio::test]
    async fn source_epoch_not_match_installs_replacements_from_responding_store() {
        let pd_client = Arc::new(MockPdClient::default());
        let mut replacement = MockPdClient::region2().region;
        replacement.id = 9;
        replacement.region_epoch = Some(crate::proto::metapb::RegionEpoch {
            conf_ver: 4,
            version: 5,
        });
        replacement.peers = vec![crate::proto::metapb::Peer {
            id: 7,
            store_id: 41,
            ..Default::default()
        }];
        let store = region_store().with_target_peer(replacement.peers[0].clone());
        let mut store = store;
        store.region_with_leader.buckets = Some(crate::proto::metapb::Buckets {
            region_id: 1,
            version: 3,
            keys: vec![vec![], vec![9]],
            ..Default::default()
        });
        let old_ver_id = store.region_with_leader.ver_id();

        assert_eq!(
            on_region_epoch_not_match(
                pd_client.clone(),
                store,
                EpochNotMatch {
                    current_regions: vec![replacement],
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
            EpochNotMatchOutcome::Stop
        );

        let installed = pd_client.epoch_not_match_regions();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].id(), 9);
        assert_eq!(installed[0].leader.as_ref().map(|peer| peer.id), Some(7));
        assert_eq!(installed[0].buckets.as_ref().unwrap().version, 3);
        assert_eq!(pd_client.invalidated_regions(), vec![old_ver_id]);
    }

    #[tokio::test]
    async fn source_epoch_not_match_keeps_an_exact_cached_version() {
        let pd_client = Arc::new(MockPdClient::default());
        let mut replacement = MockPdClient::region1().region;
        replacement.peers = vec![crate::proto::metapb::Peer {
            id: 7,
            store_id: 41,
            ..Default::default()
        }];
        let store = region_store().with_target_peer(replacement.peers[0].clone());

        assert_eq!(
            on_region_epoch_not_match(
                pd_client.clone(),
                store,
                EpochNotMatch {
                    current_regions: vec![replacement],
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
            EpochNotMatchOutcome::Stop
        );

        assert!(pd_client.invalidated_regions().is_empty());
        assert_eq!(pd_client.epoch_not_match_regions().len(), 1);
    }

    #[tokio::test]
    async fn source_epoch_not_match_retries_only_when_tikv_is_behind() {
        let pd_client = Arc::new(MockPdClient::default());
        let mut store = region_store();
        store
            .region_with_leader
            .region
            .region_epoch
            .as_mut()
            .unwrap()
            .version = 2;
        let mut stale = store.region_with_leader.region.clone();
        stale.region_epoch.as_mut().unwrap().version = 1;

        assert_eq!(
            on_region_epoch_not_match(
                pd_client.clone(),
                store.clone(),
                EpochNotMatch {
                    current_regions: vec![stale],
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
            EpochNotMatchOutcome::RetryAfterBackoff
        );
        assert!(pd_client.invalidated_regions().is_empty());

        let ver_id = store.region_with_leader.ver_id();
        assert_eq!(
            on_region_epoch_not_match(pd_client.clone(), store, EpochNotMatch::default(),)
                .await
                .unwrap(),
            EpochNotMatchOutcome::Stop
        );
        assert_eq!(pd_client.invalidated_regions(), vec![ver_id]);
    }

    #[tokio::test]
    async fn source_bucket_version_mismatch_refreshes_the_cache_and_propagates() {
        let pd_client = Arc::new(MockPdClient::default());
        let store = region_store();
        let ver_id = store.region_with_leader.ver_id();

        let error = handle_region_error(
            pd_client.clone(),
            errorpb::Error {
                bucket_version_not_match: Some(errorpb::BucketVersionNotMatch {
                    version: 5,
                    keys: vec![vec![], vec![1]],
                }),
                ..Default::default()
            },
            store,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error::RegionError(_)));
        assert_eq!(
            pd_client.bucket_updates(),
            vec![(ver_id, 5, vec![vec![], vec![1]])]
        );
    }

    #[derive(Clone)]
    struct ErrPlan;

    #[derive(Clone)]
    struct RecordingRetryState {
        forks: Arc<AtomicUsize>,
        merges: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl RegionRetryState for RecordingRetryState {
        async fn backoff(&mut self, _config: RetryConfig, _reason: String) -> Result<bool> {
            Ok(true)
        }

        fn fork(&self) -> (Self, Cancellation) {
            self.forks.fetch_add(1, Ordering::SeqCst);
            (self.clone(), Cancellation::default())
        }

        fn update_using_forked(&mut self, _forked: &Self) {
            self.merges.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Clone)]
    struct TwoShardPlan;

    #[derive(Clone)]
    struct CancellationProbeRetryState {
        cancellation: Cancellation,
        cancellation_seen: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl RegionRetryState for CancellationProbeRetryState {
        async fn backoff(&mut self, _config: RetryConfig, _reason: String) -> Result<bool> {
            tokio::select! {
                _ = self.cancellation.cancelled() => {
                    self.cancellation_seen.fetch_add(1, Ordering::SeqCst);
                    Err(Error::StringError("sibling retry cancelled".to_owned()))
                }
                _ = sleep(Duration::from_secs(1)) => Ok(true),
            }
        }

        fn fork(&self) -> (Self, Cancellation) {
            let cancellation = self.cancellation.child();
            (
                Self {
                    cancellation: cancellation.clone(),
                    cancellation_seen: self.cancellation_seen.clone(),
                },
                cancellation,
            )
        }

        fn update_using_forked(&mut self, _forked: &Self) {}
    }

    #[derive(Clone)]
    struct CancellationProbePlan {
        fails_immediately: bool,
        dispatched: Arc<Barrier>,
    }

    #[async_trait]
    impl Plan for CancellationProbePlan {
        type Result = BatchGetResponse;

        async fn execute(&self) -> Result<Self::Result> {
            self.dispatched.wait().await;
            if self.fails_immediately {
                Err(Error::Unimplemented)
            } else {
                Ok(BatchGetResponse {
                    region_error: Some(errorpb::Error {
                        // Use a source-retryable error that retains a
                        // backoff under replica selection, so the sibling
                        // cancellation test observes an actual waiting task.
                        max_timestamp_not_synced: Some(Default::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
            }
        }
    }

    #[async_trait]
    impl Plan for TwoShardPlan {
        type Result = BatchGetResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Ok(BatchGetResponse::default())
        }
    }

    impl Shardable for TwoShardPlan {
        type Shard = ();

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            Box::pin(stream::iter(vec![
                Ok(((), MockPdClient::region1())),
                Ok(((), MockPdClient::region2())),
            ]))
        }

        fn apply_shard(&mut self, _: Self::Shard) {}

        fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
            Ok(())
        }
    }

    impl Shardable for CancellationProbePlan {
        type Shard = bool;

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            Box::pin(stream::iter(vec![
                Ok((true, MockPdClient::region1())),
                Ok((false, MockPdClient::region2())),
            ]))
        }

        fn apply_shard(&mut self, fails_immediately: Self::Shard) {
            self.fails_immediately = fails_immediately;
        }

        fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl Plan for ErrPlan {
        type Result = BatchGetResponse;

        async fn execute(&self) -> Result<Self::Result> {
            Err(Error::Unimplemented)
        }
    }

    impl Shardable for ErrPlan {
        type Shard = ();

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            Box::pin(stream::iter(1..=3).map(|_| Err(Error::Unimplemented))).boxed()
        }

        fn apply_shard(&mut self, _: Self::Shard) {}

        fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct ConfigurableTimeoutPlan {
        duration_ms: u64,
    }

    #[async_trait]
    impl Plan for ConfigurableTimeoutPlan {
        type Result = BatchGetResponse;

        async fn execute(&self) -> Result<Self::Result> {
            unreachable!("configurable-timeout gate test does not dispatch")
        }
    }

    impl Shardable for ConfigurableTimeoutPlan {
        type Shard = ();

        fn shards(
            &self,
            _: &Arc<impl crate::pd::PdClient>,
        ) -> BoxStream<'static, crate::Result<(Self::Shard, RegionWithLeader)>> {
            Box::pin(stream::empty())
        }

        fn apply_shard(&mut self, _: Self::Shard) {}

        fn apply_store(&mut self, _: &crate::store::RegionStore) -> Result<()> {
            Ok(())
        }

        fn is_read_request(&self) -> bool {
            true
        }

        fn max_execution_duration_ms(&self) -> u64 {
            self.duration_ms
        }
    }

    #[test]
    fn source_configurable_read_timeout_is_below_read_timeout_short() {
        assert!(source_configurable_read_timeout(&ConfigurableTimeoutPlan {
            duration_ms: 0,
        }));
        assert!(source_configurable_read_timeout(&ConfigurableTimeoutPlan {
            duration_ms: 29_999,
        }));
        assert!(!source_configurable_read_timeout(
            &ConfigurableTimeoutPlan {
                duration_ms: 30_000,
            }
        ));
    }

    #[test]
    fn source_configurable_server_busy_timeout_requires_the_source_reason() {
        let short_read = ConfigurableTimeoutPlan {
            duration_ms: 29_999,
        };
        let deadline_busy = errorpb::Error {
            server_is_busy: Some(errorpb::ServerIsBusy {
                reason: "deadline is exceeded while waiting for read index".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(source_configurable_server_busy_timeout(
            &short_read,
            &deadline_busy
        ));

        let ordinary_busy = errorpb::Error {
            server_is_busy: Some(errorpb::ServerIsBusy {
                reason: "too many requests".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(!source_configurable_server_busy_timeout(
            &short_read,
            &ordinary_busy
        ));
        assert!(!source_configurable_server_busy_timeout(
            &ConfigurableTimeoutPlan {
                duration_ms: 30_000,
            },
            &deadline_busy
        ));
    }

    #[test]
    fn snapshot_retry_owners_use_the_supplied_variables() {
        let mut variables = crate::Variables::default();
        variables.backoff_weight = 1;
        let variables = Arc::new(variables);

        let region = SnapshotRegionBackoff::new(
            Backoff::no_jitter_backoff(1, 1, 1),
            None,
            Arc::clone(&variables),
        );
        assert_eq!(
            region.backoff.try_lock().unwrap().max_sleep_ms(),
            SNAPSHOT_MAX_BACKOFF_MS
        );

        let mut lock = SnapshotLockBackoff::new(None, variables);
        assert_eq!(
            lock.backoff.try_lock().unwrap().max_sleep_ms(),
            SNAPSHOT_MAX_BACKOFF_MS
        );
        lock.set_owner(region.owner());
        assert!(Arc::ptr_eq(&region.backoff, &lock.backoff));
    }

    #[test]
    fn source_configurable_region_error_timeout_requires_the_source_message() {
        let short_read = ConfigurableTimeoutPlan {
            duration_ms: 29_999,
        };
        assert!(source_configurable_region_error_timeout(
            &short_read,
            &errorpb::Error {
                message: "Deadline is exceeded while reading".to_owned(),
                ..Default::default()
            }
        ));
        assert!(!source_configurable_region_error_timeout(
            &short_read,
            &errorpb::Error {
                message: "deadline is exceeded while reading".to_owned(),
                ..Default::default()
            }
        ));
        assert!(!source_configurable_region_error_timeout(
            &short_read,
            &errorpb::Error {
                message: "Deadline is exceeded while reading".to_owned(),
                server_is_busy: Some(Default::default()),
                ..Default::default()
            }
        ));
        assert!(!source_configurable_region_error_timeout(
            &ConfigurableTimeoutPlan {
                duration_ms: 30_000,
            },
            &errorpb::Error {
                message: "Deadline is exceeded while reading".to_owned(),
                ..Default::default()
            }
        ));
    }

    #[test]
    fn source_configurable_timeout_fast_path_requires_a_grpc_deadline() {
        assert!(is_grpc_deadline_exceeded(&Error::GrpcAPI(
            tonic::Status::deadline_exceeded("deadline"),
        )));
        assert!(is_grpc_deadline_exceeded(&Error::Connection {
            source: Box::new(Error::GrpcAPI(tonic::Status::deadline_exceeded("deadline"))),
            address: "store".to_owned(),
            version: 1,
        }));
        assert!(!is_grpc_deadline_exceeded(&Error::GrpcAPI(
            tonic::Status::unavailable("unavailable"),
        )));
    }

    #[tokio::test]
    async fn test_err() {
        let plan = RetryableMultiRegion {
            inner: ResolveLock {
                inner: ErrPlan,
                timestamp: Timestamp::default(),
                backoff: Backoff::no_backoff(),
                pd_client: Arc::new(MockPdClient::default()),
                keyspace: Keyspace::Disable,
                keyspace_name: None,
                rpc_interceptor: None,
                resource_group_name: None,
                resource_control: None,
                ru_details: None,
                resolve_locks_context: ResolveLocksContext::default(),
                read_lock_context: None,
                snapshot_runtime_stats: None,
                snapshot_lock_backoff: None,
                response_locks_only: false,
                prewrite_lock_conflict: None,
                max_timestamp_point_get: false,
                record_async_batch_get_metric: false,
            },
            pd_client: Arc::new(MockPdClient::default()),
            backoff: Backoff::no_backoff(),
            preserve_region_results: false,
            concurrency: MULTI_REGION_CONCURRENCY,
            one_region: None,
            snapshot_region_scope: None,
        };
        assert!(plan.execute().await.is_err())
    }

    #[tokio::test]
    async fn test_join_set_results_keep_spawn_order() {
        let mut join_set = JoinSet::new();
        for (idx, delay_ms) in [(0, 30), (1, 10), (2, 20)] {
            join_set.spawn(async move {
                sleep(Duration::from_millis(delay_ms)).await;
                (idx, idx)
            });
        }

        let results = collect_join_set_results(join_set, 3, "test_handler")
            .await
            .unwrap();

        assert_eq!(results, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn cumulative_retry_state_forks_each_shard_and_merges_the_final_child() {
        let retry = RecordingRetryState {
            forks: Arc::new(AtomicUsize::new(0)),
            merges: Arc::new(AtomicUsize::new(0)),
        };
        let forks = retry.forks.clone();
        let merges = retry.merges.clone();
        let plan = RetryableMultiRegion {
            inner: TwoShardPlan,
            pd_client: Arc::new(MockPdClient::default()),
            backoff: retry,
            preserve_region_results: false,
            concurrency: MULTI_REGION_CONCURRENCY,
            one_region: None,
            snapshot_region_scope: None,
        };

        assert_eq!(plan.execute().await.unwrap().len(), 2);
        // One parent child plus one child for each source region batch.
        assert_eq!(forks.load(Ordering::SeqCst), 3);
        assert_eq!(merges.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn first_shard_error_cancels_a_sibling_cumulative_backoff() {
        let cancellation_seen = Arc::new(AtomicUsize::new(0));
        let plan = RetryableMultiRegion {
            inner: CancellationProbePlan {
                fails_immediately: false,
                dispatched: Arc::new(Barrier::new(2)),
            },
            pd_client: Arc::new(MockPdClient::default()),
            backoff: CancellationProbeRetryState {
                cancellation: Cancellation::default(),
                cancellation_seen: cancellation_seen.clone(),
            },
            preserve_region_results: false,
            concurrency: MULTI_REGION_CONCURRENCY,
            one_region: None,
            snapshot_region_scope: None,
        };

        assert!(plan.execute().await.is_err());
        assert_eq!(cancellation_seen.load(Ordering::SeqCst), 1);
    }
}

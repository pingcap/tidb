// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::time::Instant;

use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::prelude::*;
use log::debug;
use log::error;
use log::info;
use log::warn;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::async_util::Cancellation;
use crate::backoff::Backoff;
use crate::interceptor::RpcInterceptorChain;
use crate::kv::{AccessLocationType, ReplicaReadConfig};
use crate::locate::ReplicaSelectorState;
use crate::pd::PdClient;
use crate::proto::errorpb;
use crate::proto::errorpb::EpochNotMatch;
use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::region::StoreId;
use crate::region::{RegionVerId, RegionWithLeader};
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
use crate::store::HasRegionError;
use crate::store::HasRegionErrors;
use crate::store::KvClient;
use crate::store::RegionStore;
use crate::store::{HasKeyErrors, Store};
use crate::transaction::resolve_locks_with_ru_details;
use crate::transaction::HasLocks;
use crate::transaction::ResolveLocksContext;
use crate::transaction::ResolveLocksOptions;
use crate::util::iter::FlatMapOkIterExt;
use crate::Error;
use crate::Result;

use super::keyspace::Keyspace;

/// A plan for how to execute a request. A user builds up a plan with various
/// options, then exectutes it.
#[async_trait]
pub trait Plan: Sized + Clone + Sync + Send + 'static {
    /// The ultimate result of executing the plan (should be a high-level type, not a GRPC response).
    type Result: Send;

    /// Execute the plan.
    async fn execute(&self) -> Result<Self::Result>;
}

/// The simplest plan which just dispatches a request to a specific kv server.
#[derive(Clone)]
pub struct Dispatch<Req: KvRequest> {
    pub request: Req,
    pub kv_client: Option<Arc<dyn KvClient + Send + Sync>>,
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
    pub(crate) resource_control_replica_number: i64,
    pub(crate) resource_control_access_location: AccessLocationType,
    pub(crate) predicted_read_bytes: u64,
    pub(crate) ru_details: Option<Arc<crate::RuDetails>>,
    pub(crate) store_token_count: Arc<AtomicI64>,
    pub(crate) store_token_store_id: StoreId,
    /// Optional transaction-level decorator for this physical RPC.
    pub interceptor: Option<RpcInterceptorChain>,
    /// Optional client-go-compatible resource-group controller applied before
    /// the user interceptor and settled after a successful response.
    pub resource_control: Option<ResourceGroupControllerHandle>,
    pub response_codec: Option<super::keyspace::ApiV2Codec>,
    pub v1_response_codec: Option<super::keyspace::ApiV1Codec>,
}

#[async_trait]
impl<Req: KvRequest> Plan for Dispatch<Req> {
    type Result = Req::Response;

    async fn execute(&self) -> Result<Self::Result> {
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
        let next = Box::new(|| {
            Box::pin(async {
                client
                    .dispatch_with_forwarded_host(&request, &self.forwarded_host)
                    .await
            }) as futures::future::BoxFuture<'_, crate::interceptor::RpcDispatchResult>
        });
        let started_at = Instant::now();
        let result = match &self.interceptor {
            Some(interceptor) => interceptor.dispatch(&self.target, &request, next).await,
            None => next().await,
        };
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
                    )?;
                    if let Some(ru_details) = &self.ru_details {
                        ru_details.update(&settlement.consumption, settlement.wait_duration);
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
            self.request
                .decode_response(&mut response, self.response_codec.as_ref())?;
            self.request
                .decode_v1_response(&mut response, self.v1_response_codec.as_ref())?;
            Ok(response)
        })
    }
}

impl<Req: KvRequest + StoreRequest> StoreRequest for Dispatch<Req> {
    fn apply_store(&mut self, store: &Store) {
        self.kv_client = Some(store.client.clone());
        self.target = store.target.clone();
        self.forwarded_host.clear();
        self.store_health = None;
        self.record_client_side_slow_score = false;
        self.request.apply_store(store);
    }
}

const MULTI_REGION_CONCURRENCY: usize = 16;
const MULTI_STORES_CONCURRENCY: usize = 16;

pub(crate) fn is_grpc_error(e: &Error) -> bool {
    matches!(e, Error::GrpcAPI(_) | Error::Grpc(_))
        || matches!(e, Error::Connection { source, .. } if is_grpc_error(source))
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
    ) -> (Result<<Self as Plan>::Result>, R) {
        let shards = current_plan.shards(&pd_client).collect::<Vec<_>>().await;
        let shards_len = shards.len();
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
            let clone = current_plan.clone_then_apply_shard(shard);
            let pd_client = pd_client.clone();
            let (backoff, _) = forked_backoff.fork();
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
    ) -> (Result<<Self as Plan>::Result>, R) {
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
                    err,
                )
                .await;
            }
        };
        if let Some(peer) = region_store.target_peer.as_ref() {
            plan.record_replica_attempt(peer.id);
        }

        // limit concurrent requests
        let permit = permits.acquire().await.unwrap();
        let res = plan.execute().await;
        drop(permit);

        let mut resp = match res {
            Ok(resp) => resp,
            Err(e) if is_grpc_deadline_exceeded(&e) && source_configurable_read_timeout(&plan) => {
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
            if source_configurable_server_busy_timeout(&plan, &e) {
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
                )
                .await;
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
            if !plan.is_batched_coprocessor_read() {
                if let (Some(busy), Some(target_peer)) =
                    (e.server_is_busy.as_ref(), region_store.target_peer.as_ref())
                {
                    plan.record_server_busy(target_peer.id);
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
                    &region_store,
                    is_read_request,
                    plan.is_batched_coprocessor_read(),
                    busy.estimated_wait_ms,
                )
            });
            let configurable_region_error_timeout =
                source_configurable_region_error_timeout(&plan, &e);
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
        e: Error,
    ) -> (Result<<Self as Plan>::Result>, R) {
        debug!("handle_other_error: {:?}", e);
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
                    )
                    .await
                } else {
                    Self::single_plan_handler(
                        pd_client,
                        plan,
                        backoff,
                        permits,
                        preserve_region_results,
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
    region_store: &RegionStore,
    is_read_request: bool,
    is_batched_coprocessor_read: bool,
    estimated_wait_ms: u32,
) -> bool {
    !matches!(config.read_type, crate::kv::ReplicaReadType::Leader)
        || region_store.force_leader_read
        || (estimated_wait_ms != 0
            && config.busy_threshold_ms != 0
            && is_read_request
            && !is_batched_coprocessor_read)
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
    } else if e.undetermined_result.is_some() {
        // Source leaves the payload for the caller because it cannot know
        // whether the command executed.
        Err(Error::RegionError(Box::new(e)))
    } else if e.raft_entry_too_large.is_some() {
        // `onRegionError` returns `errors.New(regionErr.String())`: preserve
        // the direct terminal boundary so outer RawKV region-error recovery
        // cannot resend an oversized write.
        Err(Error::StringError(format!("{e:?}")))
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
    } else if e.stale_command.is_some() {
        Ok(RegionErrorRetry::Backoff(BO_STALE_CMD))
    } else if let Some(server_is_busy) = e.server_is_busy.as_ref() {
        if server_is_busy.estimated_wait_ms == 0 {
            if let Some(health_status) = region_store.health_status.as_ref() {
                health_status.mark_already_slow();
            }
        }
        Ok(RegionErrorRetry::Backoff(
            source_server_busy_backoff_config(&region_store),
        ))
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
        let concurrency_permits = Arc::new(Semaphore::new(MULTI_REGION_CONCURRENCY));
        Self::single_plan_handler(
            self.pd_client.clone(),
            self.inner.clone(),
            self.backoff.clone(),
            concurrency_permits.clone(),
            self.preserve_region_results,
        )
        .await
        .0
    }
}

pub struct RetryableAllStores<P: Plan, PdC: PdClient> {
    pub(super) inner: P,
    pub pd_client: Arc<PdC>,
    pub backoff: Backoff,
}

impl<P: Plan, PdC: PdClient> Clone for RetryableAllStores<P, PdC> {
    fn clone(&self) -> Self {
        RetryableAllStores {
            inner: self.inner.clone(),
            pd_client: self.pd_client.clone(),
            backoff: self.backoff.clone(),
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
        let stores = self.pd_client.clone().all_stores().await?;
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
            collect_join_set_results(join_set, stores_len, "single_store_handler").await?;
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
        }
    }
}

#[async_trait]
impl<P: Plan + Shardable, PdC: PdClient> Plan for ResolveLock<P, PdC>
where
    P::Result: HasLocks,
{
    type Result = P::Result;

    async fn execute(&self) -> Result<Self::Result> {
        let mut result = self.inner.execute().await?;
        let mut clone = self.clone();
        loop {
            let locks = result.take_locks();
            if locks.is_empty() {
                return Ok(result);
            }

            if self.backoff.is_none() {
                return Err(Error::ResolveLockError(locks));
            }

            // Source `KVSnapshot.get` turns a stale read that met a lock
            // into a threshold-free leader read before resolving/retrying it.
            // The clone is the retry owner, so this cannot alter a sibling
            // plan that shares the original immutable `ResolveLock` wrapper.
            clone.disable_stale_read_after_lock();

            let pd_client = self.pd_client.clone();
            let live_locks = resolve_locks_with_ru_details(
                locks,
                self.timestamp.clone(),
                pd_client.clone(),
                self.keyspace,
                self.keyspace_name.as_deref(),
                self.rpc_interceptor.clone(),
                self.resource_group_name.as_deref(),
                self.resource_control.clone(),
                self.ru_details.clone(),
            )
            .await?;
            if live_locks.is_empty() {
                result = clone.inner.execute().await?;
            } else {
                match clone.backoff.next_delay_duration() {
                    None => return Err(Error::ResolveLockError(live_locks)),
                    Some(delay_duration) => {
                        sleep(delay_duration).await;
                        result = clone.inner.execute().await?;
                    }
                }
            }
        }
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
                Some(range) if region.contains(range.0.as_ref()) => {
                    debug!("CleanupLocks::execute, next range:{:?}", range);
                    inner.next_batch(range);
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use futures::stream::BoxStream;
    use futures::stream::{self};
    use tokio::sync::Barrier;

    use super::*;
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::proto::kvrpcpb::BatchGetResponse;

    fn region_store() -> RegionStore {
        RegionStore::new(MockPdClient::region1(), Arc::new(MockKvClient::default()))
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
        assert!(!source_fast_server_busy_retry(
            &config, &region, true, false, 0
        ));

        config.read_type = crate::kv::ReplicaReadType::Follower;
        assert!(source_fast_server_busy_retry(
            &config, &region, true, false, 0
        ));
        config.read_type = crate::kv::ReplicaReadType::Mixed;
        assert!(source_fast_server_busy_retry(
            &config, &region, true, false, 0
        ));
        config.read_type = crate::kv::ReplicaReadType::PreferLeader;
        assert!(source_fast_server_busy_retry(
            &config, &region, true, false, 0
        ));

        config.read_type = crate::kv::ReplicaReadType::Leader;
        config.busy_threshold_ms = 10;
        assert!(source_fast_server_busy_retry(
            &config, &region, true, false, 1
        ));
        assert!(!source_fast_server_busy_retry(
            &config, &region, true, true, 1
        ));
        assert!(source_fast_server_busy_retry(
            &config,
            &region.with_force_leader_read(),
            true,
            false,
            0
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
            },
            pd_client: Arc::new(MockPdClient::default()),
            backoff: Backoff::no_backoff(),
            preserve_region_results: false,
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
        };

        assert!(plan.execute().await.is_err());
        assert_eq!(cancellation_seen.load(Ordering::SeqCst), 1);
    }
}

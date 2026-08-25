// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use super::plan::CountLockResolverAction;
use super::plan::PreserveShard;
use super::Keyspace;
use crate::backoff::Backoff;
use crate::interceptor::RpcInterceptorChain;
use crate::kv::ReplicaReadConfig;
use crate::pd::PdClient;
use crate::request::plan::{CleanupLocks, RegionRetryState, RetryableAllStores};
use crate::request::plan::{SnapshotLockBackoff, SnapshotRegionBackoff};
use crate::request::shard::HasNextBatch;
use crate::request::Dispatch;
use crate::request::ExtractError;
use crate::request::KvRequest;
use crate::request::Merge;
use crate::request::MergeResponse;
use crate::request::NextBatch;
use crate::request::Plan;
use crate::request::Process;
use crate::request::ProcessResponse;
use crate::request::ResolveLock;
use crate::request::RetryableMultiRegion;
use crate::request::Shardable;
use crate::request::{DefaultProcessor, StoreRequest};
use crate::resource_control::ResourceGroupControllerHandle;
use crate::retry::RetryBackoffer;
use crate::store::HasKeyErrors;
use crate::store::HasRegionError;
use crate::store::HasRegionErrors;
use crate::store::RegionStore;
use crate::transaction::HasLocks;
use crate::transaction::Priority;
use crate::transaction::ReadLockContext;
use crate::transaction::ResolveLocksContext;
use crate::transaction::ResolveLocksOptions;
use crate::Result;
use crate::Timestamp;

/// Builder type for plans (see that module for more).
pub struct PlanBuilder<PdC: PdClient, P: Plan, Ph: PlanBuilderPhase> {
    pd_client: Arc<PdC>,
    plan: P,
    keyspace_name: Option<String>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<String>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    phantom: PhantomData<Ph>,
}

/// Used to ensure that a plan has a designated target or targets, a target is
/// a particular TiKV server.
pub trait PlanBuilderPhase {}
pub(crate) struct NoTarget;
impl PlanBuilderPhase for NoTarget {}
pub(crate) struct Targetted;
impl PlanBuilderPhase for Targetted {}

impl<PdC: PdClient, Req: KvRequest> PlanBuilder<PdC, Dispatch<Req>, NoTarget> {
    pub fn new(pd_client: Arc<PdC>, keyspace: Keyspace, mut request: Req) -> Self {
        request.set_api_version(keyspace.api_version());
        request.set_keyspace_id(keyspace.context_keyspace_id());
        let response_codec = request
            .key_mode()
            .and_then(|mode| keyspace.response_codec(mode));
        let v1_response_codec = request
            .key_mode()
            .and_then(|mode| keyspace.v1_response_codec(mode));
        PlanBuilder {
            pd_client,
            plan: Dispatch {
                request,
                kv_client: None,
                request_timeout: None,
                retry_request_timeout: None,
                read_timestamp_validation: None,
                target: String::new(),
                forwarded_host: String::new(),
                replica_read_config: ReplicaReadConfig::default(),
                replica_selector_state: crate::locate::ReplicaSelectorState::default(),
                store_health: None,
                record_client_side_slow_score: false,
                physical_endpoint_type: crate::store::EndpointType::TiKv,
                resource_control_replica_number: 1,
                resource_control_access_location: crate::kv::AccessLocationType::Unknown,
                predicted_read_bytes: 0,
                ru_details: None,
                store_token_count: Arc::new(std::sync::atomic::AtomicI64::new(0)),
                store_token_store_id: 0,
                region_request_runtime_stats: None,
                logical_peer_id: None,
                logical_store_id: None,
                request_stale_read: false,
                request_replica_read: false,
                interceptor: None,
                execution_details_trace_handler:
                    crate::trace::current_execution_details_trace_handler(),
                network_traffic_details: crate::traffic::current_network_traffic_details(),
                network_stale_read: false,
                resource_control: None,
                response_codec,
                v1_response_codec,
            },
            keyspace_name: None,
            rpc_interceptor: None,
            resource_group_name: None,
            resource_control: None,
            ru_details: None,
            phantom: PhantomData,
        }
    }

    /// Set the TiKV command priority carried by every shard and retry of this request.
    pub fn priority(mut self, priority: Priority) -> Self {
        self.plan.request.set_priority(priority.into());
        self
    }

    /// Set TiKV's cache-fill behavior carried by every shard and retry of
    /// this request.
    pub fn not_fill_cache(mut self, not_fill_cache: bool) -> Self {
        self.plan.request.set_not_fill_cache(not_fill_cache);
        self
    }

    /// Set TiKV's isolation level carried by every shard and retry of this
    /// request.
    pub fn isolation_level(
        mut self,
        isolation_level: crate::proto::kvrpcpb::IsolationLevel,
    ) -> Self {
        self.plan.request.set_isolation_level(isolation_level);
        self
    }

    /// Set TiKV's scheduling task ID carried by every shard and retry of this
    /// request.
    pub fn task_id(mut self, task_id: u64) -> Self {
        self.plan.request.set_task_id(task_id);
        self
    }

    /// Set the source resource-group tag carried by every shard and retry.
    /// `None` deliberately leaves the protobuf default untouched.
    pub fn resource_group_tag(mut self, resource_group_tag: Option<Vec<u8>>) -> Self {
        if let Some(resource_group_tag) = resource_group_tag {
            self.plan.request.set_resource_group_tag(resource_group_tag);
        }
        self
    }

    /// Set client-go's request-source attribution on every shard and retry.
    pub fn request_source(mut self, request_source: impl Into<String>) -> Self {
        self.plan.request.set_request_source(request_source.into());
        self
    }

    /// Select replicas for this read using client-go's region selector. The
    /// setting is retained through shard and retry clones; leader is default.
    pub fn replica_read(mut self, config: ReplicaReadConfig) -> Self {
        let config = config.for_source_build();
        self.plan.network_stale_read = config.stale_read;
        self.plan.replica_read_config = config;
        self
    }

    /// Attach client-go-compatible physical request runtime statistics.
    /// The collector is shared by every shard and sender retry.
    pub fn region_request_runtime_stats(
        mut self,
        stats: Option<Arc<crate::RegionRequestRuntimeStats>>,
    ) -> Self {
        self.plan.region_request_runtime_stats = stats;
        self
    }

    /// Set TiKV's server-side maximum execution duration before this request
    /// is cloned for shards and retries.
    pub(crate) fn max_execution_duration(mut self, duration: Duration) -> Self {
        let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.plan.request.set_max_execution_duration_ms(duration_ms);
        self
    }

    /// Configure a source snapshot's initial and retry deadlines. The source
    /// uses an optional `SetKVReadTimeout` override only for the initial Get
    /// or BatchGet send; every resend returns to `retry_timeout`.
    pub(crate) fn snapshot_read_timeout(
        mut self,
        timeout: Option<Duration>,
        retry_timeout: Duration,
    ) -> Self {
        let timeout = timeout.unwrap_or(retry_timeout);
        let duration_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        self.plan.request.set_max_execution_duration_ms(duration_ms);
        self.plan.request_timeout = Some(timeout);
        self.plan.retry_request_timeout = Some(retry_timeout);
        self
    }

    /// Validate every physical snapshot read timestamp before dispatch.
    /// `None` preserves the behavior of manually constructed plans.
    pub(crate) fn validate_read_timestamp(
        mut self,
        validator: Option<Arc<dyn crate::oracle::ReadTimestampValidator>>,
        read_timestamp: u64,
        stale_read: bool,
        txn_scope: String,
    ) -> Self {
        self.plan.read_timestamp_validation =
            validator.map(|validator| crate::request::plan::ReadTimestampValidation {
                validator,
                read_timestamp,
                stale_read,
                option: crate::oracle::OracleOption { txn_scope },
            });
        self
    }

    /// Set the API V2 keyspace name carried by every clone and shard of this request.
    pub fn keyspace_name(mut self, keyspace_name: impl AsRef<str>) -> Self {
        self.plan
            .request
            .set_keyspace_name(Some(keyspace_name.as_ref()));
        self.keyspace_name = Some(keyspace_name.as_ref().to_owned());
        self
    }

    /// Set an API V2 keyspace name when the client was constructed for a
    /// keyspace. API V1 and API-V2-no-prefix clients intentionally leave it
    /// absent, matching client-go's codec metadata behavior.
    pub(crate) fn keyspace_name_option(self, keyspace_name: Option<&str>) -> Self {
        match keyspace_name {
            Some(keyspace_name) => self.keyspace_name(keyspace_name),
            None => self,
        }
    }

    /// Attach a transaction RPC interceptor to every physical dispatch produced
    /// by this request, including shards and retry clones.
    pub fn rpc_interceptor(mut self, interceptor: RpcInterceptorChain) -> Self {
        self.plan.interceptor = Some(interceptor.clone());
        self.rpc_interceptor = Some(interceptor);
        self
    }

    /// Assign this request and all of its physical shard/retry clones to a
    /// resource group. Admission only becomes active when a controller is
    /// attached with [`Self::resource_control`].
    pub fn resource_group(mut self, resource_group_name: impl AsRef<str>) -> Self {
        self.plan
            .request
            .set_resource_group_name(resource_group_name.as_ref());
        self.resource_group_name = Some(resource_group_name.as_ref().to_owned());
        self
    }

    /// Attach a PD resource-group controller to every physical TiKV RPC.
    ///
    /// The controller runs before normal RPC interceptors, fills TiKV's
    /// penalty and fallback priority, and settles only non-error responses.
    pub fn resource_control(mut self, controller: ResourceGroupControllerHandle) -> Self {
        self.plan.resource_control = Some(controller.clone());
        self.resource_control = Some(controller);
        self
    }

    /// Attach the optional source `tikvrpc.Request.PredictedReadBytes` hint.
    /// PD's resource controller uses it only for eligible coprocessor reads.
    pub fn predicted_read_bytes(mut self, predicted_read_bytes: u64) -> Self {
        self.plan.predicted_read_bytes = predicted_read_bytes;
        self
    }

    /// Attach source-compatible resource-unit accounting to every physical
    /// dispatch produced by this plan.
    pub fn ru_details(mut self, ru_details: Arc<crate::RuDetails>) -> Self {
        self.plan.ru_details = Some(ru_details.clone());
        self.ru_details = Some(ru_details);
        self
    }

    pub(crate) fn rpc_interceptor_option(self, interceptor: Option<RpcInterceptorChain>) -> Self {
        match interceptor {
            Some(interceptor) => self.rpc_interceptor(interceptor),
            None => self,
        }
    }

    pub(crate) fn resource_group_option(self, resource_group_name: Option<&str>) -> Self {
        match resource_group_name {
            Some(resource_group_name) => self.resource_group(resource_group_name),
            None => self,
        }
    }

    pub(crate) fn resource_control_option(
        self,
        controller: Option<ResourceGroupControllerHandle>,
    ) -> Self {
        match controller {
            Some(controller) => self.resource_control(controller),
            None => self,
        }
    }

    pub(crate) fn ru_details_option(self, ru_details: Option<Arc<crate::RuDetails>>) -> Self {
        match ru_details {
            Some(ru_details) => self.ru_details(ru_details),
            None => self,
        }
    }
}

impl<PdC: PdClient, P: Plan> PlanBuilder<PdC, P, Targetted> {
    /// Return the built plan, note that this can only be called once the plan
    /// has a target.
    pub fn plan(self) -> P {
        self.plan
    }
}

impl<PdC: PdClient, P: Plan, Ph: PlanBuilderPhase> PlanBuilder<PdC, P, Ph> {
    /// If there is a lock error, then resolve the lock and retry the request.
    pub fn resolve_lock(
        self,
        timestamp: Timestamp,
        backoff: Backoff,
        keyspace: Keyspace,
    ) -> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
    where
        P: Shardable,
        P::Result: HasLocks,
    {
        self.resolve_lock_with_context(timestamp, backoff, keyspace, ResolveLocksContext::default())
    }

    /// If there is a lock error, resolve the lock and retry the request with
    /// caller-owned resolver state.
    pub(crate) fn resolve_lock_with_context(
        self,
        timestamp: Timestamp,
        backoff: Backoff,
        keyspace: Keyspace,
        mut resolve_locks_context: ResolveLocksContext,
    ) -> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
    where
        P: Shardable,
        P::Result: HasLocks,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ResolveLock {
                inner: self.plan,
                timestamp,
                backoff,
                pd_client: self.pd_client,
                keyspace,
                keyspace_name: self.keyspace_name.clone(),
                rpc_interceptor: self.rpc_interceptor.clone(),
                resource_group_name: self.resource_group_name.clone(),
                resource_control: self.resource_control.clone(),
                ru_details: self.ru_details.clone(),
                resolve_locks_context: {
                    resolve_locks_context.rpc_interceptor = self.rpc_interceptor.clone();
                    resolve_locks_context.resource_group_name = self.resource_group_name.clone();
                    resolve_locks_context.resource_control = self.resource_control.clone();
                    resolve_locks_context.ru_details = self.ru_details.clone();
                    resolve_locks_context
                },
                read_lock_context: None,
                snapshot_runtime_stats: None,
                snapshot_lock_backoff: None,
                response_locks_only: false,
                prewrite_lock_conflict: None,
                max_timestamp_point_get: false,
                record_async_batch_get_metric: false,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }

    /// Resolve locks encountered by a snapshot read. Unlike a mutation,
    /// client-go reissues the read with TiKV's resolved/committed-lock hints
    /// instead of waiting for secondary-lock cleanup.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve_lock_for_read(
        self,
        timestamp: Timestamp,
        backoff: Backoff,
        keyspace: Keyspace,
        read_lock_context: ReadLockContext,
        mut resolve_locks_context: ResolveLocksContext,
        snapshot_runtime_stats: Option<Arc<crate::SnapshotRuntimeStats>>,
        snapshot_variables: Arc<crate::Variables>,
    ) -> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
    where
        P: Shardable,
        P::Result: HasLocks,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ResolveLock {
                inner: self.plan,
                timestamp,
                backoff,
                pd_client: self.pd_client,
                keyspace,
                keyspace_name: self.keyspace_name.clone(),
                rpc_interceptor: self.rpc_interceptor.clone(),
                resource_group_name: self.resource_group_name.clone(),
                resource_control: self.resource_control.clone(),
                ru_details: self.ru_details.clone(),
                resolve_locks_context: {
                    resolve_locks_context.rpc_interceptor = self.rpc_interceptor.clone();
                    resolve_locks_context.resource_group_name = self.resource_group_name.clone();
                    resolve_locks_context.resource_control = self.resource_control.clone();
                    resolve_locks_context.ru_details = self.ru_details.clone();
                    resolve_locks_context
                },
                read_lock_context: Some(read_lock_context),
                snapshot_lock_backoff: Some(SnapshotLockBackoff::new(
                    snapshot_runtime_stats.clone(),
                    snapshot_variables,
                )),
                snapshot_runtime_stats,
                response_locks_only: false,
                prewrite_lock_conflict: None,
                max_timestamp_point_get: false,
                record_async_batch_get_metric: false,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }

    /// Resolve only a response-level snapshot lock. Pair-level errors remain
    /// attached for the scanner to recover with key-local point reads.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve_response_lock_for_read(
        self,
        timestamp: Timestamp,
        backoff: Backoff,
        keyspace: Keyspace,
        read_lock_context: ReadLockContext,
        mut resolve_locks_context: ResolveLocksContext,
        snapshot_runtime_stats: Option<Arc<crate::SnapshotRuntimeStats>>,
        snapshot_variables: Arc<crate::Variables>,
    ) -> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
    where
        P: Shardable,
        P::Result: HasLocks,
    {
        resolve_locks_context.rpc_interceptor = self.rpc_interceptor.clone();
        resolve_locks_context.resource_group_name = self.resource_group_name.clone();
        resolve_locks_context.resource_control = self.resource_control.clone();
        resolve_locks_context.ru_details = self.ru_details.clone();
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ResolveLock {
                inner: self.plan,
                timestamp,
                backoff,
                pd_client: self.pd_client,
                keyspace,
                keyspace_name: self.keyspace_name.clone(),
                rpc_interceptor: self.rpc_interceptor.clone(),
                resource_group_name: self.resource_group_name.clone(),
                resource_control: self.resource_control.clone(),
                ru_details: self.ru_details.clone(),
                resolve_locks_context,
                read_lock_context: Some(read_lock_context),
                snapshot_lock_backoff: Some(SnapshotLockBackoff::new(
                    snapshot_runtime_stats.clone(),
                    snapshot_variables,
                )),
                snapshot_runtime_stats,
                response_locks_only: true,
                prewrite_lock_conflict: None,
                max_timestamp_point_get: false,
                record_async_batch_get_metric: false,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }

    pub fn cleanup_locks(
        self,
        ctx: ResolveLocksContext,
        options: ResolveLocksOptions,
        backoff: Backoff,
        keyspace: Keyspace,
    ) -> PlanBuilder<PdC, CleanupLocks<P, PdC>, Ph>
    where
        P: Shardable + NextBatch,
        P::Result: HasLocks + HasNextBatch + HasRegionError + HasKeyErrors,
    {
        let mut ctx = ctx;
        if ctx.ru_details.is_none() {
            ctx.ru_details = self.ru_details.clone();
        }
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: CleanupLocks {
                inner: self.plan,
                ctx,
                options,
                store: None,
                backoff,
                pd_client: self.pd_client,
                keyspace,
                keyspace_name: self.keyspace_name.clone(),
                rpc_interceptor: self.rpc_interceptor.clone(),
                resource_group_name: self.resource_group_name.clone(),
                resource_control: self.resource_control.clone(),
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }

    /// Merge the results of a request. Usually used where a request is sent to multiple regions
    /// to combine the responses from each region.
    pub fn merge<In, M: Merge<In>>(self, merge: M) -> PlanBuilder<PdC, MergeResponse<P, In, M>, Ph>
    where
        In: Clone + Send + Sync + 'static,
        P: Plan<Result = Vec<Result<In>>>,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: MergeResponse {
                inner: self.plan,
                merge,
                phantom: PhantomData,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }

    /// Apply the default processing step to a response (usually only needed if the request is sent
    /// to a single region because post-porcessing can be incorporated in the merge step for
    /// multi-region requests).
    pub fn post_process_default(self) -> PlanBuilder<PdC, ProcessResponse<P, DefaultProcessor>, Ph>
    where
        P: Plan,
        DefaultProcessor: Process<P::Result>,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ProcessResponse {
                inner: self.plan,
                processor: DefaultProcessor,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }

    /// Transform one plan response before a later routing/retry stage.
    pub(crate) fn process<Pr>(self, processor: Pr) -> PlanBuilder<PdC, ProcessResponse<P, Pr>, Ph>
    where
        P: Plan,
        Pr: Process<P::Result>,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: ProcessResponse {
                inner: self.plan,
                processor,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }

    pub(crate) fn count_lock_resolver_action(
        self,
        action: &'static str,
    ) -> PlanBuilder<PdC, CountLockResolverAction<P>, Ph>
    where
        P: Plan,
    {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: CountLockResolverAction {
                inner: self.plan,
                action,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }
}

impl<PdC, P, Ph> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
where
    PdC: PdClient,
    P: Plan + Shardable,
    P::Result: HasLocks,
    Ph: PlanBuilderPhase,
{
    /// Apply client-go's latest-version autocommit point-get lock rule.
    pub(crate) fn max_timestamp_point_get(mut self, enabled: bool) -> Self {
        self.plan.max_timestamp_point_get = enabled;
        self
    }

    /// Apply client-go's prewrite-only lock policy before generic resolution.
    /// `NoResolvePolicy` rejects every holder, while optimistic prewrite also
    /// rejects a holder newer than the transaction because resolution cannot
    /// make that transaction commit successfully.
    pub(crate) fn prewrite_lock_conflict(
        mut self,
        caller_start_ts: u64,
        no_resolve: bool,
        optimistic: bool,
    ) -> Self {
        self.plan.prewrite_lock_conflict = Some(crate::request::plan::PrewriteLockConflict {
            caller_start_ts,
            no_resolve,
            optimistic,
        });
        self
    }
}

impl<PdC: PdClient, P: Plan + Shardable> PlanBuilder<PdC, P, NoTarget>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    /// Split the request into shards sending a request to the region of each shard.
    pub fn retry_multi_region(
        self,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(backoff, false)
    }

    /// Preserve all results, even some of them are Err.
    /// To pass all responses to merge, and handle partial successful results correctly.
    pub fn retry_multi_region_preserve_results(
        self,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region(backoff, true)
    }

    /// Split and retry a request with a caller-owned region concurrency cap.
    pub(crate) fn retry_multi_region_with_concurrency(
        self,
        backoff: Backoff,
        concurrency: usize,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC>, Targetted> {
        self.make_retry_multi_region_with_concurrency(backoff, false, concurrency)
    }

    /// Retry a snapshot read with the ordinary schedule while optionally
    /// reporting source retry-class sleeps to snapshot runtime statistics.
    pub(crate) fn retry_multi_region_with_snapshot_stats(
        mut self,
        backoff: Backoff,
        stats: Option<Arc<crate::SnapshotRuntimeStats>>,
        variables: Arc<crate::Variables>,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC, SnapshotRegionBackoff>, Targetted> {
        self.plan.set_region_request_runtime_stats(
            stats
                .as_ref()
                .map(|stats| stats.region_request_runtime_stats()),
        );
        let snapshot_backoff = SnapshotRegionBackoff::new(backoff, stats, variables);
        self.plan.set_snapshot_retry_owner(snapshot_backoff.owner());
        self.make_retry_multi_region(snapshot_backoff, false)
    }

    /// Use client-go's cumulative retry accounting for a request path that
    /// owns its own source-compatible backoff budget (currently RawKV).
    pub(crate) fn retry_multi_region_with_retry_backoffer(
        self,
        backoff: RetryBackoffer,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC, RetryBackoffer>, Targetted> {
        self.make_retry_multi_region(backoff, false)
    }

    fn make_retry_multi_region<R: RegionRetryState>(
        self,
        backoff: R,
        preserve_region_results: bool,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC, R>, Targetted> {
        self.make_retry_multi_region_with_concurrency(
            backoff,
            preserve_region_results,
            crate::request::plan::MULTI_REGION_CONCURRENCY,
        )
    }

    fn make_retry_multi_region_with_concurrency<R: RegionRetryState>(
        self,
        backoff: R,
        preserve_region_results: bool,
        concurrency: usize,
    ) -> PlanBuilder<PdC, RetryableMultiRegion<P, PdC, R>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: RetryableMultiRegion {
                inner: self.plan,
                pd_client: self.pd_client,
                backoff,
                preserve_region_results,
                concurrency: concurrency.max(1),
                one_region: None,
                snapshot_region_scope: None,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }
}

impl<PdC, P, R> PlanBuilder<PdC, RetryableMultiRegion<P, PdC, R>, Targetted>
where
    PdC: PdClient,
    P: Plan + Shardable,
    P::Result: HasKeyErrors + HasRegionError,
    R: RegionRetryState,
{
    /// Limit this retry owner to the current scanner boundary region.
    pub(crate) fn one_region(mut self, reverse: bool) -> Self {
        self.plan.one_region = Some(reverse);
        self
    }

    /// Record the source snapshot's initial distinct-region count.
    pub(crate) fn observe_snapshot_regions(mut self, internal: bool) -> Self {
        self.plan.snapshot_region_scope = Some(internal);
        self
    }
}

impl<PdC, P, Ph> PlanBuilder<PdC, ResolveLock<P, PdC>, Ph>
where
    PdC: PdClient,
    P: Plan + Shardable,
    P::Result: HasLocks,
    Ph: PlanBuilderPhase,
{
    /// Scanner `Next` owns retry sleeps locally and does not merge them into
    /// `SnapshotRuntimeStats`; keep RPC and resolve-lock detail collection.
    pub(crate) fn without_snapshot_lock_backoff_stats(mut self) -> Self {
        if let Some(backoff) = self.plan.snapshot_lock_backoff.as_mut() {
            backoff.clear_stats();
        }
        self
    }
}

impl<PdC, P> PlanBuilder<PdC, RetryableMultiRegion<P, PdC, SnapshotRegionBackoff>, Targetted>
where
    PdC: PdClient,
    P: Plan + Shardable,
    P::Result: HasKeyErrors + HasRegionError,
{
    /// Reuse one client-go scanner Backoffer while a refill crosses empty
    /// regions and resolves pair-local locks.
    pub(crate) fn snapshot_retry_owner(
        mut self,
        owner: Arc<tokio::sync::Mutex<crate::retry::RetryBackoffer>>,
    ) -> Self {
        self.plan.backoff.set_owner(Arc::clone(&owner));
        self.plan.inner.set_snapshot_retry_owner(owner);
        self
    }

    /// Preserve scanner RPC/error collection while leaving its local retry
    /// sleeps out of snapshot runtime stats, matching client-go.
    pub(crate) fn without_snapshot_region_backoff_stats(mut self) -> Self {
        self.plan.backoff.clear_stats();
        self
    }
}

impl<PdC: PdClient, R: KvRequest> PlanBuilder<PdC, Dispatch<R>, NoTarget> {
    /// Target the request at a single region; caller supplies the store to target.
    pub async fn single_region_with_store(
        self,
        store: RegionStore,
    ) -> Result<PlanBuilder<PdC, Dispatch<R>, Targetted>> {
        set_single_region_store(
            self.plan,
            store,
            self.pd_client,
            self.keyspace_name,
            self.rpc_interceptor,
            self.resource_group_name,
            self.resource_control,
            self.ru_details,
        )
    }
}

impl<PdC: PdClient, P: Plan + StoreRequest> PlanBuilder<PdC, P, NoTarget>
where
    P::Result: HasKeyErrors + HasRegionError,
{
    pub fn all_stores(
        self,
        backoff: Backoff,
    ) -> PlanBuilder<PdC, RetryableAllStores<P, PdC>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: RetryableAllStores {
                inner: self.plan,
                pd_client: self.pd_client,
                backoff,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan + Shardable> PlanBuilder<PdC, P, NoTarget>
where
    P::Result: HasKeyErrors,
{
    pub fn preserve_shard(self) -> PlanBuilder<PdC, PreserveShard<P>, NoTarget> {
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: PreserveShard {
                inner: self.plan,
                shard: None,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: PhantomData,
        }
    }
}

impl<PdC: PdClient, P: Plan> PlanBuilder<PdC, P, Targetted>
where
    P::Result: HasKeyErrors + HasRegionErrors,
{
    pub fn extract_error(self) -> PlanBuilder<PdC, ExtractError<P>, Targetted> {
        PlanBuilder {
            pd_client: self.pd_client,
            plan: ExtractError { inner: self.plan },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            resource_group_name: self.resource_group_name,
            resource_control: self.resource_control,
            ru_details: self.ru_details,
            phantom: self.phantom,
        }
    }
}

fn set_single_region_store<PdC: PdClient, R: KvRequest>(
    mut plan: Dispatch<R>,
    store: RegionStore,
    pd_client: Arc<PdC>,
    keyspace_name: Option<String>,
    rpc_interceptor: Option<RpcInterceptorChain>,
    resource_group_name: Option<String>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
) -> Result<PlanBuilder<PdC, Dispatch<R>, Targetted>> {
    plan.request.set_leader(&store.request_region())?;
    plan.request.set_replica_read(store.is_replica_read());
    plan.request.set_stale_read(store.stale_read);
    plan.request.set_busy_threshold_ms(store.busy_threshold_ms);
    plan.request
        .set_buckets_version(store.region_with_leader.buckets_version());
    plan.network_stale_read |= store.stale_read;
    plan.resource_control_replica_number = store.resource_control_replica_number;
    plan.resource_control_access_location = store.resource_control_access_location;
    plan.logical_peer_id = store.target_peer.as_ref().map(|peer| peer.id);
    plan.logical_store_id = store.target_peer.as_ref().map(|peer| peer.store_id);
    plan.request_stale_read = store.stale_read;
    plan.request_replica_read = store.is_replica_read();
    plan.store_token_store_id = store.target_peer.as_ref().map_or(0, |peer| peer.store_id);
    plan.store_token_count = store.store_token_count;
    if store.busy_threshold_disabled {
        plan.replica_selector_state.disable_busy_threshold();
    }
    plan.kv_client = Some(store.client);
    plan.target = store.target;
    plan.forwarded_host = store.forwarded_host;
    plan.store_health = store.health_status;
    plan.record_client_side_slow_score = store.record_client_side_slow_score;
    plan.physical_endpoint_type = store.physical_endpoint_type;
    Ok(PlanBuilder {
        plan,
        pd_client,
        keyspace_name,
        rpc_interceptor,
        resource_group_name,
        resource_control,
        ru_details,
        phantom: PhantomData,
    })
}

/// Indicates that a request operates on a single key.
pub trait SingleKey {
    #[allow(clippy::ptr_arg)]
    fn key(&self) -> &Vec<u8>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::mock::MockPdClient;
    use crate::proto::kvrpcpb;
    use crate::request::Shardable;

    #[test]
    fn priority_is_written_before_requests_are_cloned_for_execution() {
        let builder = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::GetRequest::default(),
        )
        .priority(Priority::High);

        let request = builder.plan.request;
        let cloned = request.clone();
        assert_eq!(
            request.context.as_ref().unwrap().priority,
            kvrpcpb::CommandPri::High as i32
        );
        assert_eq!(
            cloned.context.as_ref().unwrap().priority,
            kvrpcpb::CommandPri::High as i32
        );
    }

    #[test]
    fn source_snapshot_read_timeout_sets_transport_and_tikv_deadlines() {
        let timeout = Duration::from_millis(17);
        let builder = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::GetRequest::default(),
        )
        .snapshot_read_timeout(Some(timeout), Duration::from_secs(30));

        assert_eq!(builder.plan.request_timeout, Some(timeout));
        assert_eq!(
            builder.plan.retry_request_timeout,
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            builder
                .plan
                .request
                .context
                .as_ref()
                .unwrap()
                .max_execution_duration_ms,
            17
        );

        let disabled = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::GetRequest::default(),
        )
        .snapshot_read_timeout(None, Duration::from_secs(30));
        assert_eq!(disabled.plan.request_timeout, Some(Duration::from_secs(30)));
        assert_eq!(
            disabled.plan.retry_request_timeout,
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            disabled
                .plan
                .request
                .context
                .as_ref()
                .unwrap()
                .max_execution_duration_ms,
            30_000
        );

        let mut retried = builder.plan;
        retried.mark_retry_request();
        assert_eq!(retried.request_timeout, Some(Duration::from_secs(30)));
        assert_eq!(
            retried
                .request
                .context
                .as_ref()
                .unwrap()
                .max_execution_duration_ms,
            30_000
        );
    }

    #[test]
    fn source_plan_builder_applies_nextgen_read_feature_gate_before_cloning() {
        let requested = ReplicaReadConfig {
            read_type: crate::kv::ReplicaReadType::PreferLeader,
            stale_read: true,
            prefer_leader: true,
            busy_threshold_ms: 123,
            ..Default::default()
        };
        let builder = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::GetRequest::default(),
        )
        .replica_read(requested.clone());
        let expected = requested.for_source_build();

        assert_eq!(builder.plan.replica_read_config, expected);
        assert_eq!(builder.plan.network_stale_read, expected.stale_read);
    }

    #[test]
    fn api_v2_keyspace_id_is_written_with_the_api_version() {
        let builder = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::try_enable(4242).unwrap(),
            kvrpcpb::GetRequest::default(),
        );

        let context = builder.plan.request.context.unwrap();
        assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
        assert_eq!(crate::request::context_keyspace_id(&context), Some(4242));
    }

    #[test]
    fn api_v1_writes_the_source_null_keyspace_oneof() {
        let builder = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::GetRequest::default(),
        );

        let context = builder.plan.request.context.unwrap();
        assert_eq!(context.api_version, kvrpcpb::ApiVersion::V1 as i32);
        assert_eq!(
            crate::request::context_keyspace_id(&context),
            Some(crate::request::NULL_KEYSPACE_ID)
        );
    }

    #[test]
    fn api_v2_keyspace_name_is_written_before_requests_are_cloned() {
        let builder = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::try_enable(4242).unwrap(),
            kvrpcpb::GetRequest::default(),
        )
        .keyspace_name("tenant");

        let request = builder.plan.request;
        assert_eq!(request.context.as_ref().unwrap().keyspace_name, "tenant");
        assert_eq!(request.clone().context.unwrap().keyspace_name, "tenant");
    }

    #[test]
    fn resource_unit_details_are_retained_by_dispatch() {
        let details = Arc::new(crate::RuDetails::new());
        let builder = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::GetRequest::default(),
        )
        .ru_details(details.clone());

        assert!(Arc::ptr_eq(
            builder.plan.ru_details.as_ref().unwrap(),
            &details
        ));
    }

    #[test]
    fn resource_unit_details_are_retained_by_lock_resolution() {
        let details = Arc::new(crate::RuDetails::new());
        let builder = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::Disable,
            kvrpcpb::GetRequest::default(),
        )
        .ru_details(details.clone())
        .resolve_lock(
            Timestamp::default(),
            Backoff::no_jitter_backoff(0, 0, 1),
            Keyspace::Disable,
        );

        assert!(Arc::ptr_eq(
            builder.plan.ru_details.as_ref().unwrap(),
            &details
        ));
    }

    #[test]
    fn only_numeric_api_v2_requests_retain_a_response_codec() {
        let pd = Arc::new(MockPdClient::default());
        let numeric_v2 = PlanBuilder::new(
            pd.clone(),
            Keyspace::try_enable(7).unwrap(),
            kvrpcpb::GetRequest::default(),
        );
        let no_prefix = PlanBuilder::new(
            pd.clone(),
            Keyspace::ApiV2NoPrefix,
            kvrpcpb::GetRequest::default(),
        );
        let v1 = PlanBuilder::new(pd, Keyspace::Disable, kvrpcpb::GetRequest::default());

        assert!(numeric_v2.plan.response_codec.is_some());
        assert!(numeric_v2.plan.v1_response_codec.is_none());
        assert!(no_prefix.plan.response_codec.is_none());
        assert!(no_prefix.plan.v1_response_codec.is_none());
        assert!(v1.plan.response_codec.is_none());
        assert!(v1.plan.v1_response_codec.is_some());
    }
}

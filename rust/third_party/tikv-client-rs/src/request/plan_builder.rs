// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use super::plan::PreserveShard;
use super::Keyspace;
use crate::backoff::Backoff;
use crate::interceptor::RpcInterceptorChain;
use crate::kv::ReplicaReadConfig;
use crate::pd::PdClient;
use crate::request::plan::{CleanupLocks, RegionRetryState, RetryableAllStores};
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
use crate::retry::RetryBackoffer;
use crate::store::HasKeyErrors;
use crate::store::HasRegionError;
use crate::store::HasRegionErrors;
use crate::store::RegionStore;
use crate::transaction::HasLocks;
use crate::transaction::Priority;
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
                target: String::new(),
                forwarded_host: String::new(),
                replica_read_config: ReplicaReadConfig::default(),
                replica_selector_state: crate::locate::ReplicaSelectorState::default(),
                store_health: None,
                record_client_side_slow_score: false,
                interceptor: None,
                response_codec,
                v1_response_codec,
            },
            keyspace_name: None,
            rpc_interceptor: None,
            phantom: PhantomData,
        }
    }

    /// Set the TiKV command priority carried by every shard and retry of this request.
    pub fn priority(mut self, priority: Priority) -> Self {
        self.plan.request.set_priority(priority.into());
        self
    }

    /// Select replicas for this read using client-go's region selector. The
    /// setting is retained through shard and retry clones; leader is default.
    pub fn replica_read(mut self, config: ReplicaReadConfig) -> Self {
        self.plan.replica_read_config = config;
        self
    }

    /// Set TiKV's server-side maximum execution duration before this request
    /// is cloned for shards and retries.
    pub(crate) fn max_execution_duration(mut self, duration: Duration) -> Self {
        let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        self.plan.request.set_max_execution_duration_ms(duration_ms);
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

    pub(crate) fn rpc_interceptor_option(self, interceptor: Option<RpcInterceptorChain>) -> Self {
        match interceptor {
            Some(interceptor) => self.rpc_interceptor(interceptor),
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
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
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
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
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
            phantom: PhantomData,
        }
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
        PlanBuilder {
            pd_client: self.pd_client.clone(),
            plan: RetryableMultiRegion {
                inner: self.plan,
                pd_client: self.pd_client,
                backoff,
                preserve_region_results,
            },
            keyspace_name: self.keyspace_name,
            rpc_interceptor: self.rpc_interceptor,
            phantom: PhantomData,
        }
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
) -> Result<PlanBuilder<PdC, Dispatch<R>, Targetted>> {
    plan.request.set_leader(&store.request_region())?;
    plan.kv_client = Some(store.client);
    plan.target = store.target;
    plan.forwarded_host = store.forwarded_host;
    plan.store_health = store.health_status;
    plan.record_client_side_slow_score = store.record_client_side_slow_score;
    Ok(PlanBuilder {
        plan,
        pd_client,
        keyspace_name,
        rpc_interceptor,
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
    fn api_v2_keyspace_id_is_written_with_the_api_version() {
        let builder = PlanBuilder::new(
            Arc::new(MockPdClient::default()),
            Keyspace::try_enable(4242).unwrap(),
            kvrpcpb::GetRequest::default(),
        );

        let context = builder.plan.request.context.unwrap();
        assert_eq!(context.api_version, kvrpcpb::ApiVersion::V2 as i32);
        assert_eq!(context.keyspace_id, 4242);
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

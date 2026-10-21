// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::IntoRequest;
use tonic::Request;

use super::service_discovery::{ChannelCache, TsoDiscovery, TsoRoute};
use super::tso::{Cli as TsoClient, StreamBuilderFactory};
use crate::async_util::Cancellation;
use crate::internal_err;
use crate::proto::keyspacepb;
use crate::proto::pdpb;
use crate::Error;
use crate::Result;
use crate::SecurityManager;
use crate::Timestamp;

type RoutedChannel =
    tonic::service::interceptor::InterceptedService<Channel, super::region_service::Forwarding>;
type RoutedPdClient = pdpb::pd_client::PdClient<RoutedChannel>;

/// A PD cluster.
pub struct Cluster {
    connection: Connection,
    region_service: Arc<super::region_service::RegionService>,
    id: u64,
    channels: Arc<ChannelCache>,
    client: Option<RoutedPdClient>,
    keyspace_client: Option<keyspacepb::keyspace_client::KeyspaceClient<Channel>>,
    members: pdpb::GetMembersResponse,
    discovery: TsoDiscovery,
    route: TsoRoute,
    /// pd-client `innerClient.tsoClient`: the current TSO client, read by
    /// every TSO request attempt (Go `getTSOClient()`).
    tso: TsoSlot,
    /// The service discovery the current TSO client consumes.
    tso_view: Arc<TsoDiscoveryView>,
    /// Membership checks the TSO client can ask for, installed by the
    /// client that owns this cluster.
    tso_hooks: Arc<std::sync::RwLock<Option<TsoDiscoveryHooks>>>,
    // Keep joins owned until completion, including when a reconnect/close future
    // is cancelled after publication. Requests never own client retirement.
    retired_tso: Vec<Arc<TsoClient>>,
}

/// The current TSO client, shared with requests in flight.
pub(crate) type TsoSlot = Arc<std::sync::RwLock<Option<Arc<TsoClient>>>>;

/// Go `ServiceDiscovery.ScheduleCheckMemberChanged` / `CheckMemberChanged`
/// as the owning client implements them.
#[derive(Clone)]
pub(crate) struct TsoDiscoveryHooks {
    pub(crate) schedule_check_member_changed: Arc<dyn Fn() + Send + Sync>,
    pub(crate) check_member_changed:
        Arc<dyn Fn() -> futures::future::BoxFuture<'static, Result<()>> + Send + Sync>,
}

/// The `servicediscovery.ServiceDiscovery` a TSO client consumes: PD's
/// leader and members (PD mode) or a keyspace group's primary and members
/// (microservice mode), as this cluster last discovered them.
pub(crate) struct TsoDiscoveryView {
    cluster_id: u64,
    keyspace_id: u32,
    keyspace_group_id: std::sync::atomic::AtomicU32,
    serving_url: std::sync::RwLock<String>,
    service_urls: std::sync::RwLock<Vec<String>>,
    connection: Connection,
    hooks: Arc<std::sync::RwLock<Option<TsoDiscoveryHooks>>>,
}

impl TsoDiscoveryView {
    fn new(
        cluster_id: u64,
        route: &TsoRoute,
        service_urls: Vec<String>,
        connection: Connection,
        hooks: Arc<std::sync::RwLock<Option<TsoDiscoveryHooks>>>,
    ) -> Self {
        Self {
            cluster_id,
            keyspace_id: route.keyspace_id,
            keyspace_group_id: std::sync::atomic::AtomicU32::new(route.group_id.unwrap_or(0)),
            serving_url: std::sync::RwLock::new(route.endpoint.clone()),
            service_urls: std::sync::RwLock::new(service_urls),
            connection,
            hooks,
        }
    }

    fn factory(route: &TsoRoute) -> StreamBuilderFactory {
        if route.group_id.is_some() {
            StreamBuilderFactory::Ms
        } else {
            StreamBuilderFactory::Pd
        }
    }

    /// Publishes a new observation; answers (serving URL changed, members
    /// changed).
    fn update(&self, route: &TsoRoute, service_urls: Vec<String>) -> (bool, bool) {
        self.keyspace_group_id.store(
            route.group_id.unwrap_or(0),
            std::sync::atomic::Ordering::Release,
        );
        let leader_changed = {
            let mut serving = self.serving_url.write().unwrap();
            let changed = *serving != route.endpoint;
            serving.clone_from(&route.endpoint);
            changed
        };
        let members_changed = {
            let mut urls = self.service_urls.write().unwrap();
            let changed = *urls != service_urls;
            *urls = service_urls;
            changed
        };
        (leader_changed, members_changed)
    }
}

#[async_trait]
impl super::tso::ServiceDiscovery for TsoDiscoveryView {
    fn get_cluster_id(&self) -> u64 {
        self.cluster_id
    }

    fn get_keyspace_id(&self) -> u32 {
        self.keyspace_id
    }

    fn get_keyspace_group_id(&self) -> u32 {
        self.keyspace_group_id
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn get_service_urls(&self) -> Vec<String> {
        self.service_urls.read().unwrap().clone()
    }

    fn get_serving_url(&self) -> String {
        self.serving_url.read().unwrap().clone()
    }

    fn get_backup_urls(&self) -> Vec<String> {
        let serving = self.get_serving_url();
        self.get_service_urls()
            .into_iter()
            .filter(|url| *url != serving)
            .collect()
    }

    async fn get_or_create_grpc_conn(&self, url: &str) -> Result<Channel> {
        Ok(self.connection.channel(url).await?)
    }

    fn remove_client_conn(&self, url: &str) {
        self.connection.channels.remove(url);
    }

    fn schedule_check_member_changed(&self) {
        let hooks = self.hooks.read().unwrap().clone();
        if let Some(hooks) = hooks {
            (hooks.schedule_check_member_changed)();
        }
    }

    async fn check_member_changed(&self) -> Result<()> {
        let hooks = self.hooks.read().unwrap().clone();
        match hooks {
            Some(hooks) => (hooks.check_member_changed)().await,
            None => Ok(()),
        }
    }
}

macro_rules! pd_request {
    ($cluster_id:expr, $type:ty) => {{
        let mut request = <$type>::default();
        let mut header = pdpb::RequestHeader::default();
        header.cluster_id = $cluster_id;
        request.header = Some(header);
        request
    }};
}

// Construct each request while borrowing the published cluster, then retain only
// its connection and owned arguments across I/O. The static future lifetime
// prevents a retry caller from accidentally holding the cluster lock while waiting.
impl Cluster {
    pub(crate) fn service_mode(&self) -> super::service_discovery::ServiceModeDiscovery {
        self.discovery.service_mode()
    }

    fn rpc_client(&self) -> impl Future<Output = Result<RoutedPdClient>> + Send + 'static {
        let connection = self.connection.clone();
        let regions = self.region_service.clone();
        let leader = self
            .members
            .leader
            .as_ref()
            .and_then(|m| self.connection.member_url(m))
            .unwrap_or_default();
        let open = self.client.is_some();
        async move {
            if !open {
                return Err(Error::ContextCanceled);
            }
            connection
                .routed_client(&regions, &leader)
                .await
                .map_err(Error::from)
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Go `GetKeyspaceID()`: the TSO route's keyspace (the null keyspace
    /// without one).
    pub(crate) fn keyspace_id(&self) -> u32 {
        self.route.keyspace_id
    }

    pub fn get_region(
        &self,
        key: Vec<u8>,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetRegionResponse>> + Send + 'static {
        self.get_region_with_buckets(key, timeout, false)
    }

    pub fn get_region_with_buckets(
        &self,
        key: Vec<u8>,
        timeout: Duration,
        need_buckets: bool,
    ) -> impl Future<Output = Result<pdpb::GetRegionResponse>> + Send + 'static {
        self.get_region_routed(key, timeout, need_buckets, false, false)
    }

    /// Region-cache lookup with explicit follower permission and previous-key selection.
    pub fn get_region_routed(
        &self,
        key: Vec<u8>,
        timeout: Duration,
        need_buckets: bool,
        previous: bool,
        allow_follower: bool,
    ) -> impl Future<Output = Result<pdpb::GetRegionResponse>> + Send + 'static {
        let mut req = pd_request!(self.id, pdpb::GetRegionRequest);
        req.region_key = key;
        req.need_buckets = need_buckets;
        self.region_request(
            req,
            timeout,
            allow_follower,
            move |mut client, request| async move {
                if previous {
                    client
                        .get_prev_region(request)
                        .await
                        .map(tonic::Response::into_inner)
                } else {
                    client
                        .get_region(request)
                        .await
                        .map(tonic::Response::into_inner)
                }
            },
        )
    }

    pub fn get_prev_region(
        &self,
        key: Vec<u8>,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetRegionResponse>> + Send + 'static {
        self.get_prev_region_with_buckets(key, timeout, false)
    }

    pub fn get_prev_region_with_buckets(
        &self,
        key: Vec<u8>,
        timeout: Duration,
        need_buckets: bool,
    ) -> impl Future<Output = Result<pdpb::GetRegionResponse>> + Send + 'static {
        self.get_region_routed(key, timeout, need_buckets, true, false)
    }

    pub fn get_region_by_id(
        &self,
        id: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetRegionResponse>> + Send + 'static {
        self.get_region_by_id_with_buckets(id, timeout, false)
    }

    pub fn get_region_by_id_with_buckets(
        &self,
        id: u64,
        timeout: Duration,
        need_buckets: bool,
    ) -> impl Future<Output = Result<pdpb::GetRegionResponse>> + Send + 'static {
        self.get_region_by_id_routed(id, timeout, need_buckets, false)
    }

    /// Region-ID lookup with explicit per-request follower permission.
    pub fn get_region_by_id_routed(
        &self,
        id: u64,
        timeout: Duration,
        need_buckets: bool,
        allow_follower: bool,
    ) -> impl Future<Output = Result<pdpb::GetRegionResponse>> + Send + 'static {
        let mut req = pd_request!(self.id, pdpb::GetRegionByIdRequest);
        req.region_id = id;
        req.need_buckets = need_buckets;
        self.region_request(
            req,
            timeout,
            allow_follower,
            |mut client, request| async move {
                client
                    .get_region_by_id(request)
                    .await
                    .map(tonic::Response::into_inner)
            },
        )
    }

    /// Fetches at most `limit` consecutive PD regions from `start_key` through
    /// `end_key` (empty end means positive infinity). This is the PD RPC used
    /// by client-go `RegionCache.BatchLoadRegionsFromKey`.
    pub fn scan_regions(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::ScanRegionsResponse>> + Send + 'static {
        self.scan_regions_routed(start_key, end_key, limit, timeout, false)
    }

    /// Legacy scan excludes router service but can explicitly permit PD followers.
    pub fn scan_regions_routed(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
        timeout: Duration,
        allow_follower: bool,
    ) -> impl Future<Output = Result<pdpb::ScanRegionsResponse>> + Send + 'static {
        let mut req = pd_request!(self.id, pdpb::ScanRegionsRequest);
        req.start_key = start_key;
        req.end_key = end_key;
        req.limit = i32::try_from(limit).unwrap_or(i32::MAX);
        self.region_request(
            req,
            timeout,
            allow_follower,
            |mut client, request| async move {
                client
                    .scan_regions(request)
                    .await
                    .map(tonic::Response::into_inner)
            },
        )
    }

    pub fn batch_scan_regions(
        &self,
        ranges: Vec<pdpb::KeyRange>,
        limit: usize,
        options: super::retry::RegionScanOptions,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::BatchScanRegionsResponse>> + Send + 'static {
        let mut req = pd_request!(self.id, pdpb::BatchScanRegionsRequest);
        req.ranges = ranges;
        req.limit = i32::try_from(limit).unwrap_or(i32::MAX);
        req.need_buckets = options.need_buckets;
        req.contain_all_key_range = options.output_must_contain_all_key_range;
        // pd-client `getServiceClient`: router-service handling first.
        let router_node = options
            .allow_router_service_handle
            .then(|| self.connection.router_node())
            .flatten();
        let routed_req = router_node.as_ref().map(|_| req.clone());
        let future = self.region_request(
            req,
            timeout,
            options.allow_follower_handle,
            |mut client, request| async move {
                client
                    .batch_scan_regions(request)
                    .await
                    .map(tonic::Response::into_inner)
            },
        );
        async move {
            if let (Some(node), Some(req)) = (router_node, routed_req) {
                let mut request = super::region_service::region_request(req, true);
                request.set_timeout(timeout);
                let routed =
                    crate::proto::routerpb::router_client::RouterClient::new(node.channel.clone())
                        .batch_scan_regions(request)
                        .await
                        .map(tonic::Response::into_inner);
                // `serviceClient.NeedRetry`: any error falls back to PD.
                if let Ok(response) = routed {
                    if response.header.as_ref().is_none_or(|h| h.error.is_none()) {
                        return Ok(response);
                    }
                }
            }
            match future.await {
                Err(Error::GrpcAPI(status)) if status.code() == tonic::Code::Unimplemented => {
                    Err(Error::Unimplemented)
                }
                result => result,
            }
        }
    }

    fn region_request<T, R, F, Fut>(
        &self,
        value: T,
        timeout: Duration,
        allowed: bool,
        call: F,
    ) -> impl Future<Output = Result<R>> + Send + 'static
    where
        T: Clone + Send + Sync + 'static,
        R: PdResponse + Send + 'static,
        F: Fn(RoutedPdClient, Request<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = GrpcResult<R>> + Send,
    {
        let connection = self.connection.clone();
        let leader_client = self.rpc_client();
        let leader = self
            .members
            .leader
            .as_ref()
            .and_then(|member| self.connection.member_url(member))
            .unwrap_or_default();
        let urls = self
            .members
            .members
            .iter()
            .filter_map(|member| self.connection.member_url(member))
            .collect::<Vec<_>>();
        let target = self.region_service.select_with_forwarding(
            &leader,
            &urls,
            connection.options.get_enable_follower_handle(),
            allowed,
            connection.options.enable_forwarding,
        );
        async move {
            let deadline = tokio::time::Instant::now() + timeout;
            let request = async {
                let first = async {
                    let client = pdpb::pd_client::PdClient::with_interceptor(
                        connection.channel(&target.endpoint).await?,
                        super::region_service::Forwarding(target.forwarded_host.clone()),
                    );
                    let mut request = target.request(value.clone());
                    request.set_timeout(
                        deadline.saturating_duration_since(tokio::time::Instant::now()),
                    );
                    call(client, request).await
                }
                .await;
                let retry = target.observe_error(
                    first.is_err(),
                    first
                        .as_ref()
                        .ok()
                        .and_then(|r| r.header())
                        .and_then(|h| h.error.as_ref())
                        .map(|error| error.r#type),
                );
                let response = if retry {
                    let leader_client = leader_client.await?;
                    let mut request = Request::new(value);
                    request.set_timeout(
                        deadline.saturating_duration_since(tokio::time::Instant::now()),
                    );
                    call(leader_client, request).await?
                } else {
                    first?
                };
                if let Some(error) = response.header().and_then(|h| h.error.as_ref()) {
                    Err(internal_err!(error.message))
                } else {
                    Ok(response)
                }
            };
            tokio::time::timeout_at(deadline, request)
                .await
                .map_err(|_| {
                    Error::GrpcAPI(tonic::Status::deadline_exceeded(
                        "PD region request timed out",
                    ))
                })?
        }
    }

    pub fn split_regions(
        &self,
        split_keys: Vec<Vec<u8>>,
        retry_limit: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::SplitRegionsResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::SplitRegionsRequest);
            req.split_keys = split_keys;
            req.retry_limit = retry_limit;
            req.send(&mut client, timeout).await
        }
    }

    pub fn get_store(
        &self,
        id: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetStoreResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::GetStoreRequest);
            req.store_id = id;
            req.send(&mut client, timeout).await
        }
    }

    pub fn get_all_stores(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetAllStoresResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let req = pd_request!(cluster_id, pdpb::GetAllStoresRequest);
            req.send(&mut client, timeout).await
        }
    }

    /// pd-client `GetAllStores(ctx, WithExcludeTombstone(),
    /// WithAllowRouterServiceHandleStoreRequest())`: a healthy router node
    /// answers (`routerpb.Router/GetAllStores`, as a follower-handled
    /// request) when there is one; PD otherwise, and when the node fails
    /// (`serviceClient.NeedRetry`).
    pub fn get_all_stores_allowing_router_service(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetAllStoresResponse>> + Send + 'static {
        let mut req = pd_request!(self.id, pdpb::GetAllStoresRequest);
        req.exclude_tombstone_stores = true;
        let node = self.connection.router_node();
        let leader = self.rpc_client();
        async move {
            if let Some(node) = node {
                let mut request = super::region_service::region_request(req.clone(), true);
                request.set_timeout(timeout);
                let routed =
                    crate::proto::routerpb::router_client::RouterClient::new(node.channel.clone())
                        .get_all_stores(request)
                        .await
                        .map(tonic::Response::into_inner);
                match routed {
                    Ok(response) if response.header.as_ref().is_none_or(|h| h.error.is_none()) => {
                        return Ok(response)
                    }
                    _ => {}
                }
            }
            let mut client = leader.await?;
            req.send(&mut client, timeout).await
        }
    }

    /// pd-client `GetTSAsync(ctx).Wait()` over the current TSO client.
    pub fn get_timestamp(&self) -> impl Future<Output = Result<Timestamp>> + Send + 'static {
        let slot = self.tso.clone();
        async move { super::tso::get_ts_async(move || slot.read().unwrap().clone()).await }
    }

    /// The slot holding the current TSO client (Go `getTSOClient()`).
    pub(crate) fn tso_slot(&self) -> TsoSlot {
        self.tso.clone()
    }

    /// Installs the owner's membership-check hooks for every TSO client.
    pub(crate) fn set_tso_discovery_hooks(&self, hooks: TsoDiscoveryHooks) {
        *self.tso_hooks.write().unwrap() = Some(hooks);
    }

    pub fn get_min_timestamp(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<Timestamp>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        let classic = self.route.group_id.is_none();
        let ordinary = self.get_timestamp();
        async move {
            // Go GetMinTS uses the ordinary provider in classic mode. API
            // compatibility falls back only when GetMinTS is unsupported.
            if classic {
                return ordinary.await;
            }
            let mut client = client.await?;
            let request = pd_request!(cluster_id, pdpb::GetMinTsRequest);
            match request.send(&mut client, timeout).await {
                Err(Error::GrpcAPI(status)) if status.code() == tonic::Code::Unimplemented => {
                    ordinary.await
                }
                result => result?.timestamp.ok_or_else(|| {
                    Error::StringError("PD GetMinTS response has no timestamp".to_owned())
                }),
            }
        }
    }

    pub fn set_external_timestamp(
        &self,
        timestamp: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut request = pd_request!(cluster_id, pdpb::SetExternalTimestampRequest);
            request.timestamp = timestamp;
            request.send(&mut client, timeout).await.map(|_| ())
        }
    }

    pub fn get_external_timestamp(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<u64>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let request = pd_request!(cluster_id, pdpb::GetExternalTimestampRequest);
            request
                .send(&mut client, timeout)
                .await
                .map(|response: pdpb::GetExternalTimestampResponse| response.timestamp)
        }
    }

    pub fn update_safepoint(
        &self,
        safepoint: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::UpdateGcSafePointResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::UpdateGcSafePointRequest);
            req.safe_point = safepoint;
            req.send(&mut client, timeout).await
        }
    }

    /// PD `UpdateGCSafePointV2`: a keyspace's GC safe point, never lowered.
    pub fn update_gc_safe_point_v2(
        &self,
        keyspace_id: u32,
        safe_point: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::UpdateGcSafePointV2Response>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::UpdateGcSafePointV2Request);
            req.keyspace = Some(pdpb::update_gc_safe_point_v2_request::Keyspace::KeyspaceId(
                keyspace_id,
            ));
            req.safe_point = safe_point;
            req.send(&mut client, timeout).await
        }
    }

    /// PD `UpdateServiceGCSafePoint`: sets (or, with a zero TTL, removes)
    /// one service's safe point and answers the minimal one.
    pub fn update_service_gc_safe_point(
        &self,
        service_id: &str,
        ttl: i64,
        safe_point: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::UpdateServiceGcSafePointResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        let service_id = service_id.as_bytes().to_vec();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::UpdateServiceGcSafePointRequest);
            req.service_id = service_id;
            req.ttl = ttl;
            req.safe_point = safe_point;
            req.send(&mut client, timeout).await
        }
    }

    /// PD `UpdateServiceSafePointV2`: `update_service_gc_safe_point` for a
    /// keyspace.
    pub fn update_service_safe_point_v2(
        &self,
        keyspace_id: u32,
        service_id: &str,
        ttl: i64,
        safe_point: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::UpdateServiceSafePointV2Response>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        let service_id = service_id.as_bytes().to_vec();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::UpdateServiceSafePointV2Request);
            req.keyspace =
                Some(pdpb::update_service_safe_point_v2_request::Keyspace::KeyspaceId(keyspace_id));
            req.service_id = service_id;
            req.ttl = ttl;
            req.safe_point = safe_point;
            req.send(&mut client, timeout).await
        }
    }

    pub fn get_gc_state(
        &self,
        keyspace_id: u32,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetGcStateResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::GetGcStateRequest);
            req.keyspace_scope = Some(keyspace_scope(keyspace_id));
            req.exclude_gc_barriers = true;
            req.send(&mut client, timeout).await
        }
    }

    pub fn advance_txn_safe_point(
        &self,
        keyspace_id: u32,
        target: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::AdvanceTxnSafePointResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::AdvanceTxnSafePointRequest);
            req.keyspace_scope = Some(keyspace_scope(keyspace_id));
            req.target = target;
            req.send(&mut client, timeout).await
        }
    }

    pub fn advance_gc_safe_point(
        &self,
        keyspace_id: u32,
        target: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::AdvanceGcSafePointResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::AdvanceGcSafePointRequest);
            req.keyspace_scope = Some(keyspace_scope(keyspace_id));
            req.target = target;
            req.send(&mut client, timeout).await
        }
    }

    pub fn scatter_regions(
        &self,
        region_ids: Vec<u64>,
        group: String,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::ScatterRegionResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::ScatterRegionRequest);
            req.regions_id = region_ids;
            req.group = group;
            req.send(&mut client, timeout).await
        }
    }

    pub fn get_operator(
        &self,
        region_id: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetOperatorResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pd_request!(cluster_id, pdpb::GetOperatorRequest);
            req.region_id = region_id;
            req.send(&mut client, timeout).await
        }
    }

    /// PD `GetAllKeyspaces`: up to `limit` keyspaces whose ids are at least
    /// `start_id`, in id order.
    pub fn get_all_keyspaces(
        &self,
        start_id: u32,
        limit: u32,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<keyspacepb::KeyspaceMeta>>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.keyspace_client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
            let mut req = pd_request!(cluster_id, keyspacepb::GetAllKeyspacesRequest);
            req.start_keyspace =
                Some(keyspacepb::get_all_keyspaces_request::StartKeyspace::StartId(start_id));
            req.limit = limit;
            Ok(req.send(&mut client, timeout).await?.keyspaces)
        }
    }

    pub fn load_keyspace(
        &self,
        keyspace: &str,
        timeout: Duration,
    ) -> impl Future<Output = Result<keyspacepb::KeyspaceMeta>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.keyspace_client.clone();
        let keyspace = keyspace.to_owned();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
            let mut req = pd_request!(cluster_id, keyspacepb::LoadKeyspaceRequest);
            req.name = keyspace.to_string();
            let resp = req.send(&mut client, timeout).await?;
            let keyspace = resp
                .keyspace
                .ok_or_else(|| Error::KeyspaceNotFound(keyspace.to_owned()))?;
            Ok(keyspace)
        }
    }
}

impl Cluster {
    fn leader_url(&self) -> Option<String> {
        self.members
            .leader
            .as_ref()
            .and_then(|member| self.connection.member_url(member))
    }

    /// Go `serviceDiscovery.GetAllServiceClients` over the accepted membership:
    /// the leader's matched client URL first (`true`), then the matched URL of
    /// every other member ID, one client per URL as Go's `updateFollowers`
    /// keys its followers.
    pub(crate) fn service_client_urls(&self) -> Vec<(String, bool)> {
        let leader_id = self.members.leader.as_ref().map(|member| member.member_id);
        let mut clients = Vec::with_capacity(self.members.members.len());
        if let Some(url) = self.leader_url() {
            clients.push((url, true));
        }
        let mut followers: Vec<String> = Vec::new();
        for member in &self.members.members {
            if Some(member.member_id) == leader_id {
                continue;
            }
            let Some(url) = self.connection.member_url(member) else {
                continue;
            };
            if !followers.contains(&url) {
                followers.push(url);
            }
        }
        clients.extend(followers.into_iter().map(|url| (url, false)));
        clients
    }

    /// pd-client `GetAllMembers`.
    pub fn get_all_members(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetMembersResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let req = pd_request!(cluster_id, pdpb::GetMembersRequest);
            req.send(&mut client, timeout).await
        }
    }

    /// pd-client `StoreGlobalConfig`.
    pub fn store_global_config(
        &self,
        config_path: String,
        changes: Vec<pdpb::GlobalConfigItem>,
        timeout: Duration,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let client = self.rpc_client();
        async move {
            let mut client = client.await?;
            let mut req = pdpb::StoreGlobalConfigRequest {
                changes,
                config_path,
            }
            .into_request();
            req.set_timeout(timeout);
            // Go's StoreGlobalConfig returns only the transport status.
            client.store_global_config(req).await?;
            Ok(())
        }
    }

    /// pd-client's resource-manager client over the PD leader (the resource
    /// manager is a PD service unless it runs as a microservice).
    /// pd-client `resourceManagerReadClient`: the standalone resource
    /// manager once discovered, PD otherwise.
    fn resource_manager_read_client(
        &self,
    ) -> impl Future<
        Output = Result<
            crate::proto::resource_manager::resource_manager_client::ResourceManagerClient<Channel>,
        >,
    > + Send
           + 'static {
        let standalone = self.connection.resource_manager_service();
        let pd = self.resource_manager_client();
        async move {
            match standalone {
                Some((_, conn)) => Ok(
                    crate::proto::resource_manager::resource_manager_client::ResourceManagerClient::new(conn),
                ),
                None => pd.await,
            }
        }
    }

    /// pd-client `resourceManagerWriteClient`: PD's serving member.
    fn resource_manager_client(
        &self,
    ) -> impl Future<
        Output = Result<
            crate::proto::resource_manager::resource_manager_client::ResourceManagerClient<Channel>,
        >,
    > + Send
           + 'static {
        let connection = self.connection.clone();
        let leader = self.leader_url();
        async move {
            let leader = leader.ok_or_else(|| internal_err!("no PD leader"))?;
            Ok(
                crate::proto::resource_manager::resource_manager_client::ResourceManagerClient::new(
                    connection.channel(&leader).await?,
                ),
            )
        }
    }

    /// pd-client `ListResourceGroups`.
    pub fn list_resource_groups(
        &self,
        keyspace_id: u32,
        with_ru_stats: bool,
        timeout: Duration,
    ) -> impl Future<Output = Result<Vec<crate::proto::resource_manager::ResourceGroup>>> + Send + 'static
    {
        let client = self.resource_manager_read_client();
        let connection = self.connection.clone();
        async move {
            let mut client = client.await?;
            let mut req = crate::proto::resource_manager::ListResourceGroupsRequest {
                with_ru_stats,
                keyspace_id: Some(resource_keyspace(keyspace_id)),
            }
            .into_request();
            req.set_timeout(timeout);
            let response = client
                .list_resource_groups(req)
                .await
                .map_err(|status| {
                    connection.resource_manager_error_handler(&status);
                    internal_err!("get all resource group failed, {}", status.message())
                })?
                .into_inner();
            match response.error {
                Some(error) => Err(internal_err!(
                    "get all resource group failed, {}",
                    error.message
                )),
                None => Ok(response.groups),
            }
        }
    }

    /// pd-client `AddResourceGroup` (`add`) or `ModifyResourceGroup`.
    pub fn put_resource_group(
        &self,
        keyspace_id: u32,
        mut group: crate::proto::resource_manager::ResourceGroup,
        add: bool,
        timeout: Duration,
    ) -> impl Future<Output = Result<String>> + Send + 'static {
        let client = self.resource_manager_client();
        async move {
            // Ensure the group carries the client's keyspace, as Go does.
            match &group.keyspace_id {
                None => group.keyspace_id = Some(resource_keyspace(keyspace_id)),
                Some(value) => {
                    let inner = match value.keyspace {
                        Some(
                            crate::proto::resource_manager::keyspace_id_value::Keyspace::Value(id),
                        ) => id,
                        _ => crate::request::NULL_KEYSPACE_ID,
                    };
                    if inner != keyspace_id {
                        return Err(internal_err!(
                            "the keyspace ID of the resource group {} mismatches the client's {}",
                            inner,
                            keyspace_id
                        ));
                    }
                }
            }
            let mut client = client.await?;
            let mut req =
                crate::proto::resource_manager::PutResourceGroupRequest { group: Some(group) }
                    .into_request();
            req.set_timeout(timeout);
            let response = if add {
                client.add_resource_group(req).await?
            } else {
                client.modify_resource_group(req).await?
            }
            .into_inner();
            match response.error {
                Some(error) => Err(internal_err!("[resource_manager] {}", error.message)),
                None => Ok(response.body),
            }
        }
    }

    /// pd-client `DeleteResourceGroup`.
    pub fn delete_resource_group(
        &self,
        keyspace_id: u32,
        name: String,
        timeout: Duration,
    ) -> impl Future<Output = Result<String>> + Send + 'static {
        let client = self.resource_manager_client();
        async move {
            let mut client = client.await?;
            let mut req = crate::proto::resource_manager::DeleteResourceGroupRequest {
                resource_group_name: name,
                keyspace_id: Some(resource_keyspace(keyspace_id)),
            }
            .into_request();
            req.set_timeout(timeout);
            let response = client.delete_resource_group(req).await?.into_inner();
            match response.error {
                Some(error) => Err(internal_err!("[resource_manager] {}", error.message)),
                None => Ok(response.body),
            }
        }
    }

    /// The URL the resource-manager token stream connects to: the
    /// standalone resource manager once discovered, PD's leader otherwise
    /// (Go `resourceManagerTokenClient`).
    pub(crate) fn resource_manager_leader_url(&self) -> Option<String> {
        match self.connection.resource_manager_service() {
            Some((url, _)) => Some(url),
            None => self.leader_url(),
        }
    }

    /// pd-client's resource-manager token client and the leader URL it uses.
    pub(crate) fn resource_manager_token_client(
        &self,
    ) -> impl Future<
        Output = Result<(
            crate::proto::resource_manager::resource_manager_client::ResourceManagerClient<Channel>,
            String,
        )>,
    > + Send
           + 'static {
        let connection = self.connection.clone();
        let standalone = self.connection.resource_manager_service();
        let leader = self.leader_url();
        async move {
            if let Some((url, conn)) = standalone {
                return Ok((
                    crate::proto::resource_manager::resource_manager_client::ResourceManagerClient::new(conn),
                    url,
                ));
            }
            let leader = leader.ok_or_else(|| internal_err!("no PD leader"))?;
            let channel = connection.channel(&leader).await?;
            Ok((
                crate::proto::resource_manager::resource_manager_client::ResourceManagerClient::new(
                    channel,
                ),
                leader,
            ))
        }
    }

    /// pd-client `GetResourceGroup`: one unretried read. RPC failures carry
    /// the gRPC status as the cause of `ErrClientGetResourceGroup`; `None`
    /// is a response without a group.
    pub fn get_resource_group(
        &self,
        keyspace_id: u32,
        name: String,
        with_ru_stats: bool,
    ) -> impl Future<Output = Result<Option<crate::proto::resource_manager::ResourceGroup>>>
           + Send
           + 'static {
        let client = self.resource_manager_read_client();
        let connection = self.connection.clone();
        async move {
            let mut client = client.await?;
            let req = crate::proto::resource_manager::GetResourceGroupRequest {
                resource_group_name: name.clone(),
                with_ru_stats,
                keyspace_id: Some(resource_keyspace(keyspace_id)),
            };
            let response = match client.get_resource_group(req).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    connection.resource_manager_error_handler(&status);
                    return Err(Error::PdResourceGroup(
                        super::errs::ClientGetResourceGroupError {
                            resource_group_name: name,
                            cause: grpc_error_text(&status),
                            error: Some(Arc::new(status)),
                        },
                    ));
                }
            };
            if let Some(error) = response.error {
                return Err(Error::PdResourceGroup(
                    super::errs::ClientGetResourceGroupError {
                        resource_group_name: name,
                        cause: error.message,
                        error: None,
                    },
                ));
            }
            Ok(response.group)
        }
    }

    /// pd-client's meta storage client on the serving PD member.
    fn meta_storage_client(
        &self,
    ) -> impl Future<
        Output = Result<
            crate::proto::meta_storagepb::meta_storage_client::MetaStorageClient<Channel>,
        >,
    > + Send
           + 'static {
        let connection = self.connection.clone();
        let leader = self.leader_url();
        async move {
            let leader = leader.ok_or_else(|| {
                Error::Pd(super::errs::ERR_CLIENT_GET_META_STORAGE_CLIENT.error())
            })?;
            Ok(
                crate::proto::meta_storagepb::meta_storage_client::MetaStorageClient::new(
                    connection.channel(&leader).await?,
                ),
            )
        }
    }

    /// pd-client meta storage `Get`, bounded by the client timeout.
    pub fn meta_storage_get(
        &self,
        request: crate::proto::meta_storagepb::GetRequest,
        timeout: Duration,
    ) -> impl Future<Output = Result<crate::proto::meta_storagepb::GetResponse>> + Send + 'static
    {
        let client = self.meta_storage_client();
        async move {
            let mut client = client.await?;
            let mut req = request.into_request();
            req.set_timeout(timeout);
            Ok(client.get(req).await?.into_inner())
        }
    }

    /// pd-client meta storage `Put`, bounded by the client timeout.
    pub fn meta_storage_put(
        &self,
        request: crate::proto::meta_storagepb::PutRequest,
        timeout: Duration,
    ) -> impl Future<Output = Result<crate::proto::meta_storagepb::PutResponse>> + Send + 'static
    {
        let client = self.meta_storage_client();
        async move {
            let mut client = client.await?;
            let mut req = request.into_request();
            req.set_timeout(timeout);
            Ok(client.put(req).await?.into_inner())
        }
    }

    /// pd-client meta storage `Watch`: the opened response stream.
    pub fn meta_storage_watch(
        &self,
        request: crate::proto::meta_storagepb::WatchRequest,
    ) -> impl Future<
        Output = Result<tonic::codec::Streaming<crate::proto::meta_storagepb::WatchResponse>>,
    > + Send
           + 'static {
        let client = self.meta_storage_client();
        async move {
            let mut client = client.await?;
            Ok(client.watch(request).await?.into_inner())
        }
    }
}

/// grpc-go's `status.Error` text, `rpc error: code = <Code> desc = <message>`.
pub(crate) fn grpc_error_text(status: &tonic::Status) -> String {
    let code = match status.code() {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "Canceled",
        tonic::Code::Unknown => "Unknown",
        tonic::Code::InvalidArgument => "InvalidArgument",
        tonic::Code::DeadlineExceeded => "DeadlineExceeded",
        tonic::Code::NotFound => "NotFound",
        tonic::Code::AlreadyExists => "AlreadyExists",
        tonic::Code::PermissionDenied => "PermissionDenied",
        tonic::Code::ResourceExhausted => "ResourceExhausted",
        tonic::Code::FailedPrecondition => "FailedPrecondition",
        tonic::Code::Aborted => "Aborted",
        tonic::Code::OutOfRange => "OutOfRange",
        tonic::Code::Unimplemented => "Unimplemented",
        tonic::Code::Internal => "Internal",
        tonic::Code::Unavailable => "Unavailable",
        tonic::Code::DataLoss => "DataLoss",
        tonic::Code::Unauthenticated => "Unauthenticated",
    };
    format!("rpc error: code = {code} desc = {}", status.message())
}

/// The resource manager's keyspace field: Go's client always sets it, the
/// null keyspace as `uint32.Max`.
fn resource_keyspace(keyspace_id: u32) -> crate::proto::resource_manager::KeyspaceIdValue {
    crate::proto::resource_manager::KeyspaceIdValue {
        keyspace: Some(
            crate::proto::resource_manager::keyspace_id_value::Keyspace::Value(keyspace_id),
        ),
    }
}

impl Cluster {
    fn update_region_members(&self) {
        let leader = self
            .members
            .leader
            .as_ref()
            .and_then(|member| self.connection.member_url(member))
            .unwrap_or_default();
        let urls = self
            .members
            .members
            .iter()
            .filter_map(|member| self.connection.member_url(member))
            .collect::<Vec<_>>();
        self.region_service.update_members(&leader, &urls);
    }

    pub(crate) fn check_health(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = ()> + Send + 'static {
        let service = self.region_service.clone();
        let connection = self.connection.clone();
        async move {
            service
                .check_health(timeout, |endpoint| {
                    let connection = connection.clone();
                    async move { connection.channel(&endpoint).await }
                })
                .await;
        }
    }

    pub(crate) fn install_leader(&mut self, leader: LeaderConnection) -> Result<()> {
        if self.client.is_none() {
            return Err(Error::ContextCanceled);
        }
        self.client = Some(leader.client);
        self.keyspace_client = Some(leader.keyspace_client);
        let previous = std::mem::replace(&mut self.members, leader.members);
        self.log_member_changes(Some(&previous));
        self.update_region_members();
        Ok(())
    }

    /// The members' client URLs, sorted (pd-client `GetServiceURLs`).
    pub(crate) fn service_urls(&self) -> Vec<String> {
        sorted_member_urls(&self.members)
    }

    /// pd-client `updateURLs` and `switchLeader`'s log lines for a new
    /// membership observation (`previous` is `None` at initialization).
    fn log_member_changes(&self, previous: Option<&pdpb::GetMembersResponse>) {
        let old_urls = previous.map(sorted_member_urls).unwrap_or_default();
        let new_urls = sorted_member_urls(&self.members);
        if old_urls != new_urls {
            log::info!(
                "old-sorted-urls":serde = old_urls,
                "new-sorted-urls":serde = new_urls;
                "[pd] update member urls"
            );
        }
        let leader_url = |members: &pdpb::GetMembersResponse| {
            members
                .leader
                .as_ref()
                .and_then(|member| self.connection.member_url(member))
                .unwrap_or_default()
        };
        let old_leader = previous.map(leader_url).unwrap_or_default();
        let new_leader = leader_url(&self.members);
        if old_leader != new_leader {
            log::info!(
                "new-leader" = new_leader.as_str(),
                "old-leader" = old_leader.as_str();
                "[pd] switch leader"
            );
        }
    }

    /// Publishes a TSO routing observation. A provider change (PD vs. TSO
    /// microservice, or another keyspace) is pd-client's
    /// `resetTSOClientLocked`: a new TSO client replaces the old one. Within
    /// a provider, a new serving URL is the leader-switched callback and new
    /// members the members-changed callback of the current client.
    pub(crate) fn install_timestamp(
        &mut self,
        (discovery, route, service_urls): TimestampConnection,
        timeout: Duration,
    ) -> Result<impl Future<Output = ()> + Send + 'static> {
        if self.client.is_none() {
            return Err(Error::ContextCanceled);
        }
        let current = self.tso.read().unwrap().clone();
        let provider_changed = TsoDiscoveryView::factory(&route)
            != TsoDiscoveryView::factory(&self.route)
            || route.keyspace_id != self.route.keyspace_id
            || current
                .as_ref()
                .is_none_or(|client| client.ctx().is_cancelled());
        let mut setup = None;
        if provider_changed {
            let (client, view) = self.new_tso_client(&route, service_urls, timeout);
            log::info!(
                "{}",
                match TsoDiscoveryView::factory(&route) {
                    StreamBuilderFactory::Pd => "[pd] tso provider changed to pd",
                    StreamBuilderFactory::Ms => "[pd] tso provider changed to tso server",
                }
            );
            *self.tso.write().unwrap() = Some(Arc::clone(&client));
            self.tso_view = view;
            self.retired_tso.extend(current);
            setup = Some(client);
        } else if let Some(client) = &current {
            let (leader_changed, members_changed) = self.tso_view.update(&route, service_urls);
            if members_changed {
                client.schedule_update_tso_connection_ctxs();
            }
            if leader_changed {
                client.update_tso_leader_url(&route.endpoint);
            }
        }
        self.discovery = discovery;
        self.route = route;
        let retire = self.retire_streams();
        Ok(async move {
            if let Some(client) = setup {
                client.setup().await;
            }
            retire.await;
        })
    }

    /// pd-client `tso.NewClient` + the leader-switched callback's first run.
    fn new_tso_client(
        &self,
        route: &TsoRoute,
        service_urls: Vec<String>,
        timeout: Duration,
    ) -> (Arc<TsoClient>, Arc<TsoDiscoveryView>) {
        let view = Arc::new(TsoDiscoveryView::new(
            self.id,
            route,
            service_urls,
            self.connection.clone(),
            self.tso_hooks.clone(),
        ));
        let client = TsoClient::new(
            &Cancellation::default(),
            self.connection.options.clone(),
            timeout,
            view.clone(),
            TsoDiscoveryView::factory(route),
        );
        client.update_tso_leader_url(&route.endpoint);
        (client, view)
    }

    fn retire_streams(&self) -> impl Future<Output = ()> + Send + 'static {
        let retired = self.retired_tso.clone();
        async move {
            for client in retired {
                client.close().await;
            }
        }
    }

    pub(crate) fn start_close(&mut self) -> impl Future<Output = ()> + Send + 'static {
        self.channels.close();
        self.client.take();
        self.keyspace_client.take();
        let current = self.tso.write().unwrap().take();
        self.retired_tso.extend(current);
        self.retire_streams()
    }

    #[cfg(test)]
    pub(crate) fn tso_for_test(&self) -> Arc<TsoClient> {
        self.tso.read().unwrap().clone().unwrap()
    }

    pub(crate) fn finish_retirement(&mut self) {
        self.retired_tso.clear();
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.channels.close();
        if let Some(client) = self.tso.write().unwrap().take() {
            client.ctx().cancel();
        }
        for client in &self.retired_tso {
            client.ctx().cancel();
        }
    }
}

fn sorted_member_urls(members: &pdpb::GetMembersResponse) -> Vec<String> {
    let mut urls = members
        .members
        .iter()
        .flat_map(|member| member.client_urls.iter().cloned())
        .collect::<Vec<_>>();
    urls.sort();
    urls
}

/// Go `ServiceDiscovery.GetServiceURLs` for the TSO client: PD's sorted
/// member URLs in PD mode, the keyspace group's members in microservice mode.
fn tso_service_urls(
    discovery: &TsoDiscovery,
    route: &TsoRoute,
    members: &pdpb::GetMembersResponse,
) -> Vec<String> {
    if route.group_id.is_some() {
        discovery.service_urls().to_vec()
    } else {
        sorted_member_urls(members)
    }
}

fn keyspace_scope(keyspace_id: u32) -> pdpb::KeyspaceScope {
    pdpb::KeyspaceScope {
        keyspace: Some(pdpb::keyspace_scope::Keyspace::KeyspaceId(keyspace_id)),
    }
}

pub(crate) struct LeaderConnection {
    client: RoutedPdClient,
    keyspace_client: keyspacepb::keyspace_client::KeyspaceClient<Channel>,
    members: pdpb::GetMembersResponse,
}

type TimestampConnection = (TsoDiscovery, TsoRoute, Vec<String>);

/// An object for connecting and reconnecting to a PD cluster.
#[derive(Clone)]
pub struct Connection {
    pub(crate) options: Arc<super::opt::Options>,
    security_mgr: Arc<SecurityManager>,
    channels: Arc<ChannelCache>,
    // Initialization retries share probe selection before a Cluster exists.
    discovery: TsoDiscovery,
    /// pd-client `innerClient.msDiscovery`: the router service discovery,
    /// once the owning client has started it.
    router_discovery: Arc<
        std::sync::RwLock<Option<Arc<super::router_service_discovery::RouterServiceDiscovery>>>,
    >,
    /// pd-client `innerClient.resourceManagerDiscovery` (microservice mode).
    rm_discovery: Arc<
        std::sync::RwLock<Option<Arc<super::resource_manager_discovery::ResourceManagerDiscovery>>>,
    >,
}

impl Connection {
    fn member_url(&self, member: &pdpb::Member) -> Option<String> {
        super::service_discovery::pick_service_url(
            &member.client_urls,
            self.security_mgr.tls_configured(),
        )
    }

    async fn routed_client(
        &self,
        regions: &super::region_service::RegionService,
        endpoint: &str,
    ) -> GrpcResult<RoutedPdClient> {
        let (endpoint, forwarding) =
            regions.forwarding_target(endpoint, self.options.enable_forwarding);
        Ok(pdpb::pd_client::PdClient::with_interceptor(
            self.channel(&endpoint).await?,
            forwarding,
        ))
    }

    pub(crate) fn security_manager(&self) -> Arc<SecurityManager> {
        self.security_mgr.clone()
    }

    pub fn new(security_mgr: Arc<SecurityManager>) -> Connection {
        Connection {
            options: Arc::new(super::opt::Options::new()),
            security_mgr,
            channels: Arc::new(ChannelCache::default()),
            discovery: TsoDiscovery::default(),
            router_discovery: Arc::default(),
            rm_discovery: Arc::default(),
        }
    }

    /// Publishes (or clears) the resource manager discovery.
    pub(crate) fn set_resource_manager_discovery(
        &self,
        discovery: Option<Arc<super::resource_manager_discovery::ResourceManagerDiscovery>>,
    ) -> Option<Arc<super::resource_manager_discovery::ResourceManagerDiscovery>> {
        std::mem::replace(&mut *self.rm_discovery.write().unwrap(), discovery)
    }

    /// The resource manager discovery, in microservice mode.
    pub(crate) fn resource_manager_discovery(
        &self,
    ) -> Option<Arc<super::resource_manager_discovery::ResourceManagerDiscovery>> {
        self.rm_discovery.read().unwrap().clone()
    }

    /// pd-client `resourceManagerErrorHandler`: an RPC error that suggests
    /// the resource manager moved asks the discovery to look again.
    pub(crate) fn resource_manager_error_handler(&self, status: &tonic::Status) {
        let error = Error::GrpcAPI(status.clone());
        if !super::resource_manager_discovery::should_update_rm_url(&error) {
            return;
        }
        log::warn!(error:% = error; "[resource-manager] resource manager error");
        if let Some(discovery) = self.resource_manager_discovery() {
            discovery.schedule_update_service_url();
        }
    }

    /// The standalone resource manager's (URL, connection), once discovered.
    fn resource_manager_service(&self) -> Option<(String, Channel)> {
        let discovery = self.resource_manager_discovery()?;
        let conn = discovery.get_conn()?;
        Some((discovery.get_service_url(), conn))
    }

    /// Publishes the router service discovery the owning client started.
    pub(crate) fn set_router_discovery(
        &self,
        discovery: Option<Arc<super::router_service_discovery::RouterServiceDiscovery>>,
    ) {
        *self.router_discovery.write().unwrap() = discovery;
    }

    /// Go `msDiscovery.GetServiceClient()`: a healthy router node, if any.
    fn router_node(&self) -> Option<Arc<super::router_service_discovery::RouterNode>> {
        self.router_discovery
            .read()
            .unwrap()
            .as_ref()
            .and_then(|discovery| discovery.get_service_client())
    }

    /// Go `GetOrCreateGRPCConn` over the shared connection cache.
    pub(crate) async fn get_or_create_channel(
        &self,
        url: &str,
    ) -> std::result::Result<Channel, tonic::Status> {
        self.channel(url).await
    }

    /// Go `RemoveClientConn` over the shared connection cache.
    pub(crate) fn remove_channel(&self, url: &str) {
        self.channels.remove(url);
    }

    pub(crate) fn tls_configured(&self) -> bool {
        self.security_mgr.tls_configured()
    }

    pub async fn connect_cluster(
        &self,
        endpoints: &[String],
        timeout: Duration,
    ) -> Result<Cluster> {
        self.connect_cluster_for_keyspace(endpoints, timeout, None)
            .await
            .map(|(cluster, _)| cluster)
    }

    pub(crate) async fn connect_cluster_for_keyspace(
        &self,
        endpoints: &[String],
        timeout: Duration,
        keyspace: Option<&str>,
    ) -> Result<(Cluster, Option<keyspacepb::KeyspaceMeta>)> {
        let members = self.validate_endpoints(endpoints, timeout).await?;
        let (client, mut keyspace_client, members, url) =
            self.try_connect_leader(&members, timeout).await?;
        let id = members.header.as_ref().unwrap().cluster_id;
        let mut discovery = self.discovery.clone();
        // Go's initialization callback resolves the keyspace before setting
        // the service mode. Never require a usable default group for V2.
        let meta = if let Some(name) = keyspace {
            let mut request = pd_request!(id, keyspacepb::LoadKeyspaceRequest);
            request.name = name.to_owned();
            let response = request.send(&mut keyspace_client, timeout).await?;
            let meta = response
                .keyspace
                .ok_or_else(|| Error::KeyspaceNotFound(name.to_owned()))?;
            crate::request::keyspace_from_pd_meta(&meta)?;
            discovery.set_keyspace(&meta)?;
            Some(meta)
        } else {
            None
        };
        let (route, _) = self.discover(&mut discovery, id, &url, timeout).await?;
        let service_urls = tso_service_urls(&discovery, &route, &members);
        let tso_hooks = Arc::new(std::sync::RwLock::new(None));
        let tso_view = Arc::new(TsoDiscoveryView::new(
            id,
            &route,
            service_urls,
            self.clone(),
            tso_hooks.clone(),
        ));
        let tso_client = TsoClient::new(
            &Cancellation::default(),
            self.options.clone(),
            timeout,
            tso_view.clone(),
            TsoDiscoveryView::factory(&route),
        );
        log::info!(
            "{}",
            match TsoDiscoveryView::factory(&route) {
                StreamBuilderFactory::Pd => "[pd] tso provider changed to pd",
                StreamBuilderFactory::Ms => "[pd] tso provider changed to tso server",
            }
        );
        tso_client.update_tso_leader_url(&route.endpoint);
        tso_client.setup().await;
        let cluster = Cluster {
            connection: self.clone(),
            region_service: Arc::default(),
            id,
            channels: self.channels.clone(),
            client: Some(client),
            keyspace_client: Some(keyspace_client),
            members,
            discovery,
            route,
            tso: Arc::new(std::sync::RwLock::new(Some(tso_client))),
            tso_view,
            tso_hooks,
            retired_tso: Vec::new(),
        };
        cluster.log_member_changes(None);
        cluster.update_region_members();
        Ok((cluster, meta))
    }

    // Re-establish connection with PD leader in asynchronous fashion.
    pub async fn reconnect(&self, cluster: &mut Cluster, timeout: Duration) -> Result<()> {
        let leader = self.prepare_reconnect(cluster, timeout).await?;
        cluster.install_leader(leader)?;
        let _ = self.prepare_service_mode(cluster, timeout).await;
        let timestamp = self.prepare_timestamp(cluster, timeout, None).await?;
        cluster.install_timestamp(timestamp, timeout)?.await;
        cluster.finish_retirement();
        Ok(())
    }

    pub(crate) fn prepare_reconnect(
        &self,
        cluster: &Cluster,
        timeout: Duration,
    ) -> impl Future<Output = Result<LeaderConnection>> + Send + 'static {
        let mut connection = self.clone();
        connection.channels = cluster.channels.clone();
        let members = cluster.members.clone();
        let closed = cluster.client.is_none();
        async move {
            if closed {
                return Err(Error::ContextCanceled);
            }
            let (client, keyspace_client, members, _) =
                connection.try_connect_leader(&members, timeout).await?;
            Ok(LeaderConnection {
                client,
                keyspace_client,
                members,
            })
        }
    }

    pub(crate) fn prepare_service_mode(
        &self,
        cluster: &Cluster,
        timeout: Duration,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let mut connection = self.clone();
        connection.channels = cluster.channels.clone();
        let mode = cluster.discovery.service_mode();
        let url = cluster
            .members
            .leader
            .as_ref()
            .and_then(|member| connection.member_url(member));
        async move {
            let url = url.ok_or_else(|| internal_err!("PD membership has no leader"))?;
            Ok(mode
                .refresh(&url, timeout, |url| {
                    let connection = connection.clone();
                    async move { connection.channel(&url).await }
                })
                .await?)
        }
    }

    pub(crate) fn prepare_timestamp(
        &self,
        cluster: &Cluster,
        timeout: Duration,
        keyspace: Option<&keyspacepb::KeyspaceMeta>,
    ) -> impl Future<Output = Result<TimestampConnection>> + Send + 'static {
        let mut connection = self.clone();
        connection.channels = cluster.channels.clone();
        let mut discovery = cluster.discovery.clone();
        let keyspace_result = keyspace
            .map(|meta| discovery.set_keyspace(meta))
            .transpose();
        let members = cluster.members.clone();
        let id = cluster.id;
        let closed = cluster.client.is_none();
        async move {
            if closed {
                return Err(Error::ContextCanceled);
            }
            keyspace_result?;
            let url = members
                .leader
                .as_ref()
                .and_then(|member| connection.member_url(member))
                .ok_or_else(|| internal_err!("PD membership has no leader"))?;
            let (route, _) = discovery
                .discover_cached(
                    id,
                    &url,
                    connection.options.use_tso_server_proxy,
                    timeout,
                    &connection.channels,
                    |url| {
                        let connection = connection.clone();
                        async move { connection.channel(&url).await }
                    },
                )
                .await?;
            let service_urls = tso_service_urls(&discovery, &route, &members);
            Ok((discovery, route, service_urls))
        }
    }

    async fn discover(
        &self,
        discovery: &mut TsoDiscovery,
        id: u64,
        url: &str,
        timeout: Duration,
    ) -> Result<(TsoRoute, Channel)> {
        Ok(discovery
            .discover(
                id,
                url,
                self.options.use_tso_server_proxy,
                timeout,
                &self.channels,
                |url| {
                    let connection = self.clone();
                    async move { connection.channel(&url).await }
                },
            )
            .await?)
    }

    async fn channel(&self, endpoint: &str) -> std::result::Result<Channel, tonic::Status> {
        self.channels
            .get_or_connect(endpoint, || async {
                self.security_mgr
                    .channel_endpoint(
                        endpoint,
                        Duration::from_secs(10),
                        Duration::from_secs(3),
                        None,
                        None,
                    )
                    .await
                    .map(|endpoint| super::service_discovery::lazy_channel(endpoint, &self.options))
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))
            })
            .await
    }

    async fn validate_endpoints(
        &self,
        endpoints: &[String],
        timeout: Duration,
    ) -> Result<pdpb::GetMembersResponse> {
        let mut endpoints_set = HashSet::with_capacity(endpoints.len());

        let mut members = None;
        let mut cluster_id = None;
        for ep in endpoints {
            if !endpoints_set.insert(ep) {
                return Err(internal_err!("duplicated PD endpoint {}", ep));
            }

            let (_, _, resp) = match self.connect(ep, timeout).await {
                Ok(resp) => resp,
                // Ignore failed PD node.
                Err(e) => {
                    log::warn!(url = ep.as_str(), error:% = e; "[pd] failed to get cluster id");
                    continue;
                }
            };

            // Check cluster ID.
            let cid = resp.header.as_ref().unwrap().cluster_id;
            if let Some(sample) = cluster_id {
                if sample != cid {
                    return Err(internal_err!(
                        "PD response cluster_id mismatch, want {}, got {}",
                        sample,
                        cid
                    ));
                }
            } else {
                cluster_id = Some(cid);
            }
            // TODO: check all fields later?

            if members.is_none() {
                members = Some(resp);
            }
        }

        match members {
            Some(members) => {
                log::info!("cluster-id" = cluster_id.unwrap_or_default(); "[pd] init cluster id");
                Ok(members)
            }
            _ => Err(internal_err!("PD cluster failed to respond")),
        }
    }

    async fn connect(
        &self,
        addr: &str,
        timeout: Duration,
    ) -> Result<(
        RoutedPdClient,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        // Go's membership RPC context bounds dialing and the response. A
        // stalled probe must return so the retry owner can make progress.
        let deadline = tokio::time::Instant::now() + timeout;
        tokio::time::timeout_at(deadline, self.connect_member(addr, deadline))
            .await
            .map_err(|_| {
                Error::GrpcAPI(tonic::Status::deadline_exceeded(
                    "PD membership probe timed out",
                ))
            })?
    }

    async fn connect_member(
        &self,
        addr: &str,
        deadline: tokio::time::Instant,
    ) -> Result<(
        RoutedPdClient,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        let channel = self.channel(addr).await?;
        let mut client = pdpb::pd_client::PdClient::with_interceptor(
            channel.clone(),
            super::region_service::Forwarding::default(),
        );
        let keyspace_client = keyspacepb::keyspace_client::KeyspaceClient::new(channel);
        let mut request = pdpb::GetMembersRequest::default().into_request();
        request.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
        let resp: pdpb::GetMembersResponse = client.get_members(request).await?.into_inner();
        if let Some(err) = resp
            .header
            .as_ref()
            .and_then(|header| header.error.as_ref())
        {
            return Err(internal_err!("failed to get PD members, err {:?}", err));
        }
        if resp.header.is_none() {
            return Err(internal_err!(
                "PD GetMembers response has no cluster header"
            ));
        }
        if resp
            .leader
            .as_ref()
            .is_none_or(|leader| leader.client_urls.is_empty())
        {
            return Err(internal_err!(
                "unexpected no PD leader in get member resp: {:?}",
                resp
            ));
        }
        Ok((client, keyspace_client, resp))
    }

    async fn try_connect(
        &self,
        addr: &str,
        cluster_id: u64,
        timeout: Duration,
    ) -> Result<(
        RoutedPdClient,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        let (client, keyspace_client, r) = self.connect(addr, timeout).await?;
        Connection::validate_cluster_id(addr, &r, cluster_id)?;
        Ok((client, keyspace_client, r))
    }

    fn validate_cluster_id(
        addr: &str,
        members: &pdpb::GetMembersResponse,
        cluster_id: u64,
    ) -> Result<()> {
        let new_cluster_id = members.header.as_ref().unwrap().cluster_id;
        if new_cluster_id != cluster_id {
            Err(internal_err!(
                "{} no longer belongs to cluster {}, it is in {}",
                addr,
                cluster_id,
                new_cluster_id
            ))
        } else {
            Ok(())
        }
    }

    /// Attempts to connect to the PD cluster leader.
    ///
    /// Accept a validated member observation and install the reported leader's
    /// lazy connection. Go updateMember/switchLeader does not require another
    /// GetMembers RPC on that leader before publishing its metadata.
    async fn try_connect_leader(
        &self,
        previous: &pdpb::GetMembersResponse,
        timeout: Duration,
    ) -> Result<(
        RoutedPdClient,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
        String,
    )> {
        let previous_leader = previous
            .leader
            .as_ref()
            .ok_or_else(|| internal_err!("PD cluster has no leader"))?;

        let members = &previous.members;
        let cluster_id = previous.header.as_ref().unwrap().cluster_id;

        let mut resp = None;
        // Try to connect to other members, then the previous leader.
        'outer: for m in members
            .iter()
            .filter(|m| *m != previous_leader)
            .chain(Some(previous_leader))
        {
            for ep in &m.client_urls {
                match self.try_connect(ep.as_str(), cluster_id, timeout).await {
                    Ok((_, _, r)) => {
                        resp = Some(r);
                        break 'outer;
                    }
                    Err(e) => {
                        log::info!(url = ep.as_str(), error:% = e; "[pd] cannot update member from this url");
                        continue;
                    }
                }
            }
        }

        // The successful member RPC already validated the cluster and leader.
        // Construct its transport without a second membership observation.
        if let Some(resp) = resp {
            let leader = resp
                .leader
                .as_ref()
                .ok_or_else(|| internal_err!("no leader found in GetMembersResponse"))?;

            let url = self
                .member_url(leader)
                .filter(|url| !url.is_empty())
                .ok_or_else(|| internal_err!("PD leader has no client URL"))?;
            let channel = self.channel(&url).await?;
            let client = pdpb::pd_client::PdClient::with_interceptor(
                channel.clone(),
                super::region_service::Forwarding::default(),
            );
            let keyspace_client = keyspacepb::keyspace_client::KeyspaceClient::new(channel);
            return Ok((client, keyspace_client, resp, url));
        }

        Err(internal_err!("failed to connect to {:?}", members))
    }
}

type GrpcResult<T> = std::result::Result<T, tonic::Status>;

#[async_trait]
trait PdMessage: Sized {
    type Client: Send;
    type Response: PdResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response>;

    async fn send(self, client: &mut Self::Client, timeout: Duration) -> Result<Self::Response> {
        let mut req = self.into_request();
        req.set_timeout(timeout);
        let response = Self::rpc(req, client).await?;

        if let Some(err) = response.header().and_then(|header| header.error.as_ref()) {
            Err(internal_err!(err.message))
        } else {
            Ok(response)
        }
    }
}

#[async_trait]
impl PdMessage for pdpb::GetRegionRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::GetRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_region(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetRegionByIdRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::GetRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_region_by_id(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::ScanRegionsRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::ScanRegionsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.scan_regions(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::BatchScanRegionsRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::BatchScanRegionsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.batch_scan_regions(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::SplitRegionsRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::SplitRegionsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.split_regions(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetStoreRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::GetStoreResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_store(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetAllStoresRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::GetAllStoresResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_all_stores(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateGcSafePointRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::UpdateGcSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_gc_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetMembersRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::GetMembersResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_members(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetGcStateRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::GetGcStateResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_gc_state(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::AdvanceTxnSafePointRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::AdvanceTxnSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.advance_txn_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::AdvanceGcSafePointRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::AdvanceGcSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.advance_gc_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::ScatterRegionRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::ScatterRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.scatter_region(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetOperatorRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::GetOperatorResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_operator(req).await?.into_inner())
    }

    /// pd-client `GetOperator` returns PD's response as it is: a region with
    /// no operator answers a header error, which the callers read as "not
    /// scattering" (`scatter_wait_completed`, `check_region_in_scattering`).
    async fn send(self, client: &mut Self::Client, timeout: Duration) -> Result<Self::Response> {
        let mut req = self.into_request();
        req.set_timeout(timeout);
        Ok(Self::rpc(req, client).await?)
    }
}

#[async_trait]
impl PdMessage for pdpb::SetExternalTimestampRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::SetExternalTimestampResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.set_external_timestamp(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetExternalTimestampRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::GetExternalTimestampResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_external_timestamp(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetMinTsRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::GetMinTsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_min_ts(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateGcSafePointV2Request {
    type Client = RoutedPdClient;
    type Response = pdpb::UpdateGcSafePointV2Response;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_gc_safe_point_v2(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateServiceGcSafePointRequest {
    type Client = RoutedPdClient;
    type Response = pdpb::UpdateServiceGcSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_service_gc_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateServiceSafePointV2Request {
    type Client = RoutedPdClient;
    type Response = pdpb::UpdateServiceSafePointV2Response;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_service_safe_point_v2(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for keyspacepb::GetAllKeyspacesRequest {
    type Client = keyspacepb::keyspace_client::KeyspaceClient<Channel>;
    type Response = keyspacepb::GetAllKeyspacesResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_all_keyspaces(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for keyspacepb::LoadKeyspaceRequest {
    type Client = keyspacepb::keyspace_client::KeyspaceClient<Channel>;
    type Response = keyspacepb::LoadKeyspaceResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.load_keyspace(req).await?.into_inner())
    }
}

trait PdResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader>;
}

impl PdResponse for pdpb::GetMembersResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetStoreResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetRegionResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::ScanRegionsResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::BatchScanRegionsResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::SplitRegionsResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetAllStoresResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::UpdateGcSafePointResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetGcStateResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::AdvanceTxnSafePointResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::AdvanceGcSafePointResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::ScatterRegionResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetOperatorResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::SetExternalTimestampResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetExternalTimestampResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::GetMinTsResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::UpdateGcSafePointV2Response {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::UpdateServiceGcSafePointResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for pdpb::UpdateServiceSafePointV2Response {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for keyspacepb::GetAllKeyspacesResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

impl PdResponse for keyspacepb::LoadKeyspaceResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use log::error;
use log::info;
use log::warn;
use tonic::transport::Channel;
use tonic::IntoRequest;
use tonic::Request;

use super::connectionctx::{ConnectionCtx, Manager};
use super::service_discovery::{ChannelCache, TsoDiscovery, TsoRoute};
use super::timestamp::TimestampOracle;
use crate::internal_err;
use crate::proto::keyspacepb;
use crate::proto::pdpb;
use crate::Error;
use crate::Result;
use crate::SecurityManager;
use crate::Timestamp;

/// A PD cluster.
pub struct Cluster {
    connection: Connection,
    region_service: Arc<super::region_service::RegionService>,
    id: u64,
    channels: Arc<ChannelCache>,
    client: Option<pdpb::pd_client::PdClient<Channel>>,
    keyspace_client: Option<keyspacepb::keyspace_client::KeyspaceClient<Channel>>,
    members: pdpb::GetMembersResponse,
    discovery: TsoDiscovery,
    route: TsoRoute,
    // Native mode has one leader stream. Its URL and cancellation lifetime
    // belong to the same manager used by Go TSO, independently of metadata RPCs.
    tso: Manager<TimestampOracle>,
    // Keep joins owned until completion, including when a reconnect/close future
    // is cancelled after publication. Requests never own stream retirement.
    retired_tso: Vec<Arc<ConnectionCtx<TimestampOracle>>>,
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
    pub(crate) fn id(&self) -> u64 {
        self.id
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
        F: Fn(pdpb::pd_client::PdClient<Channel>, Request<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = GrpcResult<R>> + Send,
    {
        let connection = self.connection.clone();
        let leader_client = self.client.clone();
        let leader = self
            .members
            .leader
            .as_ref()
            .and_then(|member| member.client_urls.first())
            .cloned()
            .unwrap_or_default();
        let urls = self
            .members
            .members
            .iter()
            .filter_map(|member| member.client_urls.first().cloned())
            .collect::<Vec<_>>();
        let target = self.region_service.select(
            &leader,
            &urls,
            connection.options.get_enable_follower_handle(),
            allowed,
        );
        async move {
            let leader_client = leader_client.ok_or(Error::ContextCanceled)?;
            let deadline = tokio::time::Instant::now() + timeout;
            let request = async {
                let first = async {
                    let client = if target.follower {
                        pdpb::pd_client::PdClient::new(connection.channel(&target.endpoint).await?)
                    } else {
                        leader_client.clone()
                    };
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
            let req = pd_request!(cluster_id, pdpb::GetAllStoresRequest);
            req.send(&mut client, timeout).await
        }
    }

    pub fn get_timestamp(&self) -> impl Future<Output = Result<Timestamp>> + Send + 'static {
        let connection = self.tso.randomly_pick();
        async move {
            let connection =
                connection.ok_or_else(|| internal_err!("no TSO connection is registered"))?;
            connection.stream.clone().get_timestamp().await
        }
    }

    pub fn get_min_timestamp(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<Timestamp>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.client.clone();
        let classic = self.route.group_id.is_none();
        let ordinary = self.get_timestamp();
        async move {
            // Go GetMinTS uses the ordinary provider in classic mode. API
            // compatibility falls back only when GetMinTS is unsupported.
            let mut client = client.ok_or(Error::ContextCanceled)?;
            if classic {
                return ordinary.await;
            }
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
            let mut req = pd_request!(cluster_id, pdpb::UpdateGcSafePointRequest);
            req.safe_point = safepoint;
            req.send(&mut client, timeout).await
        }
    }

    pub fn get_gc_state(
        &self,
        keyspace_id: u32,
        timeout: Duration,
    ) -> impl Future<Output = Result<pdpb::GetGcStateResponse>> + Send + 'static {
        let cluster_id = self.id;
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
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
        let client = self.client.clone();
        async move {
            let mut client = client.ok_or(Error::ContextCanceled)?;
            let mut req = pd_request!(cluster_id, pdpb::GetOperatorRequest);
            req.region_id = region_id;
            req.send(&mut client, timeout).await
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
    fn update_region_members(&self) {
        let leader = self
            .members
            .leader
            .as_ref()
            .and_then(|member| member.client_urls.first())
            .map(String::as_str)
            .unwrap_or_default();
        let urls = self
            .members
            .members
            .iter()
            .filter_map(|member| member.client_urls.first().cloned())
            .collect::<Vec<_>>();
        self.region_service.update_members(leader, &urls);
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

    pub(crate) fn install_leader(
        &mut self,
        leader: LeaderConnection,
        timeout: Duration,
    ) -> Result<impl Future<Output = ()> + Send + 'static> {
        if self.client.is_none() {
            return Err(Error::ContextCanceled);
        }
        let LeaderConnection {
            client,
            keyspace_client,
            members,
            timestamp,
        } = leader;
        // Metadata leadership can change while TSO discovery is unavailable.
        // Publish its valid connection independently, retaining the old TSO route.
        self.client = Some(client);
        self.keyspace_client = Some(keyspace_client);
        self.members = members;
        self.update_region_members();
        let (discovery, route, channel) = timestamp?;
        let url = route.endpoint.clone();
        let previous = self.tso.randomly_pick();
        let reuse = previous
            .as_ref()
            .is_some_and(|connection| self.route == route && !connection.ctx.is_cancelled());
        let mut rejected = None;
        if !reuse {
            let candidate = tso_connection(self.id, route.clone(), channel, timeout)?;
            // Go's dispatcher releases canceled contexts before reconnecting.
            // A canceled same-URL entry must not reject its replacement.
            self.tso.release(&url);
            if !self.tso.clean_all_and_store(&candidate) {
                candidate.cancel();
                rejected = Some(candidate);
            }
        }
        self.discovery = discovery;
        self.route = route;
        self.retired_tso.extend(rejected);
        if !reuse {
            self.retired_tso.extend(previous);
        }
        Ok(self.retire_streams())
    }

    fn retire_streams(&self) -> impl Future<Output = ()> + Send + 'static {
        let retired = self.retired_tso.clone();
        async move {
            for connection in retired {
                connection.stream.close().await;
            }
        }
    }

    pub(crate) fn start_close(&mut self) -> impl Future<Output = ()> + Send + 'static {
        self.channels.close();
        self.client.take();
        self.keyspace_client.take();
        self.retired_tso.extend(self.tso.randomly_pick());
        self.tso.release_all();
        self.retire_streams()
    }

    #[cfg(test)]
    pub(crate) fn tso_for_test(&self) -> TimestampOracle {
        self.tso.randomly_pick().unwrap().stream.clone()
    }

    pub(crate) fn finish_retirement(&mut self) {
        self.retired_tso.clear();
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.channels.close();
        self.tso.release_all();
        for connection in &self.retired_tso {
            connection.cancel();
        }
    }
}

fn tso_connection(
    cluster_id: u64,
    route: TsoRoute,
    channel: Channel,
    timeout: Duration,
) -> Result<Arc<ConnectionCtx<TimestampOracle>>> {
    let url = route.endpoint.clone();
    let oracle = TimestampOracle::discovered(cluster_id, route, channel, timeout)?;
    let ctx = oracle.cancellation();
    let cancel = ctx.clone();
    Ok(Arc::new(ConnectionCtx::new(
        ctx,
        move || cancel.cancel(),
        url,
        oracle,
    )))
}

fn keyspace_scope(keyspace_id: u32) -> pdpb::KeyspaceScope {
    pdpb::KeyspaceScope {
        keyspace: Some(pdpb::keyspace_scope::Keyspace::KeyspaceId(keyspace_id)),
    }
}

pub(crate) struct LeaderConnection {
    client: pdpb::pd_client::PdClient<Channel>,
    keyspace_client: keyspacepb::keyspace_client::KeyspaceClient<Channel>,
    members: pdpb::GetMembersResponse,
    timestamp: Result<(TsoDiscovery, TsoRoute, Channel)>,
}

/// An object for connecting and reconnecting to a PD cluster.
#[derive(Clone)]
pub struct Connection {
    pub(crate) options: Arc<super::opt::Options>,
    security_mgr: Arc<SecurityManager>,
    channels: Arc<ChannelCache>,
    // Initialization retries share probe selection before a Cluster exists.
    discovery: TsoDiscovery,
}

impl Connection {
    pub(crate) fn security_manager(&self) -> Arc<SecurityManager> {
        self.security_mgr.clone()
    }

    pub fn new(security_mgr: Arc<SecurityManager>) -> Connection {
        Connection {
            options: Arc::new(super::opt::Options::new()),
            security_mgr,
            channels: Arc::new(ChannelCache::default()),
            discovery: TsoDiscovery::default(),
        }
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
        let (route, channel) = self.discover(&mut discovery, id, &url, timeout).await?;
        let tso = Manager::new();
        tso.store(&tso_connection(id, route.clone(), channel, timeout)?, false);
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
            tso,
            retired_tso: Vec::new(),
        };
        cluster.update_region_members();
        Ok((cluster, meta))
    }

    // Re-establish connection with PD leader in asynchronous fashion.
    pub async fn reconnect(&self, cluster: &mut Cluster, timeout: Duration) -> Result<()> {
        warn!("updating pd client");
        let start = Instant::now();
        let leader = self.prepare_reconnect(cluster, timeout).await?;
        cluster.install_leader(leader, timeout)?.await;
        cluster.finish_retirement();

        info!("updating PD client done, spent {:?}", start.elapsed());
        Ok(())
    }

    pub(crate) fn prepare_reconnect(
        &self,
        cluster: &Cluster,
        timeout: Duration,
    ) -> impl Future<Output = Result<LeaderConnection>> + Send + 'static {
        self.prepare_keyspace_reconnect(cluster, timeout, None)
    }

    pub(crate) fn prepare_keyspace_reconnect(
        &self,
        cluster: &Cluster,
        timeout: Duration,
        keyspace: Option<&keyspacepb::KeyspaceMeta>,
    ) -> impl Future<Output = Result<LeaderConnection>> + Send + 'static {
        let mut connection = self.clone();
        connection.channels = cluster.channels.clone();
        let mut discovery = cluster.discovery.clone();
        let keyspace_result = keyspace
            .map(|meta| discovery.set_keyspace(meta))
            .transpose();
        let members = cluster.members.clone();
        let closed = cluster.client.is_none();
        async move {
            if closed {
                return Err(Error::ContextCanceled);
            }
            keyspace_result?;
            let (client, keyspace_client, members, url) =
                connection.try_connect_leader(&members, timeout).await?;
            let id = members.header.as_ref().unwrap().cluster_id;
            let timestamp = connection
                .discover(&mut discovery, id, &url, timeout)
                .await
                .map(|(route, channel)| (discovery, route, channel));
            Ok(LeaderConnection {
                client,
                keyspace_client,
                members,
                timestamp,
            })
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
            .discover(id, url, timeout, |url| {
                let connection = self.clone();
                async move { connection.channel(&url).await }
            })
            .await?)
    }

    async fn channel(&self, endpoint: &str) -> std::result::Result<Channel, tonic::Status> {
        self.channels
            .get_or_connect(endpoint, || async {
                self.security_mgr
                    .connect(endpoint, |channel| channel)
                    .await
                    .map_err(|error| tonic::Status::unavailable(error.to_string()))
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
                    warn!("PD endpoint {} failed to respond: {:?}", ep, e);
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
                info!("All PD endpoints are consistent: {:?}", endpoints);
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
        pdpb::pd_client::PdClient<Channel>,
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
        pdpb::pd_client::PdClient<Channel>,
        keyspacepb::keyspace_client::KeyspaceClient<Channel>,
        pdpb::GetMembersResponse,
    )> {
        let channel = self.channel(addr).await?;
        let mut client = pdpb::pd_client::PdClient::new(channel.clone());
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
        if resp.leader.is_none() {
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
        pdpb::pd_client::PdClient<Channel>,
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
    /// Iterates over known members to find a responsive node, then connects
    /// to the reported leader. Returns an error if no leader is present or
    /// reachable.
    async fn try_connect_leader(
        &self,
        previous: &pdpb::GetMembersResponse,
        timeout: Duration,
    ) -> Result<(
        pdpb::pd_client::PdClient<Channel>,
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
                        error!("failed to connect to {}, {:?}", ep, e);
                        continue;
                    }
                }
            }
        }

        // Then try to connect the PD cluster leader.
        if let Some(resp) = resp {
            let leader = resp
                .leader
                .as_ref()
                .ok_or_else(|| internal_err!("no leader found in GetMembersResponse"))?;

            for ep in &leader.client_urls {
                if let Ok((client, keyspace_client, members)) =
                    self.try_connect(ep.as_str(), cluster_id, timeout).await
                {
                    return Ok((client, keyspace_client, members, ep.clone()));
                }
            }
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
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_region(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetRegionByIdRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_region_by_id(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::ScanRegionsRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::ScanRegionsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.scan_regions(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::BatchScanRegionsRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::BatchScanRegionsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.batch_scan_regions(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::SplitRegionsRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::SplitRegionsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.split_regions(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetStoreRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetStoreResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_store(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetAllStoresRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetAllStoresResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_all_stores(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::UpdateGcSafePointRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::UpdateGcSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.update_gc_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetGcStateRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetGcStateResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_gc_state(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::AdvanceTxnSafePointRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::AdvanceTxnSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.advance_txn_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::AdvanceGcSafePointRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::AdvanceGcSafePointResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.advance_gc_safe_point(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::ScatterRegionRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::ScatterRegionResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.scatter_region(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetOperatorRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetOperatorResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_operator(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::SetExternalTimestampRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::SetExternalTimestampResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.set_external_timestamp(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetExternalTimestampRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetExternalTimestampResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_external_timestamp(req).await?.into_inner())
    }
}

#[async_trait]
impl PdMessage for pdpb::GetMinTsRequest {
    type Client = pdpb::pd_client::PdClient<Channel>;
    type Response = pdpb::GetMinTsResponse;

    async fn rpc(req: Request<Self>, client: &mut Self::Client) -> GrpcResult<Self::Response> {
        Ok(client.get_min_ts(req).await?.into_inner())
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

impl PdResponse for keyspacepb::LoadKeyspaceResponse {
    fn header(&self) -> Option<&pdpb::ResponseHeader> {
        self.header.as_ref()
    }
}

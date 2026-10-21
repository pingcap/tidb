// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! A utility module for managing and retrying PD requests.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use futures::FutureExt;
use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::async_util::Cancellation;
use crate::pd::Cluster;
use crate::pd::Connection;
use crate::proto::keyspacepb;
use crate::proto::metapb;
use crate::proto::pdpb::Timestamp;
use crate::proto::pdpb::{self};
use crate::region::RegionId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::stats::pd_stats;
use crate::Error;
use crate::Result;
use crate::SecurityManager;

// FIXME: these numbers and how they are used are all just cargo-culted in, there
// may be more optimal values.
const RECONNECT_INTERVAL_SEC: u64 = 1;
const MAX_REQUEST_COUNT: usize = 5;
const LEADER_CHANGE_RETRY: usize = 10;

/// Compatibility name for the complete Go PD region request options.
/// Construct with source fields plus `..Default::default()`.
pub type RegionScanOptions = super::opt::GetRegionOp;

#[async_trait]
pub trait RetryClientTrait {
    /// pd-client `GetClusterID`.
    async fn cluster_id(&self) -> u64;

    /// Retires the owned PD services. Adapters without owned workers may do nothing.
    async fn close(&self) {}

    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader>;

    /// Requests PD bucket metadata for a key lookup. Custom clients that do
    /// not model PD options retain their normal region result.
    async fn get_region_with_buckets(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        self.get_region(key).await
    }

    /// One cache lookup attempt. Retries must bypass follower/router metadata.
    /// Clients supporting follower reads override this compatibility default
    /// and honor `leader_only` independently of the process-wide option.
    async fn get_region_for_cache(
        self: Arc<Self>,
        key: Vec<u8>,
        previous: bool,
        _leader_only: bool,
    ) -> Result<RegionWithLeader> {
        if previous {
            self.get_prev_region_with_buckets(key).await
        } else {
            self.get_region_with_buckets(key).await
        }
    }

    async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader>;

    async fn get_prev_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<RegionWithLeader> {
        self.get_prev_region(key).await
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader>;

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<RegionWithLeader> {
        self.get_region_by_id(region_id).await
    }

    /// Source `PDClient.ScanRegions`, used to refresh a contiguous region
    /// range in one request. The default keeps custom/mock clients compatible
    /// by deriving the same bounded sequence from point lookups.
    async fn scan_regions(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<RegionWithLeader>> {
        let mut next: crate::Key = start_key.into();
        let end: crate::Key = end_key.into();
        let mut regions = Vec::with_capacity(limit);
        while regions.len() < limit {
            let region = self.clone().get_region(next.clone().into()).await?;
            let region_end = region.end_key();
            if !region_end.is_empty() && region_end <= next {
                return Err(Error::StringError(
                    "PD returned a region that does not advance ScanRegions".to_owned(),
                ));
            }
            regions.push(region);
            if region_end.is_empty() || (!end.is_empty() && region_end >= end) {
                break;
            }
            next = region_end;
        }
        Ok(regions)
    }

    /// PD's multi-range region scan. Custom clients can explicitly retain an
    /// unsupported result so callers can fall back to `ScanRegions`, as
    /// client-go's region cache does for older PD servers.
    async fn batch_scan_regions(
        self: Arc<Self>,
        _ranges: Vec<pdpb::KeyRange>,
        _limit: usize,
        _options: RegionScanOptions,
    ) -> Result<Vec<RegionWithLeader>> {
        Err(Error::Unimplemented)
    }

    /// Requests PD to split at the supplied physical keys.
    async fn split_regions(
        self: Arc<Self>,
        _split_keys: Vec<Vec<u8>>,
        _retry_limit: u64,
    ) -> Result<pdpb::SplitRegionsResponse> {
        Err(Error::Unimplemented)
    }

    /// Returns `None` when PD has no store for the requested ID. Client-go
    /// treats that outcome like a tombstone; retaining it explicitly avoids
    /// panicking on an empty `GetStoreResponse`.
    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<Option<metapb::Store>>;

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>>;

    /// pd-client `GetAllStores(ctx, WithExcludeTombstone(),
    /// WithAllowRouterServiceHandleStoreRequest())`, the store-list refresh's
    /// first attempt. Clients without a router service answer from PD.
    async fn get_all_stores_allowing_router_service(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        self.get_all_stores().await
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Err(Error::StringError(
            "PD minimum timestamp is not supported by this client".to_owned(),
        ))
    }

    async fn set_external_timestamp(self: Arc<Self>, _timestamp: u64) -> Result<()> {
        Err(Error::StringError(
            "PD external timestamp is not supported by this client".to_owned(),
        ))
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        Err(Error::StringError(
            "PD external timestamp is not supported by this client".to_owned(),
        ))
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool>;

    async fn update_safepoint_value(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    /// PD `UpdateGCSafePointV2`.
    async fn update_gc_safe_point_v2(
        self: Arc<Self>,
        _keyspace_id: u32,
        _safe_point: u64,
    ) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    /// PD `UpdateServiceGCSafePoint`: the minimal service safe point.
    async fn update_service_gc_safe_point(
        self: Arc<Self>,
        _service_id: String,
        _ttl: i64,
        _safe_point: u64,
    ) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    /// PD `UpdateServiceSafePointV2`: the keyspace's minimal service safe point.
    async fn update_service_safe_point_v2(
        self: Arc<Self>,
        _keyspace_id: u32,
        _service_id: String,
        _ttl: i64,
        _safe_point: u64,
    ) -> Result<u64> {
        Err(Error::Unimplemented)
    }

    /// PD `GetAllKeyspaces`.
    async fn get_all_keyspaces(
        &self,
        _start_id: u32,
        _limit: u32,
    ) -> Result<Vec<keyspacepb::KeyspaceMeta>> {
        Err(Error::Unimplemented)
    }

    /// Loads PD's modern transaction/GC safe-point state for one keyspace.
    async fn get_gc_state(self: Arc<Self>, _keyspace_id: u32) -> Result<pdpb::GetGcStateResponse> {
        Err(Error::Unimplemented)
    }

    /// The client's dynamic options (pd-client `UpdateOption`), when it has
    /// any.
    fn options(&self) -> Option<Arc<super::opt::Options>> {
        None
    }

    /// pd-client `GetAllMembers`.
    async fn get_all_members(self: Arc<Self>) -> Result<pdpb::GetMembersResponse> {
        Err(Error::Unimplemented)
    }

    /// pd-client `StoreGlobalConfig`.
    async fn store_global_config(
        self: Arc<Self>,
        _config_path: String,
        _changes: Vec<pdpb::GlobalConfigItem>,
    ) -> Result<()> {
        Err(Error::Unimplemented)
    }

    /// pd-client `ListResourceGroups` within `keyspace_id`.
    async fn list_resource_groups(
        self: Arc<Self>,
        _keyspace_id: u32,
        _with_ru_stats: bool,
    ) -> Result<Vec<crate::proto::resource_manager::ResourceGroup>> {
        Err(Error::Unimplemented)
    }

    /// pd-client `AddResourceGroup` (`add`) or `ModifyResourceGroup` within
    /// `keyspace_id`, answering PD's response body.
    async fn put_resource_group(
        self: Arc<Self>,
        _keyspace_id: u32,
        _group: crate::proto::resource_manager::ResourceGroup,
        _add: bool,
    ) -> Result<String> {
        Err(Error::Unimplemented)
    }

    /// pd-client `DeleteResourceGroup` within `keyspace_id`.
    async fn delete_resource_group(
        self: Arc<Self>,
        _keyspace_id: u32,
        _name: String,
    ) -> Result<String> {
        Err(Error::Unimplemented)
    }

    /// pd-client `GetResourceGroup` within `keyspace_id`; `None` when PD
    /// answers without a group.
    async fn get_resource_group(
        self: Arc<Self>,
        _keyspace_id: u32,
        _name: String,
        _with_ru_stats: bool,
    ) -> Result<Option<crate::proto::resource_manager::ResourceGroup>> {
        Err(Error::Unimplemented)
    }

    /// pd-client `AcquireTokenBuckets` over the resource manager token stream.
    async fn acquire_token_buckets(
        self: Arc<Self>,
        _request: crate::proto::resource_manager::TokenBucketsRequest,
    ) -> Result<Vec<crate::proto::resource_manager::TokenBucketResponse>> {
        Err(Error::Unimplemented)
    }

    /// pd-client meta storage `Get`.
    async fn meta_storage_get(
        self: Arc<Self>,
        _request: crate::proto::meta_storagepb::GetRequest,
    ) -> Result<crate::proto::meta_storagepb::GetResponse> {
        Err(Error::Unimplemented)
    }

    /// pd-client meta storage `Put`.
    async fn meta_storage_put(
        self: Arc<Self>,
        _request: crate::proto::meta_storagepb::PutRequest,
    ) -> Result<crate::proto::meta_storagepb::PutResponse> {
        Err(Error::Unimplemented)
    }

    /// pd-client meta storage `Watch`; the channel closes when the stream ends.
    async fn meta_storage_watch(
        self: Arc<Self>,
        _request: crate::proto::meta_storagepb::WatchRequest,
    ) -> Result<tokio::sync::mpsc::Receiver<super::metastorage::WatchResponse>> {
        Err(Error::Unimplemented)
    }

    async fn advance_txn_safe_point(
        self: Arc<Self>,
        _keyspace_id: u32,
        _target: u64,
    ) -> Result<pdpb::AdvanceTxnSafePointResponse> {
        Err(Error::Unimplemented)
    }

    async fn advance_gc_safe_point(
        self: Arc<Self>,
        _keyspace_id: u32,
        _target: u64,
    ) -> Result<pdpb::AdvanceGcSafePointResponse> {
        Err(Error::Unimplemented)
    }

    async fn scatter_regions(
        self: Arc<Self>,
        _region_ids: Vec<u64>,
        _group: String,
    ) -> Result<pdpb::ScatterRegionResponse> {
        Err(Error::Unimplemented)
    }

    async fn get_operator(self: Arc<Self>, _region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        Err(Error::Unimplemented)
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta>;
}
#[derive(Default)]
struct DiscoveryRefresh {
    members: tokio::sync::Mutex<()>,
    timestamp: tokio::sync::Mutex<()>,
    members_changed: tokio::sync::Notify,
    /// The TSO client's `ScheduleCheckMemberChanged`: also re-check the TSO
    /// routing (a keyspace group's members in microservice mode).
    tso_members_changed: tokio::sync::Notify,
}

/// Client for communication with a PD cluster. Has the facility to reconnect to the cluster.
pub struct RetryClient<Cl = Cluster> {
    // Tuple is the cluster and the time of the cluster's last reconnect.
    cluster: Arc<RwLock<(Cl, Instant)>>,
    connection: Connection,
    reconnect: Arc<DiscoveryRefresh>,
    discovery_worker: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    cancellation: Cancellation,
    timeout: Duration,
    initial_keyspace: Option<keyspacepb::KeyspaceMeta>,
    // pd-client's token dispatcher, created on the first token request.
    token_dispatcher: tokio::sync::OnceCell<super::resource_manager_client::TokenDispatcher>,
    /// pd-client `innerClient.msDiscovery`.
    router_discovery:
        tokio::sync::Mutex<Option<Arc<super::router_service_discovery::RouterServiceDiscovery>>>,
}

impl<Cl> RetryClient<Cl> {
    pub fn new_with_cluster(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        cluster: Cl,
    ) -> RetryClient<Cl> {
        let connection = Connection::new(security_mgr);
        RetryClient {
            cluster: Arc::new(RwLock::new((cluster, Instant::now()))),
            connection,
            reconnect: Arc::new(DiscoveryRefresh::default()),
            discovery_worker: tokio::sync::Mutex::new(None),
            cancellation: Cancellation::default(),
            timeout,
            initial_keyspace: None,
            token_dispatcher: tokio::sync::OnceCell::new(),
            router_discovery: tokio::sync::Mutex::new(None),
        }
    }
}

macro_rules! retry_core {
    ($self: ident, $tag: literal, $call: expr) => {{
        let stats = pd_stats($tag);
        let request = async {
            let mut last_err = Ok(());
            for _ in 0..LEADER_CHANGE_RETRY {
                let res = $call;

                match res {
                    Ok(r) => return Ok(r),
                    Err(Error::Unimplemented) => return Err(Error::Unimplemented),
                    // Empty metadata belongs to RegionCache's BoPDRPC budget,
                    // not the transport's reconnect loop.
                    Err(error @ Error::RegionForKeyNotFound { .. }) => return Err(error),
                    Err(e) => last_err = Err(e),
                }

                let mut reconnect_count = MAX_REQUEST_COUNT;
                while let Err(e) = $self.reconnect(RECONNECT_INTERVAL_SEC).await {
                    reconnect_count -= 1;
                    if reconnect_count == 0 {
                        return Err(e);
                    }
                    sleep(Duration::from_secs(RECONNECT_INTERVAL_SEC)).await;
                }
            }

            last_err?;
            unreachable!();
        };
        let cancelled = async {
            match $self.cancellation() {
                Some(cancellation) => cancellation.cancelled().await,
                None => std::future::pending().await,
            }
        };
        let result = tokio::select! {
            biased;
            _ = cancelled => Err(Error::ContextCanceled),
            result = request => result,
        };
        stats.done(result)
    }};
}

/// [`retry!`] for a call client-go's `util.InterceptedPDClient` wraps: the
/// whole wait, retries included, is charged to the statement's
/// `ExecDetails.WaitPDRespDuration` (Go `recordPDWaitTime`).
macro_rules! retry_recording_pd_wait {
    ($self: ident, $tag: literal, |$cluster: ident| $call: expr) => {{
        let started = std::time::Instant::now();
        let result = retry!($self, $tag, |$cluster| $call);
        crate::util::record_pd_wait_time(started);
        result
    }};
}

macro_rules! retry {
    ($self: ident, $tag: literal, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $tag, {
            let request = {
                let guard = $self.cluster.read().await;
                let $cluster = &guard.0;
                $call
            };
            // The future owns its connection; neither requests nor reconnect
            // wait for an RPC-duration cluster guard.
            request.await
        })
    }};
}

impl RetryClient<Cluster> {
    pub async fn connect(
        endpoints: &[String],
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
    ) -> Result<RetryClient> {
        Self::connect_for_keyspace(endpoints, security_mgr, timeout, None).await
    }

    /// Reconciles timestamp streams after a live Go PD proxy-option change.
    pub fn set_enable_tso_follower_proxy(&self, enabled: bool) {
        self.connection
            .options
            .set_enable_tso_follower_proxy(enabled);
    }

    /// Changes subsequent region requests without rebuilding discovery or TSO.
    pub fn set_enable_follower_handle(&self, enabled: bool) {
        self.connection.options.set_enable_follower_handle(enabled);
    }

    pub(crate) fn initial_keyspace(&self) -> Option<&keyspacepb::KeyspaceMeta> {
        self.initial_keyspace.as_ref()
    }

    pub(crate) async fn connect_for_keyspace(
        endpoints: &[String],
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        keyspace: Option<&str>,
    ) -> Result<RetryClient> {
        let mut options = super::opt::Options::new();
        options.timeout = timeout;
        Self::connect_with_options(endpoints, security_mgr, options, keyspace).await
    }

    /// Initialize discovery and requests with one shared Go PD options owner.
    pub async fn connect_with_options(
        endpoints: &[String],
        security_mgr: Arc<SecurityManager>,
        options: super::opt::Options,
        keyspace: Option<&str>,
    ) -> Result<RetryClient> {
        let timeout = options.timeout;
        let options = Arc::new(options);
        let mut connection = Connection::new(security_mgr);
        connection.options = options.clone();
        if options.init_metrics {
            super::metrics::init_and_register_metrics(Default::default());
        }
        let connected = std::sync::Mutex::new(None);
        super::backoff::retry(
            std::future::pending(),
            options.max_retry_times,
            1_000_000_000,
            || async {
                let cluster = connection
                    .connect_cluster_for_keyspace(endpoints, options.timeout, keyspace)
                    .await?;
                *connected.lock().unwrap() = Some(cluster);
                Ok(())
            },
        )
        .await?;
        let (cluster, initial_keyspace) = connected
            .into_inner()
            .unwrap()
            .expect("successful PD initialization");
        let mut client = Self::new_with_cluster(connection.security_manager(), timeout, cluster);
        client.connection = connection;
        client.initial_keyspace = initial_keyspace;
        client.start_discovery().await;
        client.start_router_service_discovery().await;
        Ok(client)
    }

    /// A meta-storage read through this client's current cluster, of one key
    /// or (`prefix`) of every key under it.
    fn meta_get(
        &self,
        prefix: bool,
    ) -> Arc<
        dyn Fn(
                Vec<u8>,
            ) -> futures::future::BoxFuture<
                'static,
                Result<crate::proto::meta_storagepb::GetResponse>,
            > + Send
            + Sync,
    > {
        let meta_cluster = Arc::downgrade(&self.cluster);
        let timeout = self.timeout;
        Arc::new(move |key| {
            let cluster = meta_cluster.clone();
            async move {
                let cluster = cluster.upgrade().ok_or(Error::ContextCanceled)?;
                let request = {
                    let guard = cluster.read().await;
                    let request = crate::proto::meta_storagepb::GetRequest {
                        header: Some(crate::proto::meta_storagepb::RequestHeader {
                            cluster_id: guard.0.id(),
                            ..Default::default()
                        }),
                        range_end: if prefix {
                            super::meta_storage_client::get_prefix(&key)
                        } else {
                            Vec::new()
                        },
                        key,
                        ..Default::default()
                    };
                    guard.0.meta_storage_get(request, timeout)
                };
                request.await
            }
            .boxed()
        })
    }

    fn get_or_create_conn(&self) -> super::router_service_discovery::GetOrCreateConn {
        let connection = self.connection.clone();
        Arc::new(move |url| {
            let connection = connection.clone();
            async move { connection.get_or_create_channel(&url).await }.boxed()
        })
    }

    /// pd-client `innerClient.init`'s `NewRouterServiceDiscovery`.
    async fn start_router_service_discovery(&self) {
        let (cluster_id, keyspace_id) = {
            let guard = self.cluster.read().await;
            (guard.0.id(), guard.0.keyspace_id())
        };
        let meta_get = self.meta_get(true);
        let connect = self.get_or_create_conn();
        let connection = self.connection.clone();
        let remove: super::router_service_discovery::RemoveConn =
            Arc::new(move |url| connection.remove_channel(url));
        let discovery = super::router_service_discovery::RouterServiceDiscovery::new(
            &self.cancellation,
            cluster_id,
            keyspace_id,
            self.connection.tls_configured(),
            self.connection.options.max_retry_times,
            meta_get,
            connect,
            remove,
        )
        .await;
        self.connection
            .set_router_discovery(Some(discovery.clone()));
        *self.router_discovery.lock().await = Some(discovery);
    }

    /// The TSO client's `ScheduleCheckMemberChanged` / `CheckMemberChanged`
    /// over this client's membership refresh.
    fn tso_discovery_hooks(&self) -> super::cluster::TsoDiscoveryHooks {
        let refresh = Arc::downgrade(&self.reconnect);
        let check_cluster = Arc::downgrade(&self.cluster);
        let check_refresh = Arc::downgrade(&self.reconnect);
        let connection = self.connection.clone();
        let timeout = self.timeout;
        super::cluster::TsoDiscoveryHooks {
            schedule_check_member_changed: Arc::new(move || {
                if let Some(refresh) = refresh.upgrade() {
                    refresh.members_changed.notify_one();
                    refresh.tso_members_changed.notify_one();
                }
            }),
            check_member_changed: Arc::new(move || {
                let cluster = check_cluster.clone();
                let refresh = check_refresh.clone();
                let connection = connection.clone();
                // The refresh may replace and close the very TSO client that
                // asked for it, which joins the asking task; run it in its own
                // task so the asker can be abandoned meanwhile.
                let refresh_task = tokio::spawn(async move {
                    let (Some(cluster), Some(refresh)) = (cluster.upgrade(), refresh.upgrade())
                    else {
                        return Err(Error::ContextCanceled);
                    };
                    refresh_cluster(&cluster, &connection, &refresh, timeout, 0, None, true).await
                });
                async move {
                    refresh_task
                        .await
                        .unwrap_or_else(|error| Err(Error::JoinError(error)))
                }
                .boxed()
            }),
        }
    }

    async fn start_discovery(&self) {
        self.cluster
            .read()
            .await
            .0
            .set_tso_discovery_hooks(self.tso_discovery_hooks());
        let cluster = Arc::downgrade(&self.cluster);
        let connection = self.connection.clone();
        let refresh = self.reconnect.clone();
        let cancellation = self.cancellation.clone();
        let timeout = self.timeout;
        let mode = self.cluster.read().await.0.service_mode();
        let cluster_id = self.cluster.read().await.0.id();
        let rm_meta_get = self.meta_get(false);
        let rm_connect = self.get_or_create_conn();
        let rm_connection = self.connection.clone();
        let rm_cancellation = self.cancellation.clone();
        let rm_mode = mode.clone();
        let worker = tokio::spawn(async move {
            let mut mode_changes = mode.subscribe();
            // pd-client `setServiceMode`: each new service mode resets the
            // resource manager discovery (`resetResourceManagerDiscoveryLocked`).
            let watch_mode = async {
                let mut changes = rm_mode.subscribe();
                let mut applied = pdpb::ServiceMode::UnknownSvcMode;
                loop {
                    let observed = rm_mode
                        .snapshot()
                        .and_then(|info| info.service_modes.first().copied())
                        .and_then(|mode| pdpb::ServiceMode::try_from(mode).ok());
                    if let Some(new_mode) = observed.filter(|mode| *mode != applied) {
                        set_service_mode(
                            new_mode,
                            cluster_id,
                            &rm_connection,
                            &rm_cancellation,
                            &rm_meta_get,
                            &rm_connect,
                        )
                        .await;
                        applied = new_mode;
                    }
                    if changes.changed().await.is_err() {
                        break;
                    }
                }
            };
            let observe_mode = async {
                let mut ticks = tokio::time::interval_at(
                    tokio::time::Instant::now() + super::service_discovery::UPDATE_INTERVAL,
                    super::service_discovery::UPDATE_INTERVAL,
                );
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticks.tick().await;
                    let Some(cluster) = cluster.upgrade() else {
                        break;
                    };
                    let observe = connection.prepare_service_mode(&cluster.read().await.0, timeout);
                    if let Err(error) = observe.await {
                        let urls = cluster.read().await.0.service_urls();
                        log::warn!(
                            "sorted-urls":serde = urls,
                            error:% = error;
                            "[pd] failed to update service mode"
                        );
                        refresh.members_changed.notify_one();
                    }
                }
            };
            let discover = async {
                loop {
                    tokio::select! {
                        _ = sleep(super::service_discovery::UPDATE_INTERVAL) => {},
                        _ = mode_changes.changed() => {},
                        _ = refresh.tso_members_changed.notified() => {},
                    }
                    let Some(cluster) = cluster.upgrade() else {
                        break;
                    };
                    // pd-client `tsoServiceDiscovery.startCheckMemberLoop`.
                    if let Err(error) =
                        refresh_timestamp_cached(&cluster, &connection, &refresh, timeout, None)
                            .await
                    {
                        log::warn!(error:% = error; "[tso] failed to update member");
                    }
                }
            };
            let members = async {
                let period = super::service_discovery::MEMBER_UPDATE_INTERVAL;
                let mut ticks =
                    tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = ticks.tick() => {},
                        _ = refresh.members_changed.notified() => {},
                    }
                    let Some(cluster) = cluster.upgrade() else {
                        break;
                    };
                    if let Err(error) =
                        refresh_cluster(&cluster, &connection, &refresh, timeout, 0, None, false)
                            .await
                    {
                        let urls = cluster.read().await.0.service_urls();
                        log::warn!(
                            "sorted-urls":serde = urls,
                            error:% = error;
                            "[pd] failed to update member"
                        );
                    }
                }
            };
            let health = async {
                let period = super::region_service::HEALTH_CHECK_INTERVAL;
                let mut ticks =
                    tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticks.tick().await;
                    let Some(cluster) = cluster.upgrade() else {
                        break;
                    };
                    let probe = cluster.read().await.0.check_health(timeout);
                    probe.await;
                }
            };
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {},
                _ = async { tokio::join!(observe_mode, discover, members, health, watch_mode); } => {},
            }
        });
        *self.discovery_worker.lock().await = Some(worker);
    }

    /// Installs keyspace-aware timestamp routing before API-v2 publication.
    pub async fn set_keyspace(&self, meta: &keyspacepb::KeyspaceMeta) -> Result<()> {
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(Error::ContextCanceled),
            result = refresh_cluster(&self.cluster, &self.connection, &self.reconnect, self.timeout, 0, Some(meta), true) => result,
        }
    }

    #[cfg(test)]
    pub(crate) async fn reconnect_for_test(&self) -> Result<()> {
        self.reconnect(0).await
    }

    /// Cancels requests/discovery, joins streams, and releases connections even
    /// while request handles are retained. A cancelled close can be resumed.
    pub async fn close(&self) {
        self.cancellation.cancel();
        let mut worker = self.discovery_worker.lock().await;
        if let Some(handle) = worker.as_mut() {
            let _ = handle.await;
            worker.take();
        }
        if let Some(discovery) = self.router_discovery.lock().await.take() {
            self.connection.set_router_discovery(None);
            discovery.close().await;
        }
        if let Some(discovery) = self.connection.set_resource_manager_discovery(None) {
            discovery.close().await;
        }
        let _members = self.reconnect.members.lock().await;
        let _timestamp = self.reconnect.timestamp.lock().await;
        let retire = self.cluster.write().await.0.start_close();
        retire.await;
        self.cluster.write().await.0.finish_retirement();
    }

    #[cfg(test)]
    pub(crate) async fn tso_for_test(&self) -> Arc<super::tso::Cli> {
        self.cluster.read().await.0.tso_for_test()
    }

    pub async fn cluster_id(&self) -> u64 {
        self.cluster.read().await.0.id()
    }

    /// Go `createTokenDispatcher`: the dispatcher reads the current leader
    /// from the published cluster and stops with this client.
    fn create_token_dispatcher(&self) -> super::resource_manager_client::TokenDispatcher {
        let connect_cluster = Arc::downgrade(&self.cluster);
        let leader_cluster = Arc::downgrade(&self.cluster);
        let refresh = self.reconnect.clone();
        super::resource_manager_client::TokenDispatcher::new(
            Arc::new(move || {
                let cluster = connect_cluster.clone();
                async move {
                    let cluster = cluster.upgrade().ok_or(Error::ContextCanceled)?;
                    let connect = cluster.read().await.0.resource_manager_token_client();
                    connect.await
                }
                .boxed()
            }),
            Arc::new(move || {
                let cluster = leader_cluster.clone();
                async move {
                    let cluster = cluster.upgrade()?;
                    let guard = cluster.read().await;
                    guard.0.resource_manager_leader_url()
                }
                .boxed()
            }),
            Arc::new(move || refresh.members_changed.notify_one()),
            self.cancellation.clone(),
        )
    }

    /// Go `respForMetaStorageErr`: transport errors schedule a membership
    /// check; header errors are errors too. Both record the failure duration.
    fn meta_storage_result<T>(
        &self,
        result: Result<T>,
        start: Instant,
        header: impl FnOnce(&T) -> Option<&crate::proto::meta_storagepb::ResponseHeader>,
        failed: &prometheus::Histogram,
    ) -> Result<T> {
        match result {
            Err(error) => {
                failed.observe(start.elapsed().as_secs_f64());
                self.reconnect.members_changed.notify_one();
                Err(error)
            }
            Ok(response) => {
                if let Some(error) = header(&response).and_then(|header| header.error.as_ref()) {
                    failed.observe(start.elapsed().as_secs_f64());
                    return Err(Error::StringError(
                        super::meta_storage_client::meta_storage_error_text(error),
                    ));
                }
                Ok(response)
            }
        }
    }

    /// Go `pd.Client.GetServiceDiscovery`, for the PD HTTP client: the
    /// members this client discovered, the leader first, as its membership
    /// refreshes. A membership check scheduled through it wakes this client's
    /// member refresh. The view does not keep the client alive; once the
    /// client is dropped it has no members.
    pub fn get_service_discovery(&self) -> Arc<dyn super::http::ServiceDiscovery> {
        Arc::new(MemberServiceDiscovery {
            cluster: Arc::downgrade(&self.cluster),
            refresh: Arc::downgrade(&self.reconnect),
        })
    }
}

// The PD client's membership as the HTTP client's service discovery.
struct MemberServiceDiscovery {
    cluster: std::sync::Weak<RwLock<(Cluster, Instant)>>,
    refresh: std::sync::Weak<DiscoveryRefresh>,
}

#[async_trait]
impl super::http::ServiceDiscovery for MemberServiceDiscovery {
    async fn get_all_service_clients(&self) -> Vec<super::http::ServiceClient> {
        let Some(cluster) = self.cluster.upgrade() else {
            return Vec::new();
        };
        let urls = cluster.read().await.0.service_client_urls();
        urls.into_iter()
            .map(|(url, is_leader)| super::http::ServiceClient::new(url, is_leader))
            .collect()
    }

    fn schedule_check_member_changed(&self) {
        if let Some(refresh) = self.refresh.upgrade() {
            refresh.members_changed.notify_one();
        }
    }

    // The PD client owns its discovery; the HTTP client never closes it.
    async fn close(&self) {}
}

#[async_trait]
impl RetryClientTrait for RetryClient<Cluster> {
    async fn cluster_id(&self) -> u64 {
        RetryClient::cluster_id(self).await
    }

    async fn close(&self) {
        RetryClient::close(self).await;
    }

    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry_recording_pd_wait!(self, "get_region", |cluster| {
            let key = key.clone();
            cluster
                .get_region(key.clone(), self.timeout)
                .map(move |result| {
                    result.and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
                })
        })
    }

    async fn get_region_for_cache(
        self: Arc<Self>,
        key: Vec<u8>,
        previous: bool,
        leader_only: bool,
    ) -> Result<RegionWithLeader> {
        retry_recording_pd_wait!(self, "get_region_for_cache", |cluster| {
            let key = key.clone();
            cluster
                .get_region_routed(key.clone(), self.timeout, true, previous, !leader_only)
                .map(move |result| {
                    result.and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
                })
        })
    }

    async fn get_region_with_buckets(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry_recording_pd_wait!(self, "get_region_with_buckets", |cluster| {
            let key = key.clone();
            cluster
                .get_region_with_buckets(key.clone(), self.timeout, true)
                .map(move |result| {
                    result.and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
                })
        })
    }

    async fn get_prev_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry_recording_pd_wait!(self, "get_prev_region", |cluster| {
            let key = key.clone();
            cluster
                .get_prev_region(key.clone(), self.timeout)
                .map(move |result| {
                    result.and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
                })
        })
    }

    async fn get_prev_region_with_buckets(
        self: Arc<Self>,
        key: Vec<u8>,
    ) -> Result<RegionWithLeader> {
        retry_recording_pd_wait!(self, "get_prev_region_with_buckets", |cluster| {
            let key = key.clone();
            cluster
                .get_prev_region_with_buckets(key.clone(), self.timeout, true)
                .map(move |result| {
                    result.and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
                })
        })
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        retry_recording_pd_wait!(self, "get_region_by_id", |cluster| {
            cluster
                .get_region_by_id(region_id, self.timeout)
                .map(move |result| {
                    result.and_then(|resp| {
                        region_from_response(resp, || Error::RegionNotFoundInResponse { region_id })
                    })
                })
        })
    }

    async fn get_region_by_id_with_buckets(
        self: Arc<Self>,
        region_id: RegionId,
    ) -> Result<RegionWithLeader> {
        retry_recording_pd_wait!(self, "get_region_by_id_with_buckets", |cluster| {
            cluster
                .get_region_by_id_with_buckets(region_id, self.timeout, true)
                .map(move |result| {
                    result.and_then(|resp| {
                        region_from_response(resp, || Error::RegionNotFoundInResponse { region_id })
                    })
                })
        })
    }

    async fn scan_regions(
        self: Arc<Self>,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: usize,
    ) -> Result<Vec<RegionWithLeader>> {
        retry_recording_pd_wait!(self, "scan_regions", |cluster| {
            cluster
                .scan_regions_routed(
                    start_key.clone(),
                    end_key.clone(),
                    limit,
                    self.timeout,
                    true,
                )
                .map(|result| result.and_then(regions_from_scan_response))
        })
    }

    async fn batch_scan_regions(
        self: Arc<Self>,
        ranges: Vec<pdpb::KeyRange>,
        limit: usize,
        options: RegionScanOptions,
    ) -> Result<Vec<RegionWithLeader>> {
        retry!(self, "batch_scan_regions", |cluster| {
            cluster
                .batch_scan_regions(ranges.clone(), limit, options, self.timeout)
                .map(|result| result.and_then(regions_from_batch_scan_response))
        })
    }

    async fn split_regions(
        self: Arc<Self>,
        split_keys: Vec<Vec<u8>>,
        retry_limit: u64,
    ) -> Result<pdpb::SplitRegionsResponse> {
        retry!(self, "split_regions", |cluster| {
            let split_keys = split_keys.clone();
            cluster.split_regions(split_keys, retry_limit, self.timeout)
        })
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<Option<metapb::Store>> {
        retry_recording_pd_wait!(self, "get_store", |cluster| {
            cluster
                .get_store(id, self.timeout)
                .map(|result| result.map(|resp| resp.store))
        })
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        retry!(self, "get_all_stores", |cluster| {
            cluster
                .get_all_stores(self.timeout)
                .map(|result| result.map(|resp| resp.stores))
        })
    }

    async fn get_all_stores_allowing_router_service(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        retry!(self, "get_all_stores", |cluster| {
            cluster
                .get_all_stores_allowing_router_service(self.timeout)
                .map(|result| result.map(|resp| resp.stores))
        })
    }

    /// pd-client `client.GetTS`: re-dispatch on a leader change or a callee
    /// mismatch, at once the first time and then every `retryInterval`, at
    /// most `maxTSORetryTimes` times. The TSO client recovers its own streams;
    /// this path never reconnects PD.
    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        const MAX_TSO_RETRY_TIMES: usize = 20;
        const RETRY_INTERVAL: Duration = Duration::from_millis(100);
        let started = Instant::now();
        let stats = pd_stats("get_timestamp");
        let slot = self.cluster.read().await.0.tso_slot();
        let request = async {
            let mut retry_count = 0;
            loop {
                let result = super::tso::get_ts_async(|| slot.read().unwrap().clone()).await;
                let error = match result {
                    Ok(timestamp) => return Ok(timestamp),
                    Err(error) => error,
                };
                let error_ref: &(dyn std::error::Error + 'static) = &error;
                let retryable = super::errs::is_leader_change(Some(error_ref))
                    || super::errs::is_callee_mismatch(Some(error_ref));
                if !retryable || retry_count + 1 >= MAX_TSO_RETRY_TIMES {
                    super::metrics::global_metrics()
                        .tso_retry_count
                        .observe(retry_count as f64);
                    return Err(error);
                }
                log::debug!(
                    "retry-count" = retry_count,
                    error:% = error;
                    "[pd] get tso failed, retrying"
                );
                if retry_count != 0 {
                    sleep(RETRY_INTERVAL).await;
                }
                retry_count += 1;
            }
        };
        let result = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(Error::ContextCanceled),
            result = request => result,
        };
        crate::util::record_pd_wait_time(started);
        stats.done(result)
    }

    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        retry!(self, "get_min_timestamp", |cluster| {
            cluster.get_min_timestamp(self.timeout)
        })
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        retry!(self, "set_external_timestamp", |cluster| {
            cluster.set_external_timestamp(timestamp, self.timeout)
        })
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        retry!(self, "get_external_timestamp", |cluster| {
            cluster.get_external_timestamp(self.timeout)
        })
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
        Ok(self.clone().update_safepoint_value(safepoint).await? == safepoint)
    }

    async fn update_safepoint_value(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        retry!(self, "update_gc_safepoint", |cluster| {
            cluster
                .update_safepoint(safepoint, self.timeout)
                .map(|result| result.map(|resp| resp.new_safe_point))
        })
    }

    async fn update_gc_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        safe_point: u64,
    ) -> Result<u64> {
        retry!(self, "update_gc_safe_point_v2", |cluster| {
            cluster
                .update_gc_safe_point_v2(keyspace_id, safe_point, self.timeout)
                .map(|result| result.map(|resp| resp.new_safe_point))
        })
    }

    async fn update_service_gc_safe_point(
        self: Arc<Self>,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        retry!(self, "update_service_gc_safe_point", |cluster| {
            cluster
                .update_service_gc_safe_point(&service_id, ttl, safe_point, self.timeout)
                .map(|result| result.map(|resp| resp.min_safe_point))
        })
    }

    async fn update_service_safe_point_v2(
        self: Arc<Self>,
        keyspace_id: u32,
        service_id: String,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        retry!(self, "update_service_safe_point_v2", |cluster| {
            cluster
                .update_service_safe_point_v2(
                    keyspace_id,
                    &service_id,
                    ttl,
                    safe_point,
                    self.timeout,
                )
                .map(|result| result.map(|resp| resp.min_safe_point))
        })
    }

    async fn get_all_keyspaces(
        &self,
        start_id: u32,
        limit: u32,
    ) -> Result<Vec<keyspacepb::KeyspaceMeta>> {
        retry!(self, "get_all_keyspaces", |cluster| {
            cluster.get_all_keyspaces(start_id, limit, self.timeout)
        })
    }

    async fn get_gc_state(self: Arc<Self>, keyspace_id: u32) -> Result<pdpb::GetGcStateResponse> {
        retry!(self, "get_gc_state", |cluster| {
            cluster.get_gc_state(keyspace_id, self.timeout)
        })
    }

    fn options(&self) -> Option<Arc<super::opt::Options>> {
        Some(self.connection.options.clone())
    }

    async fn get_all_members(self: Arc<Self>) -> Result<pdpb::GetMembersResponse> {
        retry!(self, "get_all_members", |cluster| {
            cluster.get_all_members(self.timeout)
        })
    }

    async fn store_global_config(
        self: Arc<Self>,
        config_path: String,
        mut changes: Vec<pdpb::GlobalConfigItem>,
    ) -> Result<()> {
        // Go's StoreGlobalConfig is one leader RPC, not retried, projecting
        // each item's name, value, kind and payload.
        for item in &mut changes {
            item.error = None;
        }
        let request = {
            let cluster = self.cluster.read().await;
            cluster
                .0
                .store_global_config(config_path, changes, self.timeout)
        };
        request.await
    }

    async fn list_resource_groups(
        self: Arc<Self>,
        keyspace_id: u32,
        with_ru_stats: bool,
    ) -> Result<Vec<crate::proto::resource_manager::ResourceGroup>> {
        retry!(self, "list_resource_groups", |cluster| {
            cluster.list_resource_groups(keyspace_id, with_ru_stats, self.timeout)
        })
    }

    async fn put_resource_group(
        self: Arc<Self>,
        keyspace_id: u32,
        group: crate::proto::resource_manager::ResourceGroup,
        add: bool,
    ) -> Result<String> {
        retry!(self, "put_resource_group", |cluster| {
            cluster.put_resource_group(keyspace_id, group.clone(), add, self.timeout)
        })
    }

    async fn delete_resource_group(
        self: Arc<Self>,
        keyspace_id: u32,
        name: String,
    ) -> Result<String> {
        retry!(self, "delete_resource_group", |cluster| {
            cluster.delete_resource_group(keyspace_id, name.clone(), self.timeout)
        })
    }

    async fn get_resource_group(
        self: Arc<Self>,
        keyspace_id: u32,
        name: String,
        with_ru_stats: bool,
    ) -> Result<Option<crate::proto::resource_manager::ResourceGroup>> {
        // Go's GetResourceGroup is one unretried RPC; a leader change
        // schedules a membership check.
        let request = {
            let cluster = self.cluster.read().await;
            cluster
                .0
                .get_resource_group(keyspace_id, name, with_ru_stats)
        };
        let result = request.await;
        if let Err(error) = &result {
            if super::errs::is_leader_change(Some(error)) {
                self.reconnect.members_changed.notify_one();
            }
        }
        result
    }

    async fn acquire_token_buckets(
        self: Arc<Self>,
        request: crate::proto::resource_manager::TokenBucketsRequest,
    ) -> Result<Vec<crate::proto::resource_manager::TokenBucketResponse>> {
        let this: &Self = &self;
        let dispatcher = self
            .token_dispatcher
            .get_or_init(|| async move { this.create_token_dispatcher() })
            .await;
        dispatcher
            .acquire_token_buckets(request, &self.cancellation)
            .await
    }

    async fn meta_storage_get(
        self: Arc<Self>,
        request: crate::proto::meta_storagepb::GetRequest,
    ) -> Result<crate::proto::meta_storagepb::GetResponse> {
        let start = Instant::now();
        let request = {
            let cluster = self.cluster.read().await;
            cluster.0.meta_storage_get(request, self.timeout)
        };
        let result = request.await;
        let metrics = super::metrics::global_metrics();
        metrics
            .cmd_duration_get
            .observe(start.elapsed().as_secs_f64());
        self.meta_storage_result(
            result,
            start,
            |response| response.header.as_ref(),
            &metrics.cmd_failed_duration_get,
        )
    }

    async fn meta_storage_put(
        self: Arc<Self>,
        request: crate::proto::meta_storagepb::PutRequest,
    ) -> Result<crate::proto::meta_storagepb::PutResponse> {
        let start = Instant::now();
        let request = {
            let cluster = self.cluster.read().await;
            cluster.0.meta_storage_put(request, self.timeout)
        };
        let result = request.await;
        let metrics = super::metrics::global_metrics();
        metrics
            .cmd_duration_put
            .observe(start.elapsed().as_secs_f64());
        self.meta_storage_result(
            result,
            start,
            |response| response.header.as_ref(),
            &metrics.cmd_failed_duration_put,
        )
    }

    async fn meta_storage_watch(
        self: Arc<Self>,
        request: crate::proto::meta_storagepb::WatchRequest,
    ) -> Result<tokio::sync::mpsc::Receiver<super::metastorage::WatchResponse>> {
        let open = {
            let cluster = self.cluster.read().await;
            cluster.0.meta_storage_watch(request)
        };
        Ok(spawn_watch_forwarder(open, self.cancellation.clone()))
    }

    async fn advance_txn_safe_point(
        self: Arc<Self>,
        keyspace_id: u32,
        target: u64,
    ) -> Result<pdpb::AdvanceTxnSafePointResponse> {
        retry!(self, "advance_txn_safe_point", |cluster| {
            cluster.advance_txn_safe_point(keyspace_id, target, self.timeout)
        })
    }

    async fn advance_gc_safe_point(
        self: Arc<Self>,
        keyspace_id: u32,
        target: u64,
    ) -> Result<pdpb::AdvanceGcSafePointResponse> {
        retry!(self, "advance_gc_safe_point", |cluster| {
            cluster.advance_gc_safe_point(keyspace_id, target, self.timeout)
        })
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<u64>,
        group: String,
    ) -> Result<pdpb::ScatterRegionResponse> {
        retry!(self, "scatter_regions", |cluster| {
            cluster.scatter_regions(region_ids.clone(), group.clone(), self.timeout)
        })
    }

    async fn get_operator(self: Arc<Self>, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        retry!(self, "get_operator", |cluster| {
            cluster.get_operator(region_id, self.timeout)
        })
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        retry!(self, "load_keyspace", |cluster| {
            cluster.load_keyspace(keyspace, self.timeout)
        })
    }
}

impl fmt::Debug for RetryClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("pd::RetryClient")
            .field("timeout", &self.timeout)
            .finish()
    }
}

fn region_from_response(
    mut resp: pdpb::GetRegionResponse,
    err: impl FnOnce() -> Error,
) -> Result<RegionWithLeader> {
    let region = resp.region.take().ok_or_else(err)?;
    let mut region = RegionWithLeader::new(region, resp.leader.take());
    region.buckets = resp.buckets.take();
    region.pending_peers = std::mem::take(&mut resp.pending_peers);
    region.down_peers = std::mem::take(&mut resp.down_peers);
    Ok(region)
}

fn regions_from_scan_response(resp: pdpb::ScanRegionsResponse) -> Result<Vec<RegionWithLeader>> {
    if !resp.regions.is_empty() {
        return resp
            .regions
            .into_iter()
            .map(|mut entry| {
                let region = entry.region.take().ok_or_else(|| {
                    Error::StringError("PD ScanRegions response has no region metadata".to_owned())
                })?;
                Ok(RegionWithLeader {
                    region,
                    leader: entry.leader.take(),
                    buckets: entry.buckets.take(),
                    pending_peers: std::mem::take(&mut entry.pending_peers),
                    down_peers: std::mem::take(&mut entry.down_peers),
                })
            })
            .collect();
    }

    Ok(resp
        .region_metas
        .into_iter()
        .enumerate()
        .map(|(index, region)| RegionWithLeader::new(region, resp.leaders.get(index).cloned()))
        .collect())
}

fn regions_from_batch_scan_response(
    resp: pdpb::BatchScanRegionsResponse,
) -> Result<Vec<RegionWithLeader>> {
    resp.regions
        .into_iter()
        .map(|mut entry| {
            let region = entry.region.take().ok_or_else(|| {
                Error::StringError("PD BatchScanRegions response has no region metadata".to_owned())
            })?;
            Ok(RegionWithLeader {
                region,
                leader: entry.leader.take(),
                buckets: entry.buckets.take(),
                pending_peers: std::mem::take(&mut entry.pending_peers),
                down_peers: std::mem::take(&mut entry.down_peers),
            })
        })
        .collect()
}

// A node-like thing that can be connected to.
#[async_trait]
trait Reconnect {
    type Cl;
    fn cancellation(&self) -> Option<&Cancellation> {
        None
    }
    async fn reconnect(&self, interval_sec: u64) -> Result<()>;
}

#[async_trait]
impl Reconnect for RetryClient<Cluster> {
    type Cl = Cluster;

    fn cancellation(&self) -> Option<&Cancellation> {
        Some(&self.cancellation)
    }

    async fn reconnect(&self, interval_sec: u64) -> Result<()> {
        let reconnect = refresh_cluster(
            &self.cluster,
            &self.connection,
            &self.reconnect,
            self.timeout,
            interval_sec,
            None,
            true,
        );
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(Error::ContextCanceled),
            result = reconnect => result,
        }
    }
}

impl<Cl> Drop for RetryClient<Cl> {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// pd-client `innerClient.setServiceMode`'s resource-manager half
/// (`resetResourceManagerDiscoveryLocked`) and its line.
async fn set_service_mode(
    new_mode: pdpb::ServiceMode,
    cluster_id: u64,
    connection: &Connection,
    cancellation: &Cancellation,
    meta_get: &super::resource_manager_discovery::MetaGet,
    connect: &super::router_service_discovery::GetOrCreateConn,
) {
    match new_mode {
        pdpb::ServiceMode::PdSvcMode => {
            if let Some(old) = connection.set_resource_manager_discovery(None) {
                old.close().await;
            }
        }
        pdpb::ServiceMode::ApiSvcMode => {
            let discovery = super::resource_manager_discovery::ResourceManagerDiscovery::new(
                cancellation,
                cluster_id,
                meta_get.clone(),
                connect.clone(),
                // The token dispatcher reconnects when the URL it reads
                // differs from its stream's.
                Arc::new(|| {}),
            );
            discovery.init().await;
            if let Some(old) = connection.set_resource_manager_discovery(Some(discovery)) {
                old.close().await;
            }
        }
        pdpb::ServiceMode::UnknownSvcMode => {
            log::warn!("[pd] intend to switch to unknown service mode, just return");
        }
    }
    log::info!("new-mode" = new_mode.as_str_name(); "[pd] service mode changed");
}

async fn refresh_cluster(
    cluster: &RwLock<(Cluster, Instant)>,
    connection: &Connection,
    refresh: &DiscoveryRefresh,
    timeout: Duration,
    interval_sec: u64,
    keyspace: Option<&keyspacepb::KeyspaceMeta>,
    observe_mode: bool,
) -> Result<()> {
    let begin = Instant::now();
    let _members = refresh.members.lock().await;
    let prepare = {
        let guard = cluster.read().await;
        if keyspace.is_none()
            && interval_sec > 0
            && begin <= guard.1 + Duration::from_secs(interval_sec)
        {
            return Ok(());
        }
        connection.prepare_reconnect(&guard.0, timeout)
    };
    let leader = prepare.await?;
    if !observe_mode {
        let _timestamp = refresh.timestamp.lock().await;
        let mut guard = cluster.write().await;
        guard.0.install_leader(leader)?;
        guard.1 = Instant::now();
        return Ok(());
    }
    refresh_timestamp(
        cluster,
        connection,
        refresh,
        timeout,
        keyspace,
        Some(leader),
    )
    .await?;
    cluster.write().await.1 = Instant::now();
    Ok(())
}

async fn refresh_timestamp(
    cluster: &RwLock<(Cluster, Instant)>,
    connection: &Connection,
    refresh: &DiscoveryRefresh,
    timeout: Duration,
    keyspace: Option<&keyspacepb::KeyspaceMeta>,
    leader: Option<super::cluster::LeaderConnection>,
) -> Result<()> {
    if let Some(leader) = leader {
        let _refresh = refresh.timestamp.lock().await;
        cluster.write().await.0.install_leader(leader)?;
    }
    // Mode observation never holds the timestamp/group publication lock.
    let observe = connection.prepare_service_mode(&cluster.read().await.0, timeout);
    if observe.await.is_err() {
        refresh.members_changed.notify_one();
    }
    refresh_timestamp_cached(cluster, connection, refresh, timeout, keyspace).await
}

async fn refresh_timestamp_cached(
    cluster: &RwLock<(Cluster, Instant)>,
    connection: &Connection,
    refresh: &DiscoveryRefresh,
    timeout: Duration,
    keyspace: Option<&keyspacepb::KeyspaceMeta>,
) -> Result<()> {
    let mut mode_changes = cluster.read().await.0.service_mode().subscribe();
    loop {
        let update = async {
            let _refresh = refresh.timestamp.lock().await;
            let prepare = connection.prepare_timestamp(&cluster.read().await.0, timeout, keyspace);
            let timestamp = prepare.await?;
            let retire = cluster
                .write()
                .await
                .0
                .install_timestamp(timestamp, timeout)?;
            retire.await;
            cluster.write().await.0.finish_retirement();
            Ok(())
        };
        tokio::select! {
            biased;
            _ = mode_changes.changed() => continue,
            result = update => return result,
        }
    }
}

/// A watch's response stream: tonic's, or a test's.
#[async_trait]
pub(crate) trait WatchStream: Send {
    async fn message(
        &mut self,
    ) -> std::result::Result<Option<crate::proto::meta_storagepb::WatchResponse>, tonic::Status>;
}

#[async_trait]
impl WatchStream for tonic::codec::Streaming<crate::proto::meta_storagepb::WatchResponse> {
    async fn message(
        &mut self,
    ) -> std::result::Result<Option<crate::proto::meta_storagepb::WatchResponse>, tonic::Status>
    {
        tonic::codec::Streaming::message(self).await
    }
}

/// pd-client meta storage `Watch`'s channel and receiving goroutine.
///
/// pd-client's `Watch` returns its channel once grpc-go has created the
/// stream, and receives in a goroutine. tonic's call resolves only on
/// the response headers, which PD sends with the first event, so the
/// stream is opened inside the receiving task: awaited here, a watch
/// on a quiet key never returned, and the resource-group controller's
/// main loop waited in it forever (never reaching its loop, and never
/// seeing its own cancellation). A failure to open closes the channel,
/// which a watcher treats as a closed watch to retry.
pub(crate) fn spawn_watch_forwarder<S: WatchStream + 'static>(
    open: impl std::future::Future<Output = Result<S>> + Send + 'static,
    cancellation: Cancellation,
) -> tokio::sync::mpsc::Receiver<super::metastorage::WatchResponse> {
    let (events, receiver) = tokio::sync::mpsc::channel(100);
    tokio::spawn(async move {
        let opened = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            _ = events.closed() => return,
            opened = open => opened,
        };
        let mut stream = match opened {
            Ok(stream) => stream,
            Err(error) => {
                // pd-client returns the open error to its caller unlogged.
                let _ = error;
                return;
            }
        };
        loop {
            let message = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                _ = events.closed() => return,
                message = stream.message() => message,
            };
            let resp = match message {
                Ok(Some(resp)) => resp,
                Ok(None) => {
                    // Go's `Recv` reports the stream end as `io.EOF`.
                    log::warn!(error = "EOF"; "watch stream closed");
                    return;
                }
                Err(error) => {
                    log::warn!(error:% = error; "watch stream closed");
                    return;
                }
            };
            // A compacted watch revision requires watching again from
            // the higher compact revision.
            let mut compact_revision = 0;
            if let Some(error) = resp
                .header
                .as_ref()
                .and_then(|header| header.error.as_ref())
            {
                if error.r#type == crate::proto::meta_storagepb::ErrorType::DataCompacted as i32 {
                    compact_revision = resp.compact_revision;
                    log::info!(
                        "required watch revision" = compact_revision,
                        err:% = super::meta_storage_client::meta_storage_error_text(error);
                        "watch stream closed due to data compacted"
                    );
                }
            }
            let response = super::metastorage::WatchResponse {
                events: resp.events,
                compact_revision,
            };
            if events.send(response).await.is_err() {
                return;
            }
        }
    });
    receiver
}

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;

    use futures::executor;
    use futures::future::ready;

    use super::*;
    use crate::internal_err;

    /// A watch stream answering scripted responses, then nothing.
    struct ScriptedWatch(std::collections::VecDeque<crate::proto::meta_storagepb::WatchResponse>);

    #[async_trait]
    impl WatchStream for ScriptedWatch {
        async fn message(
            &mut self,
        ) -> std::result::Result<Option<crate::proto::meta_storagepb::WatchResponse>, tonic::Status>
        {
            match self.0.pop_front() {
                Some(response) => Ok(Some(response)),
                None => std::future::pending().await,
            }
        }
    }

    // pd-client's `Watch` returns its channel once the stream is created and
    // receives in a goroutine; tonic resolves the call only on PD's headers,
    // which come with the first event. The channel must come back before the
    // stream opens, or a watch on a quiet key never returns (the resource
    // group controller's main loop hung there, so it never stopped).
    #[tokio::test]
    async fn a_watch_answers_its_channel_before_the_stream_opens() {
        let cancellation = Cancellation::default();
        let (open, opened) = tokio::sync::oneshot::channel::<ScriptedWatch>();
        let mut receiver = spawn_watch_forwarder(
            async move { Ok(opened.await.expect("the test opens the stream")) },
            cancellation.clone(),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err(),
            "nothing arrives while the stream is opening"
        );
        let response = crate::proto::meta_storagepb::WatchResponse {
            events: vec![Default::default()],
            ..Default::default()
        };
        let _ = open.send(ScriptedWatch(vec![response].into()));
        assert_eq!(receiver.recv().await.unwrap().events.len(), 1);
        cancellation.cancel();
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn a_watch_whose_stream_never_opens_ends_on_cancellation() {
        let cancellation = Cancellation::default();
        let mut receiver = spawn_watch_forwarder(
            std::future::pending::<Result<ScriptedWatch>>(),
            cancellation.clone(),
        );
        cancellation.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                .await
                .expect("cancellation ends the watch")
                .is_none()
        );
    }

    #[tokio::test]
    async fn logical_command_metrics_do_not_count_recovered_attempts_as_failures() {
        struct MockClient {
            attempts: AtomicUsize,
        }
        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = ();
            async fn reconnect(&self, _: u64) -> Result<()> {
                Ok(())
            }
        }
        impl MockClient {
            async fn get_region(&self) -> Result<()> {
                retry_core!(
                    self,
                    "get_region",
                    async {
                        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(internal_err!("leader changed"))
                        } else {
                            Ok(())
                        }
                    }
                    .await
                )
            }
        }
        super::super::metrics::init_and_register_metrics(Default::default());
        let metrics = super::super::metrics::global_metrics();
        let total = metrics.cmd_duration_get_region.get_sample_count();
        let failed = metrics.cmd_failed_duration_get_region.get_sample_count();
        let client = MockClient {
            attempts: AtomicUsize::new(0),
        };
        client.get_region().await.unwrap();
        assert_eq!(client.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            metrics.cmd_duration_get_region.get_sample_count(),
            total + 1
        );
        assert_eq!(
            metrics.cmd_failed_duration_get_region.get_sample_count(),
            failed
        );
    }
    #[tokio::test]
    async fn missing_region_is_returned_to_cache_without_reconnect() {
        struct MockClient {
            cluster: RwLock<((), Instant)>,
        }
        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = ();
            async fn reconnect(&self, _: u64) -> Result<()> {
                panic!("empty region metadata must use the cache PD backoff, not reconnect");
            }
        }
        async fn lookup(client: Arc<MockClient>) -> Result<()> {
            retry!(client, "test", |_cluster| ready(Err(
                Error::RegionForKeyNotFound { key: b"a".to_vec() }
            )))
        }
        let client = Arc::new(MockClient {
            cluster: RwLock::new(((), Instant::now())),
        });
        assert!(matches!(
            lookup(client).await,
            Err(Error::RegionForKeyNotFound { .. })
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_reconnect() {
        struct MockClient {
            reconnect_count: AtomicUsize,
            cluster: RwLock<((), Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = ();

            async fn reconnect(&self, _: u64) -> Result<()> {
                self.reconnect_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Not actually unimplemented, we just don't care about the error.
                Err(Error::Unimplemented)
            }
        }

        async fn retry_err(client: Arc<MockClient>) -> Result<()> {
            retry!(client, "test", |_c| ready(Err(internal_err!("whoops"))))
        }

        async fn retry_ok(client: Arc<MockClient>) -> Result<()> {
            retry!(client, "test", |_c| ready(Ok::<_, Error>(())))
        }

        executor::block_on(async {
            let client = Arc::new(MockClient {
                reconnect_count: AtomicUsize::new(0),
                cluster: RwLock::new(((), Instant::now())),
            });

            assert!(retry_err(client.clone()).await.is_err());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                MAX_REQUEST_COUNT
            );

            client
                .reconnect_count
                .store(0, std::sync::atomic::Ordering::SeqCst);
            assert!(retry_ok(client.clone()).await.is_ok());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            );
        })
    }

    #[test]
    fn test_retry() {
        struct MockClient {
            cluster: RwLock<(AtomicUsize, Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = Mutex<usize>;

            async fn reconnect(&self, _: u64) -> Result<()> {
                Ok(())
            }
        }

        async fn retry_max_err(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry!(client, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        async fn retry_max_ok(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry!(client, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        executor::block_on(async {
            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(1000));

            assert!(retry_max_err(client.clone(), max_retries).await.is_err());
            assert_eq!(
                client.cluster.read().await.0.load(Ordering::SeqCst),
                LEADER_CHANGE_RETRY
            );

            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(2));

            assert!(retry_max_ok(client.clone(), max_retries).await.is_ok());
            assert_eq!(client.cluster.read().await.0.load(Ordering::SeqCst), 2);
        })
    }

    #[test]
    fn source_scan_regions_decodes_extended_and_legacy_shapes() {
        let extended = pdpb::ScanRegionsResponse {
            regions: vec![pdpb::Region {
                region: Some(metapb::Region {
                    id: 1,
                    start_key: b"a".to_vec(),
                    end_key: b"z".to_vec(),
                    ..Default::default()
                }),
                leader: Some(metapb::Peer {
                    id: 7,
                    store_id: 8,
                    ..Default::default()
                }),
                pending_peers: vec![metapb::Peer {
                    id: 9,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let regions = regions_from_scan_response(extended).unwrap();
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].id(), 1);
        assert_eq!(regions[0].leader.as_ref().unwrap().store_id, 8);
        assert_eq!(regions[0].pending_peers[0].id, 9);

        let legacy = pdpb::ScanRegionsResponse {
            region_metas: vec![metapb::Region {
                id: 2,
                start_key: b"z".to_vec(),
                ..Default::default()
            }],
            leaders: vec![metapb::Peer {
                id: 10,
                store_id: 11,
                ..Default::default()
            }],
            ..Default::default()
        };
        let regions = regions_from_scan_response(legacy).unwrap();
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].id(), 2);
        assert_eq!(regions[0].leader.as_ref().unwrap().store_id, 11);
    }

    #[test]
    fn source_scan_regions_rejects_an_extended_entry_without_metadata() {
        let error = regions_from_scan_response(pdpb::ScanRegionsResponse {
            regions: vec![pdpb::Region::default()],
            ..Default::default()
        })
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "PD ScanRegions response has no region metadata"
        );
    }
}

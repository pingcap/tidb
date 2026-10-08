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

    /// Loads PD's modern transaction/GC safe-point state for one keyspace.
    async fn get_gc_state(self: Arc<Self>, _keyspace_id: u32) -> Result<pdpb::GetGcStateResponse> {
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
        Ok(client)
    }

    async fn start_discovery(&self) {
        let cluster = Arc::downgrade(&self.cluster);
        let connection = self.connection.clone();
        let refresh = self.reconnect.clone();
        let cancellation = self.cancellation.clone();
        let timeout = self.timeout;
        let mode = self.cluster.read().await.0.service_mode();
        let worker = tokio::spawn(async move {
            let mut mode_changes = mode.subscribe();
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
                        refresh.members_changed.notify_one();
                        log::warn!("PD service mode observation: {error}");
                    }
                }
            };
            let discover = async {
                loop {
                    tokio::select! {
                        _ = sleep(super::service_discovery::UPDATE_INTERVAL) => {},
                        _ = mode_changes.changed() => {},
                        _ = async { connection.options.enable_tso_follower_proxy_ch.receiver.lock().await.recv().await } => {},
                    }
                    let Some(cluster) = cluster.upgrade() else {
                        break;
                    };
                    if let Err(error) =
                        refresh_timestamp_cached(&cluster, &connection, &refresh, timeout, None)
                            .await
                    {
                        log::warn!("PD service discovery refresh: {error}");
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
                        log::warn!("PD membership refresh: {error}");
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
                _ = async { tokio::join!(observe_mode, discover, members, health); } => {},
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
            if let Err(error) = handle.await {
                log::error!("PD discovery worker failed: {error}");
            }
            worker.take();
        }
        let _members = self.reconnect.members.lock().await;
        let _timestamp = self.reconnect.timestamp.lock().await;
        let retire = self.cluster.write().await.0.start_close();
        retire.await;
        self.cluster.write().await.0.finish_retirement();
    }

    #[cfg(test)]
    pub(crate) async fn tso_for_test(&self) -> super::timestamp::TimestampOracle {
        self.cluster.read().await.0.tso_for_test()
    }

    pub async fn cluster_id(&self) -> u64 {
        self.cluster.read().await.0.id()
    }
}

#[async_trait]
impl RetryClientTrait for RetryClient<Cluster> {
    async fn close(&self) {
        RetryClient::close(self).await;
    }

    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry!(self, "get_region", |cluster| {
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
        retry!(self, "get_region_for_cache", |cluster| {
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
        retry!(self, "get_region_with_buckets", |cluster| {
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
        retry!(self, "get_prev_region", |cluster| {
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
        retry!(self, "get_prev_region_with_buckets", |cluster| {
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
        retry!(self, "get_region_by_id", |cluster| {
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
        retry!(self, "get_region_by_id_with_buckets", |cluster| {
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
        retry!(self, "scan_regions", |cluster| {
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
        retry!(self, "get_store", |cluster| {
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

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        retry!(self, "get_timestamp", |cluster| cluster.get_timestamp())
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

    async fn get_gc_state(self: Arc<Self>, keyspace_id: u32) -> Result<pdpb::GetGcStateResponse> {
        retry!(self, "get_gc_state", |cluster| {
            cluster.get_gc_state(keyspace_id, self.timeout)
        })
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

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;

    use futures::executor;
    use futures::future::ready;

    use super::*;
    use crate::internal_err;

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

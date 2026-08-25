// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::prelude::*;
use futures::stream::BoxStream;
use log::info;
use tokio::sync::{watch, Mutex, RwLock};
use tonic::codegen::http::uri::PathAndQuery;
use tonic_prost::ProstCodec;

use crate::compat::stream_fn;
use crate::kv::{ReplicaReadConfig, ReplicaReadType};
use crate::locate::ReplicaFlowsType;
use crate::locate::{MixedReplicaSelection, ReplicaSelectorState};
use crate::pd::codec::{CodecPdClient, PdRegionCodec};
use crate::pd::retry::RetryClientTrait;
use crate::pd::Cluster;
use crate::pd::RetryClient;
use crate::proto::errorpb;
use crate::proto::keyspacepb;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::proto::pdpb;
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::region_cache::{RegionCache, StoreLiveness};
use crate::request::{
    build_keyspace_name, keyspace_from_pd_meta, keyspace_id_from_pd_meta, KeyMode, Keyspace,
};
use crate::retry::RetryBackoffer;
use crate::store::KvConnect;
use crate::store::RegionStore;
use crate::store::TikvConnect;
use crate::store::{KvClient, Store};
use crate::BoundRange;
use crate::Config;
use crate::Key;
use crate::Result;
use crate::SecurityManager;
use crate::Timestamp;

/// The PdClient handles all the encoding stuff.
///
/// Raw APIs does not require encoding/decoding at all.
/// All keys in all places (client, PD, TiKV) are in the same encoding (here called "raw format").
///
/// Transactional APIs are a bit complicated.
/// We need encode and decode keys when we communicate with PD, but not with TiKV.
/// We encode keys before sending requests to PD, and decode keys in the response from PD.
/// That's all we need to do with encoding.
///
///  client -encoded-> PD, PD -encoded-> client
///  client -raw-> TiKV, TiKV -raw-> client
///
/// The reason for the behavior is that in transaction mode, TiKV encode keys for MVCC.
/// In raw mode, TiKV doesn't encode them.
/// TiKV tells PD using its internal representation, whatever the encoding is.
/// So if we use transactional APIs, keys in PD are encoded and PD does not know about the encoding stuff.
#[async_trait]
pub trait PdClient: Send + Sync + 'static {
    type KvClient: KvClient + Send + Sync + 'static;

    /// In transactional API, `region` is decoded (keys in raw format).
    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore>;

    /// Resolves a region using the source replica-read policy. Custom clients
    /// retain ordinary leader routing until they implement cache-backed
    /// selection themselves.
    async fn map_region_to_store_with_replica(
        self: Arc<Self>,
        region: RegionWithLeader,
        _config: ReplicaReadConfig,
        _selector_state: ReplicaSelectorState,
        _is_read_request: bool,
    ) -> Result<RegionStore> {
        self.map_region_to_store(region).await
    }

    /// Resolves a TiFlash-only route for operations such as BatchCop. Custom
    /// clients retain an explicit unsupported error until they provide their
    /// own cache/transport mapping.
    async fn map_region_to_tiflash_store(
        self: Arc<Self>,
        _region: RegionWithLeader,
        _load_balance: bool,
        _labels: &[metapb::StoreLabel],
    ) -> Result<RegionStore> {
        Err(crate::Error::StringError(
            "TiFlash region routing is not supported by this PD client".to_owned(),
        ))
    }

    /// In transactional API, the key and returned region are both decoded (keys in raw format).
    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader>;

    /// Locate the region using an inclusive end key, matching client-go's
    /// `LocateEndKey` reverse-scan routing semantics.
    async fn region_for_end_key(&self, _key: &Key) -> Result<RegionWithLeader> {
        Err(crate::Error::StringError(
            "PD end-key region lookup is not supported by this client".to_owned(),
        ))
    }

    /// In transactional API, the returned region is decoded (keys in raw format)
    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader>;

    /// Loads a bounded consecutive sequence of regions beginning at `key`.
    /// The default is source-compatible for custom PD clients, while
    /// `PdRpcClient` overrides it with PD's one-RPC ScanRegions path.
    async fn batch_load_regions_from_key(
        &self,
        key: &Key,
        count: usize,
        _backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        let mut next = key.clone();
        let mut regions = Vec::with_capacity(count);
        while regions.len() < count {
            let region = self.region_for_key(&next).await?;
            let end = region.end_key();
            if !end.is_empty() && end <= next {
                return Err(crate::Error::StringError(
                    "PD returned a region that does not advance batch loading".to_owned(),
                ));
            }
            regions.push(region);
            if end.is_empty() {
                break;
            }
            next = end;
        }
        if regions.is_empty() {
            return Err(crate::Error::StringError(
                "PD returned no region while batch loading regions from key".to_owned(),
            ));
        }
        Ok(regions)
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    /// Split regions at logical keys for large-transaction write spreading.
    async fn split_regions(
        self: Arc<Self>,
        _split_keys: Vec<Vec<u8>>,
        _retry_limit: u64,
    ) -> Result<Vec<u64>> {
        Err(crate::Error::Unimplemented)
    }

    /// PD cluster identifier retained by the connected client.
    ///
    /// Mock and custom PD implementations that do not model PD membership may
    /// retain the source-compatible default of zero.
    async fn cluster_id(&self) -> u64 {
        0
    }

    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Err(crate::Error::StringError(
            "PD minimum timestamp is not supported by this client".to_owned(),
        ))
    }

    async fn set_external_timestamp(self: Arc<Self>, _timestamp: u64) -> Result<()> {
        Err(crate::Error::StringError(
            "PD external timestamp is not supported by this client".to_owned(),
        ))
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        Err(crate::Error::StringError(
            "PD external timestamp is not supported by this client".to_owned(),
        ))
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool>;

    async fn update_safepoint_value(self: Arc<Self>, _safepoint: u64) -> Result<u64> {
        Err(crate::Error::Unimplemented)
    }

    /// Modern PD GC-state API used by root `tikv` visibility checks.
    async fn get_gc_state(self: Arc<Self>) -> Result<pdpb::GcState> {
        Err(crate::Error::Unimplemented)
    }

    async fn advance_transaction_safe_point(
        self: Arc<Self>,
        _target: u64,
    ) -> Result<pdpb::AdvanceTxnSafePointResponse> {
        Err(crate::Error::Unimplemented)
    }

    async fn advance_gc_safe_point(
        self: Arc<Self>,
        _target: u64,
    ) -> Result<pdpb::AdvanceGcSafePointResponse> {
        Err(crate::Error::Unimplemented)
    }

    async fn scatter_regions(
        self: Arc<Self>,
        _region_ids: Vec<u64>,
        _group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        Err(crate::Error::Unimplemented)
    }

    async fn get_operator(self: Arc<Self>, _region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        Err(crate::Error::Unimplemented)
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta>;

    /// Loads metadata using client-go's canonical default keyspace name.
    async fn get_keyspace_meta(&self, name: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.load_keyspace(&build_keyspace_name(name)).await
    }

    /// Loads the canonical keyspace name and returns its legacy numeric ID.
    /// Disabled keyspaces and API V3 identities are rejected exactly as by
    /// client-go's `internal/locate.GetKeyspaceID`.
    async fn get_keyspace_id(&self, name: &str) -> Result<u32> {
        let meta = self.get_keyspace_meta(name).await?;
        keyspace_id_from_pd_meta(name, &meta)
    }

    /// In transactional API, `key` is in raw format
    async fn store_for_key(self: Arc<Self>, key: &Key) -> Result<RegionStore> {
        let region = self.region_for_key(key).await?;
        self.map_region_to_store(region).await
    }

    async fn store_for_id(self: Arc<Self>, id: RegionId) -> Result<RegionStore> {
        let region = self.region_for_id(id).await?;
        self.map_region_to_store(region).await
    }

    async fn all_stores(&self) -> Result<Vec<Store>>;

    fn group_keys_by_region<K, K2>(
        self: Arc<Self>,
        keys: impl Iterator<Item = K> + Send + Sync + 'static,
    ) -> BoxStream<'static, Result<(Vec<K2>, RegionWithLeader)>>
    where
        K: AsRef<Key> + Into<K2> + Send + Sync + 'static,
        K2: Send + Sync + 'static,
    {
        let keys = keys.peekable();
        stream_fn(keys, move |mut keys| {
            let this = self.clone();
            async move {
                if let Some(key) = keys.next() {
                    let region = this.region_for_key(key.as_ref()).await?;
                    let mut grouped = vec![key.into()];
                    while let Some(key) = keys.peek() {
                        if !region.contains(key.as_ref()) {
                            break;
                        }
                        grouped.push(keys.next().unwrap().into());
                    }
                    Ok(Some((keys, (grouped, region))))
                } else {
                    Ok(None)
                }
            }
        })
        .boxed()
    }

    /// Returns a Stream which iterates over the contexts for each region covered by range.
    fn regions_for_range(
        self: Arc<Self>,
        range: BoundRange,
    ) -> BoxStream<'static, Result<RegionWithLeader>> {
        let (start_key, end_key) = range.into_keys();
        stream_fn(Some(start_key), move |start_key| {
            let end_key = end_key.clone();
            let this = self.clone();
            async move {
                let start_key = match start_key {
                    None => return Ok(None),
                    Some(sk) => sk,
                };

                let region = this.region_for_key(&start_key).await?;
                let region_end = region.end_key();
                if end_key
                    .map(|x| x <= region_end && !x.is_empty())
                    .unwrap_or(false)
                    || region_end.is_empty()
                {
                    return Ok(Some((None, region)));
                }
                Ok(Some((Some(region_end), region)))
            }
        })
        .boxed()
    }

    /// Returns a Stream which iterates over the contexts for ranges in the same region.
    fn group_ranges_by_region(
        self: Arc<Self>,
        mut ranges: Vec<kvrpcpb::KeyRange>,
    ) -> BoxStream<'static, Result<(Vec<kvrpcpb::KeyRange>, RegionWithLeader)>> {
        ranges.reverse();
        stream_fn(Some(ranges), move |ranges| {
            let this = self.clone();
            async move {
                let mut ranges = match ranges {
                    None => return Ok(None),
                    Some(r) => r,
                };

                if let Some(range) = ranges.pop() {
                    let start_key: Key = range.start_key.clone().into();
                    let end_key: Key = range.end_key.clone().into();
                    let region = this.region_for_key(&start_key).await?;
                    let region_start = region.start_key();
                    let region_end = region.end_key();
                    let mut grouped = vec![];
                    if !region_end.is_empty() && (end_key > region_end || end_key.is_empty()) {
                        grouped.push(make_key_range(start_key.into(), region_end.clone().into()));
                        ranges.push(make_key_range(region_end.into(), end_key.into()));
                        return Ok(Some((Some(ranges), (grouped, region))));
                    }
                    grouped.push(range);

                    while let Some(range) = ranges.pop() {
                        let start_key: Key = range.start_key.clone().into();
                        let end_key: Key = range.end_key.clone().into();
                        if start_key < region_start || start_key > region_end {
                            ranges.push(range);
                            break;
                        }
                        if !region_end.is_empty() && (end_key > region_end || end_key.is_empty()) {
                            grouped
                                .push(make_key_range(start_key.into(), region_end.clone().into()));
                            ranges.push(make_key_range(region_end.into(), end_key.into()));
                            return Ok(Some((Some(ranges), (grouped, region))));
                        }
                        grouped.push(range);
                    }
                    Ok(Some((Some(ranges), (grouped, region))))
                } else {
                    Ok(None)
                }
            }
        })
        .boxed()
    }

    async fn update_leader(&self, ver_id: RegionVerId, leader: metapb::Peer) -> Result<()>;

    /// Installs region metadata carried by TiKV's `EpochNotMatch` response.
    ///
    /// Custom PD clients that do not own a region cache can retain the
    /// no-op default. The concrete PD client preserves client-go's cache
    /// refresh path by inserting every replacement region before the next
    /// routing attempt.
    async fn update_region_cache(&self, _regions: Vec<RegionWithLeader>) -> Result<()> {
        Ok(())
    }

    /// Refreshes cached bucket metadata from TiKV's
    /// `BucketVersionNotMatch` response. Custom clients without a region
    /// cache retain the source-compatible no-op default.
    async fn update_buckets(&self, _ver_id: RegionVerId, _version: u64, _keys: Vec<Vec<u8>>) {}

    /// Records TiKV's estimated server queue delay. Custom PD clients that
    /// do not own a region/store cache retain a no-op default.
    fn record_server_load(&self, _store_id: StoreId, _estimated_wait_ms: u32) {}

    /// Remembers the physical proxy that successfully forwarded a logical
    /// leader request. Custom clients without a region cache retain a no-op.
    async fn record_forwarding_proxy(&self, _ver_id: RegionVerId, _store_id: StoreId) {}

    /// Handles a TiKV transport failure after a physical route has been
    /// selected. Returning `true` retains the legacy generic cache eviction;
    /// the concrete PD client checks source store liveness and keeps the
    /// region snapshot so its selector can choose another replica.
    async fn on_send_failure(self: Arc<Self>, _route: Option<&RegionStore>) -> bool {
        true
    }

    async fn invalidate_region_cache(&self, ver_id: RegionVerId);

    async fn invalidate_store_cache(&self, store_id: StoreId);

    /// Closes an address only if its cached connection generation is no newer
    /// than `version`. This prevents an old in-flight request from evicting a
    /// reconnect that has already replaced its pool.
    async fn close_kv_client_addr_ver(&self, _address: &str, _version: u64) {}
}

struct CachedKvClient<C> {
    client: C,
    version: u64,
}

struct StoreHealthFeedbackRequester<KvC: KvConnect + Clone + Send + Sync + 'static> {
    kv_connect: KvC,
    kv_client_cache: Arc<RwLock<HashMap<String, CachedKvClient<KvC::KvClient>>>>,
    kv_client_versions: Arc<RwLock<HashMap<String, u64>>>,
    kv_client_lifecycle: Arc<Mutex<()>>,
    kv_client_closed: Arc<AtomicBool>,
    event_listener: Arc<dyn crate::store::ClientEventListener>,
}

impl<KvC: KvConnect + Clone + Send + Sync + 'static> StoreHealthFeedbackRequester<KvC> {
    async fn kv_client(&self, address: &str) -> Result<KvC::KvClient> {
        if self.kv_client_closed.load(Ordering::Acquire) {
            return Err(crate::Error::StringError("rpc client is closed".to_owned()));
        }
        if let Some(cached) = self.kv_client_cache.read().await.get(address) {
            return Ok(cached.client.clone());
        }
        let _lifecycle = self.kv_client_lifecycle.lock().await;
        if self.kv_client_closed.load(Ordering::Acquire) {
            return Err(crate::Error::StringError("rpc client is closed".to_owned()));
        }
        if let Some(cached) = self.kv_client_cache.read().await.get(address) {
            return Ok(cached.client.clone());
        }
        let client = self.kv_connect.connect(address).await?;
        let version = {
            let mut versions = self.kv_client_versions.write().await;
            let version = versions.get(address).copied().unwrap_or(0).wrapping_add(1);
            versions.insert(address.to_owned(), version);
            version
        };
        let client = client.with_connection_info(address.to_owned(), version);
        self.kv_client_cache.write().await.insert(
            address.to_owned(),
            CachedKvClient {
                client: client.clone(),
                version,
            },
        );
        Ok(client)
    }

    async fn request(&self, address: &str) -> Result<()> {
        let client = self.kv_client(address).await?;
        client.set_event_listener(self.event_listener.clone());
        let response = client
            .dispatch_with_timeout(
                &kvrpcpb::GetHealthFeedbackRequest::default(),
                Some(Duration::from_secs(2)),
            )
            .await?
            .downcast::<kvrpcpb::GetHealthFeedbackResponse>()
            .map_err(|_| {
                crate::Error::StringError(
                    "GetHealthFeedback returned an unexpected response type".to_owned(),
                )
            })?;
        if let Some(region_error) = response.region_error {
            return Err(crate::Error::RegionError(Box::new(region_error)));
        }
        Ok(())
    }
}

/// This client converts requests for the logical TiKV cluster into requests
/// for a single TiKV store using PD and internal logic.
pub struct PdRpcClient<KvC: KvConnect + Send + Sync + 'static = TikvConnect, Cl = Cluster> {
    pd: Arc<RetryClient<Cl>>,
    kv_connect: KvC,
    kv_client_cache: Arc<RwLock<HashMap<String, CachedKvClient<KvC::KvClient>>>>,
    kv_client_versions: Arc<RwLock<HashMap<String, u64>>>,
    kv_client_lifecycle: Arc<Mutex<()>>,
    kv_client_closed: Arc<AtomicBool>,
    store_token_counts: Arc<std::sync::Mutex<HashMap<StoreId, Arc<AtomicI64>>>>,
    keyspace_meta: Option<keyspacepb::KeyspaceMeta>,
    enable_forwarding: bool,
    zone_label: String,
    security_mgr: Arc<SecurityManager>,
    store_liveness_timeout: Duration,
    region_cache: Arc<RegionCache<CodecPdClient<RetryClient<Cl>>>>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct HealthCheckRequest {
    #[prost(string, tag = "1")]
    service: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct HealthCheckResponse {
    #[prost(int32, tag = "1")]
    status: i32,
}

const HEALTH_SERVING: i32 = 1;
const HEALTH_UNKNOWN: i32 = 0;
const HEALTH_SERVICE_UNKNOWN: i32 = 3;
const STORE_RE_RESOLVE_INTERVAL: Duration = Duration::from_secs(30);
static STORE_LIVENESS_FLIGHTS: LazyLock<
    Mutex<HashMap<String, watch::Receiver<Option<StoreLiveness>>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn source_health_status_liveness(status: i32) -> StoreLiveness {
    match status {
        HEALTH_SERVING => StoreLiveness::Reachable,
        HEALTH_UNKNOWN | HEALTH_SERVICE_UNKNOWN => StoreLiveness::Unknown,
        _ => StoreLiveness::Unreachable,
    }
}

async fn probe_store_liveness(
    security_mgr: Arc<SecurityManager>,
    target: String,
    timeout: Duration,
) -> StoreLiveness {
    let request = async {
        let channel = security_mgr.connect(&target, |channel| channel).await?;
        let mut client = tonic::client::Grpc::new(channel);
        client
            .unary(
                tonic::Request::new(HealthCheckRequest {
                    service: String::new(),
                }),
                PathAndQuery::from_static("/grpc.health.v1.Health/Check"),
                ProstCodec::<HealthCheckRequest, HealthCheckResponse>::default(),
            )
            .await
            .map_err(crate::Error::from)
    };
    match tokio::time::timeout(timeout, request).await {
        Ok(Ok(response)) => source_health_status_liveness(response.into_inner().status),
        Ok(Err(error)) => {
            log::debug!("source store liveness check failed for {target}: {error}");
            StoreLiveness::Unreachable
        }
        Err(_) => StoreLiveness::Unreachable,
    }
}

async fn request_store_liveness_singleflight<F, Fut>(target: String, probe: F) -> StoreLiveness
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = StoreLiveness> + Send + 'static,
{
    let mut receiver = {
        let mut flights = STORE_LIVENESS_FLIGHTS.lock().await;
        if let Some(receiver) = flights.get(&target) {
            receiver.clone()
        } else {
            let (sender, receiver) = watch::channel(None);
            flights.insert(target.clone(), receiver.clone());
            let flight_target = target.clone();
            tokio::spawn(async move {
                let liveness = probe().await;
                let _ = sender.send(Some(liveness));
                STORE_LIVENESS_FLIGHTS.lock().await.remove(&flight_target);
            });
            receiver
        }
    };
    loop {
        if let Some(liveness) = *receiver.borrow() {
            return liveness;
        }
        if receiver.changed().await.is_err() {
            return StoreLiveness::Unknown;
        }
    }
}

/// Parses the Go duration grammar used by `store-liveness-timeout`. The
/// setting is intentionally kept separate from request/RPC timeouts: source
/// health probes bound both connection establishment and Health/Check by it.
fn parse_source_duration(value: &str) -> Option<Duration> {
    let value = value.trim();
    for (suffix, unit) in [
        ("ms", 1e-3_f64),
        ("us", 1e-6),
        ("µs", 1e-6),
        ("ns", 1e-9),
        ("s", 1.0),
        ("m", 60.0),
        ("h", 3_600.0),
    ] {
        if let Some(number) = value.strip_suffix(suffix) {
            let seconds = number.trim().parse::<f64>().ok()? * unit;
            if seconds.is_sign_negative() || !seconds.is_finite() {
                return None;
            }
            return Some(Duration::from_secs_f64(seconds));
        }
    }
    None
}

impl<KvC, Cl> PdRpcClient<KvC, Cl>
where
    KvC: KvConnect + Send + Sync + 'static,
    RetryClient<Cl>: RetryClientTrait + Send + Sync + 'static,
{
    fn store_token_count(&self, store_id: StoreId) -> Arc<AtomicI64> {
        self.store_token_counts
            .lock()
            .unwrap()
            .entry(store_id)
            .or_insert_with(|| Arc::new(AtomicI64::new(0)))
            .clone()
    }

    /// Builds the physical route selected by `internal/locate` state.
    ///
    /// The request context always names `target_peer`. If `proxy_peer` is
    /// present, the connection instead targets that proxy and carries the
    /// logical target address in forwarding metadata, exactly as client-go's
    /// `RPCContext` does.
    pub(crate) async fn map_region_to_route(
        self: Arc<Self>,
        region: RegionWithLeader,
        target_peer: metapb::Peer,
        proxy_peer: Option<metapb::Peer>,
    ) -> Result<RegionStore> {
        let target_store = self
            .region_cache
            .get_store_by_id(target_peer.store_id)
            .await?;
        let forwarded = proxy_peer.is_some();
        let physical_store = match proxy_peer {
            Some(proxy_peer) => {
                self.region_cache
                    .get_store_by_id(proxy_peer.store_id)
                    .await?
            }
            None => target_store.clone(),
        };
        let kv_client = self.kv_client(&physical_store.address).await?;
        kv_client.set_event_listener(self.region_cache.client_event_listener());

        let health_status = self.region_cache.store_health_status(target_peer.store_id);
        let store_token_count = self.store_token_count(target_peer.store_id);
        let physical_store_id = physical_store.id;
        let physical_endpoint_type = crate::store::EndpointType::from_store(&physical_store);
        let mut route = RegionStore::new(region, Arc::new(kv_client))
            .with_target(physical_store.address)
            .with_physical_store(physical_store_id, physical_endpoint_type)
            .with_target_peer(target_peer)
            .with_resource_control_access_location(&self.zone_label, &target_store)
            .with_store_token_count(store_token_count);
        if let Some(health_status) = health_status {
            route = route.with_health_status(health_status);
        }
        if forwarded {
            route = route.with_forwarded_host(target_store.address);
        }
        Ok(route)
    }

    async fn map_leader_route(
        self: Arc<Self>,
        region: RegionWithLeader,
        selector_state: &ReplicaSelectorState,
    ) -> Result<RegionStore> {
        let leader = region
            .leader
            .clone()
            .ok_or_else(|| crate::Error::LeaderNotFound {
                region: region.ver_id(),
            })?;
        self.region_cache.get_store_by_id(leader.store_id).await?;
        let leader_liveness = self.region_cache.store_liveness(leader.store_id);
        let proxy = if self.enable_forwarding {
            self.region_cache
                .proxy_for_unavailable_leader(&region, selector_state)
                .await?
        } else {
            None
        };
        if source_forwarding_exhausted(
            self.enable_forwarding,
            leader_liveness,
            selector_state,
            leader.id,
            proxy.is_some(),
        ) {
            return Err(selector_exhausted_error());
        }
        self.map_region_to_route(region, leader, proxy).await
    }

    async fn request_store_liveness(&self, route: &RegionStore) -> StoreLiveness {
        if route.physical_endpoint_type != crate::store::EndpointType::TiKv
            || route.target.is_empty()
        {
            return StoreLiveness::Unknown;
        }
        let Some(physical_store_id) = route.physical_store_id else {
            return StoreLiveness::Unknown;
        };
        if self.region_cache.store_resolve_state(physical_store_id)
            != Some(crate::region_cache::StoreResolveState::Resolved)
        {
            return StoreLiveness::Unknown;
        }
        if self.store_liveness_timeout.is_zero() {
            return StoreLiveness::Unreachable;
        }
        let target = route.target.clone();
        let probe_target = target.clone();
        let security_mgr = self.security_mgr.clone();
        let timeout = self.store_liveness_timeout;
        request_store_liveness_singleflight(target, move || {
            probe_store_liveness(security_mgr, probe_target, timeout)
        })
        .await
    }

    fn start_store_health_check_loop(self: Arc<Self>, store_id: StoreId, route: RegionStore) {
        let client = Arc::downgrade(&self);
        tokio::spawn(async move {
            let mut route = route;
            let mut last_resolve = std::time::Instant::now();
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Some(client) = client.upgrade() else {
                    return;
                };
                if client.kv_client_closed.load(Ordering::Acquire) {
                    client.region_cache.finish_store_health_check(store_id);
                    return;
                }
                if last_resolve.elapsed() >= STORE_RE_RESOLVE_INTERVAL {
                    last_resolve = std::time::Instant::now();
                    match client.region_cache.refresh_store_by_id(store_id).await {
                        Ok(Some(store)) => {
                            let endpoint_type = crate::store::EndpointType::from_store(&store);
                            route.target = store.address;
                            route.physical_endpoint_type = endpoint_type;
                        }
                        Ok(None) => {
                            client.region_cache.finish_store_health_check(store_id);
                            return;
                        }
                        Err(error) => {
                            log::debug!(
                                "source store liveness re-resolution failed for {store_id}: {error}"
                            );
                        }
                    }
                }
                let liveness = client.request_store_liveness(&route).await;
                client.region_cache.set_store_liveness(store_id, liveness);
                if liveness == StoreLiveness::Reachable {
                    client.region_cache.finish_store_health_check(store_id);
                    return;
                }
            }
        });
    }
}

#[async_trait]
impl<KvC: KvConnect + Send + Sync + 'static> PdClient for PdRpcClient<KvC> {
    type KvClient = KvC::KvClient;

    async fn split_regions(
        self: Arc<Self>,
        split_keys: Vec<Vec<u8>>,
        retry_limit: u64,
    ) -> Result<Vec<u64>> {
        Ok(self
            .region_cache
            .split_regions(split_keys, retry_limit)
            .await?
            .regions_id)
    }

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        self.map_leader_route(region, &ReplicaSelectorState::default())
            .await
    }

    async fn on_send_failure(self: Arc<Self>, route: Option<&RegionStore>) -> bool {
        let Some(route) = route else {
            return true;
        };
        let Some(store_id) = route.physical_store_id else {
            return true;
        };
        let liveness = self.request_store_liveness(route).await;
        self.region_cache.set_store_liveness(store_id, liveness);
        if liveness != StoreLiveness::Reachable {
            self.region_cache
                .invalidate_store_epoch_for_region(&route.region_with_leader.ver_id(), store_id)
                .await;
        }
        if liveness != StoreLiveness::Reachable
            && route.physical_endpoint_type == crate::store::EndpointType::TiKv
            && self.region_cache.begin_store_health_check(store_id)
        {
            self.clone()
                .start_store_health_check_loop(store_id, route.clone());
        }
        // `replicaSelector.onSendFailure` preserves the region snapshot. Its
        // per-request attempts and the resulting liveness state choose the
        // next candidate; evicting the entire region here is Rust-only
        // behavior that discards that source selector state.
        false
    }

    async fn map_region_to_tiflash_store(
        self: Arc<Self>,
        region: RegionWithLeader,
        load_balance: bool,
        labels: &[metapb::StoreLabel],
    ) -> Result<RegionStore> {
        let peer = self
            .region_cache
            .select_tiflash_peer(&region, load_balance, labels)
            .await
            .map_err(|reason| {
                crate::Error::StringError(format!("TiFlash route unavailable: {reason:?}"))
            })?;
        self.map_region_to_route(region, peer, None).await
    }

    async fn map_region_to_store_with_replica(
        self: Arc<Self>,
        region: RegionWithLeader,
        config: ReplicaReadConfig,
        selector_state: ReplicaSelectorState,
        is_read_request: bool,
    ) -> Result<RegionStore> {
        let config = config.for_source_build();
        let busy_threshold_ms = if selector_state.busy_threshold_disabled() {
            0
        } else {
            config.busy_threshold_ms
        };
        let leader_liveness = if let Some(leader) = region.leader.as_ref() {
            self.region_cache.get_store_by_id(leader.store_id).await?;
            self.region_cache.store_liveness(leader.store_id)
        } else {
            None
        };
        let leader_epoch_stale = match region.leader.as_ref() {
            Some(leader) => {
                self.region_cache
                    .store_epoch_is_stale(&region.ver_id(), leader.store_id)
                    .await
            }
            None => false,
        };
        if let Some(leader) = region.leader.clone() {
            if !leader_epoch_stale && selector_state.should_force_leader(leader.id) {
                self.region_cache.get_store_by_id(leader.store_id).await?;
                if self.region_cache.store_liveness(leader.store_id)
                    == Some(StoreLiveness::Reachable)
                {
                    return self
                        .map_leader_route(region, &selector_state)
                        .await
                        .map(|route| route.with_busy_threshold(busy_threshold_ms));
                }
            }
        }
        if matches!(config.read_type, ReplicaReadType::Leader) && !config.stale_read {
            if let Some(leader) = region.leader.clone().filter(|leader| {
                source_leader_needs_mixed_fallback(
                    self.enable_forwarding,
                    leader_epoch_stale,
                    leader_liveness,
                    &selector_state,
                    leader.id,
                )
            }) {
                if !config.leader_only {
                    if let Some(follower) = self
                        .region_cache
                        .select_mixed_replica(
                            &region,
                            &config.labels,
                            &config.stores,
                            &selector_state,
                            MixedReplicaSelection {
                                read_type: ReplicaReadType::Follower,
                                leader_only: false,
                                prefer_leader: false,
                                labels_requested: !config.labels.is_empty(),
                            },
                        )
                        .await?
                        .filter(|peer| peer.id != leader.id)
                    {
                        // `nextForReplicaReadLeader` falls back to a follower
                        // with leader-read wire context after the leader is
                        // exhausted or returns a hintless NotLeader; it is a
                        // probe, not a replica read.
                        let route = self
                            .map_region_to_route(region, follower, None)
                            .await?
                            .with_busy_threshold(busy_threshold_ms);
                        return Ok(
                            if source_leader_fallback_uses_replica_read(&selector_state, leader.id)
                            {
                                // A caller-configured read deadline switches the
                                // source retry to a genuine follower read.
                                route
                            } else {
                                // Ordinary exhaustion and hintless NotLeader use
                                // the follower only as a leader-read probe.
                                route.with_force_leader_read()
                            },
                        );
                    }
                    // `ReplicaSelectMixedStrategy.next` gives a leader that
                    // was skipped only for the busy-leader probe another
                    // chance after every follower is unavailable or has
                    // replied without a leader hint. This is deliberately
                    // before cache invalidation: reloading the unchanged
                    // region from PD would only resume hammering the same
                    // cached leader.
                    self.region_cache.get_store_by_id(leader.store_id).await?;
                    if source_can_restore_suspect_leader(
                        &selector_state,
                        leader.id,
                        leader_epoch_stale,
                        leader_liveness,
                    ) {
                        return self
                            .map_leader_route(region, &selector_state)
                            .await
                            .map(|route| {
                                route
                                    .with_busy_threshold(busy_threshold_ms)
                                    .with_restored_suspect_leader()
                            });
                    }
                    if !selector_state.has_deadline_exceeded() {
                        self.region_cache
                            .invalidate_region_cache(region.ver_id())
                            .await;
                    }
                    return Err(selector_exhausted_error());
                }
                // Source `leaderOnly` still applies after
                // `ReplicaSelectLeaderStrategy` exhausts the leader. Mixed
                // fallback has no eligible candidate in that mode, so return
                // to the sender/cache refresh path rather than resending the
                // known exhausted leader.
                if !selector_state.has_deadline_exceeded() {
                    self.region_cache
                        .invalidate_region_cache(region.ver_id())
                        .await;
                }
                return Err(selector_exhausted_error());
            }
            if is_read_request && busy_threshold_ms != 0 {
                if let Some(leader) = region.leader.as_ref() {
                    self.region_cache.get_store_by_id(leader.store_id).await?;
                    let busy_threshold = Duration::from_millis(u64::from(busy_threshold_ms));
                    if selector_state.is_server_busy(leader.id)
                        || self.region_cache.estimated_store_wait(leader.store_id)
                            > Some(busy_threshold)
                    {
                        if let Some(follower) = self
                            .region_cache
                            .select_idle_replica(
                                &region,
                                &config.labels,
                                &config.stores,
                                &selector_state,
                                busy_threshold,
                            )
                            .await?
                        {
                            return self
                                .map_region_to_route(region, follower, None)
                                .await
                                .map(|route| route.with_busy_threshold(busy_threshold_ms));
                        }
                        return self
                            .map_leader_route(region, &selector_state)
                            .await
                            .map(|route| {
                                route.with_busy_threshold(0).with_busy_threshold_disabled()
                            });
                    }
                }
            }
            if !config.leader_only {
                if let Some(leader) = region
                    .leader
                    .clone()
                    .filter(|leader| selector_state.should_probe_busy_leader(leader.id))
                {
                    if let Some(follower) = self
                        .region_cache
                        .select_mixed_replica(
                            &region,
                            &config.labels,
                            &config.stores,
                            &selector_state,
                            MixedReplicaSelection {
                                read_type: ReplicaReadType::Follower,
                                leader_only: false,
                                prefer_leader: false,
                                labels_requested: !config.labels.is_empty(),
                            },
                        )
                        .await?
                        .filter(|peer| peer.id != leader.id)
                    {
                        return Ok(self
                            .map_region_to_route(region, follower, None)
                            .await?
                            .with_force_leader_read()
                            .with_busy_threshold(busy_threshold_ms));
                    }
                }
            }
            return self
                .map_leader_route(region, &selector_state)
                .await
                .map(|route| route.with_busy_threshold(busy_threshold_ms));
        }
        let read_type = if config.stale_read {
            ReplicaReadType::Mixed
        } else {
            config.read_type
        };
        // client-go's second stale-read attempt probes an untried leader with
        // an ordinary leader read before returning to mixed selection.
        if config.stale_read {
            if let Some(leader) = region
                .leader
                .clone()
                .filter(|leader| selector_state.should_probe_stale_leader(leader.id))
            {
                return self
                    .map_region_to_route(region, leader, None)
                    .await
                    .map(|route| route.with_busy_threshold(busy_threshold_ms));
            }
        }
        let peer = self
            .region_cache
            .select_mixed_replica(
                &region,
                &config.labels,
                &config.stores,
                &selector_state,
                MixedReplicaSelection {
                    read_type,
                    leader_only: config.leader_only,
                    prefer_leader: config.effective_prefer_leader(),
                    labels_requested: !config.labels.is_empty(),
                },
            )
            .await?
            .ok_or_else(selector_exhausted_error);
        let peer = match peer {
            Ok(peer) => peer,
            Err(error) => {
                if !selector_state.has_deadline_exceeded() {
                    self.region_cache
                        .invalidate_region_cache(region.ver_id())
                        .await;
                }
                return Err(error);
            }
        };
        let stale_read = config.stale_read
            && !region
                .leader
                .as_ref()
                .is_some_and(|leader| selector_state.should_retry_stale_as_replica(leader.id));
        if config.effective_prefer_leader() {
            let destination = if region
                .leader
                .as_ref()
                .is_some_and(|leader| leader.id == peer.id)
            {
                ReplicaFlowsType::ToLeader
            } else {
                ReplicaFlowsType::ToFollower
            };
            self.region_cache
                .record_store_replica_flow(peer.store_id, destination);
        }
        Ok(self
            .map_region_to_route(region, peer, None)
            .await?
            .with_stale_read(stale_read)
            .with_busy_threshold(busy_threshold_ms)
            .with_prefer_leader_slow_score(matches!(
                config.read_type,
                ReplicaReadType::PreferLeader
            )))
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        self.region_cache.get_region_by_key(key).await
    }

    async fn region_for_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        self.region_cache.get_region_by_end_key(key).await
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        self.region_cache.get_region_by_id(id).await
    }

    async fn batch_load_regions_from_key(
        &self,
        key: &Key,
        count: usize,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        self.region_cache
            .batch_load_regions_from_key(key.clone(), count, backoffer)
            .await
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        let pb_stores = self.region_cache.read_through_all_stores().await?;
        let mut stores = Vec::with_capacity(pb_stores.len());
        for store in pb_stores {
            let client = self.kv_client(&store.address).await?;
            client.set_event_listener(self.region_cache.client_event_listener());
            let mut target = Store::new(Arc::new(client))
                .with_target(store.address.clone())
                .with_metadata(&store);
            if target.endpoint_type == crate::store::EndpointType::TiFlash
                && !target.peer_address.is_empty()
                && target.peer_address != target.target
            {
                let safe_ts_client = self.kv_client(&target.peer_address).await?;
                safe_ts_client.set_event_listener(self.region_cache.client_event_listener());
                target = target.with_safe_ts_client(Arc::new(safe_ts_client));
            }
            stores.push(target);
        }
        Ok(stores)
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        self.pd.clone().get_timestamp().await
    }

    async fn cluster_id(&self) -> u64 {
        self.pd.cluster_id().await
    }

    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        self.pd.clone().get_min_timestamp().await
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        self.pd.clone().set_external_timestamp(timestamp).await
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        self.pd.clone().get_external_timestamp().await
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
        self.pd.clone().update_safepoint(safepoint).await
    }

    async fn update_safepoint_value(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        self.pd.clone().update_safepoint_value(safepoint).await
    }

    async fn get_gc_state(self: Arc<Self>) -> Result<pdpb::GcState> {
        self.pd
            .clone()
            .get_gc_state(self.gc_keyspace_id()?)
            .await?
            .gc_state
            .ok_or_else(|| {
                crate::Error::StringError("PD GetGCState response has no state".to_owned())
            })
    }

    async fn advance_transaction_safe_point(
        self: Arc<Self>,
        target: u64,
    ) -> Result<pdpb::AdvanceTxnSafePointResponse> {
        self.pd
            .clone()
            .advance_txn_safe_point(self.gc_keyspace_id()?, target)
            .await
    }

    async fn advance_gc_safe_point(
        self: Arc<Self>,
        target: u64,
    ) -> Result<pdpb::AdvanceGcSafePointResponse> {
        self.pd
            .clone()
            .advance_gc_safe_point(self.gc_keyspace_id()?, target)
            .await
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<u64>,
        group: Option<String>,
    ) -> Result<pdpb::ScatterRegionResponse> {
        self.pd
            .clone()
            .scatter_regions(region_ids, group.unwrap_or_default())
            .await
    }

    async fn get_operator(self: Arc<Self>, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        self.pd.clone().get_operator(region_id).await
    }

    async fn update_leader(&self, ver_id: RegionVerId, leader: metapb::Peer) -> Result<()> {
        self.region_cache.update_leader(ver_id, leader).await
    }

    async fn update_region_cache(&self, regions: Vec<RegionWithLeader>) -> Result<()> {
        for region in regions {
            self.region_cache.add_region(region).await;
        }
        Ok(())
    }

    async fn update_buckets(&self, ver_id: RegionVerId, version: u64, keys: Vec<Vec<u8>>) {
        self.region_cache
            .update_buckets(ver_id, version, keys)
            .await;
    }

    fn record_server_load(&self, store_id: StoreId, estimated_wait_ms: u32) {
        self.region_cache
            .record_server_load(store_id, estimated_wait_ms);
    }

    async fn record_forwarding_proxy(&self, ver_id: RegionVerId, store_id: StoreId) {
        self.region_cache
            .set_region_proxy_store(&ver_id, Some(store_id))
            .await;
    }

    async fn invalidate_region_cache(&self, ver_id: RegionVerId) {
        self.region_cache.invalidate_region_cache(ver_id).await
    }

    async fn invalidate_store_cache(&self, store_id: StoreId) {
        let store = self.region_cache.invalidate_store_cache(store_id).await;
        if let Some(store) = store {
            self.close_cached_kv_client_addr_ver(&store.address, u64::MAX)
                .await;
        }
    }

    async fn close_kv_client_addr_ver(&self, address: &str, version: u64) {
        self.close_cached_kv_client_addr_ver(address, version).await;
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.pd.load_keyspace(keyspace).await
    }
}

impl PdRpcClient<TikvConnect, Cluster> {
    pub async fn connect(
        pd_endpoints: &[String],
        config: Config,
        enable_codec: bool,
    ) -> Result<PdRpcClient> {
        let mode = if enable_codec {
            KeyMode::Txn
        } else {
            KeyMode::Raw
        };
        Self::connect_with_codec_config(pd_endpoints, config, PdCodecConfig::V1(mode)).await
    }

    pub(crate) async fn connect_with_keyspace(
        pd_endpoints: &[String],
        config: Config,
        mode: KeyMode,
        keyspace_name: String,
    ) -> Result<PdRpcClient> {
        Self::connect_with_codec_config(
            pd_endpoints,
            config,
            PdCodecConfig::V2 {
                mode,
                keyspace_name,
            },
        )
        .await
    }

    async fn connect_with_codec_config(
        pd_endpoints: &[String],
        config: Config,
        codec_config: PdCodecConfig,
    ) -> Result<PdRpcClient> {
        let enable_preload = config.enable_preload;
        let regions_refresh_interval = config.regions_refresh_interval;
        let stores_refresh_interval = config.stores_refresh_interval;
        let client = PdRpcClient::new_with_codec_resolver(
            config.clone(),
            |security_mgr| {
                TikvConnect::new_with_grpc_compression(
                    security_mgr,
                    config.timeout,
                    config.grpc_max_decoding_message_size,
                    &config.tikv_client.grpc_compression_type,
                    Duration::from_secs(config.tikv_client.grpc_keep_alive_time),
                    Duration::from_secs_f64(config.tikv_client.grpc_keep_alive_timeout),
                    u32::try_from(config.tikv_client.grpc_initial_window_size)
                        .ok()
                        .filter(|size| *size > 0),
                    u32::try_from(config.tikv_client.grpc_initial_conn_window_size)
                        .ok()
                        .filter(|size| *size > 0),
                    config.tikv_client.grpc_connection_count as usize,
                )
                .with_open_tracing(config.open_tracing_enable)
                .with_tikv_client_config(config.tikv_client.clone())
            },
            |security_mgr| RetryClient::connect(pd_endpoints, security_mgr, config.timeout),
            move |pd| async move {
                match codec_config {
                    PdCodecConfig::V1(mode) => Ok((PdRegionCodec::v1(mode), None)),
                    PdCodecConfig::V2 {
                        mode,
                        keyspace_name,
                    } => {
                        let canonical_name = build_keyspace_name(keyspace_name);
                        let meta = pd.load_keyspace(&canonical_name).await?;
                        let keyspace_id = match keyspace_from_pd_meta(&meta)? {
                            Keyspace::Enable { keyspace_id } => keyspace_id,
                            _ => {
                                unreachable!("PD metadata always constructs a numeric V2 keyspace")
                            }
                        };
                        Ok((PdRegionCodec::v2(mode, keyspace_id)?, Some(meta)))
                    }
                }
            },
        )
        .await?;
        if enable_preload {
            let mut backoffer =
                RetryBackoffer::new(crate::async_util::Cancellation::default(), 20_000);
            if let Err(error) = client
                .region_cache
                .refresh_region_index(&mut backoffer)
                .await
            {
                log::debug!("preload region index failed: {error}");
            }
        }
        if regions_refresh_interval > 0 {
            client
                .region_cache
                .start_background_refresh(Duration::from_secs(regions_refresh_interval));
        } else {
            client.region_cache.start_background_gc();
        }
        let health_feedback_requester = Arc::new(StoreHealthFeedbackRequester {
            kv_connect: client.kv_connect.clone(),
            kv_client_cache: client.kv_client_cache.clone(),
            kv_client_versions: client.kv_client_versions.clone(),
            kv_client_lifecycle: client.kv_client_lifecycle.clone(),
            kv_client_closed: client.kv_client_closed.clone(),
            event_listener: client.region_cache.client_event_listener(),
        });
        client
            .region_cache
            .set_health_feedback_callback(Arc::new(move |address| {
                let requester = health_feedback_requester.clone();
                Box::pin(async move { requester.request(&address).await })
            }));
        client
            .region_cache
            .start_background_store_maintenance(Duration::from_secs(stores_refresh_interval));
        Ok(client)
    }
}

#[derive(Clone, Debug)]
enum PdCodecConfig {
    V1(KeyMode),
    V2 {
        mode: KeyMode,
        keyspace_name: String,
    },
}

impl<KvC: KvConnect + Send + Sync + 'static, Cl> PdRpcClient<KvC, Cl> {
    pub(crate) async fn advance_transaction_safe_point_for_keyspace(
        &self,
        keyspace_id: u32,
        target: u64,
    ) -> Result<pdpb::AdvanceTxnSafePointResponse>
    where
        Cl: Send + Sync + 'static,
        RetryClient<Cl>: RetryClientTrait,
    {
        self.pd
            .clone()
            .advance_txn_safe_point(keyspace_id, target)
            .await
    }

    fn gc_keyspace_id(&self) -> Result<u32> {
        match self.keyspace_meta.as_ref() {
            Some(meta) => match keyspace_from_pd_meta(meta)? {
                Keyspace::Enable { keyspace_id } => Ok(keyspace_id),
                _ => Ok(crate::request::NULL_KEYSPACE_ID),
            },
            None => Ok(crate::request::NULL_KEYSPACE_ID),
        }
    }

    async fn close_cached_kv_client_addr_ver(&self, address: &str, version: u64) {
        let _lifecycle = self.kv_client_lifecycle.lock().await;
        let retired = {
            let mut cache = self.kv_client_cache.write().await;
            match cache.get(address) {
                Some(cached) if cached.version <= version => cache.remove(address),
                _ => None,
            }
        };
        if let Some(retired) = retired {
            retired.client.close();
        }
    }

    pub async fn new<PdFut, MakeKvC, MakePd>(
        config: Config,
        kv_connect: MakeKvC,
        pd: MakePd,
        enable_codec: bool,
    ) -> Result<PdRpcClient<KvC, Cl>>
    where
        Cl: Send + Sync + 'static,
        PdFut: Future<Output = Result<RetryClient<Cl>>>,
        MakeKvC: FnOnce(Arc<SecurityManager>) -> KvC,
        MakePd: FnOnce(Arc<SecurityManager>) -> PdFut,
    {
        let mode = if enable_codec {
            KeyMode::Txn
        } else {
            KeyMode::Raw
        };
        let client = Self::new_with_codec_resolver(config, kv_connect, pd, move |_| async move {
            Ok((PdRegionCodec::v1(mode), None))
        })
        .await?;
        client.region_cache.start_background_gc();
        Ok(client)
    }

    async fn new_with_codec_resolver<PdFut, CodecFut, MakeKvC, MakePd, ResolveCodec>(
        config: Config,
        kv_connect: MakeKvC,
        pd: MakePd,
        resolve_codec: ResolveCodec,
    ) -> Result<PdRpcClient<KvC, Cl>>
    where
        Cl: Send + Sync + 'static,
        PdFut: Future<Output = Result<RetryClient<Cl>>>,
        CodecFut: Future<Output = Result<(PdRegionCodec, Option<keyspacepb::KeyspaceMeta>)>>,
        MakeKvC: FnOnce(Arc<SecurityManager>) -> KvC,
        MakePd: FnOnce(Arc<SecurityManager>) -> PdFut,
        ResolveCodec: FnOnce(Arc<RetryClient<Cl>>) -> CodecFut,
    {
        let security_mgr = Arc::new(
            config
                .security_manager()
                .map_err(|error| crate::Error::StringError(error.to_string()))?,
        );
        let store_liveness_timeout =
            parse_source_duration(&config.tikv_client.store_liveness_timeout).ok_or_else(|| {
                crate::Error::StringError(format!(
                    "invalid store-liveness-timeout: {}",
                    config.tikv_client.store_liveness_timeout
                ))
            })?;

        let pd = Arc::new(pd(security_mgr.clone()).await?);
        let (region_codec, keyspace_meta) = resolve_codec(pd.clone()).await?;
        let kv_client_cache = Default::default();
        let kv_client_versions = Default::default();
        let kv_client_lifecycle = Default::default();
        let kv_client_closed = Default::default();
        let store_token_counts = Default::default();
        crate::kv::STORE_LIMIT.store(config.tikv_client.store_limit, Ordering::Relaxed);
        let codec_pd = Arc::new(CodecPdClient::new(pd.clone(), region_codec));
        let region_cache = Arc::new(RegionCache::new(codec_pd));
        let client = PdRpcClient {
            pd: pd.clone(),
            kv_client_cache,
            kv_client_versions,
            kv_client_lifecycle,
            kv_client_closed,
            store_token_counts,
            kv_connect: kv_connect(security_mgr.clone()),
            keyspace_meta,
            enable_forwarding: config.enable_forwarding,
            zone_label: config.zone_label,
            security_mgr,
            store_liveness_timeout,
            region_cache,
        };
        Ok(client)
    }

    pub(crate) fn keyspace_meta(&self) -> Option<&keyspacepb::KeyspaceMeta> {
        self.keyspace_meta.as_ref()
    }

    async fn kv_client(&self, address: &str) -> Result<KvC::KvClient> {
        if self.kv_client_closed.load(Ordering::Acquire) {
            return Err(crate::Error::StringError("rpc client is closed".to_owned()));
        }
        if let Some(cached) = self.kv_client_cache.read().await.get(address) {
            return Ok(cached.client.clone());
        }
        let _lifecycle = self.kv_client_lifecycle.lock().await;
        if self.kv_client_closed.load(Ordering::Acquire) {
            return Err(crate::Error::StringError("rpc client is closed".to_owned()));
        }
        if let Some(cached) = self.kv_client_cache.read().await.get(address) {
            return Ok(cached.client.clone());
        }
        info!("connect to tikv endpoint: {:?}", address);
        let client = self.kv_connect.connect(address).await?;
        let mut cache = self.kv_client_cache.write().await;
        let version = {
            let mut versions = self.kv_client_versions.write().await;
            let version = versions.get(address).copied().unwrap_or(0).wrapping_add(1);
            versions.insert(address.to_owned(), version);
            version
        };
        let client = client.with_connection_info(address.to_owned(), version);
        cache.insert(
            address.to_owned(),
            CachedKvClient {
                client: client.clone(),
                version,
            },
        );
        Ok(client)
    }

    async fn invalidate_kv_client_cache(&self, address: &str) {
        self.close_cached_kv_client_addr_ver(address, u64::MAX)
            .await;
    }

    /// Retires all pooled TiKV clients and prevents future connections. This
    /// is the owning counterpart of client-go `RPCClient.Close`.
    pub async fn close(&self)
    where
        Cl: Send + Sync + 'static,
    {
        if self.kv_client_closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let _lifecycle = self.kv_client_lifecycle.lock().await;
        let retired = self
            .kv_client_cache
            .write()
            .await
            .drain()
            .map(|(_, cached)| cached.client)
            .collect::<Vec<_>>();
        for client in retired {
            client.close();
        }
        self.region_cache.close_background_task().await;
    }
}

/// `ReplicaSelectLeaderStrategy` gives a healthy cached leader one send. Once
/// that replica is exhausted, `nextForReplicaReadLeader` falls through to the
/// mixed strategy and probes a follower with leader-read context. A hintless
/// NotLeader reaches the same fallback after its scheduling backoff.
fn source_leader_falls_back_to_follower(
    selector_state: &ReplicaSelectorState,
    leader_peer_id: u64,
) -> bool {
    !selector_state.is_leader_selectable(leader_peer_id)
}

fn source_leader_needs_mixed_fallback(
    forwarding_enabled: bool,
    leader_epoch_stale: bool,
    leader_liveness: Option<StoreLiveness>,
    selector_state: &ReplicaSelectorState,
    leader_peer_id: u64,
) -> bool {
    leader_epoch_stale
        || (!forwarding_enabled && leader_liveness != Some(StoreLiveness::Reachable))
        || source_leader_falls_back_to_follower(selector_state, leader_peer_id)
}

fn source_can_restore_suspect_leader(
    selector_state: &ReplicaSelectorState,
    leader_peer_id: u64,
    leader_epoch_stale: bool,
    leader_liveness: Option<StoreLiveness>,
) -> bool {
    selector_state.should_probe_busy_leader(leader_peer_id)
        && selector_state.is_leader_candidate(leader_peer_id)
        && !leader_epoch_stale
        && leader_liveness == Some(StoreLiveness::Reachable)
}

fn source_forwarding_exhausted(
    forwarding_enabled: bool,
    leader_liveness: Option<StoreLiveness>,
    selector_state: &ReplicaSelectorState,
    leader_peer_id: u64,
    has_proxy: bool,
) -> bool {
    forwarding_enabled
        && leader_liveness != Some(StoreLiveness::Reachable)
        && !selector_state.has_no_leader(leader_peer_id)
        && !has_proxy
}

fn source_leader_fallback_uses_replica_read(
    selector_state: &ReplicaSelectorState,
    leader_peer_id: u64,
) -> bool {
    selector_state.deadline_exceeded(leader_peer_id)
}

fn selector_exhausted_error() -> crate::Error {
    crate::Error::RegionError(Box::new(errorpb::Error {
        epoch_not_match: Some(errorpb::EpochNotMatch::default()),
        ..Default::default()
    }))
}

fn make_key_range(start_key: Vec<u8>, end_key: Vec<u8>) -> kvrpcpb::KeyRange {
    let mut key_range = kvrpcpb::KeyRange::default();
    key_range.start_key = start_key;
    key_range.end_key = end_key;
    key_range
}

#[cfg(test)]
pub mod test {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::executor;
    use futures::executor::block_on;

    use super::*;
    use crate::mock::*;
    use crate::pd::RetryClient;
    use crate::store::{KvClient, KvConnect, Request};
    use crate::Config;

    #[test]
    fn source_exhausted_or_hintless_leader_falls_back_to_a_follower_probe() {
        let mut selector_state = ReplicaSelectorState::default();
        assert!(!source_leader_falls_back_to_follower(&selector_state, 11));

        for _ in 0..9 {
            selector_state.record_attempt(11);
        }
        assert!(!source_leader_falls_back_to_follower(&selector_state, 11));
        selector_state.record_attempt(11);
        assert!(source_leader_falls_back_to_follower(&selector_state, 11));

        let mut hintless = ReplicaSelectorState::default();
        hintless.mark_no_leader(11);
        assert!(source_leader_falls_back_to_follower(&hintless, 11));
        assert!(!source_leader_falls_back_to_follower(&hintless, 12));

        let mut deadline = ReplicaSelectorState::default();
        deadline.record_attempt(11);
        deadline.mark_deadline_exceeded(11);
        assert!(source_leader_falls_back_to_follower(&deadline, 11));
        assert!(source_leader_fallback_uses_replica_read(&deadline, 11));
        assert!(!source_leader_fallback_uses_replica_read(&hintless, 11));
    }

    #[test]
    fn source_unavailable_leader_uses_mixed_selection_without_forwarding() {
        let state = ReplicaSelectorState::default();
        assert!(!source_leader_needs_mixed_fallback(
            false,
            false,
            Some(StoreLiveness::Reachable),
            &state,
            11,
        ));
        for liveness in [
            None,
            Some(StoreLiveness::Unknown),
            Some(StoreLiveness::Unreachable),
        ] {
            assert!(source_leader_needs_mixed_fallback(
                false, false, liveness, &state, 11,
            ));
            assert!(!source_leader_needs_mixed_fallback(
                true, false, liveness, &state, 11,
            ));
        }
        assert!(source_leader_needs_mixed_fallback(
            true,
            true,
            Some(StoreLiveness::Reachable),
            &state,
            11,
        ));
    }

    #[test]
    fn source_busy_probe_restores_only_an_eligible_reachable_leader() {
        let mut selector_state = ReplicaSelectorState::default();
        selector_state.record_attempt(11);
        selector_state.record_attempt(11);
        selector_state.record_busy_leader(11);
        selector_state.record_busy_leader(11);

        assert!(source_leader_falls_back_to_follower(&selector_state, 11));
        assert!(source_can_restore_suspect_leader(
            &selector_state,
            11,
            false,
            Some(StoreLiveness::Reachable),
        ));
        assert!(!source_can_restore_suspect_leader(
            &selector_state,
            11,
            true,
            Some(StoreLiveness::Reachable),
        ));
        assert!(!source_can_restore_suspect_leader(
            &selector_state,
            11,
            false,
            Some(StoreLiveness::Unreachable),
        ));
        assert!(!source_can_restore_suspect_leader(
            &selector_state,
            12,
            false,
            Some(StoreLiveness::Reachable),
        ));

        selector_state.mark_deadline_exceeded(11);
        assert!(!source_can_restore_suspect_leader(
            &selector_state,
            11,
            false,
            Some(StoreLiveness::Reachable),
        ));
    }

    #[test]
    fn source_forwarding_exhausts_only_after_every_unavailable_leader_proxy() {
        let clean = ReplicaSelectorState::default();
        for liveness in [
            None,
            Some(StoreLiveness::Unknown),
            Some(StoreLiveness::Unreachable),
        ] {
            assert!(source_forwarding_exhausted(
                true, liveness, &clean, 11, false,
            ));
            assert!(!source_forwarding_exhausted(
                true, liveness, &clean, 11, true,
            ));
        }
        assert!(!source_forwarding_exhausted(
            true,
            Some(StoreLiveness::Reachable),
            &clean,
            11,
            false,
        ));
        assert!(!source_forwarding_exhausted(
            false,
            Some(StoreLiveness::Unreachable),
            &clean,
            11,
            false,
        ));

        let mut hintless = ReplicaSelectorState::default();
        hintless.mark_no_leader(11);
        assert!(!source_forwarding_exhausted(
            true,
            Some(StoreLiveness::Unreachable),
            &hintless,
            11,
            false,
        ));
    }

    #[test]
    fn source_health_check_status_and_duration_mapping() {
        assert_eq!(
            source_health_status_liveness(HEALTH_SERVING),
            StoreLiveness::Reachable
        );
        assert_eq!(
            source_health_status_liveness(HEALTH_UNKNOWN),
            StoreLiveness::Unknown
        );
        assert_eq!(
            source_health_status_liveness(HEALTH_SERVICE_UNKNOWN),
            StoreLiveness::Unknown
        );
        assert_eq!(source_health_status_liveness(2), StoreLiveness::Unreachable);
        assert_eq!(
            parse_source_duration("1.5s"),
            Some(Duration::from_millis(1500))
        );
        assert_eq!(
            parse_source_duration("250ms"),
            Some(Duration::from_millis(250))
        );
        assert_eq!(parse_source_duration("0s"), Some(Duration::ZERO));
        assert_eq!(parse_source_duration("bad"), None);
    }

    #[tokio::test]
    async fn source_store_liveness_requests_are_singleflight_by_address() {
        let probes = Arc::new(AtomicUsize::new(0));
        let first_probes = probes.clone();
        let second_probes = probes.clone();
        let target = "singleflight-store-address".to_owned();
        let first = request_store_liveness_singleflight(target.clone(), move || async move {
            first_probes.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            StoreLiveness::Reachable
        });
        let second = request_store_liveness_singleflight(target, move || async move {
            second_probes.fetch_add(1, Ordering::SeqCst);
            StoreLiveness::Unreachable
        });
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first, StoreLiveness::Reachable);
        assert_eq!(second, StoreLiveness::Reachable);
        assert_eq!(probes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_get_keyspace_id_loads_canonical_name_and_rejects_v3_identity() {
        let client = MockPdClient::default();
        client.set_keyspace_meta(keyspacepb::KeyspaceMeta {
            state: keyspacepb::KeyspaceState::Enabled as i32,
            keyspace: Some(keyspacepb::keyspace_meta::Keyspace::KeyspaceIdentity(
                crate::proto::apipb::KeyspaceIdentity {
                    namespace_id: 1,
                    keyspace_id: 2,
                },
            )),
            ..Default::default()
        });

        assert_eq!(
            client.get_keyspace_id("").await.unwrap_err().to_string(),
            "keyspace  uses an API V3 keyspace identity, which is not supported"
        );
        assert_eq!(client.loaded_keyspaces(), ["DEFAULT"]);
    }

    #[tokio::test]
    async fn test_kv_client_caching() {
        let client = block_on(pd_rpc_client());

        let kv1 = client.kv_client("foo").await.unwrap();
        let kv2 = client.kv_client("bar").await.unwrap();
        let kv3 = client.kv_client("bar").await.unwrap();
        assert!(kv1.addr != kv2.addr);
        assert_eq!(kv2.addr, kv3.addr);
    }

    #[tokio::test]
    async fn test_kv_client_cache_hits_lazily() {
        #[derive(Clone)]
        struct CountingConnect {
            connects: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl KvConnect for CountingConnect {
            type KvClient = MockKvClient;

            async fn connect(&self, address: &str) -> Result<Self::KvClient> {
                self.connects.fetch_add(1, Ordering::SeqCst);
                let mut client = MockKvClient::default();
                client.addr = address.to_owned();
                Ok(client)
            }
        }

        let connects = Arc::new(AtomicUsize::new(0));
        let connects_clone = connects.clone();
        let client = PdRpcClient::new(
            Config::default(),
            move |_| CountingConnect {
                connects: connects_clone.clone(),
            },
            |sm| async move {
                Ok(RetryClient::new_with_cluster(
                    sm,
                    Config::default().timeout,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();

        let kv1 = client.kv_client("foo").await.unwrap();
        let kv2 = client.kv_client("foo").await.unwrap();
        assert_eq!(kv1.addr, "foo");
        assert_eq!(kv2.addr, "foo");
        assert_eq!(connects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_kv_client_cache_reconnects_after_invalidation() {
        #[derive(Clone)]
        struct CountingConnect {
            connects: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl KvConnect for CountingConnect {
            type KvClient = MockKvClient;

            async fn connect(&self, address: &str) -> Result<Self::KvClient> {
                self.connects.fetch_add(1, Ordering::SeqCst);
                let mut client = MockKvClient::default();
                client.addr = address.to_owned();
                Ok(client)
            }
        }

        let connects = Arc::new(AtomicUsize::new(0));
        let connects_clone = connects.clone();
        let client = PdRpcClient::new(
            Config::default(),
            move |_| CountingConnect {
                connects: connects_clone.clone(),
            },
            |sm| async move {
                Ok(RetryClient::new_with_cluster(
                    sm,
                    Config::default().timeout,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();

        let kv1 = client.kv_client("foo").await.unwrap();
        client.invalidate_kv_client_cache("foo").await;
        let kv2 = client.kv_client("foo").await.unwrap();
        assert_eq!(kv1.addr, "foo");
        assert_eq!(kv2.addr, "foo");
        assert_eq!(connects.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn source_close_addr_ver_does_not_evict_a_newer_cached_client() {
        #[derive(Clone)]
        struct CountingConnect {
            connects: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl KvConnect for CountingConnect {
            type KvClient = MockKvClient;

            async fn connect(&self, address: &str) -> Result<Self::KvClient> {
                self.connects.fetch_add(1, Ordering::SeqCst);
                let mut client = MockKvClient::default();
                client.addr = address.to_owned();
                Ok(client)
            }
        }

        let connects = Arc::new(AtomicUsize::new(0));
        let connects_clone = connects.clone();
        let client = PdRpcClient::new(
            Config::default(),
            move |_| CountingConnect {
                connects: connects_clone.clone(),
            },
            |sm| async move {
                Ok(RetryClient::new_with_cluster(
                    sm,
                    Config::default().timeout,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();

        client.kv_client("foo").await.unwrap();
        let first_version = client.kv_client_cache.read().await["foo"].version;
        client
            .close_cached_kv_client_addr_ver("foo", first_version)
            .await;
        client.kv_client("foo").await.unwrap();
        let second_version = client.kv_client_cache.read().await["foo"].version;

        assert_eq!(second_version, first_version.wrapping_add(1));
        client
            .close_cached_kv_client_addr_ver("foo", first_version)
            .await;
        assert_eq!(
            client.kv_client_cache.read().await["foo"].version,
            second_version
        );
        assert_eq!(connects.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn source_pool_creation_is_singleflight_per_client() {
        #[derive(Clone)]
        struct DelayedConnect {
            connects: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl KvConnect for DelayedConnect {
            type KvClient = MockKvClient;

            async fn connect(&self, address: &str) -> Result<Self::KvClient> {
                self.connects.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                let mut client = MockKvClient::default();
                client.addr = address.to_owned();
                Ok(client)
            }
        }

        let connects = Arc::new(AtomicUsize::new(0));
        let connects_clone = connects.clone();
        let client = Arc::new(
            PdRpcClient::new(
                Config::default(),
                move |_| DelayedConnect {
                    connects: connects_clone.clone(),
                },
                |sm| async move {
                    Ok(RetryClient::new_with_cluster(
                        sm,
                        Config::default().timeout,
                        MockCluster,
                    ))
                },
                false,
            )
            .await
            .unwrap(),
        );

        let (first, second) = tokio::join!(client.kv_client("foo"), client.kv_client("foo"));
        assert_eq!(first.unwrap().addr, "foo");
        assert_eq!(second.unwrap().addr, "foo");
        assert_eq!(connects.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn source_client_close_retires_every_pool_once_and_prevents_reconnect() {
        #[derive(Clone)]
        struct ClosingClient {
            closes: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl KvClient for ClosingClient {
            async fn dispatch(&self, _request: &dyn Request) -> Result<Box<dyn std::any::Any>> {
                unreachable!("this lifecycle test never dispatches")
            }

            fn close(&self) {
                self.closes.fetch_add(1, Ordering::SeqCst);
            }
        }

        #[derive(Clone)]
        struct ClosingConnect {
            connects: Arc<AtomicUsize>,
            closes: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl KvConnect for ClosingConnect {
            type KvClient = ClosingClient;

            async fn connect(&self, _address: &str) -> Result<Self::KvClient> {
                self.connects.fetch_add(1, Ordering::SeqCst);
                Ok(ClosingClient {
                    closes: self.closes.clone(),
                })
            }
        }

        let connects = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let connects_clone = connects.clone();
        let closes_clone = closes.clone();
        let client = PdRpcClient::new(
            Config::default(),
            move |_| ClosingConnect {
                connects: connects_clone.clone(),
                closes: closes_clone.clone(),
            },
            |sm| async move {
                Ok(RetryClient::new_with_cluster(
                    sm,
                    Config::default().timeout,
                    MockCluster,
                ))
            },
            false,
        )
        .await
        .unwrap();

        client.kv_client("store-a").await.unwrap();
        client.kv_client("store-b").await.unwrap();
        let store_a_version = client.kv_client_cache.read().await["store-a"].version;
        tokio::join!(
            client.close(),
            client.close_cached_kv_client_addr_ver("store-a", store_a_version)
        );
        client.close().await;

        assert_eq!(connects.load(Ordering::SeqCst), 2);
        assert_eq!(closes.load(Ordering::SeqCst), 2);
        assert!(matches!(
            client.kv_client("store-a").await,
            Err(crate::Error::StringError(message)) if message == "rpc client is closed"
        ));
    }

    #[test]
    fn test_group_keys_by_region() {
        let client = MockPdClient::default();

        // FIXME This only works if the keys are in order of regions. Not sure if
        // that is a reasonable constraint.
        let tasks: Vec<Key> = vec![
            vec![1].into(),
            vec![2].into(),
            vec![3].into(),
            vec![5, 2].into(),
            vec![12].into(),
            vec![11, 4].into(),
        ];

        let stream = Arc::new(client).group_keys_by_region(tasks.into_iter());
        let mut stream = executor::block_on_stream(stream);

        let result: Vec<Key> = stream.next().unwrap().unwrap().0;
        assert_eq!(
            result,
            vec![
                vec![1].into(),
                vec![2].into(),
                vec![3].into(),
                vec![5, 2].into()
            ]
        );
        assert_eq!(
            stream.next().unwrap().unwrap().0,
            vec![vec![12].into(), vec![11, 4].into()]
        );
        assert!(stream.next().is_none());
    }

    #[test]
    fn test_regions_for_range() {
        let client = Arc::new(MockPdClient::default());
        let k1: Key = vec![1].into();
        let k2: Key = vec![5, 2].into();
        let k3: Key = vec![11, 4].into();
        let range1 = (k1, k2.clone()).into();
        let mut stream = executor::block_on_stream(client.clone().regions_for_range(range1));
        assert_eq!(stream.next().unwrap().unwrap().id(), 1);
        assert!(stream.next().is_none());

        let range2 = (k2, k3).into();
        let mut stream = executor::block_on_stream(client.regions_for_range(range2));
        assert_eq!(stream.next().unwrap().unwrap().id(), 1);
        assert_eq!(stream.next().unwrap().unwrap().id(), 2);
        assert!(stream.next().is_none());
    }

    #[test]
    fn test_group_ranges_by_region() {
        let client = Arc::new(MockPdClient::default());
        let k1 = vec![1];
        let k2 = vec![5, 2];
        let k3 = vec![11, 4];
        let k4 = vec![16, 4];
        let k5 = vec![250, 251];
        let k6 = vec![255, 251];
        let k_split = vec![10];
        let range1 = make_key_range(k1.clone(), k2.clone());
        let range2 = make_key_range(k1.clone(), k3.clone());
        let range3 = make_key_range(k2.clone(), k4.clone());
        let ranges = vec![range1, range2, range3];

        let mut stream = executor::block_on_stream(client.clone().group_ranges_by_region(ranges));
        let ranges1 = stream.next().unwrap().unwrap();
        let ranges2 = stream.next().unwrap().unwrap();
        let ranges3 = stream.next().unwrap().unwrap();
        let ranges4 = stream.next().unwrap().unwrap();

        assert_eq!(ranges1.1.id(), 1);
        assert_eq!(
            ranges1.0,
            vec![
                make_key_range(k1.clone(), k2.clone()),
                make_key_range(k1.clone(), k_split.clone()),
            ]
        );
        assert_eq!(ranges2.1.id(), 2);
        assert_eq!(ranges2.0, vec![make_key_range(k_split.clone(), k3.clone())]);
        assert_eq!(ranges3.1.id(), 1);
        assert_eq!(ranges3.0, vec![make_key_range(k2.clone(), k_split.clone())]);
        assert_eq!(ranges4.1.id(), 2);
        assert_eq!(ranges4.0, vec![make_key_range(k_split, k4.clone())]);
        assert!(stream.next().is_none());

        let range1 = make_key_range(k1.clone(), k2.clone());
        let range2 = make_key_range(k3.clone(), k4.clone());
        let range3 = make_key_range(k5.clone(), k6.clone());
        let ranges = vec![range1, range2, range3];
        stream = executor::block_on_stream(client.group_ranges_by_region(ranges));
        let ranges1 = stream.next().unwrap().unwrap();
        let ranges2 = stream.next().unwrap().unwrap();
        let ranges3 = stream.next().unwrap().unwrap();
        assert_eq!(ranges1.1.id(), 1);
        assert_eq!(ranges1.0, vec![make_key_range(k1, k2)]);
        assert_eq!(ranges2.1.id(), 2);
        assert_eq!(ranges2.0, vec![make_key_range(k3, k4)]);
        assert_eq!(ranges3.1.id(), 3);
        assert_eq!(ranges3.0, vec![make_key_range(k5, k6)]);
    }
}

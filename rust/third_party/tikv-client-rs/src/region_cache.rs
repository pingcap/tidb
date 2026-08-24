// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use log::debug;
use rand::Rng;
use tokio::sync::Notify;
use tokio::sync::RwLock;

use crate::common::Error;
use crate::kv::ReplicaReadType;
use crate::locate::{
    HealthStatusDetail, MixedReplicaSelection, ReplicaCandidate, ReplicaSelectorState,
    StoreHealthStatus,
};
use crate::pd::Cluster;
use crate::pd::RetryClient;
use crate::pd::RetryClientTrait;
use crate::proto::kvrpcpb;
use crate::proto::metapb::Store;
use crate::proto::metapb::{self};
use crate::region::RegionId;
use crate::region::RegionVerId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::retry::{RetryBackoffer, BO_PD_RPC};
use crate::store::{ClientEventListener, EndpointType};
use crate::Key;
use crate::Result;

const MAX_RETRY_WAITING_CONCURRENT_REQUEST: usize = 4;
const REGION_CACHE_TTL_SECS: i64 = 600;
const REGION_CACHE_TTL_JITTER_SECS: i64 = 60;

/// Cache-local state that client-go keeps beside immutable PD region metadata.
#[derive(Clone, Debug)]
struct CachedRegion {
    region: RegionWithLeader,
    /// Snapshot of every peer store's failure epoch at the time this region
    /// entered the cache. A later failure on any of those stores invalidates
    /// only regions that still carry this old snapshot.
    store_epochs: HashMap<StoreId, u32>,
    /// Epoch seconds; client-go treats `now == ttl` as still live.
    ttl: i64,
    /// A later store-health integration can set this to retain the original
    /// expiry instead of extending it on reads.
    expire_after_ttl: bool,
    tiflash_cursor: Arc<AtomicUsize>,
}

impl CachedRegion {
    fn new(region: RegionWithLeader, store_epochs: HashMap<StoreId, u32>, now: i64) -> Self {
        Self {
            region,
            store_epochs,
            ttl: next_region_cache_ttl(now),
            expire_after_ttl: false,
            tiflash_cursor: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Source `Region.checkRegionCacheTTL`, serialized by the cache lock.
    fn check_ttl(&mut self, now: i64) -> bool {
        if now > self.ttl {
            return false;
        }
        if !self.expire_after_ttl && self.ttl <= now + REGION_CACHE_TTL_SECS {
            self.ttl = next_region_cache_ttl(now);
        }
        true
    }
}

/// The cache-local outcome of client-go's `GetTiFlashRPCContext` peer walk.
/// Transport connection/address failures remain owned by `PdRpcClient`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TiFlashSelectionError {
    CachedRegionMissing,
    CacheExpired,
    NoTiFlashPeer,
    AllStoresFiltered,
}

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_secs() as i64
}

fn next_region_cache_ttl(now: i64) -> i64 {
    let jitter = rand::thread_rng().gen_range(0..REGION_CACHE_TTL_JITTER_SECS);
    now + REGION_CACHE_TTL_SECS + jitter
}

/// Source `regionsHaveGapInRanges` for the single unbounded range used by
/// `BatchLoadRegionsFromKey`. A bounded PD response may end at `limit`, but
/// every returned entry must cover the cursor left by its predecessor.
fn regions_have_no_gap(start_key: &Key, regions: &[RegionWithLeader]) -> bool {
    let mut cursor = start_key.clone();
    for region in regions {
        if !region.contains(&cursor) {
            return false;
        }
        let end = region.end_key();
        if end.is_empty() {
            return true;
        }
        if end <= cursor {
            return false;
        }
        cursor = end;
    }
    !regions.is_empty()
}

struct RegionCacheMap {
    /// RegionVerID -> Region. It stores the concrete region caches.
    /// RegionVerID is the unique identifer of a region *across time*.
    // TODO: does it need TTL?
    ver_id_to_region: HashMap<RegionVerId, CachedRegion>,
    /// Start_key -> RegionVerID
    ///
    /// Invariant: there are no intersecting regions in the map at any time.
    key_to_ver_id: BTreeMap<Key, RegionVerId>,
    /// RegionID -> RegionVerID. Note: regions with identical ID doesn't necessarily
    /// mean they are the same, they can be different regions across time.
    id_to_ver_id: HashMap<RegionId, RegionVerId>,
    /// We don't want to spawn multiple queries querying a same region id. If a
    /// request is on its way, others will wait for its completion.
    on_my_way_id: HashMap<RegionId, Arc<Notify>>,
}

struct CachedStore {
    meta: Store,
    health_status: Arc<StoreHealthStatus>,
    epoch: AtomicU32,
    liveness: AtomicU8,
    health_check_running: AtomicBool,
    load_stats: StdMutex<Option<StoreLoadStats>>,
}

#[derive(Clone, Copy, Debug)]
struct StoreLoadStats {
    estimated_wait: Duration,
    updated_at: Instant,
}

fn is_down_peer(region: &RegionWithLeader, candidate: &metapb::Peer) -> bool {
    region.down_peers.iter().any(|down| {
        down.peer
            .as_ref()
            .is_some_and(|peer| peer.id == candidate.id && peer.store_id == candidate.store_id)
    })
}

fn is_unroutable_peer(region: &RegionWithLeader, candidate: &metapb::Peer) -> bool {
    is_down_peer(region, candidate)
        || (candidate.is_witness
            && region
                .leader
                .as_ref()
                .is_none_or(|leader| leader.id != candidate.id))
}

impl CachedStore {
    fn new(meta: Store) -> Self {
        Self {
            meta,
            health_status: Arc::new(StoreHealthStatus::default()),
            epoch: AtomicU32::new(0),
            liveness: AtomicU8::new(StoreLiveness::Reachable as u8),
            health_check_running: AtomicBool::new(false),
            load_stats: StdMutex::new(None),
        }
    }

    fn update_metadata(&mut self, meta: Store) {
        self.meta = meta;
    }

    fn update_server_load(&self, estimated_wait_ms: u32, now: Instant) {
        *self.load_stats.lock().unwrap() = Some(StoreLoadStats {
            estimated_wait: Duration::from_millis(u64::from(estimated_wait_ms)),
            updated_at: now,
        });
    }

    /// client-go's optimistic estimate subtracts elapsed wall time from the
    /// last TiKV-provided server queue delay.
    fn estimated_wait(&self, now: Instant) -> Duration {
        let Some(stats) = *self.load_stats.lock().unwrap() else {
            return Duration::ZERO;
        };
        stats
            .estimated_wait
            .saturating_sub(now.saturating_duration_since(stats.updated_at))
    }
}

/// Cached source `livenessState`; new stores begin reachable and only a
/// request/health-check transition changes that state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum StoreLiveness {
    Reachable = 0,
    Unreachable = 1,
    Unknown = 2,
}

impl StoreLiveness {
    fn from_encoded(value: u8) -> Self {
        match value {
            0 => Self::Reachable,
            1 => Self::Unreachable,
            2 => Self::Unknown,
            _ => Self::Unknown,
        }
    }
}

impl std::fmt::Display for StoreLiveness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reachable => formatter.write_str("reachable"),
            Self::Unreachable => formatter.write_str("unreachable"),
            Self::Unknown => formatter.write_str("unknown"),
        }
    }
}

impl RegionCacheMap {
    fn new() -> RegionCacheMap {
        RegionCacheMap {
            ver_id_to_region: HashMap::new(),
            key_to_ver_id: BTreeMap::new(),
            id_to_ver_id: HashMap::new(),
            on_my_way_id: HashMap::new(),
        }
    }
}

pub struct RegionCache<Client = RetryClient<Cluster>> {
    region_cache: RwLock<RegionCacheMap>,
    store_cache: StdRwLock<HashMap<StoreId, CachedStore>>,
    bucket_refreshes: StdMutex<HashSet<RegionId>>,
    inner_client: Arc<Client>,
}

impl<Client> RegionCache<Client> {
    pub fn new(inner_client: Arc<Client>) -> RegionCache<Client> {
        RegionCache {
            region_cache: RwLock::new(RegionCacheMap::new()),
            store_cache: StdRwLock::new(HashMap::new()),
            bucket_refreshes: StdMutex::new(HashSet::new()),
            inner_client,
        }
    }
}

impl<C: RetryClientTrait + Send + Sync> RegionCache<C> {
    // Retrieve cache entry by key. If there's no entry, query PD and update cache.
    pub async fn get_region_by_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let mut region_cache_guard = self.region_cache.write().await;
        let res = {
            region_cache_guard
                .key_to_ver_id
                .range::<Key, _>(..=key)
                .next_back()
                .map(|(x, y)| (x.clone(), y.clone()))
        };

        if let Some((_, candidate_region_ver_id)) = res {
            let region = region_cache_guard
                .ver_id_to_region
                .get_mut(&candidate_region_ver_id)
                .unwrap();

            if region.region.contains(key) && region.check_ttl(now_epoch_secs()) {
                return Ok(region.region.clone());
            }
        }
        drop(region_cache_guard);
        self.read_through_region_by_key(key.clone()).await
    }

    /// Retrieves a region with PD bucket metadata. A valid cached bucket is
    /// reused; otherwise this intentionally performs the source `WithBuckets`
    /// lookup rather than treating ordinary region metadata as bucket-aware.
    pub async fn get_region_by_key_with_buckets(&self, key: &Key) -> Result<RegionWithLeader> {
        let region = self.get_region_by_key(key).await?;
        if region.buckets.is_some() {
            return Ok(region);
        }
        let region = self
            .inner_client
            .clone()
            .get_region_with_buckets(key.clone().into())
            .await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    /// Locate the region whose range contains `key` when its end is inclusive.
    ///
    /// This is the region-cache counterpart to client-go's `LocateEndKey`, used
    /// by reverse scans so an exact region boundary belongs to the preceding
    /// region rather than the following one.
    pub async fn get_region_by_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let mut region_cache_guard = self.region_cache.write().await;
        let candidate = region_cache_guard
            .key_to_ver_id
            .range::<Key, _>(..key)
            .next_back()
            .map(|(_, version)| version.clone());
        if let Some(candidate) = candidate {
            let region = region_cache_guard
                .ver_id_to_region
                .get_mut(&candidate)
                .unwrap();
            if region.region.start_key() < *key
                && (region.region.end_key().is_empty() || *key <= region.region.end_key())
                && region.check_ttl(now_epoch_secs())
            {
                return Ok(region.region.clone());
            }
        }
        drop(region_cache_guard);
        let region = self
            .inner_client
            .clone()
            .get_prev_region(key.clone().into())
            .await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    // Retrieve cache entry by RegionId. If there's no entry, query PD and update cache.
    pub async fn get_region_by_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        for _ in 0..=MAX_RETRY_WAITING_CONCURRENT_REQUEST {
            let mut region_cache_guard = self.region_cache.write().await;

            // check cache
            let ver_id = region_cache_guard.id_to_ver_id.get(&id).cloned();
            if let Some(ver_id) = ver_id {
                let region = region_cache_guard
                    .ver_id_to_region
                    .get_mut(&ver_id)
                    .unwrap();
                if region.check_ttl(now_epoch_secs()) {
                    return Ok(region.region.clone());
                }
            }

            // check concurrent requests
            let notify = region_cache_guard.on_my_way_id.get(&id).cloned();
            let notified = notify.as_ref().map(|notify| notify.notified());
            drop(region_cache_guard);

            if let Some(n) = notified {
                n.await;
                continue;
            } else {
                return self.read_through_region_by_id(id).await;
            }
        }
        Err(Error::StringError(format!(
            "Concurrent PD requests failed for {MAX_RETRY_WAITING_CONCURRENT_REQUEST} times"
        )))
    }

    pub async fn get_region_by_id_with_buckets(&self, id: RegionId) -> Result<RegionWithLeader> {
        let region = self.get_region_by_id(id).await?;
        if region.buckets.is_some() {
            return Ok(region);
        }
        let region = self
            .inner_client
            .clone()
            .get_region_by_id_with_buckets(id)
            .await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    pub async fn get_store_by_id(&self, id: StoreId) -> Result<Store> {
        let store = self
            .store_cache
            .read()
            .unwrap()
            .get(&id)
            .map(|store| store.meta.clone());
        match store {
            Some(store) => Ok(store),
            None => self.read_through_store_by_id(id).await,
        }
    }

    /// Force read through (query from PD) and update cache
    pub async fn read_through_region_by_key(&self, key: Key) -> Result<RegionWithLeader> {
        let region = self.inner_client.clone().get_region(key.into()).await?;
        self.add_region(region.clone()).await;
        Ok(region)
    }

    /// Source `BatchLoadRegionsFromKey`: always refreshes a bounded run of
    /// consecutive regions from PD, then caches only regions with a known
    /// leader. Callers still receive every PD result so they can advance the
    /// source range-task cursor through leaderless metadata.
    pub async fn batch_load_regions_from_key(
        &self,
        start_key: Key,
        count: usize,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        loop {
            let scan_started = Instant::now();
            let scanned = self
                .inner_client
                .clone()
                .scan_regions(start_key.clone().into(), Vec::new(), count)
                .await;
            crate::stats::observe_region_cache_scan(scan_started.elapsed(), scanned.is_ok());
            let regions = match scanned {
                Ok(regions) if regions_have_no_gap(&start_key, &regions) => regions,
                Ok(_) => {
                    crate::stats::increment_stale_region_from_pd();
                    backoffer
                        .backoff(
                            BO_PD_RPC,
                            "PD returned regions with gaps while batch loading",
                        )
                        .await
                        .map_err(|error| Error::StringError(error.to_string()))?;
                    continue;
                }
                Err(error) => {
                    backoffer
                        .backoff(BO_PD_RPC, format!("PD ScanRegions failed: {error}"))
                        .await
                        .map_err(|error| Error::StringError(error.to_string()))?;
                    continue;
                }
            };
            let valid_regions = regions
                .into_iter()
                .filter(|region| region.leader.is_some())
                .collect::<Vec<_>>();
            if valid_regions.is_empty() {
                crate::stats::increment_stale_region_from_pd();
                backoffer
                    .backoff(
                        BO_PD_RPC,
                        "PD returned only leaderless regions while batch loading",
                    )
                    .await
                    .map_err(|error| Error::StringError(error.to_string()))?;
                continue;
            }
            for region in &valid_regions {
                self.add_region(region.clone()).await;
            }
            return Ok(valid_regions);
        }
    }

    /// Force read through (query from PD) and update cache
    async fn read_through_region_by_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        // put a notify to let others know the region id is being queried
        let notify = Arc::new(Notify::new());
        {
            let mut region_cache_guard = self.region_cache.write().await;
            region_cache_guard.on_my_way_id.insert(id, notify.clone());
        }

        let region = self.inner_client.clone().get_region_by_id(id).await?;
        self.add_region(region.clone()).await;

        // notify others
        {
            let mut region_cache_guard = self.region_cache.write().await;
            notify.notify_waiters();
            region_cache_guard.on_my_way_id.remove(&id);
        }

        Ok(region)
    }

    async fn read_through_store_by_id(&self, id: StoreId) -> Result<Store> {
        let store = self.inner_client.clone().get_store(id).await?;
        let mut cache = self.store_cache.write().unwrap();
        match cache.get_mut(&id) {
            Some(cached) => cached.update_metadata(store.clone()),
            None => {
                cache.insert(id, CachedStore::new(store.clone()));
            }
        }
        Ok(store)
    }

    /// Insert a PD region unless it is older than the cached region epoch.
    ///
    /// This mirrors `internal/locate.regionIndexMu.insertRegionToCache`: a
    /// delayed PD response must not replace a newer version, and a region whose
    /// end key is empty intersects every following key.
    pub async fn add_region(&self, region: RegionWithLeader) -> bool {
        let store_epochs = {
            let stores = self.store_cache.read().unwrap();
            region
                .region
                .peers
                .iter()
                .map(|peer| {
                    (
                        peer.store_id,
                        stores
                            .get(&peer.store_id)
                            .map_or(0, |store| store.epoch.load(Ordering::Acquire)),
                    )
                })
                .collect::<HashMap<_, _>>()
        };
        let mut cache = self.region_cache.write().await;
        let new_ver_id = region.ver_id();

        let end_key = region.end_key();
        let mut to_be_removed: HashSet<RegionVerId> = HashSet::new();

        if let Some(ver_id) = cache.id_to_ver_id.get(&region.id()) {
            if ver_id.ver > new_ver_id.ver || ver_id.conf_ver > new_ver_id.conf_ver {
                return false;
            }
            if ver_id != &new_ver_id {
                to_be_removed.insert(ver_id.clone());
            }
        }

        let mut search_range = {
            if end_key.is_empty() {
                cache.key_to_ver_id.range::<Key, _>(..)
            } else {
                cache.key_to_ver_id.range::<Key, _>(..end_key)
            }
        };
        while let Some((_, ver_id_in_cache)) = search_range.next_back() {
            let region_in_cache = &cache.ver_id_to_region.get(ver_id_in_cache).unwrap().region;

            if region_in_cache.region.end_key.is_empty()
                || region_in_cache.region.end_key > region.region.start_key
            {
                if region_in_cache
                    .region
                    .region_epoch
                    .as_ref()
                    .unwrap()
                    .version
                    > new_ver_id.ver
                {
                    return false;
                }
                to_be_removed.insert(ver_id_in_cache.clone());
            } else {
                break;
            }
        }

        for ver_id in to_be_removed {
            let cached_region = cache.ver_id_to_region.remove(&ver_id).unwrap();
            cache
                .key_to_ver_id
                .remove(&cached_region.region.start_key());
            if cache.id_to_ver_id.get(&cached_region.region.id()) == Some(&ver_id) {
                cache.id_to_ver_id.remove(&cached_region.region.id());
            }
        }
        cache
            .key_to_ver_id
            .insert(region.start_key(), new_ver_id.clone());
        cache.id_to_ver_id.insert(region.id(), new_ver_id.clone());
        cache.ver_id_to_region.insert(
            new_ver_id,
            CachedRegion::new(region, store_epochs, now_epoch_secs()),
        );
        true
    }

    pub async fn update_leader(
        &self,
        ver_id: crate::region::RegionVerId,
        leader: metapb::Peer,
    ) -> Result<()> {
        let mut cache = self.region_cache.write().await;
        let Some(region) = cache.ver_id_to_region.get(&ver_id) else {
            return Ok(());
        };
        // `replicaSelector.updateLeader` only accepts a NotLeader hint that
        // refers to a peer in the cached region. A different peer proves this
        // region is stale and must be reloaded instead of poisoning its
        // cached leader with unrelated metadata.
        let is_known_peer = region
            .region
            .region
            .peers
            .iter()
            .any(|peer| peer.id == leader.id && peer.store_id == leader.store_id);
        if is_known_peer {
            let region = cache
                .ver_id_to_region
                .get_mut(&ver_id)
                .expect("cached region disappeared while write-locked");
            region.region.leader = Some(leader);
            debug!("updated cached region leader, region: {:?}", ver_id);
        } else {
            let region = cache
                .ver_id_to_region
                .remove(&ver_id)
                .expect("cached region disappeared while write-locked");
            let id = region.region.id();
            let start_key = region.region.start_key();
            cache.key_to_ver_id.remove(&start_key);
            if cache.id_to_ver_id.get(&id) == Some(&ver_id) {
                cache.id_to_ver_id.remove(&id);
            }
            debug!(
                "invalidated cached region after NotLeader hint for unknown peer, region: {:?}",
                ver_id
            );
        }

        Ok(())
    }

    /// Applies TiKV's direct `BucketVersionNotMatch` refresh to the cached
    /// region. Source only replaces missing or older metadata, and binds the
    /// returned keys to the cached region ID rather than trusting a response
    /// to name one.
    pub async fn update_buckets(
        &self,
        ver_id: crate::region::RegionVerId,
        version: u64,
        keys: Vec<Vec<u8>>,
    ) {
        let mut cache = self.region_cache.write().await;
        let Some(cached) = cache.ver_id_to_region.get_mut(&ver_id) else {
            return;
        };
        if cached
            .region
            .buckets
            .as_ref()
            .is_some_and(|buckets| buckets.version >= version)
        {
            return;
        }
        cached.region.buckets = Some(metapb::Buckets {
            region_id: cached.region.id(),
            version,
            keys,
            ..Default::default()
        });
    }

    /// Source `UpdateBucketsIfNeeded`: coalesce background PD reloads when a
    /// response advertises a newer bucket version than the cached region.
    pub(crate) fn update_buckets_if_needed(
        self: &Arc<Self>,
        ver_id: RegionVerId,
        request_version: u64,
        latest_version: u64,
    ) where
        C: 'static,
    {
        let cache = self.clone();
        tokio::spawn(async move {
            let needs_refresh = {
                let regions = cache.region_cache.read().await;
                let Some(region) = regions.ver_id_to_region.get(&ver_id) else {
                    return;
                };
                let cached_version = region.region.buckets_version();
                !(request_version != 0 && request_version < cached_version)
                    && cached_version < latest_version
            };
            if !needs_refresh || !cache.bucket_refreshes.lock().unwrap().insert(ver_id.id) {
                return;
            }
            let _ = cache.get_region_by_id_with_buckets(ver_id.id).await;
            cache.bucket_refreshes.lock().unwrap().remove(&ver_id.id);
        });
    }

    pub async fn invalidate_region_cache(&self, ver_id: crate::region::RegionVerId) {
        let mut cache = self.region_cache.write().await;
        let region_entry = cache.ver_id_to_region.get(&ver_id);
        if let Some(region) = region_entry {
            let id = region.region.id();
            let start_key = region.region.start_key();
            cache.ver_id_to_region.remove(&ver_id);
            cache.id_to_ver_id.remove(&id);
            cache.key_to_ver_id.remove(&start_key);
            debug!("invalidated region cache entry, region: {:?}", ver_id);
        }
    }

    pub async fn invalidate_store_cache(&self, store_id: StoreId) -> Option<Store> {
        let mut cache = self.store_cache.write().unwrap();
        let removed = cache.remove(&store_id).map(|store| store.meta);
        if removed.is_some() {
            debug!("invalidated store cache entry, store: {:?}", store_id);
        }
        removed
    }

    pub async fn read_through_all_stores(&self) -> Result<Vec<Store>> {
        let stores = self
            .inner_client
            .clone()
            .get_all_stores()
            .await?
            .into_iter()
            .filter(is_valid_data_store)
            .collect::<Vec<_>>();
        let mut cache = self.store_cache.write().unwrap();
        for store in &stores {
            match cache.get_mut(&store.id) {
                Some(cached) => cached.update_metadata(store.clone()),
                None => {
                    cache.insert(store.id, CachedStore::new(store.clone()));
                }
            }
        }
        Ok(stores)
    }

    /// Records client-go's stream-delivered TiKV health feedback for the
    /// matching cached store. Unknown stores are intentionally ignored.
    pub(crate) fn record_health_feedback(&self, feedback: &kvrpcpb::HealthFeedback) {
        self.record_health_feedback_at(feedback, Instant::now());
    }

    fn record_health_feedback_at(&self, feedback: &kvrpcpb::HealthFeedback, now: Instant) {
        let health_status = self
            .store_cache
            .read()
            .unwrap()
            .get(&feedback.store_id)
            .map(|store| store.health_status.clone());
        if let Some(health_status) = health_status {
            health_status.record_tikv_slow_score(i64::from(feedback.slow_score), now);
        }
    }

    pub(crate) fn store_health(&self, store_id: StoreId) -> Option<HealthStatusDetail> {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| store.health_status.detail())
    }

    pub(crate) fn store_health_status(&self, store_id: StoreId) -> Option<Arc<StoreHealthStatus>> {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| store.health_status.clone())
    }

    pub(crate) fn store_liveness(&self, store_id: StoreId) -> Option<StoreLiveness> {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| StoreLiveness::from_encoded(store.liveness.load(Ordering::Acquire)))
    }

    pub(crate) fn set_store_liveness(&self, store_id: StoreId, liveness: StoreLiveness) -> bool {
        let store = self
            .store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| {
                store.liveness.store(liveness as u8, Ordering::Release);
            });
        store.is_some()
    }

    /// Returns whether a cached region captured an older failure epoch for a
    /// peer store. Missing snapshots retain compatibility with callers that
    /// supplied an uncached region directly.
    pub(crate) async fn store_epoch_is_stale(
        &self,
        ver_id: &RegionVerId,
        store_id: StoreId,
    ) -> bool {
        let expected = self
            .region_cache
            .read()
            .await
            .ver_id_to_region
            .get(ver_id)
            .and_then(|region| region.store_epochs.get(&store_id).copied());
        let current = self
            .store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| store.epoch.load(Ordering::Acquire));
        expected
            .zip(current)
            .is_some_and(|(expected, current)| expected != current)
    }

    /// Source `invalidateReplicaStore`: advance a store failure epoch only
    /// when this route still owns the captured epoch. That CAS prevents a
    /// stale in-flight request from invalidating a region reloaded after a
    /// newer failure.
    pub(crate) async fn invalidate_store_epoch_for_region(
        &self,
        ver_id: &RegionVerId,
        store_id: StoreId,
    ) -> bool {
        let expected = self
            .region_cache
            .read()
            .await
            .ver_id_to_region
            .get(ver_id)
            .and_then(|region| region.store_epochs.get(&store_id).copied());
        let Some(expected) = expected else {
            return false;
        };
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .is_some_and(|store| {
                let advanced = store
                    .epoch
                    .compare_exchange(
                        expected,
                        expected.wrapping_add(1),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok();
                if advanced {
                    store.health_status.mark_already_slow();
                }
                advanced
            })
    }

    /// Claims the single source health-check loop for an unhealthy TiKV
    /// store. A later send failure observes the existing loop rather than
    /// creating another connection probe for the same destination.
    pub(crate) fn begin_store_health_check(&self, store_id: StoreId) -> bool {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .is_some_and(|store| {
                store
                    .health_check_running
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            })
    }

    pub(crate) fn finish_store_health_check(&self, store_id: StoreId) {
        if let Some(store) = self.store_cache.read().unwrap().get(&store_id) {
            store.health_check_running.store(false, Ordering::Release);
        }
    }

    /// Records the source `ServerIsBusy.estimated_wait_ms` signal for the
    /// resolved store. Unknown stores intentionally remain untouched.
    pub(crate) fn record_server_load(&self, store_id: StoreId, estimated_wait_ms: u32) {
        self.record_server_load_at(store_id, estimated_wait_ms, Instant::now());
    }

    fn record_server_load_at(&self, store_id: StoreId, estimated_wait_ms: u32, now: Instant) {
        if let Some(store) = self.store_cache.read().unwrap().get(&store_id) {
            store.update_server_load(estimated_wait_ms, now);
        }
    }

    pub(crate) fn estimated_store_wait(&self, store_id: StoreId) -> Option<Duration> {
        self.estimated_store_wait_at(store_id, Instant::now())
    }

    fn estimated_store_wait_at(&self, store_id: StoreId, now: Instant) -> Option<Duration> {
        self.store_cache
            .read()
            .unwrap()
            .get(&store_id)
            .map(|store| store.estimated_wait(now))
    }

    /// Runs the source store-health periodic update for every cached store.
    /// The owning region-cache scheduler will call this at its configured
    /// health-check cadence once store liveness is transcreated.
    pub(crate) fn tick_store_health(&self, now: Instant) {
        let health_statuses = self
            .store_cache
            .read()
            .unwrap()
            .values()
            .map(|store| store.health_status.clone())
            .collect::<Vec<_>>();
        for health_status in health_statuses {
            health_status.tick(now);
        }
    }

    /// Produces source replica-selector input snapshots from a cached region
    /// and its resolved stores. The request sender owns retry-attempt counters;
    /// callers provide them keyed by peer ID.
    pub(crate) async fn replica_candidates(
        &self,
        region: &RegionWithLeader,
        labels: &[metapb::StoreLabel],
        stores: &[u64],
        selector_state: &ReplicaSelectorState,
    ) -> Result<Vec<ReplicaCandidate>> {
        for peer in &region.region.peers {
            self.get_store_by_id(peer.store_id).await?;
        }
        let leader_peer_id = region.leader.as_ref().map(|leader| leader.id);
        let store_epochs = self
            .region_cache
            .read()
            .await
            .ver_id_to_region
            .get(&region.ver_id())
            .map(|cached| cached.store_epochs.clone())
            .unwrap_or_default();
        let cached_stores = self.store_cache.read().unwrap();
        Ok(region
            .region
            .peers
            .iter()
            .filter_map(|peer| {
                if is_unroutable_peer(region, peer) {
                    return None;
                }
                let store = cached_stores.get(&peer.store_id)?;
                if store_epochs
                    .get(&peer.store_id)
                    .is_some_and(|epoch| *epoch != store.epoch.load(Ordering::Acquire))
                {
                    return None;
                }
                // `GetTiKVRPCContext` builds its candidate set from the
                // source `tiKVOnly` access mode. TiFlash peers are cached
                // alongside TiKV peers but must never become transactional
                // follower-read candidates.
                if EndpointType::from_store(&store.meta) != EndpointType::TiKv {
                    return None;
                }
                let store_matches = stores.is_empty() || stores.contains(&store.meta.id);
                let label_matches =
                    store_matches
                        && labels.iter().all(|label| {
                            store.meta.labels.iter().any(|current| {
                                current.key == label.key && current.value == label.value
                            })
                        });
                Some(ReplicaCandidate {
                    peer_id: peer.id,
                    is_leader: leader_peer_id == Some(peer.id),
                    is_learner: metapb::PeerRole::try_from(peer.role)
                        .is_ok_and(|role| role == metapb::PeerRole::Learner),
                    label_matches,
                    is_slow: store.health_status.is_slow(),
                    reachable: StoreLiveness::from_encoded(store.liveness.load(Ordering::Acquire))
                        == StoreLiveness::Reachable,
                    attempts: selector_state.attempts(peer.id),
                    data_is_not_ready: selector_state.data_is_not_ready(peer.id),
                })
            })
            .collect())
    }

    /// Selects a TiFlash peer from a still-live cached region. This is kept
    /// separate from `replica_candidates`: client-go's `tiFlashOnly` access
    /// mode never participates in TiKV follower-read selection.
    pub(crate) async fn select_tiflash_peer(
        &self,
        region: &RegionWithLeader,
        load_balance: bool,
        labels: &[metapb::StoreLabel],
    ) -> std::result::Result<metapb::Peer, TiFlashSelectionError> {
        let cursor = {
            let mut regions = self.region_cache.write().await;
            let Some(cached) = regions.ver_id_to_region.get_mut(&region.ver_id()) else {
                return Err(TiFlashSelectionError::CachedRegionMissing);
            };
            if !cached.check_ttl(now_epoch_secs()) {
                return Err(TiFlashSelectionError::CacheExpired);
            }
            cached.tiflash_cursor.clone()
        };

        for peer in &region.region.peers {
            self.get_store_by_id(peer.store_id)
                .await
                .map_err(|_| TiFlashSelectionError::NoTiFlashPeer)?;
        }
        let stores = self.store_cache.read().unwrap();
        let peers = region
            .region
            .peers
            .iter()
            .filter(|peer| {
                if is_unroutable_peer(region, peer) {
                    return false;
                }
                stores.get(&peer.store_id).is_some_and(|store| {
                    EndpointType::from_store(&store.meta) == EndpointType::TiFlash
                })
            })
            .collect::<Vec<_>>();
        if peers.is_empty() {
            return Err(TiFlashSelectionError::NoTiFlashPeer);
        }
        let start = if load_balance {
            cursor.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
        } else {
            cursor.load(Ordering::Relaxed)
        };
        for offset in 0..peers.len() {
            let peer = peers[(start + offset) % peers.len()];
            let store = stores.get(&peer.store_id).unwrap();
            if labels.iter().all(|wanted| {
                store
                    .meta
                    .labels
                    .iter()
                    .any(|actual| actual.key == wanted.key && actual.value == wanted.value)
            }) {
                cursor.store((start + offset) % peers.len(), Ordering::Relaxed);
                return Ok(peer.clone());
            }
        }
        Err(TiFlashSelectionError::AllStoresFiltered)
    }

    /// Returns client-go's `GetAllValidTiFlashStores` result: `all` begins
    /// with the current store and `non_pending` excludes PD pending peers so
    /// batch work can prefer replicas that have caught up with TiKV.
    pub(crate) async fn valid_tiflash_store_ids(
        &self,
        region: &RegionWithLeader,
        current_store_id: StoreId,
        labels: &[metapb::StoreLabel],
    ) -> (Vec<StoreId>, Vec<StoreId>) {
        let mut all = vec![current_store_id];
        let cached_live = {
            let mut regions = self.region_cache.write().await;
            regions
                .ver_id_to_region
                .get_mut(&region.ver_id())
                .is_some_and(|cached| cached.check_ttl(now_epoch_secs()))
        };
        if !cached_live {
            return (all, vec![]);
        }
        let stores = self.store_cache.read().unwrap();
        for peer in &region.region.peers {
            if is_unroutable_peer(region, peer) {
                continue;
            }
            let Some(store) = stores.get(&peer.store_id) else {
                continue;
            };
            if peer.store_id != current_store_id
                && EndpointType::from_store(&store.meta) == EndpointType::TiFlash
                && labels.iter().all(|wanted| {
                    store
                        .meta
                        .labels
                        .iter()
                        .any(|actual| actual.key == wanted.key && actual.value == wanted.value)
                })
            {
                all.push(peer.store_id);
            }
        }
        let non_pending = all
            .iter()
            .copied()
            .filter(|id| !region.pending_peers.iter().any(|peer| peer.store_id == *id))
            .collect();
        (all, non_pending)
    }

    /// Resolves the source mixed-selector decision back to its region peer.
    /// Retry-attempt state remains owned by the request sender, while this
    /// cache method owns the peer/store snapshot and keeps PD resolution out
    /// of the score calculation.
    pub(crate) async fn select_mixed_replica(
        &self,
        region: &RegionWithLeader,
        labels: &[metapb::StoreLabel],
        stores: &[u64],
        selector_state: &ReplicaSelectorState,
        selection: MixedReplicaSelection,
    ) -> Result<Option<metapb::Peer>> {
        let candidates = self
            .replica_candidates(region, labels, stores, selector_state)
            .await?;
        let Some(selected) = selection.choose(&candidates) else {
            return Ok(None);
        };
        Ok(region
            .region
            .peers
            .iter()
            .find(|peer| peer.id == selected.peer_id)
            .cloned())
    }

    /// Source `ReplicaSelectMixedStrategy` path used when a leader read has a
    /// configured busy threshold. The leader itself and every overloaded
    /// replica are excluded; the ordinary mixed score chooses among the
    /// remaining idle followers.
    pub(crate) async fn select_idle_replica(
        &self,
        region: &RegionWithLeader,
        labels: &[metapb::StoreLabel],
        stores: &[u64],
        selector_state: &ReplicaSelectorState,
        busy_threshold: Duration,
    ) -> Result<Option<metapb::Peer>> {
        let candidates = self
            .replica_candidates(region, labels, stores, selector_state)
            .await?;
        let cached_stores = self.store_cache.read().unwrap();
        let idle = candidates
            .into_iter()
            .filter(|candidate| {
                let Some(peer) = region
                    .region
                    .peers
                    .iter()
                    .find(|peer| peer.id == candidate.peer_id)
                else {
                    return false;
                };
                !candidate.is_leader
                    && candidate.reachable
                    && candidate.attempts == 0
                    && !selector_state.is_server_busy(candidate.peer_id)
                    && cached_stores
                        .get(&peer.store_id)
                        .is_some_and(|store| store.estimated_wait(Instant::now()) <= busy_threshold)
            })
            .collect::<Vec<_>>();
        let Some(selected) = (MixedReplicaSelection {
            read_type: ReplicaReadType::Follower,
            leader_only: false,
            prefer_leader: false,
            labels_requested: !labels.is_empty(),
        })
        .choose(&idle) else {
            return Ok(None);
        };
        Ok(region
            .region
            .peers
            .iter()
            .find(|peer| peer.id == selected.peer_id)
            .cloned())
    }

    /// Returns a source-compatible forwarding proxy only for a leader known
    /// to be unreachable. Unknown leader liveness deliberately does not
    /// authorize forwarding. The returned peer is always a non-leader whose
    /// cached store is reachable; callers retain the leader as the logical
    /// request peer.
    pub(crate) async fn proxy_for_unreachable_leader(
        &self,
        region: &RegionWithLeader,
    ) -> Result<Option<metapb::Peer>> {
        let Some(leader) = region.leader.as_ref() else {
            return Ok(None);
        };
        if self.store_liveness(leader.store_id) != Some(StoreLiveness::Unreachable) {
            return Ok(None);
        }
        Ok(self
            .select_mixed_replica(
                region,
                &[],
                &[],
                &ReplicaSelectorState::default(),
                MixedReplicaSelection {
                    read_type: ReplicaReadType::Follower,
                    leader_only: false,
                    prefer_leader: false,
                    labels_requested: false,
                },
            )
            .await?
            .filter(|peer| peer.id != leader.id))
    }
}

struct RegionCacheClientEventListener<Client> {
    cache: std::sync::Weak<RegionCache<Client>>,
}

impl<Client: RetryClientTrait + Send + Sync + 'static> ClientEventListener
    for RegionCacheClientEventListener<Client>
{
    fn on_health_feedback(&self, feedback: &kvrpcpb::HealthFeedback) {
        if let Some(cache) = self.cache.upgrade() {
            cache.record_health_feedback(feedback);
        }
    }
}

impl<Client: RetryClientTrait + Send + Sync + 'static> RegionCache<Client> {
    pub(crate) fn client_event_listener(self: &Arc<Self>) -> Arc<dyn ClientEventListener> {
        Arc::new(RegionCacheClientEventListener {
            cache: Arc::downgrade(self),
        })
    }
}

/// Source `RegionCache.GetAllStores` exposes resolved TiKV and TiFlash stores
/// but excludes tombstones and TiFlash-compute nodes.
fn is_valid_data_store(store: &metapb::Store) -> bool {
    if metapb::StoreState::try_from(store.state).unwrap() == metapb::StoreState::Tombstone {
        return false;
    }
    matches!(
        EndpointType::from_store(store),
        EndpointType::TiKv | EndpointType::TiFlash
    )
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::{
        now_epoch_secs, regions_have_no_gap, CachedStore, MixedReplicaSelection, RegionCache,
        ReplicaCandidate, ReplicaSelectorState, StoreLiveness, REGION_CACHE_TTL_SECS,
    };
    use crate::common::Error;
    use crate::kv::ReplicaReadType;
    use crate::pd::RetryClientTrait;
    use crate::proto::keyspacepb;
    use crate::proto::metapb::RegionEpoch;
    use crate::proto::metapb::{self};
    use crate::region::RegionId;
    use crate::region::RegionWithLeader;
    use crate::region_cache::is_valid_data_store;
    use crate::Key;
    use crate::Result;

    #[derive(Default)]
    struct MockRetryClient {
        pub regions: Mutex<HashMap<RegionId, RegionWithLeader>>,
        pub stores: Mutex<Vec<metapb::Store>>,
        pub get_region_count: AtomicU64,
        pub get_region_with_buckets_count: AtomicU64,
    }

    #[async_trait]
    impl RetryClientTrait for MockRetryClient {
        async fn get_region(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(_, r)| r.contains(&key.clone().into()))
                .map(|(_, r)| r.clone())
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_with_buckets(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_with_buckets_count.fetch_add(1, SeqCst);
            let mut region = self.get_region(key).await?;
            region.buckets = Some(metapb::Buckets {
                region_id: region.id(),
                version: 9,
                keys: vec![
                    region.region.start_key.clone(),
                    region.region.end_key.clone(),
                ],
                ..Default::default()
            });
            Ok(region)
        }

        async fn get_prev_region(
            self: Arc<Self>,
            key: Vec<u8>,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            let key: Key = key.into();
            self.regions
                .lock()
                .await
                .values()
                .filter(|region| {
                    region.start_key() < key
                        && (region.end_key().is_empty() || key <= region.end_key())
                })
                .cloned()
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_by_id(
            self: Arc<Self>,
            region_id: crate::region::RegionId,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_count.fetch_add(1, SeqCst);
            self.regions
                .lock()
                .await
                .iter()
                .filter(|(id, _)| id == &&region_id)
                .map(|(_, r)| r.clone())
                .next()
                .ok_or_else(|| Error::StringError("MockRetryClient: region not found".to_owned()))
        }

        async fn get_region_by_id_with_buckets(
            self: Arc<Self>,
            id: RegionId,
        ) -> Result<crate::region::RegionWithLeader> {
            self.get_region_with_buckets_count.fetch_add(1, SeqCst);
            let mut region = self.get_region_by_id(id).await?;
            region.buckets = Some(metapb::Buckets {
                region_id: id,
                version: 9,
                keys: vec![
                    region.region.start_key.clone(),
                    region.region.end_key.clone(),
                ],
                ..Default::default()
            });
            Ok(region)
        }

        async fn get_store(
            self: Arc<Self>,
            _id: crate::region::StoreId,
        ) -> Result<crate::proto::metapb::Store> {
            todo!()
        }

        async fn get_all_stores(self: Arc<Self>) -> Result<Vec<crate::proto::metapb::Store>> {
            Ok(self.stores.lock().await.clone())
        }

        async fn get_timestamp(self: Arc<Self>) -> Result<crate::proto::pdpb::Timestamp> {
            todo!()
        }

        async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
            todo!()
        }

        async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn cache_is_used() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());
        retry_client.regions.lock().await.insert(
            1,
            RegionWithLeader {
                region: metapb::Region {
                    id: 1,
                    start_key: vec![],
                    end_key: vec![100],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    store_id: 1,
                    ..Default::default()
                }),
                buckets: None,
                pending_peers: vec![],
                down_peers: vec![],
            },
        );
        retry_client.regions.lock().await.insert(
            2,
            RegionWithLeader {
                region: metapb::Region {
                    id: 2,
                    start_key: vec![101],
                    end_key: vec![],
                    region_epoch: Some(RegionEpoch {
                        conf_ver: 0,
                        version: 0,
                    }),
                    peers: vec![
                        metapb::Peer {
                            id: 2,
                            store_id: 2,
                            ..Default::default()
                        },
                        metapb::Peer {
                            id: 102,
                            store_id: 102,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                leader: Some(metapb::Peer {
                    id: 2,
                    store_id: 2,
                    ..Default::default()
                }),
                buckets: None,
                pending_peers: vec![],
                down_peers: vec![],
            },
        );

        assert_eq!(retry_client.get_region_count.load(SeqCst), 0);

        // first query, read through
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // should read from cache
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);

        // invalidate, should read through
        cache
            .invalidate_region_cache(cache.get_region_by_id(1).await?.ver_id())
            .await;
        assert_eq!(cache.get_region_by_id(1).await?.end_key(), vec![100].into());
        assert_eq!(retry_client.get_region_count.load(SeqCst), 2);

        // update leader should work
        cache
            .update_leader(
                cache.get_region_by_id(2).await?.ver_id(),
                metapb::Peer {
                    id: 102,
                    store_id: 102,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            cache.get_region_by_id(2).await?.leader.unwrap().store_id,
            102
        );

        let ver_id = cache.get_region_by_id(2).await?.ver_id();
        cache
            .update_leader(
                ver_id,
                metapb::Peer {
                    id: 999,
                    store_id: 999,
                    ..Default::default()
                },
            )
            .await?;
        // The subsequent lookup must read PD metadata rather than accepting
        // an out-of-region leader hint.
        assert_eq!(cache.get_region_by_id(2).await?.leader.unwrap().store_id, 2);

        Ok(())
    }

    #[tokio::test]
    async fn source_bucket_aware_pd_lookup_refreshes_only_missing_bucket_metadata() -> Result<()> {
        let client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(client.clone());
        let region = region(1, vec![], vec![]);
        client.regions.lock().await.insert(1, region.clone());
        cache.add_region(region).await;

        let with_buckets = cache
            .get_region_by_key_with_buckets(&vec![1].into())
            .await?;
        assert_eq!(with_buckets.buckets_version(), 9);
        assert_eq!(client.get_region_with_buckets_count.load(SeqCst), 1);

        cache
            .get_region_by_key_with_buckets(&vec![1].into())
            .await?;
        assert_eq!(client.get_region_with_buckets_count.load(SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn source_background_bucket_refresh_is_deduplicated() -> Result<()> {
        let client = Arc::new(MockRetryClient::default());
        let cache = Arc::new(RegionCache::new(client.clone()));
        let region = region(1, vec![], vec![]);
        client.regions.lock().await.insert(1, region.clone());
        cache.add_region(region.clone()).await;
        let ver_id = region.ver_id();
        cache.update_buckets_if_needed(ver_id.clone(), 0, 9);
        cache.update_buckets_if_needed(ver_id, 0, 9);
        for _ in 0..20 {
            if cache.get_region_by_id(1).await?.buckets_version() == 9 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(cache.get_region_by_id(1).await?.buckets_version(), 9);
        assert_eq!(client.get_region_with_buckets_count.load(SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_add_disjoint_regions() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());
        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let region3 = region(3, vec![30], vec![]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;
        cache.add_region(region3.clone()).await;

        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region1);
        expected_cache.insert(vec![10].into(), region2);
        expected_cache.insert(vec![30].into(), region3);

        assert(&cache, &expected_cache).await
    }

    #[tokio::test]
    async fn source_bucket_mismatch_only_replaces_older_cached_metadata() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        let region = region(1, vec![], vec![10]);
        let ver_id = region.ver_id();
        cache.add_region(region).await;

        cache
            .update_buckets(ver_id.clone(), 2, vec![vec![], vec![5], vec![10]])
            .await;
        let current = cache.get_region_by_id(1).await.unwrap();
        assert_eq!(current.buckets_version(), 2);
        assert_eq!(
            current.buckets.unwrap().keys,
            vec![vec![], vec![5], vec![10]]
        );

        cache.update_buckets(ver_id.clone(), 2, vec![vec![9]]).await;
        assert_eq!(
            cache
                .get_region_by_id(1)
                .await
                .unwrap()
                .buckets
                .unwrap()
                .keys,
            vec![vec![], vec![5], vec![10]]
        );

        cache
            .update_buckets(ver_id, 3, vec![vec![], vec![10]])
            .await;
        let current = cache.get_region_by_id(1).await.unwrap();
        assert_eq!(current.buckets_version(), 3);
        assert_eq!(current.buckets.unwrap().region_id, 1);
    }

    #[tokio::test]
    async fn test_add_intersecting_regions() {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());

        cache.add_region(region(1, vec![], vec![10])).await;
        cache.add_region(region(2, vec![10], vec![20])).await;
        cache.add_region(region(3, vec![30], vec![40])).await;
        cache.add_region(region(4, vec![50], vec![60])).await;
        cache.add_region(region(5, vec![20], vec![35])).await;

        let mut expected_cache: BTreeMap<Key, _> = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![10].into(), region(2, vec![10], vec![20]));
        expected_cache.insert(vec![20].into(), region(5, vec![20], vec![35]));
        expected_cache.insert(vec![50].into(), region(4, vec![50], vec![60]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(6, vec![15], vec![25])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![15].into(), region(6, vec![15], vec![25]));
        expected_cache.insert(vec![50].into(), region(4, vec![50], vec![60]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(7, vec![20], vec![])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(1, vec![], vec![10]));
        expected_cache.insert(vec![20].into(), region(7, vec![20], vec![]));
        assert(&cache, &expected_cache).await;

        cache.add_region(region(8, vec![], vec![15])).await;
        let mut expected_cache = BTreeMap::new();
        expected_cache.insert(vec![].into(), region(8, vec![], vec![15]));
        expected_cache.insert(vec![20].into(), region(7, vec![20], vec![]));
        assert(&cache, &expected_cache).await;
    }

    #[tokio::test]
    async fn source_region_insert_rejects_stale_epochs_and_removes_max_end_intersections() {
        let cache = Arc::new(RegionCache::new(Arc::new(MockRetryClient::default())));

        let mut latest = region(1, vec![10], vec![20]);
        latest.region.region_epoch.as_mut().unwrap().version = 2;
        assert!(cache.add_region(latest.clone()).await);

        let mut stale = region(1, vec![10], vec![20]);
        stale.region.region_epoch.as_mut().unwrap().version = 1;
        assert!(!cache.add_region(stale).await);

        let mut expected = BTreeMap::new();
        expected.insert(vec![10].into(), latest);
        assert(&cache, &expected).await;

        let max_end = region(2, vec![20], vec![]);
        assert!(cache.add_region(max_end).await);
        let mut replacement = region(3, vec![15], vec![30]);
        replacement.region.region_epoch.as_mut().unwrap().version = 2;
        assert!(cache.add_region(replacement.clone()).await);

        expected.clear();
        expected.insert(vec![15].into(), replacement);
        assert(&cache, &expected).await;
    }

    #[tokio::test]
    async fn source_region_ttl_refreshes_live_entries_and_reloads_expired_ones() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());
        let cached = region(1, vec![], vec![]);
        retry_client
            .regions
            .lock()
            .await
            .insert(cached.id(), cached.clone());
        cache.add_region(cached.clone()).await;

        let now = now_epoch_secs();
        {
            let mut guard = cache.region_cache.write().await;
            guard
                .ver_id_to_region
                .get_mut(&cached.ver_id())
                .unwrap()
                .ttl = now;
        }
        assert_eq!(cache.get_region_by_key(&vec![1].into()).await?, cached);
        {
            let guard = cache.region_cache.read().await;
            assert!(guard.ver_id_to_region[&cached.ver_id()].ttl >= now + REGION_CACHE_TTL_SECS);
        }

        {
            let mut guard = cache.region_cache.write().await;
            guard
                .ver_id_to_region
                .get_mut(&cached.ver_id())
                .unwrap()
                .ttl = now_epoch_secs() - 1;
        }
        assert_eq!(cache.get_region_by_key(&vec![1].into()).await?, cached);
        assert_eq!(retry_client.get_region_count.load(SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn source_health_feedback_updates_only_the_owning_cached_store() {
        let cache = Arc::new(RegionCache::new(Arc::new(MockRetryClient::default())));
        cache.store_cache.write().unwrap().insert(
            7,
            CachedStore::new(metapb::Store {
                id: 7,
                ..Default::default()
            }),
        );
        let feedback = crate::proto::kvrpcpb::HealthFeedback {
            store_id: 7,
            slow_score: 80,
            ..Default::default()
        };
        cache.client_event_listener().on_health_feedback(&feedback);
        assert_eq!(cache.store_health(7).unwrap().tikv_side_slow_score, 80);
        assert!(cache.store_health(7).unwrap().is_slow());
        assert_eq!(cache.store_liveness(7), Some(StoreLiveness::Reachable));
        assert!(cache.set_store_liveness(7, StoreLiveness::Unreachable));
        assert_eq!(cache.store_liveness(7), Some(StoreLiveness::Unreachable));
        assert!(cache.begin_store_health_check(7));
        assert!(!cache.begin_store_health_check(7));
        cache.finish_store_health_check(7);
        assert!(cache.begin_store_health_check(7));
        cache.finish_store_health_check(7);
        assert_eq!(StoreLiveness::Unknown.to_string(), "unknown");
        cache
            .store_cache
            .write()
            .unwrap()
            .get_mut(&7)
            .unwrap()
            .update_metadata(metapb::Store {
                id: 7,
                address: "new-address".to_owned(),
                ..Default::default()
            });
        assert_eq!(cache.store_liveness(7), Some(StoreLiveness::Unreachable));
        assert_eq!(cache.store_health(7).unwrap().tikv_side_slow_score, 80);

        cache.record_health_feedback(&crate::proto::kvrpcpb::HealthFeedback {
            store_id: 8,
            slow_score: 100,
            ..Default::default()
        });
        assert!(cache.store_health(8).is_none());

        cache.store_cache.write().unwrap().insert(
            9,
            CachedStore::new(metapb::Store {
                id: 9,
                ..Default::default()
            }),
        );
        let start = std::time::Instant::now();
        cache.record_health_feedback_at(
            &crate::proto::kvrpcpb::HealthFeedback {
                store_id: 9,
                slow_score: 40,
                ..Default::default()
            },
            start,
        );
        cache.tick_store_health(start + std::time::Duration::from_secs(15));
        assert_eq!(cache.store_health(9).unwrap().tikv_side_slow_score, 35);
    }

    #[test]
    fn source_server_load_estimate_decays_from_the_last_tikv_wait() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        cache.store_cache.write().unwrap().insert(
            7,
            CachedStore::new(metapb::Store {
                id: 7,
                ..Default::default()
            }),
        );
        let start = std::time::Instant::now();
        cache.record_server_load_at(7, 1_000, start);
        assert_eq!(
            cache.estimated_store_wait_at(7, start + std::time::Duration::from_millis(250)),
            Some(std::time::Duration::from_millis(750))
        );
        assert_eq!(
            cache.estimated_store_wait_at(7, start + std::time::Duration::from_millis(1_001)),
            Some(std::time::Duration::ZERO)
        );
        cache.record_server_load(8, 100);
        assert_eq!(cache.estimated_store_wait(8), None);
    }

    #[tokio::test]
    async fn source_replica_candidates_join_region_peers_to_cached_store_state() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        cache.store_cache.write().unwrap().insert(
            1,
            CachedStore::new(metapb::Store {
                id: 1,
                labels: vec![metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "a".to_owned(),
                }],
                ..Default::default()
            }),
        );
        cache.store_cache.write().unwrap().insert(
            2,
            CachedStore::new(metapb::Store {
                id: 2,
                labels: vec![metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "b".to_owned(),
                }],
                ..Default::default()
            }),
        );
        cache.set_store_liveness(2, StoreLiveness::Unreachable);
        cache.store_cache.write().unwrap().insert(
            3,
            CachedStore::new(metapb::Store {
                id: 3,
                labels: vec![metapb::StoreLabel {
                    key: "engine".to_owned(),
                    value: "tiflash".to_owned(),
                }],
                ..Default::default()
            }),
        );

        let leader = metapb::Peer {
            id: 11,
            store_id: 1,
            ..Default::default()
        };
        let follower = metapb::Peer {
            id: 12,
            store_id: 2,
            role: metapb::PeerRole::Learner.into(),
            ..Default::default()
        };
        let tiflash_peer = metapb::Peer {
            id: 13,
            store_id: 3,
            ..Default::default()
        };
        let mut region = region(1, vec![], vec![]);
        region.region.peers = vec![leader.clone(), follower.clone(), tiflash_peer];
        region.leader = Some(leader);
        let mut selector_state = ReplicaSelectorState::default();
        selector_state.record_attempt(12);
        let candidates = cache
            .replica_candidates(
                &region,
                &[metapb::StoreLabel {
                    key: "zone".to_owned(),
                    value: "a".to_owned(),
                }],
                &[],
                &selector_state,
            )
            .await
            .unwrap();
        assert_eq!(
            candidates,
            vec![
                ReplicaCandidate {
                    peer_id: 11,
                    is_leader: true,
                    is_learner: false,
                    label_matches: true,
                    is_slow: false,
                    reachable: true,
                    attempts: 0,
                    data_is_not_ready: false,
                },
                ReplicaCandidate {
                    peer_id: 12,
                    is_leader: false,
                    is_learner: true,
                    label_matches: false,
                    is_slow: false,
                    reachable: false,
                    attempts: 1,
                    data_is_not_ready: false,
                },
            ]
        );

        assert!(cache.set_store_liveness(2, StoreLiveness::Reachable));
        let selected = cache
            .select_mixed_replica(
                &region,
                &[],
                &[],
                &ReplicaSelectorState::default(),
                MixedReplicaSelection {
                    read_type: ReplicaReadType::Follower,
                    leader_only: false,
                    prefer_leader: false,
                    labels_requested: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(selected, Some(follower.clone()));

        cache.record_server_load(1, 1_000);
        let idle = cache
            .select_idle_replica(
                &region,
                &[],
                &[],
                &ReplicaSelectorState::default(),
                std::time::Duration::from_millis(500),
            )
            .await
            .unwrap();
        assert_eq!(idle, Some(follower.clone()));

        let mut busy_follower = ReplicaSelectorState::default();
        busy_follower.record_server_busy(follower.id);
        let no_idle = cache
            .select_idle_replica(
                &region,
                &[],
                &[],
                &busy_follower,
                std::time::Duration::from_millis(500),
            )
            .await
            .unwrap();
        assert_eq!(no_idle, None);

        let selected = cache
            .select_mixed_replica(
                &region,
                &[],
                &[1],
                &ReplicaSelectorState::default(),
                MixedReplicaSelection {
                    read_type: ReplicaReadType::Follower,
                    leader_only: false,
                    prefer_leader: false,
                    labels_requested: false,
                },
            )
            .await
            .unwrap();
        // Source `WithMatchStores` raises the matching store's score; it does
        // not make non-matching stores ineligible.
        assert_eq!(selected, region.leader.clone());

        assert!(cache.set_store_liveness(1, StoreLiveness::Unreachable));
        let proxy = cache.proxy_for_unreachable_leader(&region).await.unwrap();
        assert_eq!(proxy, Some(follower));
    }

    #[tokio::test]
    async fn source_tiflash_selection_rotates_only_tiflash_peers() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        let leader = metapb::Peer {
            id: 11,
            store_id: 1,
            ..Default::default()
        };
        let flash_one = metapb::Peer {
            id: 12,
            store_id: 2,
            ..Default::default()
        };
        let flash_two = metapb::Peer {
            id: 13,
            store_id: 3,
            ..Default::default()
        };
        let mut region = region(1, vec![], vec![]);
        region.region.peers = vec![leader.clone(), flash_one.clone(), flash_two.clone()];
        region.leader = Some(leader);
        region.pending_peers = vec![flash_two.clone()];
        cache.add_region(region.clone()).await;
        for (id, engine, zone) in [(1, "tikv", "a"), (2, "tiflash", "a"), (3, "tiflash", "b")] {
            cache.store_cache.write().unwrap().insert(
                id,
                CachedStore::new(metapb::Store {
                    id,
                    labels: vec![
                        metapb::StoreLabel {
                            key: "engine".into(),
                            value: engine.into(),
                        },
                        metapb::StoreLabel {
                            key: "zone".into(),
                            value: zone.into(),
                        },
                    ],
                    ..Default::default()
                }),
            );
        }
        assert_eq!(
            cache.select_tiflash_peer(&region, true, &[]).await,
            Ok(flash_two.clone())
        );
        assert_eq!(
            cache.select_tiflash_peer(&region, true, &[]).await,
            Ok(flash_one.clone())
        );
        assert_eq!(
            cache
                .select_tiflash_peer(
                    &region,
                    false,
                    &[metapb::StoreLabel {
                        key: "zone".into(),
                        value: "b".into()
                    }]
                )
                .await,
            Ok(flash_two.clone())
        );
        assert_eq!(
            cache
                .select_tiflash_peer(
                    &region,
                    false,
                    &[metapb::StoreLabel {
                        key: "zone".into(),
                        value: "missing".into()
                    }]
                )
                .await,
            Err(super::TiFlashSelectionError::AllStoresFiltered)
        );
        assert_eq!(
            cache.valid_tiflash_store_ids(&region, 2, &[]).await,
            (vec![2, 3], vec![2])
        );
        region.region.peers[2].is_witness = true;
        assert_eq!(
            cache.select_tiflash_peer(&region, false, &[]).await,
            Ok(flash_one.clone())
        );
        assert_eq!(
            cache.valid_tiflash_store_ids(&region, 2, &[]).await,
            (vec![2], vec![2])
        );
        region.region.peers[2].is_witness = false;
        region.down_peers = vec![crate::proto::pdpb::PeerStats {
            peer: Some(flash_two),
            ..Default::default()
        }];
        assert_eq!(
            cache.select_tiflash_peer(&region, false, &[]).await,
            Ok(flash_one)
        );
        assert_eq!(
            cache.valid_tiflash_store_ids(&region, 2, &[]).await,
            (vec![2], vec![2])
        );
    }

    #[tokio::test]
    async fn source_store_failure_epoch_invalidates_only_its_cached_snapshot() {
        let cache = RegionCache::new(Arc::new(MockRetryClient::default()));
        cache.store_cache.write().unwrap().insert(
            1,
            CachedStore::new(metapb::Store {
                id: 1,
                ..Default::default()
            }),
        );
        let peer = metapb::Peer {
            id: 11,
            store_id: 1,
            ..Default::default()
        };
        let mut region = region(1, vec![], vec![]);
        region.region.peers = vec![peer.clone()];
        region.leader = Some(peer);
        assert!(cache.add_region(region.clone()).await);

        assert!(!cache.store_epoch_is_stale(&region.ver_id(), 1).await);
        assert!(
            cache
                .invalidate_store_epoch_for_region(&region.ver_id(), 1)
                .await
        );
        assert!(cache.store_epoch_is_stale(&region.ver_id(), 1).await);
        assert!(
            !cache
                .invalidate_store_epoch_for_region(&region.ver_id(), 1)
                .await
        );
        assert!(cache
            .replica_candidates(&region, &[], &[], &ReplicaSelectorState::default())
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_get_region_by_key() -> Result<()> {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client.clone());

        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        let region3 = region(3, vec![30], vec![40]);
        let region4 = region(4, vec![50], vec![]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;
        cache.add_region(region3.clone()).await;
        cache.add_region(region4.clone()).await;

        assert_eq!(
            cache.get_region_by_key(&vec![].into()).await?,
            region1.clone()
        );
        assert_eq!(
            cache.get_region_by_key(&vec![5].into()).await?,
            region1.clone()
        );
        assert_eq!(
            cache.get_region_by_key(&vec![10].into()).await?,
            region2.clone()
        );
        assert!(cache.get_region_by_key(&vec![20].into()).await.is_err());
        assert!(cache.get_region_by_key(&vec![25].into()).await.is_err());
        assert_eq!(cache.get_region_by_key(&vec![60].into()).await?, region4);
        Ok(())
    }

    #[tokio::test]
    async fn end_key_lookup_treats_region_starts_as_belonging_to_the_previous_region() -> Result<()>
    {
        let retry_client = Arc::new(MockRetryClient::default());
        let cache = RegionCache::new(retry_client);
        let region1 = region(1, vec![], vec![10]);
        let region2 = region(2, vec![10], vec![20]);
        cache.add_region(region1.clone()).await;
        cache.add_region(region2.clone()).await;

        assert_eq!(
            cache.get_region_by_end_key(&vec![10].into()).await?,
            region1
        );
        assert_eq!(
            cache.get_region_by_end_key(&vec![11].into()).await?,
            region2.clone()
        );
        assert_eq!(
            cache.get_region_by_end_key(&vec![20].into()).await?,
            region2
        );
        Ok(())
    }

    // a helper function to assert the cache is in expected state
    async fn assert(
        cache: &RegionCache<MockRetryClient>,
        expected_cache: &BTreeMap<Key, RegionWithLeader>,
    ) {
        let guard = cache.region_cache.read().await;
        let mut actual_keys = guard
            .ver_id_to_region
            .values()
            .map(|cached| &cached.region)
            .collect::<Vec<_>>();
        let mut expected_keys = expected_cache.values().collect::<Vec<_>>();
        actual_keys.sort_by_cached_key(|r| r.id());
        expected_keys.sort_by_cached_key(|r| r.id());

        assert_eq!(actual_keys, expected_keys);
        assert_eq!(
            guard.key_to_ver_id.keys().collect::<HashSet<_>>(),
            expected_cache.keys().collect::<HashSet<_>>()
        )
    }

    fn region(id: RegionId, start_key: Vec<u8>, end_key: Vec<u8>) -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = id;
        region.region.start_key = start_key;
        region.region.end_key = end_key;
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });
        // We don't care about other fields here

        region
    }

    #[test]
    fn source_batch_scan_rejects_empty_and_gapped_regions() {
        let start: Key = vec![10].into();
        assert!(!regions_have_no_gap(&start, &[]));
        assert!(!regions_have_no_gap(
            &start,
            &[region(1, vec![11], vec![20])]
        ));
        assert!(!regions_have_no_gap(
            &start,
            &[region(1, vec![10], vec![20]), region(2, vec![21], vec![])]
        ));
        assert!(regions_have_no_gap(
            &start,
            &[region(1, vec![10], vec![20]), region(2, vec![20], vec![])]
        ));
    }

    #[test]
    fn source_all_stores_includes_tikv_and_tiflash_only() {
        let mut store = metapb::Store::default();
        assert!(is_valid_data_store(&store));

        store.state = metapb::StoreState::Tombstone.into();
        assert!(!is_valid_data_store(&store));

        store.state = metapb::StoreState::Up.into();
        assert!(is_valid_data_store(&store));

        store.labels.push(metapb::StoreLabel {
            key: "some_key".to_owned(),
            value: "some_value".to_owned(),
        });
        assert!(is_valid_data_store(&store));

        store.labels.push(metapb::StoreLabel {
            key: "engine".to_owned(),
            value: "tiflash".to_owned(),
        });
        assert!(is_valid_data_store(&store));

        store.labels[1].value = "tiflash_compute".to_string();
        assert!(!is_valid_data_store(&store));

        store.labels[1].value = "other".to_string();
        assert!(is_valid_data_store(&store));
    }

    #[tokio::test]
    async fn source_all_store_refresh_caches_tikv_and_tiflash_not_compute() {
        let client = Arc::new(MockRetryClient::default());
        client.stores.lock().await.extend([
            metapb::Store {
                id: 1,
                ..Default::default()
            },
            metapb::Store {
                id: 2,
                labels: vec![metapb::StoreLabel {
                    key: "engine".to_owned(),
                    value: "tiflash".to_owned(),
                }],
                ..Default::default()
            },
            metapb::Store {
                id: 3,
                labels: vec![metapb::StoreLabel {
                    key: "engine".to_owned(),
                    value: "tiflash_compute".to_owned(),
                }],
                ..Default::default()
            },
        ]);
        let cache = RegionCache::new(client);

        assert_eq!(
            cache
                .read_through_all_stores()
                .await
                .unwrap()
                .into_iter()
                .map(|store| store.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(cache.store_cache.read().unwrap().contains_key(&1));
        assert!(cache.store_cache.read().unwrap().contains_key(&2));
        assert!(!cache.store_cache.read().unwrap().contains_key(&3));
    }
}
